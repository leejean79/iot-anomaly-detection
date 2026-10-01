# 主节点升配（4 核 8 GB）与重跑前配置变更操作手册（2026-10-01）

依据：2026-10-01 裁决书《三月第一次运行失败的处置与重跑条件》第二节（基础设施）与第三节第 2 条
（检查点 DEBUG 日志）。本手册完成四件事：

1. 主节点云主机停机改配为 4 核 8 GB；
2. 作业管理器进程内存提到 2048 MB；
3. 作业管理器与任务管理器的帧上限都提到 256 MB；
4. 作业管理器的检查点包日志调到 DEBUG。

## 一、哪些数据必须保住，靠什么保住

| 数据 | 存放位置 | 改配会不会丢 | 本手册的保护措施 |
| --- | --- | --- | --- |
| Kafka 主题（含旧项目 FA-iForest 的主题） | kafka-1 容器的匿名卷（worker 上的 kafka-2、kafka-3 不受影响） | 关机、重启不丢；**重建容器会丢** | 只停止、启动，绝不重建；前后核对卷名与末端偏移 |
| ZooKeeper 数据（Kafka 集群编号、主题元数据） | zookeeper 容器的匿名卷 | 同上 | 同上。ZooKeeper 一旦重建，三台代理都会因集群编号不符而拒绝启动 |
| 登记数据集、重放源数据 | 主机目录 `/opt/fa-iforest/datasets/` | 不丢（主机磁盘） | 无需操作 |
| 作业 jar | 主机目录 `/opt/fa-iforest/jars/` | 不丢 | 无需操作 |
| 三月第一次运行的诊断材料 | 已入库 `docs/m3_march/run1/` | 不涉及 | 无需操作 |

本手册中被**重建**的只有 jobmanager 与两台 taskmanager，它们不保存需要保留的数据：目前没有作业在运行，
jar 目录是主机挂载。

## 二、操作步骤

以下命令都在**本地 Mac 的仓库根目录**执行，第二步除外（在阿里云控制台操作）。各步骤要在同一个终端里执行，
因为第零步设置的 `BROKERS` 等变量后面还要用。

### 第零步：确认空闲并留底

```bash
git pull --rebase origin dev-claude
ssh fa-master "docker exec jobmanager flink list"
bash deploy/scripts/syn-replay.sh status
```

期望：第二条显示没有运行中的作业（`No running jobs`），第三条显示重放器未运行。若有作业在运行，先停下来
告诉我。

然后记录改配前的状态：容器创建时间、两个数据卷的名字、所有主题的末端偏移合计。

```bash
set -a; source deploy/.env; set +a
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
mkdir -p docs/infra
ssh fa-master "docker ps -a --format '{{.Names}}|{{.CreatedAt}}|{{.Status}}'; \
    docker inspect -f '{{.Name}} 卷={{range .Mounts}}{{if .Name}}{{.Name}} {{end}}{{end}}' kafka-1 zookeeper; \
    docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $BROKERS --time -1 \
      | awk -F: '{s+=\$3; n++} END{print \"全部主题分区数\", n, \"末端偏移合计\", s}'" \
    | tee docs/infra/master_before_resize.txt
```

期望：文件里有 kafka-1、zookeeper 的创建时间与卷名（每个卷名是一串 64 位十六进制字符），最后一行是分区数与
偏移合计。

### 第一步：有序停止主节点上的 Kafka 与 ZooKeeper

```bash
ssh fa-master "docker stop -t 120 kafka-1 && docker stop -t 60 zookeeper"
ssh fa-master "docker ps -a --format '{{.Names}} {{.Status}}' | grep -E 'kafka-1|zookeeper'"
```

期望：两个容器都是 `Exited`。退出码 0 或 143 都属正常关闭；137 表示超时后被强杀，数据仍在，只是 Kafka
下次启动时要做日志恢复，稍慢一些。

为什么要先手动停这两个容器，而不是直接关机：

- **关机时的等待太短。** 关机时 Docker 只给每个容器 10 秒，Kafka 可能来不及正常关闭。这里给 120 秒，
  并且趁 ZooKeeper 还在时先关 Kafka。
- **方便按顺序启动。** 手动停止过的容器，开机后不会被 Docker 自动拉起，第四步可以按「先 ZooKeeper、
  后 Kafka」的顺序启动它们。

这期间 worker 上的 kafka-2、kafka-3 会在日志里报告连不上 ZooKeeper，属于预期现象，ZooKeeper 回来后它们会
自动重连。

### 第二步：阿里云控制台停机改配

1. 在控制台对主节点实例执行「停止」（普通停止，不要强制停止）。
2. 停止后执行「更改实例规格」，选 4 vCPU、8 GiB。
3. 启动实例。

注意：没有绑定弹性公网 IP 时，停机后公网 IP 可能会变，第三步会处理；内网 IP 不变。

### 第三步：刷新 IP，确认规格

```bash
bash deploy/scripts/refresh-ips.sh
ssh fa-master 'nproc; free -g | head -2; docker ps -a --format "{{.Names}} {{.Status}}"'
```

期望：

- `nproc` 输出 4，内存总量约 7 至 8 GB。
- zookeeper、kafka-1 仍是 `Exited`。
- jobmanager、prometheus 等其他容器已经自动启动。jobmanager 此时还是旧配置，第六步会重建它。

**不要执行 `2-up-all.sh`**：它会让 compose 发现 `.env` 中公网 IP 变了而重建 kafka-1，数据随之丢失。

### 第四步：按顺序启动 ZooKeeper 与 Kafka，并核对数据完好

```bash
ssh fa-master "docker start zookeeper && sleep 15 && docker start kafka-1 && sleep 45"
ssh fa-master "docker ps -a --format '{{.Names}}|{{.CreatedAt}}|{{.Status}}'; \
    docker inspect -f '{{.Name}} 卷={{range .Mounts}}{{if .Name}}{{.Name}} {{end}}{{end}}' kafka-1 zookeeper; \
    docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $BROKERS --time -1 \
      | awk -F: '{s+=\$3; n++} END{print \"全部主题分区数\", n, \"末端偏移合计\", s}'" \
    | tee docs/infra/master_after_resize.txt
diff <(grep -E '^(kafka-1|zookeeper)\||卷=|偏移合计' docs/infra/master_before_resize.txt | sed -E 's/\|(Up|Exited|Created|Restarting).*$//') \
     <(grep -E '^(kafka-1|zookeeper)\||卷=|偏移合计' docs/infra/master_after_resize.txt  | sed -E 's/\|(Up|Exited|Created|Restarting).*$//') \
  && echo "创建时间、卷名、偏移合计全部一致，数据完好"
ssh fa-master "docker exec kafka-1 kafka-topics.sh --bootstrap-server $BROKERS --describe --unavailable-partitions"
```

期望：

- 打印「创建时间、卷名、偏移合计全部一致，数据完好」。
- 最后一条命令没有任何输出，表示所有分区都有可用的首领。

常见失败：

- **kafka-1 启动后很快退出。** 用 `ssh fa-master "docker logs --tail 100 kafka-1"` 查看。若是连不上
  ZooKeeper，等 30 秒后执行 `ssh fa-master "docker start kafka-1"` 再试。
- **出现 `InconsistentClusterIdException`。** 说明 ZooKeeper 的数据没了。请立刻停下来告诉我，不要做任何
  重建操作。
- **diff 显示偏移合计不同，或者最后一条命令列出了分区。** 请把输出发给我，不要继续往下做。

### 第五步：更新三台机器的配置（`.env` 只改两项，compose 文件同步）

本地 `.env`（macOS 的 sed）：

```bash
sed -i '' 's/^JM_HEAP_MB=.*/JM_HEAP_MB=2048/' deploy/.env
grep -q '^SYN_AKKA_FRAMESIZE=' deploy/.env \
  && sed -i '' 's/^SYN_AKKA_FRAMESIZE=.*/SYN_AKKA_FRAMESIZE=268435456b/' deploy/.env \
  || echo 'SYN_AKKA_FRAMESIZE=268435456b' >> deploy/.env
grep -E '^(JM_HEAP_MB|SYN_AKKA_FRAMESIZE)=' deploy/.env
set -a; source deploy/.env; set +a
```

三台机器上的 `.env` 只改这两行，其余内容（包括可能已过时的公网 IP）一律不动：

```bash
for h in fa-master fa-worker1 fa-worker2; do
  echo "== $h"
  ssh "$h" "f=/opt/fa-iforest/.env; sed -i 's/^JM_HEAP_MB=.*/JM_HEAP_MB=2048/' \$f; \
    grep -q '^SYN_AKKA_FRAMESIZE=' \$f && sed -i 's/^SYN_AKKA_FRAMESIZE=.*/SYN_AKKA_FRAMESIZE=268435456b/' \$f \
      || echo 'SYN_AKKA_FRAMESIZE=268435456b' >> \$f; grep -E '^(JM_HEAP_MB|SYN_AKKA_FRAMESIZE)=' \$f"
done
scp deploy/compose/docker-compose.master.yml deploy/compose/log4j-console-jm.properties fa-master:/opt/fa-iforest/compose/
scp deploy/compose/docker-compose.worker.yml fa-worker1:/opt/fa-iforest/compose/
scp deploy/compose/docker-compose.worker.yml fa-worker2:/opt/fa-iforest/compose/
```

期望：本地与三台机器各打印两行，分别为 `JM_HEAP_MB=2048` 与 `SYN_AKKA_FRAMESIZE=268435456b`。帧上限必须用纯字节
写法（以小写 `b` 结尾）；写成 `256mb` 会让作业管理器启动失败。

### 第六步：只重建 jobmanager

```bash
ssh fa-master "cd /opt/fa-iforest/compose && \
    docker compose -f docker-compose.master.yml --env-file ../.env up -d --no-deps jobmanager"
```

期望：输出里只出现 jobmanager 的 `Recreate`/`Started`。`--no-deps jobmanager` 保证不碰同一个文件里的
kafka-1 与 zookeeper。如果输出里出现 kafka-1 或 zookeeper，请立刻按 Ctrl+C 并告诉我。

### 第七步：只重建两台 taskmanager（帧上限两侧必须一致）

```bash
ssh fa-worker1 "cd /opt/fa-iforest/compose && BROKER_ID=2 NODE_SELF_IP=$NODE_WORKER1_IP \
    docker compose -f docker-compose.worker.yml --env-file ../.env up -d --no-deps taskmanager"
ssh fa-worker2 "cd /opt/fa-iforest/compose && BROKER_ID=3 NODE_SELF_IP=$NODE_WORKER2_IP \
    docker compose -f docker-compose.worker.yml --env-file ../.env up -d --no-deps taskmanager"
```

期望：每台只重建 taskmanager，kafka-2、kafka-3 不动。

### 第八步：核对

```bash
ssh fa-master "curl -s http://$NODE_MASTER_IP:8081/jobmanager/config" | python3 -c 'import json,sys
for k in json.load(sys.stdin):
    if k["key"] in ("jobmanager.memory.process.size","jobmanager.memory.heap.size","akka.framesize"): print(k["key"],"=",k["value"])'
ssh fa-master "curl -s http://$NODE_MASTER_IP:8081/overview"; echo
for p in "fa-worker1 taskmanager-2" "fa-worker2 taskmanager-3"; do set -- $p
  ssh "$1" "docker exec $2 grep '^akka.framesize' /opt/flink/conf/flink-conf.yaml; echo OMP=\$(docker exec $2 printenv OMP_NUM_THREADS)"
done
ssh fa-master "docker exec jobmanager grep -A1 '^logger.checkpoint.name' /opt/flink/conf/log4j-console.properties"
for h in fa-master fa-worker1 fa-worker2; do ssh "$h" "docker ps --format '{{.Names}}|{{.CreatedAt}}' | grep -E 'kafka|zookeeper'"; done
```

期望：

- `jobmanager.memory.process.size = 2048m`，`jobmanager.memory.heap.size` 约 1,459 MiB
  （1530082099b 左右），`akka.framesize = 268435456b`。
- `overview` 里 `"taskmanagers":2`、`"slots-total":8`。
- 两台 taskmanager 都是 `akka.framesize: 268435456b`，`OMP=1`。
- 作业管理器的日志配置里有 `logger.checkpoint.level = DEBUG`。
- 三个 Kafka 容器与 zookeeper 的创建时间都是原来的日期，没有变成今天。

常见失败：

- **作业管理器起不来，REST 无响应。** 用 `ssh fa-master "docker logs --tail 100 jobmanager"` 查看。
  若报 `Could not parse size-in-bytes unit`，说明帧上限的写法不对，回到第五步检查。
- **`taskmanagers` 少于 2。** 等 30 秒再查；仍不够就查看对应 taskmanager 的 `docker logs --tail 50`。

### 第九步：提交留底文件

```bash
git add -f docs/infra/master_before_resize.txt docs/infra/master_after_resize.txt
git commit -m "docs(infra): 主节点升配前后的容器与数据核对" && git push origin dev-claude
```

## 三、之后

重跑前照常执行 `bash deploy/scripts/syn-reset-env.sh` 作为开跑门槛。它会清空 Kafka 里的 synergia 主题，
三月第一次运行留下的数据随之清除。这部分数据已经不需要，第一次运行的诊断材料已入库。重跑的步骤会在三项
代码改动完成并推送后另行给出。
