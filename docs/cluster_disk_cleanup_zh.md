# 集群磁盘清理：现状判断、安全边界与分级清理

- **版本**：1.0
- **日期**：2026-09-19
- **触发**：开机后三台节点的可用空间分别为 master 4.6 GB（已用 88%）、worker-1 23 GB（41%）、
  worker-2 9.7 GB（75%），不足以支撑一轮完整的三月重放。
- **配套脚本**：`deploy/scripts/syn-disk-report.sh`（只读盘点，不删除任何内容）。

---

## 1. 为什么会满，以及这是不是意外

这不是意外，是 `deploy/env.example` 第 155 行早就写下的告警兑现了：

> 磁盘兜底：全集一轮 source≈5GB、m1-out≈5–10GB（RF2 翻倍），三节点各约 28GB，单轮 + 实验间清理
> 够用；**若跑多轮不清理需留意磁盘**，必要时对个别 topic 改用 `retention.bytes`。

成因有两条，都在设计上是有意为之：

第一条，`SYN_RETENTION_MS=-1`，即**永不按时间过期**。这一条不能改，原因写在同一段注释里：重放器
写入 `synergia-source`、M1 写入 `synergia-m1-out` 与 `synergia-monitoring` 的消息都显式盖**事件
时间**戳（2022 年的日期），而 Kafka 按消息自带时间戳计算年龄，一旦开启时间保留，这些消息会在写入
后几分钟内被判定超期并清空日志段，`--from-beginning` 将一条也读不到，验收与基线脚本全部失效。
因此清理只能是**实验之间显式删除**，也就是 `syn-clean-topics.sh` 的职责。

第二条，Kafka 容器**没有挂载数据卷**（见 `deploy/compose/docker-compose.master.yml` 与
`docker-compose.worker.yml`，kafka 服务下没有 `volumes:` 段）。这意味着 topic 数据全部落在容器的
**可写层**里，`du -sh /` 看不出是哪个 topic 占的，必须进容器量——这也正是
`syn-disk-report.sh` 第 5 小节要做的事。

至于「上一轮污染运行的数据是否还在盘上」，我不做推断，由盘点结果给出答案。

---

## 2. 硬性安全边界：以下四件事绝不可做

这四条不是建议，是边界。其中前两条一旦触碰，**旧项目 FA-iForest 的全部 Kafka 数据会不可恢复地
消失**，因为它和本项目的数据在同一个容器可写层里。

1. **绝不可 `docker rm` 或 `docker compose down` Kafka 与 ZooKeeper 容器。** 容器即数据。停止
   （`docker stop`）是安全的，删除不是。同理，如果 `2-up-all.sh` 的输出里出现
   `Recreating kafka-1`（而不是 `Starting kafka-1`），说明 compose 认为配置变了要重建容器——
   此时应**立即中止**并先查清是哪个变量变了。
2. **绝不可执行 `docker system prune -a` 或 `docker container prune`。** 前者会删掉所有未被
   **运行中**容器使用的镜像，后者会删掉所有已停止的容器（连同其可写层里的数据）。只允许使用
   下面第 3 节点名的、带明确过滤条件的命令。
3. **绝不可删除非 `synergia-` 前缀的 topic。** 旧项目的 `source-topic`、`tree-topic` 等一律不碰。
   `syn-clean-topics.sh` 内置了硬编码前缀白名单，在设计上无法删除它们；手工执行
   `kafka-topics --delete` 时则没有这层保护，所以不要手工删。
4. **绝不可删除 `$SYN_DATASET_DIR`（默认 `/opt/fa-iforest/datasets/synergia/files_csv`，约 2.3 GB）。**
   重放器直接读它，删了就要重新上传 2.3 GB。

---

## 3. 第一步：先盘点，再决定删什么

```bash
# 1. 执行环境：本地 Mac（bash），仓库根目录；ssh 免密到三台节点
# 2. 调用命令：
bash deploy/scripts/syn-disk-report.sh
bash deploy/scripts/syn-disk-report.sh --node master --top 40   # 只看 master、多打几行
# 3. 前置条件：三台节点已开机、ssh 可达、Docker 守护进程在跑
# 4. 期望产出：每台节点九个小节的报告——文件系统、docker system df、各容器可写层大小、镜像、
#    Kafka 逐 topic 数据量（区分 synergia-* 与旧项目）、REMOTE_HOME 明细、容器 json 日志、
#    journal、可回收候选汇总。脚本是只读的，不会删除任何内容。
# 5. 常见失败兜底：ssh 不通 → 先跑 refresh-ips.sh；容器未运行 → 该小节标 SKIP，其余照常输出
```

盘点报告的第 5 小节会直接回答最关键的问题：**本项目的 `synergia-*` 合计占了多少，旧项目占了多少**。
在看到这两个数字之前，不要执行第 4 节的任何删除动作。

---

## 4. 分级清理清单

按「安全性从高到低」排列。每一级都给出执行环境、命令、预计回收量与风险。除非上一级腾出的空间
已经够用，否则不必执行下一级。

### 4.1 第一级：完全安全，不影响任何数据

**(a) 删除镜像 tar 包。** `1-sync-to-nodes.sh` 会把 `fa-iforest-flink.tar` 分发到每个节点，
`docker load` 之后这个 tar 就没有用途了，但它一直留在盘上。

```bash
# 执行环境：本地 Mac，仓库根目录；前置条件：镜像已 load（docker images 能看到 fa-iforest/flink:1.13.6-java11）
set -a; source deploy/.env; set +a
for h in "$NODE_MASTER_PUBLIC_IP" "$NODE_WORKER1_PUBLIC_IP" "$NODE_WORKER2_PUBLIC_IP"; do
    ssh -i "$SSH_KEY" "$SSH_USER@$h" \
        "ls -lh ${REMOTE_HOME}/fa-iforest-flink.tar 2>/dev/null && rm -f ${REMOTE_HOME}/fa-iforest-flink.tar && echo '  已删除'"
done
```
预计回收：每节点约 642 MB。风险：无；日后需要重新分发镜像时，`syn-sync-flink-image.sh` 会重传。

**(b) 清理悬空镜像。** 只删没有任何标签、也没有容器引用的层。

```bash
ssh -i "$SSH_KEY" "$SSH_USER@$NODE_MASTER_PUBLIC_IP" "docker image prune -f"
```
预计回收：视历史构建次数而定，通常几百 MB 到数 GB。
风险：低。**注意不要加 `-a`**——加了之后会把当前没有运行容器引用的镜像也删掉。

**(c) 截断容器 json 日志。** jobmanager 与 kafka 长期运行，json 日志可能积累到 GB 级。

```bash
# 【先保存证据】上一轮 framesize 越限的日志就在 jobmanager 的容器日志里，若还需要它，先抓到本地：
ssh -i "$SSH_KEY" "$SSH_USER@$NODE_MASTER_PUBLIC_IP" \
    "docker logs jobmanager 2>&1 | grep -n -B 20 'framesize'" > docs/reports/jm_framesize_context.log
# 确认上面这份日志已经存到本地之后，再截断：
ssh -i "$SSH_KEY" "$SSH_USER@$NODE_MASTER_PUBLIC_IP" \
    "truncate -s 0 /var/lib/docker/containers/*/*-json.log && echo '  已截断'"
```
预计回收：见盘点第 7 小节的实测值。风险：历史容器日志丢失，因此**必须先保存 framesize 现场**。

**(d) 收缩 systemd journal。**

```bash
ssh -i "$SSH_KEY" "$SSH_USER@$NODE_MASTER_PUBLIC_IP" "journalctl --vacuum-size=200M"
```
预计回收：通常数百 MB。风险：系统历史日志丢失，不影响实验。

**(e) 删除多余的 jar。** 本项目的 fat jar 单个约 214 MB，反复上传会留下同名覆盖（不累积），但历史
上手工改过名的副本会累积。

```bash
ssh -i "$SSH_KEY" "$SSH_USER@$NODE_MASTER_PUBLIC_IP" "ls -lh ${REMOTE_HOME}/jars"
```
先看清单再逐个删。**必须保留两个**：本项目的 `iot-anomaly-detection-1.0-SNAPSHOT.jar`
与旧项目的 `FA-iForest-1.0-SNAPSHOT.jar`。

### 4.2 第二级：删除本项目上一轮的实验数据

上一轮三月重放因作业重启产生 25,517 条重复轮，**基线已经作废**，其数据没有保留价值。

```bash
# 执行环境：本地 Mac，仓库根目录
# 前置条件：确认没有本项目作业在跑（旧项目作业不受影响，也不会被触碰）
bash deploy/scripts/syn-clean-topics.sh
```
预计回收：等于盘点第 5 小节里 `synergia-* 合计` 的那个数字，三个节点都会回落。
风险：本项目实验数据全部清零——这正是目的。该脚本有硬编码前缀白名单，无法误删旧项目 topic。

**重要提醒**：Kafka 的删除是**异步**的，执行完毕后磁盘不会立刻回落。日志段先被标记、再由后台
线程删除（`file.delete.delay.ms` 默认 60 秒）。请等待一到两分钟后再用
`bash deploy/scripts/syn-disk-report.sh --node master` 复核，不要因为「删了没变小」而重复执行。

### 4.3 第三级：需要你判断的部分

**(f) 旧项目 FA-iForest 的 topic 数据。** 如果盘点显示 master 上 `source-topic` 一类的旧项目
topic 占了十几 GB，那是本次空间紧张的主要来源。**这部分我不建议、也不代为清理**——它属于另一个
项目的数据，是否还需要只有你能判断。若确认不再需要，请你自己执行删除，并且明确知道这一步不可
撤销。

**(g) Flink 的 blob 与临时文件残留。** 作业异常退出后，JobManager 的临时目录里可能留下 jar 副本。

```bash
ssh -i "$SSH_KEY" "$SSH_USER@$NODE_MASTER_PUBLIC_IP" \
    "docker exec jobmanager sh -c 'du -sm /tmp/* 2>/dev/null | sort -rn | head -10'"
```
若看到多个 `blobStore-*` 或 `flink-web-*` 目录且总量可观，可在**确认没有作业在跑**之后删除其中
明显陈旧的那些。每个残留的 jar 副本约 214 MB。

---

## 5. 这一轮三月重放到底需要多少空间

仓库里已有的估计是「一轮 source≈5 GB、m1-out≈5–10 GB，RF=2 翻倍」。但有一个反证需要一并说明：
上一轮实测的记录条数是 `synergia-source` 16,398,352 条、`synergia-m1-out` 2,075,333 条，后者
比前者少一个数量级（一轮一条对八通道原始行），因此 `m1-out` 的真实占用很可能**显著小于**注释里
5–10 GB 的估计。

这两个数字我不打算靠推算调和——`syn-disk-report.sh` 的第 5 小节给的是**实测**的逐 topic MB 数，
以它为准。判断方法很直接：如果上一轮的 `synergia-*` 数据还在盘上，那么它的合计值就是一轮三月
重放的真实占用；清理之后至少要留出同样多的空间，再加上大约 20% 的余量。

由于 RF=2、三个 broker，每个 broker 承担全部数据的约三分之二，所以三台节点的可用空间要**分别**
满足这个量，不能只看总和。目前 master 的 4.6 GB 大概率是三台里最紧的一台。

**不要用 `retention.bytes` 来解决这一轮的问题。** `synergia-m1-out` 与 `synergia-monitoring`
在重放结束后要被 `syn-replay-verify.sh` 与 `syn-m2-baseline.sh` 从头完整读取；一旦按字节数截断，
早期数据会在运行过程中被删掉，四条完整性断言与逐设备基线都会失真，而且失真的方式是**静默**的。
`retention.bytes` 只适合用于将来那些不需要事后完整回读的 topic。

---

## 6. 建议的执行顺序

1. 先跑 `bash deploy/scripts/syn-disk-report.sh`，把三台节点的报告贴出来。
2. 执行第一级清理 (a)(b)(d)，以及在保存好 framesize 现场之后的 (c)。
3. 执行第二级清理 `syn-clean-topics.sh`，等待一到两分钟。
4. 复核 `bash deploy/scripts/syn-disk-report.sh`，确认三台节点的可用空间都达到第 5 节推算出的
   目标值。
5. 达标后再回到 `docs/m2_checkpoint_instrumented_rerun_zh.md` 的阶段零第 2.5 步继续。
   **不要在空间不达标的情况下启动重放**——中途因写满而失败的运行，其数据同样不可用，只是白白
   再占一次盘。
