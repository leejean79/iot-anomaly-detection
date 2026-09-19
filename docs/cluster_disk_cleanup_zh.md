# 集群磁盘清理：现状判断、安全边界与分级清理

- **版本**：2.0（2026-09-19 实测修订，推翻 1.0 的核心判断）
- **配套脚本**：`deploy/scripts/syn-disk-report.sh`（只读盘点，不删除任何内容）

## 修订记录

**1.0 的核心判断被实测推翻。** 1.0 声称「Kafka 容器没有挂载数据卷，topic 数据落在容器可写层里，
因此绝不可删除容器」。第一次盘点的第 3 小节直接否掉了这一条：三台节点上**全部容器的可写层合计
不到 100 KB**（master 上最大的一个是 57.3 kB）。真正的占用在 Docker 的**匿名数据卷**里。

机制是这样的：`wurstmeister/kafka` 与 `wurstmeister/zookeeper` 的镜像在 Dockerfile 里声明了
`VOLUME`，因此每**新建**一个容器，Docker 就为它生成一个**匿名卷**来存放 `kafka-logs`；而容器被
删除时，这个匿名卷**不会**随之消失。历次实验反复创建、删除 broker 容器，于是在盘上留下了一堆
互不相干的孤儿卷。master 上共有 182 个卷、worker-1 有 60 个、worker-2 有 61 个，而当前**在用的
只有 2 个**。

这个修正把结论整个反了过来：删除容器不会丢数据（数据在卷里），真正不可撤销的动作是
`docker volume prune`。第 2 节的安全边界已据此重写。

---

## 1. 两个比磁盘更要紧的发现

**第一，集群的三个核心容器根本不存在。** 盘点显示 master 上只有三个容器——一个三个月前退出的
旧项目 `dumper-insects_*`、以及 `prometheus` 与 `grafana`；**没有 `zookeeper`、没有 `kafka-1`、
没有 `jobmanager`**。两台 worker 上**一个容器都没有**（`Containers 0`）。也就是说集群目前处于
「已拆除」状态，不是「已关机」状态。

由此推出一个直接影响决策的结论：**历史上所有 Kafka topic 数据都已经是孤儿**。重新执行
`2-up-all.sh` 时，Docker 会为新建的 broker 容器分配**全新的空匿名卷**，不会挂回任何旧卷。所以
那 180 多个卷不会被任何东西重新用上，它们是纯粹的死重；同时也意味着上一轮三月重放的
`synergia-*` 数据已经不存在了，`syn-clean-topics.sh` 这一步变得没有意义，取而代之的是必须
重新执行 `syn-create-topics.sh` 建 topic。

**第二，`docker system df` 的 Local Volumes 一行不可信。** 它在 worker-1 上报出 51.73 GB，而这块
盘总共才用掉 15 GB；在 master 上报 70.73 GB，而整盘只用了 31 GB。因此卷的真实占用必须用 `du`
量，这正是脚本新增的 5b 小节所做的事。在看到 5b 的实测数字之前，不要根据 `docker system df`
估算能腾出多少空间。

---

## 2. 安全边界（2.0 修订）

1. **`docker volume prune` 是本次唯一不可撤销的动作。** 它会删除所有无容器引用的卷，也就是旧项目
   FA-iForest 历次运行留下的全部 Kafka 数据。你已经说明旧项目不再运行，但「不再运行」与「数据
   不再需要」是两件事，请在执行前明确确认后者。
2. **绝不可执行 `docker system prune -a`。** 它会连同未被运行中容器引用的**镜像**一起删掉，其中
   包括 `fa-iforest/flink:1.13.6-java11`——当前没有 Flink 容器在跑，所以这个镜像正处于「未被引用」
   状态，一旦删掉就要重新分发 1.34 GB 的镜像。
3. **绝不可删除 `$SYN_DATASET_DIR`**（`/opt/fa-iforest/datasets`，2350 MB）。重放器直接读它，
   删了要重新上传。
4. **删除非 `synergia-` 前缀的 topic 仍然不做**——虽然此刻已无 topic 存在，这条边界在集群重建
   之后继续有效。

---

## 3. master 节点的实测账（40 GB 盘，已用 31 GB）

| 项目 | 实测 | 处置 |
| --- | --- | --- |
| Docker 卷（182 个，在用 2 个） | 待 5b 实测；按总账倒推约 21 GB | 第一大头，见 4.1 |
| Docker 镜像合计 | 4.58 GB，其中悬空 2 个 | 悬空的可回收，见 4.2 |
| `/opt/fa-iforest/datasets` | 2350 MB | **保留**，重放器要用 |
| `/opt/fa-iforest/m2probe` | 1105 MB | 可删，脚本下次运行会重建 |
| `/opt/fa-iforest/fa-iforest-flink.tar` | 632 MB | 可删，`docker load` 之后无用 |
| `/opt/fa-iforest/jars` | 339 MB（旧项目 125 MB + 本项目 214 MB） | **两个都保留** |
| `/opt/fa-iforest/mon_full.jsonl` | 300 MB | 删前确认，见 4.3 |
| `/opt/fa-iforest/m2baseline` | 265 MB | 可删，脚本会重建 |
| `/opt/fa-iforest/m2surge` | 71 MB | 可删，脚本会重建 |
| `/var/log` 与 journal | 378 MB / 360 MB | 可收缩 |
| 容器 json 日志 | 4 MB | 不值得动 |

已知项合计约 10 GB，而全盘已用 31 GB，**差额约 21 GB 只能落在 Docker 卷上**。这是倒推，不是实测；
以 5b 小节的 `du` 数字为准。

worker-1 已用 15 GB、worker-2 已用 26 GB，两台的 `/opt/fa-iforest` 都只有 632 MB 的镜像 tar，
镜像各 2.98 GB，因此它们的差额（约 11 GB 与 22 GB）同样落在卷上。

---

## 4. 分级清理

### 4.1 第一级：回收孤儿卷（最大的一块，需你确认）

先看清楚再动手：

```bash
# 执行环境：本地 Mac，仓库根目录
bash deploy/scripts/syn-disk-report.sh --top 30   # 看新增的 5b 小节实测值
```

确认「无容器引用的卷合计」这个数字，并确认旧项目的 Kafka 数据不再需要之后：

```bash
# 执行环境：三台节点，逐台执行；不可撤销
set -a; source deploy/.env; set +a
for h in "$NODE_MASTER_PUBLIC_IP" "$NODE_WORKER1_PUBLIC_IP" "$NODE_WORKER2_PUBLIC_IP"; do
    echo "--- $h"
    ssh -i "$SSH_KEY" "$SSH_USER@$h" "docker volume prune -f"
done
```

预计回收：master 约 21 GB、worker-1 约 11 GB、worker-2 约 22 GB（倒推值，以 5b 实测为准）。
风险：**不可撤销**，旧项目历次运行的 Kafka 数据随之消失。由于这些卷不会被任何新容器挂回，
保留它们唯一的价值是「将来手工把某个卷挂到临时容器上翻查旧数据」。

**更保守的做法**：如果你想先留一手，可以只删除**修改时间最早**的那一批。5b 小节按大小排序列出了
最大的若干个卷，`ls -ld --time-style=long-iso /var/lib/docker/volumes/*/` 可以看到各自的时间，
据此逐个 `docker volume rm <卷名>`。代价是要手工判断，收益是可以分批回收。

### 4.2 第二级：完全安全，不影响任何数据（注意执行顺序）

**顺序不可颠倒：先清悬空镜像，核对 Flink 镜像仍在，最后才删镜像 tar 包。** 原因是盘点输出里有
一处矛盾：`8852cf871082` 既出现在有标签的 `fa-iforest/flink:1.13.6-java11` 那一行，又出现在悬空
镜像列表里。按定义悬空镜像是无标签的，二者不该同时成立，我无法从这份输出断定究竟哪一边是准的。
`docker image prune`（不带 `-a`）在设计上不会删除带标签的镜像，所以正常情况下它是安全的；但既然
判据本身存在矛盾，就不要把「万一被删掉之后还能从 tar 包恢复」这条退路提前砍掉。

```bash
set -a; source deploy/.env; set +a
for h in "$NODE_MASTER_PUBLIC_IP" "$NODE_WORKER1_PUBLIC_IP" "$NODE_WORKER2_PUBLIC_IP"; do
    echo "--- $h"
    ssh -i "$SSH_KEY" "$SSH_USER@$h" "
        docker image prune -f
        journalctl --vacuum-size=200M
        echo '--- prune 之后仍存在的 flink 镜像 ---'
        docker images | grep -i flink || echo '  [警告] flink 镜像已不存在！'
    "
done
```

只有当上面每一台都打印出 `fa-iforest/flink:1.13.6-java11` 之后，才执行删除 tar 包这一步：

```bash
for h in "$NODE_MASTER_PUBLIC_IP" "$NODE_WORKER1_PUBLIC_IP" "$NODE_WORKER2_PUBLIC_IP"; do
    ssh -i "$SSH_KEY" "$SSH_USER@$h" "rm -f ${REMOTE_HOME}/fa-iforest-flink.tar && echo '  tar 已删除'"
done
```

万一某台的 flink 镜像真的消失了，立即用该节点上**尚未删除**的 tar 包恢复：
`ssh ... "docker load -i ${REMOTE_HOME}/fa-iforest-flink.tar"`。

预计回收：每节点 tar 包 632 MB、悬空镜像最多约 2.5 GB、journal 约 150 到 200 MB。
**注意 `docker image prune` 一律不加 `-a`**：当前三台节点都没有 Flink 容器在运行，加了 `-a` 会
把 `fa-iforest/flink:1.13.6-java11` 当作「未被引用」直接删掉。

### 4.3 第三级：本项目自己的中间产物

`m2probe`（1105 MB）、`m2baseline`（265 MB）、`m2surge`（71 MB）都是脚本的远端工作目录
（见 `syn-m2-probe.sh:74`、`syn-m2-baseline.sh:57`、`syn-m2-surge.sh:49`），下次运行会重新从
Kafka 转储填充，可以直接删。

`mon_full.jsonl`（300 MB）不在任何脚本的工作目录之列，看名字是一份完整的 monitoring 转储。
**删之前请确认**它不是 Java 8 参考基线 `docs/m2_probe_7d_clean.csv` 的唯一原始数据来源——现在
Kafka 数据已全部消失，如果它是唯一副本，删掉就再也无法复算那份参考。如果不确定，先拉到本地
再删；300 MB 的下载比不可恢复的丢失便宜得多。

```bash
# 若决定保留一份本地副本：
scp -i "$SSH_KEY" "$SSH_USER@$NODE_MASTER_PUBLIC_IP:${REMOTE_HOME}/mon_full.jsonl" /tmp/
# 确认之后再删：
ssh -i "$SSH_KEY" "$SSH_USER@$NODE_MASTER_PUBLIC_IP" \
    "rm -rf ${REMOTE_HOME}/m2probe ${REMOTE_HOME}/m2baseline ${REMOTE_HOME}/m2surge"
```

---

## 5. 清理之后：集群需要重建，不只是重启

因为三个核心容器都不存在、Kafka 也将以空卷启动，清理完成后的顺序与原计划不同：

1. `bash deploy/scripts/2-up-all.sh` —— 这一步现在是**创建**容器而不是启动容器，请把完整输出
   保留下来。此前它显然没有把 `zookeeper`、`kafka-1`、`jobmanager` 创建出来（而 prometheus 与
   grafana 起来了），需要从输出里看清是哪一步失败的。
2. `bash deploy/scripts/syn-create-topics.sh` —— topic 全部需要重建（8 分区、RF=2、
   `retention.ms=-1`）。
3. `bash deploy/scripts/syn-upload-m1.sh --jar-only` —— jar 仍在
   `/opt/fa-iforest/jars`，但仍建议重传一次，确保它是含提交 `2fad89d` 的那一版；
   随后用 `check-jar.sh` 核对标记。
4. 回到 `docs/m2_checkpoint_instrumented_rerun_zh.md` 的阶段零第 2.5 步继续。

关于本轮需要多少空间：由于历史数据已全部消失，现在没有任何实测基准可以参照，`env.example` 里
「source≈5 GB、m1-out≈5–10 GB」的估计也无从校验。所以正确的做法不是先估算，而是在清理后让三台
节点都留出**远大于估计值**的余量（每台 20 GB 以上），并在重放过程中用
`bash deploy/scripts/syn-disk-report.sh --node master` 间隔复核。第一轮跑完之后，5b 与第 5 小节
的实测值就是今后一切估算的基准。
