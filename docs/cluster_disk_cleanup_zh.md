# 集群磁盘清理：现状判断、安全边界与分级清理

- **版本**：2.0（2026-09-19 实测修订，推翻 1.0 的核心判断）
- **配套脚本**：`deploy/scripts/syn-disk-report.sh`（只读盘点，不删除任何内容）
- **清理结果（2026-09-19 完成）**：可用空间 master 6.4 GB → **24 GB**，worker-1 23 GB → **30 GB**，
  worker-2 12 GB → **30 GB**。三台均已越过 20 GB 门槛，磁盘不再是重跑的阻塞项；第 4 节的第二、
  第三级清理因此成为可选的余量手段，不必执行。

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

1. ~~**`docker volume prune` 是本次唯一不可撤销的动作。**~~ **（2026-09-19 已执行完毕）** 用户
   确认旧项目 FA-iForest 的 Kafka 数据早在本项目环境搭建过程中就已被误删，因此这些孤儿卷里
   并无仍需保留的内容。集群上仍属于旧项目的只剩
   `/opt/fa-iforest/jars/FA-iForest-1.0-SNAPSHOT.jar`（125 MB），**必须保留**。
2. **绝不可执行 `docker system prune -a`。** 它会连同未被运行中容器引用的**镜像**一起删掉，其中
   包括 `fa-iforest/flink:1.13.6-java11`——当前没有 Flink 容器在跑，所以这个镜像正处于「未被引用」
   状态，一旦删掉就要重新分发 1.34 GB 的镜像。
3. **绝不可删除 `$SYN_DATASET_DIR`**（`/opt/fa-iforest/datasets`，2350 MB）。重放器直接读它，
   删了要重新上传。
4. **删除非 `synergia-` 前缀的 topic 仍然不做**——虽然此刻已无 topic 存在，这条边界在集群重建
   之后继续有效。

---

## 3. 三台节点的实测账（2026-09-19 第二次盘点）

卷的实测值已经拿到，三台节点的账目如下。

| 节点 | 盘容量 / 已用 / 可用 | 卷实测合计 | 其中无引用（可回收） | 卷占已用空间比 |
| --- | --- | --- | --- | --- |
| fa-master | 40 GB / 31 GB / 6.4 GB | 17,652 MB | **17,579 MB**（180 个 / 共 182 个） | 约 57% |
| fa-worker1 | 40 GB / 15 GB / 23 GB | 7,330 MB | **7,367 MB**（60 个 / 共 60 个） | 约 49% |
| fa-worker2 | 40 GB / 26 GB / 12 GB | 18,636 MB | **18,668 MB**（61 个 / 共 61 个） | 约 72% |

（可回收值略高于卷合计，是 `du` 对目录本身按块计量造成的取整差，不影响判断。）

第 2 节倒推的「master 约 21 GB」偏高了约 3.5 GB，worker-1 的「约 11 GB」偏高约 3.7 GB，
worker-2 的「约 22 GB」偏高约 3.4 GB。三处偏差量级一致，说明倒推法系统性地把镜像层与
`/var/lib/docker` 其他内容算进了卷里。**今后一律以 5b 小节的实测值为准。**

master 上仍在使用的卷只有 2 个，其中 `compose_prom-data` 占 212 MB，另一个是 Grafana 的数据卷；
它们被运行中的 prometheus 与 grafana 容器引用，`docker volume prune` 不会碰它们，Prometheus 的
历史指标因此得以保留。

master 与 worker-2 上各有一个约 11.5 GB 的单体大卷（`94202209b1dd…` 与 `3eb86097330a…`），
两者合计就占了全部可回收量的一半。worker-1 上最大的一个只有 2,111 MB。

master 的 `/opt/fa-iforest` 明细（第二大块，合计约 5.0 GB）：

| 项目 | 实测 | 处置 |
| --- | --- | --- |
| `datasets` | 2350 MB | **保留**，重放器要用 |
| `m2probe` | 1105 MB | 可删，脚本下次运行会重建 |
| `fa-iforest-flink.tar` | 632 MB | 可删，但须在镜像核对之后，见 4.2 |
| `jars` | 339 MB（旧项目 125 MB + 本项目 214 MB） | **两个都保留** |
| `mon_full.jsonl` | 300 MB | 删前确认，见 4.3 |
| `m2baseline` | 265 MB | 可删，脚本会重建 |
| `m2surge` | 71 MB | 可删，脚本会重建 |

---

## 4. 分级清理

### 4.1 第一级：回收孤儿卷（最大的一块，需你确认）

**执行之前，先花一分钟看清最大的几个卷装的是什么。** 这一步是只读的，代价极低，而它把一个
不可撤销的删除从「不知道删了什么」变成「知道删了什么」：

```bash
# 执行环境：本地 Mac，任意目录（走 ~/.ssh/config 的主机别名，无需 source .env）
# 不要手工抄卷 ID——下面的命令自己找出最大的那个卷再列目录。
for h in fa-master fa-worker2; do
    echo "===== $h"
    ssh "$h" 'TOP=$(du -sm /var/lib/docker/volumes/*/_data 2>/dev/null | sort -rn | head -1 | awk "{print \$2}"); echo "最大卷: $TOP"; ls "$TOP" | head -20'
done

# 若想看前三大的卷各占多少（含完整的 64 位卷 ID）：
ssh fa-master 'du -sm /var/lib/docker/volumes/*/_data 2>/dev/null | sort -rn | head -3'
```

如果列出的是 `kafka-logs-*` 或一堆 `<topic>-<分区号>` 目录，那就确认了这些是历次 broker 容器
遗留的数据；如果是别的东西，请把输出贴出来再决定。

**关于卷 ID 的长度**：Docker 匿名卷的 ID 是 **64 位**十六进制字符串。`syn-disk-report.sh`
的 5b 小节最初把它截断到 40 位显示，照抄那个截断值去 `ls` 会得到
`No such file or directory`——这是显示缺陷，不是卷不存在。脚本已改为完整打印，上面的命令则干脆不依赖人工抄写。

确认无误、且确认旧项目的这些数据不再需要之后：

```bash
for h in fa-master fa-worker1 fa-worker2; do
    echo "--- $h"
    ssh "$h" "docker volume prune -f"
done
```

实测可回收：master 17,579 MB、worker-1 7,367 MB、worker-2 18,668 MB。
回收之后的可用空间约为 master 23.5 GB、worker-1 30 GB、worker-2 30 GB——**仅这一步就足以
让三台节点都越过 20 GB 的门槛**。

**不要加 `-a`。** 当前这 301 个待回收的卷全部是匿名卷（十六进制名），默认的
`docker volume prune` 就能清掉；加 `-a` 会把带名字的卷也纳入范围，而 `compose_prom-data`
与 Grafana 数据卷正是带名字的卷——虽然它们被运行中的容器引用因而不会被删，但没有必要去冒
这个风险。

风险：**不可撤销**。这些卷不会被任何新容器挂回（新建 broker 容器会拿到全新的空卷），所以保留
它们唯一的价值是「将来手工把某个卷挂到临时容器上翻查旧数据」。

**更保守的做法**：只删小的、留下两个 11.5 GB 的大卷。这样 master 仍可回收 6,035 MB、
worker-2 仍可回收 7,030 MB。再加上第二级的 tar 包与 journal、第三级的中间产物（master 多出
1,441 MB），master 约有 14.6 GB、worker-2 约有 19.7 GB 可用。这两个数字**没有把悬空镜像的
约 2.5 GB 算进去**——4.2 节说明了那份数据自相矛盾，在核实之前不计入。两台都低于 20 GB 门槛，
但很可能仍够跑完一轮。要走这条路径就逐个 `docker volume rm <卷名>`，而不是 `prune`。

### 4.2 第二级：完全安全，不影响任何数据（注意执行顺序）

**顺序不可颠倒：先清悬空镜像，核对 Flink 镜像仍在，最后才删镜像 tar 包。** 原因是盘点输出里有
一处矛盾：`8852cf871082` 既出现在有标签的 `fa-iforest/flink:1.13.6-java11` 那一行，又出现在悬空
镜像列表里。按定义悬空镜像是无标签的，二者不该同时成立，我无法从这份输出断定究竟哪一边是准的。
`docker image prune`（不带 `-a`）在设计上不会删除带标签的镜像，所以正常情况下它是安全的；但既然
判据本身存在矛盾，就不要把「万一被删掉之后还能从 tar 包恢复」这条退路提前砍掉。

```bash
for h in fa-master fa-worker1 fa-worker2; do
    echo "--- $h"
    ssh "$h" "
        docker image prune -f
        journalctl --vacuum-size=200M
        echo '--- prune 之后仍存在的 flink 镜像 ---'
        docker images | grep -i flink || echo '  [警告] flink 镜像已不存在！'
    "
done
```

只有当上面每一台都打印出 `fa-iforest/flink:1.13.6-java11` 之后，才执行删除 tar 包这一步：

```bash
for h in fa-master fa-worker1 fa-worker2; do
    ssh "$h" "rm -f /opt/fa-iforest/fa-iforest-flink.tar && echo '  tar 已删除'"
done
```

万一某台的 flink 镜像真的消失了，立即用该节点上**尚未删除**的 tar 包恢复：
`ssh <该节点别名> "docker load -i /opt/fa-iforest/fa-iforest-flink.tar"`。

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
scp fa-master:/opt/fa-iforest/mon_full.jsonl /tmp/
# 确认之后再删：
ssh fa-master \
    "rm -rf /opt/fa-iforest/m2probe /opt/fa-iforest/m2baseline /opt/fa-iforest/m2surge"
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
