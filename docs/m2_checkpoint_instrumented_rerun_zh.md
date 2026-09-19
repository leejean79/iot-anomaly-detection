# M2 三月基线：带测量的重跑操作手册

- **版本**：1.0
- **日期**：2026-09-19
- **对应提交**：`2fad89d`（`feat(m1,m2): add --checkpoint-tolerable-failures for instrumented diagnosis`）
- **定位**：本文是 `docs/m2_march_baseline_runbook_zh.md`（三月基线操作手册）的**增补**，不是它的替代。
  阶段一到阶段六的主体流程仍以那份手册为准；本文只写明**集群冷启动后的开机顺序**、**本次重跑
  与原手册不同的两个提交参数**、**测量脚本的用法**，以及**测量结果的判读方式**。

---

## 1. 这次重跑要解决的问题

上一次三月重放在第 79 次 checkpoint 上失败，JobManager 日志给出的原因是一次大小为 27,329,480 字节
（约 26.1 MB）的 RPC 调用超过了 akka 的单条消息上限（`akka.framesize`，默认 10 MB）。由于 Flink
的「容忍 checkpoint 失败次数」默认值是 0，第一次失败即判定作业失败并触发重启；重启又使
AT_LEAST_ONCE 语义的 Kafka Sink 重新发送已处理的数据，最终在 `synergia-m1-out` 上留下 25,517 条
重复轮，使整轮基线作废。

在此之前，我已经对三个候选算子逐一做了本地实测，并且**全部排除**：

| 候选 | 实测结果 | 结论 |
| --- | --- | --- |
| `RobustScaler` 预热蓄水池（`List<ListState<Double>>`，60,480 轮 × 5 通道） | 序列化后 2,419,220 字节（2.31 MB） | 与 26.1 MB 相差一个数量级，排除 |
| `PmcodFunction` 的增量窗口状态 | 冻结后全并行度合计 8.89 MB，预热期间为 0 | 单子任务远低于上限，排除 |
| `RoundAssembler` 的未闭轮缓冲（`PartialRound`） | 单轮含键实测 127.0 字节；凑满 26.06 MB 需要 215,164 个同时打开的轮次，折合单设备 597.7 小时事件时间，而正常情况下每设备只有 3 个打开的轮次 | 量级上不可能，排除 |

因此 **26.1 MB 的来源目前是未知的**。我不再从这个数字反推机制——前面三次这样做的结论都被后续实测
推翻了。本次重跑的目的就是把它**当场测出来**，而不是继续推断。

本次运行同时承担两项任务，二者可以在同一次运行中达成：

1. **测量**：完整记录每一次 checkpoint 的**逐算子**与**逐子任务峰值**状态大小，从增长曲线上定位
   哪个算子的单个子任务逼近 26 MB。
2. **取基线**：因为作业不再在第一次越限时重启，AT_LEAST_ONCE 重发就不会发生，这一次的输出**有
   可能**直接是一份干净的三月基线，从而把原手册阶段三到阶段六一并走完。

### 必须知情的代价

抬高容忍次数只是让作业**不因 checkpoint 失败而重启**，它**并不让 checkpoint 成功**。这意味着在
本次运行期间，作业实际上没有可用的恢复点：如果 TaskManager 失联或作业因其他原因失败，将没有
checkpoint 可供恢复，只能从阶段一重新清场重来。对于一次有界的、约一小时的回放实验，这个代价是可以
接受的；但它必须是知情的选择，而不是被忽略的副作用。

此外，`--checkpoint-tolerable-failures` 是**诊断用参数，默认值 0 即保持原有行为**。正式运行与交付
运行都不应带上它。

---

## 2. 阶段零：集群关机后的开机顺序

> **磁盘前置条件（2026-09-19 新增）**：开机后实测三台节点可用空间为 master 4.6 GB、
> worker-1 23 GB、worker-2 9.7 GB，不足以支撑一轮完整的三月重放。必须先按
> `docs/cluster_disk_cleanup_zh.md` 完成盘点与清理、并复核三台节点均达标，才能进入下面的
> 2.6 与第 3 节。在空间不达标的情况下启动重放，中途写满导致的失败同样会让数据作废。

集群已于前一晚整体关机，因此必须先完成下面五步，才能进入原手册的阶段一。每一步都给出了判定
是否成功的依据，不要在上一步未确认的情况下继续。

### 2.1 在云厂商控制台启动三台实例

在阿里云控制台把 master、worker-1、worker-2 三台实例开机，等待它们进入「运行中」状态。本步骤没有
脚本，必须在控制台完成。

### 2.2 刷新公网 IP 并同步到 `.env` 与 SSH 配置

实例重启后公网 IP 通常会变化，`deploy/.env` 与 `~/.ssh/config` 里的旧 IP 会让后续所有脚本失败。

```bash
# 执行环境：本地 Mac，仓库根目录；需要已安装并配置好 aliyun CLI 与 ~/.fa-iforest-aliyun.conf
bash deploy/scripts/refresh-ips.sh --dry-run    # 先只看要改什么
bash deploy/scripts/refresh-ips.sh              # 确认无误后真正写入 .env 与 ssh config
```

**期望产出**：脚本打印三台实例的新公网 IP，并提示 `.env` 与 `~/.ssh/config` 已更新。

**失败兜底**：如果没有配置 aliyun CLI，就在控制台抄下三个公网 IP，手工编辑 `deploy/.env` 中的
`NODE_MASTER_PUBLIC_IP`、`NODE_WORKER1_PUBLIC_IP`、`NODE_WORKER2_PUBLIC_IP` 三项。注意内网 IP
（`NODE_*_IP`）在重启后一般不变，不要一并改掉。

### 2.3 验证 SSH 连通

```bash
# 执行环境：本地 Mac，仓库根目录
set -a; source deploy/.env; set +a
for h in "$NODE_MASTER_PUBLIC_IP" "$NODE_WORKER1_PUBLIC_IP" "$NODE_WORKER2_PUBLIC_IP"; do
    ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$SSH_USER@$h" \
        "hostname && docker ps --format '{{.Names}}' | tr '\n' ' ' && echo" || echo "  连接失败: $h"
done
```

**期望产出**：三台机器各打印主机名与当前容器列表。刚开机时容器列表可能为空，这是正常的；
如果 Docker 守护进程配置了开机自启，也可能已经有容器在跑。

### 2.4 拉起各节点容器

```bash
# 执行环境：本地 Mac，仓库根目录；前置条件：2.2 的 IP 已刷新、2.3 的 SSH 可达
bash deploy/scripts/2-up-all.sh
```

**期望产出**：master 上出现 `zookeeper`、`kafka-1`、`jobmanager`；两台 worker 上各出现
`kafka-2`/`kafka-3` 与 `taskmanager-2`/`taskmanager-3`。

**失败兜底**：若某个 TaskManager 反复重启，先看它的日志
（`ssh fa-master "docker logs taskmanager-2 --tail 80"`）。开机后第一时间常见的是 Kafka 尚未就绪
导致的短暂重试，等 30 秒后再看一次；如果是内存参数相关报错，对照
`docs/flink_memory_tuning_zh.md` 排查。

### 2.5 确认镜像与 JavaCPP 配置仍是 Java 11 那一版

关机重启不会改变镜像，但它会暴露「配置只改在本地、没有同步到节点」这类历史遗留问题，所以开机后
统一核对一次，成本极低。

```bash
# 执行环境：本地 Mac，仓库根目录
set -a; source deploy/.env; set +a
ssh -i "$SSH_KEY" "$SSH_USER@$NODE_WORKER1_PUBLIC_IP" \
    "docker inspect taskmanager-2 --format '{{.Config.Image}}' && \
     docker inspect taskmanager-2 | grep -i maxphysicalbytes"
```

**期望产出**：镜像为 `fa-iforest/flink:1.13.6-java11`；`maxphysicalbytes` 为 `3584m`，与
`deploy/.env` 中的 `SYN_JAVACPP_MAXPHYSICALBYTES` 一致。

**失败兜底**：若两者不一致，说明节点上的 `.env` 或 compose 文件是旧的，执行
`bash deploy/scripts/syn-sync-flink-image.sh --config-only` 只同步配置（跳过 642 MB 的镜像包），
然后重新执行 2.4。

### 2.6 集群验收（可选但建议）

```bash
# 执行环境：本地 Mac，仓库根目录；前置条件：synergia-* topic 已存在（若已被清理，本步可推迟到阶段一之后）
bash deploy/scripts/syn-verify-cluster.sh
```

**期望产出**：控制台核对表与 `docs/env_acceptance.md`。这一步是只读的，不会取消任何作业；若发现
旧项目 FA-iForest 的作业处于 RUNNING，**只记录、绝不取消**。

---

## 3. 阶段一到阶段二：与原手册的差异

阶段一（构建、上传、清场）**完全沿用** `docs/m2_march_baseline_runbook_zh.md`，只有一处补充：
本次必须使用包含提交 `2fad89d` 的 jar，可用 jar 标记自检来确认。

```bash
# 执行环境：本地 Mac，仓库根目录；前置条件：JAVA_HOME 指向 JDK 11
mvn -DskipTests package
bash deploy/scripts/check-jar.sh
```

**期望产出**：全部标记 PASS，其中必须包含新增的三条——`AlignEventTime`（事件时间对齐修复，
M1Job 与 M2Job 各一条）与 `checkpoint-tolerable-failures`（本次诊断参数）。若这三条中任意一条
FAIL，说明打包的是旧代码，不要上传。

上传仍用 `bash deploy/scripts/syn-upload-m1.sh --jar-only`。

阶段二（提交作业）与原手册的**唯一差异是提交参数**：

```bash
# 执行环境：本地 Mac，仓库根目录
# 前置条件：synergia-source 已建为 8 分区、topic 已清空、无任何 M1Job/M2Job 在跑
bash deploy/scripts/syn-submit-m2.sh --extra \
    '--m3-enabled false --window-sec 3600 --checkpoint-tolerable-failures 100 --checkpoint-ms 30000' \
    2>&1 | tee /tmp/submit.log
grep -E 'W=|Window W/S|Ckpt tolerable failures' /tmp/submit.log
```

提交后必须在输出中同时看到下面三行，缺一不可：

- `W=3600s`
- `Window W/S:      3600s / 60s`
- `Ckpt tolerable failures: 100  [DIAGNOSTIC MODE: job will not restart on failed checkpoints]`

两个参数的取值理由如下。`--checkpoint-tolerable-failures 100` 取 100 而不是更大的数，是因为在
加速 3600 倍、约 744 秒的重放里，30 秒一次的 checkpoint 最多也只有约 25 次，100 足以覆盖全程连续
失败的最坏情况，同时仍保留一个上界，避免作业在异常状态下无限期挂着。`--checkpoint-ms 30000` 把
间隔从默认的 10 秒放宽到 30 秒，是在**采样分辨率**与**失败开销**之间取平衡：30 秒一次在整段重放
里能取到约 25 个采样点，足以看出增长曲线的形状，同时把失败 checkpoint 的重试开销降到十秒级方案的
三分之一。

---

## 4. 测量：`syn-ckpt-watch.sh`

**测量脚本必须在启动重放之前就开始运行**，否则重放前期的 checkpoint 曲线会缺失，而恰恰是「状态
从小涨到大」的这一段最能说明问题。建议在一个**独立的终端窗口**里运行它，让它全程常驻。

```bash
# 1. 执行环境：本地 Mac（bash + python3），仓库根目录，独立终端窗口；ssh 免密到 fa-master
# 2. 调用命令：
bash deploy/scripts/syn-ckpt-watch.sh --interval 15 --out docs/reports/ckpt_sizes_march.csv
# 3. 前置条件：M2Job 已处于 RUNNING；提交时已带 --checkpoint-tolerable-failures 100
# 4. 期望产出：每出现一次新的 checkpoint，控制台打印一张逐算子表（按单子任务峰值降序），
#    同时在 docs/reports/ckpt_sizes_march.csv 追加每行一条 (checkpoint, 算子) 记录
# 5. 常见失败兜底：读不到 /jobs → 检查 jobmanager 容器与 8081 端口；
#    history 为空 → 作业刚提交、还没触发第一次 checkpoint，等一个 --checkpoint-ms 周期；
#    退出码 3 → 作业已不可查询（多半是重启或结束），CSV 保留已采集部分
```

脚本按**单子任务峰值**而非合计大小降序排列，因为 `akka.framesize` 限制的是**单条确认 RPC**，
合计大小并不是判据；任何单子任务峰值超过阈值 80%（默认 8 MB）的行会被标注
`<== 逼近 framesize`。

确认脚本已经开始出数之后，再按原手册启动重放：

```bash
bash deploy/scripts/syn-replay.sh --speedup 3600 --start 2022-03-01 --end 2022-04-01
```

`--speedup 3600` 的节流**不可省略**，原因见原手册 2.4 的说明与 `docs/flink_memory_tuning_zh.md`
第 3.6、3.7 节。

---

## 5. 判读：三种可能的测量结果

重放结束后，先停止测量脚本（Ctrl+C），然后按下面三种情形之一判读 CSV。

**情形甲：某个算子的单子任务峰值随时间单调增长，最终逼近或超过 26 MB。**
这就是我们要找的来源，问题定位结束。把该算子名、增长曲线（checkpoint 序号对峰值 MB）与最后一次
成功 checkpoint 的数值填入 `docs/reports/m2_checkpoint_framesize_decision.md` 的修订三，交设计
会话在方案甲（抬高共享 JobManager 的 `akka.framesize`）、方案乙（基线运行关闭 checkpoint）、
方案丙（降低重放加速倍率）之间裁决。

**情形乙：所有算子的单子任务峰值都显著小于 10 MB，但仍然出现 checkpoint 失败。**
这说明越限的那条 RPC 承载的不是单个算子的键控状态，而是别的东西——例如某个算子的 union list
状态，或者确认消息之外的其他 RPC。此时不要在数据不足的情况下猜测，应当在 JobManager 日志里定位
报出 `exceeds the maximum akka framesize` 的那一行的完整上下文（`ssh fa-master "docker logs
jobmanager 2>&1 | grep -B 20 framesize"`），把原始日志片段附在报告里再交裁决。

**情形丙：全程没有任何 checkpoint 失败，作业顺利跑完。**
这是完全可能的，因为上一次失败发生在**事件时间对齐修复之前**的运行里；对齐修复改变了下游算子
实际承载的状态形态，26 MB 的来源可能已随之消失。此时 CSV 仍然是有价值的证据（它证明修复后的
峰值有多大、离上限还有多远），并且本次运行的输出就是一份可用的干净基线，直接进入第 6 节。

---

## 6. 重放之后：核验与基线

本节与原手册完全一致，按顺序执行，前一条不通过就不要读后一条的结果：

```bash
# 6.1 等作业排空：连续两次读数一致即为排空
bash deploy/scripts/syn-m2-metrics.sh
bash deploy/scripts/syn-m2-metrics.sh

# 6.2 完整性门槛（四条断言必须全 PASS）
bash deploy/scripts/syn-replay-verify.sh --tol-pct 0.2

# 6.3 逐设备基线对照（必须在 syn-clean-topics.sh 之前执行）
bash deploy/scripts/syn-m2-baseline.sh --tag march
```

其中 6.2 的第二条断言「零重复 device+ts」正是本次改动要保住的那一条：上一轮因为作业重启产生了
25,517 条重复轮，这一条断言失败。如果这次它 PASS，说明抬高容忍次数确实阻断了「checkpoint 失败
→ 重启 → 重发」这条污染链路。

最后请把本次运行的三项证据留档：`docs/reports/ckpt_sizes_march.csv`（测量曲线）、
`syn-replay-verify.sh` 的四条断言输出、`syn-m2-baseline.sh` 的逐设备对照表。这三项合起来构成
`docs/reports/m2_java11_march_baseline.md` 所需的全部素材。
