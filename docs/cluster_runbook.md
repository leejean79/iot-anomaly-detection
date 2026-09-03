# 集群实验操作手册 / Cluster Experiment Runbook

> 面向 **synergia（本项目 iot-anomaly-detection）** 在**复用的三节点旧集群**上的实验运行。
> 目标：让每一次"停止 → 次日重启 → 继续或重开"都有据可依，尤其把 **Kafka / Flink 里上一次
> 做完或出错遗留的残留数据**处理干净，避免对账被污染。
>
> 本手册与脚本一一对应（`deploy/scripts/`）。所有命令都在**本地 Mac** 执行，脚本内部通过
> ssh 免密操作 master/worker。凡涉及原始 kafka 命令的地方，`<brokers>` 指
> `NODE_MASTER_IP:9092,NODE_WORKER1_IP:9092,NODE_WORKER2_IP:9092`（见 `deploy/.env`）。

---

## 0. 先建立心智模型：会跨实验残留的四层状态

一次实验结束或中断后，下面四层状态**不会自动消失**。看懂它们，后面所有流程都能自己推导：

| 层 | 是什么 | 存在哪里 | 如何复位 |
|---|---|---|---|
| **L1 Kafka topic 数据** | `synergia-source / -m1-out / -monitoring / -scores` 里累积的消息 | broker 的持久卷（docker volume），**容器重启/集群重启后依然在** | `syn-clean-topics.sh --yes`（删+按 .env 参数重建） |
| **L2 Kafka 消费位移** | M1Job 消费者组已提交的 offset | broker 的 `__consumer_offsets` | **无需处理**——见下方要点 |
| **L3 Flink 作业与状态** | 正在跑的 M1Job 及其算子内存状态（RobustScaler 预热、RawCache、RoundAssembler 未闭轮） | Flink JobManager/TaskManager 内存 | `flink cancel <本项目JobID>`；重新提交即为**空状态**（当前未用 savepoint） |
| **L4 重放器本地状态** | `.replayer.offset` 续跑文件、残留 tmux 会话/容器、`syn-replay.log` | master 上的数据集目录与主目录 | `syn-replay.sh stop` + 按需删 `.replayer.offset` |

**L2 为什么无需处理（很重要）：** M1Job 每次提交都用**随机消费者组** `m1-job-<随机8位>` 且默认
`setStartFromEarliest()`（见 `M1Job.java:82,90`）。所以**每次提交都从 topic 最开头重读全部消息，
根本不记得上次消费到哪**。这带来一个必须记住的推论：

> **只要 topic 里还有上一次的旧消息，新提交的作业一定会把它们连同新数据一起重读。**
> 因此"开新实验前清 topic（L1）"是**强制步骤**，不是可选优化——不能指望消费位移帮你跳过旧数据。

---

## 1. 共存红线（每次操作前默念）/ Coexistence hard rules

同一套集群上**还并存着旧项目 FA-iForest**（其容器、镜像、topic、数据）。以下是不可逾越的红线：

1. **`flink list` 会同时列出旧 job 和本项目 M1Job。只按 JobID cancel 自己的 M1Job，绝不 cancel 旧 job。**
   本项目作业主类是 `com.leejean.m1.M1Job`；不确定时先在 Flink UI 里按名字/主类核对再动手。
2. **只碰 `synergia-` 前缀 topic。** `syn-clean-topics.sh` 内建前缀白名单护栏，结构上无法删除
   `source-topic / tree-topic` 等旧 topic；不要绕过它去手敲 `kafka-topics --delete` 删别的名字。
3. **绝不 `9-teardown.sh --purge`。** `--purge` 会带 `-v` 删除持久卷并 `rm -rf` 远端目录，
   **等于同时销毁旧项目 FA-iForest 的全部 Kafka/ZK 数据**。停集群只用不带参数的 `9-teardown.sh`。
4. **`compose down`（即 `9-teardown.sh` 不带 --purge）会同时停掉旧项目容器。** 因为旧项目实验结果
   已拉回本地，过夜停机可接受；但要清楚这一步影响的是**双方**，不是只停自己。

---

## 2. 我该跑哪套流程？（场景速查）

| 你的情形 | 去看 |
|---|---|
| 今天做完了，想收工，但集群不关 | §3.1（只停数据端）或 §3.2（连作业一起停） |
| 今天做完了，想连集群一起关（过夜省钱） | §3.3 |
| 次日开机，集群昨天**没关**（一直在跑） | 直接进 §5 选岔路 |
| 次日开机，集群昨天**关了** | §4（先把集群拉起来并自检）→ 再进 §5 |
| **开一次全新实验**（最常见、最安全） | §5.A 全清重来 |
| **续跑一次被中断的实验** | §5.B（有严格前提，先读完再决定） |
| 正式验收前，想确认起点是干净的 | §6 干净起点自检 |
| 出了怪现象想查是不是残留坑 | §7 常见残留陷阱 |

---

## 3. 停止实验 / Stopping

### 3.1 只停数据端（最常用，作业留着）

适合"今天先灌一段，明天接着灌，作业一直挂着"。

```bash
bash deploy/scripts/syn-replay.sh stop      # 停并删重放容器 + 杀 tmux 会话
bash deploy/scripts/syn-replay.sh status    # 确认已停（会话 not running、无残留容器）
```

M1Job 仍 RUNNING、继续待在集群上。**记下当前进度**（`syn-replay.log` 末尾的
`fileIdx/line` 或最后一次 offset 落盘），以便次日判断是"续跑"还是"重开"。

### 3.2 停实验但集群继续跑（连作业一起停）

```bash
bash deploy/scripts/syn-replay.sh stop
# 找到并只 cancel 本项目 M1Job（切勿 cancel 旧 job）：
ssh fa-master "docker exec jobmanager flink list"          # 看清哪个是 com.leejean.m1.M1Job
ssh fa-master "docker exec jobmanager flink cancel <M1_JOBID>"
```

Kafka / Flink / ZK 容器继续运行，topic 数据（L1）保留在卷里。

### 3.3 停整个集群过夜（省钱）

```bash
bash deploy/scripts/syn-replay.sh stop
ssh fa-master "docker exec jobmanager flink cancel <M1_JOBID>"   # 可选；停集群时作业本就会停
bash deploy/scripts/9-teardown.sh          # ← 绝不加 --purge！卷保留，数据不丢
```

`9-teardown.sh`（不带 `--purge`）只 `docker compose down`，**保留持久卷**，所以 topic、消息、
分区配置在次日重启后依然在。若同时用 ECS 控制台关机省费用也可以——卷在磁盘上，不受关机影响。

> **切记**：`--purge` = 毁数据（含旧项目），永远不要在共存集群上用。

---

## 4. 次日开启集群 / Next-day cluster start

**仅当昨天执行了 §3.3（集群被关）时才需要本节；否则跳到 §5。**

```bash
# 1) 公网 IP 可能在 ECS 重启后变化 → 刷新并写回 .env
bash deploy/scripts/refresh-ips.sh

# 2) 按 master(ZK先) → worker-1 → worker-2 顺序拉起
bash deploy/scripts/2-up-all.sh

# 3) 一键自检（应为 14/14 PASS）；顺带确认 broker 存活、topic 与分区数还在
bash deploy/scripts/syn-verify-cluster.sh

# 4) 手动再确认关键 topic 的分区数没被谁改坏（source 必须是 8）
ssh fa-master "docker exec kafka-1 kafka-topics.sh --bootstrap-server <brokers> --describe --topic synergia-source"
```

- 卷保留意味着**昨天的消息还在 topic 里**（L1 未清）。这正是下一步要在 §5 里决策的核心。
- 若 `refresh-ips.sh` 报公网 IP 变了，`.env` 会被更新；后续脚本自动用新值。
- 若某个 broker 没起来，先看 `syn-verify-cluster.sh` 的失败项，再单独 `docker ps` 排查，
  **不要**重建 jobmanager 容器（旧作业挂载已固化，DEV-D8）。

---

## 5. 残留数据处置：两条岔路（本手册的核心）

到这里集群已在跑。现在只有两种意图，**二选一**：

### 5.A 开一次全新实验 —— 全清重来（推荐默认）

只要你不是在"接着上一次没跑完的同一次实验"，就走这条。它保证起点绝对干净。

```bash
# (1) 只 cancel 自己的旧 M1Job（若还挂着）。绝不动旧项目 job。
ssh fa-master "docker exec jobmanager flink list"
ssh fa-master "docker exec jobmanager flink cancel <M1_JOBID>"    # 若列表里没有本项目 job 可跳过

# (2) 清掉重放器的本地残留（tmux 会话 / 容器）
bash deploy/scripts/syn-replay.sh stop

# (3) 删掉续跑 offset 文件，确保这次从头灌（也可以不删，只要 §5.A(6) 不传 --resume 即可）
ssh fa-master "rm -f /opt/fa-iforest/datasets/synergia/files_csv/.replayer.offset"

# (4) 清 topic：删+重建 synergia-*，数据清零，并顺带把 source 保证成 8 分区
bash deploy/scripts/syn-clean-topics.sh --yes

# (5) 先提交作业（内建分区数预检，会挡住"source 被误建成 1 分区"）
bash deploy/scripts/syn-submit-m1.sh

# (6) 再重放（不带 --resume！从头灌）
bash deploy/scripts/syn-replay.sh --speedup 600 --start 2022-05-21 --end 2022-05-22
```

**为什么必须清 topic：** 见 §0 的 L2 推论——新作业用随机组从 earliest 重读整个 topic，
不清就会把昨天的旧消息一并重算，导致 m1-out 轮数翻倍、守卫计数虚高。清 topic 是唯一可靠的隔离手段。

**作业状态无需担心：** 当前 `syn-submit-m1.sh` 是普通 `flink run`（无 savepoint），每次提交都是
**空状态**（RobustScaler 重新预热、RawCache 从零），与"全新实验"的语义天然一致。

### 5.B 续跑一次被中断的实验 —— 仅在作业状态还活着时才允许

**先判断前提。以下三条必须全部成立，才可以续跑：**

1. **集群 / Flink JobManager / TaskManager 从未重启**（作业的内存状态还在）；
2. **那个 M1Job 仍在 RUNNING**（`flink list` 能看到**同一个** JobID，不是你新提交的）；
3. **中断只发生在数据端**（比如 Mac 断连、tmux 里的重放容器被杀），**处理端一直没动**。

三条都满足时，只需把剩余数据接着灌进去：

```bash
bash deploy/scripts/syn-replay.sh status                       # 确认作业侧无残留、日志有上次 offset
bash deploy/scripts/syn-replay.sh --speedup 600 --resume       # 读 .replayer.offset，跳过已发送，续发剩余
```

**只要有任一前提不成立（集群关过、作业被 cancel 过、JM/TM 重启过），就不能续跑：**
作业的内存状态（预热中位数/IQR、RawCache 环、未闭合的采样轮）已经**随作业消失**，此时用
`--resume` 只把"剩余数据"喂给一个**空状态的新作业**，会造成归一化基线、缓存基线与前半段不一致，
对账必然失真。**这种情况一律改走 §5.A 全清重来。**

> **设计诚实说明：** 当前 M1Job 未启用 savepoint / checkpoint 恢复，因此**"处理端断点续跑"
> 在设计上并不支持**。`--resume` 只服务于"作业自始至终活着、仅数据端断了"这一窄场景
> （正是引入 tmux 的初衷：Mac 断连时 tmux 里的重放不中断，作业也不受影响）。
> 若未来需要真正的跨重启续跑，需在 M1Job 提交/取消流程里接入 savepoint，届时另行改造。

---

## 6. 验收前的干净起点自检 / Pre-run checklist

正式跑 V-M1-* 验收前，逐条确认起点是干净的（任何一条不符就回 §5.A）：

```bash
# a) 只有旧 job，没有游离的、上一轮忘了 cancel 的 M1Job
ssh fa-master "docker exec jobmanager flink list"

# b) source 分区数 = 8（不是被自动建成的 1）
ssh fa-master "docker exec kafka-1 kafka-topics.sh --bootstrap-server <brokers> --describe --topic synergia-source"

# c) 三个 topic 都从 0 开始（非 0 说明没清干净）
for t in synergia-source synergia-m1-out synergia-monitoring; do
  echo "== $t =="
  ssh fa-master "docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list <brokers> --topic $t"
done

# d) 重放器无残留会话/容器
bash deploy/scripts/syn-replay.sh status

# e) 新实验：确认续跑 offset 文件不存在（或确定本次不传 --resume）
ssh fa-master "ls -l /opt/fa-iforest/datasets/synergia/files_csv/.replayer.offset 2>/dev/null || echo '  无 offset 文件（新实验的期望状态）'"
```

---

## 7. 常见残留陷阱与症状 / Footgun table

| 陷阱 | 典型症状 | 根因 / 处置 |
|---|---|---|
| **忘清 topic 就提交新作业** | m1-out 轮数远大于 EDA；GetOffsetShell 起点非 0 | 随机组从 earliest 重读旧消息。→ §5.A 清 topic 重来 |
| **清了 topic 却传了 `--resume`** | source 一直 0，重放"没反应" | 重放器读旧 offset 以为已发完 → 一条不发。→ 删 `.replayer.offset` 或去掉 `--resume` |
| **source 被自动建成 1 分区** | 只有 partition 0 有数据，1–7 全 0 | 作业先订阅了不存在的 topic 触发 broker 自动建 1 分区。→ 已由 submit 预检 + KafkaSink 预检拦截；先 `syn-clean-topics.sh --yes` 重建 |
| **忘 cancel 昨天的 M1Job** | `flink list` 出现两个 M1Job；各消费一部分、m1-out 偏少 | 两作业抢同一 topic 的分区。→ cancel 掉多余的那个（按 JobID） |
| **误 cancel 旧 FA-iForest job / 用了 --purge** | 旧项目挂了 / 旧数据没了 | 触碰红线（§1）。→ 事前避免；用脚本的白名单/不带 --purge |
| **公网 IP 变了没刷新** | ssh 连不上 / broker 连接超时 | ECS 重启后公网 IP 变。→ `refresh-ips.sh` 更新 .env |
| **topic 数据写进去几分钟就没了** | GetOffsetShell 末端偏移量正常（如 57442），但 `--from-beginning` 消费到 0 条、转储文件 0 字节；最早偏移量≈最末偏移量 | 消息盖的是**事件时间**（2022），旧 `retention.ms=24h` 按消息时间戳判其超期即删。→ 已改 `SYN_RETENTION_MS=-1`；**旧 topic 需 `syn-clean-topics.sh --yes` 重建**才生效，然后重跑重放 |
| **RoundAssembler 收到全部数据却一条不发**（RobustScaler/RawCache/M2/scores 全 0） | Flink UI 该算子 Low Watermark 是"当下墙上时钟"（如 17880 亿+ ms，比 2022 数据超前数年） | topic 是 `LogAppendTime`，Kafka 用落盘墙上时钟覆盖了重放器的 2022 CreateTime → watermark 跑到当下、关轮定时器（事件时间 2022+30s）永不触发。→ 已在建表脚本显式钉 `message.timestamp.type=CreateTime`；**旧 topic 需 alter 或重建**，再重放。检查：`kafka-configs --describe --all --topic synergia-source | grep message.timestamp.type` 应为 CreateTime |
| **改了代码却像没生效**（同上 watermark 冻结、或行为仍是旧逻辑） | 明明本地改好了，集群上跑出来还是老样子；watermark 仍是墙上时钟 | **集群跑的是已上传的 jar，不是你的源码**。改任何 `.java` 后必须**重打包 + 重传**才生效：`mvn clean package && bash deploy/scripts/syn-upload-m1.sh --jar-only`。M2 阶段曾因部署的旧 jar 未给记录设事件时间戳，CreateTime 退化成墙上时钟、watermark 冻结（症状同上一行，但根因是"忘了重传 jar"）|
| **只重放单日，M2 一条离群都不出**（`m2_gate_admitted≈0`、scores 空、`m2WindowPoints=0`） | 单日重放跑完，M1 对账正常，但 M2 完全没产出 | **RobustScaler 预热 = 8640 轮/设备 ≈ 正好 1 天**，单日整天都在预热期，`M2Gate` 把 warmup 轮全丢 → M2 不 admit。**这是预期不是故障**。→ 要看 M2 产出：重放 **≥1.x 天**，或功能验证时 `syn-submit-m2.sh --extra '--warmup-rounds 600'`（约 100 分钟即冻结）|
| **`kafka-console-consumer` 挂住不返回** | 转储/抽看 topic 时终端卡死，`tail` 永不输出，脚本超时 | `--max-messages N` 若 N 超过 topic 实际消息数，消费者会一直等新消息不退出。→ 抽看/转储一律用 **`--timeout-ms 20000`**（消费完即退）而非 `--max-messages`；两者别混用 |
| **临时容器写文件报 Permission denied**（探针 CSV、重放器 offset 落盘失败） | `docker run --user root ... java ...` 仍报 `(Permission denied)`；重放器打印 `[resume] failed to save offset` | **`fa-iforest/flink` 镜像 entrypoint 会把命令降权成 uid 9999，`--user root` 被忽略**，写不进 root 拥有的挂载目录。→ 把要写的目录设为全可写：宿主 `mkdir -p <dir> && chmod 777 <dir>` 再挂载；重放器 offset 已改挂到独立可写目录 `/state`（见 `SYN_REPLAY_STATE_DIR`），探针工作目录已 `chmod 777` |
| **传了新 jar 但作业仍跑旧代码**（如新加的监测字段/参数不生效） | 明明 `syn-upload-m1.sh` 传了新 jar，产出里却缺新字段 | 两种时序坑：① 作业是在**传 jar 之前**提交的——已 RUNNING 的 job 锁定了当时的 jar，换文件不影响它，**须重新 `syn-submit-m2.sh`** 才加载新 jar；② 传的本地 jar 本身没重打包。→ 重放前先**用容器权威地验**（宿主机常无 unzip，必须走容器）：`ssh fa-master "docker exec jobmanager sh -c 'unzip -p /opt/flink/usrlib/<jar> com/leejean/m1/MonitoringSnapshot.class \| grep -a -c m2ColdCleared'"` 打印 >0 才是新；本地 jar 用 `bash deploy/scripts/check-jar.sh <jar>` 全 PASS 才对 |

---

## 7.1 M2 阶段的操作差异 / M2-stage workflow deltas

M2 与 M1 共用同一套集群流程（§3–§6 全部适用），只有以下几处不同，务必记牢：

- **提交的是 `M2Job`，不是 `M1Job`，且二者不可并存**（都消费 `synergia-source`）。做 M2 验收时用
  `syn-submit-m2.sh`；若集群上还挂着自己的 M1Job，先按 JobID cancel 掉它（绝不动旧项目 job）。
  `M2Job` 内部已复用 M1 全部算子并照常产出 M1 监测快照，所以**只提交 M2Job 一个作业**即可。
- **`M2Job` 会额外把标准化轮写进 `synergia-m1-out`**（`--out-topic`，供 V-M2-3 探针离线取数）。
  因此 M2 验收前清 topic 时，`synergia-m1-out` 也要一并清（`syn-clean-topics.sh` 已覆盖）。
- **计数器对账用 `syn-m2-metrics.sh`**（本地直连 Flink REST，自动找 RUNNING 的 M2Job 并对账
  `admitted = rounds − warmup − missing`）。**它只对"正在跑"的作业有效**——作业结束后自定义计数器
  与反压都查不到；唯有 checkpoint 存档（`/jobs/<jid>/checkpoints`，状态规模 + 时长）在作业还留在
  JobManager 已完成列表期间可查。
- **(R,k) 校准探针 `syn-m2-probe.sh` 是离线的**：它把 `m1-out` 转 JSONL 后在临时容器里单进程跑，
  **不经过 Flink 集群**，所以没有反压/ checkpoint/算子——探针本身不可能产生 Flink 界面上的标红。
- **预热与单日的关系（M2 尤其要记）**：见 §7 footgun——单日重放整天都在预热期，M2 不 admit、scores
  空，属预期；要看 M2 产出得重放 ≥1.x 天或临时 `--warmup-rounds` 小预热。

---

## 8. 日志与产物留存 / Logs & artifacts

- **重放日志**：master 上 `${REMOTE_HOME}/syn-replay.log`（`REMOTE_HOME=/opt/fa-iforest`）。
  容器被 `--rm` 清理后仍可回看结果摘要；跨天会**累积追加**，需要时归档或清空：
  `ssh fa-master "mv /opt/fa-iforest/syn-replay.log /opt/fa-iforest/syn-replay.$(date +%F).log"`。
- **重放器 offset**：写在独立可写目录 `${SYN_REPLAY_STATE_DIR:-/opt/fa-iforest/replay-state}/.replayer.offset`
  （不再写数据集目录，见 §7 权限坑）。开新实验若想从头灌，删它或不传 `--resume` 即可。
- **验收结果**：M1 填 `docs/m1_acceptance.md`（V-M1-1..5）；M2 填 `docs/m2_acceptance.md`（V-M2-1..4），
  (R,k) 探针表在 `docs/m2_probe.csv`。与集群实测冲突的数据事实按交接文档 §7 如实交回设计会话，不自行改写方案。
- **Flink UI**：`http://<master 公网IP>:8081`，看作业拓扑、各算子 Records、反压、checkpoint。

---

## 9. 一页速查 / One-page cheat sheet

```
收工不关集群：      syn-replay.sh stop            （作业留着；记下进度）
收工连集群一起关：  syn-replay.sh stop
                    flink cancel <M1_JOBID>       （只 cancel 自己的）
                    9-teardown.sh                 （绝不加 --purge）

次日集群没关：      直接进"开新实验/续跑"
次日集群关过：      refresh-ips.sh → 2-up-all.sh → syn-verify-cluster.sh
                    → kafka-topics --describe synergia-source（确认 8 分区）

开新实验（默认）：  flink cancel <旧M1_JOBID>（若有）
                    syn-replay.sh stop
                    rm .replayer.offset
                    syn-clean-topics.sh --yes     ← 必须！否则旧消息被重读
                    syn-submit-m1.sh              （先作业）
                    syn-replay.sh --start.. --end..（后重放，不带 --resume）

续跑中断实验：      仅当 集群/作业 从未重启 且 M1Job 仍 RUNNING：
                    syn-replay.sh --resume
                    否则一律改走"开新实验"

红线：             只 cancel 自己的 job；只碰 synergia-* topic；永不 --purge
```
