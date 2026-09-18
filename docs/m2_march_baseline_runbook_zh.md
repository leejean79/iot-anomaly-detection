# 执行手册：三月全月重放（Java 11）与 M2 基线记录

> **依据**：设计会话 *Runbook: Full-March Replay on Java 11 — Recording the M2 Baseline*
> （English edition v1.0, 2026-09-16）及其后续两项澄清裁决（2026-09-17）。
> **本文件用途**：把 runbook 翻译成可逐条执行的操作序列，补齐它未覆盖的实现细节，并记录
> 代码侧已完成的准备工作。runbook 是权威；本文件与之冲突时以 runbook 为准。
> **执行角色**：标注「操作员」的步骤在你的本地 Mac 上执行；标注「已完成（代码侧）」的由开发
> agent 在仓库中完成，无需重做。

---

## ⚠ 2026-09-18 暂停：发现 M2 只纳入了每条轮流的一半

两次完整重放都干净（四条断言全过、M1 与干净标定运行逐位相同），但 M2 逐设备数字作废：`RoundAssembler` 在
`onTimer` 里以 `轮时间戳 + 30 s` 发射，窗口按该时间戳分配，而 MCOD 的插入条件比对的是轮时间戳本身，导致
`(arrival mod 60 s) ≥ 30 s` 的那一半轮永不进入 MCOD 状态。这不是 M3 或 Java 11 阶段引入的回归，而是 M2
阶段以来一直存在、此前无任何验收步骤能暴露的缺陷。完整发现报告见 `docs/reports/m2_window_timestamp_finding.md`。
**在设计会话裁决修复方案之前，不要再重跑本手册。**

## 0. 三项已落实的裁决（开跑前请确认你理解这三点）

**裁决一：jar 来源。** 不从 M3 改动之前的提交（`9236228`）构建——它的 `pom.xml` 仍是 Java 8
目标配 Java 11 字节码的 DL4J，且早于原生库打包修复与 enforcer 守卫，会引入两个不受控变量。改为
**用 HEAD 的 jar，运行时加 `--m3-enabled false`**。此时 `M2Job` 传给 `PmcodFunction` 的
`m3AnnotatedTag` 为 `null`，转发分支整体不执行。这样后续开启标志的对照是**同一个 jar 的 A/B**，
比换 jar 更干净。

**裁决二：断言一的参照与容差。** `--expected-total` 取 EDA 权威值 **2,047,283**（断言一是
「实测对 EDA」），容差显式取 **±0.2%**，覆盖已知的 +0.12% 全局归并效应。干净标定运行实测到的
**2,049,816 轮**只作**可复现性观察**记入报告，**不是断言**。

**裁决三（本地）：`--max-idle-wall` 沿用默认。** runbook 写的 `--max-idle-wall 2` 会被解释为
**2 毫秒**（该参数单位是毫秒，默认 2000）。经确认**省略该参数**，沿用 2000 毫秒，与此前各次运行一致。

---

## 1. 代码侧已完成的准备（无需操作）

| 项目 | 状态 | 位置 |
|---|---|---|
| 保障测试：开关 M3 标志不改变任何 M2 产出 | 已完成，75/75 通过，并经变异验证不是盲测 | `src/test/java/com/leejean/m2/PmcodM3FlagEquivalenceTest.java` |
| 逐设备聚合与 Java 8 等值核验（runbook §4.2 / §5） | 新增 | `deploy/scripts/m2_device_baseline.py` |
| 上述工具的集群侧驱动脚本 | 新增 | `deploy/scripts/syn-m2-baseline.sh` |
| 逐设备半径终值 | 已在 `.env` 模板，与 runbook 逐字一致 | `SYN_M2_R_PER_DEVICE` |
| Light 通道 log1p 预变换 | 默认开启 | `ChannelTransform.defaultTable()` |
| 四条完整性断言 | 与 runbook §3 的 (a)~(d) 一一对应 | `ReplayVerify` |
| 评分通道标识 | `m2_point` / `m3_context` 已就位 | `ScoreEvent` / `M3ScoreRecord` |

保障测试的全限定名（报告表头需引用）：
`com.leejean.m2.PmcodM3FlagEquivalenceTest#togglingTheM3TagLeavesEveryM2OutputIdentical`

---

## 2. 阶段一：前置条件核对与 jar 投放

**执行环境**：本地 Mac（bash + JDK 11 + Maven），工作目录为项目根目录。

**前置条件**：`deploy/.env` 已配置且可 ssh 到 master；本机 `java -version` 为 11.x。

```bash
cd ~/你的项目目录/iot-anomaly-detection
git pull origin dev-claude

# 1.1 记录代码状态（报告第一张表需要）
git rev-parse HEAD
java -version 2>&1 | head -2

# 1.2 全量测试必须全绿（含本次新增的保障测试）
mvn test

# 1.3 打包并记录 jar 指纹
mvn clean package -DskipTests
shasum -a 256 target/iot-anomaly-detection-1.0-SNAPSHOT.jar
bash deploy/scripts/check-jar.sh

# 1.4 记录配置（报告需 dump 这些 SYN_* 变量）
grep -E '^SYN_(M2_R_PER_DEVICE|M1_CALIB_DAYS|M1_RELATIVE_GUARD|TM_|JAVACPP_|EDA_MARCH)' deploy/.env

# 1.5 记录三节点磁盘（清理后要对照）
for h in "$NODE_MASTER_PUBLIC_IP" "$NODE_WORKER1_PUBLIC_IP" "$NODE_WORKER2_PUBLIC_IP"; do
    echo "--- $h"; ssh -i "$SSH_KEY" root@"$h" "df -h /"; done

# 1.6 确认运行时
curl -s "http://$NODE_MASTER_PUBLIC_IP:8081/config" | python3 -m json.tool | grep -i version
curl -s "http://$NODE_MASTER_PUBLIC_IP:8081/jobs" | python3 -m json.tool   # 必须无 RUNNING 作业

# 1.7 彻底清场
bash deploy/scripts/syn-clean-topics.sh --yes
bash deploy/scripts/syn-replay.sh stop            # 清掉残留会话/容器
ssh -i "$SSH_KEY" root@"$NODE_MASTER_PUBLIC_IP" "rm -f /root/replay-state/.replayer.offset"

# 1.8 投放 jar
bash deploy/scripts/syn-upload-m1.sh --jar-only
```

**期望产出**：`mvn test` 报 `Tests run: 75, Failures: 0, Errors: 0`；`check-jar.sh` 全部 PASS；
`/jobs` 返回的 jobs 数组为空或无 RUNNING；清理后 `synergia-*` 各 topic 末端偏移量为 0。

**失败兜底**：`mvn test` 若因 enforcer 报 Java 版本，说明本机 JDK 不是 11.x，**不得**改用更高
JDK 绕过（Addendum 2 §7），切换 `JAVA_HOME` 后重试。`/jobs` 若有 RUNNING 作业，先在 Flink UI 上
确认它不是旧项目 FA-iForest 的作业——**旧项目的作业绝不可取消**。

---

## 3. 阶段二：先提交作业，再开始重放

**只提交 `M2Job` 一个作业。** `M2Job` 是**联合作业**，它自身就包含完整的 M1 管线
（`RawLineParser → RoundAssembler → RobustScaler → RawCache`），在同一作业内算子链接续、不经 Kafka
中转。`M1Job` 与 `M2Job` **不可同时运行**——二者都消费 `synergia-source`，都写 `synergia-m1-out`
与 `synergia-monitoring`，并行会产生重复轮，正是完整性断言 (b)「零重复」要抓的故障。报告 §4.3 需要的
M1 计数器就在 `M2Job` 内部产生，无需另起 `M1Job`。
The joint M2Job already contains the whole M1 chain; M1Job and M2Job must never run together.

**顺序不可颠倒**：必须先让作业 RUNNING，再启动重放器，否则早期消息会在无人消费的状态下堆积，
并使 `earliest` 起始点与重放起点错位。

```bash
# 2.1 确认没有任何 M1Job/M2Job 在跑（本项目的作业；旧项目 FA-iForest 的作业不得触碰）
ssh -i "$SSH_KEY" root@"$NODE_MASTER_PUBLIC_IP" \
    "docker exec jobmanager flink list" | grep -Ei 'M1Job|M2Job' || echo "  无本项目作业在跑，可继续"

# 2.2 提交联合作业（关键：关闭 M3），并显式带上窗口长度
#     显式传 --window-sec 3600 可覆盖 .env，杜绝配置漂移；提交后必须核对下面两行。
bash deploy/scripts/syn-submit-m2.sh --extra '--m3-enabled false --window-sec 3600' 2>&1 | tee /tmp/submit.log
grep -E 'W=|Window W/S' /tmp/submit.log
#     必须同时看到  W=3600s  与  Window W/S:      3600s / 60s
#     【首跑教训】第一次全月基线作废，正是因为作业实际跑的是 1800 秒窗口。窗口减半 → 每窗点数
#     减半 → 半径内邻居密度减半 → 八台设备离群率同向抬高 92%~272%，而这一点从离群率本身看不出
#     原因，要等一小时重放跑完、再从 m2_points_total/admitted 反推才能定位。此处一分钟的核对
#     可以省掉那一小时。

# 2.3 等作业 RUNNING，记录 job id
curl -s "http://$NODE_MASTER_PUBLIC_IP:8081/jobs/overview" | python3 -m json.tool | grep -E '"jid"|"name"|"state"'

# 2.4 启动重放（tmux 常驻，本机断连不影响；不传 --max-idle-wall，沿用 2000 毫秒）
#     【不可省】--speedup 3600 的节流是必须的。曾尝试"源 topic 数据还在，直接重新消费、免去重放"
#     这条捷径，结果全速追赶时八个分区推进不同步，RoundAssembler 的未闭轮缓冲涨到 23.1 MB，
#     超过 akka.framesize 默认的 10 MB，checkpoint 连续失败并最终把共享的 JobManager 也拖重启
#     （集群无 JM 高可用，重启会让旧项目 FA-iForest 的作业一并消失）。详见
#     docs/flink_memory_tuning_zh.md 第 3.6、3.7 节。
bash deploy/scripts/syn-replay.sh --speedup 3600 --start 2022-03-01 --end 2022-04-01

# 2.5 观察进度（Ctrl+C 只停跟踪，不停重放）
bash deploy/scripts/syn-replay.sh logs
```

**期望产出**：`/jobs/overview` 显示**恰好一个** RUNNING 作业，名为
`M2Job - M1 ingestion/normalization + pMCOD + LSTM-AE contextual anomaly detection`；重放器最终
打印 `Finished.` 汇总块与 `rc=0`。加速 3600 倍下墙钟耗时约 20 到 60 分钟。
若看到两个本项目作业同时 RUNNING，**立即停止重放并取消多余的那个**，否则整轮会因重复轮作废。

**必须记录的重放器汇总行**：`Produced (sent)`、`Send errors`、`Skipped malformed lines`、
`Data files (csv/sniffed)`、`Unknown-device records`、`File read errors`、`Idle-compression events`、
`Total compressed wall`、`rc`。

**失败兜底**：若 2.4 报「已有会话在运行」或「残留容器」，先 `bash deploy/scripts/syn-replay.sh stop`
再重试。若重放中途失败，**不要续跑**——回到 1.7 彻底清场后重来，污染的运行不可用于基线。

---

## 4. 阶段三：完整性门槛（在读取任何结果之前）

```bash
bash deploy/scripts/syn-replay-verify.sh --tol-pct 0.2
```

脚本的默认值已经是三月窗口（`2022-03-01T00:00:00Z` 至 `2022-04-01T00:00:00Z`）、`--calib-days 7`、
`--period-sec 10`，并自动读取 `.env` 的 `SYN_EDA_MARCH_ROUNDS_TOTAL=2047283`，因此**只需补容差**。

**期望产出**：四条断言全部 PASS，退出码 0，并在 `docs/m2_replay_verify.csv` 写出逐设备汇总。
四条断言依次是：轮数对账（实测对 EDA，±0.2%）、零重复 device+ts、边界对齐、逐设备冻结落第八天
（2022-03-08）。

**额外记录（可复现性观察，不是断言）**：把脚本打印的实测总轮数与干净标定运行的 **2,049,816**
对照，记入报告。相同即复现，不同也不构成失败，但需在报告中写明差值与你的观察。

**失败兜底**：退出码 3 表示缺 EDA 参照，检查 `.env` 的 `SYN_EDA_MARCH_ROUNDS_TOTAL`。退出码 1
表示有断言未过——**不要继续读结果**，按 runbook §8 清理后重跑。冻结日若落在第四天左右，通常是
重放压缩或重发导致，重点查重放器日志的 `idle-compress` 行与作业重启次数。

---

## 5. 阶段四：等排空，再采集

```bash
# 4.1 确认已排空：两次读数间隔两分钟且完全一致
bash deploy/scripts/syn-m2-metrics.sh | tee /tmp/m2_t1.txt
sleep 120
bash deploy/scripts/syn-m2-metrics.sh | tee /tmp/m2_t2.txt
diff /tmp/m2_t1.txt /tmp/m2_t2.txt && echo "已排空 / drained"

# 4.2 逐设备聚合 + Java 8 等值核验 + 出图（必须在清理 topic 之前）
bash deploy/scripts/syn-m2-baseline.sh --tag march

# 4.3 偏移量记录（三月窗口无 Java 8 基线，故用 --no-baseline 只打印实测值）
bash deploy/scripts/syn-m1-reconcile.sh --no-baseline

# 4.4 checkpoint 与内存（在运行最繁忙阶段截取，或事后读 History）
#     浏览器打开 http://<master 公网 IP>:8081 → 选中 M2Job → Checkpoints → History
```

**期望产出**：4.1 的 `diff` 无输出即表示计数器已稳定；4.2 打印逐设备对照表并在
`docs/reports/` 下生成 CSV、Markdown 表与并排 SVG 图，退出码 0 表示八台设备全部落在 ±1% 相对
容差内；4.3 打印八个分区末端偏移量与 `m1-out` 轮数。

**必须记录的计数器**：三道闸（`m2_gate_admitted`、`m2_gate_warmup_bypass`、`m2_gate_missing_bypass`、
`m2_gate_censored_entered`、`m2_gate_coldstart_clear`、`m2_gate_late_drop`）与对账等式
`admitted = rounds − warmup − missing`；M2 总量（`m2_outliers_total`、`m2_points_total`、
`m2_windows_total` 与总体比率）；M1 计数器（轮数、未闭轮、重复键、删失 Light、RSSI 哨兵值、未知传感器）。

**失败兜底**：4.2 若报「转储里没有 M2 快照」，说明整段仍在预热期——检查 4.1 的
`m2_gate_admitted` 是否为 0；三月全月扣掉 7 天标定后应有约 24 天进入 M2，若仍为 0 则重放窗口或
`--calib-days` 有误。**4.2 必须在第 7 节清理之前完成**，清理会删掉它要读的监测数据。

---

## 6. 阶段五：与 Java 8 的等值核验

`syn-m2-baseline.sh` 已自动完成该核验：它从 `docs/m2_probe_7d_clean.csv` 读取每台设备在其终值
半径、k=10 处的 Java 8 `meanOutlierRate`，与本次实测按 ±1% 相对容差比对。参照值如下，与 runbook
第 5 节给出的表逐位一致（我已核对过）：

| 设备 | R | Java 8 离群率 |
|---|---:|---:|
| A | 1.0 | 0.0481% |
| B | 1.0 | 0.0377% |
| C | 1.0 | 0.0583% |
| D | 0.75 | 0.1196% |
| E | 1.0 | 0.0438% |
| F | 1.0 | 0.0362% |
| G | 1.5 | 0.2650% |
| H | 1.0 | 0.0612% |

**口径说明（runbook §4.2 要求显式声明）**：`meanOutlierRate` 是每个滑动步的「窗口内离群数 ÷
窗口内点数」再对所有滑动步取**算术平均**，与探针完全同口径。它**不等于**
`m2_outliers_total ÷ m2_points_total`（后者按点数加权）。脚本两个口径都输出，报告中不可混用。

**若有设备超差**：按 runbook §5，**不要调参**。先判断是否属于探针与作业的机制差异（窗口边界处理、
标定切换时刻），无法用机制差异解释的偏差是需要上报设计会话的发现，并按 §8 在任何 M3 改动触碰 M2
之前停下。

---

## 7. 阶段六：写基线报告

报告骨架已生成在 `docs/reports/m2_java11_march_baseline.md`，按其中的占位符逐项填入即可。表头必须
写明的三项(裁决一要求)是：jar 的 SHA-256、`--m3-enabled false` 这个标志、以及保障测试的全限定名。

图由 4.2 步自动生成（`m2_java11_march_rate_cmp.svg`），已满足 §6「一张 Java 8 与 Java 11 并排图」
的要求。

可选的按设备按天评分聚合（仅计数、不归档原始记录）在报告骨架中留了位置，若设计会话需要再补。

---

## 8. 阶段七：收尾

**顺序**：确认报告已写完、`docs/reports/` 下的产物已生成并提交之后，才执行清理。

```bash
bash deploy/scripts/syn-clean-topics.sh --yes
ssh -i "$SSH_KEY" root@"$NODE_MASTER_PUBLIC_IP" "rm -f /root/replay-state/.replayer.offset"
for h in "$NODE_MASTER_PUBLIC_IP" "$NODE_WORKER1_PUBLIC_IP" "$NODE_WORKER2_PUBLIC_IP"; do
    echo "--- $h"; ssh -i "$SSH_KEY" root@"$h" "df -h /"; done   # 与 1.5 对照
# 取消两个作业（在 Flink UI 上 Cancel，或用 REST）
```

**注意**：只取消本次提交的两个作业。旧项目 FA-iForest 的任何作业与容器都不得触碰。

---

## 9. 边界（来自 runbook §8）

运行期间不改任何代码或配置。完整性核验失败则清理重跑，不得读取被污染的运行。逐设备等值核验若因
非机制差异的原因失败，停下并上报，不得让任何 M3 改动在此之前触碰 M2。
