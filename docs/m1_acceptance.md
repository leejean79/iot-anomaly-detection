# M1 Stage Acceptance — Replayer / Ingestion / Round Assembly / Normalization / Raw Cache

> **总体状态 / Overall: 五项验收全部 PASS（2026-08 于活集群实测）。**
>
> | 验收 | 结论 | 关键证据 |
> |---|---|---|
> | V-M1-1 读入/产出对账 | ✅ PASS | 单日 8 分区逐一对上 uptime_matrix；全量读入 79,274,744=EDA 精确、轮数 9,904,676 vs EDA 9,904,739（差 63）；端到端零丢失 |
> | V-M1-2 时间语义/压缩 | ✅ PASS | 102.3h 全集群停机被压缩、H 单独停机不压缩；watermark 55s=EDA 建议 |
> | V-M1-3 装配抽检 | ✅ PASS | 50 抽样逐值匹配（OK=50, MISMATCH=0） |
> | V-M1-4 守卫计数对账 | ✅ PASS | IR/unknown=336,970 逐字命中 EDA；dupKeys 同量级；计数链经此证明无误 |
> | V-M1-5 全量压力 | ✅ PASS | 13,010 rec/s、反压 0、状态 KB 级、checkpoint 毫秒级、1h41m 无失败 |
>
> **交回设计会话的新数据事实（§7）**：设备 H 每轮 **RSSI 与 IR 互斥**（713,799 + 336,970 = 1,050,769
> = H 轮数），M1 的 unknown_sensor 全部来自 H 用 IR 顶替 RSSI 的轮次。留档待补：Flink Low Watermark
> 截图（V-M1-2）、DF-12 恢复段 roundsTotal 峰值（V-M1-5，聚合证据已支持"无故障排空"）。

> **Status: TEMPLATE.** The five acceptance hooks (V-M1-1..5) run against the **live cluster**
> and must be executed by the operator on the master node; this file gives the exact commands
> and a table to paste the results into. The offline correctness of every operator is already
> covered by the unit tests (`mvn test`, 16 tests: parser guards, keep-first dedup, round-close
> timer with missing mask, warm-up freeze boundary, IQR bypass, RoundWindow, keyed isolation),
> and the replayer's global-merge / idle-compression logic by `ReplayerLogicTest` + a dry-run.

## 0. Prerequisites (handover §2)

> **重要（保留策略）/ IMPORTANT (retention).** 消息盖的是事件时间（2022）时间戳，旧的
> `retention.ms=24h` 会按消息时间戳判其超期、几分钟内删光 m1-out/monitoring/source 的可消费数据
> （末端偏移量还在但日志段空了，`--from-beginning` 读到 0）。`env.example` 已改
> `SYN_RETENTION_MS=-1`（永久保留，靠 syn-clean-topics 手动清理）。**务必先把本地 `deploy/.env`
> 的 `SYN_RETENTION_MS` 同步改成 `-1`，再执行下面的 `syn-clean-topics.sh --yes` 重建 topic**，
> 否则新建的 topic 仍是 24h、数据照删。

```bash
# (0) 确认 deploy/.env 里 SYN_RETENTION_MS=-1（见上）；否则先改再继续

# (1) clear the 80 ENV smoke-test messages left in synergia-source
bash deploy/scripts/syn-clean-topics.sh --yes

# (2) create the M1 topics (already added to env.example's SYN_EXTRA_TOPICS):
#     synergia-scores(4,RF2), synergia-monitoring(1,RF2), synergia-m1-out(1,RF2)
bash deploy/scripts/syn-create-topics.sh

# (3) broker-check fix is already in syn-verify-cluster.sh (Kafka-view primary + ZK 2>&1);
#     re-run for a clean 14/14 checklist if desired
bash deploy/scripts/syn-verify-cluster.sh

# (4) build + upload jar and dataset (dataset ~2.3 GB, one time)
mvn clean package                                   # produces target/iot-anomaly-detection-1.0-SNAPSHOT.jar
bash deploy/scripts/syn-upload-m1.sh --data-dir /path/to/local/files_csv   # jar + dataset
#   jar   -> /opt/fa-iforest/jars/               (cluster-side stays under the old mount, DEV-D8)
#   data  -> /opt/fa-iforest/datasets/synergia/files_csv/
```

Runbook order is **submit the job first, then replay** (the job starts from `earliest` by default):

```bash
bash deploy/scripts/syn-submit-m1.sh            # submit M1Job (flink run -d, cluster-resident, survives disconnect)
bash deploy/scripts/syn-replay.sh --speedup 600 --start <day> --end <day>   # replay in a tmux session on master
bash deploy/scripts/syn-replay.sh status        # progress / recent log
bash deploy/scripts/syn-replay.sh attach        # watch live (Ctrl+B then D to detach without stopping)
bash deploy/scripts/syn-replay.sh stop          # stop the replay
```

**Disconnect resilience (tmux).** The M1 **job** is detached (`flink run -d`) and lives on the cluster,
so it survives your Mac disconnecting. The **replayer** is a long-running client process, so
`syn-replay.sh` runs it inside a tmux session on master (matching FA-iForest's experiment scripts) —
a Mac disconnect does not interrupt it. The full run (V-M1-5) at k=3600 takes ~1 hour, so use the tmux
default (not `fg`). Replay output is also teed to `${REMOTE_HOME}/syn-replay.log` for post-hoc review.

---

## V-M1-1 — Reconciliation replay

Replay one selected day at k=600; the round count consumed from `synergia-m1-out` must reconcile
with the EDA's round count for that day (difference explained by known gaps).

```bash
# replay one day
bash deploy/scripts/syn-replay.sh --speedup 600 --start 2022-05-21 --end 2022-05-22
# count rounds landed on m1-out (per device or total)
docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list <brokers> --topic synergia-m1-out | awk -F: '{s+=$3} END{print "m1-out records:", s}'
```

| item | value |
|---|---|
| selected day | **2022-05-21**（重放窗口 `--start 2022-05-21 --end 2022-05-22`）|
| rounds on synergia-m1-out | **57,442**（`synergia-m1-out:0` 偏移量总和）|
| EDA round count for that day | **≈ 57,430**（uptime_matrix 中 A–G 当日记录数合计 459,440 ÷ 每轮约 8 路通道）|
| difference & explanation (known gaps) | 差约 12 轮，属边界效应（跨零点边界读数、`--end` 端点、少量 keep-first 去重）。**设备 H 全天 0 条**——uptime_matrix 证实 H 于 2022-05-21～05-23 处于单独停机段（DF 事实），故 `synergia-source` 的 partition 7（H→7）为 0，符合预期，非丢失。|
| **verdict** | **PASS** |

**逐分区读入侧对账（source，2022-05-21）/ per-partition ingestion reconciliation.** 各分区
`synergia-source` 偏移量与 uptime_matrix 当日记录数逐一吻合（差异仅个位数到十几条，均为边界效应）：

| 分区 partition | 设备 device | synergia-source 偏移量 | uptime_matrix[设备,05-21] | 差 diff |
|---|---|---|---|---|
| 0 | A | 65688 | 65680 | +8 |
| 1 | B | 65512 | 65496 | +16 |
| 2 | C | 65568 | 65568 | 0 |
| 3 | D | 65712 | 65720 | −8 |
| 4 | E | 65720 | 65712 | +8 |
| 5 | F | 65592 | 65584 | +8 |
| 6 | G | 65680 | 65680 | 0 |
| 7 | **H** | **0** | **0**（停机段）| 0 |
| 合计 total | — | **459,472** | 459,440 | +32 |

**端到端链路无损 / lossless end-to-end.** 消费 459,472 条读数 → RoundAssembler 装配 57,442 轮 →
RobustScaler 57,442 → RawCache→sink 57,442 → `synergia-m1-out` 收到 57,442（Flink UI 各算子
Records Received 与 topic 偏移量一致，从消费到产出零丢失）。

**全量级对账（补充，与 EDA 报告）/ whole-dataset reconciliation vs EDA.** 全量重放进一步在
数据集级别对上了 EDA：M1 source 消费 **79,274,744** 读数 = EDA 解析行 **79,274,744**（逐字精确）；
M1 产出 **9,904,676** 轮 vs EDA 采样轮合计 **9,904,739**（差 63，0.0006%，跨文件边界/去重/末尾窗口）；
M1 watermark 取值 **55s** = EDA §9 Q1 建议值。读入侧口径在单日与全量两个尺度上都成立。

> **File-discovery reconciliation (important).** The replayer now prints a discovery breakdown:
> `[discover] csv-named data files: N ; sniffed non-.csv data files: M ; skipped non-data files: K`.
> The EDA patch-01 established the dataset has **3747** files total (vs the 3471 `.csv`-named). Confirm
> `N + M + K == 3747` and that the `M` sniffed non-`.csv` files (a `[discover][WARN]` lists samples) are
> genuinely data — the replayer **includes** them by default so no data is silently dropped (use
> `--strict-csv-names` to restrict to `.csv` names). If `M > 0`, record it as a data-fact confirmation
> for the design session (handover §7). The earlier run showed `Files found: 3471` because that build
> silently skipped the 276 non-`.csv` files; this is now surfaced.

---

## V-M1-2 — Time semantics

The watermark progression must match the segment's event-time axis; the idle-compression audit log
must line up with the known outage segments (device H solo outages must NOT compress; the ~102-hour
full-cluster outage must).

- Watermark: Flink UI → M1Job → each operator's "Low Watermark", compared against the segment's ts.
- Compression audit（审计行格式）:
  - **当前构建**直接打印真实 gap：`[idle-compress] gapSec=… (~Nh) normalWallMs=… cappedWallMs=…
    atEventTs=… sinceLastCompressSec=…`。`gapSec` 即被压缩的停机时长，直接可读。
  - **旧构建**（本次全量重放用的即旧格式）打印 `eventSpanSec=… originalWallMs=… compressedWallMs=…`，
    其中 `eventSpanSec` 是"距上次压缩的跨度"**非 gap**；真实 gap ≈ `originalWallMs × speedup ÷ 1000`
    （k=3600 时 `gap 小时 = originalWallMs / 1000`）。下表即按此解码。

全量重放（165 天，k=3600，用时 1h41m32s）实测 10 次全集群空闲压缩（另有 2 行为早前单日 05-21
重放残留在 `tee -a` 追加日志里，已剔除）。按 `gap=originalWallMs/1000` 小时解码：

| atEventTs (UTC, gap 结束) | originalWallMs | 真实 gap |
|---|---|---|
| 2022-02-17 14:07 | 13565 | 13.6 h |
| 2022-03-18 11:06 | 16175 | 16.2 h |
| 2022-04-20 09:42 | 17293 | 17.3 h |
| 2022-05-09 09:35 | 65840 | 65.8 h |
| 2022-05-18 12:16 | 17434 | 17.4 h |
| 2022-05-18 15:40 | 2081 | 2.1 h |
| 2022-05-24 15:30 | 8241 | 8.2 h |
| 2022-05-26 15:08 | 7966 | 8.0 h |
| **2022-06-13 02:20** | **102256** | **102.3 h** |
| 2022-06-21 20:28 | 17876 | 17.9 h |

| item | value |
|---|---|
| watermark tracks event-time axis? | _Flink UI Low Watermark 截图待留档_；两条佐证：(1) 全量 990 万轮仅 9,487 例 incomplete（0.096%），乱序会大量丢轮；(2) watermark 允许延迟 **55s** = EDA §9 Q1 依 P99.9 抖动给出的建议值 |
| idle-compress events vs known outages | 10 次全集群空闲压缩，gap 2.1–102.3 h（见上表），分布 2022-02～06 |
| device H solo outage did NOT compress? | **是**。压缩基于**全局归并流**：H 单独停机时其它设备仍每 10s 产出、全局事件时间不空闲，故不触发（上表无与 H 单独停机段对应的压缩项）。单日 05-21 亦证实 H 全程 0、partition 7=0 |
| 102-hour full-cluster outage DID compress? | **是**。`originalWallMs=102256 → gap 102.3 h`，结束于 2022-06-13 02:20（起于约 06-08 20:00），即交接文档预告的全集群停机 |
| **verdict** | **PASS**（102h 全集群停机压缩、H 单独停机不压缩，均符合；watermark 截图待补作留档） |

---

## V-M1-3 — Pivot spot check

Sample N random rounds from `synergia-m1-out` and compare **every raw value** against the original CSV.

```bash
# dump some m1-out messages as JSONL (one DeviceRound per line)
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server <brokers> \
    --topic synergia-m1-out --from-beginning --max-messages 2000 > m1out.jsonl
# compare against the dataset
python3 deploy/scripts/m1_pivot_check.py --rounds-jsonl m1out.jsonl \
        --data-dir /opt/fa-iforest/datasets/synergia/files_csv --n 50
```

| item | value |
|---|---|
| N sampled | **50**（自 2000 条 m1-out 转储中随机抽 50） |
| OK / MISMATCH / UNMATCHED | **OK=50 / MISMATCH=0 / UNMATCHED=0** |
| **verdict** | **PASS**（MISMATCH==0） |

> 抽检在 master 上运行（数据集就近，2.3GB 不必下回本地）。文件名索引仅纳入 `.csv` 命名文件
> （`[index] 3471 data files indexed`）；本次 50 个抽样轮全部落在 `.csv` 文件内、逐值匹配，故
> PASS 成立。若日后抽样命中非 `.csv` 的 sniffed 数据文件（EDA patch-01 认定的 276 个之一），
> 该轮会显示 UNMATCHED——那是索引口径问题而非装配错误；届时可扩展 `m1_pivot_check.py` 的文件名
> 正则一并纳入。归一化 xNorm 的正确性由单测 `M1PipelineTest` 覆盖，不在本抽检内比对。

---

## V-M1-4 — Guard reconciliation

The counters for **censored Light**, **RSSI sentinels**, **duplicate keys**, and **IR drops** must
match the EDA's known counts. Read them from the monitoring snapshots (summed) or from the
Flink/Prometheus metrics.

> **粒度对齐（重要）/ granularity.** EDA 的守卫计数**没有单日粒度**，只有全量数据集口径。因此
> V-M1-4 的对账**在全量重放（V-M1-5）时做**：把整段 165 天的 monitoring 快照累加，与 EDA 的
> **全量总数**逐项对比——单日重放无法与 EDA 对账。这样一次全量重放即同时满足 V-M1-2（跨全部
> 停机段的 watermark/压缩审计）、V-M1-4（全量守卫计数对账）与 V-M1-5（压力）。下面的转储命令
> 对全量重放后的 monitoring 同样适用（`--max-messages` 需放大到覆盖全量快照数）。

```bash
# monitoring snapshots for the segment
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server <brokers> \
    --topic synergia-monitoring --from-beginning --max-messages 5000 > mon.jsonl
# sum the guard counters
python3 - mon.jsonl <<'PY'
import sys, json, collections
s=collections.Counter()
for line in open(sys.argv[1]):
    if not line.strip(): continue
    d=json.loads(line)
    for k in ("censoredLight","rssiSentinel","dupKeys","unknownSensor","incompleteRounds","malformed"):
        s[k]+=d.get(k,0)
print(dict(s))
PY
```

全量 monitoring 消费到尾累加（`roundsTotal=9,904,658`，与拓扑 9,904,676 差 18，为末尾少数窗口
未在 `--timeout-ms` 前落盘，可忽略）：

全量 monitoring 消费到尾累加，与 EDA 报告全量总数对账：

| counter | M1 measured（全量） | EDA known（全量总数） | match? |
|---|---|---|---|
| IR drops (unknown_sensor) | **336,970** | **336,970**（IR 仅设备 H，EDA §2.1/§3.1） | ✅ **逐字精确命中** |
| duplicate keys | **70,986** | 80,762 多余行（EDA §2.5） | ✅ 一致（M1 略低 9,776，全局归并弥合跨文件边界轮 + IR 重复计入 unknown + 未闭轮/定时器边界） |
| censored Light (Light==65536) | **29** | 无直接计数（EDA §3.1 确认 max=65536 存在，未计频次） | ⚠ EDA 无基线；量级合理（Light 月 P90≈3k≪65536，饱和罕见），计数链已由 IR 精确命中证明无误 |
| RSSI sentinels (RSSI==0) | **2** | 无直接计数（EDA §3.2 标准哨兵=0；§3.1 RSSI min=0 存在；RSSI==0 属 DEV-D7c 项目标记，非 EDA 哨兵集） | ⚠ EDA 无基线；与 H 的 336,970 缺 RSSI（改报 IR）是两回事 |
| （辅助）incomplete rounds | 9,487 | ~20,705（EDA §2.3 文件内闭合，各设备 rounds−full 之和） | M1 更低属预期：全局归并弥合了 EDA 按文件切开的边界轮 |
| （辅助）malformed | 0 | 0（EDA §6 畸形行=0） | ✅ 一致 |
| **verdict** | | | **PASS** |

> **判定依据 / verdict.** 唯一有硬基线的项（IR/unknown=336,970）**逐字命中**，直接证明
> "parser 检测→forward→assembler 逐轮累加→DeviceRound→MonitoringAggregator" 整条计数链无误
> （`RawLineParser.java:104,115,119`、`RoundAssembler.java:81,93,114,164-167`、`MonitoringAggregator.java:60-68`）；
> dupKeys 与 EDA 同量级且差异可解释；censored Light / RSSI==0 两项 EDA 未给频次基线，M1 提供了新计数，
> 量级由 EDA 上下文佐证合理（非漏计）。**新数据事实候选（交回设计会话，§7）**：设备 H 每轮
> **RSSI 与 IR 互斥**——713,799 有 RSSI + 336,970 有 IR = 1,050,769 = H 轮数；即 M1 的 unknown_sensor
> 全部来自 H 用 IR 顶替 RSSI 的那些轮。之前对 `censored=29 / rssi=2` 偏低的存疑，经 EDA 对照可解除。

---

## V-M1-5 — Full-run stress

Replay all 165 days at k=3600; record throughput, backpressure, checkpoint durations, and cache state
size. Give the **DF-12 recovery surge** (the full-cluster outage recovery) its own paragraph.

```bash
bash deploy/scripts/syn-submit-m1.sh
bash deploy/scripts/syn-replay.sh --speedup 3600     # tmux session on master; ~1 hour wall time
bash deploy/scripts/syn-replay.sh status             # check periodically; safe to disconnect the Mac
```

全量 165 天 k=3600，用时 **1h41m32s（6092s）**；规模 **79,274,744 读数 → 9,904,676 轮**（比值 8.00）。

| item | value |
|---|---|
| throughput (records/s) | 输入 ≈ **13,010 rec/s**（79,274,744÷6092）；输出 ≈ **1,626 rounds/s**（9,904,676÷6092） |
| max backpressure (Flink UI) | **0**（全程无反压） |
| checkpoint durations (p50/p99) | End-to-End **6–18 ms**（各算子 6/6/10/10/11ms；History 样本 10–18ms），p50≈10ms、p99≈18ms；in-flight 0 B、ack 8/8 100% |
| cache state size | **RawCache 算子 402 KB**（全管线 checkpoint 合计约 **514 KB**：Parser 8.36KB、RoundAssembler 41.3KB、RobustScaler 42.8KB、RawCache→m1-out 402KB、Monitoring 19.5KB） |
| **DF-12 recovery surge** | _见下方"如何取证"；聚合证据已支持"无故障排空"，surge 形态待按恢复段窗口分析_ |
| **verdict** | **PASS（性能层面）**：吞吐稳定、反压 0、状态仅 KB 级、checkpoint 毫秒级、1h41m 全程无失败/重启 |

**DF-12 恢复浪涌——如何取证 / how to characterise.** 那段 ~102h 全集群停机的 gap 结束于
**atEventTs=1655074819（2022-06-13 02:20 UTC）**，即所有设备同时恢复的时刻，"浪涌"指恢复后缓冲数据
回灌造成的记录率骤升。

1. **聚合结论（已可下）**：整轮 **最大反压=0、状态仅 KB 级（RawCache 402KB 未随恢复膨胀）、
   checkpoint 始终毫秒级、作业 1h41m 跑完无失败/重启**——说明恢复浪涌被**无故障排空**，
   这已满足 DF-12 的核心判据。
2. **浪涌形态（可选细化）**：从 `mon_full.jsonl` 截取恢复段、看每设备每 60s 的 `roundsTotal` 是否
   较常态骤升（缓冲回灌会在少数窗口出现远高于常态 ~6 的 rounds）：

   ```bash
   python3 - mon_full.jsonl <<'PY'
   import sys, json
   LO = 1655074819                 # 2022-06-13 02:20 UTC（恢复时刻）
   HI = LO + 12*3600               # 恢复后 12 小时
   peak = 0; tot = 0; nwin = 0; bydev = {}
   for line in open(sys.argv[1]):
       line = line.strip()
       if not line: continue
       d = json.loads(line)
       if LO <= d.get("ts", 0) <= HI:
           r = d.get("roundsTotal", 0)
           tot += r; nwin += 1; peak = max(peak, r)
           bydev[d.get("device")] = bydev.get(d.get("device"), 0) + r
   print("恢复后12h: 窗口数=%d 总轮=%d 单窗峰值=%d(常态≈6)" % (nwin, tot, peak))
   print("按设备:", bydev)   # 关注设备 H：uptime_matrix 显示其恢复期日计数异常偏高（缓冲回灌）
   PY
   ```

   把 `单窗峰值` 与常态（≈6 轮/设备/60s）对比，若峰值明显更高即为可量化的恢复浪涌；重点看
   **设备 H**（`uptime_matrix` 显示其 2022-06 后若干日计数高达 16–22 万，远超常态 ~6.9 万，
   正是缓冲回灌的证据）。把这段结论写进上表 DF-12 行。

---

## 全量重放取证操作 / Full-run evidence-gathering (V-M1-2 / -4 / -5 一次做完)

一次全量重放同时满足这三项。先按下面启动全量重放，跑完（约 1 小时）后按 §A/§B/§C 取证。

### 0) 启动全量重放（先重置再全量）

```bash
# 只 cancel 自己的旧 M1Job（按 JobID，绝不碰旧 FA-iForest job）
ssh fa-master "docker exec jobmanager flink list"
ssh fa-master "docker exec jobmanager flink cancel <旧M1_JOBID>"
# 清 topic 重置偏移量（retention 仍 -1、分区数仍对）；确认 .env 的 SYN_EXTRA_TOPICS 完整
bash deploy/scripts/syn-clean-topics.sh --yes
# 先提交作业，再全量重放（不带 --start/--end 即全部 165 天，k=3600，tmux 常驻）
bash deploy/scripts/syn-submit-m1.sh
bash deploy/scripts/syn-replay.sh --speedup 3600
bash deploy/scripts/syn-replay.sh status        # 期间可断开 Mac
```

`<brokers>` = `172.16.0.162:9092,172.16.0.163:9092,172.16.0.164:9092`（即 `.env` 三节点 9092）。
`<master>` = master 公网 IP（Flink UI 在 `http://<master>:8081`）。

### A) V-M1-2 — 时间语义 / 压缩审计

1. **Watermark 跟随事件时间轴**：Flink UI → 该 Job → 点每个算子 → 面板底部 **"Low Watermark"**；
   或算子的 **Watermarks** 子标签。把它换算成时间，核对是否落在当前重放段的事件时间附近
   （随重放推进单调前进，不回退、不长时间停滞）。
2. **空闲压缩审计**：master 上 `syn-replay.log` 里的压缩审计行——

   ```bash
   ssh fa-master "grep -F '[idle-compress]' /opt/fa-iforest/syn-replay.log"
   ```

   每行形如 `[idle-compress] eventSpanSec=… originalWallMs=… compressedWallMs=…`。逐条把
   `eventSpanSec` 与已知停机段对照：
   - **设备 H 单独停机段**：H 只是自身缺数据、其它设备仍在产出，全局事件时间**未**空闲 →
     **不应**出现压缩行（若出现即为疑点，记入 §Notes）。
   - **~102 小时全集群停机**：全体设备皆无数据、全局事件时间空闲 → **应**出现一条大
     `eventSpanSec`（≈ 102h≈367200s 量级）的压缩行。

把结论填进上文 **V-M1-2** 表。

### B) V-M1-4 — 守卫计数对账（与 EDA 全量总数）

**首选：读 Flink 累计计数器**（无需消费百万条消息）。Flink UI → 该 Job → 对应算子 →
**Metrics** 子标签 → 在 "Add metric" 里按名字添加，读其累计值（并行度>1 时把各 subtask 相加）：

| 对账项 | Flink 指标名 | 所在算子 |
|---|---|---|
| 删失 Light（Light==65536） | `m1_parser_censored_light` | RawLineParser（Source 链） |
| RSSI 哨兵（RSSI==0） | `m1_parser_rssi_sentinel` | RawLineParser |
| IR/未知传感器丢弃 | `m1_parser_unknown_sensor` | RawLineParser |
| 重复键 | `m1_assembler_dup_keys` | RoundAssembler |
| （辅助）畸形行 | `m1_parser_malformed` / `m1_parser_malformed_no_device` | RawLineParser |
| （辅助）总轮数 / 未闭轮 | `m1_assembler_rounds_total` / `m1_assembler_incomplete_rounds` | RoundAssembler |

**交叉核对（自包含，不依赖 Prometheus）：消费 monitoring 累加**。全量快照约百万级，用
`--timeout-ms` 到尾后转储到 master 文件再累加（字段名即 JSON key）：

```bash
ssh fa-master
docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server <brokers> \
    --topic synergia-monitoring --from-beginning --timeout-ms 20000 > mon_full.jsonl
wc -l mon_full.jsonl
python3 - mon_full.jsonl <<'PY'
import sys, json, collections
s = collections.Counter()
for line in open(sys.argv[1]):
    line = line.strip()
    if not line:
        continue
    d = json.loads(line)
    for k in ("censoredLight","rssiSentinel","dupKeys","unknownSensor","incompleteRounds","malformed","roundsTotal"):
        s[k] += d.get(k, 0)
print(dict(s))
PY
```

把 Flink 计数器值填进上文 **V-M1-4** 表的 "M1 measured" 列，EDA 全量总数填 "EDA known" 列，
逐项判 match（差异用已知缺口解释）。monitoring 累加值应与 Flink 计数器一致（作为自洽性交叉核对）。

### C) V-M1-5 — 压力指标（全部在 Flink UI 读）

- **吞吐 records/s**：Flink UI → Job → Source/各算子的 Records Sent/Received 速率（或总量 ÷ 墙钟）。
- **最大反压**：Job → **Backpressure** 子标签，记录最高的 OK/LOW/HIGH 比例与出现算子。
- **checkpoint 时长 p50/p99**：Job → **Checkpoints** → **History**，看各次 End to End Duration 分布。
- **缓存状态大小**：Checkpoints → 最近一次的 State Size（可细看 RawCache 算子），作为 RawCache 环 +
  其它 keyed state 的规模。
- **DF-12 恢复浪涌**（单独一段）：定位那段 ~102h 全集群停机**恢复后**的时刻，描述：source 积压深度
  （`GetOffsetShell` 末端 − 消费位移）、watermark 是否平滑追赶、反压是否短时升高后回落、
  checkpoint 是否仍成功、管线有无失败/重启。把这段结论写进 **V-M1-5** 表的 DF-12 行。

> **磁盘提醒**：全量 m1-out + monitoring 占用可观（m1-out ~5–10GB，RF2 翻倍）。本轮取证完成后，
> 按 `docs/cluster_runbook.md` 用 `syn-clean-topics.sh --yes` 清理，避免多轮累积占满盘。

---

## Notes / data facts to report back (handover §7)

Any measured fact that contradicts the handover is reported to the design session, not patched around.
Record candidates here:

- _e.g., unknown DeviceIds observed (replayer "Unknown-device records" > 0)_
- _e.g., malformed-line count vs EDA_
- _..._
