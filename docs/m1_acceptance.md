# M1 Stage Acceptance — Replayer / Ingestion / Round Assembly / Normalization / Raw Cache

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
- Compression audit: the `[idle-compress] eventSpanSec=… originalWallMs=… compressedWallMs=…` lines
  printed by `syn-replay.sh` — cross-check `eventSpanSec` against the known outage spans.

| item | value |
|---|---|
| watermark tracks event-time axis? | _待全量/多日重放时填 / pending full-run_ |
| idle-compress events vs known outages | _待填 / pending_ |
| device H solo outage did NOT compress? | **已由 uptime_matrix 证实**：设备 H 于 2022-05-21～05-23 记录数为 0（单独停机段），2022-05-24 恢复（41,840）。单日 05-21 重放时 H 全程无数据，partition 7 = 0，不存在需压缩的空闲流；本条数据事实成立。_压缩行为本身待跨越该停机段的多日重放验证_ |
| 102-hour full-cluster outage DID compress? | _待跨越该停机段的多日重放时填 / pending multi-day run_ |
| **verdict** | _部分确认（H 停机段数据事实已证实）；压缩审计待多日重放 / partially confirmed_ |

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

| counter | M1 measured | EDA known | match? |
|---|---|---|---|
| censored Light (Light==65536) | _paste_ | _paste_ | |
| RSSI sentinels (RSSI==0) | _paste_ | _paste_ | |
| duplicate keys | _paste_ | _paste_ | |
| IR drops (unknown_sensor) | _paste_ | _paste_ | |
| **verdict** | | | _PASS / FAIL（全量重放后填 / fill after the full run）_ |

> **注 / note.** 因 EDA 无单日基线，本项与 V-M1-2、V-M1-5 合并在同一次全量重放中完成；
> 单日重放阶段（V-M1-1/-3 已 PASS）不填此表。

---

## V-M1-5 — Full-run stress

Replay all 165 days at k=3600; record throughput, backpressure, checkpoint durations, and cache state
size. Give the **DF-12 recovery surge** (the full-cluster outage recovery) its own paragraph.

```bash
bash deploy/scripts/syn-submit-m1.sh
bash deploy/scripts/syn-replay.sh --speedup 3600     # tmux session on master; ~1 hour wall time
bash deploy/scripts/syn-replay.sh status             # check periodically; safe to disconnect the Mac
```

| item | value |
|---|---|
| throughput (records/s) | _paste_ |
| max backpressure (Flink UI) | _paste_ |
| checkpoint durations (p50/p99) | _paste_ |
| cache state size | _paste_ |
| **DF-12 recovery surge** | _paste a paragraph: backlog depth, watermark behavior, whether the pipeline drained it without failure_ |
| **verdict** | _PASS / FAIL_ |

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
