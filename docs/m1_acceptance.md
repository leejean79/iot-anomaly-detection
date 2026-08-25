# M1 Stage Acceptance — Replayer / Ingestion / Round Assembly / Normalization / Raw Cache

> **Status: TEMPLATE.** The five acceptance hooks (V-M1-1..5) run against the **live cluster**
> and must be executed by the operator on the master node; this file gives the exact commands
> and a table to paste the results into. The offline correctness of every operator is already
> covered by the unit tests (`mvn test`, 16 tests: parser guards, keep-first dedup, round-close
> timer with missing mask, warm-up freeze boundary, IQR bypass, RoundWindow, keyed isolation),
> and the replayer's global-merge / idle-compression logic by `ReplayerLogicTest` + a dry-run.

## 0. Prerequisites (handover §2)

```bash
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
bash deploy/scripts/syn-submit-m1.sh            # submit M1Job, waits for RUNNING
bash deploy/scripts/syn-replay.sh --speedup 600 --start <day> --end <day>   # foreground replay
```

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
| selected day | _paste_ |
| rounds on synergia-m1-out | _paste_ |
| EDA round count for that day | _paste_ |
| difference & explanation (known gaps) | _paste_ |
| **verdict** | _PASS / FAIL_ |

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
| watermark tracks event-time axis? | _paste_ |
| idle-compress events vs known outages | _paste_ |
| device H solo outage did NOT compress? | _paste_ |
| 102-hour full-cluster outage DID compress? | _paste_ |
| **verdict** | _PASS / FAIL_ |

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
| N sampled | _paste_ |
| OK / MISMATCH / UNMATCHED | _paste_ |
| **verdict** | _PASS (MISMATCH==0) / FAIL_ |

---

## V-M1-4 — Guard reconciliation

Within the chosen segment, the counters for **censored Light**, **RSSI sentinels**, **duplicate keys**,
and **IR drops** must match the EDA's known counts for that segment. Read them from the monitoring
snapshots (summed over the segment) or from the Flink/Prometheus metrics.

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
| **verdict** | | | _PASS / FAIL_ |

---

## V-M1-5 — Full-run stress

Replay all 165 days at k=3600; record throughput, backpressure, checkpoint durations, and cache state
size. Give the **DF-12 recovery surge** (the full-cluster outage recovery) its own paragraph.

```bash
bash deploy/scripts/syn-submit-m1.sh
bash deploy/scripts/syn-replay.sh --speedup 3600
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

## Notes / data facts to report back (handover §7)

Any measured fact that contradicts the handover is reported to the design session, not patched around.
Record candidates here:

- _e.g., unknown DeviceIds observed (replayer "Unknown-device records" > 0)_
- _e.g., malformed-line count vs EDA_
- _..._
