# M2 Baseline on Java 11 — Full March 2022

> **Baseline for M2 on Java 11 with the final configuration; the pre-change datum for the M3
> forwarding modification.**
>
> | Provenance | Value |
> |---|---|
> | Commit | `<git rev-parse HEAD>` |
> | Jar SHA-256 | `<shasum -a 256 target/iot-anomaly-detection-1.0-SNAPSHOT.jar>` |
> | Jar on cluster | `/opt/fa-iforest/jars/iot-anomaly-detection-1.0-SNAPSHOT.jar` |
> | M3 forwarding | **disabled** — `--m3-enabled false` (`m3AnnotatedTag` is null; the branch is inert) |
> | Safeguard test | `com.leejean.m2.PmcodM3FlagEquivalenceTest#togglingTheM3TagLeavesEveryM2OutputIdentical` — asserts the ScoreEvent list, every MonitoringSnapshot field and every `m2_*` counter are identical with the tag null and with the tag set |
> | Image | `fa-iforest/flink:1.13.6-java11` |
> | Runtime | Flink `<curl :8081/config>`, TaskManager `java.version <11.x>` |
> | Build JDK | `<java -version on the build machine>` |
> | Test suite | `<Tests run: 75, Failures: 0, Errors: 0, Skipped: 0>` |
> | Replay window | 2022-03-01T00:00:00Z .. 2022-04-01T00:00:00Z, `--speedup 3600` |
>
> Written per the design session's runbook *Full-March Replay on Java 11 — Recording the M2 Baseline*
> (v1.0, 2026-09-16) and its two clarifications of 2026-09-17.

---

## 1. Preconditions

### 1.1 Configuration in force

| Setting | Value |
|---|---|
| `k` (all devices) | 10 |
| `SYN_M2_R_PER_DEVICE` | `<dump from .env>` |
| `--calib-days` | 7 |
| Light channel | log1p pre-transform **on** |
| Relative-IQR guard | **off** (retracted) |
| Absolute IQR guard | on |
| M2 window / slide / allowed lateness | 3600 s / 60 s / 0 s |
| `taskmanager.memory.task.off-heap.size` | 768 MB |
| `taskmanager.memory.managed.size` | 256 MB |
| `javacpp.maxbytes` / `maxphysicalbytes` | 512 MB / 3584 MB |
| `javacpp.cachedir` | `/tmp/javacpp-cache` |

### 1.2 Disk before the run

| Node | Filesystem | Size | Used | Avail | Use% |
|---|---|---|---|---|---|
| master | | | | | |
| worker1 | | | | | |
| worker2 | | | | | |

### 1.3 Clean start

`curl :8081/jobs` showed no RUNNING job; the replayer offset file was deleted; `syn-clean-topics.sh`
was run and every `synergia-*` topic reported a zero end offset. No Java 8 checkpoint was restored.

---

## 2. Replay

The joint `M2Job` was submitted before the replayer started, and it was the **only** job running:
it already contains the full M1 chain, so `M1Job` must not run alongside it (both consume
`synergia-source` and both write `synergia-m1-out`). Job id: `<jid>`.

The replayer ran in a detached `tmux` session. `--max-idle-wall` was **not** passed, so the default
2000 ms applied — the runbook's literal `--max-idle-wall 2` would have meant 2 **milliseconds**,
since that parameter is expressed in milliseconds.

```
<paste the replayer's Finished. summary block and the exit line here>
```

| Item | Value |
|---|---|
| Produced (sent) | |
| Send errors | |
| Skipped malformed lines | |
| Data files (csv/sniffed) | |
| Unknown-device records | |
| File read errors | |
| Idle-compression events | |
| Total compressed wall | |
| Exit code | |
| Wall time | |

---

## 3. Integrity check — the four assertions

`syn-replay-verify.sh --tol-pct 0.2` over 2022-03-01T00:00:00Z .. 2022-04-01T00:00:00Z, with
`--expected-total` taking the EDA authority value **2,047,283** (assertion (a) is measured-versus-EDA)
and an explicit ±0.2% tolerance covering the known +0.12% global-merge effect.

| # | Assertion | Result |
|---|---|---|
| a | Consumed round count reconciles with the EDA March total (±0.2%) | |
| b | Zero duplicate (device, timestamp) records | |
| c | Earliest and latest timestamps on the segment boundaries | |
| d | Each device's scaler freeze time falls on day 8 (2022-03-08) | |

Per-device freeze times:

| Device | A | B | C | D | E | F | G | H |
|---|---|---|---|---|---|---|---|---|
| Freeze | | | | | | | | |

```
<paste the script's stdout here>
```

**Reproducibility observation (not an assertion).** The clean calibration run recorded exactly
2,049,816 rounds. This run recorded `<N>` — difference `<Δ>` (`<Δ%>`). `<state whether it reproduces
and what you observe>`.

---

## 4. Counters

### 4.1 Gates and the reconciliation identity

| Counter | Value |
|---|---|
| `m2_gate_admitted` | |
| `m2_gate_warmup_bypass` | |
| `m2_gate_missing_bypass` | |
| `m2_gate_censored_entered` | |
| `m2_gate_coldstart_clear` | |
| `m2_gate_late_drop` | |

`admitted = rounds − warmup − missing` → `<closes at zero / does not close>`

### 4.2 M2 totals

| Counter | Value |
|---|---|
| `m2_outliers_total` | |
| `m2_points_total` | |
| `m2_windows_total` | |
| `m2_mc_points_total` | |
| `m2_state_cold_clears` | |
| Overall ratio (point-weighted) | |

### 4.3 M1 counters — emitted by the M1 segment inside the joint M2Job; the Java 11 M1 baseline for a full month, as a by-product

| Counter | Value |
|---|---|
| `m1_assembler_rounds_total` | |
| `m1_assembler_incomplete_rounds` | |
| `m1_assembler_dup_keys` | |
| `m1_scaler_warmup_rounds` | |
| `m1_parser_censored_light` | |
| `m1_parser_rssi_sentinel` | |
| `m1_parser_unknown_sensor` | |

---

## 5. Per-device figures and the Java 8 equality check

**Source:** the `synergia-monitoring` snapshots emitted by `PmcodFunction` (M2 snapshots carry
`windowEnd > 0`; M1 snapshots carry 0 and are filtered out), aggregated by
`deploy/scripts/m2_device_baseline.py`.

**Metric definition (confirmed to match the probe's).** `meanOutlierRate` is the per-slide ratio
(outliers in the window ÷ points in the window) averaged arithmetically over all slides — the same
definition `M2Probe` uses. It is **not** `m2_outliers_total ÷ m2_points_total`, which is
point-weighted; that figure is reported separately in section 4.2 and the two must not be conflated.

**Java 8 reference:** each device's value at its final radius, k = 10, read from
`docs/m2_probe_7d_clean.csv` (the clean seven-day calibration run).

<!-- paste docs/reports/m2_java11_march_per_device.md here -->

![per-device mean outlier rate, Java 8 versus Java 11](m2_java11_march_rate_cmp.svg)

**Verdict:** `<all eight devices within ±1% relative / device X deviates by Y%>`

**Deviation analysis (only if any device is outside tolerance):** `<identify the exact mechanism —
probe harness versus job, window-boundary handling, calibration cut-over — and state it. Do not tune.
An unexplained deviation is a finding for the design session and stops any M3 change touching M2.>`

---

## 6. Timing, checkpoints and memory

| Item | Value |
|---|---|
| Replay wall time | |
| Drain time after the replayer finished | |
| Checkpoints completed / failed | |
| Checkpoint duration (min / avg / max) | |
| Checkpointed state size (max per subtask) | |
| TaskManager `physicalBytes` at the busiest phase | ` / 3584 MB ceiling` |
| TaskManager `heapUsed` at the busiest phase | |

---

## 7. Cleanup

| Node | Avail before | Avail after |
|---|---|---|
| master | | |
| worker1 | | |
| worker2 | | |

Topics cleaned with `syn-clean-topics.sh --yes`; replayer offset file deleted; both jobs cancelled.
The old FA-iForest containers, jar and topics were not touched at any point.

---

## 8. Optional per-device per-day score aggregate

`<counts only, never the raw records; omit this section if the design session does not ask for it>`
