# Decision brief: the March baseline is blocked by a transient warm-up checkpoint peak

> **From:** the coding agent. **To:** the design session.
> **Date:** 2026-09-18. **Status:** blocked, awaiting a ruling. No code or configuration changed since the M2 addendum fix; nothing tuned.
> **Context:** M2 Addendum — Timestamp Alignment Fix (Option A), section 3 step 2 (the March baseline) and section 7 (boundaries).
> **What is asked:** pick one of options A / B / E in section 6, and rule on the section 7 boundary question.

---

## 1. Summary

The Option A timestamp fix **works and is validated**. The March baseline nevertheless cannot be recorded, because the job restarts mid-run for an unrelated reason: during the seven-day calibration warm-up, one subtask's checkpoint payload reaches **26.1 MB**, which exceeds Flink's `akka.framesize` (default **10 MB**, never changed in this cluster). Checkpoints are declined, the tolerable-failure threshold is crossed, the job restarts, and the at-least-once Kafka sink re-emits **25,517 duplicate rounds** into `synergia-m1-out` — which fails integrity assertion 2 and voids the run under the runbook's section 8.

The peak is **transient**: it exists only while the `RobustScaler` reservoir is accumulating. Once the reservoir freezes on day 8 it is cleared, and the whole job checkpoints at **9.40 MB across all eight subtasks** (≈1.17 MB each). A ruling is needed because the three ways out differ in whether they touch the shared cluster, whether they keep fault tolerance, and whether one of them crosses the addendum's section 7 boundary.

---

## 2. What the fix achieved (independent of the blocker)

These results come from the March run and are unaffected by the restart. They validate Option A and can be recorded as they stand.

| Check | Before the fix | After the fix |
|---|---|---|
| Points per window vs the offline probe, all eight devices | 175.4 vs 356.2 — ratio **0.500** | 356.5 vs 356.2 — ratio **1.001** (H 1.000) |
| `m2_points_total ÷ m2_gate_admitted` | 29.98 (expected 60) | 60 — the window-geometry warning no longer fires |
| Integrity assertion 3, boundary alignment | PASS | PASS |
| Integrity assertion 4, freeze on day 8 | never reached on a valid run | **PASS on all eight devices**, every freeze on 2022-03-08 |
| `m2_gate_late_drop` | 0 | 0 |

Assertion 4 passing is itself meaningful: it shows the event-time re-alignment did not disturb the calibration timing.

Unit coverage shipped with the fix remains green (79/79), including `PmcodTimestampOffsetTest` (pins the 60 / 50 / 30 response at offsets 0 / 10 s / 30 s) and `M1M2TimestampAlignmentTest` (job-level, mutation-verified).

---

## 3. The blocker, with evidence

### 3.1 The failure chain

```
Decline checkpoint 79 by task 5efafa7f… at 172.16.0.164
  Caused by: The rpc invocation size 27329480 exceeds the maximum akka framesize.
→ org.apache.flink.util.FlinkRuntimeException: Exceeded checkpoint tolerable failure threshold.
→ Job df4b689e… switched from RUNNING to RESTARTING at 13:41:13, back to RUNNING at 13:41:14.
```

27,329,480 bytes = **26.1 MB**, against a 10 MB limit — a factor of 2.6.

### 3.2 The duplicates follow from the restart

| Quantity | Value |
|---|---|
| Rounds on `synergia-m1-out` | 2,075,333 |
| Distinct rounds (previous run, and the clean calibration run) | 2,049,816 |
| Difference | +25,517 |
| Integrity assertion 2, duplicate (device, timestamp) | **25,517 — FAIL** |
| Post-restart counter `m2_gate_admitted` | 25,518 |

The counters confirm the restart independently: `m1_scaler_warmup_rounds = 0` while `m2_gate_admitted = 25,518`. A fresh job must count its first 60,480 rounds per device as warm-up, so zero is impossible — the counters were reset by a restart while the scaler's frozen state survived in the checkpoint. The **distinct** round count is therefore still 2,049,816, identical to the clean calibration run; the excess is purely at-least-once re-emission.

`m1_assembler_dup_keys = 0`, so round assembly itself is sound.

### 3.3 The peak is confined to the warm-up window

A **successful** checkpoint, taken after the reservoir froze:

| Operator | Checkpointed size (all 8 subtasks) |
|---|---:|
| Source → RawLineParser | 8.36 KB |
| RoundAssembler → AlignEventTime | 41.7 KB |
| **RobustScaler** | **46.1 KB** |
| RawCache → (m1-out sink, M2Gate) | 402 KB |
| Pmcod → (scores sink, monitoring sink, M2LateDrops) | 8.89 MB |
| MonitoringAggregator → sink | 19.6 KB |
| **Total** | **9.40 MB** (≈1.17 MB per subtask) |

`RobustScaler` shows 46.1 KB here because `RobustScalerFunction` clears the reservoirs at the freeze boundary. The failing checkpoint 79, at a 10 s interval, falls roughly 790 s into the run — inside the accumulation phase, before that clear. This corrects an earlier assessment of mine that called the risk inherent to the whole month; it is inherent to the **warm-up window** only.

### 3.4 Why the payload is 26 MB

`RobustScalerFunction` holds `List<ListState<Double>> reservoirs` — one `ListState<Double>` per channel, values added one at a time and boxed:

```java
private transient List<ListState<Double>> reservoirs;   // one reservoir per channel
reservoirs.get(c).add(transforms[c].apply(round.getX()[c]));
…
reservoirs.get(c).clear();   // released at the freeze boundary
```

Per device the reservoir holds `--calib-days 7 × 8,640 rounds/day = 60,480` rounds × 5 channels = **302,400 boxed entries**. At the observed 26.1 MB that is ≈91 bytes per entry, consistent with boxed `Double` plus per-element list-state framing.

### 3.5 A misleading configuration worth noting

`M1Job` and `M2Job` both set `JobManagerCheckpointStorage(--checkpoint-max-state-mb × 1 MB)`, default **128 MB**, raised from Flink's 5 MB default after the March 2022 incident. That figure has **never been the binding constraint**: `akka.framesize` caps the acknowledge RPC at 10 MB regardless. The 5 → 128 MB change appeared to work only because the state at the time happened to fit under 10 MB.

---

## 4. Scope of what is blocked

- Addendum section 3 step 2 (the March baseline) cannot be recorded; it is the pre-change datum for the M3 forwarding comparison, so by section 5 nothing in M3 is enabled until it exists.
- Steps 3 and 4 (the June surge and device-G hour-of-day re-runs) depend on step 2 and are not started.
- The residual per-device deviation seen in the voided run (a near-constant additive −0.0058 percentage points on seven devices, ≈2 saturated windows' worth, H ≈ 0) is **not** analysed here: it was measured on a contaminated run. It is noted only so the design session knows it exists and will be re-examined on a clean run.

---

## 5. What is not in question

- The replayer and the whole M1 chain: source partitions reconcile with the EDA March record counts device by device (+0.138 % on seven, exactly 0 on H), and the distinct round count reproduces the clean calibration run exactly.
- `McodCore`, the per-device radii and the calibration procedure: untouched, and the offline probe tables remain correct.
- The Option A fix: validated as in section 2.

---

## 6. Options

| | Change | Touches the shared cluster | Keeps fault tolerance | Fixes the cause | Crosses section 7 |
|---|---|---|---|---|---|
| **A** | Raise `akka.framesize` (e.g. to 64 MB) | **Yes** — cluster-wide setting; the JobManager is shared with the FA-iForest project and must be restarted, which drops every job on it (there is no JobManager high availability) | Yes | Yes | No |
| **B** | Disable checkpointing for baseline runs (`--checkpoint-ms 0`, a new "0 disables" branch in the jobs) | No | **No** — a mid-run failure loses the whole run; the at-least-once Kafka sink's flush behaviour without checkpoints needs confirming | No — the state still peaks, it is simply never snapshotted | No |
| **E** | Store the reservoir as one `double[]` per round instead of boxed `Double` per channel value | No | Yes | **Yes** — estimated ≈3.2 MB, under the 10 MB limit | **Needs a ruling — see section 7** |

Option C (longer checkpoint interval, higher tolerable-failure count) only delays the failure: the state stays large for the whole warm-up, so every attempt in that window fails. Option D (shrink the reservoir by shortening `--calib-days`) is excluded by section 7, which forbids changing the calibration.

**Coding agent's recommendation: E, with B as the no-code-change fallback if the design session wants to re-run immediately.** E removes the cause, leaves the shared cluster alone and keeps fault tolerance; its cost is one storage-representation change under test. A is not recommended: restarting a JobManager shared with another project, to accommodate a peak that exists only during warm-up, is disproportionate.

---

## 7. The boundary question the design session must settle

Addendum section 7 forbids changing "the calibration procedure". Option E changes **how the reservoir is stored**, not what is computed: the per-channel median and IQR, the freeze instant, the bypass decisions and every emitted value are bit-for-bit unchanged, because the same sample values are appended in the same order and read back in the same order.

The coding agent's reading is that a serialization-form change is not a change to the calibration procedure — but that line should be drawn by the design session, not assumed.

If E is authorised, it ships with a regression test asserting that, for one identical input stream, the old and new storage forms produce identical per-channel medians, IQRs, freeze round indices and bypass flags — turning "the computation is unchanged" from a claim into an executable assertion, in the same style as the two tests delivered with the Option A fix.

---

## 8. A second, smaller ruling requested

Whichever option is chosen, `--checkpoint-max-state-mb 128` remains misleading: the effective ceiling is `akka.framesize`. Options: lower the default to the true ceiling, keep it but document the relationship at the call site and in `docs/flink_memory_tuning_zh.md`, or leave it. The coding agent suggests documenting it at both call sites, since the number is also what a future operator would reach for when a checkpoint fails.

---

## 9. State of the cluster and the artifacts

The `synergia-*` topics and the local monitoring dump from the voided run are retained so any of this can be re-verified. `syn-clean-topics.sh` has not been run. The baseline report `docs/reports/m2_java11_march_baseline.md` carries a hold notice. No parameter was tuned at any point.

Once a ruling arrives: if E, the change and its test are delivered first, then the four re-runs proceed in the addendum's order; if B, the re-runs can begin immediately.
