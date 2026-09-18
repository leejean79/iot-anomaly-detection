# Finding: M2 windows only half of each round stream (timestamp offset between window assignment and MCOD admission)

> **From:** the coding agent. **To:** the design session.
> **Trigger:** full-March baseline runbook (v1.0, 2026-09-16), section 5 — per-device equality check failed on all eight devices; section 8 — "identify the exact mechanism and report before any M3 change touches M2".
> **Status:** mechanism identified and reproduced; **no code changed, nothing tuned**. Baseline recording is paused pending a ruling.

---

## 1. Summary

The joint M2 job admits every round into the M2 window operator, but **MCOD inserts only the rounds whose timestamp falls in the first 30 s of each 60 s slide bucket; the other half never enter MCOD state.** Every online M2 statistic — outlier rate, micro-cluster occupancy, neighbour-count quantiles — has therefore been computed over roughly half the points that a 3600 s window should hold. The offline calibration probe is unaffected, which is why the probe and the job disagree by a factor of two on points per window (356 versus 175) and why every device's online outlier rate is +92 % to +272 % above its probe value.

This is **not a regression** from the M3 or Java 11 work. It has been present since the M2 job was assembled, and it was never visible because no acceptance step before this runbook compared job-side per-device rates against the probe.

## 2. Evidence (four independent lines)

| # | Line of evidence | Result |
|---|---|---|
| 1 | Cluster counters: `m2_points_total / m2_gate_admitted` | 46,936,182 / 1,565,797 = **29.98** (a 3600/60 window should give 60) |
| 2 | Cluster snapshots: mean points per window, every device | **175.4** (probe at the same window: 356) |
| 3 | Code: `RoundAssembler.ROUND_CLOSE_DELAY_MS` | **30_000 ms**; rounds are emitted in `onTimer` at `roundTs + 30 s` |
| 4 | Sandbox reproduction with the real `PmcodFunction`, Flink timestamp = `arrival + δ` | δ = 0 → 60.00 / 331 pts; **δ = 30 s → 30.00 / 165 pts**; δ = 10 s → 50.00 |

Lines 1 and 2 were identical across two independent replays (bit-for-bit), which rules out data or replay variance. Line 4 reproduces line 1 exactly.

## 3. Mechanism

Timestamps are assigned once, at the Kafka source (`M2Job.java:158-164`, `kafkaTs`). `RoundAssembler` closes a round on an event-time timer registered at `roundTsMs + ROUND_CLOSE_DELAY_MS` and emits the `DeviceRound` from `onTimer` (`RoundAssembler.java:76, 133, 183`). A record emitted from `onTimer` carries the **timer's** timestamp, so every `DeviceRound` reaches the M2 window with Flink timestamp = `roundTs + 30 s`. `RobustScaler` and `RawCache` pass through in `processElement` and preserve it.

`M2Gate` builds `McodPoint.arrival = round.getTs() × 1000` (`M2Gate.java:75-77`) — the round's **own** timestamp, 30 s earlier than the Flink timestamp used to assign it to windows.

`PmcodFunction` inserts a point into MCOD state only when `p.arrival >= windowEnd − slide` (`McodCore.java:64`). A point is assigned to the windows whose `[ws, we)` contains `arrival + 30 s`; it satisfies the insert test only in a window with `we − 60 s ≤ arrival < we`. Both hold together only when `(arrival mod 60 s) < 30 s`. For the other half of the rounds no firing window ever satisfies the insert condition, and the point is silently dropped from MCOD — it is still counted by `m2_gate_admitted`, still present in the Flink window's `elements`, but never enters `state.pd` or any micro-cluster.

In general the fraction never inserted is `δ / slide`; with δ = 30 s and slide = 60 s that is exactly one half. Deletion is unaffected (points that were inserted are removed at the correct window), so inserted points still see 60 windows and `sum(windowPoints)/admitted = 60 × (1 − δ/slide) = 30`.

The offline probe (`M2Probe.java:124-422`) sorts by `arrival` and derives `windowStart`/`windowEnd` from `arrival` itself — δ = 0 — so it inserts every point. That is the "probe versus job harness difference" section 5 of the runbook anticipated, but it is a defect in the job, not an acceptable difference.

## 4. Not a regression

Non-comment changes to the five relevant files between the M2 close-out commit `2fa8d10` and `HEAD`:

| File | Non-comment lines changed | Nature |
|---|---|---|
| `RoundAssembler.java` | 0 | — |
| `McodCore.java` | 0 | — |
| `M2Gate.java` | 4 | adds the censored mask to `DevicePoint`; timestamps untouched |
| `PmcodFunction.java` | 31 | M3 annotated-round side output; proven inert by `PmcodM3FlagEquivalenceTest` |
| `M2Job.java` | 83 | M3 wiring; timestamp assignment untouched |

`ROUND_CLOSE_DELAY_MS = 30_000L` is present in the earliest version of `RoundAssembler.java` in this repository. The replayer's timestamp stamping is likewise unchanged since `2fa8d10`, and the M1 side of this run reconciles exactly with the Java 8 baseline (2,049,816 rounds, all four integrity assertions PASS), which independently exonerates the replayer and the whole M1 chain.

## 5. Why it was never caught

- V-M2-1 (equivalence tests) exercise `McodCore` directly with δ = 0.
- V-M2-2 asserted counter closure and `scores > 0`, not a per-device rate comparison.
- V-M2-3 calibrated (R, k) with the **offline** probe (δ = 0); the resulting radii are therefore sound in themselves.
- V-M2-4 measured throughput, checkpoints and state size; the online outlier ratio was recorded but had nothing to be compared against.
- `PmcodM3FlagEquivalenceTest` and every sandbox harness assign the Flink timestamp from `arrival` (δ = 0), so they reproduce the probe, not the job.

The first step that ever compared the job's per-device rates with the probe's is this runbook's section 5. It failed on the first run.

## 6. Scope of impact

Every online M2 result derived from `synergia-monitoring` or `synergia-scores` was computed over ~half the points per window and with the per-device radii applied at half the intended density:

- `docs/m2_acceptance.md` — V-M2-2 and V-M2-4 online figures (outlier ratio 2.1 %, MC occupancy 93.8 %).
- `docs/m2_df12_surge.md`, `docs/m2_surge_stats.csv`, `docs/m2_surge_timeline.csv` — the June DF-12 recovery-surge quantification.
- `docs/m2_hod_G_before.csv`, `docs/m2_hod_G_after.csv` — hour-of-day analyses for device G.
- The M1 monitoring snapshots (`MonitoringAggregator`) also key their 60 s windows on `ctx.timestamp()`, so their window labels are phase-shifted by +30 s; counts are unaffected.

**Unaffected:** the offline probe tables (`docs/m2_probe*.csv`), the per-device radius calibration built on them, the M1 acceptance, `M3Function` (windows by round count, not by time), and everything in the Java 11 migration report except section 4.3's M2 half.

## 7. What remains valid from the two March runs

- Four integrity assertions PASS (round count +0.124 % versus EDA, zero duplicates, boundaries aligned, freeze on day 8 for all eight devices).
- `m1_assembler_rounds_total = 2,049,816`, identical to the clean calibration run — the reproducibility observation of clarification 2 is a perfect reproduction.
- Gate identity closes: `1,565,797 = 2,049,816 − 483,840 − 179`; late drops 0.

Only the M2 per-device figures are void.

## 8. Fix options — for the design session to rule on

| Option | Change | Effect | Trade-off |
|---|---|---|---|
| **A** | Re-assign event timestamps to `round.getTs() × 1000` on the `DeviceRound` stream after `RawCache`, with a fresh bounded-out-of-orderness strategy | Flink timestamp = `arrival` everywhere downstream; also realigns the M1 monitoring window labels | Touches watermark generation; needs the out-of-orderness bound re-derived (rounds already arrive in order per key, so 55 s remains safe) |
| **B** | In `M2Gate`, set `McodPoint.arrival = ctx.timestamp()` (the Flink timestamp) instead of `round.getTs() × 1000`; keep `id = round.getTs()` | MCOD's admission test and the window assignment use the same clock; `arrival` becomes "close time" (+30 s uniformly), which is invisible to MCOD since it only compares arrivals against window bounds | One-line change; M1 snapshot phase unchanged; `arrival` no longer equals the round's nominal time (documentation only) |
| C | Make `PmcodFunction` admit by window bounds instead of `arrival` | Not available: a `ProcessWindowFunction` receives values without per-element timestamps | — |

Either A or B should ship with a regression test that feeds the real `PmcodFunction` with Flink timestamp = `arrival + 30 s` and asserts `sum(windowPoints)/admitted == 60` — the exact case every existing harness leaves at δ = 0. The coding agent's recommendation is **B** for the smallest blast radius, unless the design session wants the Flink timestamp to equal the round timestamp for every downstream consumer, in which case **A**.

## 9. Consequences to decide

1. Whether prior online M2 conclusions (section 6) must be re-derived after the fix, or annotated as "computed at half density".
2. Whether the per-device radii (calibrated offline at full density) are to be applied unchanged once the job runs at full density — the coding agent expects the online rates to land on the probe values within the 1 % tolerance, which would close section 5 of the runbook.
3. Whether the fix is authorised before the M3 forwarding change touches M2 (runbook section 8 ordering).

## 10. Boundaries respected

No code or configuration was changed during or after the run; no parameter was tuned. Two temporary sandbox probes were written to reproduce the mechanism and deleted afterwards; the working tree is clean. The `synergia-*` topics and the local monitoring dump from the last run are retained so the finding can be re-verified.
