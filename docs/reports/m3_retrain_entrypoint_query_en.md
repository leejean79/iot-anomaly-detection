# Query: the `retrain` entry point and `sanitizationStrength`

- **Date:** 2026-09-21
- **From:** the coding agent
- **To:** the design session
- **Re:** `handover_m3_addendum3_scope_and_batch_two_en.md` v1.0, sections 0 and 9
- **Status:** blocking for the scope of step B only. Steps A and D are unaffected and can
  proceed on the answer to Q1 alone.

---

## 1. The point needing clarification

Section 0 lists two entry points as owned by M3:

> `train(windows, config)` and `retrain(windows, sanitizationStrength)` — implemented as
> **pure functions** of their inputs.

Section 9 then states:

> No drift, retraining or alarm-weighting logic in M3.

I read these as compatible: M3 owns the *function* that retrains, while M6 owns the *policy*
that decides when to call it. Please confirm, because the alternative reading (that `retrain`
should not exist in M3 at all in any form) leads to different work.

The harder problem is the parameter. **`sanitizationStrength` is not defined anywhere** — not in
the M3 handover v1.1, not in addendum 1 or 2, and not in the code.

## 2. What sanitization is today, verbatim from the code

Training sanitization is currently **binary and has no notion of strength**. A training window is
either kept whole or discarded whole:

- `M3Function.java:238-246` sets `hasOutlier` true if **any** round in the window carries M2's
  outlier flag; `:253-261` then adds the window to the training set only when `hasOutlier` is
  false, otherwise increments an exclusion counter. `M3Grid` reproduces the same rule offline.
- The flag itself is a single boolean. `AnnotatedRound.java:22` declares `private boolean
  outlier`, and `PmcodFunction.java:167` fills it from set membership
  (`outlierSet.contains(mp.id)`). `ScoreEvent.java:21` likewise carries only `boolean outlier`.

**There is no continuous outlier score anywhere in the M2 → M3 path.** This is not an oversight
in the plumbing: MCOD's criterion is "at least k neighbours within radius R", which is a
predicate, not a magnitude. Surfacing any severity measure would require changing M2's side
output — which section 9 does not authorize.

## 3. Candidate readings of `sanitizationStrength`

I can construct at least four, and they differ materially in cost and in what they require.

**(a) A tolerated fraction of flagged rounds per window.** `0.0` excludes a window containing a
single flagged round (exactly today's behaviour); `1.0` excludes nothing. Implementable today
with no change outside M3, since the per-round flags are already buffered per window.

**(b) A severity threshold on the outlier score.** Not implementable today: no such score exists
(section 2). Requires an authorized change to `AnnotatedRound` and to `PmcodFunction`, and first
a decision on what MCOD quantity would serve as the score.

**(c) A soft weight rather than a hard exclusion.** Instead of dropping a suspect window,
down-weight its flagged time steps through the label-mask mechanism already used for censored
Light readings (`LstmAutoEncoder.java:96` and `toMaskArray`). Implementable today, but it changes
the loss semantics and would make retrained models not directly comparable to cold-start models
under the 5% parity rule of section 6.

**(d) Something else**, for instance a strength expressed over the retraining *window span*
(how much recent history to admit) rather than over outliers at all.

## 4. Questions

**Q1.** Is `retrain` required in batch one at all, or is it a signature to be specified in
Addendum 4 together with M6? If the latter, I will implement only `train(windows, config)` now,
as a pure function shared by `M3Grid` and `M3Function`, and leave `retrain` out entirely.

**Q2.** If `retrain` is required in batch one: please define `sanitizationStrength` — its type,
its range, its semantics, and the value that reproduces today's behaviour exactly. Reading (a)
is the only one of the four that is both implementable inside M3's current boundary and
reducible to today's behaviour at one end of its range.

**Q3.** If the intended reading is (b): please authorize the corresponding M2 change and state
which MCOD quantity is to be exported as the score. Without that authorization, (b) cannot be
built within the boundaries of section 9, and I will not attempt a workaround.

## 5. What I am doing in the meantime

Implementing `train(windows, config)` as a pure function of its inputs, touching no Flink state,
called by both the offline grid and the online cold-start path so that the two cannot diverge —
this is required in any case by section 6's parity item and by the purity constraint of section 0.

I am **not** implementing `retrain`, on the grounds that it has no caller and no defined
semantics, and that guessing at them would be speculative code. If this is the wrong call,
say so and I will follow Q2's definition instead.
