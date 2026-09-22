# Report: the batch-size sweep produced a 12–30× drop in early-stopping loss
# — the step-A selection rule does not apply, and the V-M3-3 criterion needs a ruling

- **Date:** 2026-09-22
- **From:** the coding agent
- **To:** the design session
- **Re:** `handover_m3_addendum3_scope_and_batch_two_en.md` §2 (step A) and §5 (step D);
  `response_m3_plan_and_retrain_query_en.md` §1
- **Decisions requested:** two, in section 6. **Step D is not started** pending them.
- **Artifacts:** `docs/m3_batch_sweep.csv` (four rows, device E, hidden 60, window 60);
  code at `dev-claude` commit `f97e53a`.

---

## 1. Summary

Step A ran as specified. Two of its outcomes are as expected and one is not.

As expected: the batch-1 row **reproduces the 2026-09-21 reference exactly**, and batching
delivers close to the predicted speed-up ceiling.

Not as expected: batch sizes 16, 32 and 64 did not land *within 5%* of the reference — they landed
**92–97% below** it, a 12–30× reduction in early-stopping loss. The selection rule of §2 ("the
largest batch size whose early-stopping loss is within 5% relative of the batch-1 reference") was
written to bound a possible *degradation*. Every candidate passes it, but by an improvement of a
magnitude the rule did not anticipate, so the rule no longer discriminates between them.

Because the M3 anomaly score **is** the reconstruction error, a large across-the-board reduction in
reconstruction error is not self-evidently an improvement. That is the substance of this report.

## 2. Measurements

Device E, hidden 60, window 60, epoch cap 60, patience 10, OpenMP threads 1, one process, one dump,
one window split. Training set 1,001 windows (7 excluded by sanitization), early-stopping set 288
windows — identical in all four rows, so the split caliber is not a variable here.

| batch | early-stopping loss | rel. to reference | ratio vs reference | epochs | s/epoch |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.16060953 | −0.0003% | 1.00× | 26 | 79.6 |
| 16 | 0.00535281 | −96.67% | **30.0× lower** | 46 | 11.6 |
| 32 | 0.01273737 | −92.07% | **12.6× lower** | 56 | 9.2 |
| 64 | 0.00660448 | −95.89% | **24.3× lower** | 41 | 8.1 |

Three observations on this table.

**2.1 The batch-1 row is a clean regression check on step B.** Loss 0.16060953 against the
reference 0.160610, relative deviation −0.000003, and the same 26 epochs. Extracting the training
core into a pure function, rewriting the tensor construction and adding the mini-batch path changed
no numerical behaviour at batch 1.

**2.2 The loss is not monotone in batch size** (16 → 0.0054, 32 → 0.0127, 64 → 0.0066). A clean
batch-size effect would be ordered. This scatter suggests the four runs are not separated by a
systematic batch-size effect so much as by where each run happened to stop.

**2.3 The batched rows all train longer before early stopping** (41–56 epochs against 26). The
batch-1 run stopped early because it stopped improving, not because it had converged to a good
reconstruction.

**2.4 The predicted speed-up ceiling was confirmed.** Per the plan's §3.4, batching accelerates only
the training segment; the early-stopping evaluation remains 288 per-window forward passes. Under the
usual rule of thumb that a backward pass costs about twice a forward pass — *an assumption, not a
measurement* — the evaluation floor works out to roughly 7 s/epoch, and batch 64 measured 8.1. The
authorized evaluation-path batching (§1.1 of your response) should therefore still yield a further
several-fold gain; it is numerically equivalent and will ship with the equality test you specified.

## 3. What the architecture does and does not constrain

Verified from `LstmAutoEncoder.java:57-68`, the network is exactly what decision 4 of the M3
handover specifies:

| layer | type | shape | activation |
| --- | --- | --- | --- |
| 0 | LSTM | 5 → 60 | tanh |
| 1 | RnnOutputLayer | 60 → 5 | identity, MSE |

Both layers act **per time step**. There is no sequence-level encoding into a fixed-size vector, and
the hidden width is twelve times the input width. Parameter count is about 16,145 against 300,300
training scalars.

The consequence is factual and worth stating plainly: **nothing in this architecture forces the
model to discard information about the current input.** A reconstruction-error detector relies on the
model reconstructing normal data well and anomalous data poorly; whatever produces that asymmetry
here, it is not a capacity constraint on the path from x_t to y_t.

**Correction of our own document, not of yours.** The handover does not use the word "bottleneck";
that framing is mine. `docs/m3_lstm_design_zh.md` §2 stated that the encoder "compresses the
five-dimensional input into a hiddenSize-dimensional state". 5 → 60 is an expansion, not a
compression. That sentence is wrong and is being corrected. No part of this report depends on it.

## 4. Inference, labelled

**Verified:** everything in sections 2 and 3.

**Inference (moderate confidence):** at batch 1 the model underfits. One thousand and one Adam
updates per epoch at learning rate 0.01 is a high-variance regime; the run stalls at 26 epochs with
a loss an order of magnitude above what the same architecture reaches once gradients are averaged.
Batching stabilises the gradient, training continues for 41–56 epochs, and the model moves toward
reproducing its input.

**Inference (lower confidence, and the reason for this report):** if the model is moving toward
reproducing its input, it reproduces anomalous input too, and discrimination falls. Early-stopping
loss measures how well the model reconstructs; it does not measure how *differently* it reconstructs
normal and abnormal windows. Under V-M3-3's criterion — lowest early-stopping loss wins — the
selection would then favour the widest hidden size and the largest batch, which would be the wrong
direction for detection.

**Counterweight, stated for balance:** tanh saturation bounds the hidden state, so inputs far outside
the normalised range will still reconstruct poorly. Discrimination at the extremes probably survives
even if it degrades in the middle of the range. We have **not measured detection ability at any batch
size**, and this report does not claim that it has collapsed — only that the criterion in use cannot
tell us either way.

## 5. What we have not done

We have not run the full grid (step D). Running 27 combinations under a criterion that may rank
models by how faithfully they copy their input would consume roughly a day of cluster time and
produce a table that could not be acted on.

We have not changed the learning rate, the optimizer, the epoch cap or the patience, per §9 of the
addendum. We have not changed the architecture.

## 6. Decisions requested

**Q1 — the step-A selection.** Is the rule to be read as "any candidate not worse than 5% qualifies,
take the largest", which selects batch 64? Or does an improvement of this magnitude require the
selection to be deferred until discrimination is measured? We recommend the latter, for the reason in
section 4, but the criterion is yours.

**Q2 — the V-M3-3 criterion.** Should the offline grid continue to select (hidden, window) by lowest
early-stopping loss alone, given section 3? If a discrimination term is to be added, we need its
definition from you. We can supply evidence cheaply either way — see section 7.

A third matter is raised for the record rather than for decision now: whether the architecture itself
should be revisited (a narrower hidden state, or a genuine sequence-level encoding) is a design
question we are not equipped to settle and are not acting on.

## 7. A cheap measurement we can run to inform Q1 and Q2

The early-stopping set is deliberately **not** sanitized, so it already contains windows holding
M2-flagged outlier rounds. Splitting its loss in two — mean over clean windows, mean over
outlier-bearing windows — gives a separation ratio. A model with discrimination has a ratio well
above 1; a ratio collapsing toward 1 as batch size grows would confirm section 4's inference, and a
ratio that holds would refute it.

Cost: three extra CSV columns and one re-run of the same four rows, under an hour now that the
batched rows take five to nine minutes each.

Its limitation is real and we state it up front: M2 flags **point** anomalies while M3 targets
**contextual** ones, so this ratio is a coarse proxy and cannot replace V-M3-4's false-alarm rate or
V-M3-5's injection recall. It is offered as a fast indicator, not as a selection criterion — we would
not propose adopting it as one without your ruling.

Say the word and we will run it; otherwise we hold at the end of step A.
