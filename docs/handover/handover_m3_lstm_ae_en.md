# Handover: M3 Stage — Shallow LSTM Autoencoder (Contextual Anomaly Channel)

(English edition v1.1 — operative; supersedes the Chinese v1.0 issued the same day)

**From:** the design session (per `dev_design_log_en` v0.36 and the eight approved M3 decisions;
theory document: M3 module card, decision D12, validation protocol V6, archive chapter 18).
**To:** the coding agent.

**Ground rules:** follow the repository `CLAUDE.md` (Java 8, Maven, package root `com.leejean`,
bilingual code comments, five-element script delivery). All documents and reports are written in
English (user ruling 2026-09-10), in full explanatory sentences; annotate every abbreviation at
first occurrence; whenever a report describes a data distribution or a statistical phenomenon,
include a figure. Run the replay-integrity check (the four assertions established at M2 closeout)
before every cluster run. When this document cites a FA-iForest template class, copy the pattern;
do not import its history.

## 1. The task in one sentence

Build, per device, a shallow Long Short-Term Memory (LSTM) autoencoder that is trained inside the
Flink operator on normal data only, produces a contextual anomaly score from its reconstruction
error into the scores topic, and forwards the error series to the monitoring topic for the future
performance-layer drift detector; additionally deliver a minimal injection mode in the replayer so
that detection capability can be demonstrated.

## 2. First task: the compatibility smoke test is the gate (decision 1)

Pin Deeplearning4j 1.0.0-M2.1: dependencies `deeplearning4j-nn` and `nd4j-native` (the native
backend of the ND4J tensor library), the latter with the explicit `linux-x86_64` classifier, never
the all-platform bundle. The smoke test has three parts: (1) on JDK 8, add these dependencies to
the existing fat jar, verify there are no conflicts with Flink's own dependencies (Jackson, Guava,
Protobuf and friends), and report the jar size; (2) in a MiniCluster test, train and run a tiny
LSTM inside a keyed `KeyedProcessFunction`, proving that training and inference work on the
operator thread; (3) on the real cluster, verify that the TaskManager's off-heap memory budget
(`taskmanager.memory.task.off-heap.size`) is sufficient for the tensor library and that the
native-library extraction directory is writable by container user 9999 (if not, point
`org.bytedeco.javacpp.cachedir` at a writable mounted directory). If any part fails, stop and
report back to the design session; do not force a workaround in code. The fallback (a simpler
reconstruction model on Smile or JSAT) is the design session's decision.

## 3. Deliverable A — the M3 operator (package `com.leejean.m3`)

**Wiring, including one authorized change to M2.** Initial-training sanitization (decision 3)
needs the point channel's outlier flags. The change: at the end of every slide, the M2 operator
forwards the rounds of that slide annotated with their outlier flag as its main output stream (the
existing outlier-list output stays). M3 consumes this annotated stream. The cost is up to one slide
(60 seconds of event time) of extra latency on the M3 path; this is accepted for the current stage
and must be measured and reported for future high-frequency review.

**State machine** (decision 2, from the `LocalProcessorFunction` template). Each device walks
through: collecting (after the scaler warm-up, accumulate the training set), training (synchronous;
the device's operator pauses processing during training), calibrating (choose hyperparameters and
the early-stopping point on the early-stopping set; estimate the score distribution on the
threshold-calibration set), online (inference and scoring). A retraining entry point exists but no
trigger logic is written in this stage.

**Data segments** (decision 3). After the scaler's seven-day warm-up: seven days of training, two
days of early stopping, two days of threshold calibration — parameters `--m3-train-days`,
`--m3-earlystop-days`, `--m3-thresh-days`. Training-set sanitization: any window containing a round
flagged as an outlier by the point channel is excluded from the training set; the excluded fraction
is reported per device.

**Model and hyperparameters** (decision 4). One LSTM layer in the encoder and one in the decoder;
five input dimensions; the hidden size is chosen automatically per device on the grid {40, 60, 90}
by the early-stopping-set reconstruction error (no labels of any kind); window length defaults to
60 rounds (ten minutes) and is instantiated from M1's `RoundWindow` class inside the operator; the
window-length grid {30, 60, 120} is explored only in the offline acceptance experiment (section 5);
fixed random seed; maximum epochs and early-stopping patience are parameters.

**Loss function** (decision 5). Element-weighted mean squared error: weight zero for missing
entries and for censored entries; a device × channel weight table `SYN_M3_CHANNEL_WEIGHTS` (default
all ones). In this stage device G's Light weight is zero and its input is zeroed too, with a
configuration comment "temporary; restore once M6's level-0 re-estimation fixes the scale".
Per-window reconstruction errors are kept per channel.

**Scoring and threshold** (decision 6). Main score: the window's weighted mean squared error
standardized on the threshold-calibration set as (error − median) ÷ IQR; alarm threshold default
2.22 (`--m3-z-threshold`) — this is three standard deviations converted into interquartile-range
units (for a normal distribution one standard deviation is about 0.741 IQR, so three of them are
2.22). In parallel, compute the Mahalanobis score on the error vector restricted to unmasked
channels, using a covariance matrix estimated on the calibration set with a small ridge
regularization (to prevent singularity); report it, do not alarm on it.

**Outputs and state** (decision 7). One record per window to `synergia-scores` (channel id
`m3_context`: main score, Mahalanobis score, weighted mean squared error, per-channel errors,
above-threshold flag, window end time); the monitoring snapshot gains `m3_recon_error` (window
error) and per-channel error fields; model weights are serialized into checkpointed state; expose
`train(windows)` and `retrain(windows, sanitizationStrength)`.

## 4. Deliverable B — minimal injection mode in the replayer (decision 8)

Add `--inject` to `CsvKafkaReplayer`, taking a list of injection specs (device, channel, start
time, duration, type, magnitude) applied to the raw values before sending, so injections flow
through M1's normalization naturally. Four types: amplitude spike (one or a few rounds), step
(sustained offset), slow ramp (magnitude grows linearly — the false-data-injection attack
analogue), stuck sensor (value frozen — the data-quality scenario S6 analogue). Write the injected
ground truth (device, channel, start, end, type, magnitude) to a log file for recall computation.

## 5. Acceptance (two batches)

**Batch one** (gate and offline):

- **V-M3-1** — the three-part compatibility smoke test passes.
- **V-M3-2** — unit and integration tests pass: loss masking, every state-machine transition,
  early stopping, threshold calibration, model weights surviving checkpoint restore, device
  isolation.
- **V-M3-3** — offline grid table on three representative devices (E in band, G masked, C
  moderately structured): hidden size {40, 60, 90} × window length {30, 60, 120}, nine
  combinations, early-stopping-set error, the selection rationale and run times; conclusions with
  a figure.

**Batch two** (online and detection capability):

- **V-M3-4** — held-out normal week false-alarm rate: first week of April, fraction of windows
  above threshold; expected order of magnitude 0.1%–1%; include the main-score distribution figure
  with the threshold marked.
- **V-M3-5** — injection recall table on device E: four types × three magnitudes; detection means
  the score crosses the threshold inside the injection interval; report first-detection latency;
  one figure per type showing the injection interval against the score curve.
- **V-M3-6** — performance report: per-device training time (including the grid), per-window
  inference latency, end-to-end latency added by the M2 forwarding, TaskManager off-heap memory
  usage.

The acceptance report `m3_acceptance.md` is written in English with commands and output excerpts.

## 6. Boundaries

No drift detection (the performance-layer OPTWIN detector belongs to M5b); no retraining triggers
(M6); no changes to M1 or M2 semantics beyond the forwarding change authorized in section 3; the
zeroing of device G's Light is temporary and must not spread to other devices or channels; report
any measurement that contradicts this document instead of working around it.
