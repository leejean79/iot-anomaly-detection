# Java 11 Runtime Migration Report

Migration of the cluster runtime from Java 8 to Java 11 while keeping Flink 1.13.6, the Kafka
brokers (2.6.3), the Kafka connector, Scala 2.12, Smile and JSAT unchanged. The purpose is to make
Deeplearning4j 1.0.0-M2.1 (Java 11 bytecode, class-file major version 55) loadable so that the M3
stage proceeds with the shallow LSTM autoencoder as designed. This report follows the Java 11
Migration Addendum 2 (v1.0, 2026-09-13).

This document records the two halves of the work explicitly, because they run in different places:
the **build-side and repository changes** were completed and verified by the coding agent, while the
**cluster-side container recreation and the section-4 verifications** must be executed by the
operator on the cluster and their outputs pasted into this report.

---

## Section 1 — Assumptions verified (no changes)

| Assumption | Result |
|---|---|
| Image tag `flink:1.13.6-scala_2.12-java11` exists | **Confirmed.** Docker Hub registry API returned HTTP 200 for the tag (and 200 for the `-java8` tag it replaces). |
| JDK 11 on the build machine (Mac) | **Operator to record.** The coding-agent sandbox runs OpenJDK 21; the real `release 11` build must be run on the Mac's JDK 11 toolchain — record the exact `java -version`. |
| Old FA-iForest jar still runnable (Java 8, major 52) | **Operator to confirm** on the cluster (Section 4.2). Java 11 runs Java 8 bytecode unchanged. |
| Disk and rollback | The Java 8 image `fa-iforest/flink:1.13.6` remains on the nodes; rollback is a container recreation, not a rebuild (Section 6 of the addendum). |

---

## Section 2 — Build side (completed in the repository)

1. **Maven baseline.** `pom.xml`: `java.version` and `maven.compiler.source/target` moved to 11,
   `maven.compiler.release=11` added, and the compiler plugin switched to a single `<release>`
   switch. The `maven-enforcer-plugin` was added with two guard rules, backed by
   `extra-enforcer-rules 1.7.0`:
   - `requireJavaVersion [11,12)` — the build JDK must be Java 11.x.
   - `enforceBytecodeVersion maxJdkVersion=11` — no dependency's bytecode may exceed Java 11.
2. **Deep-learning dependencies.** `dl4j.version` returned to `1.0.0-M2.1`. `javacpp.platform` set to
   `linux-x86_64` so only linux-x86_64 native binaries resolve. The shade plugin already carried
   `ServicesResourceTransformer` and does not use `minimizeJar`.
3. **Test run.** `mvn clean package` on the sandbox (OpenJDK 21, `-Denforcer.skip=true` because the
   guard correctly requires JDK 11) produced `BUILD SUCCESS` with **74 tests, 0 failures, 0 errors**,
   including the DL4J three-layer smoke test and `M3CoreTest`. Fat jar size **214 MB**.
   - **Operator action:** re-run `mvn clean verify` on the Mac's **JDK 11** (enforcer active, not
     skipped) and record the JDK version and the test count here.

### Jar inventory (pass conditions met)

- Tensor natives are **linux-x86_64 only** (10 entries under `org/nd4j/**` and `org/bytedeco/**`;
  no `macosx-*` / `windows-*` / other-linux tensor natives). Multi-platform `.so` from unrelated
  dependencies (JNA, netty, snappy, lz4) are irrelevant to the ND4J classifier and are ignored.
- The ND4J backend service file `META-INF/services/org.nd4j.linalg.factory.Nd4jBackend` is present
  and resolves to `org.nd4j.linalg.cpu.nativecpu.CpuBackend` (correctly merged by the transformer).

### Enforcer guard verification (in the sandbox)

- With the Java-version range temporarily widened to include JDK 21, both rules **passed** against
  the real M2.1 dependency tree — `enforceBytecodeVersion maxJdkVersion=11` raised no violation
  (no multi-release-jar false positive).
- With the range restored to `[11,12)`, the build on JDK 21 **fails fast**:
  `Detected JDK version 21.0.10 ... is not in the allowed range [11,12)`. The guard is live: it will
  pass on the Mac's JDK 11 and reject any other JDK.

---

## Section 3 — Cluster side (repository changes done; recreation is the operator's step)

Repository changes completed:
- `deploy/docker/Dockerfile.flink`: base image changed from `flink:1.13.6-scala_2.12-java8` to
  `flink:1.13.6-scala_2.12-java11`; every other layer (the Prometheus reporter, the usrlib mount
  point) unchanged.
- `deploy/env.example`: new variable `FLINK_IMAGE_TAG=fa-iforest/flink:1.13.6-java11` (with a
  rollback note). The old value is not overwritten in place.
- `deploy/scripts/0-prepare-local.sh`: builds and saves the image under `$FLINK_IMAGE_TAG` (falling
  back to the old tag), so the Java 8 image `fa-iforest/flink:1.13.6` remains for rollback.
- `deploy/compose/docker-compose.master.yml` and `docker-compose.worker.yml`: image reference is now
  `${FLINK_IMAGE_TAG:-fa-iforest/flink:1.13.6-java11}`.
- Memory configuration from the earlier off-heap work is carried over unchanged in the worker
  compose (task off-heap 768 MB, managed 256 MB, JavaCPP `maxbytes`/`maxphysicalbytes`/`cachedir`).

**Operator actions (on the cluster, in order):**
1. Add `FLINK_IMAGE_TAG=fa-iforest/flink:1.13.6-java11` to the local `deploy/.env`.
2. Build the Java 11 image (`0-prepare-local.sh`, or `docker build` of `Dockerfile.flink`) under the
   new tag, ship it to the three nodes, and `docker load` it. This does **not** overwrite the Java 8
   image `fa-iforest/flink:1.13.6`.
3. Recreate **only** the JobManager and the two TaskManagers with `--force-recreate` on the new
   image, keeping the jar mount `/opt/fa-iforest/jars:/opt/flink/usrlib` and all volumes. Do **not**
   touch ZooKeeper, Kafka, Prometheus, Grafana or node-exporter. Do not restore any Java 8
   checkpoint.

---

## Section 4 — Verification (executed on the cluster)

Status: **4.1, 4.3 and 4.4 executed and recorded below. 4.2 is still outstanding.**

### 4.1 Cluster health and JVM identity — PASS

Every TaskManager subtask reported by the M3 cluster smoke job (`M3ClusterSmoke`, parallelism 8,
four slots on each of the two TaskManagers):

```
java.version=11.0.16 | java.vendor=Oracle Corporation | java.vm.name=OpenJDK 64-Bit Server VM
os.name=Linux | os.arch=amd64 | user.name=flink | heap.maxBytes=2030043136
```

The smoke script's own cross-check against the JobManager container (`docker exec jobmanager
java -version`) reports Java 11 as well, and `curl :8081/config` reports Flink 1.13.6. Point 3 of
the smoke summary reads `JDK is Java 11 : PASS`. The Flink version is therefore unchanged while
both the JobManager and every TaskManager now run on Java 11.

Note on the build machine (closes the open item in Section 1): the Mac's `mvn clean package`
succeeded with `maven-enforcer-plugin`'s `requireJavaVersion [11,12)` bound to the default
`validate` phase. A JDK outside 11.x would have failed the build before compilation, so the build
toolchain is proven to be 11.x. The exact `java -version` string is still worth pasting here for
the record.

### 4.2 Java 8 jar regression — NOT YET RUN

The Flink WordCount example (Java 8 bytecode, shipped inside the image at
`/opt/flink/examples/streaming/WordCount.jar`) has not been submitted since the recreation onto the
Java 11 image. This item is outstanding.

There is, however, adjacent evidence that Java 11 runs Java 8 bytecode here: the old FA-iForest
containers and their Java 8 jar coexist untouched on the same nodes, and `deploy/scripts/5-load-data.sh`
still runs that jar on the Java 8 image by design. That is coexistence, not a regression test — it
does not substitute for actually running a Java 8 jar on the Java 11 runtime.

### 4.3 M1 + M2 regression under Java 11 — M1 PASS (exact), M2 closes but cannot be compared

Preconditions met: topics and job state were cleared before the replay, so Kafka end offsets equal
the message counts written by this run; M1Job and M2Job were RUNNING and consumed the data
synchronously; no Java 8 checkpoint was restored.

Replay (single day 2022-05-21, `--speedup 600 --start 2022-05-21 --end 2022-05-22`):

```
Finished. Produced (sent): 459472    Send errors: 0    Skipped malformed lines: 0
Data files (csv/sniffed): 3471 / 0   Unknown-device records: 0   File read errors: 0
Idle-compression events: 1           Total compressed wall: 2000 ms      rc=0
```

Offset reconciliation (`syn-m1-reconcile.sh`) against the V-M1-1 Java 8 baseline in
`docs/m1_acceptance.md`:

| partition | device | Java 11 actual | Java 8 baseline | diff |
|---|---|---|---|---|
| 0 | A | 65,688 | 65,688 | 0 |
| 1 | B | 65,512 | 65,512 | 0 |
| 2 | C | 65,568 | 65,568 | 0 |
| 3 | D | 65,712 | 65,712 | 0 |
| 4 | E | 65,720 | 65,720 | 0 |
| 5 | F | 65,592 | 65,592 | 0 |
| 6 | G | 65,680 | 65,680 | 0 |
| 7 | H | 0 | 0 | 0 |
| **source total** | — | **459,472** | **459,472** | **0** |
| **rounds on `synergia-m1-out`** | — | **57,442** | **57,442** | **0** |

Every partition, the ingestion total and the round count match the Java 8 baseline exactly. Device H
is 0 on both sides, which is the recorded downtime window of 2022-05-21..05-23, not a loss.

Counters (`syn-m2-metrics.sh`, job `55182a8615e051ca5336e6787a0d5b01`):

```
m2_gate_admitted 0        m2_gate_warmup_bypass 57442   m2_gate_missing_bypass 0
m2_gate_censored_entered 0  m2_gate_coldstart_clear 0   m2_gate_late_drop 0
m2_outliers_total 0       m2_points_total 0             m2_windows_total 0
m1_assembler_rounds_total 57442      m1_assembler_incomplete_rounds 124
m1_assembler_dup_keys 0              m1_scaler_warmup_rounds 57442
m1_parser_censored_light 0   m1_parser_rssi_sentinel 0   m1_parser_unknown_sensor 0
reconcile: admitted(0) = rounds(57442) − warmup(57442) − missing(0) = 0   → closed
cross-check warmup: scaler(57442) vs gate(57442) → consistent
```

The M1 counters agree with the offsets (`m1_assembler_rounds_total` = 57,442 = the `m1-out` offset)
and `dup_keys` is 0, so no duplicate rounds were emitted — the AT_LEAST_ONCE resend path was not
triggered, i.e. the job did not restart mid-run.

`m2_gate_admitted = 0` is expected on a one-day window and is not a failure. `M2Job` derives the
warmup from `--calib-days` (default 7) times the rounds per day (86400 / `--nominal-period-sec` 10 =
8,640), i.e. **60,480 rounds per device**, and `syn-submit-m2.sh` passes no override. This day's
57,442 rounds over the seven active devices is roughly 8,206 each — about 13.6% of the threshold — so
every round left the gate through `warmup_bypass` and MCOD was never fed. The reconciliation identity
closes exactly at zero, which is the assertion this stage can make. **What it cannot make is an
outlier-count comparison**: with no admitted points there are no outliers to compare, and the Java 8
M2 baseline (`docs/m2_acceptance.md`, V-M2-2/V-M2-4) was recorded over the whole of 2022-03, not over
this day. Section 5 below records the decision.

### 4.4 M3 gate under Java 11 — PASS (closes V-M3-1)

`bash deploy/scripts/syn-m3-smoke.sh --parallelism 8`:

```
Point 1  off-heap budget (JavaCPP bounded)  : PASS
Point 2  native cachedir writable (uid 9999): PASS
Point 3  JDK is Java 11                     : PASS
Point 4  jar natives = linux-x86_64 only    : PASS
(aux)    ND4J native load on TMs            : PASS
```

All eight subtasks report `nd4j_native_ok=true`. Two defects had to be fixed before this passed, both
recorded here because they are migration artifacts rather than M3 logic:

1. **Missing OpenBLAS/JavaCPP natives in the fat jar.** `pom.xml` declared only
   `nd4j-native:linux-x86_64` explicitly; `openblas` and `javacpp` resolved their native classifier
   from the *build host*, so a macOS build produced macOS natives, which the shade excludes then
   removed, leaving none. The TaskManagers failed with `UnsatisfiedLinkError: Could not find
   jniopenblas_nolapack`. Fixed by declaring `org.bytedeco:openblas:0.3.19-1.5.7:linux-x86_64` and
   `org.bytedeco:javacpp:1.5.7:linux-x86_64` explicitly.
2. **`javacpp.maxphysicalbytes` sized as an off-heap budget.** It had been set to 768 MB to match
   `taskmanager.memory.task.off-heap.size`, but JavaCPP compares it against the whole JVM process
   RSS, which is already 769 MB before any tensor is allocated (`totalBytes = 0, physicalBytes =
   769M`). Re-sized against `taskmanager.memory.process.size` (4096 MB) to 3584 MB.

Memory configuration in force on each TaskManager: `javacpp.maxBytes=536870912` (512 MB),
`javacpp.maxPhysicalBytes=3758096384` (3584 MB), `javacpp.cacheDir=/tmp/javacpp-cache` writable,
`heap.maxBytes=2030043136`. _Paste the per-subtask readout from the passing run here, including the
new `javacpp.physicalBytes` field, so the headroom under the 3584 MB ceiling is on record._

---

## Section 5 — Open items

1. **4.2 Java 8 jar regression.** Run `flink run /opt/flink/examples/streaming/WordCount.jar` from
   the JobManager container and record that it completes.
2. **M2 outlier comparison — decided, no further cluster run.** The user chose to accept 4.3 as it
   stands: M1 reconciles exactly, M2's counter identity closes, and the correctness of the outlier
   path itself is carried by the V-M2-1 equivalence tests (MCOD versus an O(n²) reference asserted
   equal per sliding step), which pass on JDK 11 as part of the 74-test suite. The runtime migration
   changed the JVM, not the algorithm, and a numerical divergence would surface in exactly those
   tests. The rejected alternative — replaying the whole of 2022-03 to compare against the V-M2-4
   Java 8 baseline (`m2_outliers_total` 1,249,163 / `m2_points_total` 59,378,802 = 2.1%, MC
   occupancy 93.8%) — remains available if a cluster-level equality datum is ever needed for the
   write-up; it costs roughly an hour of cluster time.
3. **Per-subtask memory readout** for 4.4, as noted above.

---

## Section 6 — Rollback

If any Section 4 step fails for a reason not fixable within the addendum: recreate the JobManager and
TaskManagers on the Java 8 image tag `fa-iforest/flink:1.13.6` (set `FLINK_IMAGE_TAG` back to it),
restore the Java 8 Maven baseline from version control, and report. Rollback is a container
recreation, not a rebuild.

---

## Before / after

| Item | Before | After |
|---|---|---|
| Cluster runtime image | `flink:1.13.6-scala_2.12-java8` (`fa-iforest/flink:1.13.6`) | `flink:1.13.6-scala_2.12-java11` (`fa-iforest/flink:1.13.6-java11`) |
| Build target | Java 8 (`release`/source/target 8) | Java 11 (`release 11`, JDK 11 toolchain) |
| DL4J / ND4J | 1.0.0-beta7 (Java 7 bytecode, interim) | 1.0.0-M2.1 (Java 11 bytecode) |
| Enforcer guards | none in repo | `requireJavaVersion [11,12)`, `enforceBytecodeVersion maxJdkVersion 11` |
| Flink / Kafka / Scala | 1.13.6 / 2.6.3 / 2.12 | unchanged |
