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

## Section 4 — Verification (operator to run on the cluster, paste outputs)

1. **Cluster health** — `syn-verify-cluster.sh` passes; `curl :8081/config` shows Flink 1.13.6;
   `curl :8081/taskmanagers` and a subtask report show `java.version` 11 on every TaskManager.
   _Paste output:_
2. **Java 8 jar regression** — the Flink WordCount example (and, if practical, the old FA-iForest
   smoke job) runs to completion, proving old Java 8 jars still run on Java 11.
   _Paste output:_
3. **M1 + M2 regression under Java 11** — replay the single day from M1 acceptance V-M1-1 (with the
   integrity check) through the joint job; counts must reconcile exactly (rounds produced, guard
   counters, M2 outlier counts for the locked radii). Start from fresh state; no Java 8 checkpoint
   is restored.
   _Paste output:_
4. **M3 gate under Java 11** — `bash deploy/scripts/syn-m3-smoke.sh --parallelism 8`: all four points
   plus `nd4j_native_ok=true` on all eight subtasks, with the memory readout. This closes V-M3-1.
   _Paste the per-subtask JVM/memory readout:_

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
