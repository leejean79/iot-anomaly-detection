# Java 11 迁移 · 集群侧执行文档 / Cluster-Side Execution Guide

本文件是 Java 11 运行时迁移在**集群上的逐步执行手册**，与 `docs/java11_migration_report.md`（正式记录）
配套。仓库侧改动已完成并推送（提交 `4911ff3`）；本文件只讲你要在本机与集群上按序执行的操作。执行完把
第 6 步的输出回填到迁移报告第 4 节。

> 目标 / Goal：把集群 Flink 运行时从 Java 8 换到 **Java 11**（镜像 `flink:1.13.6-scala_2.12-java11`），
> Flink 版本不变（1.13.6），使 DL4J 1.0.0-M2.1 可加载。**只重建 JobManager 与两台 TaskManager**；
> ZooKeeper、Kafka、Prometheus、Grafana、node-exporter、jar 挂载路径与所有 topic 均不动。

---

## 0. 前提、访问模型与红线 / Prerequisites, access model, hard rules

**访问模型 / Access model**（按你确认的情况）：
- 本机 Mac → master：`ssh -i <KEY> root@<MASTER_PUBLIC_IP>`，其中 `<KEY>` = `/Users/lijing/Documents/scripts/test/fa-iforest-key.pem`。
- worker：**从 master 走内网访问**（master → worker 内网 IP）。因此涉及 worker 的步骤在 master 上执行，
  再由 master ssh/scp 到 worker 内网 IP。这要求 master 上存有能登录 worker 的私钥（下称 `<KEY_ON_MASTER>`）。
  若 master 上没有该私钥，先把 `<KEY>` 拷到 master（如 `scp -i <KEY> <KEY>  root@<MASTER>:/root/.ssh/fa.pem` 并 `chmod 600`）。

**先决条件 / Preconditions**：
- 本机已安装 **JDK 11**（Temurin 或等价）并设为当前 `JAVA_HOME`（否则 pom 的 enforcer 会拒绝构建）。
- 本机已安装 Docker（用于构建并导出 Flink 镜像）。
- 集群已在运行（M1/M2 曾运行其上）；`deploy/.env` 已填真实 IP 与 SSH。

**重要风险 / Critical warning**：本集群是 Flink standalone（无 JobManager HA）。**重建 JobManager 会丢弃当前
正在运行的所有 Flink 作业**（不会自动重投），重建 TaskManager 也会中断其上任务。执行第 4 步前，务必确认
没有需要保命的旧 FA-iForest 作业在跑，或与相关负责人协调、在可容忍窗口进行。

**共存红线 / Coexistence hard rules**：
1. 只重建 `jobmanager` 与 `taskmanager` 两个服务；**不动** zookeeper / kafka / prometheus / grafana / node-exporter。
2. 绝不 `flink cancel` 任何旧作业；绝不删除或改动任何 topic 与数据。
3. 绝不 `9-teardown.sh --purge`。
4. 不恢复任何 Java 8 下产生的 checkpoint。

**记号 / Placeholders**（按你的实际值替换）：
`<KEY>`、`<MASTER>`（master 公网 IP）、`<W1_INT>`/`<W2_INT>`（两台 worker 内网 IP）、
`<REMOTE_HOME>`=`/opt/fa-iforest`、旧镜像 `fa-iforest/flink:1.13.6`（Java 8，保留回滚）、
新镜像 `fa-iforest/flink:1.13.6-java11`（本次目标）。

---

## 1. 本机：JDK 11 构建 jar 并导出 Java 11 镜像 / Build on JDK 11 and export the image

在仓库根目录执行。`0-prepare-local.sh` 会一次性完成：`mvn clean package`（enforcer 生效，需 JDK 11）
生成 M2.1/Java11 的 fat jar，构建自定义 Flink 镜像并按 `FLINK_IMAGE_TAG` 打标签、`--platform linux/amd64`
（集群是 x86_64，官方 `flink:1.13.6-*-java11` 仅发布 amd64；Apple Silicon 上不固定平台会报
`no match for platform in manifest`——脚本已固定），导出为 tar。

```bash
java -version                          # 确认是 11.x / must be 11.x
# .env 里已有 FLINK_IMAGE_TAG=fa-iforest/flink:1.13.6-java11（env.example 已带；本地 .env 请补上此行）
bash deploy/scripts/0-prepare-local.sh
bash deploy/scripts/check-jar.sh       # 全 PASS 才继续 / must be all PASS
```
- 期望产出 / Expected：`BUILD SUCCESS`；`deploy/.build/fa-iforest-flink.tar`（内含 `fa-iforest/flink:1.13.6-java11`）；
  `target/iot-anomaly-detection-1.0-SNAPSHOT.jar`；`check-jar` 全 PASS。
- 失败兜底 / Fallback（平台）：若仍报 `no match for platform`，确认 Docker Desktop 已开启对 amd64 的模拟
  （Apple Silicon 默认可模拟）；或用 `DOCKER_PLATFORM=linux/amd64 bash deploy/scripts/0-prepare-local.sh` 显式指定。
- 失败兜底 / Fallback（网络，国内常见）：若卡在 `load metadata for docker.io/library/flink:...`，是拉 Docker Hub
  太慢/被限速。给 Docker Desktop 配镜像加速器（Settings → Docker Engine 加
  `"registry-mirrors": ["https://docker.m.daocloud.io"]` 或阿里云专属 `https://<ID>.mirror.aliyuncs.com`，重启 Docker），
  先 `docker pull --platform linux/amd64 flink:1.13.6-scala_2.12-java11` 拉通再跑本步。或改在集群 amd64 节点上构建
  （节点通常已配加速器、且原生 amd64 免模拟），再内网分发。
- 门槛验证 / Gate（另跑一次）：`0-prepare-local.sh` 用 `-DskipTests` 只出可部署产物；handover §2.3 的
  "JDK 11 全量测试"请另跑 `mvn clean verify`（enforcer 生效、跑测试），把 JDK 版本与测试数记入迁移报告 §2。
- 失败兜底 / Fallback：若 enforcer 报 `Detected JDK version ... not in [11,12)`，说明当前不是 JDK 11——切换后重来。

---

## 2. 分发镜像+compose+.env 到三节点 / Distribute image + config to all three nodes

> **为什么不用 `1-sync-to-nodes.sh`？/ Why not 1-sync?** `1-sync` 是"原样继承"的旧 FA-iForest 部署脚本
> （见 `deploy/README`）。它的 `sync_master` 会把 `$BUILD_DIR/$JOB_JAR_NAME`（`JOB_JAR_NAME=`
> `FA-iForest-1.0-SNAPSHOT.jar`）rsync 到共享的 `jars/`——这会**覆盖旧项目的 jar**（违反共存红线），
> 且把我们的 jar 放成了错误的文件名（M3 作业用 `SYN_JOB_JAR_NAME=iot-anomaly-detection-*.jar`，由
> `syn-upload-m1.sh` 单独投放）。因此本项目不用 `1-sync`。为复用其可靠的 ssh/rsync 分发模式又不碰任何
> jar，提供了共存安全的 `syn-sync-flink-image.sh`：只发镜像 tar + 两个 compose + `.env` 并 `docker load`。

```bash
bash deploy/scripts/syn-sync-flink-image.sh
```
- 期望产出 / Expected：三节点 `<REMOTE_HOME>` 下有最新 `.env` 与 `compose/`，镜像已 `docker load`；旧
  Java 8 镜像 `fa-iforest/flink:1.13.6` 保留不动（回滚用）。脚本可达性与 `1-sync` 相同（优先公网 IP，
  未填用内网 IP）——若你的 `1-sync` 之前能连通三节点，本脚本同样能。
- 失败兜底 / Fallback：找不到镜像 tar → 先跑第 1 步的 `0-prepare-local.sh`；某 worker 从本机不可达
  （只能经 master 内网）→ 见文末附录 A 的"经 master 转发"手动分发。

验证三节点都拿到新镜像与标签 / verify each node has the image + tag:
```bash
ssh -i <KEY> root@<MASTER> "docker images | grep fa-iforest/flink; grep FLINK_IMAGE_TAG <REMOTE_HOME>/.env"
```
- 期望：`docker images` 同时列出旧 `:1.13.6`（Java 8）与新 `:1.13.6-java11`；`.env` 含
  `FLINK_IMAGE_TAG=fa-iforest/flink:1.13.6-java11`。

---

## 3. 只重建 JobManager 与两台 TaskManager / Recreate only JM + the two TMs

> 再次确认：这会丢弃当前运行的 Flink 作业（standalone 无 HA，见第 0 节风险）。确认可执行后再做。

用现成的 `2-up-all.sh`：它对每个节点做 `docker compose up -d`，**只重建配置/镜像变更了的容器**——本次仅
flink 镜像标签变了，故只有 `jobmanager`（master）与 `taskmanager`（两台 worker）被重建，ZooKeeper、Kafka、
Prometheus、Grafana、node-exporter 因未变更而保持运行、不受影响。

```bash
bash deploy/scripts/2-up-all.sh
```
- jobmanager 的 jar 挂载 `${REMOTE_HOME}/jars:/opt/flink/usrlib` 写在 compose 里，重建后自动保留。
- 若你想更外科手术式地只动 flink 容器（例如担心 `up -d` 触碰其它服务），可改用附录 B 的
  `--no-deps --force-recreate jobmanager` / `taskmanager` 精确重建（其中 worker 若只能经 master 内网访问，
  按附录 A 的转发方式执行）。

---

## 4. 上传 M3 jar / Upload the M3 job jar

镜像换好后，上传本项目 jar（只连 master；用 `SYN_JOB_JAR_NAME`，与旧 FA jar 不同名、互不覆盖）：
```bash
bash deploy/scripts/syn-upload-m1.sh --jar-only
# topic 若已存在可跳过；确认一下（幂等）：
bash deploy/scripts/syn-create-topics.sh
```

---

## 5. 验证（按序执行，输出回填迁移报告第 4 节）/ Verification

**5.1 集群健康 + JVM 为 11 / cluster health + java.version 11**
```bash
bash deploy/scripts/syn-verify-cluster.sh          # 只读验证，绝不 cancel 旧作业
ssh -i <KEY> root@<MASTER> "curl -s localhost:8081/config | grep -o '\"flink-version\":\"[^\"]*\"'"      # 应为 1.13.6
ssh -i <KEY> root@<MASTER> "curl -s localhost:8081/taskmanagers"   # 两个 TM、共 8 slot
```
- 期望：两台 TaskManager、8 个 slot、Prometheus targets up；Flink 版本 1.13.6。TM 的 `java.version` 为 11
  由第 5.4 步冒烟报告在每个子任务上权威给出。

**5.2 旧 Java 8 jar 回归 / old Java 8 jar still runs**
```bash
ssh -i <KEY> root@<MASTER> "docker exec jobmanager flink run /opt/flink/examples/streaming/WordCount.jar"
```
- 期望：作业跑到 FINISHED，证明 Java 8 字节码的旧 jar 在 Java 11 上仍可运行。

**5.3 M1 + M2 单日回归对账 / M1+M2 regression, counts reconcile**
- 用 M1 验收 V-M1-1 的同一天，经 M1+M2 联合作业重放并核验（含完整性检查），确认轮数、守卫计数、
  锁定半径下的 M2 离群数与迁移前完全一致。**从干净状态开始，不恢复任何 Java 8 checkpoint。**
- 具体命令按你既有的 M1/M2 验收流程；重放完整性门槛见 `syn-replay-verify.sh`。

**5.4 M3 门槛 / M3 gate（关闭 V-M3-1）**
```bash
bash deploy/scripts/syn-m3-smoke.sh --parallelism 8
```
- 期望：四点全 PASS，八个子任务均 `nd4j_native_ok=true`，并给出堆外内存读数。把每子任务的 JVM/内存读数
  回填到 `docs/java11_migration_report.md` 第 4.4 节。

---

## 6. 回滚 / Rollback

若第 5 步任一环节失败且无法在本迁移范围内解决：
1. 把三台节点 `<REMOTE_HOME>/.env` 的 `FLINK_IMAGE_TAG` 改回旧的 Java 8 镜像：`fa-iforest/flink:1.13.6`。
2. 重新 `bash deploy/scripts/2-up-all.sh`（此时 flink 容器会用回旧镜像重建），或用附录 B 精确重建 jobmanager 与两台 taskmanager。
3. 本机把 pom 的 Java 8 基线从版本库恢复（`git checkout <迁移前提交> -- pom.xml` 或回退相应提交），并回报设计会。
回滚是容器重建，不是重构镜像；旧镜像一直在节点上。

---

## 7. 失败兜底速查 / Quick failure reference

| 现象 | 排查方向 |
|---|---|
| 本机构建报 `not in the allowed range [11,12)` | 当前不是 JDK 11，切换 `JAVA_HOME` 后重跑第 1 步 |
| `docker load` 后 `up` 仍用旧镜像 | 该节点 `.env` 缺 `FLINK_IMAGE_TAG`，或未 `--force-recreate`；补齐后重来 |
| jobmanager 重建后看不到 usrlib 里的 jar | 确认 compose 的 `${REMOTE_HOME}/jars:/opt/flink/usrlib` 卷未被改动；重新 `syn-upload-m1.sh --jar-only` |
| master 无法 ssh 到 worker | master 上没有 worker 私钥；把 `<KEY>` 拷到 master 并 `chmod 600`，或确认 worker 内网可达 |
| `syn-m3-smoke.sh` 无空闲 slot | 有旧作业占用 slot；等其结束或 `--parallelism` 调小，**绝不 cancel 旧作业** |

---

## 附录 A：worker 只能经 master 内网访问时的手动分发 / Appendix A — distribute via master when workers aren't reachable from the Mac

若 `syn-sync-flink-image.sh` 在 worker 步失败（本机直连不到 worker，只能从 master 走内网），改为手动经 master 转发。
`<KEY_ON_MASTER>` 是 master 上能登录 worker 的私钥（若无，先 `scp` 上去并 `chmod 600`）。

```bash
# 1) 本机 → master：镜像 tar + 两个 compose + .env
scp -i <KEY> deploy/.build/fa-iforest-flink.tar         root@<MASTER>:<REMOTE_HOME>/
scp -i <KEY> deploy/compose/docker-compose.master.yml   root@<MASTER>:<REMOTE_HOME>/compose/
scp -i <KEY> deploy/compose/docker-compose.worker.yml   root@<MASTER>:<REMOTE_HOME>/compose/
scp -i <KEY> deploy/.env                                root@<MASTER>:<REMOTE_HOME>/.env
ssh -i <KEY> root@<MASTER> "docker load -i <REMOTE_HOME>/fa-iforest-flink.tar"
# 2) 登录 master，再从 master → 两台 worker（内网）
ssh -i <KEY> root@<MASTER>
for w in <W1_INT> <W2_INT>; do
  scp -i <KEY_ON_MASTER> <REMOTE_HOME>/fa-iforest-flink.tar              root@$w:<REMOTE_HOME>/
  scp -i <KEY_ON_MASTER> <REMOTE_HOME>/compose/docker-compose.worker.yml root@$w:<REMOTE_HOME>/compose/
  scp -i <KEY_ON_MASTER> <REMOTE_HOME>/.env                             root@$w:<REMOTE_HOME>/.env
  ssh -i <KEY_ON_MASTER> root@$w "docker load -i <REMOTE_HOME>/fa-iforest-flink.tar"
done
```
分发完后回到第 3 步（`2-up-all.sh`；若 `2-up-all` 也从本机连不到 worker，用附录 B 在 master 上逐台重建）。

## 附录 B：精确重建（只动 flink 容器）/ Appendix B — targeted recreate (flink containers only)

不想用 `2-up-all.sh` 的整体 `up -d`，或需在 master 上逐台操作 worker 时，用 `--no-deps --force-recreate`：

```bash
# JobManager（master）
ssh -i <KEY> root@<MASTER> "cd <REMOTE_HOME>/compose && \
  docker compose -f docker-compose.master.yml --env-file ../.env up -d --no-deps --force-recreate jobmanager"
# TaskManager（每台 worker；若只能经 master，则先 ssh master 再用 <KEY_ON_MASTER> 转发）
ssh -i <KEY> root@<W1_INT> "cd <REMOTE_HOME>/compose && \
  BROKER_ID=2 NODE_SELF_IP=<W1_INT> docker compose -f docker-compose.worker.yml --env-file ../.env up -d --no-deps --force-recreate taskmanager"
ssh -i <KEY> root@<W2_INT> "cd <REMOTE_HOME>/compose && \
  BROKER_ID=3 NODE_SELF_IP=<W2_INT> docker compose -f docker-compose.worker.yml --env-file ../.env up -d --no-deps --force-recreate taskmanager"
```
- `--no-deps`：不连带重建 kafka 等依赖；`--force-recreate <service>`：用新镜像与新配置重建该服务。
