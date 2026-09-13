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

在仓库根目录执行。`0-prepare-local.sh` 会一次性完成：`mvn clean package`（此时 enforcer 生效，需 JDK 11）
生成 M2.1/Java11 的 fat jar，构建自定义 Flink 镜像并按 `FLINK_IMAGE_TAG` 打标签，导出为 tar。

```bash
java -version                          # 确认是 11.x / must be 11.x
# .env 里已有 FLINK_IMAGE_TAG=fa-iforest/flink:1.13.6-java11（env.example 已带；本地 .env 请补上此行）
bash deploy/scripts/0-prepare-local.sh
bash deploy/scripts/check-jar.sh       # 全 PASS 才继续 / must be all PASS
```
- 期望产出 / Expected：`BUILD SUCCESS`；`deploy/.build/fa-iforest-flink.tar`（内含 `fa-iforest/flink:1.13.6-java11`）；
  `target/iot-anomaly-detection-1.0-SNAPSHOT.jar`；`check-jar` 全 PASS。
- 失败兜底 / Fallback：若 enforcer 报 `Detected JDK version ... not in [11,12)`，说明当前不是 JDK 11——切换后重来。

---

## 2. 分发到三节点并加载镜像（旧 Java 8 镜像保留）/ Distribute and load the image

新镜像 tar 与更新后的 compose、`.env` 需要到达三台节点。master 从本机直传；worker 经 master 转发。

**2a. master（从本机）/ master (from the Mac)**
```bash
scp -i <KEY> deploy/.build/fa-iforest-flink.tar          root@<MASTER>:<REMOTE_HOME>/
scp -i <KEY> deploy/compose/docker-compose.master.yml    root@<MASTER>:<REMOTE_HOME>/compose/
scp -i <KEY> deploy/compose/docker-compose.worker.yml    root@<MASTER>:<REMOTE_HOME>/compose/
scp -i <KEY> deploy/.env                                 root@<MASTER>:<REMOTE_HOME>/.env
ssh -i <KEY> root@<MASTER> "docker load -i <REMOTE_HOME>/fa-iforest-flink.tar && docker images | grep fa-iforest/flink"
```
- 期望：`docker images` 同时列出旧 `fa-iforest/flink:1.13.6`（Java 8）与新 `fa-iforest/flink:1.13.6-java11`。

**2b. 两台 worker（在 master 上，转发到 worker 内网）/ workers (from master, over the internal network)**
```bash
ssh -i <KEY> root@<MASTER>          # 登录 master 后，用 master 上的私钥转发到 worker
for w in <W1_INT> <W2_INT>; do
  scp -i <KEY_ON_MASTER> <REMOTE_HOME>/fa-iforest-flink.tar        root@$w:<REMOTE_HOME>/
  scp -i <KEY_ON_MASTER> <REMOTE_HOME>/compose/docker-compose.worker.yml root@$w:<REMOTE_HOME>/compose/
  scp -i <KEY_ON_MASTER> <REMOTE_HOME>/.env                        root@$w:<REMOTE_HOME>/.env
  ssh -i <KEY_ON_MASTER> root@$w "docker load -i <REMOTE_HOME>/fa-iforest-flink.tar"
done
```
- 说明：本项目 worker compose 无需 master 的 jar 与 prometheus.yml，只需 worker compose、`.env` 与镜像 tar。

---

## 3. 确认每个节点的 .env 含新镜像标签 / Confirm FLINK_IMAGE_TAG on every node

三台节点的 `<REMOTE_HOME>/.env` 都必须含：
```
FLINK_IMAGE_TAG=fa-iforest/flink:1.13.6-java11
```
第 2 步已把本机 `.env` 覆盖上去；确认一下：
```bash
ssh -i <KEY> root@<MASTER> "grep FLINK_IMAGE_TAG <REMOTE_HOME>/.env"
# worker（在 master 上）：ssh -i <KEY_ON_MASTER> root@<W1_INT> "grep FLINK_IMAGE_TAG <REMOTE_HOME>/.env"
```

---

## 4. 只重建 JobManager 与两台 TaskManager / Recreate only JM + the two TMs

> 再次确认：这会丢弃当前运行的 Flink 作业（见第 0 节风险）。确认可执行后再做。

**4a. JobManager（master，从本机）**
```bash
ssh -i <KEY> root@<MASTER> "cd <REMOTE_HOME>/compose && \
  docker compose -f docker-compose.master.yml --env-file ../.env up -d --no-deps --force-recreate jobmanager"
```

**4b. TaskManager（在 master 上，转发到每台 worker）**
```bash
ssh -i <KEY> root@<MASTER>
# worker-1（BROKER_ID=2）：
ssh -i <KEY_ON_MASTER> root@<W1_INT> "cd <REMOTE_HOME>/compose && \
  BROKER_ID=2 NODE_SELF_IP=<W1_INT> docker compose -f docker-compose.worker.yml --env-file ../.env up -d --no-deps --force-recreate taskmanager"
# worker-2（BROKER_ID=3）：
ssh -i <KEY_ON_MASTER> root@<W2_INT> "cd <REMOTE_HOME>/compose && \
  BROKER_ID=3 NODE_SELF_IP=<W2_INT> docker compose -f docker-compose.worker.yml --env-file ../.env up -d --no-deps --force-recreate taskmanager"
```
- `--no-deps`：不连带重建 kafka 等依赖服务；`--force-recreate <service>`：用新镜像与新配置重建该服务。
- jobmanager 的 jar 挂载 `${REMOTE_HOME}/jars:/opt/flink/usrlib` 写在 compose 里，重建后自动保留。

---

## 5. 上传 M3 jar / Upload the M3 job jar

镜像换好后，上传本项目 jar（只连 master）：
```bash
bash deploy/scripts/syn-upload-m1.sh --jar-only
# topic 若已存在可跳过；确认一下（幂等）：
bash deploy/scripts/syn-create-topics.sh
```

---

## 6. 验证（按序执行，输出回填迁移报告第 4 节）/ Verification

**6.1 集群健康 + JVM 为 11 / cluster health + java.version 11**
```bash
bash deploy/scripts/syn-verify-cluster.sh          # 只读验证，绝不 cancel 旧作业
ssh -i <KEY> root@<MASTER> "curl -s localhost:8081/config | grep -o '\"flink-version\":\"[^\"]*\"'"      # 应为 1.13.6
ssh -i <KEY> root@<MASTER> "curl -s localhost:8081/taskmanagers"   # 两个 TM、共 8 slot
```
- 期望：两台 TaskManager、8 个 slot、Prometheus targets up；Flink 版本 1.13.6。TM 的 `java.version` 为 11
  由第 6.4 步冒烟报告在每个子任务上权威给出。

**6.2 旧 Java 8 jar 回归 / old Java 8 jar still runs**
```bash
ssh -i <KEY> root@<MASTER> "docker exec jobmanager flink run /opt/flink/examples/streaming/WordCount.jar"
```
- 期望：作业跑到 FINISHED，证明 Java 8 字节码的旧 jar 在 Java 11 上仍可运行。

**6.3 M1 + M2 单日回归对账 / M1+M2 regression, counts reconcile**
- 用 M1 验收 V-M1-1 的同一天，经 M1+M2 联合作业重放并核验（含完整性检查），确认轮数、守卫计数、
  锁定半径下的 M2 离群数与迁移前完全一致。**从干净状态开始，不恢复任何 Java 8 checkpoint。**
- 具体命令按你既有的 M1/M2 验收流程；重放完整性门槛见 `syn-replay-verify.sh`。

**6.4 M3 门槛 / M3 gate（关闭 V-M3-1）**
```bash
bash deploy/scripts/syn-m3-smoke.sh --parallelism 8
```
- 期望：四点全 PASS，八个子任务均 `nd4j_native_ok=true`，并给出堆外内存读数。把每子任务的 JVM/内存读数
  回填到 `docs/java11_migration_report.md` 第 4.4 节。

---

## 7. 回滚 / Rollback

若第 6 步任一环节失败且无法在本迁移范围内解决：
1. 把三台节点 `<REMOTE_HOME>/.env` 的 `FLINK_IMAGE_TAG` 改回旧的 Java 8 镜像：`fa-iforest/flink:1.13.6`。
2. 按第 4 步同样的 `--no-deps --force-recreate` 重建 jobmanager 与两台 taskmanager（此时会用回旧镜像）。
3. 本机把 pom 的 Java 8 基线从版本库恢复（`git checkout <迁移前提交> -- pom.xml` 或回退相应提交），并回报设计会。
回滚是容器重建，不是重构镜像；旧镜像一直在节点上。

---

## 8. 失败兜底速查 / Quick failure reference

| 现象 | 排查方向 |
|---|---|
| 本机构建报 `not in the allowed range [11,12)` | 当前不是 JDK 11，切换 `JAVA_HOME` 后重跑第 1 步 |
| `docker load` 后 `up` 仍用旧镜像 | 该节点 `.env` 缺 `FLINK_IMAGE_TAG`，或未 `--force-recreate`；补齐后重来 |
| jobmanager 重建后看不到 usrlib 里的 jar | 确认 compose 的 `${REMOTE_HOME}/jars:/opt/flink/usrlib` 卷未被改动；重新 `syn-upload-m1.sh --jar-only` |
| master 无法 ssh 到 worker | master 上没有 worker 私钥；把 `<KEY>` 拷到 master 并 `chmod 600`，或确认 worker 内网可达 |
| `syn-m3-smoke.sh` 无空闲 slot | 有旧作业占用 slot；等其结束或 `--parallelism` 调小，**绝不 cancel 旧作业** |
