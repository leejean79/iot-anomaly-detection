# 交接文档：ENV 阶段 —— 运行环境建立（旧集群共存复用）
# Handover: Environment Stage — Runtime Setup on Reused Cluster (Coexistence)

> **发出方**：开发设计会话（`dev_design_log` v0.9，决策 DEV-D8 / DEV-D9）
> **接收方**：代码开发 agent
> **前置声明**：遵守项目 `CLAUDE.md` 全部适用条款——中英双语注释、完整书面沟通、脚本交付五要素（执行环境、调用命令、前置条件、期望产出、失败兜底）。交付前执行**缩写自查**：所有缩写首次出现处附中文注解。

---

## 1. 任务一句话

在已验证存活的三节点旧集群（FA-iForest 部署骨架）上，以**共存策略**为本项目建立运行命名空间：在工程仓库 `iot-anomaly-detection` 内扩建部署骨架，新建 `synergia-*` topic 集、部署脚本增量适配、完成端到端验收冒烟；**不改动任何旧项目容器、镜像、topic 与数据**。

## 0. 仓库骨架扩建（DEV-D9，本阶段第一步）

工程仓库为**既有** `github.com/leejean79/iot-anomaly-detection`（EDA 已入驻，用户本地已 pull 同步）。本步在该仓库内新增：
1. `deploy/` —— 自 FA-iForest-master 拷贝 `deploy/` 骨架（compose ×2、Dockerfile.flink、prometheus 模板、scripts 0–9 与 refresh-ips），**拷贝后仅做两类修改**：(a) 本阶段 §5 的 `syn-*` 增量脚本置于 `deploy/scripts/`；(b) `.env` 自旧仓库 `.env` 复制（节点 IP、SSH 密钥路径、版本段原样保留）并追加 `SYN_*` 变量段。其余脚本暂不改（M1 阶段按需适配）。
2. `pom.xml` —— 自 FA-iForest-master fork：groupId `com.leejean`、artifactId `iot-anomaly-detection`、version `1.0-SNAPSHOT`；裁剪 flink-table/avro 依赖；**保留** Smile 的 Kotlin 全排除、dependencyManagement 钉版、shade 配置、log4j2/JUnit5/Guava/Jackson（资产清单 §9 第 1 行）。`src/main/java/com/leejean/` 与 `src/test/java/` 建空包架子，本阶段不写业务代码。
3. `docs/` —— 建目录，归档本交接文档与后续验收记录。
仓库根 `CLAUDE.md` 保持权威；`eda/` 目录不动。

**集群侧目录共存规则（重要）**：旧 jobmanager 容器创建时已固化挂载 `/opt/fa-iforest/jars:/opt/flink/usrlib`，且 DEV-D8 禁止重建容器——因此本项目的 fat jar 与数据集在**集群侧**继续落 `/opt/fa-iforest/jars`、`/opt/fa-iforest/datasets`，以文件名区分（`iot-anomaly-detection-*.jar` 不与 `FA-iForest-*.jar` 冲突）。`.env` 的 `REMOTE_HOME=/opt/fa-iforest` **保持不变**；仓库侧命名归新项目，集群侧目录归旧基础设施，两侧解耦。

## 2. 集群现状（2026-08-20 用户实测，编码时作为前置事实）

- 拓扑：master（ZooKeeper + kafka-1 + Flink JobManager + Prometheus + Grafana + node-exporter）、worker-1（kafka-2 + TaskManager 4 slots）、worker-2（kafka-3 + TaskManager 4 slots）；全部容器运行中。
- **Broker 存活实测 [1, 2, 3] 三台** → 新 topic 副本因子 RF（Replication Factor，每分区数据副本份数）= 2。
- 版本：Flink 1.13.6（Scala 2.12，镜像 `fa-iforest/flink:1.13.6`，含 Prometheus reporter）、Kafka 2.6.3（wurstmeister/kafka:2.13-2.6.3）、ZK 3.6.3。与项目 CLAUDE.md 全等。
- 磁盘：三节点各约 28 GB 可用。内网 IP 见 `deploy/.env`（NODE_MASTER_IP 等）；公网 IP 已刷新，ssh 别名 fa-master / fa-worker1 / fa-worker2 可用。
- 旧项目约束澄清：`SOURCE_PARTITIONS=1` 是旧架构（全局单模型需全局保序）的有意约束，本项目**不继承**（D2 每设备独立管线只需设备内保序）。

## 3. 保护性前置（不可逆保护点，执行改造前完成）

1. 确认旧项目实验结果已 `pull-results.sh` 拉回本地（向用户确认，得到肯定答复后才动手）；
2. 旧 topic（source-topic / tree-topic / model-topic / output-scores / feature-drift-topic / drift-round-topic）**保留不动**；本阶段所有脚本严禁 delete/alter 任何非 `synergia-` 前缀的 topic；
3. 旧 `.env` 变量段保留；新增变量以 `SYN_` 前缀追加于文件尾部注释块内，不覆盖同名旧变量。

## 4. 新 topic 设计（本阶段创建）

| Topic | Partitions | RF | 用途与配置 |
|---|---|---|---|
| `synergia-source` | **8** | 2 | 主数据流；DeviceId（A–H）为消息 key；`retention.ms=86400000`（24 h，可参数化）——全集重放一轮约 5 GB 消息量，24 h 保留 + 实验间清理足够 |
| `synergia-smoke` | 1 | 2 | 本阶段验收冒烟专用，验收后可留作日常连通性检查 |

其余输出/监测 topic（报警流、监测信号流等）由 M1 阶段定稿后增建——本阶段在创建脚本中**留出参数化的 topic 清单数组**，M1 只改清单不改脚本。

**分区映射预告（写入脚本注释，供 M1 重放器遵循）**：Kafka 默认按 `hash(key) % partitions` 分区，8 个 DeviceId 经哈希可能碰撞（两设备同分区、部分分区空置）。这不破坏正确性（同 key 恒同分区，设备内保序成立），但为实现"一设备一分区"的字面语义与均衡负载，M1 重放器将使用**显式分区映射**（A→0, B→1, …, H→7 的自定义 partitioner）。本阶段冒烟用 console 工具，按默认哈希即可。

## 5. 脚本交付清单（全部为增量新增，置于 `deploy/scripts/`，不修改旧脚本）

0. 仓库骨架扩建产物见 §0（deploy/ 拷贝改造、pom 基座、docs/）。
1. `syn-create-topics.sh`：按 §4 创建 topic（幂等：已存在则 describe 校验参数一致并跳过；参数不一致则报错退出，**不自动 alter**）；topic 清单、分区数、RF、retention 全部取自 `.env` 的 `SYN_*` 变量。
2. `syn-clean-topics.sh`：仅清理 `synergia-` 前缀 topic（delete → 等待 → create 回原参数）；脚本内硬编码前缀白名单校验，任何非前缀 topic 名直接拒绝执行。
3. `syn-verify-cluster.sh`：验收冒烟一键脚本，执行 §6 全部步骤并输出核对表（通过/失败逐项打印）。
4. `.env` 追加 `SYN_*` 变量段（含双语注释）：`SYN_TOPIC_SOURCE=synergia-source`、`SYN_TOPIC_SMOKE=synergia-smoke`、`SYN_SOURCE_PARTITIONS=8`、`SYN_TOPIC_REPLICATION=2`、`SYN_RETENTION_MS=86400000`、`SYN_JOB_JAR_NAME=`（占位，M1 填）、`SYN_JOB_MAIN=`（占位，M1 填）。

## 6. 验收步骤（`syn-verify-cluster.sh` 的内容，逐项为验收项）

1. **Broker 与版本自证**：`zookeeper-shell ls /brokers/ids` 应返回 [1,2,3]；`curl http://<master内网IP>:8081/config` 应含 flink-version 1.13.6；`curl :8081/taskmanagers` 应见 2 TM / 共 8 slots；`curl :8081/jobs` 记录当前 job 列表（预期为空或仅旧项目残留，如有 RUNNING 的旧 job 上报用户，不擅自 cancel）。
2. **Topic 创建核验**：`kafka-topics.sh --describe synergia-source` 显示 8 分区、RF=2、leader 分布覆盖 3 个 broker（副本均衡性人工可读输出）。
3. **保序冒烟**：console-producer 向 `synergia-source` 以 `key=A..H` 各写 10 条带序号消息（`--property parse.key=true`）；console-consumer 按分区消费，核验（a）同 key 消息落于同一分区且序号有序，（b）实际占用的分区数（预期 ≤8，哈希碰撞属正常，记录实测映射表供 M1 参考）。
4. **Flink 调度冒烟**：提交 Flink 自带 example（如 `examples/streaming/WordCount.jar`）验证 JM 接受提交、任务在 TM 上运行并正常结束——证明 slots 可调度、jar 分发链路通。
5. **监控核验**：Prometheus `/targets` 全绿（flink-jobmanager ×1、flink-taskmanagers ×2、node ×3）；Grafana 可登录。
6. 全部通过后输出 `env_acceptance.md`：各步骤命令、原始输出摘录、核对表、实测的 DeviceId→分区哈希映射表。

## 7. 边界与禁区

- 不 rebuild / 不重命名任何镜像与容器；不修改旧 compose 文件与旧脚本；不触碰旧 topic 与其数据；
- 本阶段不部署本项目 jar（M1 交付后走既有 `4-submit-job.sh` 模式的增量脚本）；
- 发现与本文档前置事实（§2）不符的实测结果，如实报告并交回设计会话，不自行改写方案。

## 8. 交付物汇总

仓库骨架（deploy/ 拷贝改造 + pom.xml 基座 + src 空架 + docs/）、`syn-create-topics.sh`、`syn-clean-topics.sh`、`syn-verify-cluster.sh`、`.env`（含 `SYN_*` 增量段）、`env_acceptance.md`（验收记录）。每个脚本按 CLAUDE.md 五要素交付，含缩写自查。验收增加一项：`mvn clean package` 在空架子上成功产出 `iot-anomaly-detection-1.0-SNAPSHOT.jar`（shade 链路自证，jar 内无业务类属正常）。
