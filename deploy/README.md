# 部署骨架 / Deployment skeleton — iot-anomaly-detection（ENV 阶段）

> 本目录自 `FA-iForest-master/deploy/` 拷贝改造（DEV-D9），以**共存策略**在既有三节点旧集群上
> 为本项目建立运行命名空间：**不改动任何旧项目容器、镜像、topic 与数据**。
> Copied and adapted from `FA-iForest-master/deploy/` (DEV-D9). It runs this project on the
> existing 3-node cluster under a **coexistence strategy**: no old container, image, topic, or
> datum is ever modified.

## 目录 / Layout

```
deploy/
  compose/
    docker-compose.master.yml    # 旧骨架，原样保留 / inherited verbatim
    docker-compose.worker.yml    # 旧骨架，原样保留
    prometheus.yml.template      # 旧骨架，原样保留
  docker/Dockerfile.flink        # 旧骨架，原样保留
  scripts/
    0-prepare-local.sh … 9-teardown.sh, refresh-ips.sh   # 旧脚本，原样保留，M1 按需适配
    syn-create-topics.sh         # 【新】创建 synergia-* topic（幂等，参数不一致报错不 alter）
    syn-clean-topics.sh          # 【新】仅清理 synergia-* topic（硬编码前缀白名单）
    syn-verify-cluster.sh        # 【新】ENV 验收一键脚本 → 生成 docs/env_acceptance.md
  env.example                    # 环境变量模板（含 SYN_* 增量段）/ env template (with SYN_* block)
  .env                           # 【本地，git 忽略】真实 IP/密钥路径 / local, gitignored
```

**旧脚本为何原样保留 / why the old scripts are kept verbatim**：交接文档 §0 规定拷贝后仅做两类
修改（新增 `syn-*` 脚本、`.env` 追加 `SYN_*` 段），其余脚本 M1 阶段按需适配。它们仍引用 `.env`，
对本项目直接可用（如 `refresh-ips.sh` 刷新公网 IP、`0-prepare-local.sh` 构镜像/渲染 prometheus）。

## 首次设置 / First-time setup

`.env` 含真实节点 IP、`SSH_USER`、SSH 密钥路径，属机器本地配置，**不入公开仓库**（已 gitignore）。
本地据模板生成：

```bash
cd deploy
cp env.example .env
# 编辑 .env：填 NODE_*_IP / NODE_*_PUBLIC_IP / SSH_USER / SSH_KEY
#   —— 或直接复用旧仓库 FA-iForest-master/deploy/.env 的对应值（同一集群）。
# SYN_* 段已在 env.example 内，按需微调 retention / 额外 topic 清单即可。
```

> `.env` includes real node IPs, `SSH_USER`, and the SSH key path — machine-local, gitignored,
> never committed to this public repo. Create it from the template (or reuse the old repo's
> `.env` for the same cluster). The `SYN_*` block already ships inside `env.example`.

## 本阶段流程 / ENV-stage flow

```bash
# 1. 建 synergia-* topic（synergia-source: 8 分区 RF2 24h; synergia-smoke: 1 分区 RF2）
bash deploy/scripts/syn-create-topics.sh

# 2. 一键验收（跑交接文档 §6 全部步骤，生成 docs/env_acceptance.md）
bash deploy/scripts/syn-verify-cluster.sh

# 3. 实验间清零（仅 synergia-*，旧 topic 不受影响）
bash deploy/scripts/syn-clean-topics.sh
```

## 共存规则（重要）/ Coexistence rules (important)

- **只碰 `synergia-` 前缀 topic**：`syn-create` / `syn-clean` 均内建前缀白名单，任何非
  `synergia-` 名（如旧的 `source-topic` / `tree-topic`）一律拒绝执行。
- **集群侧目录归旧基础设施**：`REMOTE_HOME=/opt/fa-iforest` 保持不变；本项目 fat jar 与数据集
  仍落 `/opt/fa-iforest/jars`、`/opt/fa-iforest/datasets`，以文件名 `iot-anomaly-detection-*`
  与 `FA-iForest-*` 区分（旧 jobmanager 容器已固化挂载该目录，DEV-D8 禁止重建容器）。
- **不 rebuild / 不重命名镜像容器，不改旧 compose 与旧脚本，不 cancel 旧 job**（验收只读）。

## 分区映射预告 / partition-mapping note (for M1)

Kafka 默认按 `hash(key) % partitions` 分区，8 个 DeviceId（A–H）经哈希可能碰撞（两设备同分区、
部分分区空置）。这不破坏正确性（同 key 恒同分区，设备内保序成立）。M1 重放器将用**显式
partitioner**（A→0 … H→7）实现"一设备一分区"的字面语义与均衡负载；本阶段冒烟用 console 工具按
默认哈希即可，实测映射由 `syn-verify-cluster.sh` 记入 `docs/env_acceptance.md`。
