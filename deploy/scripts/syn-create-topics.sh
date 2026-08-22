#!/usr/bin/env bash
# ============================================================================
# syn-create-topics.sh
# 为本项目（iot-anomaly-detection）在旧集群上创建 synergia-* topic（共存策略）。
# Create this project's synergia-* topics on the reused cluster (coexistence strategy).
#
# ---------------------------- 脚本交付五要素 -------------------------------
# ------------------ Five delivery elements (per CLAUDE.md) -----------------
# 1. 执行环境 / Environment:
#      本地 Mac（bash 3.2+），已配 ssh 免密到 fa-master；.env 填好节点 IP/SSH_KEY。
#      Local Mac (bash), passwordless ssh to fa-master, .env with node IPs / SSH_KEY.
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-create-topics.sh
# 3. 前置条件 / Preconditions:
#      集群运行中；deploy/.env 存在且含 SYN_* 段；master 节点 kafka-1 容器在跑。
#      Cluster up; deploy/.env exists with the SYN_* block; kafka-1 container running on master.
# 4. 期望产出 / Expected output:
#      synergia-source(8 分区, RF=2, retention.ms=SYN_RETENTION_MS)、synergia-smoke(1, RF=2)，
#      以及 SYN_EXTRA_TOPICS 列出的 M1 扩展 topic；末尾打印 topic 列表与详情。
# 5. 失败兜底 / Failure fallback:
#      幂等——已存在则 describe 校验 分区数/RF/retention 一致并跳过；不一致则**报错退出，
#      绝不自动 alter**（避免误改旧数据或破坏分区）。任何非 synergia- 前缀 topic 一律不碰。
#      Idempotent: existing topics are verified (partitions/RF/retention) and skipped; on any
#      mismatch the script EXITS WITH ERROR and never auto-alters. Never touches non-synergia- topics.
#
# 缩写自查 / Abbreviations:
#   RF = Replication Factor 副本因子；ms = milliseconds 毫秒；
#   CLI = Command-Line Interface 命令行工具；TM/JM = TaskManager/JobManager。
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"

# 加载 .env（含旧变量段与 SYN_* 段）/ load .env (old block + SYN_* block)
if [[ ! -f "$DEPLOY_DIR/.env" ]]; then
    echo "ERROR: $DEPLOY_DIR/.env not found. 先复制 env.example 并填真实值 / copy env.example first." >&2
    exit 1
fi
set -a; source "$DEPLOY_DIR/.env"; set +a

# 前缀护栏 / prefix guard: 本脚本只创建 synergia- 前缀 topic。
SYN_PREFIX="synergia-"

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
RF="${SYN_TOPIC_REPLICATION:-2}"
RETENTION="${SYN_RETENTION_MS:-86400000}"

# 在 master 的 kafka-1 容器内调用 Kafka CLI / run the Kafka CLI inside kafka-1 on master
kcmd() { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "docker exec kafka-1 $*"; }

# ---------------------------------------------------------------------------
# 构造待建 topic 清单：source / smoke / SYN_EXTRA_TOPICS（"name:partitions" 逗号分隔）
# Build the topic list: source / smoke / SYN_EXTRA_TOPICS ("name:partitions", comma-sep)
# ---------------------------------------------------------------------------
declare -a NAMES=("${SYN_TOPIC_SOURCE:-synergia-source}" "${SYN_TOPIC_SMOKE:-synergia-smoke}")
declare -a PARTS=("${SYN_SOURCE_PARTITIONS:-8}" "1")

if [[ -n "${SYN_EXTRA_TOPICS:-}" ]]; then
    IFS=',' read -ra _extra <<< "$SYN_EXTRA_TOPICS"
    for item in "${_extra[@]}"; do
        item="$(echo "$item" | xargs)"           # 去空白 / trim
        [[ -z "$item" ]] && continue
        name="${item%%:*}"
        part="${item##*:}"
        [[ "$name" == "$part" ]] && part=1        # 未写分区数则默认 1 / default 1 partition
        NAMES+=("$name")
        PARTS+=("$part")
    done
fi

# 前缀护栏：清单中任何非 synergia- 前缀名直接拒绝 / reject any non-synergia- name
for n in "${NAMES[@]}"; do
    if [[ "$n" != ${SYN_PREFIX}* ]]; then
        echo "ERROR: 拒绝创建非 '${SYN_PREFIX}' 前缀 topic: '$n' / refusing non-'${SYN_PREFIX}' topic." >&2
        echo "       本项目脚本只允许操作 synergia-* topic（共存保护）。" >&2
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# 读取已存在 topic 的 分区数 / RF / retention.ms（用于幂等校验）
# Read an existing topic's partitions / RF / retention.ms for the idempotency check.
# ---------------------------------------------------------------------------
describe_summary() {
    # 输出摘要行，含 "PartitionCount: N  ReplicationFactor: M ..." / summary line
    kcmd kafka-topics.sh --bootstrap-server "$BROKERS" --describe --topic "$1" 2>/dev/null | grep -E "PartitionCount:" || true
}
read_retention() {
    # 从 kafka-configs 读取 retention.ms（未显式设置时可能为空）/ read retention.ms via kafka-configs
    kcmd kafka-configs.sh --bootstrap-server "$BROKERS" --entity-type topics --entity-name "$1" \
        --describe 2>/dev/null | grep -oE "retention\.ms=[0-9]+" | head -1 | cut -d= -f2 || true
}

fail=0
echo "===================================="
echo "syn-create-topics: RF=$RF, retention.ms=$RETENTION, brokers=$BROKERS"
echo "===================================="

for i in "${!NAMES[@]}"; do
    t="${NAMES[$i]}"; p="${PARTS[$i]}"
    summary="$(describe_summary "$t")"

    if [[ -z "$summary" ]]; then
        # 不存在 → 创建 / not present → create
        echo "[create] $t (partitions=$p, RF=$RF, retention.ms=$RETENTION)"
        kcmd kafka-topics.sh --bootstrap-server "$BROKERS" \
            --create --if-not-exists \
            --topic "$t" \
            --partitions "$p" \
            --replication-factor "$RF" \
            --config "retention.ms=$RETENTION"
        continue
    fi

    # 已存在 → 校验参数一致 / exists → verify parameters match
    cur_p="$(echo "$summary" | grep -oE "PartitionCount: *[0-9]+" | grep -oE "[0-9]+")"
    cur_rf="$(echo "$summary" | grep -oE "ReplicationFactor: *[0-9]+" | grep -oE "[0-9]+")"
    cur_ret="$(read_retention "$t")"
    ok=1
    [[ "$cur_p" == "$p" ]] || { echo "[MISMATCH] $t partitions: 期望 $p, 实测 $cur_p"; ok=0; }
    [[ "$cur_rf" == "$RF" ]] || { echo "[MISMATCH] $t RF: 期望 $RF, 实测 $cur_rf"; ok=0; }
    if [[ -n "$cur_ret" && "$cur_ret" != "$RETENTION" ]]; then
        echo "[MISMATCH] $t retention.ms: 期望 $RETENTION, 实测 $cur_ret"; ok=0
    fi
    if [[ "$ok" == 1 ]]; then
        echo "[skip] $t 已存在且参数一致 (partitions=$cur_p, RF=$cur_rf, retention.ms=${cur_ret:-<default>})"
    else
        echo "       → 不自动 alter。如需改参数，用 syn-clean-topics.sh 重建，或人工确认后手动 alter。"
        fail=1
    fi
done

if [[ "$fail" == 1 ]]; then
    echo ""
    echo "ERROR: 存在参数不一致的 topic，已按约定报错退出（未做任何 alter）。" >&2
    exit 2
fi

echo ""
echo "[verify] synergia-* topic 列表 / topic list:"
kcmd kafka-topics.sh --bootstrap-server "$BROKERS" --list | grep "^${SYN_PREFIX}" || true

echo ""
echo "[verify] 详情（分区与 leader 分布）/ details (partitions & leader distribution):"
for t in "${NAMES[@]}"; do
    kcmd kafka-topics.sh --bootstrap-server "$BROKERS" --describe --topic "$t" || true
done

echo ""
echo "===================================="
echo "DONE. 下一步 / Next: bash $SCRIPT_DIR/syn-verify-cluster.sh"
echo "===================================="
