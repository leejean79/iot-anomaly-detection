#!/usr/bin/env bash
# ============================================================================
# syn-clean-topics.sh
# 仅清理本项目 synergia-* topic（delete → 等待 → 按原参数 recreate），用于实验间隔离。
# Clean ONLY this project's synergia-* topics (delete → wait → recreate), for run isolation.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash），ssh 免密到 fa-master；.env 含 SYN_* 段。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-clean-topics.sh            # 清理 source + smoke + SYN_EXTRA_TOPICS
#      bash deploy/scripts/syn-clean-topics.sh --yes      # 跳过交互确认（供自动化调用）
# 3. 前置条件 / Preconditions: 集群运行中；kafka-1 容器在 master 上跑。
# 4. 期望产出 / Expected output: synergia-* 全部删除并按 .env 原参数重建，量清零。
# 5. 失败兜底 / Failure fallback:
#      **硬编码前缀白名单校验**——任何非 'synergia-' 前缀的 topic 名一律拒绝、立即退出；
#      因此本脚本在设计上无法删除任何旧项目 topic（source-topic / tree-topic 等）。
#      删除是异步的，重建带 3 次重试（撞 "marked for deletion" 时退避重试）。
#      Hardcoded prefix whitelist: any non-'synergia-' name is refused and the script exits,
#      so it structurally cannot delete the old project's topics. Recreate retries 3x.
#
# 与 syn-create-topics.sh 的区别 / difference:
#   create 是幂等保守（存在即校验跳过，不删）；clean 是破坏性重置（总是 delete→create），
#   仅用于"实验间从零开始"。生产/长跑数据勿用。
#   create is conservative & idempotent; clean is a destructive reset for inter-run isolation only.
#
# 缩写自查 / Abbreviations: RF = Replication Factor 副本因子；ms = milliseconds 毫秒。
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"

if [[ ! -f "$DEPLOY_DIR/.env" ]]; then
    echo "ERROR: $DEPLOY_DIR/.env not found." >&2
    exit 1
fi
set -a; source "$DEPLOY_DIR/.env"; set +a

# 前缀白名单（硬编码，安全护栏）/ hardcoded prefix whitelist (safety guard)
SYN_PREFIX="synergia-"

ASSUME_YES=false
[[ "${1:-}" == "--yes" ]] && ASSUME_YES=true

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
RF="${SYN_TOPIC_REPLICATION:-2}"
RETENTION="${SYN_RETENTION_MS:-86400000}"

kcmd() { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "docker exec kafka-1 $*"; }

# 待清理清单（与 create 同源）/ target list (same source as create)
declare -a NAMES=("${SYN_TOPIC_SOURCE:-synergia-source}" "${SYN_TOPIC_SMOKE:-synergia-smoke}")
declare -a PARTS=("${SYN_SOURCE_PARTITIONS:-8}" "1")
if [[ -n "${SYN_EXTRA_TOPICS:-}" ]]; then
    IFS=',' read -ra _extra <<< "$SYN_EXTRA_TOPICS"
    for item in "${_extra[@]}"; do
        item="$(echo "$item" | xargs)"; [[ -z "$item" ]] && continue
        name="${item%%:*}"; part="${item##*:}"; [[ "$name" == "$part" ]] && part=1
        NAMES+=("$name"); PARTS+=("$part")
    done
fi

# 前缀护栏：任何非 synergia- 名 → 拒绝并退出（安全第一）/ refuse any non-synergia- name
for n in "${NAMES[@]}"; do
    if [[ "$n" != ${SYN_PREFIX}* ]]; then
        echo "FATAL: 清单含非 '${SYN_PREFIX}' 前缀 topic: '$n' —— 拒绝执行以保护旧项目 topic。" >&2
        echo "FATAL: refusing to run; a non-'${SYN_PREFIX}' topic in the list would risk old data." >&2
        exit 1
    fi
done

echo "===================================="
echo "syn-clean-topics: 将删除并重建以下 synergia-* topic / will DELETE and recreate:"
for i in "${!NAMES[@]}"; do echo "  - ${NAMES[$i]} (partitions=${PARTS[$i]}, RF=$RF)"; done
echo "旧项目 topic（source-topic / tree-topic / model-topic / output-scores /"
echo "feature-drift-topic / drift-round-topic）不受影响 / old topics are NOT touched."
echo "===================================="
if ! $ASSUME_YES; then
    read -p "确认清空以上 synergia-* topic 数据? [y/N] / Confirm wiping these topics? " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || { echo "已取消 / aborted"; exit 0; }
fi

echo ""
echo "=== delete ==="
for t in "${NAMES[@]}"; do
    echo "[delete] $t"
    kcmd kafka-topics.sh --bootstrap-server "$BROKERS" --delete --topic "$t" --if-exists || true
done

echo "等待 10s 让删除传播 / waiting 10s for delete to propagate..."
sleep 10

echo ""
echo "=== recreate ==="
for i in "${!NAMES[@]}"; do
    t="${NAMES[$i]}"; p="${PARTS[$i]}"
    echo "[create] $t (partitions=$p, RF=$RF, retention.ms=$RETENTION)"
    for attempt in 1 2 3; do
        if kcmd kafka-topics.sh --bootstrap-server "$BROKERS" \
            --create --topic "$t" \
            --partitions "$p" \
            --replication-factor "$RF" \
            --config "retention.ms=$RETENTION" 2>/dev/null; then
            break
        fi
        if [[ "$attempt" -lt 3 ]]; then
            echo "  创建失败（可能仍在删除），$attempt/3 退避 5s 重试 / retry after 5s..."
            sleep 5
        else
            echo "  ERROR: $t 重试 3 次仍失败 / failed after 3 attempts" >&2
            exit 1
        fi
    done
done

echo ""
echo "=== verify ==="
kcmd kafka-topics.sh --bootstrap-server "$BROKERS" --list | grep "^${SYN_PREFIX}" || true
echo "DONE."
