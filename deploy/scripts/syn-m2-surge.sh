#!/usr/bin/env bash
# ============================================================================
# syn-m2-surge.sh
# DF-12 六月停机恢复浪涌留档（收尾任务二）：转储 synergia-monitoring 的六月段，本地跑 m2_surge.py，
# 产出逐设备浪涌量化（峰值/基线/衰减/同时性/冷启动先后）+ 时间线 CSV。
# Dump the June segment of synergia-monitoring and run m2_surge.py locally to quantify the DF-12
# recovery surge (peak/baseline/decay/simultaneity/cold-clear timing) + a timeline CSV.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash + python3），ssh 免密到 fa-master。
# 2. 调用命令 / Invocation:
#      # 前提：先用 M2Job 重放跨 DF-12 的六月段（见文末顺序），monitoring 里已有六月 M2 快照。
#      bash deploy/scripts/syn-m2-surge.sh
#      bash deploy/scripts/syn-m2-surge.sh --outage-end 2022-06-13T02:20:00Z --outage-hours 102.26
# 3. 前置条件 / Preconditions: 六月段已重放进 synergia-source 并被 M2Job 处理；jar 含 m2ColdCleared 字段。
# 4. 期望产出 / Expected output: docs/m2_surge_stats.csv、docs/m2_surge_timeline.csv + stdout 四问量化。
# 5. 失败兜底 / Failure fallback: monitoring 转储为空 → 提示先重放六月并等 M2 产出；转储用 --timeout-ms 避免挂起。
#
# 缩写自查 / Abbreviations: DF-12 = 数据事实 12（六月大停机）；W = 窗长；baseline = 常态基线。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

OUTAGE_END="2022-06-13T02:20:00Z"
OUTAGE_HOURS="102.26"
WINDOW_SEC="${SYN_M2_WINDOW_SEC:-3600}"
MAX_MESSAGES=3000000
while [[ $# -gt 0 ]]; do
    case "$1" in
        --outage-end) OUTAGE_END="$2"; shift 2 ;;
        --outage-hours) OUTAGE_HOURS="$2"; shift 2 ;;
        --window-sec) WINDOW_SEC="$2"; shift 2 ;;
        --max-messages) MAX_MESSAGES="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
MON_TOPIC="${SYN_TOPIC_MONITORING:-synergia-monitoring}"
WORK="${REMOTE_HOME}/m2surge"
REMOTE_JSONL="$WORK/monitoring.jsonl"
LOCAL_JSONL="$PROJECT_ROOT/docs/m2_monitoring_june.jsonl"

on_master() { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "$@"; }

echo "===================================="
echo "[surge] 转储 ${MON_TOPIC}（最多 ${MAX_MESSAGES} 条）→ ${REMOTE_JSONL}"
echo "===================================="
# --timeout-ms 兜底：console-consumer 若等不满 --max-messages 会挂住；chmod 777 供容器 uid 9999 可写。
on_master "mkdir -p $WORK && chmod 777 $WORK && docker exec kafka-1 kafka-console-consumer.sh \
    --bootstrap-server $BROKERS --topic $MON_TOPIC \
    --from-beginning --max-messages $MAX_MESSAGES --timeout-ms 30000 > $REMOTE_JSONL 2>/dev/null || true"

LINES=$(on_master "wc -l < $REMOTE_JSONL 2>/dev/null || echo 0")
LINES=$(echo "$LINES" | tr -d '[:space:]')
if [ "${LINES:-0}" -eq 0 ]; then
    echo "ERROR: $MON_TOPIC 转储为空。先用 M2Job 重放六月段并等 M2 产出：" >&2
    echo "       syn-submit-m2.sh 然后 syn-replay.sh --speedup 3600 --start 2022-06-05 --end 2022-06-20" >&2
    exit 2
fi
echo "[surge] 转储 ${LINES} 行；拉回本地并分析"

# 拉回本地（ssh cat 稳）/ pull back via ssh cat
if ! on_master "cat $REMOTE_JSONL" > "$LOCAL_JSONL" 2>/dev/null || [ ! -s "$LOCAL_JSONL" ]; then
    echo "ERROR: 拉回 monitoring 转储失败。可手动：ssh $SSH_USER@$MASTER_SSH \"cat $REMOTE_JSONL\" > $LOCAL_JSONL" >&2
    exit 3
fi

python3 "$SCRIPT_DIR/m2_surge.py" \
    --monitoring-jsonl "$LOCAL_JSONL" \
    --outage-end "$OUTAGE_END" --outage-hours "$OUTAGE_HOURS" --window-sec "$WINDOW_SEC" \
    --stats-csv "$PROJECT_ROOT/docs/m2_surge_stats.csv" \
    --timeline-csv "$PROJECT_ROOT/docs/m2_surge_timeline.csv"

echo "===================================="
echo "[surge] 完成。产出："
echo "  docs/m2_surge_stats.csv     （逐设备 基线/峰值/倍数/衰减/冷启动）"
echo "  docs/m2_surge_timeline.csv  （停机前2h~恢复后 的离群率时间线，画图用）"
echo "  上方 stdout 四问量化可直接誊入 docs/m2_df12_surge.md"
