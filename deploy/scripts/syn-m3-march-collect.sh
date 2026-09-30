#!/usr/bin/env bash
# ============================================================================
# syn-m3-march-collect.sh
# 三月重放（第二批在线验收）结束后，把验收要用的原始材料一次收齐：synergia-scores 与
# synergia-monitoring 的转储、注入地面真值、两台 TaskManager 日志中的 M3 训练记录，并拉回本地。
# Collect the raw material for the March online acceptance after the replay: dumps of synergia-scores
# and synergia-monitoring, the injection ground truth, and the M3 training lines from both TaskManagers.
#
# 依据：2026-09-28 裁决书第三节（同一次重放承载 V-M3-4、V-M3-5、V-M3-6 与等值核验）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录；ssh 别名 fa-master、fa-worker1、fa-worker2 可用。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m3-march-collect.sh
#      bash deploy/scripts/syn-m3-march-collect.sh --out-dir docs/m3_march --max-messages 5000000
# 3. 前置条件 / Preconditions: 重放已结束且作业已排空（两次 syn-m2-metrics.sh 读数相同）；三台 Kafka
#    代理都在运行；**在 syn-reset-env.sh 清理之前执行**（清理会删掉主题与 TaskManager 重启前的日志）。
# 4. 期望产出 / Expected output: master 上 ${REMOTE_HOME}/m3march/ 下的两份转储（monitoring 较大，留在
#    master）；本地 --out-dir 下 scores.jsonl、m3_scores.jsonl（仅上下文通道记录）、inject-truth.csv、
#    m3_tm_log.txt（M3 训练与上报行）与 collect_summary.txt。
# 5. 失败兜底 / Failure fallback: 某个主题读到 0 条时保留原文件不覆盖，并打印消费者错误与 kafka 容器状态；
#    注入真值缺失时注明（说明重放没有带 --inject-file，或 replay-state 已被清理）。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

OUT_DIR="$PROJECT_ROOT/docs/m3_march"
MAX_MESSAGES=5000000
while [[ $# -gt 0 ]]; do
    case "$1" in
        --out-dir) OUT_DIR="$2"; shift 2 ;;
        --max-messages) MAX_MESSAGES="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

RHOME="${REMOTE_HOME:-/opt/fa-iforest}"
WORK="$RHOME/m3march"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
TRUTH="${SYN_REPLAY_STATE_DIR:-$RHOME/replay-state}/inject-truth.csv"
mkdir -p "$OUT_DIR"

# 先写临时文件，非空才替换，读到 0 条时保留原文件并给出原因（与 syn-m3-grid.sh 同一做法）。
# Temp file first, replace only when non-empty (same approach as syn-m3-grid.sh).
dump_topic() {   # $1 = topic, $2 = 文件名 / file name
    ssh fa-master "mkdir -p $WORK && chmod 777 $WORK && docker exec kafka-1 kafka-console-consumer.sh \
        --bootstrap-server $BROKERS --topic $1 --from-beginning --max-messages $MAX_MESSAGES \
        --timeout-ms 60000 > $WORK/$2.tmp 2> $WORK/$2.err || true"
    local n
    n="$(ssh fa-master "wc -l < $WORK/$2.tmp 2>/dev/null || echo 0" | tr -d '[:space:]')"
    if [ "${n:-0}" -gt 0 ]; then
        ssh fa-master "mv -f $WORK/$2.tmp $WORK/$2 && rm -f $WORK/$2.err"
        echo "[collect] $1：$n 行 → master:$WORK/$2"
    else
        echo "[collect] ⚠ $1 读到 0 条，保留原有文件不动。消费者错误输出（末 15 行）：" >&2
        ssh fa-master "tail -n 15 $WORK/$2.err 2>/dev/null | sed 's/^/    /'; docker ps --format '    容器 {{.Names}}：{{.Status}}' | grep -i kafka" >&2
    fi
}

echo "[collect] 转储 synergia-scores 与 synergia-monitoring（各最多 $MAX_MESSAGES 条）……"
dump_topic "${SYN_TOPIC_SCORES:-synergia-scores}" scores.jsonl
dump_topic "${SYN_TOPIC_MONITORING:-synergia-monitoring}" monitoring.jsonl

# scores 较小，整份拉回；另抽出上下文通道记录供 V-M3-4 与 V-M3-5 使用。monitoring 较大，留在 master。
ssh fa-master "cat $WORK/scores.jsonl 2>/dev/null" > "$OUT_DIR/scores.jsonl"
grep '"m3_context"' "$OUT_DIR/scores.jsonl" > "$OUT_DIR/m3_scores.jsonl" || true

if ssh fa-master "test -s $TRUTH"; then
    ssh fa-master "cat $TRUTH" > "$OUT_DIR/inject-truth.csv"
    echo "[collect] 注入地面真值 → $OUT_DIR/inject-truth.csv"
else
    echo "[collect] ⚠ master 上没有 ${TRUTH}：重放没有带 --inject-file，或 replay-state 已被清理。" >&2
fi

# M3 训练记录：逐设备的轮数、最后改善轮次、最长平台、早停集误差、耗时，以及 REPORT 警告。
: > "$OUT_DIR/m3_tm_log.txt"
for pair in "fa-worker1 taskmanager-2" "fa-worker2 taskmanager-3"; do
    set -- $pair
    echo "===== $1 / $2 =====" >> "$OUT_DIR/m3_tm_log.txt"
    ssh "$1" "docker logs $2 2>&1 | grep -E '\[M3\] Device .* (entering|trained|REPORT|calibrated)|OpenMP BLAS|threads used for'" \
        >> "$OUT_DIR/m3_tm_log.txt" 2>/dev/null || true
done

{
    echo "收集时间：$(date '+%F %T %Z')"
    echo "scores 行数：$(wc -l < "$OUT_DIR/scores.jsonl" | tr -d ' ')"
    echo "其中上下文通道（m3_context）：$(wc -l < "$OUT_DIR/m3_scores.jsonl" | tr -d ' ')"
    echo "monitoring 行数（留在 master）：$(ssh fa-master "wc -l < $WORK/monitoring.jsonl 2>/dev/null || echo 0" | tr -d '[:space:]')"
    echo "注入真值行数（含表头）：$(wc -l < "$OUT_DIR/inject-truth.csv" 2>/dev/null | tr -d ' ' || echo 0)"
    echo "M3 训练完成的设备数：$(grep -c 'trained:' "$OUT_DIR/m3_tm_log.txt")"
    echo "REPORT 警告数：$(grep -c 'REPORT' "$OUT_DIR/m3_tm_log.txt")"
} | tee "$OUT_DIR/collect_summary.txt"
