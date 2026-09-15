#!/usr/bin/env bash
# ============================================================================
# syn-m1-reconcile.sh
# 单日重放后的读入/产出侧偏移量对账：把 synergia-source 的逐分区末端偏移量与 synergia-m1-out 的
# 轮数，同 docs/m1_acceptance.md 记录的 Java 8 基线（V-M1-1，选定日 2022-05-21）逐一比对。
# Post-replay offset reconciliation: compare per-partition end offsets of synergia-source and the
# round count on synergia-m1-out against the Java 8 baseline recorded in docs/m1_acceptance.md
# (V-M1-1, selected day 2022-05-21).
#
# 只读脚本：只调用 Kafka 的 GetOffsetShell 查询末端偏移量，不消费、不提交位移、不改任何 topic。
# Read-only: it only queries end offsets via GetOffsetShell; it consumes nothing and commits nothing.
#
# 计数器侧（M1 守卫计数、M2 离群/点数）不在本脚本范围内——那部分由已有的 syn-m2-metrics.sh
# 经 Flink REST 拉取并做 V-M2-2 对账，此处不重复实现。
# Counter-side checks (M1 guards, M2 outlier/point totals) are out of scope: the existing
# syn-m2-metrics.sh already pulls those over the Flink REST API; this script does not duplicate it.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash），ssh 免密到 fa-master；deploy/.env 已配置。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m1-reconcile.sh                  # 与内置 2022-05-21 基线比对
#      bash deploy/scripts/syn-m1-reconcile.sh --no-baseline    # 只打印实测值，不做比对（换其它日期时用）
# 3. 前置条件 / Preconditions: 已清空 topic 与作业状态后重放完毕；M1Job（及 M2Job）已消费完毕，
#    即 syn-replay.sh 已 rc=0 退出，且 Flink UI 上算子的 Records Received 不再增长。
# 4. 期望产出 / Expected output: 逐分区对账表 + 总计行 + m1-out 轮数行；全部相等则退出码 0。
# 5. 失败兜底 / Failure fallback: 偏移量大于基线通常意味着 topic 未清空或重放了不止一次；
#    小于基线则查 syn-replay.sh 日志的 Produced 行与作业是否中途失败。退出码 1 表示存在差异。
#
# 缩写自查 / Abbreviations: GetOffsetShell = Kafka 自带的偏移量查询工具；
#   末端偏移量 end offset = 分区中下一条待写入消息的位移，topic 从零开始时即等于消息总数。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
SRC_TOPIC="${SYN_TOPIC_SOURCE:-synergia-source}"
OUT_TOPIC="${SYN_TOPIC_M1_OUT:-synergia-m1-out}"

master() { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "$@"; }

USE_BASELINE=true
[ "${1:-}" = "--no-baseline" ] && USE_BASELINE=false

# Java 8 基线，取自 docs/m1_acceptance.md V-M1-1 的逐分区对账表（选定日 2022-05-21）。
# 顺序即分区号 0..7，对应设备 A..H；H 当日处于停机段故为 0（DF 事实，非丢失）。
# Java 8 baseline from the V-M1-1 per-partition table in docs/m1_acceptance.md (day 2022-05-21).
# Index = partition 0..7 = devices A..H; H is 0 because it was in a downtime window that day.
BASE_PART=(65688 65512 65568 65712 65720 65592 65680 0)
BASE_DEVICE=(A B C D E F G H)
BASE_SOURCE_TOTAL=459472
BASE_M1OUT_ROUNDS=57442

# 查询一个 topic 的逐分区末端偏移量，输出 "分区<TAB>偏移量" 行。
# Query per-partition end offsets for a topic; emits "partition<TAB>offset" lines.
# GetOffsetShell 的 --time -1 表示"末端"（latest）。输出形如 topic:partition:offset。
# --time -1 means "latest"; the tool prints topic:partition:offset.
end_offsets() {
    local topic=$1
    master "docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list $BROKERS --topic $topic --time -1" 2>/dev/null \
        | awk -F: 'NF==3 {print $2"\t"$3}' | sort -n
}

echo "===================================================================="
echo "syn-m1-reconcile.sh — 单日重放偏移量对账 / post-replay offset reconciliation"
echo "  source topic: $SRC_TOPIC    out topic: $OUT_TOPIC"
if [ "$USE_BASELINE" = true ]; then
    echo "  基线 / baseline: docs/m1_acceptance.md V-M1-1, day 2022-05-21 (Java 8)"
else
    echo "  基线比对已关闭（--no-baseline）/ baseline comparison disabled"
fi
echo "===================================================================="

SRC_RAW="$(end_offsets "$SRC_TOPIC")"
if [ -z "$SRC_RAW" ]; then
    echo "ERROR: 读不到 $SRC_TOPIC 的偏移量。检查 master 上 kafka-1 容器是否在跑。" >&2
    echo "ERROR: no offsets for $SRC_TOPIC — is the kafka-1 container running on master?" >&2
    exit 2
fi

FAIL=0
SRC_TOTAL=0
echo ""
if [ "$USE_BASELINE" = true ]; then
    printf "%-10s %-8s %14s %14s %8s\n" "partition" "device" "实测/actual" "基线/baseline" "差/diff"
else
    printf "%-10s %14s\n" "partition" "实测/actual"
fi
while IFS=$'\t' read -r part off; do
    SRC_TOTAL=$((SRC_TOTAL + off))
    if [ "$USE_BASELINE" = true ] && [ "$part" -lt ${#BASE_PART[@]} ]; then
        base=${BASE_PART[$part]}
        diff=$((off - base))
        [ "$diff" -ne 0 ] && FAIL=1
        printf "%-10s %-8s %14s %14s %8s\n" "$part" "${BASE_DEVICE[$part]}" "$off" "$base" "$diff"
    else
        printf "%-10s %14s\n" "$part" "$off"
    fi
done <<< "$SRC_RAW"

echo ""
if [ "$USE_BASELINE" = true ]; then
    d=$((SRC_TOTAL - BASE_SOURCE_TOTAL)); [ "$d" -ne 0 ] && FAIL=1
    printf "%-19s %14s %14s %8s\n" "source 合计/total" "$SRC_TOTAL" "$BASE_SOURCE_TOTAL" "$d"
else
    printf "%-19s %14s\n" "source 合计/total" "$SRC_TOTAL"
fi

# m1-out 为单分区（env.example 的 SYN_EXTRA_TOPICS 建为 synergia-m1-out:1），合计即轮数。
# m1-out is single-partition (created as synergia-m1-out:1), so the sum is the round count.
OUT_TOTAL=0
while IFS=$'\t' read -r _ off; do
    [ -n "$off" ] && OUT_TOTAL=$((OUT_TOTAL + off))
done <<< "$(end_offsets "$OUT_TOPIC")"
if [ "$USE_BASELINE" = true ]; then
    d=$((OUT_TOTAL - BASE_M1OUT_ROUNDS)); [ "$d" -ne 0 ] && FAIL=1
    printf "%-19s %14s %14s %8s\n" "m1-out 轮数/rounds" "$OUT_TOTAL" "$BASE_M1OUT_ROUNDS" "$d"
else
    printf "%-19s %14s\n" "m1-out 轮数/rounds" "$OUT_TOTAL"
fi

echo ""
echo "===================================================================="
if [ "$USE_BASELINE" != true ]; then
    echo "实测值已打印（未做基线比对）/ actual values printed, no baseline comparison."
    exit 0
elif [ "$FAIL" -eq 0 ]; then
    echo "[PASS] 逐分区、合计与 m1-out 轮数全部与 Java 8 基线相等。"
    echo "[PASS] every partition, the total and the m1-out round count match the Java 8 baseline."
else
    echo "[FAIL] 存在差异，见上表。常见原因："
    echo "   大于基线 → topic 未彻底清空，或重放执行了不止一次（旧数据与新数据叠加）；"
    echo "   小于基线 → 重放未跑完（查 syn-replay.sh 的 Produced 行），或作业中途失败。"
    echo "[FAIL] mismatch above: higher than baseline means the topic was not cleared or the replay ran"
    echo "       twice; lower means the replay did not finish or the job failed mid-way."
fi
echo "计数器侧对账请另跑 / for the counter side run: bash $SCRIPT_DIR/syn-m2-metrics.sh"
echo "===================================================================="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
