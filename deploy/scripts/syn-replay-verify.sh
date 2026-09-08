#!/usr/bin/env bash
# ============================================================================
# syn-replay-verify.sh
# 重放完整性核验（补充指令五 step1）——每次标定/探针运行前的**固定前置门槛**。把 synergia-m1-out
# 转储成 JSONL，在 master 临时 flink 容器里跑 com.leejean.m2.ReplayVerify，四条断言全过才放行。
# Replay-integrity gate: dump synergia-m1-out and run ReplayVerify; proceed only if all four assertions pass.
#
# 四条断言 / four assertions:
#   ① 轮数对账：消费总轮数与 EDA 三月逐日合计一致（需 --expected-total；否则退出码 3，不臆造参照）。
#   ② 零重复：同设备同时间戳零重复（重发在此暴露）。
#   ③ 边界对齐：最早=start、最晚=end−周期（含边界空档容差）。
#   ④ 冻结落第八天：八台标准化冻结时刻落在第 8 天区间（压缩/重发会提前到第 4 天，本条抓它）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash），ssh 免密到 fa-master；master 有 flink 镜像与最新 jar。
# 2. 调用命令 / Invocation:
#      # 前提：干净重放已完成、M1（--calib-days 7）已把整月消费进 synergia-m1-out。
#      bash deploy/scripts/syn-replay-verify.sh --expected-total 2006400
#      bash deploy/scripts/syn-replay-verify.sh --start-utc 2022-03-01T00:00:00Z --end-utc 2022-04-01T00:00:00Z \
#           --calib-days 7 --period-sec 10 --expected-total <EDA三月合计>
# 3. 前置条件 / Preconditions: synergia-m1-out 已含目标整月的标准化 DeviceRound（含 warmup 标记）。
# 4. 期望产出 / Expected output: stdout 打印四条断言 PASS/FAIL + 逐台冻结日；docs/<report-name> 逐设备汇总；
#      退出码 0=全过、1=有失败、3=断言一缺 EDA 参照。**本脚本的退出码就是门槛**（配 && 串联下一步）。
# 5. 失败兜底 / Failure fallback: m1-out 为空则提示先跑 M1；任一断言失败即非零退出、拦住后续标定/探针。
#
# 缩写自查 / Abbreviations: EDA = 探索性数据分析；JSONL = 每行一条 JSON；ts = 记录 epoch 秒；UTC = 协调世界时。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

MAX_MESSAGES=3000000
START_UTC="2022-03-01T00:00:00Z"
END_UTC="2022-04-01T00:00:00Z"
CALIB_DAYS="${SYN_M1_CALIB_DAYS:-7}"
PERIOD_SEC=10
# EDA 三月逐日轮数合计（权威值）；缺省取 .env 的 SYN_EDA_MARCH_ROUNDS_TOTAL（空则不传→退出码 3 提示补参）。
EXPECTED_TOTAL="${SYN_EDA_MARCH_ROUNDS_TOTAL:-}"
TOL_PCT=2.0
REPORT_NAME="m2_replay_verify.csv"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --max-messages) MAX_MESSAGES="$2"; shift 2 ;;
        --start-utc) START_UTC="$2"; shift 2 ;;
        --end-utc) END_UTC="$2"; shift 2 ;;
        --calib-days) CALIB_DAYS="$2"; shift 2 ;;
        --period-sec) PERIOD_SEC="$2"; shift 2 ;;
        --expected-total) EXPECTED_TOTAL="$2"; shift 2 ;;
        --tol-pct) TOL_PCT="$2"; shift 2 ;;
        --report-name) REPORT_NAME="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
JAR_NAME="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"
WORK="${REMOTE_HOME}/m2probe"
JSONL="$WORK/m1out.jsonl"

on_master() { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "$@"; }

echo "===================================="
echo "[verify] 转储 synergia-m1-out（最多 ${MAX_MESSAGES} 条）→ $JSONL"
echo "===================================="
on_master "mkdir -p $WORK && chmod 777 $WORK && docker exec kafka-1 kafka-console-consumer.sh \
    --bootstrap-server $BROKERS --topic ${SYN_TOPIC_M1_OUT:-synergia-m1-out} \
    --from-beginning --max-messages $MAX_MESSAGES --timeout-ms 30000 > $JSONL 2>/dev/null || true"

LINES=$(on_master "wc -l < $JSONL 2>/dev/null || echo 0"); LINES=$(echo "$LINES" | tr -d '[:space:]')
if [ "${LINES:-0}" -eq 0 ]; then
    echo "ERROR: $JSONL 为空——synergia-m1-out 无数据。先干净重放并跑 M1（--calib-days 7）。" >&2
    exit 2
fi
echo "[verify] 转储 ${LINES} 行；运行 ReplayVerify（start=$START_UTC end=$END_UTC calib-days=$CALIB_DAYS）"

EXP_ARG=""
if [ -n "$EXPECTED_TOTAL" ]; then
    EXP_ARG="--expected-total $EXPECTED_TOTAL"
else
    echo "[verify] 注意：未提供 --expected-total（也未在 .env 设 SYN_EDA_MARCH_ROUNDS_TOTAL）；" >&2
    echo "         断言一将判为'无参照'、脚本以退出码 3 结束。请补 EDA 三月逐日轮数合计后重跑。" >&2
fi

# 在临时容器里跑核验；容器/ssh 的退出码即断言门槛（0 全过 / 1 有失败 / 3 无 EDA 参照）。
on_master "docker run --rm --user root \
    -v ${REMOTE_HOME}/jars:/jars:ro -v $WORK:/work \
    fa-iforest/flink:${FLINK_VERSION} \
    java -cp /jars/$JAR_NAME com.leejean.m2.ReplayVerify \
        --rounds-jsonl /work/m1out.jsonl \
        --start-utc $START_UTC --end-utc $END_UTC \
        --calib-days $CALIB_DAYS --period-sec $PERIOD_SEC --tol-pct $TOL_PCT \
        $EXP_ARG --report-out /work/m2_replay_verify.csv"
VERIFY_RC=$?

# 拉回逐设备汇总（不论通过与否都拉回，便于留档）/ pull back the per-device report regardless
LOCAL_REPORT="$PROJECT_ROOT/docs/$REPORT_NAME"
if on_master "cat $WORK/m2_replay_verify.csv" > "$LOCAL_REPORT" 2>/dev/null && [ -s "$LOCAL_REPORT" ]; then
    echo "[verify] 逐设备汇总已拉回：${LOCAL_REPORT}"
else
    rm -f "$LOCAL_REPORT" 2>/dev/null || true
fi

echo "===================================="
case "$VERIFY_RC" in
    0) echo "✅ 四条断言全部通过——**允许**进入标定/探针（step2）。" ;;
    3) echo "⛔ 退出码 3：断言一缺 EDA 参照。请用 --expected-total <三月逐日合计> 或 .env 的 SYN_EDA_MARCH_ROUNDS_TOTAL 重跑。" ;;
    *) echo "⛔ 退出码 $VERIFY_RC：有断言未通过——**拦住**后续标定/探针，请先解决重放完整性问题（见上方逐条与 docs/$REPORT_NAME）。" ;;
esac
exit "$VERIFY_RC"
