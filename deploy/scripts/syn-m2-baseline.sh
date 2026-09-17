#!/usr/bin/env bash
# ============================================================================
# syn-m2-baseline.sh
# 转储 synergia-monitoring，本地跑 m2_device_baseline.py，产出 M2 逐设备基线表 + Java 8 等值核验 + 图。
# Dump synergia-monitoring and run m2_device_baseline.py locally to produce the per-device M2 baseline,
# the Java 8 equality check and the figure.
#
# 为什么需要它 / Why this exists:
#   三月全月重放 runbook 第 4.2 与第 5 节要求的逐设备平均离群率、微簇占用率、邻居数 P10/P50 来自
#   **作业侧**的 M2 监测快照；已有脚本覆盖不到——syn-m2-metrics.sh 只给全局计数器（Flink REST），
#   m2_probe_compare.py 只吃离线探针 CSV，m2_surge.py 是为停机浪涌形态写的。本脚本沿用
#   syn-m2-surge.sh 的转储模式，只替换本地分析环节。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash + python3），工作目录为项目根目录；ssh 免密到 fa-master。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m2-baseline.sh
#      bash deploy/scripts/syn-m2-baseline.sh --tag march --max-messages 400000
#      bash deploy/scripts/syn-m2-baseline.sh --reuse-dump      # 复用上次转储，只重跑分析
# 3. 前置条件 / Preconditions: 三月重放已跑完且 M1/M2 作业已排空（syn-m2-metrics.sh 两次读数一致）；
#    synergia-monitoring 里已有 M2 快照（windowEnd>0）。**必须在 syn-clean-topics.sh 之前运行**——
#    清理会删掉本脚本要读的监测数据。
# 4. 期望产出 / Expected output:
#      docs/m2_monitoring_<tag>.jsonl                       监测转储（原始，供复算）
#      docs/reports/m2_java11_<tag>_per_device.csv / .md    逐设备基线表
#      docs/reports/m2_java11_<tag>_rate_cmp.svg            Java 8 与 Java 11 并排图
#    退出码 0 = 逐设备等值核验通过；1 = 有设备超容差；2 = 转储里没有 M2 快照。
# 5. 失败兜底 / Failure fallback: 转储为空或只有 M1 快照（windowEnd=0）→ 多半是 M2 整段仍在预热
#    （标定窗口 7 天 = 每设备 60,480 轮），确认重放时段是否足够长；console-consumer 用 --timeout-ms
#    兜底避免挂起；转储过大时用 --max-messages 限制。
#
# 缩写自查 / Abbreviations: JSONL = 每行一个 JSON 对象；tag = 本次运行的标识后缀（进文件名）。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

TAG="march"; MAX_MESSAGES=2000000; TIMEOUT_MS=60000; REUSE=false; TOL_REL=1.0
while [ $# -gt 0 ]; do
    case "$1" in
        --tag) TAG="$2"; shift 2 ;;
        --max-messages) MAX_MESSAGES="$2"; shift 2 ;;
        --timeout-ms) TIMEOUT_MS="$2"; shift 2 ;;
        --tol-rel) TOL_REL="$2"; shift 2 ;;
        --reuse-dump) REUSE=true; shift ;;
        *) echo "未知参数 / unknown option: $1" >&2; exit 2 ;;
    esac
done

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
MON_TOPIC="${SYN_TOPIC_MONITORING:-synergia-monitoring}"
WORK="${REMOTE_HOME}/m2baseline"
REMOTE_JSONL="$WORK/monitoring.jsonl"
LOCAL_JSONL="$PROJECT_ROOT/docs/m2_monitoring_${TAG}.jsonl"
OUT_DIR="$PROJECT_ROOT/docs/reports"

on_master() { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "$@"; }

echo "===================================================================="
echo "syn-m2-baseline.sh — M2 逐设备基线 / per-device M2 baseline (tag=$TAG)"
echo "===================================================================="

if [ "$REUSE" = false ]; then
    echo "[1/2] 转储 $MON_TOPIC → $LOCAL_JSONL"
    # chmod 777 供容器 uid 9999 可写；--timeout-ms 避免等不满 --max-messages 时挂住。
    # chmod 777 so the uid-9999 container can write; --timeout-ms prevents a hang.
    on_master "mkdir -p $WORK && chmod 777 $WORK && docker exec kafka-1 kafka-console-consumer.sh \
        --bootstrap-server $BROKERS --topic $MON_TOPIC --from-beginning \
        --max-messages $MAX_MESSAGES --timeout-ms $TIMEOUT_MS > $REMOTE_JSONL 2>/dev/null; true"
    if ! ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "cat $REMOTE_JSONL" > "$LOCAL_JSONL"; then
        echo "ERROR: 拉回监测转储失败。可手动：ssh $SSH_USER@$MASTER_SSH \"cat $REMOTE_JSONL\" > $LOCAL_JSONL" >&2
        exit 2
    fi
    echo "  -> $LOCAL_JSONL ($(wc -l < "$LOCAL_JSONL") 行)"
else
    echo "[1/2] --reuse-dump：复用 $LOCAL_JSONL ($(wc -l < "$LOCAL_JSONL" 2>/dev/null || echo 0) 行)"
fi

if [ ! -s "$LOCAL_JSONL" ]; then
    echo "ERROR: 监测转储为空。确认 M1/M2 作业已产出监测快照，且本脚本在 syn-clean-topics.sh 之前运行。" >&2
    exit 2
fi

echo ""
echo "[2/2] 逐设备聚合 + Java 8 等值核验"
python3 "$SCRIPT_DIR/m2_device_baseline.py" \
    --monitoring-jsonl "$LOCAL_JSONL" \
    --java8-probe "$PROJECT_ROOT/docs/m2_probe_7d_clean.csv" \
    --r-per-device "${SYN_M2_R_PER_DEVICE:-A=1.0,B=1.0,C=1.0,D=0.75,E=1.0,F=1.0,G=1.5,H=1.0}" \
    --tol-rel "$TOL_REL" \
    --out-csv "$OUT_DIR/m2_java11_${TAG}_per_device.csv" \
    --out-md  "$OUT_DIR/m2_java11_${TAG}_per_device.md" \
    --out-svg "$OUT_DIR/m2_java11_${TAG}_rate_cmp.svg"
RC=$?

echo ""
echo "===================================================================="
if [ "$RC" -eq 0 ]; then
    echo "[PASS] 逐设备等值核验通过。产物见 $OUT_DIR/"
else
    echo "退出码 $RC —— 见上方说明。产物（若已生成）见 $OUT_DIR/"
fi
echo "提醒：本脚本读的是 synergia-monitoring，请在 syn-clean-topics.sh 清理之前完成。"
echo "===================================================================="
exit $RC
