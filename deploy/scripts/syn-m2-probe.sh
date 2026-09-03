#!/usr/bin/env bash
# ============================================================================
# syn-m2-probe.sh
# (R,k) 校准探针（交接文档 §7）：把 synergia-m1-out 的标准化轮转储成 JSONL，在 master 的临时 flink
# 容器里离线跑 M2Probe（复用 McodCore，忠实一致），对参数网格逐组输出逐设备离群率表（CSV + 解读）。
# The (R,k) calibration probe: dump normalized rounds from synergia-m1-out, run M2Probe offline in a
# transient flink container on master, producing the per-device per-parameter outlier-rate table.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash），ssh 免密到 fa-master；master 有 flink 镜像与 jar。
# 2. 调用命令 / Invocation:
#      # 前提：先用 M1Job 把想探的月份（建议 2022-03，EDA 相对平稳）重放进 synergia-m1-out。
#      bash deploy/scripts/syn-m2-probe.sh --max-messages 300000
#      bash deploy/scripts/syn-m2-probe.sh --r-grid 0.5,1.0,1.5,2.0 --k-grid 5,10,20 --window-sec 3600 --slide-sec 60
#      # 补跑不同网格、换个文件名免覆盖已存表（如逐设备 R 标定）：
#      bash deploy/scripts/syn-m2-probe.sh --r-grid 0.75,1.0,1.25,1.5,1.75 --k-grid 10 --out-name m2_probe_calib.csv
# 3. 前置条件 / Preconditions: synergia-m1-out 已含目标月份的标准化 DeviceRound（M1Job 已跑过该段）。
# 4. 期望产出 / Expected output: master 上 ${REMOTE_HOME}/m2_probe.csv（device,R,k,slides,meanWindowPoints,
#      meanOutlierRate,fracZeroSlides）+ stdout 通俗解读；末尾把 CSV 拉回本地 deploy/../docs/ 便于附验收。
# 5. 失败兜底 / Failure fallback: m1-out 为空则提示先跑 M1；容器缺失/挂载错则报错退出。**不定 (R,k) 终值**。
#
# 缩写自查 / Abbreviations: JSONL = 每行一条 JSON；CSV = 逗号分隔值；R = 半径；k = 邻居阈值。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

MAX_MESSAGES=300000
R_GRID="${SYN_M2_PROBE_R_GRID:-0.5,1.0,1.5,2.0,2.5,3.0}"
K_GRID="${SYN_M2_PROBE_K_GRID:-5,10,20}"
WINDOW_SEC="${SYN_M2_WINDOW_SEC:-3600}"
SLIDE_SEC="${SYN_M2_SLIDE_SEC:-60}"
# 本地 CSV 文件名（默认 m2_probe.csv）；补跑不同网格时可换名，免覆盖已存表。
# Local CSV basename (default m2_probe.csv); override it on a re-run so a different grid does not clobber it.
OUT_NAME="m2_probe.csv"
# 可选：逐设备逐通道离散度诊断 CSV 的本地文件名（空 = 不产出）。/ optional per-channel dispersion CSV (empty = off)
DISPERSION_NAME=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --max-messages) MAX_MESSAGES="$2"; shift 2 ;;
        --r-grid) R_GRID="$2"; shift 2 ;;
        --k-grid) K_GRID="$2"; shift 2 ;;
        --window-sec) WINDOW_SEC="$2"; shift 2 ;;
        --slide-sec) SLIDE_SEC="$2"; shift 2 ;;
        --out-name) OUT_NAME="$2"; shift 2 ;;
        --dispersion-name) DISPERSION_NAME="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
JAR_NAME="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"
PROBE_MAIN="${SYN_M2_PROBE_MAIN:-com.leejean.m2.M2Probe}"
WORK="${REMOTE_HOME}/m2probe"
JSONL="$WORK/m1out.jsonl"
CSV="$WORK/m2_probe.csv"

on_master() { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "$@"; }

echo "===================================="
echo "[probe] 转储 synergia-m1-out（最多 ${MAX_MESSAGES} 条）→ $JSONL"
echo "===================================="
# 用 --timeout-ms 兜底：console-consumer 若等不满 --max-messages 会一直挂住，加超时让它消费完即退。
# chmod 777：flink 镜像 entrypoint 会把命令降权成 uid 9999，写不进 root 拥有的挂载目录，故把工作目录设为全可写。
on_master "mkdir -p $WORK && chmod 777 $WORK && docker exec kafka-1 kafka-console-consumer.sh \
    --bootstrap-server $BROKERS --topic ${SYN_TOPIC_M1_OUT:-synergia-m1-out} \
    --from-beginning --max-messages $MAX_MESSAGES --timeout-ms 30000 > $JSONL 2>/dev/null || true"

LINES=$(on_master "wc -l < $JSONL 2>/dev/null || echo 0")
LINES=$(echo "$LINES" | tr -d '[:space:]')
if [ "${LINES:-0}" -eq 0 ]; then
    echo "ERROR: $JSONL 为空——synergia-m1-out 无数据。先用 M1Job 把目标月份重放进 m1-out：" >&2
    echo "       syn-submit-m1.sh 然后 syn-replay.sh --start 2022-03-01 --end 2022-04-01" >&2
    exit 2
fi
echo "[probe] 转储 ${LINES} 行；运行离线网格 R=${R_GRID} × k=${K_GRID}（W=${WINDOW_SEC}s S=${SLIDE_SEC}s）"

# 离散度诊断：可选，若传 --dispersion-name 则让 M2Probe 额外产出逐通道 P1/P99/峰度 CSV（复用同一份转储）。
DISP_ARG=""
if [ -n "$DISPERSION_NAME" ]; then
    DISP_ARG="--dispersion-out /work/m2_dispersion.csv"
fi

# 在临时 flink 容器里跑纯 Java 探针（host 无 JDK 时靠镜像自带）
on_master "docker run --rm --user root \
    -v ${REMOTE_HOME}/jars:/jars:ro -v $WORK:/work \
    fa-iforest/flink:${FLINK_VERSION} \
    java -cp /jars/$JAR_NAME $PROBE_MAIN \
        --rounds-jsonl /work/m1out.jsonl \
        --out /work/m2_probe.csv \
        --window-sec $WINDOW_SEC --slide-sec $SLIDE_SEC \
        --r-grid $R_GRID --k-grid $K_GRID $DISP_ARG"

echo "===================================="
echo "[probe] CSV（master）：$CSV"
LOCAL_CSV="$PROJECT_ROOT/docs/$OUT_NAME"
# 用 ssh cat 拉回（比 scp 稳，复用一直可用的 ssh 通道）/ pull back via ssh cat (more robust than scp)
if on_master "cat $CSV" > "$LOCAL_CSV" 2>/dev/null && [ -s "$LOCAL_CSV" ]; then
    echo "[probe] 已拉回本地：${LOCAL_CSV}（可附入 docs/m2_acceptance.md 的 V-M2-3）"
else
    rm -f "$LOCAL_CSV" 2>/dev/null || true
    echo "[probe] 拉回失败，可手动：ssh $SSH_USER@$MASTER_SSH \"cat $CSV\" > docs/m2_probe.csv" >&2
fi

# 离散度 CSV 拉回（若启用）/ pull back the dispersion CSV if enabled
if [ -n "$DISPERSION_NAME" ]; then
    LOCAL_DISP="$PROJECT_ROOT/docs/$DISPERSION_NAME"
    if on_master "cat $WORK/m2_dispersion.csv" > "$LOCAL_DISP" 2>/dev/null && [ -s "$LOCAL_DISP" ]; then
        echo "[probe] 离散度诊断已拉回：${LOCAL_DISP}（逐设备逐通道 P1/P99/spread/峰度）"
    else
        rm -f "$LOCAL_DISP" 2>/dev/null || true
        echo "[probe] 离散度 CSV 拉回失败，可手动：ssh $SSH_USER@$MASTER_SSH \"cat $WORK/m2_dispersion.csv\" > docs/$DISPERSION_NAME" >&2
    fi
fi
echo "提醒 / note：本阶段**不定 (R,k) 终值**——表格交回设计会话裁决。"
