#!/usr/bin/env bash
# ============================================================================
# syn-m3-grid.sh
# V-M3-3 离线超参数网格：把 synergia-m1-out 与 synergia-scores 转储下来，在 master 的临时容器里
# 运行 com.leejean.m3.M3Grid，对三台代表设备逐一训练隐藏层大小 × 窗口长度的每一种组合，
# 输出早停集误差、实际训练轮数（epoch）与耗时，最后把 CSV 拉回本地。
# Offline hyper-parameter grid for V-M3-3: dump m1-out and scores, run M3Grid in a transient
# container on master, pull the CSV back.
#
# 【为什么要两份转储】在线算子从 AnnotatedRound 上直接读到 M2 给每一轮打的离群标记，据此把含离群轮
# 的整窗从训练集剔除（训练净化）。而 m1-out 里装的是 DeviceRound，**不带这个标记**。因此还要转储
# synergia-scores——它列出所有被判为离群的 (设备, 轮时间戳)，据此可以还原标记，使离线口径与在线一致。
# 不转 scores 也能跑，但训练净化会被跳过，结果偏乐观，CSV 的 sanitized 列会记为 false。
# Why two dumps: the outlier flag the online operator sanitizes on lives in AnnotatedRound, not in
# the m1-out DeviceRound; synergia-scores restores it.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash），仓库根目录；ssh 主机别名 fa-master 可用；
#      master 上有 Java 11 的 flink 镜像与本项目的 jar。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m3-grid.sh
#      bash deploy/scripts/syn-m3-grid.sh --devices E,G,C --hidden-grid 40,60,90 --window-grid 30,60,120
#      bash deploy/scripts/syn-m3-grid.sh --no-scores        # 不转储 scores，跳过训练净化
#      bash deploy/scripts/syn-m3-grid.sh --reuse-dump       # 复用上次转储，只重跑网格
#    参数 / Arguments:
#      --devices <列表>        代表设备，默认 E,G,C（E 在带内、G 被掩码、C 结构中等）
#      --hidden-grid <列表>    隐藏层大小网格，默认 40,60,90
#      --window-grid <列表>    窗口长度网格，默认 30,60,120
#      --train-days <n>        训练段天数，默认 7
#      --early-stop-days <n>   早停段天数，默认 2
#      --max-epochs <n>        单次训练的上限轮数，默认 200
#      --patience <n>          早停耐心，默认 10
#      --max-messages <n>      每个 topic 的转储条数上限，默认 3000000
#      --out-name <文件名>     本地 CSV 文件名，默认 m3_grid.csv
#      --no-scores             不转储 scores（跳过训练净化）
#      --reuse-dump            复用 master 上已有的转储
# 3. 前置条件 / Preconditions: synergia-m1-out 与 synergia-scores 已含目标月份的数据
#      （即 M2 联合作业已跑过该段重放），且 topic 尚未被清理。
# 4. 期望产出 / Expected output: stdout 逐组合打印进度与解读；本地 docs/<out-name>；
#      每一行含 device,hiddenSize,windowLength,trainWindows,trainExcluded,esWindows,
#      epochs,esLoss,trainSeconds,sanitized。
# 5. 常见失败兜底 / Failure fallback:
#      「转储里没有任何一台目标设备的可用轮」→ 多半是 --max-messages 不足以覆盖标定期
#      （预热轮会被整段跳过，7 天标定 = 483,840 轮），提高它后重跑；
#      「样本不足，跳过」→ 该设备在训练段或早停段没切出窗口，检查转储是否覆盖足够天数。
#
# 缩写自查 / Abbreviations: epoch = 训练时在整个训练集上完整跑一遍；
#   早停（early stopping）= 早停集误差不再下降时提前结束训练；CSV = 逗号分隔值。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

DEVICES="E,G,C"; HIDDEN_GRID="40,60,90"; WINDOW_GRID="30,60,120"
TRAIN_DAYS=7; ES_DAYS=2; MAX_EPOCHS=200; PATIENCE=10
MAX_MESSAGES=3000000; OUT_NAME="m3_grid.csv"; USE_SCORES=1; REUSE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --devices) DEVICES="$2"; shift 2 ;;
        --hidden-grid) HIDDEN_GRID="$2"; shift 2 ;;
        --window-grid) WINDOW_GRID="$2"; shift 2 ;;
        --train-days) TRAIN_DAYS="$2"; shift 2 ;;
        --early-stop-days) ES_DAYS="$2"; shift 2 ;;
        --max-epochs) MAX_EPOCHS="$2"; shift 2 ;;
        --patience) PATIENCE="$2"; shift 2 ;;
        --max-messages) MAX_MESSAGES="$2"; shift 2 ;;
        --out-name) OUT_NAME="$2"; shift 2 ;;
        --no-scores) USE_SCORES=0; shift ;;
        --reuse-dump) REUSE=1; shift ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
JAR_NAME="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"
RHOME="${REMOTE_HOME:-/opt/fa-iforest}"
WORK="${RHOME}/m3grid"
RUN_IMAGE="${FLINK_IMAGE_TAG:-fa-iforest/flink:1.13.6-java11}"

echo "===================================================================="
echo "syn-m3-grid.sh — V-M3-3 离线超参数网格"
echo "  设备 ${DEVICES}   隐藏层 ${HIDDEN_GRID}   窗口长度 ${WINDOW_GRID}"
echo "  训练 ${TRAIN_DAYS} 天 / 早停 ${ES_DAYS} 天   maxEpochs=${MAX_EPOCHS} patience=${PATIENCE}"
echo "  训练净化：$([ "$USE_SCORES" -eq 1 ] && echo '开启（转储 scores 还原离群标记）' || echo '关闭')"
echo "===================================================================="

if [ "$REUSE" -eq 0 ]; then
    echo "[grid] 转储 synergia-m1-out（最多 ${MAX_MESSAGES} 条）…"
    ssh fa-master "mkdir -p ${WORK} && chmod 777 ${WORK} && docker exec kafka-1 kafka-console-consumer.sh \
        --bootstrap-server ${BROKERS} --topic ${SYN_TOPIC_M1_OUT:-synergia-m1-out} \
        --from-beginning --max-messages ${MAX_MESSAGES} --timeout-ms 60000 > ${WORK}/m1out.jsonl 2>/dev/null || true"
    if [ "$USE_SCORES" -eq 1 ]; then
        echo "[grid] 转储 synergia-scores（用于还原离群标记）…"
        ssh fa-master "docker exec kafka-1 kafka-console-consumer.sh \
            --bootstrap-server ${BROKERS} --topic ${SYN_TOPIC_SCORES:-synergia-scores} \
            --from-beginning --max-messages ${MAX_MESSAGES} --timeout-ms 60000 > ${WORK}/scores.jsonl 2>/dev/null || true"
    fi
fi

LINES="$(ssh fa-master "wc -l < ${WORK}/m1out.jsonl 2>/dev/null || echo 0" | tr -d '[:space:]')"
if [ "${LINES:-0}" -eq 0 ]; then
    echo "ERROR: ${WORK}/m1out.jsonl 为空——synergia-m1-out 无数据。" >&2
    echo "       请先跑一次重放让 M2 联合作业把目标月份写进 m1-out，再执行本脚本。" >&2
    exit 2
fi
echo "[grid] m1-out 转储 ${LINES} 行。"

SCORES_ARG=""
if [ "$USE_SCORES" -eq 1 ]; then
    SLINES="$(ssh fa-master "wc -l < ${WORK}/scores.jsonl 2>/dev/null || echo 0" | tr -d '[:space:]')"
    echo "[grid] scores 转储 ${SLINES:-0} 行。"
    if [ "${SLINES:-0}" -gt 0 ]; then
        SCORES_ARG="--scores-jsonl /work/scores.jsonl"
    else
        echo "[grid] scores 转储为空——将**跳过训练净化**，结果偏乐观，CSV 的 sanitized 列记为 false。"
    fi
fi

ssh fa-master "docker run --rm --user root \
    -v ${RHOME}/jars:/jars:ro -v ${WORK}:/work \
    ${RUN_IMAGE} \
    java -cp /jars/${JAR_NAME} com.leejean.m3.M3Grid \
        --rounds-jsonl /work/m1out.jsonl ${SCORES_ARG} \
        --devices ${DEVICES} --hidden-grid ${HIDDEN_GRID} --window-grid ${WINDOW_GRID} \
        --train-days ${TRAIN_DAYS} --early-stop-days ${ES_DAYS} \
        --max-epochs ${MAX_EPOCHS} --patience ${PATIENCE} \
        --out /work/m3_grid.csv"
RC=$?
if [ "$RC" -ne 0 ]; then
    echo "" >&2
    echo "ERROR: M3Grid 退出码 ${RC}——网格未完成，**不拉回 CSV**（避免留下不完整的产物）。" >&2
    exit "$RC"
fi

LOCAL_CSV="${PROJECT_ROOT}/docs/${OUT_NAME}"
if ssh fa-master "cat ${WORK}/m3_grid.csv" > "$LOCAL_CSV" 2>/dev/null && [ -s "$LOCAL_CSV" ]; then
    echo "[grid] 已拉回本地：${LOCAL_CSV}"
else
    rm -f "$LOCAL_CSV" 2>/dev/null || true
    echo "[grid] 拉回失败，可手动：ssh fa-master \"cat ${WORK}/m3_grid.csv\" > docs/${OUT_NAME}" >&2
    exit 1
fi
echo "提醒：本阶段**不定终值**——网格表交设计会话裁决 (hidden, window)。"
