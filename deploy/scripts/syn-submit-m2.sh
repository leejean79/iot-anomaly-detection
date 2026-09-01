#!/usr/bin/env bash
# ============================================================================
# syn-submit-m2.sh
# 提交 M2 联合作业（com.leejean.m2.M2Job = M1 段复用 + pMCOD）到旧集群，沿用 syn-submit-m1.sh 模式。
# Submit the joint M2 job (M1 stage reused + pMCOD) to the reused cluster, mirroring syn-submit-m1.sh.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash），ssh 免密到 fa-master；jar 已上传（syn-upload-m1.sh）。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-submit-m2.sh                              # 用 .env 里的 W/S/R/k 占位默认
#      bash deploy/scripts/syn-submit-m2.sh --extra '--mcod-r 1.5 --mcod-k 20'
# 3. 前置条件 / Preconditions: synergia-source(8 分区)/-scores/-monitoring 已建；jar 在
#      <REMOTE_HOME>/jars/${SYN_JOB_JAR_NAME}；**M1Job 与 M2Job 不可同时运行**（都消费 synergia-source）。
# 4. 期望产出 / Expected output: 打印 JobID 并轮询至 RUNNING；作业读 synergia-source，写 synergia-scores
#      与 synergia-monitoring（M1 快照 + M2 三路信号）。
# 5. 失败兜底 / Failure fallback: 提交失败或 60s 内未 RUNNING → 报错退出；**绝不 cancel 任何旧 job**。
#
# 缩写自查 / Abbreviations: W = window 窗长；S = slide 滑动步；R = 半径；k = 邻居阈值；p = parallelism。
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

EXTRA_ARGS=""
START_OFFSET="earliest"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --extra) EXTRA_ARGS="$2"; shift 2 ;;
        --start-offset) START_OFFSET="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
JAR_NAME="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"
MAIN="${SYN_M2_JOB_MAIN:-com.leejean.m2.M2Job}"
PARALLELISM="${SYN_SOURCE_PARTITIONS:-8}"
SRC_TOPIC="${SYN_TOPIC_SOURCE:-synergia-source}"
WINDOW_SEC="${SYN_M2_WINDOW_SEC:-3600}"
SLIDE_SEC="${SYN_M2_SLIDE_SEC:-60}"
MCOD_R="${SYN_M2_R:-1.0}"
MCOD_K="${SYN_M2_K:-10}"

# 分区数预检：synergia-source 必须已是 8 分区（否则 Flink 消费者触发 broker 自动建 1 分区，重放静默丢失）。
SRC_DESC=$(ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" \
    "docker exec kafka-1 kafka-topics.sh --bootstrap-server $BROKERS --describe --topic $SRC_TOPIC" 2>/dev/null || true)
ACTUAL_PARTS=$(echo "$SRC_DESC" | grep -oE 'PartitionCount: *[0-9]+' | grep -oE '[0-9]+' | head -1)
if [ -z "$ACTUAL_PARTS" ]; then
    echo "ERROR: topic '$SRC_TOPIC' 不存在。先建 topic：bash deploy/scripts/syn-create-topics.sh" >&2
    exit 2
fi
if [ "$ACTUAL_PARTS" != "$PARALLELISM" ]; then
    echo "ERROR: topic '$SRC_TOPIC' 有 $ACTUAL_PARTS 个分区，期望 ${PARALLELISM}；先重建：syn-clean-topics.sh --yes" >&2
    exit 2
fi
echo "[preflight] topic '$SRC_TOPIC' 分区数 = $ACTUAL_PARTS OK"

echo "===================================="
echo "[submit] M2Job  main=$MAIN  p=$PARALLELISM  W=${WINDOW_SEC}s S=${SLIDE_SEC}s R=$MCOD_R k=$MCOD_K  start=$START_OFFSET"
echo "  提醒 / reminder：确认没有 M1Job 在跑（二者都消费 synergia-source，不可并存）。"
echo "===================================="
submit_output=$(ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "
    docker exec jobmanager flink run -d \
        -c $MAIN \
        -p $PARALLELISM \
        /opt/flink/usrlib/$JAR_NAME \
        --brokers $BROKERS \
        --source-topic ${SYN_TOPIC_SOURCE:-synergia-source} \
        --scores-topic ${SYN_TOPIC_SCORES:-synergia-scores} \
        --monitoring-topic ${SYN_TOPIC_MONITORING:-synergia-monitoring} \
        --out-topic ${SYN_TOPIC_M1_OUT:-synergia-m1-out} \
        --window-sec $WINDOW_SEC \
        --slide-sec $SLIDE_SEC \
        --mcod-r $MCOD_R \
        --mcod-k $MCOD_K \
        --start-offset $START_OFFSET \
        --parallelism $PARALLELISM \
        $EXTRA_ARGS
" 2>&1)
echo "$submit_output"

jobid=$(echo "$submit_output" | grep -oE 'JobID [a-f0-9]{32}' | awk '{print $2}' | head -1)
if [[ -z "$jobid" ]]; then
    echo "ERROR: failed to extract JobID from submit output (see above)." >&2
    exit 1
fi

echo "[wait] M2Job ($jobid) to be RUNNING ..."
for _ in $(seq 1 12); do
    status=$(ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "docker exec jobmanager flink list 2>&1" \
        | grep -F "$jobid" | grep -oE '\(RUNNING\)|\(FAILED\)|\(FINISHED\)|\(SCHEDULED\)' | head -1 || true)
    if [[ "$status" == "(RUNNING)" ]]; then
        echo "  M2Job is RUNNING (JobID $jobid)"
        echo "下一步 / Next: bash $SCRIPT_DIR/syn-replay.sh --speedup 600 --start <day> --end <day>"
        exit 0
    fi
    sleep 5
done
echo "ERROR: M2Job not RUNNING after 60s. Check 'docker exec jobmanager flink list' and JM logs." >&2
exit 1
