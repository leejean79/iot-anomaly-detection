#!/usr/bin/env bash
# ============================================================================
# syn-submit-m1.sh
# 提交 M1 Flink 作业（com.leejean.m1.M1Job）到旧集群，沿用 4-submit-job.sh 的 docker-exec 模式。
# Submit the M1 Flink job to the reused cluster, following 4-submit-job.sh's docker-exec pattern.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash），ssh 免密到 fa-master；jar 已上传（syn-upload-m1.sh）。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-submit-m1.sh                       # earliest 起始（runbook：先提交再重放）
#      bash deploy/scripts/syn-submit-m1.sh --extra '--warmup-rounds 8640 --cache-depth 1000'
# 3. 前置条件 / Preconditions: synergia-source/-m1-out/-monitoring 已建；jar 在
#      <REMOTE_HOME>/jars/${SYN_JOB_JAR_NAME}（= jobmanager 容器 /opt/flink/usrlib）。
# 4. 期望产出 / Expected output: 打印 JobID 并轮询至 RUNNING；作业读 synergia-source、写
#      synergia-m1-out 与 synergia-monitoring。
# 5. 失败兜底 / Failure fallback: 提交失败或 60s 内未 RUNNING → 报错退出并提示查 flink list/日志；
#      **绝不 cancel 任何旧 job**。
#
# 缩写自查 / Abbreviations: JM = JobManager；TM = TaskManager；p = parallelism 并行度。
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
MAIN="${SYN_JOB_MAIN:-com.leejean.m1.M1Job}"
PARALLELISM="${SYN_SOURCE_PARTITIONS:-8}"
SRC_TOPIC="${SYN_TOPIC_SOURCE:-synergia-source}"

# 分区数预检：确保 synergia-source 已是 8 分区再提交。若 topic 不存在，直接提交会让 Flink 消费者
# 触发 broker 自动建 topic（默认 1 分区），导致后续重放器按显式分区器发往分区 1-7 全部失败。
# Partition-count preflight: ensure synergia-source has the expected partitions before submitting, so
# the Flink consumer cannot auto-create a 1-partition topic (which then breaks the explicit-partition replay).
SRC_DESC=$(ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" \
    "docker exec kafka-1 kafka-topics.sh --bootstrap-server $BROKERS --describe --topic $SRC_TOPIC" 2>/dev/null || true)
ACTUAL_PARTS=$(echo "$SRC_DESC" | grep -oE 'PartitionCount: *[0-9]+' | grep -oE '[0-9]+' | head -1)
if [ -z "$ACTUAL_PARTS" ]; then
    echo "ERROR: topic '$SRC_TOPIC' 不存在。先建 topic：bash deploy/scripts/syn-create-topics.sh" >&2
    echo "ERROR: topic '$SRC_TOPIC' does not exist; run syn-create-topics.sh first." >&2
    exit 2
fi
if [ "$ACTUAL_PARTS" != "$PARALLELISM" ]; then
    echo "ERROR: topic '$SRC_TOPIC' 有 $ACTUAL_PARTS 个分区，期望 ${PARALLELISM}。" >&2
    echo "       很可能被 broker 自动建成了 1 分区。先重建：bash deploy/scripts/syn-clean-topics.sh --yes" >&2
    echo "ERROR: '$SRC_TOPIC' has $ACTUAL_PARTS partitions, expected $PARALLELISM; recreate it first." >&2
    exit 2
fi
echo "[preflight] topic '$SRC_TOPIC' 分区数 = $ACTUAL_PARTS OK"

echo "===================================="
echo "[submit] M1Job  main=$MAIN  p=$PARALLELISM  start=$START_OFFSET"
echo "===================================="
submit_output=$(ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "
    docker exec jobmanager flink run -d \
        -c $MAIN \
        -p $PARALLELISM \
        /opt/flink/usrlib/$JAR_NAME \
        --brokers $BROKERS \
        --source-topic ${SYN_TOPIC_SOURCE:-synergia-source} \
        --out-topic synergia-m1-out \
        --monitoring-topic synergia-monitoring \
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

echo "[wait] M1Job ($jobid) to be RUNNING ..."
for _ in $(seq 1 12); do
    status=$(ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "docker exec jobmanager flink list 2>&1" \
        | grep -F "$jobid" | grep -oE '\(RUNNING\)|\(FAILED\)|\(FINISHED\)|\(SCHEDULED\)' | head -1 || true)
    if [[ "$status" == "(RUNNING)" ]]; then
        echo "  M1Job is RUNNING (JobID $jobid)"
        echo "下一步 / Next: bash $SCRIPT_DIR/syn-replay.sh --start <day> --end <day> --speedup 600"
        exit 0
    fi
    sleep 5
done
echo "ERROR: M1Job not RUNNING after 60s. Check 'docker exec jobmanager flink list' and JM logs." >&2
exit 1
