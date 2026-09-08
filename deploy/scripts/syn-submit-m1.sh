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
#      bash deploy/scripts/syn-submit-m1.sh --extra '--calib-days 7 --cache-depth 1000'
#      （标定窗口默认 7 天=补充指令四；相对退化防护默认关闭；两者也可经 .env 的
#        SYN_M1_CALIB_DAYS / SYN_M1_RELATIVE_GUARD 设置。功能验证可用 --extra '--warmup-rounds 600'。）
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
FORCE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --extra) EXTRA_ARGS="$2"; shift 2 ;;
        --start-offset) START_OFFSET="$2"; shift 2 ;;
        --force) FORCE=1; shift ;;   # 跳过隔离预检（m1-out 非空 / 已有作业）/ skip isolation preflight
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

# M1 标定参数（补充指令四）：标定窗口天数与相对退化防护开关。由 .env 提供默认（作业本身也默认 7 天/关闭）。
# 置于 --extra **之前**：Flink ParameterTool 对重复键以**后出现者为准**，故若 --extra 显式给了同名参数，
# 它出现在后、会覆盖这里的默认，命令行显式覆盖 .env 默认，符合预期。
# M1 calibration args (instruction 4): placed BEFORE --extra so an explicit --extra override wins
# (ParameterTool keeps the LAST occurrence of a duplicate key).
CALIB_ARGS=""
if [[ -n "${SYN_M1_CALIB_DAYS:-}" ]]; then
    CALIB_ARGS="$CALIB_ARGS --calib-days ${SYN_M1_CALIB_DAYS}"
fi
if [[ -n "${SYN_M1_RELATIVE_GUARD:-}" ]]; then
    CALIB_ARGS="$CALIB_ARGS --relative-guard ${SYN_M1_RELATIVE_GUARD}"
fi

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

# 隔离预检（补充指令五 后续加固）：防"m1-out 累积重复"这一类根因。两条硬性拦截，--force 可跳过。
# Isolation preflight: prevent the duplicate-rounds accumulation seen under instruction 5. Two hard gates.
if [ "$FORCE" -ne 1 ]; then
    MON_TOPIC="${SYN_TOPIC_MONITORING:-synergia-monitoring}"
    OUT_TOPIC="${SYN_TOPIC_M1_OUT:-synergia-m1-out}"
    # (1) 已有 M1/M2 作业在跑？两个生产者同写 m1-out 会造成重复。/ another producer already writing m1-out?
    RUNNING=$(ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "docker exec jobmanager flink list 2>/dev/null" \
        | grep -E '\(RUNNING\)' | grep -Ei 'M1Job|M2Job' || true)
    if [ -n "$RUNNING" ]; then
        echo "ERROR: 已有 M1/M2 作业在运行，会与本次一同写 ${OUT_TOPIC} 造成重复轮：" >&2
        echo "$RUNNING" | sed 's/^/       /' >&2
        echo "       先取消我们自己的那个作业（flink cancel <JobID>），再提交；确需并行用 --force。" >&2
        echo "ERROR: an M1/M2 job is already running and would double-write ${OUT_TOPIC}; cancel it first." >&2
        exit 3
    fi
    # (2) m1-out 非空？上一轮遗留 + 本轮新写 = 重复累积（正是核验断言二报的重复）。/ m1-out not empty → accumulation
    OFF=$(ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" \
        "docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $BROKERS --topic $OUT_TOPIC --time -1" 2>/dev/null || true)
    OUT_SUM=$(echo "$OFF" | awk -F: '{s+=$3} END{print s+0}')
    if [ "${OUT_SUM:-0}" -gt 0 ]; then
        echo "ERROR: ${OUT_TOPIC} 非空（约 ${OUT_SUM} 条），本次写入会叠加成重复轮（核验断言二会 FAIL）。" >&2
        echo "       先彻底重置：bash deploy/scripts/syn-clean-topics.sh --yes（现已连 m1-out/monitoring 一起清）；" >&2
        echo "       确认 ${OUT_TOPIC} 归零后再提交；确需追加用 --force。" >&2
        echo "ERROR: ${OUT_TOPIC} is not empty (~${OUT_SUM} msgs); writing now would accumulate duplicates." >&2
        exit 3
    fi
    echo "[preflight] 无并发 M1/M2 作业；${OUT_TOPIC} 为空 OK"
fi

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
        $CALIB_ARGS \
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
