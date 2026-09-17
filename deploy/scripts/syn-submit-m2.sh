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
#      bash deploy/scripts/syn-submit-m2.sh --extra '--m3-enabled false'   # M2 基线运行（关闭 M3 转发）
#      bash deploy/scripts/syn-submit-m2.sh --force                        # 跳过隔离预检（确需并行）
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
FORCE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --extra) EXTRA_ARGS="$2"; shift 2 ;;
        --start-offset) START_OFFSET="$2"; shift 2 ;;
        --force) FORCE=1; shift ;;   # 跳过隔离预检（确需并行时）/ skip the isolation preflight
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
# 逐设备半径 R（收尾任务；空则全用全局 MCOD_R）/ per-device R (empty = global for all)
MCOD_R_PER_DEVICE="${SYN_M2_R_PER_DEVICE:-}"

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
echo "===================================="

# 隔离预检：与 syn-submit-m1.sh 对称的硬性拦截。此前这里只有一句提醒、不拦截，导致「先提交 M1Job
# 再提交 M2Job」两个作业都能起来——而 M2Job 本身就含完整 M1 管线，二者并行会双写 synergia-m1-out
# 与 synergia-monitoring 造成重复轮，且要等整轮重放跑完、核验断言（b）FAIL 才暴露，代价是一整轮作废。
# Isolation preflight, symmetric with syn-submit-m1.sh. This used to be a printed reminder only, so
# submitting M1Job and then M2Job started both — and M2Job already contains the full M1 chain, so the
# two double-write synergia-m1-out and synergia-monitoring. The duplicates only surface when integrity
# assertion (b) fails after the whole replay, costing the entire run.
if [ "$FORCE" -ne 1 ]; then
    RUNNING=$(ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "docker exec jobmanager flink list 2>/dev/null" \
        | grep -E '\(RUNNING\)' | grep -Ei 'M1Job|M2Job' || true)
    if [ -n "$RUNNING" ]; then
        echo "ERROR: 已有 M1/M2 作业在运行。M2Job 本身包含完整 M1 管线，二者并行会双写" >&2
        echo "       synergia-m1-out / synergia-monitoring 造成重复轮：" >&2
        echo "$RUNNING" | sed 's/^/       /' >&2
        echo "       先取消我们自己的那个作业（flink cancel <JobID>），再提交；确需并行用 --force。" >&2
        echo "       注意：旧项目 FA-iForest 的作业不在此列，绝不可取消。" >&2
        echo "ERROR: an M1/M2 job is already running; M2Job contains the M1 chain, so the two would" >&2
        echo "       double-write m1-out. Cancel ours first, or pass --force." >&2
        exit 3
    fi
    echo "[preflight] 无并行的 M1/M2 作业 OK / no competing M1/M2 job"
fi
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
        --mcod-r-per-device \"$MCOD_R_PER_DEVICE\" \
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
