#!/usr/bin/env bash
# ============================================================================
# syn-m3-a4-disrupt.sh
# 补遗四步骤 C 的两项扰动测试：在某台设备训练进行中（日志出现「training started」之后若干秒）
#   cancel  取消作业，计时到作业状态变为 CANCELED（要求三十秒之内），并核对训练线程被中断；
#   kill    强杀承载这台设备的 TaskManager 容器并立即重新启动它，记录时刻，等作业重新回到 RUNNING。
# Two step-C disruptions while a device is training: cancel the job and time it, or kill and restart the
# TaskManager hosting the training device.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录；ssh 别名 fa-master、fa-worker1、fa-worker2 可用；python3。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m3-a4-disrupt.sh cancel --jid <JobID> --since <提交前的 UTC 时刻> --out docs/m3_a4/stepc_cancel/cancel.txt
#      bash deploy/scripts/syn-m3-a4-disrupt.sh kill   --jid <JobID> --since <提交前的 UTC 时刻> --out docs/m3_a4/stepc_kill/kill.txt
#    可选 --delay <秒>：看到「training started」之后再等多少秒才动手，默认 15（短重放的训练约 30 至 100 秒）。
#    可选 --timeout <秒>：等「training started」的最长时间，默认 3600。
# 3. 前置条件 / Preconditions: 作业已提交、重放已启动；集群上没有其他项目（FA-iForest）的作业——kill 会让同一
#    TaskManager 上的所有作业重启，脚本开头会核对并在发现其他作业时退出。
# 4. 期望产出 / Expected output: 终端与 --out 中记下：触发的设备与 TaskManager、动手时刻；cancel 给出「取消
#    端到端 N 秒」与该 TaskManager 日志里的 interrupted 行；kill 给出容器重启时刻与作业重新 RUNNING 的时刻。
# 5. 失败兜底 / Failure fallback: 退出码 2 参数错误；3 发现其他作业；4 超时仍未看到训练开始；5 取消后 120 秒
#    仍不是 CANCELED，或 kill 后 300 秒作业仍未回到 RUNNING（请查看 jobmanager 日志并告诉我）。
# ============================================================================
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a
MODE="${1:-}"; shift || true
JID=""; SINCE=""; OUT=""; DELAY=15; TIMEOUT=3600
while [[ $# -gt 0 ]]; do
    case "$1" in
        --jid) JID="$2"; shift 2 ;;
        --since) SINCE="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --delay) DELAY="$2"; shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done
[[ "$MODE" == cancel || "$MODE" == kill ]] || { echo "ERROR: 第一个参数应为 cancel 或 kill" >&2; exit 2; }
[ -z "$JID" ] || [ -z "$SINCE" ] || [ -z "$OUT" ] && { echo "ERROR: 需要 --jid、--since、--out" >&2; exit 2; }
mkdir -p "$(dirname "$OUT")"
REST="http://$NODE_MASTER_IP:8081"
log() { echo "$*" | tee -a "$OUT"; }
state() {
    ssh fa-master "curl -s --max-time 10 '$REST/jobs/$JID'" 2>/dev/null \
        | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("state","UNREACHABLE"))
except ValueError: print("UNREACHABLE")'
}

OTHERS=$(ssh fa-master "docker exec jobmanager flink list -r 2>/dev/null" | grep -E '^[0-9]' | grep -v "$JID" || true)
if [ -n "$OTHERS" ]; then
    echo "ERROR: 集群上还有其他作业，停止（不得影响其他项目）：" >&2; echo "$OTHERS" >&2; exit 3
fi

: > "$OUT"
log "模式：$MODE；作业 $JID；等待日志里出现 training started（最长 ${TIMEOUT}s）"
T_END=$(( $(date +%s) + TIMEOUT ))
HIT=""
while [ -z "$HIT" ]; do
    [ "$(date +%s)" -ge "$T_END" ] && { log "ERROR: 超时仍未看到训练开始"; exit 4; }
    for pair in "fa-worker1 taskmanager-2" "fa-worker2 taskmanager-3"; do
        set -- $pair
        L=$(ssh "$1" "docker logs --since $SINCE $2 2>&1 | grep -m1 'training started on'" 2>/dev/null || true)
        if [ -n "$L" ]; then HIT="$1 $2 $L"; break; fi
    done
    [ -z "$HIT" ] && sleep 5
done
set -- $HIT
HOST="$1"; TM="$2"; DEV=$(echo "$HIT" | sed -n 's/.*Device \([^ ]*\) training started.*/\1/p')
log "看到训练开始：设备 $DEV，$HOST / $TM；${DELAY}s 后动手"
sleep "$DELAY"

if [ "$MODE" = cancel ]; then
    T0=$(python3 -c 'import time;print(time.time())')
    log "取消开始：$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    ssh fa-master "docker exec jobmanager flink cancel $JID" 2>&1 | tee -a "$OUT"
    for _ in $(seq 1 120); do
        S=$(state); [ "$S" = CANCELED ] && break; sleep 1
    done
    T1=$(python3 -c 'import time;print(time.time())')
    [ "$S" = CANCELED ] || { log "ERROR: 120 秒后作业状态仍为 $S"; exit 5; }
    log "取消端到端 $(python3 -c "print('%.1f' % ($T1 - $T0))") 秒（要求不超过 30 秒）"
    sleep 5
    log "$TM 日志中的中断与关闭记录："
    ssh "$HOST" "docker logs --since $SINCE $TM 2>&1 | grep -E 'training interrupted|Training executor shut down'" 2>&1 | tee -a "$OUT"
else
    log "强杀 $TM：$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    ssh "$HOST" "docker kill $TM && docker start $TM" 2>&1 | tee -a "$OUT"
    log "容器已重新启动：$(date -u +%Y-%m-%dT%H:%M:%SZ)；等待作业重新 RUNNING"
    sleep 20
    for _ in $(seq 1 56); do
        S=$(state)
        R=$(ssh fa-master "curl -s --max-time 10 '$REST/jobs/$JID/metrics?get=numRestarts'" 2>/dev/null)
        [ "$S" = RUNNING ] && echo "$R" | grep -q '"value":"[1-9]' && break
        sleep 5
    done
    [ "$S" = RUNNING ] || { log "ERROR: 作业状态为 $S"; exit 5; }
    log "作业已重新 RUNNING：$(date -u +%Y-%m-%dT%H:%M:%SZ)；重启计数 $R"
fi
