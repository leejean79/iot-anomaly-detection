#!/usr/bin/env bash
# ============================================================================
# syn-m3-diag.sh
# 作业异常结束后的只读诊断：一次收齐 JobManager 与两台 TaskManager 的日志、容器状态（是否重启、是否因内存
# 被杀）、集群配置、作业列表与三台机器的内存情况，并从日志里摘出检查点与异常相关的行。不改动任何东西。
# Read-only diagnostics after a job ends abnormally: logs, container state (restarts, OOM kills), cluster
# configuration, the job list and memory on all three machines, plus a summary of checkpoint and
# exception lines. Changes nothing.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录；ssh 别名 fa-master、fa-worker1、fa-worker2 可用。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m3-diag.sh --since 2026-10-01T02:40:39Z --out-dir docs/m3_march/diag
#    --since 为提交作业前记下的 UTC 时刻（syn-m3-watch.sh status 第一行也会打印）。
# 3. 前置条件 / Preconditions: 在 syn-reset-env.sh 之前执行（复位会重启容器；docker logs 虽然保留，
#    但作业列表与 REST 状态会被清掉）。
# 4. 期望产出 / Expected output: --out-dir 下 containers.txt、jm.log、tm2.log、tm3.log、jm_config.json、
#    jobs.json、memory.txt，以及摘要 diag_summary.txt（终端同时打印摘要）。
# 5. 失败兜底 / Failure fallback: 某台机器连不上时对应文件为空并在摘要中注明，其余照常收集；
#    dmesg 需要权限时会读不到，摘要中注明即可，不影响其他项。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

SINCE=""; OUT=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --since) SINCE="$2"; shift 2 ;;
        --out-dir) OUT="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done
[ -z "$SINCE" ] || [ -z "$OUT" ] && { echo "ERROR: 需要 --since 与 --out-dir" >&2; exit 2; }
mkdir -p "$OUT"
REST="http://$NODE_MASTER_IP:8081"
FMT='{{.Name}} 状态={{.State.Status}} 启动于={{.State.StartedAt}} 重启次数={{.RestartCount}} 内存被杀={{.State.OOMKilled}} 退出码={{.State.ExitCode}}'

echo "[diag] 容器状态 …"
{
    ssh fa-master  "docker inspect -f '$FMT' jobmanager kafka-1 zookeeper" 2>&1
    ssh fa-worker1 "docker inspect -f '$FMT' taskmanager-2 kafka-2" 2>&1
    ssh fa-worker2 "docker inspect -f '$FMT' taskmanager-3 kafka-3" 2>&1
} > "$OUT/containers.txt"

echo "[diag] 日志（自 $SINCE 起）…"
ssh fa-master  "docker logs --since $SINCE jobmanager 2>&1"    > "$OUT/jm.log"
ssh fa-worker1 "docker logs --since $SINCE taskmanager-2 2>&1" > "$OUT/tm2.log"
ssh fa-worker2 "docker logs --since $SINCE taskmanager-3 2>&1" > "$OUT/tm3.log"

echo "[diag] 集群配置与作业列表 …"
ssh fa-master "curl -s --max-time 20 '$REST/jobmanager/config'" > "$OUT/jm_config.json"
ssh fa-master "curl -s --max-time 20 '$REST/jobs/overview'"     > "$OUT/jobs.json"

echo "[diag] 内存 …"
{
    for h in fa-master fa-worker1 fa-worker2; do
        echo "===== $h ====="
        ssh "$h" "free -m; echo; docker stats --no-stream --format '{{.Name}} CPU={{.CPUPerc}} 内存={{.MemUsage}}'; echo; \
            (dmesg -T 2>/dev/null || echo '（dmesg 需要权限，读不到）') | grep -iE 'killed process|out of memory|oom' | tail -n 20" 2>&1
    done
} > "$OUT/memory.txt"

# 摘要：检查点、作业状态变化、异常与内存相关的行。/ Summary of checkpoint, state-change and error lines.
{
    echo "诊断时间：$(date '+%F %T %Z')，日志起点：$SINCE"
    echo; echo "== 容器状态 =="; cat "$OUT/containers.txt"
    echo; echo "== JobManager 进程启动记录（出现多次说明 JobManager 重启过）=="
    grep -nE 'Starting StandaloneSessionClusterEntrypoint|Starting the cluster entrypoint|Shutting (down|StandaloneSession)|Terminating cluster entrypoint' "$OUT/jm.log" | head -n 20
    echo; echo "== 作业状态变化 =="
    grep -nE 'switched from state|switched from [A-Z]+ to [A-Z]+' "$OUT/jm.log" | grep -v 'DEPLOYING\|INITIALIZING to' | head -n 60
    echo; echo "== 检查点（触发、完成、失败、过期、拒绝）=="
    grep -nE 'Triggering checkpoint|Completed checkpoint|Checkpoint [0-9]+ of job|Decline checkpoint|expired|Failed to trigger|Error while processing|Discarding checkpoint|subsumed' "$OUT/jm.log" | tail -n 80
    echo; echo "== 异常与内存（JobManager）=="
    grep -nE 'OutOfMemory|Java heap space|GC overhead|Exception|FATAL|Fatal error|ERROR' "$OUT/jm.log" | head -n 60
    echo; echo "== 异常与内存（TaskManager）=="
    for f in tm2.log tm3.log; do
        echo "-- $f --"
        grep -nE 'OutOfMemory|Java heap space|Exception|FATAL|Fatal error|ERROR|maxphysicalbytes|maxbytes' "$OUT/$f" | head -n 30
    done
    echo; echo "== M3 训练记录 =="
    grep -hE '\[M3\] Device .* (entering|trained|REPORT|calibrated)' "$OUT/tm2.log" "$OUT/tm3.log" | sed 's/^.*\[M3\]/[M3]/' 
    echo; echo "== 检查点相关集群配置 =="
    python3 -c '
import json,sys
try:
    for kv in json.load(open(sys.argv[1])):
        k=kv.get("key","")
        if any(s in k for s in ("checkpoint","memory","akka.framesize","heartbeat","restart")):
            print(k, "=", kv.get("value"))
except Exception as e:
    print("（读不到配置：%s）" % e)' "$OUT/jm_config.json"
    echo; echo "== 作业列表 =="; cat "$OUT/jobs.json"; echo
    echo; echo "== 内存 =="; cat "$OUT/memory.txt"
} > "$OUT/diag_summary.txt"
cat "$OUT/diag_summary.txt"
echo
echo "[diag] 完成，文件在 ${OUT}。日志体积：$(du -sh "$OUT" | cut -f1)"
