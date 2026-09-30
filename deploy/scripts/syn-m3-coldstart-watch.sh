#!/usr/bin/env bash
# ============================================================================
# syn-m3-coldstart-watch.sh
# 冷启动期间的检查点时间线：定时读 Flink REST 接口，逐行记录作业状态、检查点计数（进行中、已完成、
# 已失败）、最近一次完成与失败的检查点、正在挂起的检查点已挂起多久，以及作业重启次数。
# Checkpoint timeline during the M3 cold start: poll the Flink REST API and log, per line, the job state,
# checkpoint counts, the latest completed and failed checkpoints, how long the pending one has been
# pending, and the job's restart count.
#
# 为什么需要它：syn-ckpt-watch.sh 记的是逐算子的状态大小，每个检查点只记第一次看到时的状态；训练期间
# 挂起的那次检查点在它那里只会留下 IN_PROGRESS。2026-09-30 裁决要求观察的恰恰是「训练期间挂起的检查点
# 在训练结束后是否正常完成」，所以需要一条按时间展开的计数曲线。
# Why: syn-ckpt-watch.sh records sizes once per checkpoint, so a checkpoint pending across training
# stays IN_PROGRESS there. The ruling asks whether that checkpoint completes after training.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash + python3），仓库根目录；ssh 别名 fa-master 可用。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m3-coldstart-watch.sh --out docs/m3_coldstart/ckpt_timeline.csv
#      bash deploy/scripts/syn-m3-coldstart-watch.sh --jid <JobID> --interval 15 --out <CSV 路径>
#    参数：--jid 不给时自动选名字以 M2Job 开头的唯一 RUNNING 作业；--interval 轮询秒数，默认 15；
#    --duration 总秒数，默认 0 表示一直到 Ctrl+C；--out CSV 路径（必需）。
# 3. 前置条件 / Preconditions: M2Job 已提交并处于 RUNNING。
# 4. 期望产出 / Expected output: 终端每次打印一行；--out 处逐行追加 CSV。训练期间应看到「进行中 1」
#    且挂起秒数持续增长、已完成计数不变；训练结束后已完成计数加一，最近完成的那次检查点端到端耗时
#    接近训练时长；重启次数始终为 0。
# 5. 失败兜底 / Failure fallback: 读不到 /jobs 时退出码 2（检查 jobmanager 容器）；找不到唯一的 M2Job
#    时退出码 2，用 --jid 指定；作业从 RUNNING 变为其他状态时照常记录一行后继续，以便看到重启过程。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

JID=""
INTERVAL=15
DURATION=0
OUT=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --jid) JID="$2"; shift 2 ;;
        --interval) INTERVAL="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done
[ -z "$OUT" ] && { echo "ERROR: 必须给出 --out <CSV 路径>" >&2; exit 2; }

REST="http://$NODE_MASTER_IP:8081"
mcurl() { ssh fa-master "curl -s --max-time 20 '$1'"; }

if [ -z "$JID" ]; then
    JOBS="$(mcurl "$REST/jobs/overview")"
    [ -z "$JOBS" ] && { echo "ERROR: 读不到 $REST/jobs/overview，请确认 jobmanager 容器在运行。" >&2; exit 2; }
    JID="$(printf '%s' "$JOBS" | python3 -c '
import json,sys
js=[j["jid"] for j in json.load(sys.stdin).get("jobs",[]) if j.get("state")=="RUNNING" and j.get("name","").startswith("M2Job")]
print(js[0] if len(js)==1 else "")')"
    [ -z "$JID" ] && { echo "ERROR: RUNNING 的 M2Job 不是恰好一个，请用 --jid 指定。" >&2; exit 2; }
fi
mkdir -p "$(dirname "$OUT")"
[ -f "$OUT" ] || echo "wall_clock,job_state,num_restarts,in_progress,completed,failed,pending_id,pending_sec,last_completed_id,last_completed_e2e_sec,last_completed_at,last_failed_id,last_failed_at,last_failed_reason" > "$OUT"
echo "[watch] 作业 ${JID}，每 ${INTERVAL}s 记录一次 → ${OUT}（Ctrl+C 结束）"

PARSER="$(mktemp)"
trap 'rm -f "$PARSER"' EXIT
cat > "$PARSER" <<'PY'
import json, sys, time, datetime
out = sys.argv[1]
docs = sys.stdin.read().split("\n@@\n")
def load(i):
    try:
        return json.loads(docs[i]) if i < len(docs) and docs[i].strip() else {}
    except ValueError:
        return {}
job, ck, met = load(0), load(1), load(2)
now_ms = time.time() * 1000
fmt = lambda ms: datetime.datetime.fromtimestamp(ms / 1000).strftime("%H:%M:%S") if ms else ""
state = job.get("state", "UNREACHABLE")
restarts = ""
for m in met if isinstance(met, list) else []:
    if m.get("id") in ("numRestarts", "fullRestarts"):
        restarts = m.get("value", "")
counts = ck.get("counts") or {}
pend = [h for h in (ck.get("history") or []) if h.get("status") == "IN_PROGRESS"]
pend_id, pend_sec = "", ""
if pend:
    p = min(pend, key=lambda h: h.get("id", 0))
    pend_id = p.get("id", "")
    pend_sec = "%.0f" % ((now_ms - p.get("trigger_timestamp", now_ms)) / 1000)
latest = ck.get("latest") or {}
lc, lf = latest.get("completed") or {}, latest.get("failed") or {}
lc_e2e = "%.1f" % (lc["end_to_end_duration"] / 1000) if lc.get("end_to_end_duration") is not None else ""
reason = (lf.get("failure_message") or "").replace(",", ";").replace("\n", " ")[:160]
row = [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), state, str(restarts),
       str(counts.get("in_progress", "")), str(counts.get("completed", "")), str(counts.get("failed", "")),
       str(pend_id), pend_sec, str(lc.get("id", "")), lc_e2e, fmt(lc.get("latest_ack_timestamp")),
       str(lf.get("id", "")), fmt(lf.get("failure_timestamp")), reason]
with open(out, "a", encoding="utf-8") as f:
    f.write(",".join(row) + "\n")
print("[%s] %s 重启=%s 进行中=%s 完成=%s 失败=%s 挂起=%s%s 最近完成=%s（端到端 %ss）%s"
      % (row[0][11:], state, restarts or "?", row[3], row[4], row[5],
         pend_id or "无", (" 已 %ss" % pend_sec) if pend_id else "", row[8] or "无", lc_e2e or "-",
         ("  最近失败=%s：%s" % (row[11], reason[:60])) if row[11] else ""))
PY

START=$(date +%s)
while true; do
    if [ "$DURATION" -gt 0 ] && [ $(( $(date +%s) - START )) -ge "$DURATION" ]; then
        echo "[watch] 达到 --duration ${DURATION}s，结束。"; exit 0
    fi
    # 三份 JSON 用单独一行 @@ 分隔，一次 ssh 取回，减少往返。/ three documents in one ssh round trip
    ssh fa-master "curl -s --max-time 20 '$REST/jobs/$JID'; printf '\n@@\n'; \
        curl -s --max-time 20 '$REST/jobs/$JID/checkpoints'; printf '\n@@\n'; \
        curl -s --max-time 20 '$REST/jobs/$JID/metrics?get=numRestarts,fullRestarts'" 2>/dev/null \
        | python3 "$PARSER" "$OUT"
    sleep "$INTERVAL"
done
