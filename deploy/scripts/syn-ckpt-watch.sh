#!/usr/bin/env bash
# ============================================================================
# syn-ckpt-watch.sh
# 轮询 Flink REST 接口，按 checkpoint 逐个记录**每算子**与**单子任务峰值**的状态大小，用于定位
# 哪个算子把 checkpoint 确认 RPC 撑到 akka.framesize（默认 10 MB）之上。
# Poll the Flink REST API and record per-operator and peak-per-subtask checkpoint state sizes, to
# attribute the acknowledge-RPC that breaches akka.framesize (10 MB by default) to a single operator.
#
# 为什么需要它：越限发生在**确认 RPC** 上，因此越限的那一次 checkpoint 本身往往记不全大小；能定位来源
# 的是越限之前若干次**成功** checkpoint 的逐算子增长曲线。本脚本把这条曲线完整留存为 CSV。
# Why: the breach happens on the acknowledge RPC, so the failing checkpoint often records nothing.
# What identifies the culprit is the per-operator growth curve across the SUCCESSFUL checkpoints that
# precede it; this script persists that curve as a CSV.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash + python3），ssh 免密到 fa-master；deploy/.env 已就绪。
#    工作目录：仓库根目录（/path/to/iot-anomaly-detection）。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-ckpt-watch.sh                        # 自动挑选唯一 RUNNING 作业，20s 轮询
#      bash deploy/scripts/syn-ckpt-watch.sh --interval 15 --duration 1800
#      bash deploy/scripts/syn-ckpt-watch.sh --jid <JobID> --out docs/reports/ckpt_sizes.csv
#    参数 / Arguments:
#      --jid <JobID>      指定作业（不指定则自动选择唯一 RUNNING 作业；多个则报错要求显式指定）
#      --interval <秒>    轮询间隔，默认 20
#      --duration <秒>    总时长，默认 0 = 一直到 Ctrl-C 或作业结束
#      --out <路径>       CSV 输出路径，默认 docs/reports/ckpt_sizes_<JobID 前 8 位>.csv
#      --framesize-mb <n> 告警阈值（单子任务字节数超过它的 80% 即告警），默认 10
#      --top <n>          每次 checkpoint 控制台只打印最大的 n 个算子，默认 8
# 3. 前置条件 / Preconditions:
#      集群已启动且作业处于 RUNNING；提交作业时应带 --checkpoint-tolerable-failures（例如 100），
#      否则第一次越限就会重启作业，曲线在此中断。
# 4. 期望产出 / Expected output:
#      控制台每次 checkpoint 打印一张逐算子表（含单子任务峰值与是否逼近 framesize）；
#      同时在 --out 指定的 CSV 里累积每行一条 (checkpoint, 算子) 记录，供事后作图与归档。
# 5. 常见失败兜底 / Failure fallback:
#      读不到 /jobs → 检查 master 上 jobmanager 容器是否在跑、REST 端口是否为 8081；
#      /checkpoints 返回空 history → 作业刚提交、尚未触发第一次 checkpoint，等一个 --checkpoint-ms 周期；
#      作业中途消失（重启或失败）→ 脚本打印告警并退出码 3，CSV 保留已采集的部分。
#
# 缩写自查 / Abbreviations: REST = Flink 的 HTTP 接口（:8081）；JID = JobID；
#   framesize = Flink 内部 RPC（akka）单条消息上限；子任务 = 算子的一个并行实例。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

JID=""
INTERVAL=20
DURATION=0
OUT=""
FRAMESIZE_MB=10
TOP=8
while [[ $# -gt 0 ]]; do
    case "$1" in
        --jid) JID="$2"; shift 2 ;;
        --interval) INTERVAL="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --framesize-mb) FRAMESIZE_MB="$2"; shift 2 ;;
        --top) TOP="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
REST="http://$NODE_MASTER_IP:8081"
mcurl() { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "curl -s --max-time 20 '$1'"; }

# ---------- 选定作业 / resolve the job ----------
JOBS_JSON="$(mcurl "$REST/jobs")"
if [ -z "$JOBS_JSON" ]; then
    echo "ERROR: 读不到 $REST/jobs。请确认 master 上 jobmanager 容器在运行。" >&2
    exit 2
fi
if [ -z "$JID" ]; then
    JID="$(printf '%s' "$JOBS_JSON" | python3 -c '
import json,sys
running=[j["id"] for j in json.load(sys.stdin).get("jobs",[]) if j.get("status")=="RUNNING"]
print(running[0] if len(running)==1 else "")')"
    if [ -z "$JID" ]; then
        echo "ERROR: 未能自动确定作业（RUNNING 作业不唯一或为 0）。请用 --jid <JobID> 显式指定。" >&2
        printf '%s\n' "$JOBS_JSON" >&2
        exit 2
    fi
fi
[ -z "$OUT" ] && OUT="$REPO_DIR/docs/reports/ckpt_sizes_${JID:0:8}.csv"
mkdir -p "$(dirname "$OUT")"

JOB_JSON_FILE="$(mktemp)"
mcurl "$REST/jobs/$JID" > "$JOB_JSON_FILE"
JOB_NAME="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("name","?"))' "$JOB_JSON_FILE" 2>/dev/null || echo "?")"
echo "===================================================================="
echo "  作业 / job : $JOB_NAME"
echo "  JobID      : $JID"
echo "  轮询间隔   : ${INTERVAL}s    总时长: $([ "$DURATION" -eq 0 ] && echo '直到 Ctrl-C 或作业结束' || echo "${DURATION}s")"
echo "  CSV 输出   : $OUT"
echo "  framesize  : ${FRAMESIZE_MB} MB（单子任务超过其 80% 即告警）"
echo "===================================================================="

if [ ! -f "$OUT" ]; then
    echo "wall_clock,ckpt_id,ckpt_status,operator,state_size_bytes,state_size_mb,acked_subtasks,max_subtask_bytes,max_subtask_mb" > "$OUT"
fi

# 逐算子解析器：写成独立文件，供下面的循环用管道喂 JSON（见循环内的注释）。
# Per-operator parser, written to a file so the loop can pipe JSON into it.
PARSER="$(mktemp)"
cat > "$PARSER" <<'PY'
import json, sys, datetime

jid, cid, out, top, frame_mb, job_file = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), float(sys.argv[5]), sys.argv[6]
try:
    det = json.load(sys.stdin)
except ValueError as e:
    raw = sys.stdin.read() if not sys.stdin.closed else ""
    print("  [WARN] checkpoint %s 的 details 响应不是合法 JSON（%s）；前 200 字符：%s"
          % (cid, e, raw[:200]))
    sys.exit(0)
try:
    with open(job_file, encoding="utf-8") as jf:
        names = {v["id"]: v["name"] for v in json.load(jf).get("vertices", [])}
except Exception:
    names = {}

def pick(d, *keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return 0

status = det.get("status", "?")
rows = []
for vid, t in (det.get("tasks") or {}).items():
    total = pick(t, "state_size", "stateSize")
    acked = pick(t, "num_acknowledged_subtasks", "numAcknowledgedSubtasks")
    summary = t.get("summary") or {}
    ss = summary.get("state_size") or summary.get("stateSize") or {}
    peak = pick(ss, "max") if isinstance(ss, dict) else 0
    rows.append((names.get(vid, vid), int(total or 0), int(acked or 0), int(peak or 0)))

# 按"单子任务峰值"降序，而非合计：framesize 限制的是单条确认 RPC，合计大小不是判据。
# Sort by peak-per-subtask, not total: framesize caps a single acknowledge RPC.
rows.sort(key=lambda r: (-r[3], -r[1]))
wall = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
with open(out, "a", encoding="utf-8") as f:
    for name, total, acked, peak in rows:
        f.write('%s,%s,%s,"%s",%d,%.3f,%d,%d,%.3f\n'
                % (wall, cid, status, name.replace('"', "'"), total, total / 1048576.0, acked, peak, peak / 1048576.0))

warn = frame_mb * 1048576 * 0.8
print("\n[%s] checkpoint %s  状态=%s" % (wall, cid, status))
print("  %-46s %12s %10s %14s" % ("算子 / operator", "合计 MB", "已确认", "单子任务峰值 MB"))
for name, total, acked, peak in rows[:top]:
    flag = "  <== 逼近 framesize" if peak >= warn else ""
    print("  %-46s %12.3f %10d %14.3f%s"
          % (name[:46], total / 1048576.0, acked, peak / 1048576.0, flag))
if len(rows) > top:
    print("  …… 其余 %d 个算子已写入 CSV" % (len(rows) - top))
print("  本次合计：%.3f MB" % (sum(r[1] for r in rows) / 1048576.0))
PY

SEEN_FILE="$(mktemp)"
trap 'rm -f "$SEEN_FILE" "$JOB_JSON_FILE" "$PARSER"' EXIT
START_TS=$(date +%s)

while true; do
    NOW=$(date +%s)
    if [ "$DURATION" -gt 0 ] && [ $((NOW - START_TS)) -ge "$DURATION" ]; then
        echo "达到 --duration ${DURATION}s，结束采集。CSV: $OUT"
        exit 0
    fi

    CK_JSON="$(mcurl "$REST/jobs/$JID/checkpoints")"
    if [ -z "$CK_JSON" ] || printf '%s' "$CK_JSON" | grep -q '"errors"'; then
        echo "[$(date '+%F %T')] WARN: 作业 $JID 已不可查询（可能已重启/结束）。CSV 保留已采集部分: $OUT" >&2
        exit 3
    fi

    # 待处理的 checkpoint id（history 中尚未记录过的）/ checkpoint ids not yet recorded
    IDS="$(printf '%s' "$CK_JSON" | python3 -c '
import json,sys
h=json.load(sys.stdin).get("history",[])
print(" ".join(str(c["id"]) for c in sorted(h,key=lambda c:c["id"])))')"

    for CID in $IDS; do
        grep -qx "$CID" "$SEEN_FILE" 2>/dev/null && continue
        DET="$(mcurl "$REST/jobs/$JID/checkpoints/details/$CID")"
        [ -z "$DET" ] && continue
        printf '%s' "$DET" | grep -q '"errors"' && { echo "$CID" >> "$SEEN_FILE"; continue; }

        # 逐算子解析：tasks 是 vertexId → 汇总；名字需从作业计划里取。
        # Per-operator parse: "tasks" maps vertexId → summary; names come from the job plan.
        # 注意：这里必须调用**文件**而不是 `python3 - <<'PY'`。后者把 heredoc 当作标准输入来读取
        # 程序本身，管道里的 JSON 会被丢弃，json.load(sys.stdin) 只能读到 EOF。
        # NOTE: must invoke the parser as a FILE. With `python3 - <<'PY'` the heredoc becomes stdin
        # (the program itself), the piped JSON is discarded, and json.load(sys.stdin) sees EOF.
        printf '%s' "$DET" | python3 "$PARSER" "$JID" "$CID" "$OUT" "$TOP" "$FRAMESIZE_MB" "$JOB_JSON_FILE"
        echo "$CID" >> "$SEEN_FILE"
    done

    sleep "$INTERVAL"
done
