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
#      --no-subtasks      关闭逐子任务峰值采集。默认**开启**：脚本会对每个算子额外调一次
#                         /checkpoints/details/<id>/subtasks/<vertexId>，取该算子各并行实例中
#                         最大的那个状态大小。这是判断是否逼近 akka.framesize 的唯一可靠依据，
#                         因为 framesize 限制的是单条确认 RPC（即单个子任务），而 details 接口
#                         在 Flink 1.13 上只给出该算子所有子任务的合计。代价是每次 checkpoint
#                         每个算子多一次 HTTP 请求，轮询间隔 15 秒以上时可忽略。
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
# 是否逐算子再调一次「逐子任务」接口取每个并行实例的状态大小。默认开启。
# 【为何默认开启】akka.framesize 限制的是**单条确认 RPC**，也就是**单个子任务**的状态，而
# /checkpoints/details/<id> 这个接口在 Flink 1.13 上只给出该算子所有子任务的**合计**，不含逐子任务
# 明细。若不取逐子任务，峰值一列只能留空，看上去像"远低于上限"，而这正是需要盯住的那个量。
# 代价是每次 checkpoint 每个算子多一次 HTTP 请求（经 ssh 转发），轮询间隔 15 秒以上时可忽略。
# Enabled by default: akka.framesize caps a single acknowledge RPC, i.e. one subtask, while the
# details endpoint on Flink 1.13 reports only the per-operator total across subtasks.
SUBTASKS=1
while [[ $# -gt 0 ]]; do
    case "$1" in
        --jid) JID="$2"; shift 2 ;;
        --interval) INTERVAL="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --framesize-mb) FRAMESIZE_MB="$2"; shift 2 ;;
        --top) TOP="$2"; shift 2 ;;
        --no-subtasks) SUBTASKS=0; shift ;;   # 关闭逐子任务采集（减少 HTTP 请求）
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
PEAKS="$(mktemp)"   # 逐子任务峰值 {vertexId: bytes} / per-subtask peaks
cat > "$PARSER" <<'PY'
import json, sys, datetime

jid, cid, out, top, frame_mb, job_file = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), float(sys.argv[5]), sys.argv[6]
peaks_file = sys.argv[7] if len(sys.argv) > 7 else ""
# 逐子任务峰值 {vertexId: bytes}，由外层脚本调 subtasks 接口收集；文件为空表示本次没有采集。
peaks = {}
if peaks_file:
    try:
        with open(peaks_file, encoding="utf-8") as pf:
            txt = pf.read().strip()
        if txt:
            peaks = json.loads(txt)
    except Exception:
        peaks = {}
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
    # 逐子任务峰值：Flink 1.13 的 /checkpoints/details/<id> 响应里**没有** per-task summary，
    # 该字段缺失时必须记为「不可用」而不是 0——否则会读成「远低于 framesize」的假象。
    # 真要拿峰值需再调 /checkpoints/details/<id>/subtasks/<vertexId>（本脚本暂未调用）。
    # Peak per subtask: Flink 1.13's details response has no per-task summary. Report it as
    # unavailable rather than 0, which would read as "far below framesize".
    summary = t.get("summary") or {}
    ss = summary.get("state_size") or summary.get("stateSize") or {}
    peak = ss.get("max") if isinstance(ss, dict) else None
    # 优先用 subtasks 接口取回的逐子任务峰值；details 自带的 summary 只是退路。
    if vid in peaks:
        peak = peaks[vid]
    peak = int(peak) if peak is not None else None
    rows.append((names.get(vid, vid), int(total or 0), int(acked or 0), peak))

# 按"单子任务峰值"降序，而非合计：framesize 限制的是单条确认 RPC，合计大小不是判据。
# Sort by peak-per-subtask, not total: framesize caps a single acknowledge RPC.
rows.sort(key=lambda r: (-(r[3] if r[3] is not None else -1), -r[1]))
wall = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
with open(out, "a", encoding="utf-8") as f:
    for name, total, acked, peak in rows:
        pk = "" if peak is None else "%d" % peak
        pk_mb = "" if peak is None else "%.3f" % (peak / 1048576.0)
        f.write('%s,%s,%s,"%s",%d,%.3f,%d,%s,%s\n'
                % (wall, cid, status, name.replace('"', "'"), total, total / 1048576.0, acked, pk, pk_mb))

warn = frame_mb * 1048576 * 0.8
print("\n[%s] checkpoint %s  状态=%s" % (wall, cid, status))
print("  %-46s %12s %10s %14s" % ("算子 / operator", "合计 MB", "已确认", "单子任务峰值 MB"))
if all(r[3] is None for r in rows):
    print("  （未取到逐子任务峰值：本 Flink 版本的 details 响应不含 per-task summary，且逐子任务接口"
          "也未返回可用数值；合计除以并行度只能作粗略下界。若是用 --no-subtasks 关闭了采集，去掉它即可）")
for name, total, acked, peak in rows[:top]:
    if peak is None:
        print("  %-46s %12.3f %10d %14s" % (name[:46], total / 1048576.0, acked, "n/a"))
        continue
    flag = "  <== 逼近 framesize" if peak >= warn else ""
    print("  %-46s %12.3f %10d %14.3f%s"
          % (name[:46], total / 1048576.0, acked, peak / 1048576.0, flag))
if len(rows) > top:
    print("  …… 其余 %d 个算子已写入 CSV" % (len(rows) - top))
print("  本次合计：%.3f MB" % (sum(r[1] for r in rows) / 1048576.0))
PY

SEEN_FILE="$(mktemp)"
trap 'rm -f "$SEEN_FILE" "$JOB_JSON_FILE" "$PARSER" "$PEAKS"' EXIT
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

        # 逐子任务峰值：details 接口不含该明细，需对每个算子再调一次 subtasks 接口。
        # 结果汇总成 {vertexId: 最大子任务状态字节数} 写入临时文件，供解析器读取。
        # Per-subtask peak: the details endpoint omits it, so call the subtasks endpoint per vertex
        # and collect {vertexId: max subtask state bytes} into a temp file for the parser.
        : > "$PEAKS"
        if [ "$SUBTASKS" -eq 1 ]; then
            VIDS="$(printf '%s' "$DET" | python3 -c '
import json,sys
try:
    print(" ".join((json.load(sys.stdin).get("tasks") or {}).keys()))
except Exception:
    print("")' 2>/dev/null)"
            {
                printf '{'
                FIRST=1
                for VID in $VIDS; do
                    SUB="$(mcurl "$REST/jobs/$JID/checkpoints/details/$CID/subtasks/$VID")"
                    MAXB="$(printf '%s' "$SUB" | python3 -c '
import json,sys
def size(d):
    if not isinstance(d, dict):
        return None
    for k in ("state_size", "stateSize"):
        if isinstance(d.get(k), (int, float)):
            return int(d[k])
    return None
try:
    doc = json.load(sys.stdin)
except Exception:
    print(""); raise SystemExit
best = None
for st in (doc.get("subtasks") or []):
    # 逐子任务的状态大小可能直接在子任务对象上，也可能在其嵌套的 checkpoint 对象里。
    v = size(st)
    if v is None:
        v = size(st.get("checkpoint"))
    if v is not None and (best is None or v > best):
        best = v
# 退路：若逐子任务明细缺失，但该接口给了 summary.state_size.max，也可用。
if best is None:
    sm = ((doc.get("summary") or {}).get("state_size") or {})
    if isinstance(sm.get("max"), (int, float)):
        best = int(sm["max"])
print("" if best is None else best)' 2>/dev/null)"
                    [ -z "$MAXB" ] && continue
                    [ "$FIRST" -eq 0 ] && printf ','
                    printf '"%s":%s' "$VID" "$MAXB"
                    FIRST=0
                done
                printf '}'
            } > "$PEAKS"
        fi

        # 逐算子解析：tasks 是 vertexId → 汇总；名字需从作业计划里取。
        # Per-operator parse: "tasks" maps vertexId → summary; names come from the job plan.
        # 注意：这里必须调用**文件**而不是 `python3 - <<'PY'`。后者把 heredoc 当作标准输入来读取
        # 程序本身，管道里的 JSON 会被丢弃，json.load(sys.stdin) 只能读到 EOF。
        # NOTE: must invoke the parser as a FILE. With `python3 - <<'PY'` the heredoc becomes stdin
        # (the program itself), the piped JSON is discarded, and json.load(sys.stdin) sees EOF.
        printf '%s' "$DET" | python3 "$PARSER" "$JID" "$CID" "$OUT" "$TOP" "$FRAMESIZE_MB" "$JOB_JSON_FILE" "$PEAKS"
        echo "$CID" >> "$SEEN_FILE"
    done

    sleep "$INTERVAL"
done
