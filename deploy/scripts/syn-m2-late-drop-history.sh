#!/usr/bin/env bash
# ============================================================================
# syn-m2-late-drop-history.sh
# 从 master 上的 Prometheus 取回点通道迟到丢弃计数 m2_gate_late_drop 的历史（只读），回答三个问题：
#   1. 近 15 天里每个 Flink 作业各丢了多少（三月重跑、三月至四月运行等能否放在一起比较）；
#   2. 本次运行的丢弃发生在什么时候（与 M3 冷启动训练的时段对照）；
#   3. 丢在哪些子任务上，也就是哪些设备。子任务与设备的对应取决于作业是否用了设备代理键：
#      用代理键（2026-10-05 起的默认）时 A..H 依次在子任务 0..7；用原始设备号时子任务 1 = B、C、E，
#      7 = D、G，2 = H，5 = F，6 = A。表中两种对应都列出。
# Read the history of the point channel's late-drop counter from Prometheus (read-only): totals per job
# over 15 days, and this run's timeline per subtask.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录，python3；ssh 别名 fa-master 可用。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m2-late-drop-history.sh --since "$SINCE" --out-dir docs/m3_marapr/late_drop
#    --since 为第五步记下的 SINCE，决定第 2、3 问的起点（到现在为止）。
#    可选 --job <作业编号前缀>：第 2、3 问只看这个作业（例如查三月重跑：--since 2026-10-01T09:20:00Z --job b6687789）。
# 3. 前置条件 / Preconditions: master 上 Prometheus 在运行（保留期默认 15 天）。
# 4. 期望产出 / Expected output: --out-dir 下 per_job.csv（每个作业的首末采样时刻与丢弃合计）、
#    timeline.csv（本次运行每分钟、每个子任务的累计丢弃）、late_drop_summary.md；终端打印摘要。
# 5. 失败兜底 / Failure fallback: Prometheus 查询失败或无数据时，摘要里注明「无数据」并以退出码 3 结束。
# ============================================================================
set -uo pipefail

SINCE=""; OUT=""; JOB=""
while [[ $# -gt 0 ]]; do
    if [[ "$1" == --* && -z "${2:-}" ]]; then
        echo "ERROR: 参数 $1 后面缺少取值（命令是否被拆成了两行？请写在同一行）" >&2; exit 2
    fi
    case "$1" in
        --since) SINCE="$2"; shift 2 ;;
        --out-dir) OUT="$2"; shift 2 ;;
        --job) JOB="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done
{ [ -z "$SINCE" ] || [ -z "$OUT" ]; } && { echo "ERROR: 需要 --since 与 --out-dir" >&2; exit 2; }
mkdir -p "$OUT"

M="flink_taskmanager_job_task_operator_m2_gate_late_drop"
NOW=$(date -u +%s)
T_SINCE=$(python3 -c "
import datetime as d,sys
print(int(d.datetime.strptime(sys.argv[1],'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=d.timezone.utc).timestamp()))" "$SINCE")
qr() {   # $1 = 输出文件，$2 = PromQL，$3 = 起点，$4 = 步长秒
    ssh fa-master "curl -s --max-time 60 'http://localhost:9090/api/v1/query_range' \
        --data-urlencode 'query=$2' --data-urlencode 'start=$3' --data-urlencode 'end=$NOW' \
        --data-urlencode 'step=$4'" > "$OUT/$1"
}
qr prom_per_job.json "sum by (job_id, job_name) ($M)" $((NOW - 15 * 86400)) 300
SEL=""; [ -n "$JOB" ] && SEL="{job_id=~\"${JOB}.*\"}"
qr prom_timeline.json "sum by (subtask_index) (${M}${SEL})" "$T_SINCE" 60

python3 - "$OUT" <<'PY'
import csv, json, os, sys
from datetime import datetime, timezone
out = sys.argv[1]
RAW = {"0": "-", "1": "B、C、E", "2": "H", "3": "-", "4": "-", "5": "F", "6": "A", "7": "D、G"}
SUR = {str(i): d for i, d in enumerate("ABCDEFGH")}
def load(name):
    try:
        d = json.load(open(os.path.join(out, name)))
        return d["data"]["result"] if d.get("status") == "success" else None
    except Exception:
        return None
iso = lambda t: datetime.fromtimestamp(float(t), timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
L = ["# 点通道迟到丢弃计数的历史（Prometheus）", ""]
pj, tl = load("prom_per_job.json"), load("prom_timeline.json")
if not pj and not tl:
    print("Prometheus 无数据或查询失败。")
    sys.exit(3)

L += ["## 一、近 15 天每个作业的丢弃合计", "",
      "| 作业编号 | 作业名 | 首次采样 | 末次采样 | 首次出现丢弃 | 丢弃合计 |", "| --- | --- | --- | --- | --- | --- |"]
rows = []
for r in pj or []:
    v = [(float(t), float(x)) for t, x in r["values"]]
    first_pos = next((t for t, x in v if x > 0), None)
    rows.append((v[0][0], r["metric"].get("job_id", "?"), r["metric"].get("job_name", "?"), v[0][0], v[-1][0],
                 first_pos, max(x for _, x in v)))
rows.sort()
with open(os.path.join(out, "per_job.csv"), "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["job_id", "job_name", "first_sample_utc", "last_sample_utc", "first_drop_utc", "late_drop_total"])
    for _, jid, name, f, l, fp, tot in rows:
        w.writerow([jid, name, iso(f), iso(l), iso(fp) if fp else "", int(tot)])
        L.append("| %s | %s | %s | %s | %s | %d |" % (jid[:8], name, iso(f), iso(l), iso(fp) if fp else "无", tot))
L.append("")

L += ["## 二、本次运行逐子任务的丢弃", "",
      "| 子任务 | 设备（代理键） | 设备（原始编号） | 丢弃合计 | 首次出现丢弃 | 最后一次增加 |",
      "| --- | --- | --- | --- | --- | --- |"]
series = {}
for r in tl or []:
    series[r["metric"].get("subtask_index", "?")] = [(float(t), float(x)) for t, x in r["values"]]
with open(os.path.join(out, "timeline.csv"), "w", newline="") as fh:
    w = csv.writer(fh)
    subs = sorted(series, key=lambda s: int(s) if s.isdigit() else 99)
    w.writerow(["time_utc"] + ["subtask_" + s for s in subs])
    times = sorted({t for v in series.values() for t, _ in v})
    idx = {s: dict(v) for s, v in series.items()}
    for t in times:
        w.writerow([iso(t)] + [("%d" % idx[s][t]) if t in idx[s] else "" for s in subs])
for s in sorted(series, key=lambda s: int(s) if s.isdigit() else 99):
    v = series[s]
    tot = max(x for _, x in v)
    first = next((t for t, x in v if x > 0), None)
    last_inc = None
    for (t0, x0), (t1, x1) in zip(v, v[1:]):
        if x1 > x0:
            last_inc = t1
    L.append("| %s | %s | %s | %d | %s | %s |" % (s, SUR.get(s, "?"), RAW.get(s, "?"), tot, iso(first) if first else "无",
                                             iso(last_inc) if last_inc else "-"))
L.append("")
text = "\n".join(L) + "\n"
print(text)
open(os.path.join(out, "late_drop_summary.md"), "w", encoding="utf-8").write(text)
PY
