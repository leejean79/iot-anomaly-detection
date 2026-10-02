#!/usr/bin/env bash
# ============================================================================
# syn-m3-perf-collect.sh
# 从 master 上的 Prometheus 取回 V-M3-6 的三项运行期测量（2026-10-02 裁决第五节），写成本地表格：
#   1. 逐窗推理时延 m3_inference_latency_ms（上下文算子前向传播两侧计时，直方图）；
#   2. 点异常通道转发引入的延迟 m3_forward_delay_sec（滑动步末减轮时间戳，事件时间，直方图）；
#   3. 任务管理器内存 m3_javacpp_physical_bytes（进程物理内存）与 m3_javacpp_total_bytes（JavaCPP 堆外）。
# Fetch the three V-M3-6 run-time measurements from Prometheus on master and tabulate them locally.
#
# Flink 的 Prometheus 上报器把直方图导出为分位数序列（quantile 标签），每个分位数是该子任务最近 10,000 个
# 样本的统计。本脚本取运行期间每个分位数序列的最大值与最后一次读数，内存取运行期间的最大值。
# Flink exports a histogram as quantile series over the last 10,000 samples; this takes, per series,
# the maximum over the run and the last reading, and the maximum memory over the run.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录，python3；ssh 别名 fa-master 可用；master 上 Prometheus
#    容器在运行（端口 9090），并按 deploy/compose/prometheus.yml.template 采集两台 TaskManager 的 9249 端口。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m3-perf-collect.sh --since 2026-10-03T02:00:00Z --out-dir docs/m3_march/perf
#    --since 为提交作业前记下的 UTC 时刻，决定统计的时间范围（从该时刻到现在）。
# 3. 前置条件 / Preconditions: 作业运行过且用的是 2026-10-02 之后的 jar（带这几项指标）；
#    在作业取消后 Prometheus 保留期（默认 15 天）之内执行。
# 4. 期望产出 / Expected output: --out-dir 下 prom_*.json（原始查询结果）与 perf_summary.md（三张表）；
#    终端同时打印摘要。
# 5. 失败兜底 / Failure fallback: 某项查询无结果时摘要里注明「Prometheus 没有该指标的数据」，常见原因是
#    jar 太旧、Prometheus 没有采集 TaskManager，或 --since 晚于作业运行时段；用
#    `ssh fa-master "curl -s localhost:9090/api/v1/targets" | head -c 2000` 查看采集目标状态。
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

RANGE_SEC=$(python3 -c "
import datetime as d,sys
t=d.datetime.strptime(sys.argv[1],'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=d.timezone.utc)
print(int((d.datetime.now(d.timezone.utc)-t).total_seconds())+60)" "$SINCE")
echo "[perf] 统计范围：自 $SINCE 起，共 ${RANGE_SEC} 秒"

P="flink_taskmanager_job_task_operator"
q() {   # $1 = 输出文件名，$2 = PromQL
    ssh fa-master "curl -s --max-time 30 'http://localhost:9090/api/v1/query' --data-urlencode 'query=$2'" > "$OUT/$1"
}
for m in m3_inference_latency_ms m3_forward_delay_sec; do
    q "prom_${m}_max.json"  "max_over_time(${P}_${m}[${RANGE_SEC}s])"
    q "prom_${m}_last.json" "last_over_time(${P}_${m}[${RANGE_SEC}s])"
    q "prom_${m}_count.json" "max_over_time(${P}_${m}_count[${RANGE_SEC}s])"
done
for m in m3_javacpp_physical_bytes m3_javacpp_total_bytes; do
    q "prom_${m}_max.json" "max by (tm_id) (max_over_time(${P}_${m}[${RANGE_SEC}s]))"
done

python3 - "$OUT" <<'PY'
import json, os, sys
out = sys.argv[1]
def load(name):
    try:
        d = json.load(open(os.path.join(out, name)))
        return d.get("data", {}).get("result", []) if d.get("status") == "success" else None
    except Exception:
        return None
lines = ["# V-M3-6 运行期测量摘要", ""]
for m, unit in (("m3_inference_latency_ms", "毫秒"), ("m3_forward_delay_sec", "秒")):
    title = "逐窗推理时延（前向传播）" if "inference" in m else "点异常通道转发引入的延迟（滑动步末减轮时间戳）"
    lines += ["## %s，单位%s" % (title, unit), ""]
    mx, last, cnt = load("prom_%s_max.json" % m), load("prom_%s_last.json" % m), load("prom_%s_count.json" % m)
    if not mx:
        lines += ["Prometheus 没有该指标的数据。", ""]
        continue
    rows = {}
    for kind, res in (("max", mx), ("last", last or [])):
        for r in res:
            sub = r["metric"].get("subtask_index", "?")
            qn = r["metric"].get("quantile", "?")
            rows.setdefault(sub, {})[(kind, qn)] = float(r["value"][1])
    counts = {r["metric"].get("subtask_index", "?"): float(r["value"][1]) for r in (cnt or [])}
    qs = sorted({k[1] for v in rows.values() for k in v}, key=lambda x: float(x) if x != "?" else 9)
    lines.append("| 子任务 | 样本数 | " + " | ".join("P%s 最大/最后" % ("%g" % (float(x) * 100)) for x in qs) + " |")
    lines.append("| --- | --- | " + " | ".join("---" for _ in qs) + " |")
    for sub in sorted(rows, key=lambda x: int(x) if x.isdigit() else 99):
        v = rows[sub]
        lines.append("| %s | %s | %s |" % (sub, "%d" % counts[sub] if sub in counts else "-", " | ".join(
            "%.2f / %s" % (v.get(("max", x), float("nan")), ("%.2f" % v[("last", x)]) if ("last", x) in v else "-")
            for x in qs)))
    lines.append("")
lines += ["## 任务管理器内存（运行期间最大值）", ""]
for m, title in (("m3_javacpp_physical_bytes", "进程物理内存"), ("m3_javacpp_total_bytes", "JavaCPP 堆外分配")):
    res = load("prom_%s_max.json" % m)
    if not res:
        lines.append("- %s：Prometheus 没有该指标的数据。" % title)
        continue
    for r in res:
        lines.append("- %s，%s：%.0f MB" % (title, r["metric"].get("tm_id", "?"), float(r["value"][1]) / 1048576))
lines.append("")
text = "\n".join(lines)
print(text)
open(os.path.join(out, "perf_summary.md"), "w", encoding="utf-8").write(text + "\n")
PY
echo "[perf] 完成：$OUT/perf_summary.md"
