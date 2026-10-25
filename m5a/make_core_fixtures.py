#!/usr/bin/env python3
# ============================================================================
# m5a/make_core_fixtures.py
# 为 Java 测试 `M5aCoreTest` 生成对照数据：一台设备 13 天的合成标准化轮（跨 2022-03-27 夏令时切换日；含预热、缺失通道、光照删失、
# 守卫打标、一天大量缺轮、两段漂移）与点通道离群记录（含之后滑动步的重复记录），以及 Python 离线实现（`eda/m5a_extract.py` 的
# 统计量、`m5b/optwin_offline.py` 的检测器、`m5a/run_offline.py` 的爆发定义）在它上面算出的参照直方图、日记录、状态转移与爆发。
# Fixtures for M5aCoreTest: synthetic rounds and outliers plus the Python offline results on them.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机，仓库根目录；python3 与 numpy、pandas。
# 2. 调用命令 / Invocation: python3 m5a/make_core_fixtures.py
# 3. 前置条件 / Preconditions: eda/m5a_extract.py、m5a/run_offline.py、m5b/optwin_offline.py 与 docs/m5b/tables/thesis_0.99_0.5_2016.csv。
# 4. 期望产出 / Expected output: src/test/resources/m5a/core_rounds.csv.gz、core_outliers.csv、core_ref.csv、core_records.csv、core_events.csv。
# 5. 失败兜底 / Failure fallback: 导入失败时检查上述路径。产出被 .gitignore 的 *.csv 规则屏蔽，提交时须 git add -f。
# ============================================================================
import gzip
import importlib.util
import os
import sys
from datetime import datetime, timezone

import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "m5a"))
sys.path.insert(0, os.path.join(ROOT, "m5b"))
import optwin_offline as ow   # noqa: E402
import run_offline as ro      # noqa: E402

spec = importlib.util.spec_from_file_location("ex", os.path.join(ROOT, "eda", "m5a_extract.py"))
ex = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ex)

D0 = [0.38, 0.73, 0.84, 0.43, 0.51]
WARMUP = 2 * 8640
rng = np.random.default_rng(21)
t0 = int(datetime(2022, 3, 20, tzinfo=timezone.utc).timestamp())
ts = []
t = t0
while t < t0 + 13 * 86400:
    if t0 + 9 * 86400 <= t < t0 + 10 * 86400 and rng.random() < 0.3:      # 03-29 大量缺轮 / heavy loss
        t += 10
        continue
    if rng.random() > 0.004:
        ts.append(t)
    t += 10 if rng.random() > 0.00002 else 3 * 3600
ts = np.array(ts, dtype="int64")
n = len(ts)
day = (ts - t0) / 86400.0
x = np.column_stack([
    np.round(21 + rng.normal(0, 0.3, n) + np.where((day > 6) & (day < 10), 1.2, 0), 1),
    np.round(40 + rng.normal(0, 1.0, n), 2),
    np.round(101000 + 300 * np.sin(day * 1.7) + rng.normal(0, 20, n)),
    np.round(20000 + rng.normal(0, 400, n) + np.where((day > 8) & (day < 10.5), 3000, 0)),
    np.where(((ts // 3600) % 24 < 6), 0.0, np.round(300 + rng.normal(0, 30, n))),
]).astype(float)
cens = rng.random(n) < 0.001
x[cens, 4] = 65536.0
missing = rng.random((n, 5)) < 0.002
stuck = np.zeros(n, dtype=bool)
stuck[(ts >= t0 + 10 * 86400 + 3600) & (ts < t0 + 10 * 86400 + 3600 + 6 * 3600)] = True     # 03-30 守卫打标约 25%
warm = np.arange(n) < WARMUP

# 离群记录：若干簇 / outlier clusters
out_rounds = []
for c in range(40):
    i = int(rng.integers(WARMUP + 10, n - 400))
    k = int(rng.integers(1, 40))
    out_rounds += list(range(i, i + k))
out_rounds = sorted(i for i in set(out_rounds) if not missing[i].any())     # 离群只出现在入口轮上 / outliers only on admitted rounds
scores = []
for i in out_rounds:
    rt = int(ts[i])
    we = (rt // 60) * 60 + 60
    scores.append((rt, we))
    if rng.random() < 0.5:
        scores.append((rt, we + 60))                                            # 之后滑动步的重复 / later slide duplicate

res = os.path.join(ROOT, "src", "test", "resources", "m5a")
os.makedirs(res, exist_ok=True)
f = lambda v: repr(float(v))
with gzip.open(os.path.join(res, "core_rounds.csv.gz"), "wt") as fh:
    fh.write("ts,x0,x1,x2,x3,x4,m0,m1,m2,m3,m4,censored,stuck,warmup\n")
    for i in range(n):
        fh.write(",".join([str(ts[i])] + [f(v) for v in x[i]] + [str(int(b)) for b in missing[i]]
                          + [str(int(cens[i])), str(int(stuck[i])), str(int(warm[i]))]) + "\n")
with open(os.path.join(res, "core_outliers.csv"), "w") as fh:
    fh.write("round_ts,window_end\n")
    for rt, we in scores:
        fh.write("%d,%d\n" % (rt, we))

# ---- Python 参照 / Python expectations
tv = x.copy()
tv[:, 4] = np.where(cens, np.nan, np.log1p(x[:, 4]))
tv[missing] = np.nan
refs = []
with open(os.path.join(res, "core_ref.csv"), "w") as fh:
    fh.write("channel,m,p10,median,p90,edges\n")
    for c in range(5):
        r = tv[warm, c]
        r = np.sort(r[~np.isnan(r)])
        e = ex.ref_edges(r, 400)
        refs.append((r, e, ex.cells(e, r)))
        fh.write("%d,%d,%r,%r,%r,%s\n" % (c, len(r), *[float(v) for v in np.percentile(r, [10, 50, 90])], " ".join(repr(float(v)) for v in e)))

ldays = ex.local_day(ts)
calib = ldays[WARMUP]                                  # 冻结后第一条轮所在的当地日 / day of the first frozen round
last_ts = int(ts[-1])
final = last_ts - 1
rows, quality = [], {}
for d in sorted(set(ldays)):
    if d < calib or ro.local_day_bounds(d)[1] > final:
        continue
    sel = ldays == d
    expected = ex.day_seconds(d) // 10
    rounds, guard = int(sel.sum()), int(stuck[sel].sum())
    reason = "标定日" if d == calib else ("缺轮" if 1 - rounds / expected > 0.20 else ("守卫打标" if guard / expected > 0.20 else ""))
    quality[d] = reason
    for c in range(5):
        st = None
        if d != calib:
            xs = tv[sel & ~warm, c]
            xs = xs[~np.isnan(xs)]
            if len(xs):
                r, e, rc = refs[c]
                st = ex.day_stats(xs, r, e, rc)
                w = np.percentile(r, 90) - np.percentile(r, 10)
                st["shift"] = (st["median"] - np.percentile(r, 50)) / w
                st["width_x"] = (st["p90"] - st["p10"]) / w
        rows.append(dict(day=d, c=c, rounds=rounds, expected=expected, guard=guard, reason=reason, st=st))
# 状态转移 / transitions
state = [False] * 5
events = []
recs = []
for r in rows:
    st, c = r["st"], r["c"]
    if r["reason"] == "" and st is not None:
        if not state[c] and st["p_ks"] < 0.01 / 5 and st["D_bin"] > D0[c]:
            state[c] = True
            events.append((ro.local_day_bounds(r["day"])[1], "DEVIATE", ex.CHANNELS[c], r["day"], st))
        elif state[c] and st["D_bin"] <= D0[c]:
            state[c] = False
            events.append((ro.local_day_bounds(r["day"])[1], "RECOVER", ex.CHANNELS[c], r["day"], st))
    recs.append((r, state[c]))
with open(os.path.join(res, "core_records.csv"), "w") as fh:
    fh.write("day,channel,evaluable,reason,rounds,expected,guard_rounds,n,d,p,p10,median,p90,shift,width_x,max_cell_mass,deviated\n")
    for r, dev in recs:
        st = r["st"] or {}
        g = lambda k: f(st[k]) if st else "0.0"
        fh.write(",".join([r["day"], ex.CHANNELS[r["c"]], "1" if r["reason"] == "" else "0", r["reason"], str(r["rounds"]),
                           str(r["expected"]), str(r["guard"]), str(st.get("n", 0)), g("D_bin"), g("p_ks"), g("p10"), g("median"),
                           g("p90"), g("shift"), g("width_x"), g("max_cell_mass_day"), "1" if dev else "0"]) + "\n")
# 爆发 / bursts
adm = {}
for i in range(n):
    if not warm[i] and not missing[i].any():
        b = (int(ts[i]) // 600) * 600 + 600
        adm[b] = adm.get(b, 0) + 1
num = {}
seen = set()
for rt, we in scores:
    if we - 60 <= rt < we and rt not in seen:
        seen.add(rt)
        b = (rt // 600) * 600 + 600
        num[b] = num.get(b, 0) + 1
elems = [("E", b, num.get(b, 0) / a) for b, a in sorted(adm.items()) if a > 0 and b + 60 <= final]
table = ow.load_table(os.path.join(ROOT, "docs", "m5b", "tables", "thesis_0.99_0.5_2016.csv"))
alarms = ow.run_stream(elems, table, 36)
bl = ro.bursts(alarms, elems, {"E": num})
with open(os.path.join(res, "core_events.csv"), "w") as fh:
    fh.write("ts,level,channel,test,magnitude,burst_size,burst_peak,burst_duration,data_day,data_d,data_p,data_shift,data_width_x\n")
    for tsv, lvl, ch, d, st in events:
        fh.write(",".join([str(tsv), lvl, ch, "KS", "0.0", "0", "0.0", "0", d, f(st["D_bin"]), f(st["p_ks"]), f(st["shift"]), f(st["width_x"])]) + "\n")
    rates = [r for _, _, r in elems]
    order = [b for _, b, _ in elems]
    for bu in bl:
        # 在线只在「其后一小时之外的元素已到、且包含报警元素的连续非零段已结束」时发出 / emitted online only when complete
        i = order.index(bu["ts"])
        close = i
        if rates[i] > 0:
            close = next((j for j in range(i + 1, len(order)) if rates[j] == 0), None)
        if close is None or not any(order[k] > bu["ts"] + 3600 for k in range(max(close, i + 1), len(order))):
            continue
        fh.write(",".join([str(bu["ts"]), "BURST", "outlier_rate", bu["test"], f(bu["magnitude"]), str(bu["size"]), f(bu["peak"]),
                           str(bu["duration"]), "", "0.0", "0.0", "0.0", "0.0"]) + "\n")
print("rounds %d, calib day %s, records %d, evaluable %d, transitions %d, bursts %d" % (n, calib, len(recs), sum(r["reason"] == "" for r, _ in recs), len(events), len(bl)))
