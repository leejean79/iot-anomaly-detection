#!/usr/bin/env python3
# ============================================================================
# m6a/reestimate.py
# 零级重估的 Python 参考实现（M6a 交接文档第四、五节）：从 eda/m6a_extract.py 的轮表出发，按数据接入模块的规则复现冻结，再对一条
# 手工命令执行零级：取 [生效 − D, 生效) 的轮、剔除守卫打标的轮、缺失与光照删失不计入，重估五个通道的中位数与分母并建参照直方图；
# 取 [生效 − 回填窗长, 生效) 内五个通道都有读数的轮，按新参数标准化为回填附件。结果写成 JSON，由 m6a/compare.py 与 Java 离线
# 运行器 com.leejean.m6.M6aOfflineRunner 的输出逐值比较（四个非光照通道逐位相同，光照通道容差 1e-12，1.11 版跨语言例外）。
# Python reference of the level-0 re-estimation, starting from the extracted round table.
#
# 与 Java 逐位对应的三处：中位数与四分位距用标准化算子自己的线性插值（lo·(1−frac) + hi·frac，不是 numpy 的公式）；参照直方图的
# 边界取第 ⌊j(m−1)/箱数⌋ 个顺序统计量后按位去重，P10、中位数、P90 用 numpy 2.x 的线性插值；冻结按轮数（含缺失轮）计。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation:
#      python3 m6a/reestimate.py --rounds docs/m6a/extract/rounds_G.csv.gz --device G --day 2022-03-25 --out docs/m6a/sim/G_py.json
#    可选：--effective <秒>（代替 --day）、--seq 1、--reestimate-days 1、--backfill-sec 3600、--warmup-rounds 60480、
#    --iqr-epsilon 1e-9、--start <秒>（轮表从此刻起算，短重放取区间起点）。
# 3. 前置条件 / Preconditions: 轮表已由 eda/m6a_extract.py 生成。
# 4. 期望产出 / Expected output: out 处的 JSON（冻结参数、新参数、重估窗口轮数与剔除数、参照直方图、回填附件）；终端打印新旧参数。
# 5. 失败兜底 / Failure fallback: 轮表不足以冻结或生效时刻早于冻结时退出码 2。
# ============================================================================
import argparse
import csv
import gzip
import json
import math
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

CHANNELS = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]
LIGHT = 4
CENSOR = 65536.0
BINS = 400
ZONE = ZoneInfo("Europe/London")


def transform(c, v):
    return math.log1p(v) if c == LIGHT else v


def read_rounds(path, start=None):
    rows = []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            t = int(r["ts"])
            if start is not None and t < start:
                continue
            x = [float(r[c]) if r[c] != "" else None for c in CHANNELS]
            rows.append((t, x, r["stuck"] == "1"))
    return rows


def scaler_percentile(s, q):
    """RobustScalerFunction.percentile 的逐位移植 / bitwise port of the scaler's own percentile."""
    n = len(s)
    if n == 1:
        return s[0]
    pos = q * (n - 1)
    lo, hi = math.floor(pos), math.ceil(pos)
    frac = pos - lo
    return s[lo] * (1 - frac) + s[hi] * frac


def np_percentile(s, q):
    """RefHistogram.percentile（numpy 2.x 线性插值）的逐位移植 / bitwise port of numpy's linear percentile."""
    n = len(s)
    v = (n - 1) * q
    if v >= n - 1:
        return s[n - 1]
    lo = math.floor(v)
    a, b = s[lo], s[lo + 1]
    g = v - lo
    d = b - a
    return b - d * (1 - g) if g >= 0.5 else a + d * g


def bits(v):
    return v.hex()


def ref_hist(s):
    m = len(s)
    raw = [s[(j * (m - 1)) // BINS] for j in range(BINS + 1)]
    edges, seen = [], set()
    for e in raw:                                          # 按位去重，与 Java 的 DoubleStream.distinct 相同 / dedup by bits
        if bits(e) not in seen:
            seen.add(bits(e))
            edges.append(e)
    import bisect
    cells = [bisect.bisect_left(s, edges[0])]
    prev = cells[0]
    for e in edges:
        le = bisect.bisect_right(s, e)
        cells.append(le - prev)
        prev = le
    cells.append(m - prev)
    return dict(edges=edges, cells=cells, m=m, p10=np_percentile(s, 0.10), median=np_percentile(s, 0.50),
                p90=np_percentile(s, 0.90))


def calibrate(samples, eps):
    out = dict(median=[], scale=[], bypass=[], hist=[])
    for c in range(5):
        s = sorted(samples[c])
        if not s:
            out["median"].append(0.0), out["scale"].append(1.0), out["bypass"].append(True), out["hist"].append(None)
            continue
        out["hist"].append(ref_hist(s))
        med = scaler_percentile(s, 0.50)
        rng = scaler_percentile(s, 0.75) - scaler_percentile(s, 0.25)
        if rng <= eps:
            out["median"].append(med), out["scale"].append(1.0), out["bypass"].append(True)
        else:
            out["median"].append(med), out["scale"].append(rng), out["bypass"].append(False)
    return out


def usable(c, x):
    return x[c] is not None and not (c == LIGHT and x[c] == CENSOR)


def freeze(rows, warmup, eps):
    samples = [[] for _ in range(5)]
    for t, x, _ in rows[:warmup]:
        for c in range(5):
            if usable(c, x):
                samples[c].append(transform(c, x[c]))
    return calibrate(samples, eps), rows[warmup - 1][0]


def normalize(x, cal):
    out = []
    for c in range(5):
        if x[c] is None:
            out.append(0.0)
            continue
        tv = transform(c, x[c])
        out.append(tv if cal["bypass"][c] else (tv - cal["median"][c]) / cal["scale"][c])
    return out


def level0(rows, eff, d_sec, bf_sec, eps):
    samples = [[] for _ in range(5)]
    window = guard = 0
    for t, x, stuck in rows:
        if eff - d_sec <= t < eff:
            window += 1
            if stuck:
                guard += 1
                continue
            for c in range(5):
                if usable(c, x):
                    samples[c].append(transform(c, x[c]))
    cal = calibrate(samples, eps)
    bf = [(t, normalize(x, cal)) for t, x, _ in rows if eff - bf_sec <= t < eff and all(v is not None for v in x)]
    return cal, window, guard, bf


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", required=True)
    ap.add_argument("--device", required=True)
    ap.add_argument("--day")
    ap.add_argument("--effective", type=int)
    ap.add_argument("--seq", type=int, default=1)
    ap.add_argument("--reestimate-days", type=int, default=1)
    ap.add_argument("--backfill-sec", type=int, default=3600)
    ap.add_argument("--warmup-rounds", type=int, default=60480)
    ap.add_argument("--iqr-epsilon", type=float, default=1e-9)
    ap.add_argument("--start", type=int)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    eff = a.effective if a.effective is not None else int(datetime.strptime(a.day, "%Y-%m-%d").replace(tzinfo=ZONE).timestamp())
    rows = read_rounds(a.rounds, a.start)
    if len(rows) < a.warmup_rounds:
        print("ERROR: 只有 %d 轮，不足以冻结" % len(rows), file=sys.stderr)
        return 2
    frz, t_freeze = freeze(rows, a.warmup_rounds, a.iqr_epsilon)
    if eff <= t_freeze:
        print("ERROR: 生效时刻 %d 不晚于冻结时刻 %d" % (eff, t_freeze), file=sys.stderr)
        return 2
    cal, window, guard, bf = level0([r for r in rows if r[0] < eff], eff, a.reestimate_days * 86400, a.backfill_sec, a.iqr_epsilon)
    res = dict(device=a.device, effectiveAt=eff, seq=a.seq, version=a.seq + 1, freezeTs=t_freeze,
               reestimateSec=a.reestimate_days * 86400, backfillSec=a.backfill_sec, windowRounds=window, guardExcluded=guard,
               freeze=dict(median=frz["median"], scale=frz["scale"], bypass=frz["bypass"]),
               new=dict(median=cal["median"], scale=cal["scale"], bypass=cal["bypass"]), hist=cal["hist"],
               backfill=dict(ts=[t for t, _ in bf], xNorm=[v for _, v in bf]))
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump(res, fh)
    print("%s 生效 %d：窗口 %d 轮，剔除守卫 %d 轮，回填 %d 轮" % (a.device, eff, window, guard, len(bf)))
    for c in range(5):
        print("  %-11s 冻结 中位数 %.6g 分母 %.6g → 重估 中位数 %.6g 分母 %.6g" % (
            CHANNELS[c], frz["median"][c], frz["scale"][c], cal["median"][c], cal["scale"][c]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
