#!/usr/bin/env python3
# ============================================================================
# m5a_extract.py
# M5a 离线实验第一步（`docs/reports/m5a_offline_protocol.md` 第一至三节）：从原始读数算出数据层第一、二族信号所需的逐日统计量、
# 质量门控所需的逐日轮数与守卫打标轮数、第三族离群率的分母，以及两种参照的对照量。只做计算，不判读；判读由第二步
# `m5a/run_offline.py` 完成（不需要原始数据）。
# Step 1 of the M5a offline experiment: per-device daily statistics, quality counts, outlier-rate denominators and the
# reference comparison, computed from the raw readings. No evaluation here; step 2 is m5a/run_offline.py.
#
# 处理要点（与操作口径一致）：
#   - 轮 = 同一设备的不同时间戳；从 2022-03-01 00:00 UTC 起第 60,480 轮处冻结，参照样本为冻结之前（含）的全部读数；
#   - 变换域：光照取 log1p，光照等于 65,536 的右删失读数不进分布统计（守卫与入口判据照常计入，与数据接入模块相同）；
#   - 箱：参照排序后第 ⌊j(m−1)/400⌋ 个顺序统计量去重为边界，加下溢、上溢两个箱；
#   - G 检验：参照频数每箱（含溢出箱）加 0.5 再归一化；
#   - 设备 E 两遍：excl（去掉被注入通道在 [开始, 结束] 内的读数）与 inj（按 Injector 的语义在 [开始, 结束) 内改写读数）；
#   - 守卫：按 RepeatRunGuard 的规则（原始值逐位相等、缺失不延长不打断、相邻读数相隔超过 60 秒断开、冷启动清零、底值游程不计）；
#   - 冷启动：同一设备相邻两轮相隔超过 1000 × 10 秒；
#   - 离群率分母：冻结之后五个通道都有读数（含删失光照）的轮，按世界协调时整十分钟计数。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt（只用 pandas、numpy）。
# 2. 调用命令：
#      python3 eda/m5a_extract.py --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv
#    可选：--out-dir docs/m5a/extract（输出目录）、--start 2022-03-01、--end 2022-05-01（读入的世界协调时范围，含首不含尾）、
#    --inject docs/m3_a4/full_fix/inject-truth.csv、--guard docs/stuck_guard/thresholds.env、--bins 400、--devices ABCDEFGH。
# 3. 前置条件：原始 CSV 目录可读；仓库里有注入真值与守卫阈值文件。
# 4. 期望产出：docs/m5a/extract/ 下 daily.csv（设备、遍、日界、日、通道的统计量）、quality.csv（逐设备日的轮数、缺轮与守卫比例）、
#    denominators.csv（逐设备十分钟的入口轮数）、refs.csv（两种参照的中位数与宽度）、meta.md（冻结时刻、读数数）。终端逐台打印进度。
#    运行时长与全程扫描的分析脚本相近（读一遍全部文件，再逐台计算）。
# 5. 失败兜底：没有读到读数时退出码 2（多半是 --data-dir 不对）；缺注入或守卫文件时退出码 3；某台设备不足 60,480 轮时跳过该台并在
#    meta.md 中注明。
# ============================================================================
import argparse
import io
import math
import os
import re
import sys
import warnings

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from edalib import config                            # noqa: E402
from edalib.inventory import list_all_files          # noqa: E402
from edalib.scan import sniff_schema                 # noqa: E402

TZ = "Europe/London"
CHANNELS = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]
LIGHT = 4
LIGHT_CENSOR = 65536.0
CALIB_ROUNDS = 60480
GAP_SEC = 60
COLD_GAP_SEC = 1000 * 10
FIRST_DAY, LAST_DAY = "2022-03-08", "2022-04-30"
UTC_DAYS = ["2022-03-25", "2022-03-26", "2022-03-27", "2022-03-28", "2022-03-29"]   # 第 7 条
OLD_REF = ("2022-03-08", "2022-03-17")                                               # 已登记口径的参照（当地日，首尾都含）


# ---------------------------------------------------------------- 统计量（与 m5a/stats.py 相同）/ statistics
def ref_edges(ref, bins):
    s = np.sort(ref)
    idx = (np.arange(bins + 1) * (len(s) - 1)) // bins
    return np.unique(s[idx])


def cells(edges, x):
    xs = np.sort(x)
    under = np.searchsorted(xs, edges[0], side="left")
    c = np.searchsorted(xs, edges, side="right")
    return np.concatenate([[under], [c[0] - under], np.diff(c), [len(xs) - c[-1]]]).astype(float)


def ks_exact(a, b):
    a, b = np.sort(a), np.sort(b)
    grid = np.concatenate([a, b])
    return float(np.max(np.abs(np.searchsorted(a, grid, side="right") / len(a)
                               - np.searchsorted(b, grid, side="right") / len(b))))


def ks_p(d, n, m):
    lam = math.sqrt(n * m / (n + m)) * d
    if lam < 1e-3:
        return 1.0
    s = sum((-1) ** (k - 1) * math.exp(-2 * k * k * lam * lam) for k in range(1, 101))
    return max(0.0, min(1.0, 2 * s))


def day_stats(x, ref_sorted, edges, ref_cells):
    """当日样本对参照的全部统计量 / all statistics of one day against the reference."""
    n, m = len(x), len(ref_sorted)
    cx = cells(edges, x)
    d_bin = float(np.max(np.abs(np.cumsum(cx) / n - np.cumsum(ref_cells) / m)))
    pr = (ref_cells + 0.5) / (ref_cells + 0.5).sum()
    nz = cx > 0
    g = 2.0 * float(np.sum(cx[nz] * np.log(cx[nz] / (n * pr[nz]))))
    p10, med, p90 = np.percentile(x, [10, 50, 90])
    return dict(n=n, m=m, D_bin=d_bin, D_exact=ks_exact(x, ref_sorted), p_ks=ks_p(d_bin, n, m), G=g,
                G_df=len(cx) - 1, G_kl=g / (2.0 * n), max_cell_mass_day=float(cx.max() / n),
                median=float(med), p10=float(p10), p90=float(p90))


# ---------------------------------------------------------------- 输入 / inputs
def load(data_dir, t0, t1, devices):
    parts, nfiles = [], 0
    for path in list_all_files(data_dir):
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
        except OSError:
            continue
        has_header, looks_like_data, _ = sniff_schema(raw[: config.SNIFF_BYTES])
        if not looks_like_data:
            continue
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = pd.read_csv(io.BytesIO(raw), header=0 if has_header else None,
                                 names=config.CSV_COLUMNS, on_bad_lines="skip", engine="c",
                                 encoding="utf-8", encoding_errors="replace")
        except Exception:  # noqa: BLE001
            continue
        sensor = df["Sensor"].astype(str).str.strip()
        dev = df["DeviceId"].astype(str).str.strip()
        t = pd.to_numeric(df["Time"], errors="coerce")
        sel = sensor.isin(CHANNELS) & dev.isin(list(devices)) & (t >= t0) & (t < t1)
        if not sel.any():
            continue
        nfiles += 1
        parts.append(pd.DataFrame({"dev": dev[sel].astype("category"),
                                   "ch": sensor[sel].map({c: i for i, c in enumerate(CHANNELS)}).astype("int8"),
                                   "t": t[sel].astype("int64"),
                                   "v": pd.to_numeric(df["Value"][sel], errors="coerce")}).dropna())
    if not parts:
        return None, nfiles
    d = pd.concat(parts, ignore_index=True)
    d["dev"] = d["dev"].astype(str)
    return d.drop_duplicates(["dev", "ch", "t"], keep="first"), nfiles


def read_injections(path):
    out = []
    for r in pd.read_csv(path).itertuples():
        out.append(dict(device=r.device, ch=CHANNELS.index(r.channel), start=int(r.start_ts), end=int(r.end_ts),
                        type=r.type, mag=float(r.magnitude)))
    return out


def read_guard(path):
    txt = open(path, encoding="utf-8").read()
    thr = re.search(r'SYN_STUCK_RUN_THRESHOLDS="([^"]*)"', txt).group(1)
    flo = re.search(r'SYN_STUCK_FLOOR_VALUES="([^"]*)"', txt).group(1)
    thresholds = {k: [int(x) for x in v.split("/")] for k, v in (i.split("=") for i in thr.split(",") if i)}
    floors = {}
    for item in flo.split(","):
        if not item:
            continue
        key, vals = item.split("=")
        dev, ch = key.split(".")
        floors.setdefault(dev, [set() for _ in CHANNELS])[CHANNELS.index(ch)] = {float(x) for x in vals.split("/")}
    return thresholds, floors


# ---------------------------------------------------------------- 注入与守卫的移植 / ports
def apply_injections(ts, wide, specs):
    """Injector 的语义：[开始, 结束) 内，尖峰与阶跃加幅度，爬坡加 幅度 × 进度，卡死冻结为区间内第一条读数的值。"""
    wide = wide.copy()
    for s in specs:
        col = wide[:, s["ch"]]
        sel = (ts >= s["start"]) & (ts < s["end"]) & ~np.isnan(col)
        if not sel.any():
            continue
        if s["type"] in ("spike", "step"):
            col[sel] = col[sel] + s["mag"]
        elif s["type"] == "ramp":
            col[sel] = col[sel] + s["mag"] * ((ts[sel] - s["start"]) / (s["end"] - s["start"]))
        elif s["type"] == "stuck":
            col[sel] = col[sel][0]
        wide[:, s["ch"]] = col
    return wide


def exclude_injections(ts, wide, specs):
    wide = wide.copy()
    for s in specs:
        sel = (ts >= s["start"]) & (ts <= s["end"])
        wide[sel, s["ch"]] = np.nan
    return wide


def guard_flags(ts, wide, thr, floors):
    """RepeatRunGuard 的移植：返回每轮是否写了 stuckMask / port of RepeatRunGuard: per-round stuck flag."""
    n = len(ts)
    stuck = np.zeros(n, dtype=bool)
    last = [None] * 5
    last_ts = [0] * 5
    run = [0] * 5
    seen = [False] * 5
    prev = None
    for i in range(n):
        t = int(ts[i])
        if prev is not None and t - prev > COLD_GAP_SEC:            # 冷启动清零 / cold start resets
            seen = [False] * 5
            run = [0] * 5
        prev = t
        row = wide[i]
        for c in range(5):
            v = row[c]
            if v != v:                                              # 缺失：不延长不打断 / missing
                continue
            if seen[c] and v == last[c] and t - last_ts[c] <= GAP_SEC:
                run[c] += 1
            else:
                last[c], run[c], seen[c] = v, 1, True
            last_ts[c] = t
            if thr[c] > 0 and run[c] >= thr[c] and last[c] not in floors[c]:
                stuck[i] = True
    return stuck


# ---------------------------------------------------------------- 主程序 / main
def local_day(ts):
    return pd.to_datetime(ts, unit="s", utc=True).tz_convert(TZ).strftime("%Y-%m-%d").to_numpy()


def utc_day(ts):
    return pd.to_datetime(ts, unit="s", utc=True).strftime("%Y-%m-%d").to_numpy()


def day_seconds(day):
    """当地日的秒数（夏令时开始那天为 23 小时）。不能用「加一天的时长」，那是绝对的 24 小时。/ length of a local day."""
    a = pd.Timestamp(day, tz=TZ)
    nxt = pd.Timestamp((pd.Timestamp(day) + pd.Timedelta(days=1)).strftime("%Y-%m-%d"), tz=TZ)
    return int(nxt.timestamp() - a.timestamp())


def process_device(dev, g, specs, thr, floors, bins, out):
    g = g.sort_values("t", kind="mergesort")
    wide_df = g.pivot(index="t", columns="ch", values="v").reindex(columns=range(5))
    ts = wide_df.index.to_numpy().astype("int64")
    raw = wide_df.to_numpy(dtype=float)
    if len(ts) < CALIB_ROUNDS:
        out["meta"].append("%s：只有 %d 轮，不足 %d，跳过。" % (dev, len(ts), CALIB_ROUNDS))
        return
    tf = int(ts[CALIB_ROUNDS - 1])
    out["meta"].append("%s：冻结于 %s UTC；轮数 %d。" % (dev, pd.Timestamp(tf, unit="s", tz="UTC").strftime("%Y-%m-%d %H:%M:%S"), len(ts)))
    ldays, udays = local_day(ts), utc_day(ts)
    # 离群率分母：冻结之后五通道都有读数 / denominator: post-freeze rounds with all five channels
    adm = (ts > tf) & ~np.isnan(raw).any(axis=1)
    bins_end = (ts[adm] // 600) * 600 + 600
    for b, c in zip(*np.unique(bins_end, return_counts=True)):
        out["den"].append(dict(device=dev, bin_end=int(b), admitted=int(c)))
    my_specs = [s for s in specs if s["device"] == dev]
    variants = {"excl": exclude_injections(ts, raw, my_specs), "inj": apply_injections(ts, raw, my_specs)} if my_specs \
        else {"excl": raw, "inj": raw}
    for var, w in variants.items():
        stuck = guard_flags(ts, w, thr.get(dev, [0] * 5), floors.get(dev, [set() for _ in CHANNELS]))
        tv = w.copy()
        light = tv[:, LIGHT]
        light[light == LIGHT_CENSOR] = np.nan
        tv[:, LIGHT] = np.log1p(light)
        cal = ts <= tf
        refs = []
        for c in range(5):
            r = tv[cal, c]
            r = np.sort(r[~np.isnan(r)])
            e = ref_edges(r, bins)
            refs.append((r, e, cells(e, r)))
            if var == "excl":
                old = tv[(ldays >= OLD_REF[0]) & (ldays <= OLD_REF[1]), c]
                old = old[~np.isnan(old)]
                p10, med, p90 = np.percentile(r, [10, 50, 90])
                o10, omed, o90 = np.percentile(old, [10, 50, 90]) if len(old) else (np.nan,) * 3
                out["refs"].append(dict(device=dev, channel=CHANNELS[c], m=len(r), edges=len(e), median=med, p10=p10, p90=p90,
                                        old_n=len(old), old_median=omed, old_p10=o10, old_p90=o90))
        for kind, days, labels in (("local", sorted(set(ldays)), ldays), ("utc", UTC_DAYS, udays)):
            for day in days:
                if not (FIRST_DAY <= day <= LAST_DAY):
                    continue
                sel = labels == day
                if kind == "local":
                    expected = day_seconds(day) // 10
                    rounds, guard = int(sel.sum()), int(stuck[sel].sum())
                    reason = "标定日" if day == FIRST_DAY else (
                        "缺轮" if 1 - rounds / expected > 0.20 else ("守卫打标" if guard / expected > 0.20 else ""))
                    out["quality"].append(dict(device=dev, variant=var, day=day, expected=expected, rounds=rounds,
                                               missing_frac=round(1 - rounds / expected, 5), guard_rounds=guard,
                                               guard_frac=round(guard / expected, 5), evaluable=int(reason == ""), reason=reason))
                if day == FIRST_DAY:
                    continue
                for c in range(5):
                    x = tv[sel, c]
                    x = x[~np.isnan(x)]
                    if len(x) == 0:
                        continue
                    r, e, rc = refs[c]
                    st = day_stats(x, r, e, rc)
                    w_ref = np.percentile(r, 90) - np.percentile(r, 10)
                    st.update(device=dev, variant=var, daykind=kind, day=day, channel=CHANNELS[c],
                              shift=(st["median"] - np.percentile(r, 50)) / w_ref if w_ref > 0 else np.nan,
                              width_x=(st["p90"] - st["p10"]) / w_ref if w_ref > 0 else np.nan)
                    out["daily"].append(st)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--out-dir", default="docs/m5a/extract")
    ap.add_argument("--start", default="2022-03-01")
    ap.add_argument("--end", default="2022-05-01")
    ap.add_argument("--inject", default="docs/m3_a4/full_fix/inject-truth.csv")
    ap.add_argument("--guard", default="docs/stuck_guard/thresholds.env")
    ap.add_argument("--bins", type=int, default=400)
    ap.add_argument("--devices", default="ABCDEFGH")
    a = ap.parse_args()
    if not (os.path.exists(a.inject) and os.path.exists(a.guard)):
        print("ERROR: 缺少 %s 或 %s" % (a.inject, a.guard), file=sys.stderr)
        return 3
    specs = read_injections(a.inject)
    thr, floors = read_guard(a.guard)
    t0 = int(pd.Timestamp(a.start, tz="UTC").timestamp())
    t1 = int(pd.Timestamp(a.end, tz="UTC").timestamp()) + 86400        # 多读一天，覆盖 04-30 当地日的最后一小时
    d, nfiles = load(a.data_dir, t0, t1, a.devices)
    if d is None or d.empty:
        print("ERROR: 没有读到读数（检查 --data-dir）", file=sys.stderr)
        return 2
    print("读入文件 %d 个；去重后读数 %d 条" % (nfiles, len(d)))
    out = {k: [] for k in ("daily", "quality", "den", "refs", "meta")}
    for dev, g in d.groupby("dev"):
        print("处理设备 %s（%d 条读数）……" % (dev, len(g)))
        process_device(dev, g, specs, thr, floors, a.bins, out)
    os.makedirs(a.out_dir, exist_ok=True)
    pd.DataFrame(out["daily"]).to_csv(os.path.join(a.out_dir, "daily.csv"), index=False)
    pd.DataFrame(out["quality"]).to_csv(os.path.join(a.out_dir, "quality.csv"), index=False)
    pd.DataFrame(out["den"]).to_csv(os.path.join(a.out_dir, "denominators.csv"), index=False)
    pd.DataFrame(out["refs"]).to_csv(os.path.join(a.out_dir, "refs.csv"), index=False)
    with open(os.path.join(a.out_dir, "meta.md"), "w", encoding="utf-8") as fh:
        fh.write("# M5a 离线实验第一步的元数据\n\n读入文件 %d 个，读数 %d 条（%s 至 %s，世界协调时）。\n\n" % (nfiles, len(d), a.start, a.end))
        fh.write("\n".join("- " + x for x in out["meta"]) + "\n")
    print("完成：%s/ 下 daily.csv（%d 行）、quality.csv、denominators.csv、refs.csv、meta.md" % (a.out_dir, len(out["daily"])))
    return 0


if __name__ == "__main__":
    sys.exit(main())
