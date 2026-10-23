#!/usr/bin/env python3
# ============================================================================
# m5a_reference_check.py
# 数据层漂移监测（M5a）开工前的参照核查（交接文档决定二、三与第三节「参照样本的保存」，纯分析、只报事实）：
#   一、逐台的标定窗口：从 2022-03-01 00:00 UTC 起按轮（同一设备的不同时间戳）计数，第 60,480 轮处冻结（与数据接入模块
#       --calib-days 7 的口径相同），给出冻结时刻；
#   二、效应量门槛 D 零的推导表：标定窗口内每个完整的当地日，与整个标定窗口做两样本分布检验（精确经验累积分布），逐台逐通道
#       逐日给出 D；D 零 = 全部 D 的最大值向上取整到两位小数（决定三），并列出最大的十个来源；1.04 版起另给逐通道推导
#       （光照剔除 G），并在同一组参照分位数箱上算似然比（G）检验，以 G ÷ 2n（KL 散度估计）为效应量，同法推导 G 零；
#   三、直方图近似的核查：对每台每通道，按交接文档的「四百个等宽箱，范围为实测范围加余量」与另一种「按参照分位数等质量分箱」
#       各算一遍，给出参照样本在单个箱里的最大质量，以及标定日上分箱 D 与精确 D 的最大差。
# 读数在变换域处理（光照取 log1p，与数据接入模块相同）；光照等于 65536 的右删失读数不计入，与标定相同。分布检验的 D 与分箱 D
# 对任何单调变换不变，所以不需要冻结的中位数与四分位距。
# Pre-implementation reference checks for M5a: per-device calibration windows, the D0 derivation table, and the
# histogram approximation (equal-width vs reference-quantile bins). Facts only.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt（只用 pandas、numpy）。
# 2. 调用命令：
#      python3 eda/m5a_reference_check.py \
#          --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv
#    可选：--start 2022-03-01（标定起点，UTC）、--calib-rounds 60480、--bins 400、--margin 0.05（等宽箱范围两端各加实测
#    范围的百分之五）、--out-dir docs/m5a_ref、--report docs/reports/m5a_reference_check.md。
# 3. 前置条件：原始 CSV 目录可读（与 daily_channel_profile.py 相同）。
# 4. 期望产出：docs/reports/m5a_reference_check.md（报告）；docs/m5a_ref/calib_windows.csv、d_by_day.csv、hist_check.csv。
#    终端打印读入的文件数与读数。只读 03-01 至 03-10 的读数，运行时长约为全程扫描的一部分（仍需读一遍全部文件）。
# 5. 失败兜底：没有读到读数时退出码 2（多半是 --data-dir 不对）；某台设备不足 60,480 轮时报告中注明，不中止。
# ============================================================================
import argparse
import io
import math
import os
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
LIGHT_CENSOR = 65536.0


def load(data_dir, t0, t1):
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
        t = pd.to_numeric(df["Time"], errors="coerce")
        sel = sensor.isin(CHANNELS) & (t >= t0) & (t < t1)
        if not sel.any():
            continue
        nfiles += 1
        parts.append(pd.DataFrame({"dev": df["DeviceId"].astype(str).str.strip()[sel], "ch": sensor[sel],
                                   "t": t[sel], "v": pd.to_numeric(df["Value"][sel], errors="coerce")}).dropna())
    if not parts:
        return None, nfiles
    d = pd.concat(parts, ignore_index=True)
    d["t"] = d["t"].astype("int64")
    d = d.drop_duplicates(["dev", "ch", "t"], keep="first").sort_values(["dev", "t"], kind="mergesort")
    d["day"] = pd.to_datetime(d["t"], unit="s", utc=True).dt.tz_convert(TZ).dt.strftime("%Y-%m-%d")
    return d.reset_index(drop=True), nfiles


def ks_d(a, b):
    """两样本分布检验的统计量（精确经验累积分布）/ exact two-sample KS statistic."""
    a, b = np.sort(a), np.sort(b)
    grid = np.concatenate([a, b])
    fa = np.searchsorted(a, grid, side="right") / len(a)
    fb = np.searchsorted(b, grid, side="right") / len(b)
    return float(np.max(np.abs(fa - fb)))


def binned_d(edges, a, b):
    """在箱边界上比较累积分布（落在范围外的计入两端）/ CDF difference at the bin edges."""
    ca = np.searchsorted(np.sort(a), edges, side="right") / len(a)
    cb = np.searchsorted(np.sort(b), edges, side="right") / len(b)
    return float(np.max(np.abs(ca - cb)))


def g_test(edges, x, ref):
    """似然比（G）检验：当日在参照分位数箱上的频数对参照比例，G = 2 Σ O ln(O/E)；返回 (G, 自由度, G ÷ 2n)。
    G ÷ 2n 是两分布的 KL 散度（自然对数）的估计，不随样本量增长，作为 G 检验的效应量。
    Likelihood-ratio (G) test on the reference-quantile bins; G/(2n) estimates the KL divergence (effect size)."""
    rs, xs = np.sort(ref), np.sort(x)
    cr = np.searchsorted(rs, edges, side="right")
    cx = np.searchsorted(xs, edges, side="right")
    pr = np.diff(np.concatenate([[0], cr, [len(rs)]])) / len(rs)
    ox = np.diff(np.concatenate([[0], cx, [len(xs)]])).astype(float)
    keep = pr > 0
    e = len(xs) * pr[keep]
    o = ox[keep]
    nz = o > 0
    g = 2.0 * float(np.sum(o[nz] * np.log(o[nz] / e[nz])))
    return g, int(keep.sum()) - 1, g / (2.0 * len(xs))


def p_value(d, n, m):
    """渐近 p 值：Q_KS(√(nm/(n+m))·D)（与 scipy 的 asymp 方法相同的极限分布）/ asymptotic p-value."""
    lam = math.sqrt(n * m / (n + m)) * d
    if lam < 1e-3:
        return 1.0
    s = sum((-1) ** (k - 1) * math.exp(-2 * k * k * lam * lam) for k in range(1, 101))
    return max(0.0, min(1.0, 2 * s))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--start", default="2022-03-01")
    ap.add_argument("--calib-rounds", type=int, default=60480)
    ap.add_argument("--bins", type=int, default=400)
    ap.add_argument("--margin", type=float, default=0.05)
    ap.add_argument("--out-dir", default="docs/m5a_ref")
    ap.add_argument("--report", default="docs/reports/m5a_reference_check.md")
    a = ap.parse_args()
    t0 = int(pd.Timestamp(a.start, tz="UTC").timestamp())
    d, nfiles = load(a.data_dir, t0, t0 + 10 * 86400)
    if d is None or d.empty:
        print("ERROR: 没有读到读数（检查 --data-dir）", file=sys.stderr)
        return 2
    print("读入文件 %d 个；去重后读数 %d 条；设备 %s" % (nfiles, len(d), ",".join(sorted(d["dev"].unique()))))

    win, drows, hrows = [], [], []
    for dev, g in d.groupby("dev"):
        ts = np.unique(g["t"].to_numpy())
        if len(ts) < a.calib_rounds:
            win.append(dict(device=dev, rounds=len(ts), freeze_utc="不足", note="10 天内不足标定轮数"))
            continue
        tf = int(ts[a.calib_rounds - 1])
        win.append(dict(device=dev, rounds=a.calib_rounds,
                        freeze_utc=pd.Timestamp(tf, unit="s", tz="UTC").strftime("%Y-%m-%d %H:%M:%S"),
                        freeze_local=pd.Timestamp(tf, unit="s", tz="UTC").tz_convert(TZ).strftime("%Y-%m-%d %H:%M:%S"), note=""))
        cal = g[g["t"] <= tf]
        for ch in CHANNELS:
            c = cal[cal["ch"] == ch]
            if ch == "Light":
                c = c[c["v"] != LIGHT_CENSOR]
                v = np.log1p(c["v"].to_numpy())
            else:
                v = c["v"].to_numpy()
            if len(v) == 0:
                continue
            days = c["day"].to_numpy()
            lo, hi = float(v.min()), float(v.max())
            span = (hi - lo) or 1.0
            ew = np.linspace(lo - a.margin * span, hi + a.margin * span, a.bins + 1)
            qe = np.unique(np.quantile(v, np.linspace(0, 1, a.bins + 1)))
            ew_mass = np.histogram(v, bins=ew)[0].max() / len(v)
            # 分位数箱：边界上的并列值各成一箱，最大质量按「不大于该边界」的累积跳跃计
            cq = np.searchsorted(np.sort(v), qe, side="right") / len(v)
            q_mass = float(np.max(np.diff(np.concatenate([[0.0], cq]))))
            err_ew = err_q = 0.0
            full_days = sorted(set(days))[:-1]       # 最后一个当地日（冻结所在日）不完整，不计
            for day in full_days:
                x = v[days == day]
                if len(x) < 100:
                    continue
                de = ks_d(x, v)
                be, bq = binned_d(ew, x, v), binned_d(qe, x, v)
                gs, gdf, gk = g_test(qe, x, v)
                err_ew, err_q = max(err_ew, abs(de - be)), max(err_q, abs(de - bq))
                drows.append(dict(device=dev, channel=ch, day=day, n=len(x), m=len(v), D=round(de, 4),
                                  D_equal_width=round(be, 4), D_quantile=round(bq, 4),
                                  p_asymp="%.3g" % p_value(de, len(x), len(v)),
                                  G=round(gs, 2), G_df=gdf, G_over_2n=round(gk, 5)))
            hrows.append(dict(device=dev, channel=ch, m=len(v), distinct=int(len(np.unique(v))), lo=round(lo, 4), hi=round(hi, 4),
                              equal_width_bin=round((ew[1] - ew[0]), 5), equal_width_max_bin_mass=round(float(ew_mass), 4),
                              quantile_edges=len(qe), quantile_max_bin_mass=round(q_mass, 4),
                              max_err_equal_width=round(err_ew, 4), max_err_quantile=round(err_q, 4)))
    os.makedirs(a.out_dir, exist_ok=True)
    W, D, H = pd.DataFrame(win), pd.DataFrame(drows), pd.DataFrame(hrows)
    W.to_csv(os.path.join(a.out_dir, "calib_windows.csv"), index=False)
    D.to_csv(os.path.join(a.out_dir, "d_by_day.csv"), index=False)
    H.to_csv(os.path.join(a.out_dir, "hist_check.csv"), index=False)
    write_report(a, W, D, H, nfiles, len(d))
    print("报告：%s" % a.report)
    return 0


def md(df, cols):
    out = ["| " + " | ".join(cols) + " |", "| " + " | ".join("---" for _ in cols) + " |"]
    for _, r in df.iterrows():
        out.append("| " + " | ".join(str(r[c]) for c in cols) + " |")
    return out


def write_report(a, W, D, H, nfiles, nread):
    d0_raw = float(D["D"].max())
    d0 = math.ceil(d0_raw * 100) / 100
    L = ["# 数据层漂移监测（M5a）开工前的参照核查", "",
         "由 `eda/m5a_reference_check.py` 生成；纯分析、只报事实。读入文件 %d 个，03-01 起十天的读数 %d 条。读数在变换域（光照取 log1p），"
         "光照右删失读数（65536）不计；分布检验的 D 对单调变换不变，与是否标准化无关。" % (nfiles, nread), "",
         "## 一、逐台标定窗口（从 %s 00:00 UTC 起第 %d 轮处冻结）" % (a.start, a.calib_rounds), ""]
    L += md(W, list(W.columns)) + [""]
    L += ["## 二、效应量门槛 D 零的推导表（决定三）", "",
          "标定窗口内每个完整的当地日（冻结所在的那一天不完整，不计）与整个标定窗口比较。**全部 D 的最大值 %.4f，向上取整到两位小数，"
          "D 零 = %.2f。**" % (d0_raw, d0), "",
          "最大的十个来源：", ""]
    L += md(D.sort_values("D", ascending=False).head(10), ["device", "channel", "day", "n", "m", "D", "p_asymp"]) + [""]
    L += ["逐通道的最大值（只供参考，决定三是在设备与通道上一起取最大）：" + "；".join(
        "%s %.4f（%s %s）" % (ch, g["D"].max(), g.loc[g["D"].idxmax(), "device"], g.loc[g["D"].idxmax(), "day"])
        for ch, g in D.groupby("channel")) + "。", ""]
    L += ["**逐通道推导（开发设计日志 1.04 版，做法乙）**：每个通道在八台设备与七个完整标定日上取最大值，向上取整到两位小数（G 零同法，"
          "以 G ÷ 2n 为效应量）；光照通道剔除设备 G（标定窗口内有已登记的状态变化）。", "",
          "| 通道 | D 零 | D 的最大值（来源） | G 零（G ÷ 2n） | G ÷ 2n 的最大值（来源） | 参与推导的设备日 |",
          "| --- | --- | --- | --- | --- | --- |"]
    for ch in CHANNELS:
        g = D[(D["channel"] == ch) & ~((D["channel"] == "Light") & (D["device"] == "G"))]
        if g.empty:
            continue
        rd, rg = g.loc[g["D"].idxmax()], g.loc[g["G_over_2n"].idxmax()]
        L.append("| %s | %.2f | %.4f（%s %s） | %.2f | %.5f（%s %s） | %d |" % (
            ch, math.ceil(rd["D"] * 100) / 100, rd["D"], rd["device"], rd["day"],
            math.ceil(rg["G_over_2n"] * 100) / 100, rg["G_over_2n"], rg["device"], rg["day"], len(g)))
    L += [""]
    L += ["逐台逐通道的最大值：", "", "| 设备 | " + " | ".join(CHANNELS) + " |", "| --- |" + " --- |" * len(CHANNELS)]
    for dev, g in D.groupby("device"):
        L.append("| %s | %s |" % (dev, " | ".join("%.3f" % g[g["channel"] == c]["D"].max() if (g["channel"] == c).any() else "—"
                                                for c in CHANNELS)))
    L += ["", "## 三、直方图近似的核查（%d 个箱）" % a.bins, "",
          "「等宽」= 交接文档的方案：范围取参照样本的最小值到最大值、两端各加实测范围的 %d%%；「分位数」= 以参照样本的分位数为边界的等质量箱"
          "（并列值合并）。「单箱最大质量」是参照样本落在一个箱里的最大比例；分箱 D 与精确 D 之差不超过两样本单箱质量的较大者。"
          "最后两列是标定日上分箱 D 与精确 D 的最大差。" % round(a.margin * 100), ""]
    L += md(H, ["device", "channel", "m", "distinct", "equal_width_bin", "equal_width_max_bin_mass", "quantile_edges",
                "quantile_max_bin_mass", "max_err_equal_width", "max_err_quantile"]) + [""]
    with open(a.report, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")


if __name__ == "__main__":
    sys.exit(main())
