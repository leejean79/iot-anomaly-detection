#!/usr/bin/env python3
# ============================================================================
# daily_channel_profile.py
# 逐设备、逐通道、逐日（UTC）统计原始读数的中位数与 P10、P90，并与一个参照时段（默认 M3 阈值标定期
# 03-17 至 03-19）比较，用来核对三月重跑中 03-22 至 03-27 上下文通道告警集中出现的那几天，原始数据是否
# 确有全机队同时发生的变化。可选地读入 m3_scores.jsonl，把逐日告警率并排列出。
# Per-device, per-channel, per-UTC-day median and P10/P90 of raw readings, compared with a reference
# period (by default the M3 threshold-calibration days 03-17 to 03-19), to check whether the raw data
# really shifts fleet-wide on the days when the contextual channel's alarms cluster. Optionally lines
# up the daily alarm rate from m3_scores.jsonl.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt。
# 2. 调用命令：
#      python3 eda/daily_channel_profile.py \
#          --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
#          --start 2022-03-15 --end 2022-04-01 --scores docs/m3_march/m3_scores.jsonl \
#          --out docs/m3_march/daily_channel_profile.csv
#    可选：--ref-start 2022-03-17、--ref-end 2022-03-19、--channels Temperature,Gas,Humidity,Pressure,Light。
#    逐小时（2026-10-03 设计会话：预言三需要小时级的宽度回落时刻）：另加
#      --hourly-start 2022-03-26 --hourly-end 2022-03-29 --hourly-out docs/m3_march/hourly_gas_profile.csv
#    可选 --hourly-channels Gas。小时宽度倍数 = 该小时 P10–P90 宽度 ÷ 参照期内逐小时宽度的中位数；
#    用参照期的逐小时宽度作分母，是因为一小时内的散布天然小于一整天（日内周期不在其中），
#    拿日宽度作分母会把小时宽度系统性地压低。
# 3. 前置条件：原始 CSV 目录可读；--scores 可省略。
# 4. 期望产出：终端按通道打印一张「设备 × 日期」的表，每格为当日中位数相对参照期中位数的偏移，以参照期
#    P10 至 P90 的宽度为单位（0 表示与参照期相同，±1 表示偏移一个参照期的 P10–P90 宽度）；有 --scores 时
#    另打印逐日告警率。--out 处写出逐设备逐通道逐日的长表 CSV。有 --hourly-out 时另写逐设备逐小时表，
#    列含小时宽度倍数 width_x 与参照期逐小时宽度倍数的 P90（ref_p90_x，判定「回落」的上限）。
# 5. 失败兜底：区间内没有任何数据时退出码 2；某设备某通道参照期没有数据时该行显示「无参照」。
# ============================================================================
import argparse
import collections
import csv
import io
import json
import os
import sys
import warnings
from datetime import datetime, timezone

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from edalib import config                            # noqa: E402
from edalib.inventory import list_all_files          # noqa: E402
from edalib.scan import sniff_schema                 # noqa: E402


def ts(day: str) -> int:
    return int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--start", default="2022-03-15")
    ap.add_argument("--end", default="2022-04-01")
    ap.add_argument("--ref-start", default="2022-03-17")
    ap.add_argument("--ref-end", default="2022-03-19")
    ap.add_argument("--channels", default="Temperature,Gas,Humidity,Pressure,Light")
    ap.add_argument("--scores", default="")
    ap.add_argument("--out", required=True)
    ap.add_argument("--hourly-start", default="")
    ap.add_argument("--hourly-end", default="")
    ap.add_argument("--hourly-channels", default="Gas")
    ap.add_argument("--hourly-out", default="")
    a = ap.parse_args()
    hourly = bool(a.hourly_out and a.hourly_start and a.hourly_end)
    hch = a.hourly_channels.split(",")
    hwin = [(ts(a.hourly_start), ts(a.hourly_end)), (ts(a.ref_start), ts(a.ref_end))] if hourly else []
    hvals = collections.defaultdict(list)         # (device, channel, hour epoch) -> arrays

    lo, hi = ts(a.start), ts(a.end)
    channels = a.channels.split(",")
    vals = collections.defaultdict(list)          # (device, channel, day) -> arrays
    for path in list_all_files(a.data_dir):
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
        t = pd.to_numeric(df["Time"], errors="coerce")
        sens = df["Sensor"].astype(str).str.strip()
        sel = (t >= lo) & (t < hi) & sens.isin(channels)
        if not sel.any():
            continue
        sub = pd.DataFrame({"dev": df["DeviceId"].astype(str).str.strip()[sel], "ch": sens[sel],
                            "day": pd.to_datetime(t[sel], unit="s", utc=True).dt.strftime("%m-%d"),
                            "v": pd.to_numeric(df["Value"][sel], errors="coerce")}).dropna()
        for (dev, ch, day), g in sub.groupby(["dev", "ch", "day"]):
            vals[(dev, ch, day)].append(g["v"].to_numpy())
        if hourly:
            tt = t[sel]
            hsel = sens[sel].isin(hch) & np.logical_or.reduce([(tt >= x) & (tt < y) for x, y in hwin])
            if hsel.any():
                hs = pd.DataFrame({"dev": sub["dev"].reindex(tt.index)[hsel], "ch": sens[sel][hsel],
                                   "hour": (tt[hsel] // 3600 * 3600).astype("int64"),
                                   "v": pd.to_numeric(df["Value"][sel][hsel], errors="coerce")}).dropna()
                for (dev, ch, hr), g in hs.groupby(["dev", "ch", "hour"]):
                    hvals[(dev, ch, int(hr))].append(g["v"].to_numpy())
    if not vals:
        print("ERROR: %s 至 %s 内没有数据。" % (a.start, a.end), file=sys.stderr)
        return 2

    ref_days = {datetime.utcfromtimestamp(x).strftime("%m-%d") for x in range(ts(a.ref_start), ts(a.ref_end), 86400)}
    stats = {}
    for k, arrs in vals.items():
        v = np.concatenate(arrs)
        stats[k] = (len(v), float(np.median(v)), float(np.percentile(v, 10)), float(np.percentile(v, 90)), v)
    devices = sorted({k[0] for k in stats})
    days = sorted({k[2] for k in stats})

    rows = []
    for ch in channels:
        print("\n== %s：当日中位数相对参照期（%s 至 %s）中位数的偏移，单位为参照期 P10–P90 宽度 ==" % (ch, a.ref_start, a.ref_end))
        print("设备 " + " ".join("%6s" % d for d in days))
        for dev in devices:
            ref = [stats[(dev, ch, d)][4] for d in ref_days if (dev, ch, d) in stats]
            if not ref:
                print("%-4s 无参照" % dev)
                continue
            r = np.concatenate(ref)
            rmed, width = float(np.median(r)), float(np.percentile(r, 90) - np.percentile(r, 10)) or 1e-9
            cells = []
            for d in days:
                s = stats.get((dev, ch, d))
                if s is None:
                    cells.append("%6s" % "-")
                    continue
                shift = (s[1] - rmed) / width
                cells.append("%6.2f" % shift)
                rows.append({"device": dev, "channel": ch, "day": d, "n": s[0], "median": round(s[1], 4),
                             "p10": round(s[2], 4), "p90": round(s[3], 4), "ref_median": round(rmed, 4),
                             "ref_p10_p90_width": round(width, 4), "shift_in_ref_widths": round(shift, 3)})
            print("%-4s " % dev + " ".join(cells))

    if a.scores and os.path.exists(a.scores):
        n, al = collections.Counter(), collections.Counter()
        for line in open(a.scores, encoding="utf-8"):
            r = json.loads(line)
            d = datetime.utcfromtimestamp(r["windowEnd"]).strftime("%m-%d")
            n[(r["device"], d)] += 1
            al[(r["device"], d)] += bool(r["aboveThreshold"])
        print("\n== 上下文通道逐日告警率（%）==")
        print("设备 " + " ".join("%6s" % d for d in days))
        for dev in devices:
            print("%-4s " % dev + " ".join(
                "%6.0f" % (100.0 * al[(dev, d)] / n[(dev, d)]) if n[(dev, d)] else "%6s" % "-" for d in days))

    if hourly:
        write_hourly(a, hvals, stats, ref_days)

    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print("\n长表：%s（%d 行）" % (a.out, len(rows)))
    return 0


def write_hourly(a, hvals, stats, ref_days):
    """逐小时表：只含 --hourly-start 至 --hourly-end 的小时；参照期的小时只用来定分母与回落上限。
    Hourly table for the requested hours; reference-period hours only set the denominator and the cap."""
    r0, r1, h0, h1 = ts(a.ref_start), ts(a.ref_end), ts(a.hourly_start), ts(a.hourly_end)
    rows = []
    for dev, ch in sorted({(k[0], k[1]) for k in hvals}):
        def width(hr):
            v = np.concatenate(hvals[(dev, ch, hr)])
            return v, float(np.percentile(v, 90) - np.percentile(v, 10))
        ref_w = [width(h)[1] for h in range(r0, r1, 3600) if (dev, ch, h) in hvals]
        ref_all = [stats[(dev, ch, d)][4] for d in ref_days if (dev, ch, d) in stats]
        if not ref_w or not ref_all:
            continue
        ref_med_w = float(np.median(ref_w)) or 1e-9
        ref_p90_x = float(np.percentile([w / ref_med_w for w in ref_w], 90))
        r = np.concatenate(ref_all)
        rmed, dwidth = float(np.median(r)), float(np.percentile(r, 90) - np.percentile(r, 10)) or 1e-9
        for h in range(h0, h1, 3600):
            if (dev, ch, h) not in hvals:
                continue
            v, w = width(h)
            rows.append({"device": dev, "channel": ch,
                         "hour_utc": datetime.fromtimestamp(h, timezone.utc).strftime("%Y-%m-%dT%H:00Z"),
                         "n": len(v), "median": round(float(np.median(v)), 4), "p10_p90_width": round(w, 4),
                         "ref_hourly_median_width": round(ref_med_w, 4), "width_x": round(w / ref_med_w, 3),
                         "ref_p90_x": round(ref_p90_x, 3),
                         "shift_in_ref_widths": round((float(np.median(v)) - rmed) / dwidth, 3)})
    if not rows:
        print("逐小时：%s 至 %s 内没有可用数据（或参照期缺数据）。" % (a.hourly_start, a.hourly_end))
        return
    os.makedirs(os.path.dirname(a.hourly_out) or ".", exist_ok=True)
    with open(a.hourly_out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print("逐小时表：%s（%d 行）" % (a.hourly_out, len(rows)))


if __name__ == "__main__":
    sys.exit(main())
