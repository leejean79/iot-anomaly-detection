#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
m2_surge.py —— DF-12 六月大停机恢复浪涌的量化分析（收尾任务二）。
Quantify the DF-12 June-outage recovery surge in the M2 point-anomaly channel (closeout Task 2).

从 synergia-monitoring 的转储里挑出 M2 快照（windowEnd>0），按设备构建离群率时间线，回答任务书四问：
  1. 恢复时刻各设备的离群率峰值 vs 各自常态基线；
  2. 从峰值衰减回常态用了多长事件时间（对照窗长 W）；
  3. 八台设备的浪涌是否近似同时（全局事件 vs 单点异常的形态实证）；
  4. 冷启动清空与浪涌的先后关系（用 M2 快照的 m2ColdCleared 精确定位清空时刻）。

================================ 脚本交付五要素 / Five delivery elements ================================
1. 执行环境 / Environment: 本地 python3（仅标准库）。输入为 synergia-monitoring 的 JSONL 转储。
2. 调用命令 / Invocation:
     # 先转储六月段监测（syn-m2-surge.sh 会做；或手动 console-consumer --timeout-ms 导出）
     python3 deploy/scripts/m2_surge.py --monitoring-jsonl monitoring.jsonl \
         --outage-end 2022-06-13T02:20:00Z --outage-hours 102.26 --window-sec 3600 \
         --timeline-csv docs/m2_surge_timeline.csv --stats-csv docs/m2_surge_stats.csv
3. 前置条件 / Preconditions: 转储含六月段 M2 快照（jar 需含 m2ColdCleared 字段——本次已加）。
4. 期望产出 / Expected output: stdout 四问量化表；timeline CSV（画图用）；stats CSV（逐设备汇总）。
5. 失败兜底 / Failure fallback: 无法解析的行跳过并计数；某设备无 M2 快照 → 该设备标 n/a，不臆造。
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone


def parse_epoch(s):
    """接受 epoch 秒 或 ISO8601（带 Z 或 +HHMM）。/ accept epoch seconds or ISO8601."""
    s = s.strip()
    if s.isdigit():
        return int(s)
    s = s.replace("Z", "+0000")
    if "+" not in s[10:] and "-" not in s[10:]:   # 无时区 → 当作 UTC
        s += "+0000"
    return int(datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z").timestamp())


def iso(ep):
    return datetime.fromtimestamp(ep, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def median(xs):
    if not xs:
        return None
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--monitoring-jsonl", required=True)
    ap.add_argument("--outage-end", default="2022-06-13T02:20:00Z", help="DF-12 恢复时刻（UTC）")
    ap.add_argument("--outage-hours", type=float, default=102.26)
    ap.add_argument("--window-sec", type=int, default=3600, help="窗长 W")
    ap.add_argument("--peak-search-hours", type=float, default=24.0, help="恢复后找峰值的时窗")
    ap.add_argument("--return-factor", type=float, default=1.5,
                    help="衰减回常态判据：率 ≤ 基线×该系数（基线≈0 时用绝对下限）")
    ap.add_argument("--return-abs-floor", type=float, default=0.001,
                    help="基线≈0 设备的绝对回落阈值（离群率）")
    ap.add_argument("--timeline-csv", default=None)
    ap.add_argument("--stats-csv", default=None)
    args = ap.parse_args()

    outage_end = parse_epoch(args.outage_end)
    outage_start = outage_end - int(args.outage_hours * 3600)
    W = args.window_sec
    peak_win = int(args.peak_search_hours * 3600)

    # 读入 M2 快照（windowEnd>0）：device -> list[(t, rate, points, occ, cold)]
    series = defaultdict(list)
    total = m2 = parse_err = 0
    with open(args.monitoring_jsonl, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                o = json.loads(line)
            except Exception:
                parse_err += 1
                continue
            we = int(o.get("windowEnd", 0) or 0)
            if we <= 0:               # M1 快照 windowEnd==0，跳过
                continue
            m2 += 1
            series[o.get("device", "?")].append((
                we,
                float(o.get("m2OutlierRate", 0.0) or 0.0),
                int(o.get("m2WindowPoints", 0) or 0),
                float(o.get("m2McOccupancy", 0.0) or 0.0),
                bool(o.get("m2ColdCleared", False)),
            ))
    for dev in series:
        series[dev].sort()

    print("=" * 74)
    print("DF-12 六月停机恢复浪涌分析 / DF-12 recovery-surge analysis")
    print("停机段 outage: %s ~ %s (%.2f h)；W=%ds" % (iso(outage_start), iso(outage_end), args.outage_hours, W))
    print("输入行 %d；M2 快照 %d；解析失败 %d；设备 %d" % (total, m2, parse_err, len(series)))
    print("=" * 74)

    stats = {}
    for dev in sorted(series):
        pts = series[dev]
        # 常态基线：停机前、窗口非空（排除预热空窗）的离群率中位数
        base_vals = [r for (t, r, n, occ, c) in pts if t < outage_start and n > 0]
        baseline = median(base_vals)
        # 恢复峰值：停机结束后 peak_win 内、窗口非空的最大离群率
        post = [(t, r, n, occ, c) for (t, r, n, occ, c) in pts
                if outage_end <= t <= outage_end + peak_win and n > 0]
        if baseline is None or not post:
            stats[dev] = None
            print("  %-2s  n/a（缺停机前基线或恢复后数据）" % dev)
            continue
        peak_t, peak_r = max(((t, r) for (t, r, n, occ, c) in post), key=lambda x: x[1])
        # 衰减回常态：从峰值时刻起，率首次 ≤ max(基线×return_factor, 绝对下限)
        thr = max(baseline * args.return_factor, args.return_abs_floor)
        decay_t = None
        for (t, r, n, occ, c) in pts:
            if t >= peak_t and n > 0 and r <= thr:
                decay_t = t
                break
        decay_sec = (decay_t - peak_t) if decay_t is not None else None
        # 冷启动清空时刻（恢复窗附近首个 m2ColdCleared）
        cold_ts = [t for (t, r, n, occ, c) in pts if c and t >= outage_start]
        first_cold = min(cold_ts) if cold_ts else None
        ratio = (peak_r / baseline) if baseline > 1e-9 else float("inf")
        stats[dev] = dict(baseline=baseline, peak_r=peak_r, peak_t=peak_t, ratio=ratio,
                          decay_sec=decay_sec, first_cold=first_cold, n_cold=len(cold_ts))

    # ---- 打印四问 ----
    print("\n【问一 + 问二】逐设备：常态基线 / 恢复峰值 / 倍数 / 衰减时长（对照 W=%ds）" % W)
    print("  dev  baseline%%   peak%%    ×倍    peak@             decay(s)  decay/W")
    for dev in sorted(stats):
        s = stats[dev]
        if s is None:
            print("  %-3s  n/a" % dev)
            continue
        rtxt = "inf" if s["ratio"] == float("inf") else "%.1f" % s["ratio"]
        dtxt = "n/a" if s["decay_sec"] is None else "%d" % s["decay_sec"]
        dwt = "n/a" if s["decay_sec"] is None else "%.2f" % (s["decay_sec"] / W)
        print("  %-3s  %8.4f  %7.4f  %5s  %s   %8s  %6s"
              % (dev, s["baseline"] * 100, s["peak_r"] * 100, rtxt, iso(s["peak_t"]), dtxt, dwt))

    # ---- 问三：同时性 ----
    peak_times = [(dev, s["peak_t"]) for dev, s in stats.items() if s]
    if peak_times:
        tmin = min(t for _, t in peak_times)
        tmax = max(t for _, t in peak_times)
        print("\n【问三】八台设备峰值时刻的离散度（同时性）：")
        for dev, t in sorted(peak_times, key=lambda x: x[1]):
            print("   %-3s  峰值@ %s   (距最早 %+d s)" % (dev, iso(t), t - tmin))
        print("   峰值时刻跨度 max−min = %d s = %.2f×W；%s"
              % (tmax - tmin, (tmax - tmin) / W,
                 "≈ 同时（同一窗口周转内）→ 支持'全局事件'" if (tmax - tmin) <= W
                 else "跨度大于一个 W，需看时间线判断"))

    # ---- 问四：冷启动 vs 浪涌 ----
    print("\n【问四】冷启动清空时刻 vs 峰值时刻（先后关系）：")
    for dev in sorted(stats):
        s = stats[dev]
        if not s:
            continue
        if s["first_cold"] is None:
            print("   %-3s  停机后无 m2ColdCleared（可能停机未超缓存深度，或数据缺该字段）" % dev)
        else:
            lead = s["peak_t"] - s["first_cold"]
            print("   %-3s  首个清空@ %s；峰值@ %s；清空领先峰值 %+d s（共清空 %d 次）"
                  % (dev, iso(s["first_cold"]), iso(s["peak_t"]), lead, s["n_cold"]))

    # ---- CSV ----
    if args.stats_csv:
        with open(args.stats_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["device", "baseline_rate", "peak_rate", "ratio", "peak_ts", "peak_iso",
                        "decay_sec", "decay_over_W", "first_cold_ts", "first_cold_iso", "n_cold"])
            for dev in sorted(stats):
                s = stats[dev]
                if not s:
                    w.writerow([dev] + ["n/a"] * 10)
                    continue
                w.writerow([dev, "%.6f" % s["baseline"], "%.6f" % s["peak_r"],
                            ("inf" if s["ratio"] == float("inf") else "%.3f" % s["ratio"]),
                            s["peak_t"], iso(s["peak_t"]),
                            (s["decay_sec"] if s["decay_sec"] is not None else "n/a"),
                            ("%.3f" % (s["decay_sec"] / W) if s["decay_sec"] is not None else "n/a"),
                            (s["first_cold"] if s["first_cold"] is not None else "n/a"),
                            (iso(s["first_cold"]) if s["first_cold"] is not None else "n/a"),
                            s["n_cold"]])
        print("\n[stats] → %s" % args.stats_csv)

    if args.timeline_csv:
        # 时间线：停机前 2h 至恢复后 peak_win，供画离群率时间线图
        lo = outage_start - 2 * 3600
        hi = outage_end + peak_win
        with open(args.timeline_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["device", "windowEnd", "iso", "m2OutlierRate", "m2WindowPoints",
                        "m2McOccupancy", "m2ColdCleared"])
            for dev in sorted(series):
                for (t, r, n, occ, c) in series[dev]:
                    if lo <= t <= hi:
                        w.writerow([dev, t, iso(t), "%.6f" % r, n, "%.4f" % occ, int(c)])
        print("[timeline] → %s（停机前2h ~ 恢复后%.0fh）" % (args.timeline_csv, args.peak_search_hours))


if __name__ == "__main__":
    main()
