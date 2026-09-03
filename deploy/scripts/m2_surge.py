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

方法要点（相对初版的修正）：
  - **恢复时刻按数据自动侦测**：取每设备 M2 时间线里最大的事件时间空档（= 停机），空档后第一条即恢复点；
    不再依赖外部给定的停机结束时刻（实测恢复时刻可能与元数据不符）。可用 --outage-end 作兜底。
  - **常态基线用均值**（不是中位数）：大多数滑动步本就零离群，中位数=0 无意义；均值与探针 meanOutlierRate 同口径。
  - **区分两种时长**：离群浪涌的衰减时长（率回落到近基线）与窗口物理重填时长（点数回到常态）——二者未必相等。

================================ 脚本交付五要素 / Five delivery elements ================================
1. 执行环境 / Environment: 本地 python3（仅标准库）。输入为 synergia-monitoring 的 JSONL 转储。
2. 调用命令 / Invocation:
     python3 deploy/scripts/m2_surge.py --monitoring-jsonl monitoring.jsonl \
         --window-sec 3600 --timeline-csv docs/m2_surge_timeline.csv --stats-csv docs/m2_surge_stats.csv
3. 前置条件 / Preconditions: 转储含六月段 M2 快照；jar 需含 m2ColdCleared 字段（本次已加——若缺，脚本会告警）。
4. 期望产出 / Expected output: stdout 四问量化表；timeline CSV（画图用）；stats CSV（逐设备汇总）。
5. 失败兜底 / Failure fallback: 无法解析的行跳过并计数；某设备无 M2 快照/无停机空档 → 该设备标 n/a，不臆造。
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone


def parse_epoch(s):
    s = s.strip()
    if s.isdigit():
        return int(s)
    s = s.replace("Z", "+0000")
    if "+" not in s[10:] and "-" not in s[10:]:
        s += "+0000"
    return int(datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z").timestamp())


def iso(ep):
    if ep is None:
        return "n/a"
    return datetime.fromtimestamp(ep, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def mean(xs):
    return sum(xs) / len(xs) if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--monitoring-jsonl", required=True)
    ap.add_argument("--window-sec", type=int, default=3600, help="窗长 W")
    ap.add_argument("--min-outage-gap-hours", type=float, default=6.0,
                    help="判为停机的最小事件时间空档（小时）")
    ap.add_argument("--peak-search-hours", type=float, default=6.0, help="恢复后找峰值/衰减的时窗")
    ap.add_argument("--return-factor", type=float, default=1.5,
                    help="衰减回常态判据：率 ≤ 基线×该系数（基线≈0 时用绝对下限）")
    ap.add_argument("--return-abs-floor", type=float, default=0.002,
                    help="基线≈0 设备的绝对回落阈值（离群率）")
    ap.add_argument("--outage-end", default=None, help="兜底：无法自动侦测停机时用此恢复时刻")
    ap.add_argument("--timeline-csv", default=None)
    ap.add_argument("--stats-csv", default=None)
    args = ap.parse_args()

    W = args.window_sec
    min_gap = int(args.min_outage_gap_hours * 3600)
    peak_win = int(args.peak_search_hours * 3600)
    fallback_recovery = parse_epoch(args.outage_end) if args.outage_end else None

    series = defaultdict(list)
    total = m2 = parse_err = has_cold_field = 0
    with open(args.monitoring_jsonl, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total += 1
            if '"m2ColdCleared"' in line:
                has_cold_field += 1
            try:
                o = json.loads(line)
            except Exception:
                parse_err += 1
                continue
            we = int(o.get("windowEnd", 0) or 0)
            if we <= 0:
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

    print("=" * 76)
    print("DF-12 六月停机恢复浪涌分析 / DF-12 recovery-surge analysis（W=%ds）" % W)
    print("输入行 %d；M2 快照 %d；解析失败 %d；设备 %d" % (total, m2, parse_err, len(series)))
    if has_cold_field == 0:
        print("⚠ 转储里**没有 m2ColdCleared 字段**——多半是部署的 jar 未重打包重传；问题四无法用该标记，"
              "且逐设备 R 可能也未生效。建议 mvn clean package + syn-upload-m1.sh --jar-only 后重跑六月段。")
    else:
        print("m2ColdCleared 字段present：%d 行（jar 已含该字段）" % has_cold_field)
    print("=" * 76)

    def detect_recovery(pts):
        """取最大事件时间空档（>min_gap）作为停机；返回 (recovery_t, pre_outage_pts, post_pts)。"""
        if len(pts) < 2:
            return (None, [], [])
        best_i, best_gap = -1, 0
        for i in range(1, len(pts)):
            g = pts[i][0] - pts[i - 1][0]
            if g > best_gap:
                best_gap, best_i = g, i
        if best_gap >= min_gap:
            rec = pts[best_i][0]
            return (rec, pts[:best_i], pts[best_i:])
        # 无明显空档 → 用兜底恢复时刻
        if fallback_recovery:
            pre = [p for p in pts if p[0] < fallback_recovery]
            post = [p for p in pts if p[0] >= fallback_recovery]
            return (fallback_recovery, pre, post)
        return (None, pts, [])

    stats = {}
    for dev in sorted(series):
        rec, pre, post = detect_recovery(series[dev])
        if rec is None or not post:
            # 没侦测到停机空档 → 无法区分停机前/后，基线不可信，一律 n/a（不拿恢复段数据充当基线）
            stats[dev] = dict(recovery=rec, baseline=None, peak_r=None, peak_t=None,
                              transient=None, refill=None, first_cold=None, n_cold=0,
                              note="无停机空档/无停机前基线（该设备恢复较晚，转储未含其停机前段）")
            continue
        base = mean([r for (t, r, n, o, c) in pre if n > 0]) if pre else None
        win = [(t, r, n, o, c) for (t, r, n, o, c) in post if t <= rec + peak_win]
        peak_t, peak_r = max(((t, r) for (t, r, n, o, c) in win), key=lambda x: x[1])
        # 浪涌衰减：率首次回落到 ≤ max(基线×factor, 绝对下限)
        thr = max((base or 0) * args.return_factor, args.return_abs_floor)
        trans_end = next((t for (t, r, n, o, c) in win if t > rec and r <= thr), None)
        transient = (trans_end - rec) if trans_end else None
        # 物理重填：点数首次达到常态窗口点数（用恢复后点数的 90 分位近似常态满窗）
        full = sorted(n for (t, r, n, o, c) in post if n > 0)
        target = full[int(0.9 * (len(full) - 1))] * 0.9 if full else 0
        refill_t = next((t for (t, r, n, o, c) in post if n >= target), None)
        refill = (refill_t - rec) if refill_t else None
        # 冷启动清空：恢复窗附近首个 m2ColdCleared
        colds = [t for (t, r, n, o, c) in series[dev] if c and t >= rec - W]
        stats[dev] = dict(recovery=rec, baseline=base, peak_r=peak_r, peak_t=peak_t,
                          transient=transient, refill=refill,
                          first_cold=min(colds) if colds else None, n_cold=len(colds), note="")

    # ---- 问一 + 问二 ----
    print("\n【问一+问二】恢复时刻 / 常态基线均值 / 峰值 / 倍数 / 浪涌衰减 / 窗口重填（对照 W=%ds）" % W)
    print("  dev  recovery@          baseline%%   peak%%   ×倍    衰减s(/W)      重填s(/W)")
    for dev in sorted(stats):
        s = stats[dev]
        b = "n/a" if s["baseline"] is None else "%.4f" % (s["baseline"] * 100)
        pk = "n/a" if s["peak_r"] is None else "%.3f" % (s["peak_r"] * 100)
        if s["baseline"] and s["peak_r"] is not None and s["baseline"] > 1e-9:
            ratio = "%.0f" % (s["peak_r"] / s["baseline"])
        elif s["peak_r"] is not None:
            ratio = "inf"
        else:
            ratio = "n/a"
        tr = "n/a" if s["transient"] is None else "%d(%.2f)" % (s["transient"], s["transient"] / W)
        rf = "n/a" if s["refill"] is None else "%d(%.2f)" % (s["refill"], s["refill"] / W)
        print("  %-3s  %-16s  %8s  %6s  %5s  %-12s  %-12s"
              % (dev, iso(s["recovery"]), b, pk, ratio, tr, rf))

    # ---- 问三：同时性（用恢复时刻）----
    recs = [(dev, s["recovery"]) for dev, s in stats.items() if s["recovery"]]
    if recs:
        tmin = min(t for _, t in recs)
        tmax = max(t for _, t in recs)
        print("\n【问三】各设备恢复时刻的离散度（同时性 = 全局事件的实证）：")
        for dev, t in sorted(recs, key=lambda x: x[1]):
            print("   %-3s  恢复@ %s   (距最早 %+d s)" % (dev, iso(t), t - tmin))
        print("   恢复时刻跨度 max−min = %d s = %.2f×W；%s"
              % (tmax - tmin, (tmax - tmin) / W,
                 "≈ 同时 → 支持'全场同时 = 全局事件'" if (tmax - tmin) <= W
                 else "跨度大于一个 W，需看时间线判断是否分批恢复"))

    # ---- 问四 ----
    print("\n【问四】冷启动清空 vs 浪涌（先后）：")
    for dev in sorted(stats):
        s = stats[dev]
        if not s["recovery"]:
            continue
        if s["first_cold"] is None:
            print("   %-3s  恢复窗内无 m2ColdCleared 标记（见顶部字段告警；可改由窗口从空重填 = 事实冷启动 推断）" % dev)
        else:
            lead = (s["peak_t"] - s["first_cold"]) if s["peak_t"] else None
            print("   %-3s  首个清空@ %s；峰值@ %s；清空领先峰值 %s（共 %d 次）"
                  % (dev, iso(s["first_cold"]), iso(s["peak_t"]),
                     ("%+d s" % lead) if lead is not None else "n/a", s["n_cold"]))

    # ---- CSV ----
    if args.stats_csv:
        with open(args.stats_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["device", "recovery_ts", "recovery_iso", "baseline_rate", "peak_rate",
                        "ratio", "transient_sec", "transient_over_W", "refill_sec", "refill_over_W",
                        "first_cold_iso", "n_cold", "note"])
            for dev in sorted(stats):
                s = stats[dev]
                ratio = ""
                if s["baseline"] and s["peak_r"] is not None and s["baseline"] > 1e-9:
                    ratio = "%.1f" % (s["peak_r"] / s["baseline"])
                elif s["peak_r"] is not None:
                    ratio = "inf"
                w.writerow([dev, s["recovery"] or "n/a", iso(s["recovery"]),
                            ("%.6f" % s["baseline"]) if s["baseline"] is not None else "n/a",
                            ("%.6f" % s["peak_r"]) if s["peak_r"] is not None else "n/a", ratio,
                            s["transient"] if s["transient"] is not None else "n/a",
                            ("%.3f" % (s["transient"] / W)) if s["transient"] is not None else "n/a",
                            s["refill"] if s["refill"] is not None else "n/a",
                            ("%.3f" % (s["refill"] / W)) if s["refill"] is not None else "n/a",
                            iso(s["first_cold"]), s["n_cold"], s["note"]])
        print("\n[stats] → %s" % args.stats_csv)

    if args.timeline_csv:
        # 时间线：各设备恢复前 1h 至恢复后 peak_search，供画离群率时间线图
        with open(args.timeline_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["device", "windowEnd", "iso", "m2OutlierRate", "m2WindowPoints",
                        "m2McOccupancy", "m2ColdCleared"])
            for dev in sorted(series):
                rec = stats[dev]["recovery"]
                lo = (rec - 3600) if rec else 0
                hi = (rec + peak_win) if rec else 1 << 62
                for (t, r, n, o, c) in series[dev]:
                    if lo <= t <= hi:
                        w.writerow([dev, t, iso(t), "%.6f" % r, n, "%.4f" % o, int(c)])
        print("[timeline] → %s（各设备恢复前1h ~ 恢复后%.0fh）" % (args.timeline_csv, args.peak_search_hours))


if __name__ == "__main__":
    main()
