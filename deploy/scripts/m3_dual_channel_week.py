#!/usr/bin/env python3
# ============================================================================
# m3_dual_channel_week.py
# 三月二十二日至二十七日事件的双通道响应对比（设计会话 2026-10-02 分析任务书）。
# Dual-channel response to the 2022-03-22..27 fleet event (design-session analysis task, 2026-10-02).
#
# 逐设备、逐小时计算两个检测通道的响应，并与逐日气体剖面并排：
#   1. 点异常通道（来自 synergia-monitoring 转储的 M2 快照）：离群率、微簇占比、邻居数中位数；
#   2. 上下文通道（来自 synergia-scores 转储中的 m3_context 记录）：超阈比例、分数中位数；
#   3. 气体通道的逐日中位偏移与宽度倍数（来自 daily_channel_profile.csv；宽度倍数 = 当日 P10–P90 宽度
#      ÷ 参照期宽度）。
# 然后按任务书逐条核对五条预言，每条给出数字与判定口径。
# Per device and hour: point-channel outlier rate, micro-cluster occupancy and median neighbour count;
# context-channel above-threshold fraction and median score; daily gas shift and width multiple.
# The five predictions are then checked with explicit criteria.
#
# 判定口径（写在报告里，设计会话可以改）：
#   基线期默认 03-19 至 03-21（事件前）；「偏高」指高于基线均值的 2 倍；上下文通道的参照取
#   max(基线均值, 1%)，避免基线为零时比值无意义。
# Criteria: baseline 03-19..03-21; "elevated" means above twice the baseline mean (context channel:
# twice max(baseline, 1%)).
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac 或任何有 Python 3.9+、numpy、matplotlib 的机器，仓库根目录。
# 2. 调用命令：
#      python3 deploy/scripts/m3_dual_channel_week.py \
#          --monitoring docs/m2_monitoring_marapr-m3.jsonl \
#          --scores docs/m3_march/m3_scores.jsonl \
#          --profile docs/m3_march/daily_channel_profile.csv \
#          --out-dir docs/reports/m3_march_dual_channel_week
#    --monitoring 可省略，此时只做上下文通道与气体剖面两部分，点异常通道各项标为「缺少监测转储」。
#    可选：--start 2022-03-19、--end 2022-04-01（不含）、--base-start 2022-03-19、--base-end 2022-03-22（不含）、
#    --devices D,E（单独出图的设备）。
# 3. 前置条件：--profile 的参照期必须是 03-17 至 03-19（即三月重跑那一份），这样宽度倍数才与任务书中
#    「D 六点一三倍、E 五点二九倍」同一口径；--scores 必须是在线段覆盖 03-19 至 03-31 的那次运行（三月重跑）。
# 4. 期望产出：--out-dir 下 dual_channel_hourly.csv（逐设备逐小时）、dual_channel_check.md（五条预言的数字
#    与判定）、dual_fleet.png、dual_D.png、dual_E.png（每幅五个面板）。
# 5. 失败兜底：输入文件不存在时退出码 2；时段内没有任何记录时退出码 3。
# ============================================================================
import argparse
import csv
import json
import os
import statistics
import sys
from datetime import datetime, timedelta, timezone

import numpy as np

SURFACE, INK, INK2, GRID, BLUE = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df", "#2a78d6"
UTC = timezone.utc


def day_of(ts):
    return datetime.fromtimestamp(ts, UTC).strftime("%m-%d")


def load_monitoring(path, t0, t1):
    """M2 快照（windowEnd > 0）按 (设备, 小时) 聚合 / aggregate M2 snapshots by (device, hour)."""
    agg = {}
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            try:
                o = json.loads(line)
            except ValueError:
                continue
            we = int(o.get("windowEnd", 0) or 0)
            if we <= 0 or not (t0 <= we < t1) or not o.get("device"):
                continue
            a = agg.setdefault((o["device"], we // 3600 * 3600), ([], [], []))
            a[0].append(float(o.get("m2OutlierRate", 0.0) or 0.0))
            a[1].append(float(o.get("m2McOccupancy", 0.0) or 0.0))
            a[2].append(float(o.get("m2NeighborCountP50", 0.0) or 0.0))
    return {k: {"outlier_rate": statistics.mean(v[0]), "mc_occupancy": statistics.mean(v[1]),
                "neighbor_p50": statistics.median(v[2])} for k, v in agg.items()}


def load_scores(path, t0, t1):
    """上下文通道记录按 (设备, 小时) 聚合 / aggregate context-channel records by (device, hour)."""
    agg = {}
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            try:
                o = json.loads(line)
            except ValueError:
                continue
            we = int(o.get("windowEnd", 0) or 0)
            if o.get("channel") != "m3_context" or not (t0 <= we < t1):
                continue
            a = agg.setdefault((o["device"], we // 3600 * 3600), ([], []))
            a[0].append(1.0 if o.get("aboveThreshold") else 0.0)
            a[1].append(float(o["mainScore"]))
    return {k: {"above_frac": statistics.mean(v[0]), "score_median": statistics.median(v[1])}
            for k, v in agg.items()}


def load_profile(path, channel="Gas"):
    out = {}
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if r["channel"] != channel:
                continue
            w = float(r["ref_p10_p90_width"])
            out[(r["device"], r["day"])] = {"gas_shift": float(r["shift_in_ref_widths"]),
                                            "gas_width_x": (float(r["p90"]) - float(r["p10"])) / w if w else None}
    return out


def mean_or_none(xs):
    xs = [x for x in xs if x is not None]
    return statistics.mean(xs) if xs else None


def spearman(x, y):
    pairs = [(a, b) for a, b in zip(x, y) if a is not None and b is not None]
    if len(pairs) < 4:
        return None
    rx = np.argsort(np.argsort([p[0] for p in pairs]))
    ry = np.argsort(np.argsort([p[1] for p in pairs]))
    return float(np.corrcoef(rx, ry)[0, 1])


def fmt(v, spec="%.4f"):
    return "-" if v is None else spec % v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--monitoring")
    ap.add_argument("--scores", required=True)
    ap.add_argument("--profile", required=True)
    ap.add_argument("--start", default="2022-03-19")
    ap.add_argument("--end", default="2022-04-01")
    ap.add_argument("--base-start", default="2022-03-19")
    ap.add_argument("--base-end", default="2022-03-22")
    ap.add_argument("--devices", default="D,E")
    ap.add_argument("--out-dir", required=True)
    a = ap.parse_args()
    for p in [a.scores, a.profile] + ([a.monitoring] if a.monitoring else []):
        if not os.path.isfile(p):
            print("ERROR: 找不到输入文件 %s" % p, file=sys.stderr)
            return 2
    os.makedirs(a.out_dir, exist_ok=True)
    ts = lambda s: int(datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=UTC).timestamp())
    t0, t1, b0, b1 = ts(a.start), ts(a.end), ts(a.base_start), ts(a.base_end)

    mon = load_monitoring(a.monitoring, t0, t1) if a.monitoring else {}
    sco = load_scores(a.scores, t0, t1)
    prof = load_profile(a.profile)
    if not mon and not sco:
        print("ERROR: 时段内没有任何记录", file=sys.stderr)
        return 3
    devices = sorted({k[0] for k in list(mon) + list(sco)})
    hours = list(range(t0, t1, 3600))
    days = [day_of(t) for t in range(t0, t1, 86400)]
    base_days = [day_of(t) for t in range(b0, b1, 86400)]
    FIELDS = ("outlier_rate", "mc_occupancy", "neighbor_p50", "above_frac", "score_median")

    def hv(dev, h, f):
        src = mon if f in ("outlier_rate", "mc_occupancy", "neighbor_p50") else sco
        return src.get((dev, h), {}).get(f)

    # 逐小时表：逐设备一行，另加机队中位（各设备该小时值的中位数）。
    # Hourly table, one row per device plus the fleet median across devices.
    series = {}
    for dev in devices:
        for f in FIELDS:
            series[(dev, f)] = [hv(dev, h, f) for h in hours]
    for f in FIELDS:
        col = []
        for i in range(len(hours)):
            vals = [series[(d, f)][i] for d in devices if series[(d, f)][i] is not None]
            col.append(statistics.median(vals) if vals else None)
        series[("fleet", f)] = col
    for dev in devices:
        for f in ("gas_shift", "gas_width_x"):
            series[(dev, f)] = [prof.get((dev, d), {}).get(f) for d in days]
    for f in ("gas_shift", "gas_width_x"):
        series[("fleet", f)] = [statistics.median(v) if v else None for v in
                                ([series[(d, f)][i] for d in devices if series[(d, f)][i] is not None]
                                 for i in range(len(days)))]

    with open(os.path.join(a.out_dir, "dual_channel_hourly.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["device", "hour_utc", "day"] + list(FIELDS) + ["gas_shift", "gas_width_x"])
        for dev in devices + ["fleet"]:
            for i, h in enumerate(hours):
                di = (h - t0) // 86400
                w.writerow([dev, datetime.fromtimestamp(h, UTC).strftime("%Y-%m-%dT%H:00Z"), days[di]]
                           + [fmt(series[(dev, f)][i], "%.6g") for f in FIELDS]
                           + [fmt(series[(dev, f)][di], "%.4g") for f in ("gas_shift", "gas_width_x")])

    def daily(dev, f, day):
        i0 = days.index(day) * 24
        return mean_or_none(series[(dev, f)][i0:i0 + 24])

    def base(dev, f):
        return mean_or_none([daily(dev, f, d) for d in base_days])

    has_mon = bool(mon)
    who = ["fleet"] + devices
    L = ["# 三月二十二日至二十七日事件的双通道响应：五条预言的核对", "",
         "- 时段：%s 至 %s（不含），逐小时；基线期 %s 至 %s（不含）。" % (a.start, a.end, a.base_start, a.base_end),
         "- 「偏高」指高于基线均值的 2 倍；上下文通道的参照取 max(基线均值, 1%%)。",
         "- 宽度倍数 = 当日气体 P10–P90 宽度 ÷ 参照期宽度（参照期取 %s 所用的那一段）。" % os.path.basename(a.profile),
         "- 点异常通道数据：%s。" % ("已提供" if has_mon else "缺少监测转储，相关预言无法核对"), ""]

    # 逐日总表 / daily overview
    L += ["## 逐日总表（机队中位）", "",
          "| 日期 | 离群率 | 微簇占比 | 邻居数中位 | 超阈比例 | 分数中位 | 气体中位偏移 | 气体宽度倍数 |",
          "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for d in days:
        L.append("| %s | %s | %s | %s | %s | %s | %s | %s |" % (
            d, fmt(daily("fleet", "outlier_rate", d)), fmt(daily("fleet", "mc_occupancy", d)),
            fmt(daily("fleet", "neighbor_p50", d), "%.1f"), fmt(daily("fleet", "above_frac", d)),
            fmt(daily("fleet", "score_median", d), "%.3f"),
            fmt(series[("fleet", "gas_shift")][days.index(d)], "%.2f"),
            fmt(series[("fleet", "gas_width_x")][days.index(d)], "%.2f")))
    L.append("")

    ev = [d for d in ("03-23", "03-24", "03-25", "03-26") if d in days]

    # 预言一 / prediction 1
    L += ["## 预言一：点异常通道离群率在 23 至 26 日持续偏高，且逐日跟随宽度倍数（23、24 日最高）", ""]
    if has_mon:
        L += ["| 对象 | 基线 | 23–26 日逐日 | 四天都偏高 | 与宽度倍数的秩相关 | 离群率最高的两天 |",
              "| --- | --- | --- | --- | --- | --- |"]
        for dv in who:
            bl = base(dv, "outlier_rate")
            dd = [daily(dv, "outlier_rate", d) for d in ev]
            allhi = bl is not None and all(x is not None and x > 2 * bl for x in dd)
            drange = [daily(dv, "outlier_rate", d) for d in days]
            rho = spearman(drange, series[(dv, "gas_width_x")])
            top = sorted([(x, d) for x, d in zip(drange, days) if x is not None], reverse=True)[:2]
            L.append("| %s | %s | %s | %s | %s | %s |" % (
                dv, fmt(bl), " / ".join(fmt(x) for x in dd), "是" if allhi else "否",
                fmt(rho, "%.2f"), "、".join(d for _, d in top)))
        L.append("")
    else:
        L += ["缺少监测转储，无法核对。", ""]

    # 预言二 / prediction 2
    L += ["## 预言二：同期微簇占比下降", ""]
    if has_mon:
        L += ["| 对象 | 基线 | 23–26 日逐日 | 四天都低于基线 |", "| --- | --- | --- | --- |"]
        for dv in who:
            bl = base(dv, "mc_occupancy")
            dd = [daily(dv, "mc_occupancy", d) for d in ev]
            L.append("| %s | %s | %s | %s |" % (dv, fmt(bl), " / ".join(fmt(x) for x in dd),
                                                "是" if bl is not None and all(x is not None and x < bl for x in dd) else "否"))
        L.append("")
    else:
        L += ["缺少监测转储，无法核对。", ""]

    # 预言三 / prediction 3：恢复时刻 = 03-26 起第一个之后连续 6 小时都不高于基线 P90 的小时。
    L += ["## 预言三：宽度回落后点异常通道在一个窗长（一小时）内回到基线", "",
          "恢复时刻定义为 03-26 起第一个「其后连续 6 小时离群率都不高于基线期逐小时 P90」的小时。宽度倍数只有"
          "逐日分辨率，任务书称 27 日起回到一倍左右，因此这里给出恢复时刻与 03-27 00:00 的差，判定需结合这一分辨率。", ""]
    if has_mon:
        L += ["| 对象 | 基线逐小时 P90 | 恢复时刻（UTC） | 相对 03-27 00:00 |", "| --- | --- | --- | --- |"]
        ref = ts("2022-03-27") if "03-27" in days else None
        for dv in who:
            bh = [x for x, h in zip(series[(dv, "outlier_rate")], hours) if b0 <= h < b1 and x is not None]
            p90 = float(np.percentile(bh, 90)) if bh else None
            rec, i26 = None, days.index("03-26") * 24 if "03-26" in days else None
            if p90 is not None and i26 is not None:
                s = series[(dv, "outlier_rate")]
                for i in range(i26, len(hours) - 6):
                    if all(x is not None and x <= p90 for x in s[i:i + 6]):
                        rec = hours[i]
                        break
            if rec is not None and rec == hours[i26]:
                L.append("| %s | %s | 26 日起未偏高 | - |" % (dv, fmt(p90)))
                continue
            L.append("| %s | %s | %s | %s |" % (
                dv, fmt(p90), datetime.fromtimestamp(rec, UTC).strftime("%m-%d %H:00") if rec else "未恢复",
                ("%+.0f 小时" % ((rec - ref) / 3600)) if rec and ref else "-"))
        L.append("")
    else:
        L += ["缺少监测转储，无法核对。", ""]

    # 预言四 / prediction 4
    cev = [d for d in ("03-22", "03-23", "03-24", "03-25", "03-26", "03-27") if d in days]
    L += ["## 预言四：上下文通道持续偏高，到 28 日分布回到训练区间附近", "",
          "| 对象 | 基线 | 22–27 日逐日超阈比例 | 偏高的天数 | 25 日起第一个不偏高的日子 |", "| --- | --- | --- | --- | --- |"]
    for dv in who:
        bl = base(dv, "above_frac")
        refv = max(bl if bl is not None else 0.0, 0.01)
        dd = [daily(dv, "above_frac", d) for d in cev]
        nhi = sum(1 for x in dd if x is not None and x > 2 * refv)
        back = next((d for d in days[days.index("03-25"):] if (daily(dv, "above_frac", d) or 0) <= 2 * refv), None) \
            if "03-25" in days else None
        L.append("| %s | %s | %s | %d / %d | %s |" % (dv, fmt(bl), " / ".join(fmt(x, "%.3f") for x in dd),
                                                     nhi, len(cev), back or "无"))
    L.append("")

    # 预言五 / prediction 5
    L += ["## 预言五：两个通道都以 D、E 两台最严重", ""]
    rows5 = []
    for dv in devices:
        pt = None
        if has_mon and base(dv, "outlier_rate") is not None:
            pt = mean_or_none([daily(dv, "outlier_rate", d) for d in ev])
            pt = None if pt is None else pt - base(dv, "outlier_rate")
        cx = mean_or_none([daily(dv, "above_frac", d) for d in cev])
        cb = base(dv, "above_frac")
        cx = None if cx is None else cx - (cb or 0.0)
        rows5.append((dv, pt, cx))
    rank = lambda i: [r[0] for r in sorted([r for r in rows5 if r[i] is not None], key=lambda r: -r[i])]
    rp, rc = rank(1), rank(2)
    L += ["| 设备 | 点异常通道：23–26 日离群率减基线 | 名次 | 上下文通道：22–27 日超阈比例减基线 | 名次 |",
          "| --- | --- | --- | --- | --- |"]
    for dv, pt, cx in rows5:
        L.append("| %s | %s | %s | %s | %s |" % (dv, fmt(pt), rp.index(dv) + 1 if dv in rp else "-",
                                                fmt(cx, "%.3f"), rc.index(dv) + 1 if dv in rc else "-"))
    L += ["", "点异常通道前两名：%s；上下文通道前两名：%s。" % ("、".join(rp[:2]) or "无数据", "、".join(rc[:2])), ""]

    text = "\n".join(L) + "\n"
    open(os.path.join(a.out_dir, "dual_channel_check.md"), "w", encoding="utf-8").write(text)
    print(text)
    figures(a, series, hours, days, t0, [x for x in a.devices.split(",") if x in devices], has_mon)
    return 0


def figures(a, series, hours, days, t0, devs, has_mon):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.size": 9, "axes.edgecolor": GRID, "axes.labelcolor": INK2,
                         "xtick.color": INK2, "ytick.color": INK2, "axes.titlecolor": INK,
                         "figure.facecolor": SURFACE, "axes.facecolor": SURFACE})
    xh = [datetime.fromtimestamp(h, UTC) for h in hours]
    xd = [datetime.fromtimestamp(t0 + i * 86400, UTC) for i in range(len(days) + 1)]
    ev0, ev1 = datetime(2022, 3, 22, tzinfo=UTC), datetime(2022, 3, 28, tzinfo=UTC)
    # 每个量一个面板、共用横轴；不用双纵轴（离群率与微簇占比、中位偏移与宽度倍数各占一个面板）。
    # One panel per quantity on a shared time axis, never two y-scales on one panel.
    panels = [("outlier_rate", "Point: outlier rate", "h"), ("mc_occupancy", "Point: micro-cluster\noccupancy", "h"),
              ("above_frac", "Context: above-\nthreshold fraction", "h"),
              ("gas_shift", "Gas: median shift\n(widths)", "d"), ("gas_width_x", "Gas: width\nmultiple", "d")]
    for who in ["fleet"] + devs:
        fig, axes = plt.subplots(len(panels), 1, figsize=(11, 10), sharex=True)
        for ax, (f, lab, res) in zip(axes, panels):
            ax.axvspan(ev0, ev1, color=GRID, alpha=0.6, lw=0)
            ax.grid(axis="y", color=GRID, lw=0.6)
            ax.set_axisbelow(True)
            for s in ("top", "right"):
                ax.spines[s].set_visible(False)
            ys = [np.nan if v is None else v for v in series[(who, f)]]
            if res == "h":
                if f in ("outlier_rate", "mc_occupancy") and not has_mon:
                    ax.text(0.5, 0.5, "no monitoring dump", transform=ax.transAxes, ha="center", color=INK2)
                else:
                    ax.plot(xh, ys, color=BLUE, lw=1.2)
            else:
                ax.step(xd, ys + [ys[-1] if ys else np.nan], where="post", color=BLUE, lw=2)
                ax.axhline(1.0 if f == "gas_width_x" else 0.0, color=INK2, lw=0.8, ls="--")
            ax.set_ylabel(lab, rotation=0, ha="right", va="center")
        axes[0].set_title("%s: hourly response, 2022-03-19 to 03-31 UTC (grey = 03-22 to 03-27)"
                          % ("Fleet median" if who == "fleet" else "Device " + who), loc="left")
        axes[-1].xaxis.set_major_locator(mdates.DayLocator())
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
        fig.tight_layout()
        fig.savefig(os.path.join(a.out_dir, "dual_%s.png" % who), dpi=150)
        plt.close(fig)


if __name__ == "__main__":
    sys.exit(main())
