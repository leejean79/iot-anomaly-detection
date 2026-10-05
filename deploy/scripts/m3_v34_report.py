#!/usr/bin/env python3
# ============================================================================
# m3_v34_report.py
# V-M3-4 的两项报告（2026-10-02 裁决第四节第 3、4 条）：
#   其一，全程逐日告警率与逐日通道剖面并排；
#   其二，平稳日误报率：平稳日定义为五个检测通道的机队中位偏移绝对值都不超过 0.5 个标定期宽度的日子，
#         期望 0.1% 至 1%；另加「共模比例」：告警窗口中，同一小时内八台设备至少六台告警的占比。
# 持续数日、全机队同时的高告警率记为漂移事件，不计为误报（第四节第 4 条）。
# V-M3-4 per the ruling of 2026-10-02 section 4: daily alarm rate beside the daily channel profile, the
# false-alarm rate on stable days, and the common-mode share of alarm windows.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，numpy、matplotlib（eda/requirements.txt 已含）。
# 2. 调用命令：
#      python3 deploy/scripts/m3_v34_report.py --scores docs/m3_march/m3_scores.jsonl \
#          --profile docs/m3_march/daily_channel_profile.csv --start 2022-03-24 --end 2022-05-01 \
#          --out-dir docs/m3_march/v34
#    --profile 由 eda/daily_channel_profile.py 生成，其参照期须为阈值校准期（--ref-start 2022-03-17
#    --ref-end 2022-03-24），「标定期宽度」即该期间的 P10 至 P90 宽度。
#    可选：--stable-max 0.5（平稳日阈值）、--common-min 6（共模的设备数下限）、--threshold 2.22（只用于作图）。
# 3. 前置条件：两份输入文件存在；评分来自在线算子（channel 为 m3_context）。
# 4. 期望产出：--out-dir 下 v34_report.md（逐日表、平稳日清单、平稳日误报率表、共模比例）、
#    v34_daily.csv、v34_daily.png（逐日告警率与五个通道的机队中位偏移，上下对齐）、
#    v34_score_hist.png（平稳日主分分布，逐设备，阈值以虚线标出）。终端同时打印报告。
# 5. 失败兜底：评估区间内没有评分时退出码 2；剖面缺少某个通道的某一天时，该日不算平稳日，并在报告中列出。
# ============================================================================
import argparse
import collections
import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np

CHANNELS = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]
# 参照调色板（dataviz 技能 references/palette.md 的浅色值）/ reference palette, light mode
SURFACE, INK, INK2, GRID, BLUE = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df", "#2a78d6"


def day_of(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%m-%d")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scores", required=True)
    ap.add_argument("--profile", required=True)
    ap.add_argument("--start", default="2022-03-24")
    ap.add_argument("--end", default="2022-05-01")
    ap.add_argument("--stable-max", type=float, default=0.5)
    ap.add_argument("--common-min", type=int, default=6)
    ap.add_argument("--threshold", type=float, default=2.22)
    ap.add_argument("--out-dir", required=True)
    # 排除评分不可用的设备（2026-10-05 裁决第二节第 1 条：三月至四月运行按 A、B、C、E、F、H 六台口径出）。
    # 只排除评分；平稳日按全部设备的原始数据剖面判定，不受影响。
    ap.add_argument("--exclude-devices", default="")
    a = ap.parse_args()
    os.makedirs(a.out_dir, exist_ok=True)

    t0 = datetime.strptime(a.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    t1 = datetime.strptime(a.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    days = [(t0 + timedelta(days=i)).strftime("%m-%d") for i in range((t1 - t0).days)]
    lo, hi = int(t0.timestamp()), int(t1.timestamp())

    excluded = {x.strip() for x in a.exclude_devices.split(",") if x.strip()}
    recs = []
    for line in open(a.scores, encoding="utf-8"):
        r = json.loads(line)
        if r.get("channel", "m3_context") == "m3_context" and lo <= r["windowEnd"] < hi \
                and r["device"] not in excluded:
            recs.append(r)
    if not recs:
        print("ERROR: %s 至 %s 之间没有上下文通道评分。" % (a.start, a.end), file=sys.stderr)
        return 2
    devices = sorted({r["device"] for r in recs})

    # 逐日通道剖面：每个通道每天取各设备偏移的中位数（机队中位偏移）。
    shift = collections.defaultdict(list)
    for r in csv.DictReader(open(a.profile, encoding="utf-8")):
        shift[(r["channel"], r["day"])].append(float(r["shift_in_ref_widths"]))
    fleet = {(c, d): float(np.median(shift[(c, d)])) for c in CHANNELS for d in days if shift.get((c, d))}
    stable, missing = [], []
    for d in days:
        vals = [fleet.get((c, d)) for c in CHANNELS]
        if any(v is None for v in vals):
            missing.append(d)
        elif all(abs(v) <= a.stable_max for v in vals):
            stable.append(d)

    # 逐日告警率 / daily alarm rate
    n, al = collections.Counter(), collections.Counter()
    for r in recs:
        d = day_of(r["windowEnd"])
        for k in ((r["device"], d), ("机队", d)):
            n[k] += 1
            al[k] += bool(r["aboveThreshold"])
    rate = lambda k: (100.0 * al[k] / n[k]) if n[k] else None

    # 共模比例：告警窗口中，同一小时内告警设备数 ≥ common-min 的占比。
    hour_devs = collections.defaultdict(set)
    for r in recs:
        if r["aboveThreshold"]:
            hour_devs[r["windowEnd"] // 3600].add(r["device"])

    def common_share(rs):
        alarms = [r for r in rs if r["aboveThreshold"]]
        if not alarms:
            return None, 0
        cm = sum(1 for r in alarms if len(hour_devs[r["windowEnd"] // 3600]) >= a.common_min)
        return 100.0 * cm / len(alarms), len(alarms)

    st_set = set(stable)
    st_recs = [r for r in recs if day_of(r["windowEnd"]) in st_set]

    out = []
    p = out.append
    p("# V-M3-4 报告（%s 至 %s）\n" % (a.start, (t1 - timedelta(days=1)).strftime("%Y-%m-%d")))
    p("平稳日定义：五个检测通道的机队中位偏移（各设备当日中位数相对阈值校准期中位数的偏移，以校准期 P10–P90 "
      "宽度为单位，再取设备间中位数）绝对值都不超过 %.1f。共模：同一小时内至少 %d 台设备告警。\n"
      % (a.stable_max, a.common_min))
    if excluded:
        p("本报告排除了 %s 的评分（%d 台口径）；平稳日仍按全部设备的原始数据剖面判定。共模门槛 %d 台不随之缩小，"
          "在 %d 台口径下更严。\n" % ("、".join(sorted(excluded)), len(devices), a.common_min, len(devices)))
    p("## 一、平稳日误报率\n")
    p("平稳日共 %d 天：%s。" % (len(stable), "、".join(stable) or "无"))
    if missing:
        p("剖面缺少数据、不计为平稳日的日子：%s。" % "、".join(missing))
    p("\n| 设备 | 平稳日窗口数 | 超阈值 | 误报率 |")
    p("| --- | --- | --- | --- |")
    tot_n = tot_a = 0
    for dv in devices:
        rs = [r for r in st_recs if r["device"] == dv]
        k = sum(bool(r["aboveThreshold"]) for r in rs)
        tot_n += len(rs)
        tot_a += k
        p("| %s | %d | %d | %s |" % (dv, len(rs), k, ("%.2f%%" % (100.0 * k / len(rs))) if rs else "-"))
    p("| 机队 | %d | %d | %s |" % (tot_n, tot_a, ("%.2f%%" % (100.0 * tot_a / tot_n)) if tot_n else "-"))
    cs_all, na_all = common_share(recs)
    cs_st, na_st = common_share(st_recs)
    p("\n共模比例：全程告警窗口 %d 个，其中共模 %s；平稳日告警窗口 %d 个，其中共模 %s。期望：平稳日误报率在 "
      "0.1%% 至 1%% 之间。" % (na_all, ("%.1f%%" % cs_all) if cs_all is not None else "-",
                         na_st, ("%.1f%%" % cs_st) if cs_st is not None else "-"))

    p("\n## 二、逐日告警率与机队中位偏移\n")
    p("| 日期 | 平稳 | 机队告警率 | " + " | ".join(devices) + " | " + " | ".join(CHANNELS) + " |")
    p("| --- | --- | --- | " + " | ".join("---" for _ in devices) + " | " + " | ".join("---" for _ in CHANNELS) + " |")
    rows = []
    for d in days:
        fr = rate(("机队", d))
        cells = [("%.0f" % rate((dv, d))) if rate((dv, d)) is not None else "-" for dv in devices]
        ch = [("%+.2f" % fleet[(c, d)]) if (c, d) in fleet else "-" for c in CHANNELS]
        p("| %s | %s | %s | %s | %s |" % (d, "是" if d in st_set else "", ("%.1f%%" % fr) if fr is not None else "-",
                                         " | ".join(cells), " | ".join(ch)))
        row = {"day": d, "stable": d in st_set, "fleet_alarm_pct": fr}
        row.update({"alarm_pct_" + dv: rate((dv, d)) for dv in devices})
        row.update({"fleet_shift_" + c: fleet.get((c, d)) for c in CHANNELS})
        rows.append(row)
    p("\n各设备列为当日告警率（%）；通道列为机队中位偏移（单位：阈值校准期 P10–P90 宽度）。"
      "持续数日、全机队同时的高告警率按裁决记为漂移事件，不计为误报。")

    with open(os.path.join(a.out_dir, "v34_daily.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    figures(a, days, st_set, rows, st_recs, devices)
    p("\n图：`v34_daily.png`（逐日告警率与五个通道的机队中位偏移，上下对齐，灰底为平稳日）；"
      "`v34_score_hist.png`（平稳日主分分布，逐设备，虚线为阈值 %.2f）。" % a.threshold)
    text = "\n".join(out) + "\n"
    print(text)
    open(os.path.join(a.out_dir, "v34_report.md"), "w", encoding="utf-8").write(text)
    return 0


def figures(a, days, st_set, rows, st_recs, devices):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker  # noqa: F401
    plt.rcParams.update({"font.size": 9, "axes.edgecolor": GRID, "axes.labelcolor": INK2,
                         "xtick.color": INK2, "ytick.color": INK2, "axes.titlecolor": INK,
                         "figure.facecolor": SURFACE, "axes.facecolor": SURFACE})
    x = np.arange(len(days))

    # 图一：逐日告警率（柱）与五个通道的机队中位偏移（每个通道一个子图），共用横轴，不用双纵轴。
    fig, axes = plt.subplots(1 + len(CHANNELS), 1, figsize=(11, 10), sharex=True,
                             gridspec_kw={"height_ratios": [1.6] + [1] * len(CHANNELS)})
    for ax in axes:
        for i, d in enumerate(days):
            if d in st_set:
                ax.axvspan(i - 0.5, i + 0.5, color=GRID, alpha=0.6, lw=0)
        ax.grid(axis="y", color=GRID, lw=0.6)
        ax.set_axisbelow(True)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
    fr = [r["fleet_alarm_pct"] if r["fleet_alarm_pct"] is not None else 0 for r in rows]
    axes[0].bar(x, fr, width=0.7, color=BLUE)
    axes[0].set_ylabel("Fleet alarm rate (%)")
    axes[0].set_title("Daily alarm rate and fleet-median channel shift (grey = stable day)", loc="left")
    for ax, c in zip(axes[1:], CHANNELS):
        ys = [r["fleet_shift_" + c] for r in rows]
        ax.plot(x, [np.nan if y is None else y for y in ys], color=BLUE, lw=2, marker="o", ms=3)
        ax.axhline(a.stable_max, color=INK2, lw=0.8, ls="--")
        ax.axhline(-a.stable_max, color=INK2, lw=0.8, ls="--")
        ax.axhline(0, color=INK2, lw=0.6)
        ax.set_ylabel(c + "\n(widths)", rotation=0, ha="right", va="center")
    step = max(1, len(days) // 20)
    axes[-1].set_xticks(x[::step])
    axes[-1].set_xticklabels(days[::step], rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig(os.path.join(a.out_dir, "v34_daily.png"), dpi=150)
    plt.close(fig)

    # 图二：平稳日主分分布，逐设备小多图，阈值以虚线标出；横轴截在 [-3, 12]，超出部分计数写在图内。
    cols = 4
    nrow = int(np.ceil(len(devices) / cols))
    fig, axes = plt.subplots(nrow, cols, figsize=(12, 3 * nrow), sharex=True)
    axes = np.atleast_1d(axes).ravel()
    lo_x, hi_x = -3.0, 12.0
    bins = np.linspace(lo_x, hi_x, 61)
    for ax, dv in zip(axes, devices):
        v = np.array([r["mainScore"] for r in st_recs if r["device"] == dv])
        over = int((v > hi_x).sum()) if len(v) else 0
        if len(v):
            # 超出横轴范围的窗口不画进末柱（那会造成虚假的尖峰），只在标题里计数。
            ax.hist(np.clip(v[v <= hi_x], lo_x, hi_x), bins=bins, color=BLUE, edgecolor=SURFACE, linewidth=0.5)
        ax.axvline(a.threshold, color=INK, lw=1.2, ls="--")
        ax.set_yscale("log")
        ax.yaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
        ax.set_title("%s   n=%d, >%.0f: %d" % (dv, len(v), hi_x, over), loc="left")
        ax.grid(axis="y", color=GRID, lw=0.6)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
    for ax in axes[len(devices):]:
        ax.set_visible(False)
    fig.suptitle("Main score on stable days (dashed line = threshold %.2f; windows above %.0f are counted "
                 "in each title, not drawn)" % (a.threshold, hi_x), x=0.01, ha="left")
    fig.supxlabel("Main score z = (WMSE - median) / IQR")
    fig.tight_layout()
    fig.savefig(os.path.join(a.out_dir, "v34_score_hist.png"), dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    sys.exit(main())
