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
#    设计会话 2026-10-03 回复后增加的输入（都可省略，省略时对应小节注明缺什么）：
#      --hourly-gas docs/m3_march/hourly_gas_profile.csv   逐小时气体宽度（daily_channel_profile.py --hourly-out），
#                                                         预言三用小时级的宽度回落时刻去对点通道的回落时刻；
#      --freeze-ref docs/m2_replay_verify.csv             三月重跑的逐台标准化冻结时刻；
#      --freeze-new docs/m2_replay_verify_marapr.csv      本次运行的逐台冻结时刻；
#      --verify-log docs/m3_marapr/replay_verify.txt      本次重放核验的终端输出（读五条断言）；
#      --metrics docs/m3_marapr/m2_metrics_final.txt      本次运行排空后的计数器（读点通道迟到丢弃 m2_gate_late_drop）。
#    后四项用来核对「点通道数字取自 --monitoring 所给的运行、与三月重跑等价」的两个条件。
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
import re
import statistics
import sys
from datetime import datetime, timedelta, timezone

import numpy as np

SURFACE, INK, INK2, GRID, BLUE = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df", "#2a78d6"
UTC = timezone.utc


def day_of(ts):
    return datetime.fromtimestamp(ts, UTC).strftime("%m-%d")


def load_monitoring(path, t0, t1):
    """M2 快照按 (设备, 小时) 聚合 / aggregate M2 snapshots by (device, hour).
    监测主题里还有 M1 快照（windowEnd 为 0）与上下文通道快照（windowEnd 非 0，但 m2WindowPoints 为 0），
    只认 m2WindowPoints > 0 的点通道快照，否则上下文通道的零值会把离群率与微簇占比拉低。
    Keep only point-channel snapshots (m2WindowPoints > 0); context-channel snapshots also carry a windowEnd."""
    agg = {}
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            try:
                o = json.loads(line)
            except ValueError:
                continue
            we = int(o.get("windowEnd", 0) or 0)
            if we <= 0 or int(o.get("m2WindowPoints", 0) or 0) <= 0 or not (t0 <= we < t1) or not o.get("device"):
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
    ap.add_argument("--hourly-gas")
    ap.add_argument("--freeze-ref")
    ap.add_argument("--freeze-new")
    ap.add_argument("--verify-log")
    ap.add_argument("--metrics")
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
         "- 点异常通道数据：%s。" % ("已提供" if has_mon else "缺少监测转储，相关预言无法核对"),
         "- 加注（2026-10-05 裁决第二节第 3 条）：三月重跑中 D、G 在事件时间 03-20 06:21 至 17:03 约 10.5 小时没有输出"
         "（点通道迟到丢弃），上下文通道基线期 03-19 至 03-21 内两台少了这一段。", ""]
    L += equivalence_section(a)

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

    # 预言三 / prediction 3：点通道恢复时刻对小时级气体宽度回落时刻。
    # 两个时刻同一定义：03-26 00:00 起第一个「其后连续 6 小时都不高于各自基线上限」的小时。
    L += ["## 预言三：宽度回落后点异常通道在一个窗长（一小时）内回到基线", "",
          "两个时刻用同一个定义：从 03-26 00:00 起，第一个「其后连续 6 小时都不高于基线上限」的小时。点通道的上限是"
          "基线期逐小时离群率的 P90；气体宽度的上限是参照期逐小时宽度倍数的 P90（逐小时表的 ref_p90_x 列）。"
          "两者都只有小时分辨率，因此差的绝对值不超过 1 小时判为成立。", ""]
    hg = load_hourly_gas(a.hourly_gas) if a.hourly_gas else {}
    if not has_mon or not hg:
        L += ["缺少%s，无法核对。" % "与".join(x for x, ok in (("监测转储", has_mon), ("逐小时气体宽度", bool(hg))) if not ok), ""]
    else:
        L += ["| 对象 | 点通道恢复时刻（UTC） | 气体宽度回落时刻（UTC） | 差（点减宽度） | 判定 |",
              "| --- | --- | --- | --- | --- |"]
        h26 = ts("2022-03-26")
        for dv in who:
            bh = [x for x, h in zip(series[(dv, "outlier_rate")], hours) if b0 <= h < b1 and x is not None]
            p90 = float(np.percentile(bh, 90)) if bh else None
            pt = first_settled(list(zip(hours, series[(dv, "outlier_rate")])), lambda x: p90 is not None and x <= p90, h26)
            gw = first_settled([(h, v[0]) for h, v in sorted(hg.get(dv, {}).items())],
                               None, h26, caps={h: v[1] for h, v in hg.get(dv, {}).items()})
            show = lambda r: "26 日零点前已回到基线" if r == "early" else (
                "未回到基线" if r is None else datetime.fromtimestamp(r, UTC).strftime("%m-%d %H:00"))
            if isinstance(pt, int) and isinstance(gw, int):
                diff = (pt - gw) / 3600
                verdict = "成立" if abs(diff) <= 1 else "不成立"
                L.append("| %s | %s | %s | %+.0f 小时 | %s |" % (dv, show(pt), show(gw), diff, verdict))
            else:
                L.append("| %s | %s | %s | - | 无法判定 |" % (dv, show(pt), show(gw)))
        L.append("")

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

    # 预言五 / prediction 5：按设计会话 2026-10-03 回复，只保留给点通道；上下文通道改为设备内检验。
    L += ["## 预言五：点异常通道以 D、E 两台最严重", "",
          "按设计会话 2026-10-03 的回复，跨设备排序只对点通道有意义：各台的半径按相近的离群率标定，灵敏度大致"
          "均匀。上下文通道各台的告警取决于各自冻结的模型与阈值，跨设备排序不是干净的检验，改为下一节的设备内检验。", ""]
    if has_mon:
        rows5 = []
        for dv in devices:
            bl = base(dv, "outlier_rate")
            pt = mean_or_none([daily(dv, "outlier_rate", d) for d in ev])
            rows5.append((dv, None if pt is None or bl is None else pt - bl))
        rp = [r[0] for r in sorted([r for r in rows5 if r[1] is not None], key=lambda r: -r[1])]
        L += ["| 设备 | 23–26 日离群率减基线 | 名次 |", "| --- | --- | --- |"]
        for dv, pt in rows5:
            L.append("| %s | %s | %s |" % (dv, fmt(pt), rp.index(dv) + 1 if dv in rp else "-"))
        L += ["", "前两名：%s；判定：%s。" % ("、".join(rp[:2]),
                                        "成立" if set(rp[:2]) == {"D", "E"} else "不成立"), ""]
    else:
        L += ["缺少监测转储，无法核对。", ""]

    L += ["## 上下文通道的设备内检验：告警率是否随本台的气体偏移与宽度逐日起伏", "",
          "每台设备在 %s 至 %s 的逐日超阈比例，分别与本台逐日气体中位偏移、宽度倍数求秩相关；"
          "秩相关不低于 0.5 记为「跟随」（暂定口径，设计会话可改）。" % (days[0], days[-1]), "",
          "| 设备 | 与中位偏移的秩相关 | 与宽度倍数的秩相关 | 判定 |", "| --- | --- | --- | --- |"]
    for dv in devices:
        af = [daily(dv, "above_frac", d) for d in days]
        r1 = spearman(af, series[(dv, "gas_shift")])
        r2 = spearman(af, series[(dv, "gas_width_x")])
        ok = [r for r in (r1, r2) if r is not None]
        L.append("| %s | %s | %s | %s |" % (dv, fmt(r1, "%.2f"), fmt(r2, "%.2f"),
                                           "无数据" if not ok else ("跟随" if max(ok) >= 0.5 else "不跟随")))
    L.append("")

    text = "\n".join(L) + "\n"
    open(os.path.join(a.out_dir, "dual_channel_check.md"), "w", encoding="utf-8").write(text)
    print(text)
    figures(a, series, hours, days, t0, [x for x in a.devices.split(",") if x in devices], has_mon)
    return 0


def load_hourly_gas(path):
    """逐小时气体表 → {设备: {小时: (宽度倍数, 回落上限)}}，另加机队中位 / hourly gas table plus fleet median."""
    out = {}
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if r["channel"] != "Gas":
                continue
            h = int(datetime.strptime(r["hour_utc"], "%Y-%m-%dT%H:00Z").replace(tzinfo=UTC).timestamp())
            out.setdefault(r["device"], {})[h] = (float(r["width_x"]), float(r["ref_p90_x"]))
    hrs = sorted({h for v in out.values() for h in v})
    out["fleet"] = {h: (statistics.median(v[h][0] for v in out.values() if h in v),
                        statistics.median(v[h][1] for v in out.values() if h in v)) for h in hrs}
    return out


def first_settled(pairs, ok, start, caps=None, run=6):
    """从 start 起第一个「其后连续 run 小时都满足上限」的小时；若第一个小时就满足，返回 "early"。
    First hour from start after which `run` consecutive hours are within the cap; "early" if already so."""
    pairs = [(h, v) for h, v in pairs if h >= start]
    good = lambda h, v: v is not None and (v <= caps[h] if caps is not None else ok(v))
    for i in range(len(pairs) - run + 1):
        if all(good(h, v) for h, v in pairs[i:i + run]):
            return "early" if i == 0 else pairs[i][0]
    return None


def equivalence_section(a):
    """点通道等价性的两个条件（设计会话 2026-10-03 回复第一节）。
    Two conditions under which the point channel of this run equals the March rerun's."""
    L = ["## 点通道等价性的两个条件", "",
         "点通道的数字取自 --monitoring 所给的运行。点异常检测没有训练出来的参数，状态只由输入与事件时间决定，"
         "满足以下两个条件时，03-19 至 03-31 的输出与三月重跑逐位相同。", ""]
    if not (a.freeze_ref and a.freeze_new and a.verify_log and a.metrics):
        return L + ["缺少 --freeze-ref、--freeze-new、--verify-log 或 --metrics，未核对。", ""]
    st = {}
    for line in open(a.verify_log, encoding="utf-8", errors="replace"):
        m = re.match(r"\s*\[(断言[一二三四五])[^\]]*\]\s*(PASS|FAIL|SKIP)", line)
        if m:
            st[m.group(1)] = m.group(2)
    late = None
    for line in open(a.metrics, encoding="utf-8", errors="replace"):
        m = re.match(r"\s*m2_gate_late_drop\s+(\S+)", line)
        if m:
            late = m.group(1)
    names = ["断言一", "断言二", "断言三", "断言四", "断言五"]
    c1 = all(st.get(n) == "PASS" for n in names) and late is not None and late.replace(".0", "") == "0"
    L += ["条件一：五条完整性断言全部通过，且点通道迟到丢弃计数为零。", "",
          "| 断言一 | 断言二 | 断言三 | 断言四 | 断言五 | 迟到丢弃 | 结论 |", "| --- | --- | --- | --- | --- | --- | --- |",
          "| %s | %s | %s | %s | %s | %s | %s |" % tuple([st.get(n, "缺") for n in names] + [late or "缺",
                                                       "成立" if c1 else "不成立"]), ""]
    rd = lambda p: {r["device"]: r["freeze_ts"] for r in csv.DictReader(open(p, encoding="utf-8"))}
    old, new = rd(a.freeze_ref), rd(a.freeze_new)
    devs = sorted(set(old) | set(new))
    c2 = all(old.get(d) == new.get(d) for d in devs)
    L += ["条件二：逐台标准化冻结时刻与三月重跑逐一相同。", "",
          "| 设备 | 三月重跑 | 本次 | 相同 |", "| --- | --- | --- | --- |"]
    for d in devs:
        f = lambda v: datetime.fromtimestamp(int(v), UTC).strftime("%m-%d %H:%M:%S") if v else "缺"
        L.append("| %s | %s | %s | %s |" % (d, f(old.get(d)), f(new.get(d)), "是" if old.get(d) == new.get(d) else "否"))
    L += ["", "结论：%s" % ("两个条件都成立。点通道数字取自 --monitoring 所给的运行，与三月重跑的等价性由确定性保证，条件已核对。"
                          if c1 and c2 else "条件不全成立，点通道数字不能当作三月重跑的结果，须上报设计会话。"), ""]
    return L


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
