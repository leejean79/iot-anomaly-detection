#!/usr/bin/env python3
# ============================================================================
# m3_bh_analysis.py
# B、H 两台误报偏高的并行分析（2026-10-06 裁决第二节，不重放，只读已有转储）。
# Parallel analysis of the high false-alarm rates on B and H (ruling of 2026-10-06 section 2).
#
# 回答三个问题 / three questions：
#   1. 逐台：阈值校准期误差（加权均方误差）的中位数与四分位距、阈值对应的误差值（中位数 + 2.22 × 四分位距）、
#      平稳日误差中位数及其相对校准中位数的位置（以四分位距为单位）。据此区分：
#        「误差分布抬高」——平稳日误差中位数明显高于校准中位数，整体上移；
#        「阈值过紧」——平稳日误差中位数与校准中位数相近，但超阈比例仍高，说明校准期的离散度偏小或尾部偏重。
#   2. B：告警的逐日分布、逐小时（UTC）分布、主导通道分布（超阈窗口中逐通道误差最大的那个通道）。
#   3. H：告警是否集中在 04-04 恢复之后；恢复后第一批窗口是否跨越了停机缺口（窗口由连续收到的 60 轮拼成，
#      上下文通道不处理冷启动标记，跨缺口的窗口会把停机前后的轮缝在一起）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac 或任何有 Python 3.9+ 的机器，仓库根目录。
# 2. 调用命令：
#      python3 deploy/scripts/m3_bh_analysis.py --scores docs/m3_marapr/m3_scores.jsonl \
#          --tm-log docs/m3_marapr/m3_tm_log.txt --profile docs/m3_marapr/daily_channel_profile_ref0308.csv \
#          --out docs/reports/m3_bh_analysis.md
#    --profile 须用参照期 03-08 至 03-17 生成（2026-10-06 裁决第三节），平稳日据此判定。
#    可选：--start 2022-03-24、--end 2022-05-01（不含）、--recovery 2022-04-04（H 的恢复日）。
# 3. 前置条件：三月至四月运行的评分与训练日志已入库；剖面已按新参照期生成。
# 4. 期望产出：终端与 --out 处写出三节表格与初步判读。
# 5. 失败兜底：输入文件不存在时退出码 2；训练日志里找不到 B 或 H 的标定记录时退出码 3。
# ============================================================================
import argparse
import collections
import csv
import json
import os
import re
import statistics
import sys
from datetime import datetime, timezone

CHANNELS = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]
day = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%m-%d")
hour = lambda t: datetime.fromtimestamp(t, timezone.utc).hour


def stable_days(profile, lo, hi, limit=0.5):
    shift = collections.defaultdict(list)
    for r in csv.DictReader(open(profile, encoding="utf-8")):
        shift[(r["channel"], r["day"])].append(float(r["shift_in_ref_widths"]))
    days = sorted({k[1] for k in shift if lo <= k[1] < hi})
    out = []
    for d in days:
        meds = [statistics.median(shift[(c, d)]) for c in CHANNELS if shift.get((c, d))]
        if len(meds) == len(CHANNELS) and all(abs(m) <= limit for m in meds):
            out.append(d)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scores", required=True)
    ap.add_argument("--tm-log", required=True)
    ap.add_argument("--profile", required=True)
    ap.add_argument("--start", default="2022-03-24")
    ap.add_argument("--end", default="2022-05-01")
    ap.add_argument("--recovery", default="2022-04-04")
    ap.add_argument("--ref-label", default="03-08 至 03-17", help="剖面所用的参照期，只用于报告标注")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    for p in (a.scores, a.tm_log, a.profile):
        if not os.path.isfile(p):
            print("ERROR: 找不到 %s" % p, file=sys.stderr)
            return 2

    cal = {}
    for line in open(a.tm_log, encoding="utf-8", errors="replace"):
        m = re.search(r"Device ([A-H]) calibrated: median=([0-9.Ee-]+), IQR=([0-9.Ee-]+), threshold=([0-9.]+)", line)
        if m:
            cal[m.group(1)] = (float(m.group(2)), float(m.group(3)), float(m.group(4)))
    if "B" not in cal or "H" not in cal:
        print("ERROR: 训练日志里找不到 B 或 H 的标定记录", file=sys.stderr)
        return 3

    lo, hi = a.start[5:], a.end[5:]
    st = set(stable_days(a.profile, lo, hi))
    recs = [json.loads(l) for l in open(a.scores, encoding="utf-8")]
    recs = [r for r in recs if r.get("channel", "m3_context") == "m3_context" and lo <= day(r["windowEnd"]) < hi]

    L = ["# B、H 误报偏高的并行分析（不重放）", "",
         "- 数据：三月至四月运行（作业 d9c21609）的上下文通道评分与训练日志；时段 %s 至 %s（不含）。" % (a.start, a.end),
         "- 平稳日按参照期 %s 的剖面判定（2026-10-06 裁决第三节规定为 03-08 至 03-17），共 %d 天：%s。"
         % (a.ref_label, len(st), "、".join(sorted(st)) or "无"),
         "- D、G 自 03-25 起无输出（迟到丢弃事故），表中只作参考。", ""]

    # 一、逐台阈值与误差 / per-device threshold and error
    L += ["## 一、逐台：阈值校准期误差、阈值与平稳日误差", "",
          "| 设备 | 校准中位数 | 校准四分位距 | 阈值对应误差 | 平稳日窗口 | 平稳日误差中位数 | 相对校准中位数（四分位距） | 平稳日超阈比例 | 平稳日误差 P90 |",
          "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for dv in sorted(cal):
        med, iqr, thr = cal[dv]
        ws = sorted(r["wmse"] for r in recs if r["device"] == dv and day(r["windowEnd"]) in st)
        al = sum(1 for r in recs if r["device"] == dv and day(r["windowEnd"]) in st and r["aboveThreshold"])
        if ws:
            sm = statistics.median(ws)
            p90 = ws[int(0.9 * (len(ws) - 1))]
            L.append("| %s | %.4f | %.4f | %.4f | %d | %.4f | %+.2f | %.2f%% | %.4f |" % (
                dv, med, iqr, med + thr * iqr, len(ws), sm, (sm - med) / iqr, 100.0 * al / len(ws), p90))
        else:
            L.append("| %s | %.4f | %.4f | %.4f | 0 | - | - | - | - |" % (dv, med, iqr, med + thr * iqr))
    L += ["", "判读规则：「相对校准中位数」明显大于 0（例如超过 0.5 个四分位距）说明平稳日的误差整体上移，属「误差分布抬高」；"
          "接近 0 而超阈比例仍高，说明尾部偏重或校准期离散度偏小，属「阈值过紧」。", ""]

    # 二、B 的告警分布 / B's alarm distribution
    b = [r for r in recs if r["device"] == "B"]
    ba = [r for r in b if r["aboveThreshold"]]
    L += ["## 二、B 的告警分布", "", "全程窗口 %d 个，超阈 %d 个（%.2f%%）。" % (len(b), len(ba), 100.0 * len(ba) / max(1, len(b))), "",
          "**逐日**（超阈 / 窗口；「平稳」标记平稳日）：", "", "| 日期 | 平稳 | 超阈 | 窗口 | 比例 |", "| --- | --- | --- | --- | --- |"]
    bd, bn = collections.Counter(day(r["windowEnd"]) for r in ba), collections.Counter(day(r["windowEnd"]) for r in b)
    for d in sorted(bn):
        L.append("| %s | %s | %d | %d | %.1f%% |" % (d, "是" if d in st else "", bd[d], bn[d], 100.0 * bd[d] / bn[d]))
    bh, bhn = collections.Counter(hour(r["windowEnd"]) for r in ba), collections.Counter(hour(r["windowEnd"]) for r in b)
    L += ["", "**逐小时（UTC）超阈比例**：", "", "| 小时 | " + " | ".join("%02d" % h for h in range(24)) + " |",
          "| --- | " + " | ".join("---" for _ in range(24)) + " |",
          "| 比例 | " + " | ".join("%.0f%%" % (100.0 * bh[h] / bhn[h]) if bhn[h] else "-" for h in range(24)) + " |", ""]
    dom = collections.Counter(CHANNELS[max(range(5), key=lambda i: r["perChannelErrors"][i])] for r in ba)
    L += ["**主导通道**（超阈窗口中逐通道误差最大的通道）：" + "；".join(
        "%s %d（%.0f%%）" % (c, dom[c], 100.0 * dom[c] / max(1, len(ba))) for c in CHANNELS), ""]

    # 三、H：恢复前后 / H before and after recovery
    h = sorted((r for r in recs if r["device"] == "H"), key=lambda r: r["windowEnd"])
    rec = a.recovery[5:]
    pre = [r for r in h if day(r["windowEnd"]) < "04-01"]
    post = [r for r in h if day(r["windowEnd"]) >= rec]
    rate = lambda rs: (100.0 * sum(r["aboveThreshold"] for r in rs) / len(rs)) if rs else float("nan")
    pre_st = [r for r in pre if day(r["windowEnd"]) in st]
    post_st = [r for r in post if day(r["windowEnd"]) in st]
    L += ["## 三、H：告警是否集中在 %s 恢复之后" % a.recovery, "",
          "| 时段 | 窗口 | 超阈比例 | 其中平稳日窗口 | 平稳日超阈比例 |", "| --- | --- | --- | --- | --- |",
          "| 停机前（至 03-31） | %d | %.2f%% | %d | %.2f%% |" % (len(pre), rate(pre), len(pre_st), rate(pre_st)),
          "| 恢复后（%s 起） | %d | %.2f%% | %d | %.2f%% |" % (rec, len(post), rate(post), len(post_st), rate(post_st)), ""]
    gaps = [(x, y) for x, y in zip(h, h[1:]) if y["windowEnd"] - x["windowEnd"] > 3600]
    L.append("**跨缺口的窗口**（相邻两条评分的窗口末相差超过 1 小时，说明后一个窗口的 60 轮跨越了缺口）：")
    L.append("")
    for x, y in gaps:
        L.append("- %s → %s，间隔 %.1f 小时；缺口后第一个窗口主分 %.2f，%s。" % (
            datetime.fromtimestamp(x["windowEnd"], timezone.utc).strftime("%m-%d %H:%M"),
            datetime.fromtimestamp(y["windowEnd"], timezone.utc).strftime("%m-%d %H:%M"),
            (y["windowEnd"] - x["windowEnd"]) / 3600.0, y["mainScore"], "超阈" if y["aboveThreshold"] else "未超阈"))
    hd, hn = collections.Counter(day(r["windowEnd"]) for r in h if r["aboveThreshold"]), collections.Counter(day(r["windowEnd"]) for r in h)
    L += ["", "**H 逐日超阈比例**：" + "；".join("%s %.0f%%" % (d, 100.0 * hd[d] / hn[d]) for d in sorted(hn)), ""]
    ha = [r for r in h if r["aboveThreshold"]]
    hdom = collections.Counter(CHANNELS[max(range(5), key=lambda i: r["perChannelErrors"][i])] for r in ha)
    L += ["**H 的主导通道**：" + "；".join("%s %d（%.0f%%）" % (c, hdom[c], 100.0 * hdom[c] / max(1, len(ha))) for c in CHANNELS), ""]
    text = "\n".join(L) + "\n"
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    open(a.out, "w", encoding="utf-8").write(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
