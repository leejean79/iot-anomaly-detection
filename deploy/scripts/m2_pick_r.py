#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
m2_pick_r.py —— 按 M2 收尾任务书「任务一」的规则，从 (R,k) 探针 CSV 逐设备选取 R，并输出标定表。
Pick a per-device R from the (R,k) probe CSV using the M2-closeout Task-1 rules; emit the calibration table.

规则（用户已确认，照此执行，不自行变通）/ Rules (user-confirmed; applied verbatim):
  1. k 全局固定为 10（本脚本只看 k==10 的行）。
  2. 标定段沿用探针同款正常月（2022-03），预热日已由探针按 isWarmup() 剔除。
  3. R 网格 {0.75, 1.0, 1.25, 1.5, 1.75}。
  4. 每台设备取「离群率落入目标带 [0.1%, 0.5%] 且最接近带中心 0.3%」的 R。
  5. 两种越界处置：
     - 全部网格点 > 0.5%      → 取最大 R(1.75)，标记「过度活跃、待人工复核」；
     - 最小 R(0.75) 仍约等于零 → 取 0.75，标记「分辨力疑似不足、待人工复核」。
     （补充的稳健分支：网格恰好跨过目标带、无点落入 → 取最接近带中心者，标记「网格跨带、待人工复核」，
       并在报告里点名，交人工/设计会话定夺——不自行裁决。）
  6. 选定即冻结（决策 D14）；后续尺度漂移由标准化参数重估承接，不动 R。

离群率口径 / rate metric: 探针 CSV 的 meanOutlierRate（逐滑动步离群占比的均值），与目标带同口径。

================================ 脚本交付五要素 / Five delivery elements ================================
1. 执行环境 / Environment: 任意有 python3 标准库的机器（本地即可）；纯离线，读 CSV、写 Markdown。
2. 调用命令 / Invocation:
     python3 deploy/scripts/m2_pick_r.py --csv docs/m2_probe_calib.csv --out docs/m2_rk_calibration.md
     python3 deploy/scripts/m2_pick_r.py --csv docs/m2_probe_calib.csv        # 只打印，不写文件
3. 前置条件 / Preconditions: CSV 含表头 device,R,k,slides,meanWindowPoints,meanOutlierRate,fracZeroSlides，
     且 k==10 的行覆盖 R∈{0.75,1.0,1.25,1.5,1.75}（缺点会被点名，不静默略过）。
4. 期望产出 / Expected output: stdout 逐设备选取表 + 越界标记；--out 时另写一份 Markdown（含每设备形态段）。
5. 失败兜底 / Failure fallback: 某设备 R 网格不全 → 该设备标「网格不全、需补跑」并列出已有点，不臆造。
"""

import argparse
import csv
import sys
from collections import defaultdict

K_FIXED = 10
R_GRID = [0.75, 1.0, 1.25, 1.5, 1.75]
BAND_LOW = 0.001      # 0.1%
BAND_HIGH = 0.005     # 0.5%
BAND_CENTER = 0.003   # 0.3%
R_MIN = 0.75
R_MAX = 1.75
EPS = 1e-9


def load(csv_path):
    """读 CSV，返回 {device: {R(float): rate(float)}}（只保留 k==10）。"""
    by_dev = defaultdict(dict)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        need = {"device", "R", "k", "meanOutlierRate"}
        if not need.issubset(reader.fieldnames or []):
            sys.exit("ERROR: CSV 缺列，需含 %s，实得 %s" % (need, reader.fieldnames))
        for row in reader:
            if int(float(row["k"])) != K_FIXED:
                continue
            dev = row["device"].strip()
            r = round(float(row["R"]), 2)
            by_dev[dev][r] = float(row["meanOutlierRate"])
    return by_dev


def pick_for_device(rates):
    """
    对一台设备的 {R: rate} 应用规则 4/5，返回 (chosen_R, chosen_rate, flag, shape_note)。
    rates 以 R 升序访问；离群率随 R 单调不增（探针实测性质），据此判定越界分支。
    """
    present = sorted(rates.keys())
    missing = [r for r in R_GRID if r not in rates]
    # 网格不全：不臆造，交回补跑
    if missing:
        return (None, None, "网格不全、需补跑",
                "已有点 R=%s；缺 R=%s。" % (
                    ", ".join("%.2f" % p for p in present),
                    ", ".join("%.2f" % m for m in missing)))

    # 目标带内候选：取最接近带中心者；平距时取较小 R（更紧半径、更敏感，确定性平局处置）
    in_band = [r for r in R_GRID if BAND_LOW - EPS <= rates[r] <= BAND_HIGH + EPS]
    if in_band:
        chosen = min(in_band, key=lambda r: (abs(rates[r] - BAND_CENTER), r))
        return (chosen, rates[chosen], "", shape_note(rates))

    # 无点落带内 → 三种越界
    rate_at_min_r = rates[R_MIN]   # 最小 R → 最大离群率（单调）
    rate_at_max_r = rates[R_MAX]   # 最大 R → 最小离群率
    if rate_at_max_r > BAND_HIGH + EPS:
        # 连最大 R 都过报 → 全网格 > 0.5%
        return (R_MAX, rate_at_max_r, "过度活跃、待人工复核", shape_note(rates))
    if rate_at_min_r < BAND_LOW - EPS:
        # 连最小 R 都约等于零 → 全网格 < 0.1%
        return (R_MIN, rate_at_min_r, "分辨力疑似不足、待人工复核", shape_note(rates))
    # 剩余：网格跨过目标带却无点落入（相邻两点一个在带上、一个在带下）→ 取最接近带中心者，点名复核
    chosen = min(R_GRID, key=lambda r: (abs(rates[r] - BAND_CENTER), r))
    return (chosen, rates[chosen], "网格跨带、待人工复核", shape_note(rates))


def shape_note(rates):
    """一句话描述离群率随 R 的形态（供报告的通俗说明段）。"""
    return " ".join("R%.2f=%.4f%%" % (r, rates[r] * 100.0) for r in R_GRID)


# 变换前对比用的规范网格（含延伸点），算"变化几格"/ canonical grid (with extensions) for step-delta
CANON_GRID = [0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 3.0]


def parse_rmap(spec):
    """解析 "A=1.0,B=1.0,..." 为 {设备:R}。"""
    m = {}
    for item in (spec or "").split(","):
        s = item.strip()
        if not s or "=" not in s:
            continue
        d, v = s.split("=", 1)
        m[d.strip()] = round(float(v), 2)
    return m


def step_delta(new_r, prev_r):
    """两个 R 在规范网格上的位置差（格数）；不在网格上取最近位。"""
    def idx(r):
        return min(range(len(CANON_GRID)), key=lambda i: abs(CANON_GRID[i] - r))
    return idx(new_r) - idx(prev_r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="探针 CSV（含 k==10 的 R∈{0.75..1.75} 行）")
    ap.add_argument("--out", default=None, help="输出 Markdown 路径（省略则只打印）")
    ap.add_argument("--prev", default="A=1.0,B=1.0,C=1.75,D=1.75,E=0.75,F=2.25,G=0.75,H=1.0",
                    help="变换前选值（用于'与变换前对比'列）；默认为收尾任务一定稿/临时值")
    args = ap.parse_args()

    prev = parse_rmap(args.prev)

    by_dev = load(args.csv)
    if not by_dev:
        sys.exit("ERROR: CSV 里没有 k==%d 的行。" % K_FIXED)

    rows = []
    for dev in sorted(by_dev.keys()):
        chosen_r, chosen_rate, flag, shape = pick_for_device(by_dev[dev])
        pr = prev.get(dev)
        delta = None if (chosen_r is None or pr is None) else step_delta(chosen_r, pr)
        rows.append((dev, chosen_r, chosen_rate, flag, shape, pr, delta))

    # ---- stdout ----
    print("=" * 68)
    print("M2 逐设备 R 标定（k=%d，目标带 [%.1f%%, %.1f%%]，中心 %.1f%%）"
          % (K_FIXED, BAND_LOW * 100, BAND_HIGH * 100, BAND_CENTER * 100))
    print("=" * 68)
    print("%-3s %-6s %-6s %-6s %-11s %s" % ("dev", "R", "prev", "Δ格", "rate", "flag"))
    for dev, r, rate, flag, _, pr, delta in rows:
        rtxt = "n/a" if r is None else "%.2f" % r
        prtxt = "n/a" if pr is None else "%.2f" % pr
        dtxt = "n/a" if delta is None else "%+d" % delta
        ratetxt = "n/a" if rate is None else "%.4f%%" % (rate * 100.0)
        print("%-3s %-6s %-6s %-6s %-11s %s" % (dev, rtxt, prtxt, dtxt, ratetxt, flag or "OK"))
    flagged = [r for r in rows if r[3]]
    print("-" * 68)
    print("标记复核设备数 / flagged: %d" % len(flagged))

    if args.out:
        write_md(args.out, rows)
        print("已写入 / wrote: %s" % args.out)


def write_md(path, rows):
    lines = []
    lines.append("# M2 逐设备 R 标定表（对数变换后重标定，收尾补充指令二第三步）")
    lines.append("")
    lines.append("> 依据 M2 收尾任务书「任务一」的**原规则一字不改**由 `deploy/scripts/m2_pick_r.py` 自动选取；"
                 "k 全局固定 10，标定段 2022-03（预热日已剔除），离群率口径 = 探针 `meanOutlierRate`。")
    lines.append("> 本轮数据来自**含 Light 通道 log1p 预变换**的重标定链条。**本表回传设计会话做终值确认**；"
                 "被标记设备只观察成因、不自行裁决。")
    lines.append("")
    lines.append("目标带 [0.1%, 0.5%]，中心 0.3%；R 网格 {0.75, 1.0, 1.25, 1.5, 1.75}（不足则补 2.0/2.5）。"
                 "「变换前 R」为对数变换前的定稿/临时值；「Δ格」为在规范网格上的移动格数。")
    lines.append("")
    lines.append("| 设备 | 选定 R | 变换前 R | Δ格 | 该 R 下标定段离群率 | 是否标记复核 |")
    lines.append("|---|---|---|---|---|---|")
    for dev, r, rate, flag, _, pr, delta in rows:
        rtxt = "n/a" if r is None else "%.2f" % r
        prtxt = "n/a" if pr is None else "%.2f" % pr
        dtxt = "n/a" if delta is None else "%+d" % delta
        ratetxt = "n/a" if rate is None else "%.4f%%" % (rate * 100.0)
        lines.append("| %s | %s | %s | %s | %s | %s |"
                     % (dev, rtxt, prtxt, dtxt, ratetxt, flag or "否"))
    lines.append("")
    # 变化超过一格的设备各写一段观察（补充指令二第三步要求）
    moved = [row for row in rows if row[6] is not None and abs(row[6]) > 1]
    if moved:
        lines.append("## 与变换前相比移动超过一格的设备（观察）")
        lines.append("")
        for dev, r, rate, flag, shape, pr, delta in moved:
            lines.append("- **设备 %s**：R 由 %.2f 变为 %.2f（%+d 格）。形态 %s。"
                         % (dev, pr, r, delta, shape))
        lines.append("")
    flagged = [row for row in rows if row[3]]
    if flagged:
        lines.append("## 被标记设备的形态观察（只观察成因，不裁决）")
        lines.append("")
        for dev, r, rate, flag, shape, pr, delta in flagged:
            lines.append("- **设备 %s（%s）**：离群率随 R 的形态 %s。" % (dev, flag, shape))
        lines.append("")
    lines.append("## 逐设备离群率随 R 的完整形态（k=10）")
    lines.append("")
    for dev, r, rate, flag, shape, pr, delta in rows:
        lines.append("- %s：%s" % (dev, shape))
    lines.append("")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
