#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
m2_dispersion_report.py —— 逐通道离散度诊断的自动判读：给定目标设备，找出它相对全队在哪个通道发散。
Read a per-channel dispersion CSV and pinpoint which channel(s) make a target device disperse
relative to the rest of the fleet — automating the by-hand comparison used in M2 Task-1.

用途：定位某设备过度活跃的通道来源。M2 收尾中 C/D 指向 Light（ch4）；对数变换后 G 反转为过度活跃，
本工具用来确认 G 的发散落在哪个**非 Light**通道。判据是把目标设备每个通道的 spread(P99−P1) 与超额峰度
同"其余设备该通道的中位数"比，比值显著偏大的通道即嫌疑。

================================ 脚本交付五要素 / Five delivery elements ================================
1. 执行环境 / Environment: 本地 python3（仅标准库）。输入为 M2Probe --dispersion-out 产出的 CSV。
2. 调用命令 / Invocation:
     python3 deploy/scripts/m2_dispersion_report.py --csv docs/m2_dispersion_logtf.csv --target G
3. 前置条件 / Preconditions: CSV 含表头 device,channel,n,p1,p99,spread_p99_p1,excess_kurtosis。
4. 期望产出 / Expected output: 目标设备逐通道 spread/峰度 vs 全队中位数的倍数表 + 嫌疑通道结论。
5. 失败兜底 / Failure fallback: 目标设备缺失/CSV 格式不符 → 报错退出，不臆造。
"""

import argparse
import csv
import sys
from collections import defaultdict

# 通道固定顺序（Channels.DETECTION）/ fixed channel order
CH_NAMES = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]
SPREAD_RATIO_FLAG = 3.0     # spread 比全队中位数大 ≥3× → 嫌疑
KURT_ABS_FLAG = 5.0         # 超额峰度绝对值 ≥5 且远高于全队 → 重尾嫌疑


def median(xs):
    s = sorted(xs)
    n = len(s)
    if n == 0:
        return None
    return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="离散度 CSV（M2Probe --dispersion-out 产出）")
    ap.add_argument("--target", default="G", help="目标设备（默认 G）")
    args = ap.parse_args()

    # {device: {channel: (spread, kurt)}}
    data = defaultdict(dict)
    with open(args.csv, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        need = {"device", "channel", "spread_p99_p1", "excess_kurtosis"}
        if not need.issubset(rd.fieldnames or []):
            sys.exit("ERROR: CSV 缺列，需含 %s，实得 %s" % (need, rd.fieldnames))
        for r in rd:
            data[r["device"]][int(r["channel"])] = (
                float(r["spread_p99_p1"]), float(r["excess_kurtosis"]))

    if args.target not in data:
        sys.exit("ERROR: 目标设备 %s 不在 CSV 中（有 %s）" % (args.target, sorted(data)))

    others = [d for d in data if d != args.target]
    channels = sorted(data[args.target].keys())

    print("=" * 78)
    print("逐通道离散度判读：目标设备 %s vs 其余 %d 台中位数" % (args.target, len(others)))
    print("=" * 78)
    print("%-12s %10s %12s %6s %10s %12s %s"
          % ("channel", "tgt_spread", "others_med", "×", "tgt_kurt", "others_med", "嫌疑"))
    suspects = []
    for c in channels:
        name = CH_NAMES[c] if c < len(CH_NAMES) else ("ch%d" % c)
        tsp, tku = data[args.target][c]
        osp = median([data[d][c][0] for d in others if c in data[d]])
        oku = median([data[d][c][1] for d in others if c in data[d]])
        ratio = (tsp / osp) if (osp and osp > 1e-12) else float("inf")
        flag = ""
        if ratio >= SPREAD_RATIO_FLAG or (tku >= KURT_ABS_FLAG and tku >= 2 * (oku or 0)):
            flag = "← 嫌疑"
            suspects.append((name, c, ratio, tku))
        rtxt = "inf" if ratio == float("inf") else "%.1f" % ratio
        print("%-12s %10.3f %12.3f %6s %10.2f %12.2f %s"
              % ("%d %s" % (c, name), tsp, osp if osp is not None else float("nan"),
                 rtxt, tku, oku if oku is not None else float("nan"), flag))

    print("-" * 78)
    if suspects:
        names = "、".join("%s(ch%d)" % (n, c) for n, c, _, _ in suspects)
        light_only = all(c == 4 for _, c, _, _ in suspects)
        non_light = [s for s in suspects if s[1] != 4]
        print("结论：%s 的发散集中在 %s。" % (args.target, names))
        if light_only:
            print("  → 仅 Light（ch4）——同 C/D 的标准化重尾假象，处置在通道层面（如已用的 log1p）。")
        elif non_light:
            nl = "、".join("%s(ch%d)" % (n, c) for n, c, _, _ in non_light)
            print("  → 含**非 Light** 通道 %s：这是该通道上的真实发散，不是 Light 假象；" % nl)
            print("     若要压其离群率，方向在这些通道本身（真实动态设备则接受较高基线），而非再调 Light。")
    else:
        print("结论：%s 在各通道均未显著偏离全队——其过度活跃可能来自多通道的轻度叠加或邻居结构，"
              "非单一通道重尾；建议按目标带直接取 R 或接受其基线。" % args.target)


if __name__ == "__main__":
    main()
