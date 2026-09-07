#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
m2_dispersion_report.py —— 逐通道离散度诊断的自动判读：给定目标设备，找出它相对全队在哪个通道发散，
并按数据事实第 24 条的**两类病理**给出处置方向。
Read a per-channel dispersion CSV, pinpoint which channel(s) make a target device disperse relative to
the fleet, and classify each per DF-24's two pathologies.

两类病理（DF-24）/ two pathologies:
  - **高峰度型（乘性重尾）**：spread 大、超额峰度高（尖峰）——指向**对数变换**（如 Light 的 log1p）。
  - **退化 IQR 型**：spread 大、峰度为负/平、且"IQR ÷ 主体宽度(P5~P95)"很小——指向**相对退化防护**
    （用 主体宽度/2.44 作分母），对数变换对这类无效甚至加剧。

判据：目标设备某通道 spread ≥ 全队中位数 ×3 即嫌疑；再按峰度与 iqr/body 比二分类。

================================ 脚本交付五要素 / Five delivery elements ================================
1. 执行环境 / Environment: 本地 python3（仅标准库）。输入为 M2Probe --dispersion-out 产出的 CSV
   （需含 iqr_over_body 列；旧 CSV 无该列时该项显示 n/a，仍可按 spread/峰度判读）。
2. 调用命令 / Invocation:
     python3 deploy/scripts/m2_dispersion_report.py --csv docs/m2_dispersion_logtf.csv --target G
3. 前置条件 / Preconditions: CSV 表头含 device,channel,spread_p99_p1,excess_kurtosis[,iqr_over_body]。
4. 期望产出 / Expected output: 目标逐通道 spread/峰度/(IQR÷主体) vs 全队中位数 + 嫌疑通道与两类病理结论。
5. 失败兜底 / Failure fallback: 目标设备缺失/CSV 格式不符 → 报错退出，不臆造。
"""

import argparse
import csv
import sys
from collections import defaultdict

CH_NAMES = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]
SPREAD_RATIO_FLAG = 3.0     # spread ≥ 全队中位数 ×3 → 嫌疑
KURT_HIGH = 10.0            # 超额峰度 ≥ 此值 → 高峰度（乘性重尾）→ 对数变换
IQR_BODY_SMALL = 0.10       # IQR/主体宽度 < 此值 → 退化 IQR → 相对防护


def median(xs):
    s = sorted(x for x in xs if x is not None)
    n = len(s)
    if n == 0:
        return None
    return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--target", default="G")
    args = ap.parse_args()

    # {device: {channel: (spread, kurt, ratio_or_None)}}
    data = defaultdict(dict)
    with open(args.csv, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        need = {"device", "channel", "spread_p99_p1", "excess_kurtosis"}
        if not need.issubset(rd.fieldnames or []):
            sys.exit("ERROR: CSV 缺列，需含 %s，实得 %s" % (need, rd.fieldnames))
        has_ratio = "iqr_over_body" in (rd.fieldnames or [])
        for r in rd:
            ratio = None
            if has_ratio:
                try:
                    ratio = float(r["iqr_over_body"])
                except (ValueError, KeyError):
                    ratio = None
            data[r["device"]][int(r["channel"])] = (
                float(r["spread_p99_p1"]), float(r["excess_kurtosis"]), ratio)

    if args.target not in data:
        sys.exit("ERROR: 目标设备 %s 不在 CSV 中（有 %s）" % (args.target, sorted(data)))

    others = [d for d in data if d != args.target]
    channels = sorted(data[args.target].keys())

    print("=" * 84)
    print("逐通道离散度判读：目标设备 %s vs 其余 %d 台中位数" % (args.target, len(others)))
    print("=" * 84)
    print("%-13s %10s %11s %6s %9s %10s %9s %s"
          % ("channel", "tgt_spread", "others_med", "×", "tgt_kurt", "IQR/body", "others_r", "嫌疑"))
    suspects = []
    for c in channels:
        name = CH_NAMES[c] if c < len(CH_NAMES) else ("ch%d" % c)
        tsp, tku, tra = data[args.target][c]
        osp = median([data[d][c][0] for d in others if c in data[d]])
        ora = median([data[d][c][2] for d in others if c in data[d] and data[d][c][2] is not None])
        ratio = (tsp / osp) if (osp and osp > 1e-12) else float("inf")
        flag = "← 嫌疑" if ratio >= SPREAD_RATIO_FLAG else ""
        if flag:
            suspects.append((name, c, tsp, tku, tra, ratio))
        rtxt = "inf" if ratio == float("inf") else "%.1f" % ratio
        tratxt = "n/a" if tra is None else "%.3f" % tra
        oratxt = "n/a" if ora is None else "%.3f" % ora
        print("%-13s %10.3f %11.3f %6s %9.2f %10s %9s %s"
              % ("%d %s" % (c, name), tsp, osp if osp is not None else float("nan"),
                 rtxt, tku, tratxt, oratxt, flag))

    print("-" * 84)
    if not suspects:
        print("结论：%s 在各通道均未显著偏离全队——过度活跃可能来自多通道轻度叠加或邻居结构，"
              "非单一通道；建议按目标带直接取 R 或接受其基线。" % args.target)
        return

    print("结论：%s 的发散集中在 %s。逐通道按数据事实第 24 条分型："
          % (args.target, "、".join("%s(ch%d)" % (n, c) for n, c, *_ in suspects)))
    for name, c, tsp, tku, tra, ratio in suspects:
        if tku >= KURT_HIGH:
            kind = "**高峰度型（乘性重尾）→ 对数变换**（如 Light 的 log1p）"
        elif tra is not None and tra < IQR_BODY_SMALL:
            kind = ("**退化 IQR 型 → 相对退化防护**（宽间距 spread=%.1f、负/平峰 kurt=%.2f、"
                    "IQR/主体=%.3f<%.2f）；对数变换对此无效甚至加剧" % (tsp, tku, tra, IQR_BODY_SMALL))
        elif tku < 0 and (tra is None):
            kind = ("疑**退化 IQR 型**（宽间距 + 负峰 kurt=%.2f，但 CSV 无 IQR/body 列无法确证）；"
                    "建议用带 iqr_over_body 的探针复跑确认" % tku)
        else:
            kind = "形态不典型（spread 高但峰度/IQR-body 不落两类）——建议人工复核该通道"
        print("  - %s(ch%d)：%s" % (name, c, kind))


if __name__ == "__main__":
    main()
