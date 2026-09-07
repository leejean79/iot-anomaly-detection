#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
m2_probe_compare.py —— 探针离群率两轮对比与断言（补充指令三 step3）。
Compare two probe runs' per-device per-R outlier rates and assert the non-target devices barely moved.

用途：相对退化防护上线后重跑探针，核验"除 G 外七台设备各网格点离群率相对变化 ≤ 阈值"——它们的光照
IQR 本就健康、防护不应触发。若某台越阈值，如实列出（哪台、哪个 R、变化多少），不自行调整。

================================ 脚本交付五要素 / Five delivery elements ================================
1. 执行环境 / Environment: 本地 python3（仅标准库）。
2. 调用命令 / Invocation:
     python3 deploy/scripts/m2_probe_compare.py --old docs/m2_probe_logtf.csv --new docs/m2_probe_relguard.csv \
         --exclude G --tol 0.05 --k 10
3. 前置条件 / Preconditions: 两 CSV 同格式（device,R,k,...,meanOutlierRate）。
4. 期望产出 / Expected output: 逐设备最大相对变化 + 总断言 PASS/FAIL；越阈明细逐条列出。
5. 失败兜底 / Failure fallback: 缺设备/缺网格点 → 该项标注，不臆造；断言以退出码反映（0=PASS，1=FAIL）。
"""

import argparse
import csv
import sys
from collections import defaultdict

RATE_FLOOR = 1e-5   # 相对变化分母下限，避免极小基数噪声 / floor for the relative-change denominator


def load(path, k):
    d = defaultdict(dict)
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if int(float(r["k"])) != k:
                continue
            d[r["device"]][round(float(r["R"]), 2)] = float(r["meanOutlierRate"])
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True)
    ap.add_argument("--new", required=True)
    ap.add_argument("--exclude", default="G", help="不参与断言的设备（逗号分隔），默认 G")
    ap.add_argument("--tol", type=float, default=0.05, help="相对变化阈值（默认 0.05 = 5%）")
    ap.add_argument("--k", type=int, default=10)
    args = ap.parse_args()

    excl = {s.strip() for s in args.exclude.split(",") if s.strip()}
    old = load(args.old, args.k)
    new = load(args.new, args.k)

    print("=" * 72)
    print("探针两轮对比（k=%d，阈值 %.0f%%，排除 %s）" % (args.k, args.tol * 100, "、".join(sorted(excl)) or "无"))
    print("  old=%s\n  new=%s" % (args.old, args.new))
    print("=" * 72)

    violations = []
    print("%-4s %-8s %-10s %-10s %-8s" % ("dev", "R", "old", "new", "Δrel"))
    for dev in sorted(set(old) | set(new)):
        rs = sorted(set(old.get(dev, {})) | set(new.get(dev, {})))
        dev_max = 0.0
        for r in rs:
            ov = old.get(dev, {}).get(r)
            nv = new.get(dev, {}).get(r)
            if ov is None or nv is None:
                print("%-4s %-8.2f %-10s %-10s %-8s" % (dev, r,
                      "n/a" if ov is None else "%.6f" % ov,
                      "n/a" if nv is None else "%.6f" % nv, "缺点"))
                continue
            rel = abs(nv - ov) / max(ov, RATE_FLOOR)
            dev_max = max(dev_max, rel)
            mark = ""
            if dev not in excl and rel > args.tol:
                mark = "← 越阈"
                violations.append((dev, r, ov, nv, rel))
            print("%-4s %-8.2f %-10.6f %-10.6f %-7.1f%% %s" % (dev, r, ov, nv, rel * 100, mark))
        tag = "（排除，不判）" if dev in excl else ""
        print("   %s 最大相对变化 %.1f%% %s" % (dev, dev_max * 100, tag))

    print("-" * 72)
    if violations:
        print("❌ 断言 FAIL：以下（非排除）设备网格点相对变化 > %.0f%%（如实报回，勿自行调整）：" % (args.tol * 100))
        for dev, r, ov, nv, rel in violations:
            print("   %s R=%.2f：%.6f → %.6f（%.1f%%）" % (dev, r, ov, nv, rel * 100))
        sys.exit(1)
    else:
        print("✅ 断言 PASS：除排除设备外，各设备各网格点离群率相对变化均 ≤ %.0f%%——防护未误伤健康通道。"
              % (args.tol * 100))


if __name__ == "__main__":
    main()
