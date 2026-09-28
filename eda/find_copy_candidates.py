#!/usr/bin/env python3
# ============================================================================
# find_copy_candidates.py
# 从逐设备逐日轮数表里找「平移拷贝」的候选：把每一天的 8 台设备轮数看成一个向量，找出向量完全相同
# 的日期对（平移天数不限），把同一平移量下连续的日期并成一段。候选只是线索，须再用
# check_shift_copy.py 在原始数据上确认。
# Find shifted-copy candidates from the per-device daily round table: days whose eight-device count
# vectors are identical, at any shift, merged into runs; candidates must then be confirmed.
#
# 依据：2026-09-28 裁决书第二节第 1 条。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac 或任意有 python3 与 pandas 的机器，仓库根目录。
# 2. 调用命令：
#      python3 eda/find_copy_candidates.py --daily docs/dataset_daily_rounds.csv \
#          --out docs/copy_candidates.csv
# 3. 前置条件：先用 count_rounds.py 对全数据集写出逐日表（--out docs/dataset_daily_rounds.csv）。
# 4. 期望产出：终端列出每个候选段（源起始、目标起始、天数、平移天数、其中「平凡日」天数）；写出
#    候选 CSV 供 check_shift_copy.py --pairs-csv 使用。
# 5. 失败兜底：逐日表缺 device 列时退出码 2；没有候选时写出只有表头的 CSV 并说明。
#
# 「平凡日」：非零的设备不足 2 台，或非零设备全部恰为 8,640 轮（满勤）。这类日子的向量天然容易
# 相同，单独计数，不从候选里删除，交给原始数据核查判断。
# A trivial day has fewer than two non-zero devices, or every non-zero device at exactly 8,640.
# ============================================================================
import argparse
import csv
import sys
from collections import defaultdict
from datetime import date, timedelta

import pandas as pd

FULL_DAY = 8640


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    t = pd.read_csv(a.daily)
    if "device" not in t.columns:
        print(f"ERROR: {a.daily} 缺 device 列，不是 count_rounds.py 写出的逐日表。", file=sys.stderr)
        return 2
    t = t.set_index("device")
    vec = {c: tuple(int(x) for x in t[c]) for c in t.columns}      # 日期 → 各设备轮数 / day → counts

    def trivial(v):
        nz = [x for x in v if x > 0]
        return len(nz) < 2 or all(x == FULL_DAY for x in nz)

    by_vec = defaultdict(list)
    for d, v in vec.items():
        if any(v):
            by_vec[v].append(date.fromisoformat(d))
    matches = defaultdict(set)                      # 平移天数 → 较早一天的集合 / shift → earlier days
    for days in by_vec.values():
        days.sort()
        for i, d1 in enumerate(days):
            for d2 in days[i + 1:]:
                matches[(d2 - d1).days].add(d1)

    runs = []
    for shift, starts in matches.items():
        ordered = sorted(starts)
        run = [ordered[0]]
        for d in ordered[1:]:
            if d - run[-1] == timedelta(days=1):
                run.append(d)
            else:
                runs.append((run, shift))
                run = [d]
        runs.append((run, shift))

    rows = []
    for run, shift in sorted(runs, key=lambda x: (x[0][0], x[1])):
        n_triv = sum(trivial(vec[d.isoformat()]) for d in run)
        rows.append({"source_start": run[0].isoformat(),
                     "target_start": (run[0] + timedelta(days=shift)).isoformat(),
                     "days": len(run), "shift_days": shift, "trivial_days": n_triv})
    with open(a.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["source_start", "target_start", "days", "shift_days",
                                           "trivial_days"])
        w.writeheader()
        w.writerows(rows)
    if not rows:
        print("没有找到逐日轮数向量完全相同的日期对。")
    for r in rows:
        print(f"候选：{r['source_start']} → {r['target_start']}，{r['days']} 天，平移 {r['shift_days']} 天"
              f"（平凡日 {r['trivial_days']} 天）")
    print(f"共 {len(rows)} 个候选段，已写出：{a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
