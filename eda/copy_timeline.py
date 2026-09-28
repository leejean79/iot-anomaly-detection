#!/usr/bin/env python3
# ============================================================================
# copy_timeline.py
# 画全数据集的时间线：设备 × 日期的覆盖率（当天轮数 ÷ 8,640），拷贝段（目标区间）涂色，其源区间
# 在图上方标出；并按设备算出「有效独立覆盖天数」= 不在拷贝目标区间内的各天覆盖率之和。
# Timeline of the whole dataset: device-by-day coverage, copy targets shaded, their sources marked,
# and per-device effective independent coverage days (coverage summed over non-copy days).
#
# 依据：2026-09-28 裁决书第二节第 2 条。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac 或任意有 python3、pandas、matplotlib 的机器，仓库根目录。
# 2. 调用命令：
#      python3 eda/copy_timeline.py --daily docs/dataset_daily_rounds.csv \
#          --segments docs/copy_segments.csv \
#          --out docs/figures/dataset_copy_timeline.png --coverage-out docs/dataset_coverage.csv
# 3. 前置条件：count_rounds.py 写出的全数据集逐日表；check_shift_copy.py --list-out 写出的拷贝段清单
#    （只取 copy 列为 True 的行，按 copied_devices 列逐设备涂色）。
# 4. 期望产出：一幅 PNG 时间线；终端与 --coverage-out 给出逐设备的总覆盖天数与有效独立覆盖天数。
# 5. 失败兜底：清单里没有 copy 为 True 的行时照常出图，只是不涂色，并注明。
#
# 局限：按整天统计。拷贝段起止若不在零点（例如六月段的拷贝从 06-12 23:00 前后开始），首尾那一天
# 的零头不会计入拷贝，报告中须另行说明。
# Limitation: whole days only; partial first and last days of a copy are not counted as copied.
# ============================================================================
import argparse
import os
import sys
from datetime import date, timedelta

import numpy as np
import pandas as pd

FULL_DAY = 8640


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily", required=True)
    ap.add_argument("--segments", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--coverage-out")
    a = ap.parse_args()

    t = pd.read_csv(a.daily).set_index("device")
    first, last = date.fromisoformat(min(t.columns)), date.fromisoformat(max(t.columns))
    days = [first + timedelta(days=i) for i in range((last - first).days + 1)]
    cov = np.zeros((len(t.index), len(days)))
    for j, d in enumerate(days):
        if d.isoformat() in t.columns:
            cov[:, j] = np.minimum(t[d.isoformat()].to_numpy() / FULL_DAY, 1.0)

    seg = pd.read_csv(a.segments)
    seg = seg[seg["copy"].astype(str) == "True"] if "copy" in seg.columns else seg.iloc[0:0]
    devs = list(t.index)
    target = np.zeros((len(devs), len(days)), dtype=bool)   # 逐设备：该日是拷贝目标 / per device
    source = np.zeros((len(devs), len(days)), dtype=bool)
    for _, r in seg.iterrows():
        rows = [devs.index(x) for x in str(r["copied_devices"]).split(";") if x in devs]
        for col, mask in (("target", target), ("source", source)):
            s, e = date.fromisoformat(r[f"{col}_start"]), date.fromisoformat(r[f"{col}_end"])
            for j, d in enumerate(days):
                if s <= d < e:
                    mask[rows, j] = True
    if seg.empty:
        print("拷贝段清单中没有 copy 为 True 的行：时间线不涂色。")

    total = cov.sum(axis=1)
    indep = np.where(target, 0.0, cov).sum(axis=1)
    print(f"时间线：{first} 至 {last}，共 {len(days)} 天；拷贝目标 {target.any(axis=0).sum()} 天")
    print(f"{'设备':<4}{'总覆盖天数':>12}{'有效独立覆盖天数':>18}")
    lines = ["device,covered_days,independent_days"]
    for i, dev in enumerate(t.index):
        print(f"{dev:<6}{total[i]:>12.1f}{indep[i]:>18.1f}")
        lines.append(f"{dev},{total[i]:.2f},{indep[i]:.2f}")
    if a.coverage_out:
        with open(a.coverage_out, "w") as fh:
            fh.write("\n".join(lines) + "\n")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap
    from matplotlib.patches import Patch

    fig, ax = plt.subplots(figsize=(14, 3.6))
    blues = LinearSegmentedColormap.from_list("cov", ["#ffffff", "#2a78d6"])
    ax.imshow(cov, aspect="auto", cmap=blues, vmin=0, vmax=1, interpolation="nearest",
              extent=(-0.5, len(days) - 0.5, len(t.index) - 0.5, -0.5))
    for i, j in zip(*np.nonzero(target)):
        ax.add_patch(plt.Rectangle((j - 0.5, i - 0.5), 1, 1, color="#eb6834", lw=0))
    for j in np.flatnonzero(source.any(axis=0)):
        ax.plot([j - 0.5, j + 0.5], [-0.75, -0.75], color="#eb6834", lw=4, solid_capstyle="butt",
                clip_on=False)
    ax.set_yticks(range(len(t.index)))
    ax.set_yticklabels(t.index)
    ticks = [j for j, d in enumerate(days) if d.day == 1 or j == 0]
    ax.set_xticks(ticks)
    ax.set_xticklabels([days[j].isoformat() for j in ticks], fontsize=8)
    ax.set_ylim(len(t.index) - 0.5, -1.0)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.legend(handles=[Patch(color="#2a78d6", label="coverage (rounds / 8640)"),
                       Patch(color="#eb6834", label="copy target (not independent)"),
                       Patch(color="#eb6834", label="copy source (bar above)")],
              loc="upper left", bbox_to_anchor=(1.0, 1.0), frameon=False, fontsize=8)
    # 图中文字用英文：matplotlib 默认字体没有中文字形 / English labels: no CJK glyphs by default
    ax.set_title(f"Dataset timeline {first} to {last} ({len(days)} days): daily coverage and "
                 f"shifted-copy segments", fontsize=10, loc="left")
    fig.tight_layout()
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    fig.savefig(a.out, dpi=150)
    print(f"时间线已写出：{a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
