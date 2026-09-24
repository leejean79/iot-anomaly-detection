#!/usr/bin/env python3
# ============================================================================
# m3_grad_norms.py
# 把逐次梯度范数的 CSV 画成分布图，并按裁决书《梯度裁剪阈值的定法与步骤 A 选择规则的改写》
# 第二节算出生产裁剪阈值。
# Plot the per-update gradient-norm distribution and derive the production clipping threshold.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，终端，仓库根目录。需要 Python 3 与 matplotlib；
#      若未安装：python3 -m pip install matplotlib
# 2. 调用命令 / Invocation:
#      python3 deploy/scripts/m3_grad_norms.py docs/m3_grad_norms.csv
#      python3 deploy/scripts/m3_grad_norms.py docs/m3_grad_norms.csv --batch 32
#    参数 / Arguments:
#      <csv>          逐次范数 CSV，由 syn-m3-grid.sh 的 --grad-norm-name 产出
#      --batch <n>    只看某一个小批量大小；缺省则每个小批量各出一组读数，图上各一条
#      --out <路径>   输出 PNG，默认与输入同名但扩展名为 .png
# 3. 前置条件 / Preconditions: CSV 须含列 batchSize, update, gradNormTotal, gradNormMaxLayer。
# 4. 期望产出 / Expected output: 一幅直方图（横轴对数），画出逐层范数最大值的分布，
#      并标出该分布的第 99.9 百分位、它的三倍（即生产阈值）以及被否决的旧阈值 1.0；
#      终端打印每个小批量的中位数、第 99、第 99.9 百分位、最大值，以及在候选阈值下被裁的更新占比。
# 5. 常见失败兜底 / Failure fallback:
#      ModuleNotFoundError: matplotlib → 按第 1 条安装；
#      「缺少必需的列」→ 多半传成了汇总 CSV，本脚本要的是 --grad-norm-name 产出的那一份。
#
# 【为什么以「逐层最大」为准】深度学习库的二范数裁剪是**逐层**比较的：某一层的梯度范数超过阈值才裁
# 那一层。整模型范数是各层范数的平方和开方，必然大于任何单层的范数，用它定阈值会系统性偏大、
# 使裁剪几乎永不触发。测的量必须与裁的量一致，故阈值取自逐层口径，整模型口径仅作参照。
# The threshold must come from the per-layer distribution, which is what clipping compares.
#
# 缩写自查 / Abbreviations: 二范数 = 各分量平方和的平方根；
#   第 99.9 百分位 = 千分之九百九十九的取值不超过它的那个值。
# ============================================================================
import argparse
import csv
import os
import sys
from collections import OrderedDict

SERIES_COLORS = ["#2a78d6", "#eb6834"]
INK_PRIMARY = "#1a1a19"
INK_MUTED = "#6b6a63"
GRID = "#e4e3dd"
REQUIRED = ["batchSize", "update", "gradNormTotal", "gradNormMaxLayer"]


def percentile(sorted_values, p):
    """最近邻取法，与 Java 侧 GradientNormRecorder 一致，便于两边对照。"""
    if not sorted_values:
        return float("nan")
    import math
    idx = int(math.ceil(p / 100.0 * len(sorted_values))) - 1
    return sorted_values[max(0, min(len(sorted_values) - 1, idx))]


def main():
    ap = argparse.ArgumentParser(description="Plot M3 gradient-norm distributions.")
    ap.add_argument("csv")
    ap.add_argument("--batch", type=int, default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("ERROR: 需要 matplotlib。请执行：python3 -m pip install matplotlib", file=sys.stderr)
        return 2

    groups = OrderedDict()
    with open(args.csv, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        missing = [c for c in REQUIRED if c not in (reader.fieldnames or [])]
        if missing:
            raise SystemExit("ERROR: {} 缺少必需的列：{}\n       本脚本要的是 --grad-norm-name "
                             "产出的逐次范数 CSV，不是汇总 CSV。".format(args.csv, ", ".join(missing)))
        for row in reader:
            bs = int(row["batchSize"])
            if args.batch is not None and bs != args.batch:
                continue
            groups.setdefault(bs, {"total": [], "layer": []})
            groups[bs]["total"].append(float(row["gradNormTotal"]))
            groups[bs]["layer"].append(float(row["gradNormMaxLayer"]))
    if not groups:
        raise SystemExit("ERROR: CSV 里没有匹配的数据行。")

    print("梯度二范数分布（阈值以**逐层最大**为准——那才是裁剪实际比较的量）：")
    thresholds = {}
    for bs, d in groups.items():
        lay = sorted(d["layer"])
        tot = sorted(d["total"])
        p999 = percentile(lay, 99.9)
        th = 3.0 * p999
        thresholds[bs] = th
        clipped = sum(1 for v in lay if v > th)
        clipped_at_one = sum(1 for v in lay if v > 1.0)
        print("  小批量 %-3d（%d 次更新）" % (bs, len(lay)))
        print("      逐层最大：中位数 %.4f  p99 %.4f  p99.9 %.4f  最大 %.4f"
              % (percentile(lay, 50), percentile(lay, 99), p999, lay[-1]))
        print("      整模型  ：中位数 %.4f  p99 %.4f  p99.9 %.4f  最大 %.4f"
              % (percentile(tot, 50), percentile(tot, 99), percentile(tot, 99.9), tot[-1]))
        print("      → 生产阈值 = 3 × 逐层 p99.9 = %.4f，该阈值下被裁 %d 次（占 %.4f%%）"
              % (th, clipped, 100.0 * clipped / len(lay)))
        print("      参照：被否决的旧阈值 1.0 会裁到 %d 次（占 %.1f%%）"
              % (clipped_at_one, 100.0 * clipped_at_one / len(lay)))

    out = args.out or os.path.splitext(args.csv)[0] + ".png"

    # 每个小批量一个面板（小多图），不把六组直方图叠在同一张里。
    # 叠加时颜色只够区分「逐层／整模型」两类，三个小批量便只能靠透明度区分——
    # 那等于让三条不同的数据共用一个颜色，图例与图形对不上，而且三条阈值线会叠在一起看不清。
    # One panel per batch: overlaying six histograms would force three batches to share one hue.
    import math
    keys = list(groups.keys())
    fig, axes = plt.subplots(1, len(keys), figsize=(5.2 * len(keys), 5.0), sharex=True, sharey=True)
    if len(keys) == 1:
        axes = [axes]
    fig.patch.set_facecolor("#fcfcfb")

    allv = [v for d in groups.values() for v in d["layer"] + d["total"] if v > 0]
    lo, hi = min(allv + [1.0]), max(allv + list(thresholds.values()))
    bins = [10 ** x for x in
            [math.log10(lo) + i * (math.log10(hi * 1.3) - math.log10(lo)) / 36 for i in range(37)]]

    halo = dict(facecolor="#fcfcfb", edgecolor="none", pad=1.5)
    for ax, bs in zip(axes, keys):
        d = groups[bs]
        ax.set_facecolor("#fcfcfb")
        ax.set_xscale("log")
        ax.grid(True, color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color(GRID)
        ax.tick_params(colors=INK_MUTED, labelsize=9)
        ax.set_xlabel("gradient L2 norm (log scale)", color=INK_MUTED, fontsize=10)
        ax.set_title("batch %d  (%d updates)" % (bs, len(d["layer"])),
                     color=INK_PRIMARY, fontsize=11, loc="left", pad=10)

        ax.hist(d["layer"], bins=bins, color=SERIES_COLORS[0], alpha=0.8,
                label="per-layer max (clipped quantity)")
        ax.hist(d["total"], bins=bins, color=SERIES_COLORS[1], alpha=0.5,
                label="whole model (reference)")

        th = thresholds[bs]
        ax.axvline(th, color=INK_MUTED, linewidth=1.6, linestyle="--")
        ax.axvline(1.0, color=INK_MUTED, linewidth=1.6, linestyle=":")
        top = ax.get_ylim()[1]
        # 标注放在面板中段：上方留给图例，下方是直方图主体，中段在参考线处恰好是空的。
        ax.annotate("threshold %.0f" % th, xy=(th, top * 0.58), color=INK_MUTED, fontsize=9,
                    ha="right", va="center", bbox=halo, xytext=(-4, 0), textcoords="offset points")
        ax.annotate("rejected 1.0", xy=(1.0, top * 0.44), color=INK_MUTED, fontsize=9,
                    ha="left", va="center", bbox=halo, xytext=(4, 0), textcoords="offset points")

    axes[0].set_ylabel("updates", color=INK_MUTED, fontsize=10)
    # 图例只在第一个面板出现一次：三个面板的两类含义相同，重复三遍是噪声。
    axes[0].legend(frameon=False, fontsize=9, labelcolor=INK_PRIMARY, loc="upper left")
    fig.suptitle("M3 gradient-norm distribution — device E, hidden 60, window 60, lr 0.001, no clipping",
                 color=INK_PRIMARY, fontsize=12, x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(out, dpi=160, facecolor=fig.get_facecolor())
    print("图已写出：%s" % out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
