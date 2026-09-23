#!/usr/bin/env python3
# ============================================================================
# m3_lr_curves.py
# 把学习率诊断的逐轮 CSV 画成曲线图（裁决书《分离比检查的处置与训练停滞的诊断》第三节）。
# Plot the per-epoch learning-rate diagnosis CSV.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，终端，仓库根目录。需要 Python 3 与 matplotlib；
#      若未安装：python3 -m pip install matplotlib
# 2. 调用命令 / Invocation:
#      python3 deploy/scripts/m3_lr_curves.py docs/m3_lr_diagnosis.csv
#      python3 deploy/scripts/m3_lr_curves.py docs/m3_lr_diagnosis.csv --out docs/m3_lr_curves.png
#    参数 / Arguments:
#      <csv>            逐轮 CSV，由 syn-m3-grid.sh 的 --per-epoch-name 产出
#      --out <路径>     输出 PNG，默认与输入同名但扩展名为 .png
# 3. 前置条件 / Preconditions: CSV 须含列 learningRate, epoch, trainLoss, esLoss, esLossBaseline。
# 4. 期望产出 / Expected output: 一幅两联图。左联是早停集误差随轮数的变化（裁决书指定的那一幅），
#      画有两条水平参考线：平凡基线，以及基线的七成即「训练成功」判据线（第四节）。
#      右联是训练集误差，用来把「没在学」与「学了但过拟合」区分开——只看早停集误差无法区分这两者。
#      终端另打印每一档的关键读数：末轮误差、最好轮次、最长平台期、典型每轮改善幅度。
# 5. 常见失败兜底 / Failure fallback:
#      ModuleNotFoundError: matplotlib → 按第 1 条安装；
#      图上中文显示成方框 → 本脚本刻意全用英文标注，见下方说明，不应出现此问题。
#
# 【为什么图上标注用英文】matplotlib 默认字体不含中日韩字形，中文标注在多数环境下会渲染成方框。
# 与其让图变成一堆方框，不如图上用英文、把中文说明留在终端输出与文档里。
# Labels are in English on purpose: matplotlib's default fonts lack CJK glyphs.
#
# 缩写自查 / Abbreviations: epoch = 在整个训练集上完整跑一遍；
#   早停集（early-stopping set）= 不参与梯度更新、只用来衡量泛化的那一段数据；
#   平凡基线 = 「一律输出 0」，归一化之后 0 即每通道的中位数，相当于什么都不学。
# ============================================================================
import argparse
import csv
import os
import sys
from collections import OrderedDict

# 分类色按固定次序取用，不循环生成（配色规范的硬性要求）。这三色已通过配色校验：
# 最差相邻对的色觉缺陷区分度 9.2、常视区分度 27.6，均高于门槛。
# Fixed categorical order, validated: worst adjacent CVD dE 9.2, normal-vision dE 27.6.
SERIES_COLORS = ["#2a78d6", "#eb6834", "#1baf7a"]
INK_PRIMARY = "#1a1a19"
INK_MUTED = "#6b6a63"
GRID = "#e4e3dd"


REQUIRED = ["learningRate", "epoch", "trainLoss", "esLoss", "esLossBaseline"]


def load(path):
    """按学习率分组读入，每组按轮次排序。"""
    groups = OrderedDict()
    baseline = None
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        missing = [c for c in REQUIRED if c not in (reader.fieldnames or [])]
        if missing:
            # 最常见的错误是把**汇总** CSV 当成逐轮 CSV 传进来：前者每个组合一行、列名是
            # epochs（复数），后者每轮一行、列名是 epoch（单数）。直接说清楚，别让人对着
            # KeyError 猜。
            # The usual mistake is passing the summary CSV instead of the per-epoch one.
            hint = ""
            if "epochs" in (reader.fieldnames or []) and "epoch" in missing:
                hint = ("\n       看起来你传的是**汇总** CSV（每个组合一行）。本脚本要的是**逐轮** CSV"
                        "（每轮一行），由 syn-m3-grid.sh 的 --per-epoch-name 产出，"
                        "默认文件名形如 docs/<汇总名去掉扩展名>_per_epoch.csv。")
            raise SystemExit("ERROR: {} 缺少必需的列：{}{}".format(path, ", ".join(missing), hint))
        for row in reader:
            lr = float(row["learningRate"])
            groups.setdefault(lr, []).append(
                (int(row["epoch"]), float(row["trainLoss"]), float(row["esLoss"])))
            if baseline is None:
                baseline = float(row["esLossBaseline"])
    for lr in groups:
        groups[lr].sort(key=lambda r: r[0])
    return groups, baseline


def summarize(lr, rows, baseline):
    """打印一档的关键读数，对应裁决书第三节要求从曲线上读取的三个答案。"""
    es = [r[2] for r in rows]
    best_i = min(range(len(es)), key=lambda i: es[i])
    # 最长平台期：连续多少轮没有刷新历史最好值，而其后又出现了改善。耐心须长过它再加余量。
    longest_plateau, run, best_so_far = 0, 0, float("inf")
    for v in es:
        if v < best_so_far - 1e-12:
            best_so_far = v
            longest_plateau = max(longest_plateau, run)
            run = 0
        else:
            run += 1
    # 典型每轮改善幅度：相邻两轮之差的绝对值的中位数，用来判断改善阈值该用绝对值还是相对值。
    deltas = sorted(abs(es[i] - es[i - 1]) for i in range(1, len(es)))
    median_delta = deltas[len(deltas) // 2] if deltas else float("nan")
    # 末段统计：取最后十轮的均值与标准差。全程的波动幅度被前期陡降段主导，用它去比较两档收敛
    # 之后的高下会失真；真正该比较的是末段的水平，而标准差给出「两档之差是否大于自身抖动」的尺子。
    # Late-phase stats: the whole-run spread is dominated by the early descent and cannot tell whether
    # two converged runs actually differ; the last-10-epoch mean and sd can.
    tail = es[-10:] if len(es) >= 10 else es
    tail_mean = sum(tail) / len(tail)
    tail_sd = (sum((v - tail_mean) ** 2 for v in tail) / len(tail)) ** 0.5
    tail_deltas = sorted(abs(tail[i] - tail[i - 1]) for i in range(1, len(tail)))
    tail_median_delta = tail_deltas[len(tail_deltas) // 2] if tail_deltas else float("nan")
    ok = es[-1] <= 0.7 * baseline
    print("  学习率 {:<8g} 末轮误差 {:.6f}  最好在第 {} 轮（{:.6f}）  最长平台期 {} 轮".format(
        lr, es[-1], rows[best_i][0], es[best_i], longest_plateau))
    print("           末十轮 均值 {:.6f} ± 标准差 {:.6f}；典型每轮变动 全程 {:.8f} / 末段 {:.8f}；{}".format(
        tail_mean, tail_sd, median_delta, tail_median_delta,
        "低于基线七成，视为训练成功" if ok else "**未低于基线七成，视为未训练成功**"))
    return tail_mean, tail_sd


def main():
    ap = argparse.ArgumentParser(description="Plot the M3 learning-rate diagnosis curves.")
    ap.add_argument("csv")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("ERROR: 需要 matplotlib。请执行：python3 -m pip install matplotlib", file=sys.stderr)
        return 2

    groups, baseline = load(args.csv)
    if not groups:
        print("ERROR: CSV 里没有任何数据行。", file=sys.stderr)
        return 3
    out = args.out or os.path.splitext(args.csv)[0] + ".png"

    print("逐档读数（供裁决书第三节的三个问题使用）：")
    stats = OrderedDict()
    for lr, rows in groups.items():
        stats[lr] = summarize(lr, rows, baseline)
    print("平凡基线 {:.6f}；训练成功判据线（基线的七成）{:.6f}".format(baseline, 0.7 * baseline))
    # 两档之差若小于各自末段抖动之和，就说明这次实验分不开它们，不能假装分得开。
    # If two runs differ by less than their own late-phase jitter, the experiment cannot separate them.
    ok_lrs = [lr for lr in stats if stats[lr][0] <= 0.7 * baseline]
    if len(ok_lrs) >= 2:
        ok_lrs.sort(key=lambda x: stats[x][0])
        a, b = ok_lrs[0], ok_lrs[1]
        gap = abs(stats[a][0] - stats[b][0])
        jitter = stats[a][1] + stats[b][1]
        print("最好两档 {:g} 与 {:g}：末十轮均值相差 {:.6f}，两者抖动之和 {:.6f} —— {}".format(
            a, b, gap, jitter,
            "差距大于抖动，可以区分" if gap > jitter
            else "**差距小于抖动，本次实验分不开这两档**"))

    fig, (ax_es, ax_tr) = plt.subplots(1, 2, figsize=(13, 5.2), sharex=True)
    fig.patch.set_facecolor("#fcfcfb")

    for ax, title in ((ax_es, "Early-stopping loss (generalisation)"),
                      (ax_tr, "Training loss (fit)")):
        ax.set_facecolor("#fcfcfb")
        ax.set_title(title, color=INK_PRIMARY, fontsize=12, pad=12, loc="left")
        ax.set_xlabel("epoch", color=INK_MUTED, fontsize=10)
        ax.grid(True, color=GRID, linewidth=0.8)          # 网格退到背景 / recessive grid
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color(GRID)
        ax.tick_params(colors=INK_MUTED, labelsize=9)
    # 左联用对数纵轴：0.01 那一档的尖峰高达 0.86，线性轴会把已收敛的两档压成贴地的一条线，
    # 平台期与末段抖动都看不出来——而那正是要从曲线上读的东西。
    # Log y-axis: the spikes of the unstable run flatten the converged ones on a linear axis.
    ax_es.set_yscale("log")
    ax_es.set_ylabel("weighted MSE per element (log scale)", color=INK_MUTED, fontsize=10)
    # 右联是 DL4J 的内部 score，它在时间步上聚合，量级实测约为左联的四五十倍。
    # 两联只可比较**形状**，不可比较数值——否则会被误读成严重过拟合。
    # The right panel is DL4J's internal score on a different scale; compare shapes, not values.
    ax_tr.set_ylabel("DL4J internal score (different scale)", color=INK_MUTED, fontsize=10)

    # 两条水平参考线画成中性灰虚线：它们是参照，不是数据系列，不占分类色。
    # Reference lines are neutral, not series: they must not consume a categorical hue.
    ax_es.axhline(baseline, color=INK_MUTED, linewidth=1.4, linestyle="--")
    ax_es.axhline(0.7 * baseline, color=INK_MUTED, linewidth=1.4, linestyle=":")

    for i, (lr, rows) in enumerate(groups.items()):
        color = SERIES_COLORS[i % len(SERIES_COLORS)]
        xs = [r[0] for r in rows]
        ax_es.plot(xs, [r[2] for r in rows], color=color, linewidth=2, label="lr = {:g}".format(lr))
        ax_tr.plot(xs, [r[1] for r in rows], color=color, linewidth=2, label="lr = {:g}".format(lr))
        # 直接标注每条线的末端。配色校验对其中一色给出对比度警告，规范要求以可见标注作为补偿。
        # Direct end labels: required relief for the contrast warning on one hue.
        for ax, idx in ((ax_es, 2), (ax_tr, 1)):
            ax.annotate(" {:g}".format(lr), xy=(xs[-1], rows[-1][idx]), color=color,
                        fontsize=9, va="center", ha="left", annotation_clip=False)

    # 参考线的文字会压在曲线上，加一层与画布同色的衬底保证可读——这是渲染后目视检查出来的。
    # A surface-coloured halo keeps these readable where they cross the curves.
    halo = dict(facecolor="#fcfcfb", edgecolor="none", pad=1.5)
    # 再把文字抬离参考线几个点，使衬底不压在曲线上——衬底虽保证了可读性，却会抹掉一小段数据线。
    # Lift the text off the line so its halo does not erase a slice of the curves.
    lift = dict(xytext=(4, 5), textcoords="offset points")
    ax_es.annotate("baseline (predict median)", xy=(1, baseline), color=INK_MUTED,
                   fontsize=9, va="bottom", ha="left", bbox=halo, **lift)
    ax_es.annotate("0.7 x baseline = 'trained' threshold", xy=(1, 0.7 * baseline),
                   color=INK_MUTED, fontsize=9, va="bottom", ha="left", bbox=halo, **lift)
    # 两条系列以上必须有图例，identity 不能只靠颜色。
    ax_es.legend(frameon=False, fontsize=10, labelcolor=INK_PRIMARY, loc="upper right")

    fig.suptitle("M3 learning-rate diagnosis — device E, hidden 60, window 60, batch 64",
                 color=INK_PRIMARY, fontsize=13, x=0.06, ha="left")
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(out, dpi=160, facecolor=fig.get_facecolor())
    print("图已写出：{}".format(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
