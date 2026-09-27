#!/usr/bin/env python3
# ============================================================================
# m3_grid_final.py
# 步骤 D 收尾：把补跑的格子并入网格汇总，按 2026-09-27 裁决书第四节的跨设备规则选隐单元数，
# 并画 V-M3-3 图。
# Step D close-out: merge the re-run cells into the grid summary, apply the ruling's cross-device
# rule to pick the hidden size, and draw the V-M3-3 figure.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac 或任意装有 python3 与 matplotlib 的机器，工作目录为仓库根目录。
# 2. 调用命令：
#      python3 deploy/scripts/m3_grid_final.py \
#          --grid docs/m3_grid.csv --grid-cap 100 \
#          --rerun docs/m3_grid_rerun.csv --rerun-cap 300 \
#          --out docs/figures/m3_v3_grid.png
#    --rerun 可省略（只画原网格）；--grid-cap / --rerun-cap 是各自运行的 epoch 上限，用于标出
#    被上限截断的格子。
# 3. 前置条件：两份 CSV 都是 syn-m3-grid.sh 写出的汇总格式（含 device、hiddenSize、
#    windowLength、epochs、esLossLast10、esSdLast10 列）。
# 4. 期望产出：终端打印合并后的窗长六十一列、每台设备的相对倍数、各隐单元数的最差倍数与选定值；
#    在 --out 处生成三面板的 PNG。
# 5. 失败兜底：缺列时报出缺哪一列并退出码 2；窗长六十一列不满九格时报出缺哪几格并退出码 3。
# ============================================================================
import argparse
import csv
import os
import sys

REQUIRED = ["device", "hiddenSize", "windowLength", "epochs", "esLossLast10", "esSdLast10"]
TIE = 1.10          # 与首选相差不超过百分之十视为并列 / within 10% of the first choice is a tie
WINDOW = 60         # 裁决书第二节：窗长固定为六十 / window fixed at 60 by the ruling


def load(path, cap):
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    missing = [c for c in REQUIRED if rows and c not in rows[0]]
    if not rows or missing:
        sys.exit(f"ERROR: {path} 缺少列 {missing or REQUIRED}，这不是网格汇总 CSV。")  # exit 1
    out = {}
    for r in rows:
        key = (r["device"], int(r["hiddenSize"]), int(r["windowLength"]))
        out[key] = {"last10": float(r["esLossLast10"]), "sd": float(r["esSdLast10"]),
                    "epochs": int(r["epochs"]), "capped": int(r["epochs"]) >= cap,
                    "source": os.path.basename(path)}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grid", required=True)
    ap.add_argument("--grid-cap", type=int, default=100)
    ap.add_argument("--rerun")
    ap.add_argument("--rerun-cap", type=int, default=300)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    cells = load(a.grid, a.grid_cap)
    if a.rerun:
        for key, v in load(a.rerun, a.rerun_cap).items():
            old = cells.get(key)
            if old:
                print(f"补跑替换 {key}：{old['epochs']} 轮 {old['last10']:.6f} → "
                      f"{v['epochs']} 轮 {v['last10']:.6f}")
            cells[key] = v                          # 补跑结果取代原格 / the re-run replaces the cell

    devices = sorted({k[0] for k in cells})
    hiddens = sorted({k[1] for k in cells})
    windows = sorted({k[2] for k in cells})
    col = {k: v for k, v in cells.items() if k[2] == WINDOW}
    lacking = [(d, h) for d in devices for h in hiddens if (d, h, WINDOW) not in col]
    if lacking:
        print(f"ERROR: 窗长 {WINDOW} 一列缺格 {lacking}", file=sys.stderr)
        sys.exit(3)

    # 第四节第 1 步：每台设备内除以该设备的最优值 / step 1: divide by each device's best
    ratio = {}
    print(f"\n窗长 {WINDOW} 一列（末十轮均值 ± 标准差，轮数；星号为被上限截断）：")
    for d in devices:
        best = min(col[(d, h, WINDOW)]["last10"] for h in hiddens)
        for h in hiddens:
            c = col[(d, h, WINDOW)]
            ratio[(d, h)] = c["last10"] / best
            print(f"  {d} 隐单元 {h:>3}：{c['last10']:.6f} ± {c['sd']:.6f}，{c['epochs']} 轮"
                  f"{'*' if c['capped'] else ' '}  相对倍数 {ratio[(d, h)]:.3f}")

    # 第 2 步：每个隐单元数取三台设备中的最差倍数 / step 2: worst multiple across devices
    worst = {h: max(ratio[(d, h)] for d in devices) for h in hiddens}
    first = min(hiddens, key=lambda h: worst[h])
    # 第 3 步：与首选相差不超过 10% 为并列，取参数更少者 / step 3: tie band, fewer parameters wins
    tied = [h for h in hiddens if worst[h] <= worst[first] * TIE]
    pick = min(tied)
    print("\n各隐单元数的最差倍数：")
    for h in hiddens:
        print(f"  隐单元 {h:>3}：{worst[h]:.3f}{'  并列' if h in tied else ''}")
    print(f"首选（最差倍数最小）：隐单元 {first}；并列带 ≤ {worst[first] * TIE:.3f}；"
          f"按参数更少取 → 隐单元 {pick}、窗长 {WINDOW}")
    capped = sorted(k for k, v in col.items() if v["capped"])
    if capped:
        print(f"注意：窗长 {WINDOW} 一列仍有被上限截断的格子 {capped}，选型结论须附此保留。")

    draw(cells, devices, hiddens, windows, a.out)


def draw(cells, devices, hiddens, windows, out):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    # 参考调色板前三个分类色，按固定顺序分配给窗长 / first three categorical slots, fixed order
    colors = {w: c for w, c in zip(windows, ["#2a78d6", "#eb6834", "#1baf7a"])}
    fig, axes = plt.subplots(1, len(devices), figsize=(4.2 * len(devices), 4.2))
    for ax, d in zip(axes, devices):
        for w in windows:
            # 三个窗长在横轴上错开两个单位，避免同一隐单元数的点与误差棒重叠
            # Offset the windows by 2 units so points at the same hidden size do not overlap
            dx = 2.0 * (windows.index(w) - (len(windows) - 1) / 2)
            pts = [(h + dx, cells[(d, h, w)]) for h in hiddens if (d, h, w) in cells]
            xs = [h for h, _ in pts]
            ys = [c["last10"] for _, c in pts]
            bold = w == WINDOW
            ax.errorbar(xs, ys, yerr=[c["sd"] for _, c in pts], color=colors[w],
                        linewidth=3.0 if bold else 1.5, capsize=3, zorder=3 if bold else 2,
                        label=f"window {w}" + (" (used for selection)" if bold else ""))
            for h, c in pts:
                # 被上限截断的格子画空心，其值仍在下降，偏高估 / hollow = cut off by the cap
                ax.plot(h, c["last10"], "o", markersize=8, color=colors[w],
                        markerfacecolor="white" if c["capped"] else colors[w],
                        markeredgewidth=2, zorder=4)
        ax.set_title(f"device {d}", loc="left")
        ax.set_xticks(hiddens)
        ax.set_xlabel("hidden units")
        ax.grid(axis="y", color="#dddddd", linewidth=0.8)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
    axes[0].set_ylabel("early-stop loss, last-10 mean ± sd")
    axes[-1].legend(frameon=False, fontsize=9)
    # 图中文字用英文：matplotlib 默认字体没有中文字形，与其余绘图脚本一致。
    # Labels in English: matplotlib's default fonts lack CJK glyphs, as in the other plot scripts.
    fig.suptitle("V-M3-3: hidden units vs early-stop loss "
                 "(hollow = cut off by the epoch cap; each panel has its own y-axis)", fontsize=11)
    fig.tight_layout()
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    fig.savefig(out, dpi=150)
    print(f"\n图已写出：{out}")


if __name__ == "__main__":
    main()
