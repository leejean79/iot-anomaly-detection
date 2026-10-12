#!/usr/bin/env python3
# ============================================================================
# m3_injection_recall.py
# V-M3-5 注入召回表与双通道对注入的响应对比。
# V-M3-5 injection recall table and the dual-channel response to each injection.
#
# 口径（2026-10-09 裁决第二节第 2 条，设计会话 10-10 答复定稿）/ definitions (ruling of 2026-10-09, item 2,
# finalised by the design session's reply of 2026-10-10)：
#   - 检出：区间内出现超阈才算。点通道逐轮评分，只计时间戳落在注入区间 [开始, 结束) 内的轮；上下文通道的一个窗口
#     计入区间内，当且仅当窗口起点早于注入结束、且窗口终点晚于注入开始（窗口与区间重叠）。窗口起点按
#     「窗口终点 − 600 秒」估算（60 轮、约每 10 秒一轮）。
#   - 首次检出延迟：首个计入的超阈记录的窗口终点减注入开始时刻（窗口终点是评分真正产生的时刻）。
#   - 卡死期间点通道的检出记为「附带，跨通道不一致」：卡死只冻结一个通道，冻结值本身是正常值，点通道报警只能来自
#     其余通道走开后五维组合失配，不能作为点通道承担卡死场景的依据。
#   - 点通道的「超阈」：注入设备的离群记录，且该轮在它到达的滑动步内被判离群（窗口末 − 60 秒 ≤ 轮时间戳
#     < 窗口末）。只取到达滑动步，是为了不把注入开始前就已在窗口里的离群点（每个滑动步都会重发）算作检出。
#   - 结束时跳回响应单列，不计入检出：点通道取 [结束, 结束 + 3600 秒) 内的离群轮；上下文通道只取窗口起点不早于
#     注入结束、且在结束后一个窗长（600 秒）之内的窗口。数据回到基线本身是一次突变，这部分报警证明的是「结束被
#     看见」，不是「异常被看见」。
#   - 背景：同一设备在注入所在日期、所有注入区间（前后各加一个窗长）之外的超阈比例；据此给出「偶然检出概率」
#     = 1 − (1 − 背景比例)^(检出时段内的窗口或轮数)，用来区分真检出与碰巧落在区间里的误报。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac 或任何有 Python 3.9+、matplotlib 的机器，仓库根目录。
# 2. 调用命令：
#      python3 deploy/scripts/m3_injection_recall.py \
#          --truth docs/m3_inject/inject-truth.csv --scores docs/m3_inject/scores.jsonl \
#          --out-dir docs/m3_inject/recall
#    可选：--plan docs/m3_inject_plan.csv（带上时报告里显示档位名称，如「2xP10P90/2轮」）。
# 3. 前置条件：注入运行已收集（syn-m3-march-collect.sh 拉回了 scores.jsonl 与 inject-truth.csv）。
# 4. 期望产出：--out-dir 下 recall.md（召回表、背景、三条预言的核对数据）、recall.csv（逐条注入）、
#    figs/inject_<序号>_<类型>.png（每条注入一幅：上为点通道每 10 分钟离群轮数，下为上下文通道主分）。
# 5. 失败兜底：真值文件或评分文件不存在时退出码 2；评分里没有注入设备的记录时退出码 3。
# ============================================================================
import argparse
import collections
import csv
import json
import os
import sys
from datetime import datetime, timezone

W_POINT, W_CTX, SLIDE = 3600, 600, 60
SURFACE, INK, INK2, GRID, BLUE, ORANGE = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df", "#2a78d6", "#d9822b"
iso = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%m-%d %H:%M:%S")
day = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--truth", required=True)
    ap.add_argument("--scores", required=True)
    ap.add_argument("--plan", default="")
    ap.add_argument("--out-dir", required=True)
    a = ap.parse_args()
    for p in (a.truth, a.scores):
        if not os.path.isfile(p):
            print("ERROR: 找不到 %s" % p, file=sys.stderr)
            return 2
    truth = [dict(r, start=int(r["start_ts"]), end=int(r["end_ts"]))
             for r in csv.DictReader(open(a.truth, encoding="utf-8"))]
    level = {}
    if a.plan and os.path.isfile(a.plan):
        for r in csv.DictReader(open(a.plan, encoding="utf-8")):
            level[(r["channel"], int(r["start_ts"]))] = r["level"]
    devices = {t["device"] for t in truth}

    point, ctx = collections.defaultdict(list), collections.defaultdict(list)   # 设备 → [(时刻, 是否超阈, 值)]
    for line in open(a.scores, encoding="utf-8", errors="replace"):
        try:
            o = json.loads(line)
        except ValueError:
            continue
        d = o.get("device")
        if d not in devices:
            continue
        if o.get("channel") == "m2_point":
            if o["windowEnd"] - SLIDE <= o["roundTs"] < o["windowEnd"]:
                point[d].append((o["roundTs"], o["windowEnd"]))
        elif o.get("channel") == "m3_context":
            ctx[d].append((o["windowEnd"], bool(o["aboveThreshold"]), float(o["mainScore"])))
    if not any(ctx[d] for d in devices):
        print("ERROR: 评分里没有注入设备的上下文通道记录", file=sys.stderr)
        return 3
    for d in devices:
        point[d].sort()
        ctx[d].sort()

    # 背景：注入日期内、所有注入区间（前后各加一个窗长）之外 / background outside all injection spans
    # 区间左闭右开：结束时刻恰在零点时不属于次日，所以取 end - 1 所在的日期。
    # Intervals are half-open: an injection ending exactly at midnight does not touch the next day.
    inj_days = {day(t["start"]) for t in truth} | {day(t["end"] - 1) for t in truth}
    def outside(dv, ts, w):
        return all(not (t["start"] - w <= ts < t["end"] + w) for t in truth if t["device"] == dv)
    bg = {}
    for dv in devices:
        cw = [x for x in ctx[dv] if day(x[0]) in inj_days and outside(dv, x[0], W_CTX)]
        bg_ctx = sum(1 for x in cw if x[1]) / len(cw) if cw else 0.0
        # 点通道背景按轮计：背景时段内离群的到达轮数 ÷ 背景时段内的轮数（按每 10 秒一轮估算）
        bg_secs = sum(86400 for _ in inj_days) - sum(t["end"] - t["start"] + 2 * W_POINT for t in truth if t["device"] == dv)
        pw = [x for x in point[dv] if day(x[0]) in inj_days and outside(dv, x[0], W_POINT)]
        bg_pt = len(pw) / max(1, bg_secs / 10.0)
        bg[dv] = (bg_ctx, bg_pt, len(cw))

    rows = []
    for i, t in enumerate(sorted(truth, key=lambda r: r["start"]), 1):
        dv, s0, s1 = t["device"], t["start"], t["end"]
        p_hits = [we for rt, we in point[dv] if s0 <= rt < s1]                          # 区间内 / inside
        p_jump = [we for rt, we in point[dv] if s1 <= rt < s1 + W_POINT]                # 结束时跳回 / end jump
        in_win = lambda we: we - W_CTX < s1 and we > s0                                 # 窗口与区间重叠 / overlap
        c_hits = [we for we, hit, _ in ctx[dv] if hit and in_win(we)]
        n_ctx = sum(1 for we, _, _ in ctx[dv] if in_win(we))
        jp_win = lambda we: s1 <= we - W_CTX < s1 + W_CTX                               # 跳回列 / end-jump windows
        c_jump = [we for we, hit, _ in ctx[dv] if hit and jp_win(we)]
        n_jump = sum(1 for we, _, _ in ctx[dv] if jp_win(we))
        n_rounds = max(1.0, (s1 - s0) / 10.0)
        bctx, bpt, _ = bg[dv]
        rows.append({
            "no": i, "device": dv, "channel": t["channel"], "type": t["type"],
            "level": level.get((t["channel"], s0), "%.4g" % float(t["magnitude"])),
            "start_utc": iso(s0), "duration_sec": s1 - s0,
            "point_detected": ("是（附带，跨通道不一致）" if t["type"] == "stuck" else "是") if p_hits else "否",
            "point_delay_sec": (min(p_hits) - s0) if p_hits else "",
            "point_in": len(p_hits),
            "point_jump": len(p_jump),
            "point_jump_delay_sec": (min(p_jump) - s1) if p_jump else "",
            "point_chance": "%.3f" % (1 - (1 - bpt) ** n_rounds),
            "ctx_detected": "是" if c_hits else "否",
            "ctx_delay_sec": (min(c_hits) - s0) if c_hits else "",
            "ctx_hits": "%d/%d" % (len(c_hits), n_ctx),
            "ctx_chance": "%.3f" % (1 - (1 - bctx) ** max(n_ctx, 1)),
            "ctx_jump": "%d/%d" % (len(c_jump), n_jump),
        })

    os.makedirs(os.path.join(a.out_dir, "figs"), exist_ok=True)
    with open(os.path.join(a.out_dir, "recall.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    fmt_delay = lambda v: "-" if v == "" else ("%d 秒" % v if v < 600 else "%.1f 分钟" % (v / 60.0))
    L = ["# V-M3-5 注入召回表", "",
         "口径（2026-10-09 裁决第二节第 2 条，设计会话 10-10 答复定稿）：区间内出现超阈即为检出。点通道逐轮评分，只计"
         "时间戳在注入区间内、且在到达滑动步内被判离群的轮；上下文通道的窗口与注入区间重叠（窗口起点早于注入结束、窗口"
         "终点晚于注入开始，起点按终点 − 600 秒估算）即计入。首次检出延迟 = 首个计入记录的窗口终点减注入开始时刻。"
         "「结束时跳回」单列、不计入检出：点通道取注入结束后 3600 秒内的离群轮（并给出首个离群距结束的时间），上下文通道"
         "取起点不早于注入结束、且在结束后 600 秒之内的窗口。「偶然检出概率」按同一设备在注入日期、注入区间以外的背景"
         "超阈比例估算。", "",
         "| 序号 | 通道 | 类型 | 档位 | 开始（UTC） | 时长 | 点通道检出 | 点通道延迟 | 点通道区间内离群轮 | 点通道偶然概率 | 上下文检出 | 上下文延迟 | 上下文超阈窗/窗 | 上下文偶然概率 | 结束时跳回：点通道（离群轮/距结束） | 结束时跳回：上下文（超阈窗/窗） |",
         "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for r in rows:
        L.append("| %d | %s | %s | %s | %s | %s | %s | %s | %d | %s | %s | %s | %s | %s | %d / %s | %s |" % (
            r["no"], r["channel"], r["type"], r["level"], r["start_utc"],
            fmt_delay(r["duration_sec"]), r["point_detected"], fmt_delay(r["point_delay_sec"]),
            r["point_in"], r["point_chance"],
            r["ctx_detected"], fmt_delay(r["ctx_delay_sec"]), r["ctx_hits"], r["ctx_chance"],
            r["point_jump"], fmt_delay(r["point_jump_delay_sec"]), r["ctx_jump"]))
    if any(r["type"] == "stuck" and r["point_in"] for r in rows):
        L += ["", "表注：卡死期间点通道的检出记为「附带，跨通道不一致」。卡死只冻结一个通道，冻结值本身是正常值；其余四个通道"
              "继续随环境变化，真实值走开之后，五维向量变成一个冻结通道与其他通道不匹配的组合，在一小时局部密度里没有足够"
              "的邻居，于是被点通道标为离群。能否发生取决于卡死期间其余通道走了多远，不能作为点通道承担卡死场景的依据，"
              "卡死由数据接入的重复值游程守卫承担（开发决策第十条）。"]
    L += ["", "## 背景", ""]
    for dv, (bctx, bpt, n) in sorted(bg.items()):
        L.append("- 设备 %s：上下文通道背景超阈比例 %.2f%%（%d 个窗口）；点通道背景离群轮比例 %.3f%%。"
                 % (dv, 100 * bctx, n, 100 * bpt))
    L += ["", "## 三条预言的核对数据（判定由报告给出）", ""]
    by = lambda typ: [r for r in rows if r["type"] == typ and r["channel"] == truth[0]["channel"]]
    st = by("stuck")
    L.append("1. 卡死：点通道检出 %d/%d 次（附带，跨通道不一致），上下文通道检出 %d/%d 次。预言为点通道不可见、"
             "上下文通道可见。" % (sum(r["point_detected"] != "否" for r in st), len(st),
                                sum(r["ctx_detected"] == "是" for r in st), len(st)))
    rp = by("ramp")
    L.append("2. 爬坡：各档首次检出延迟（点通道 / 上下文通道）：%s。预言为上下文通道早于点通道。" % "；".join(
        "%s %s / %s" % (r["level"], fmt_delay(r["point_delay_sec"]), fmt_delay(r["ctx_delay_sec"])) for r in rp))
    sp = by("spike")
    L.append("3. 尖峰：点通道检出 %s；上下文通道检出 %s（按档位顺序）。预言为点通道即时可见，一轮尖峰在上下文通道上"
             "可能不可见。" % ("、".join(r["point_detected"] for r in sp), "、".join(r["ctx_detected"] for r in sp)))
    text = "\n".join(L) + "\n"
    open(os.path.join(a.out_dir, "recall.md"), "w", encoding="utf-8").write(text)
    print(text)
    figures(a, rows, truth, point, ctx)
    return 0


def figures(a, rows, truth, point, ctx):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.size": 9, "axes.edgecolor": GRID, "axes.labelcolor": INK2,
                         "xtick.color": INK2, "ytick.color": INK2, "axes.titlecolor": INK,
                         "figure.facecolor": SURFACE, "axes.facecolor": SURFACE})
    dt = lambda t: datetime.fromtimestamp(t, timezone.utc)
    for r, t in zip(rows, sorted(truth, key=lambda x: x["start"])):
        dv, s0, s1 = t["device"], t["start"], t["end"]
        lo, hi = s0 - 7200, s1 + W_POINT + 3600
        # 点通道：每 10 分钟内在到达滑动步被判离群的轮数 / point: arrival-slide outlier rounds per 10 min
        bins = collections.Counter((rt - lo) // 600 for rt, _ in point[dv] if lo <= rt < hi)
        nb = (hi - lo) // 600 + 1
        xs = [dt(lo + k * 600) for k in range(nb)]
        cs = [(dt(we), sc) for we, _, sc in ctx[dv] if lo <= we < hi]
        fig, axes = plt.subplots(2, 1, figsize=(10, 4.6), sharex=True)
        for ax in axes:
            ax.axvspan(dt(s0), dt(s1), color=GRID, alpha=0.8, lw=0)
            ax.grid(axis="y", color=GRID, lw=0.6)
            ax.set_axisbelow(True)
            for sp in ("top", "right"):
                ax.spines[sp].set_visible(False)
        axes[0].bar(xs, [bins.get(k, 0) for k in range(nb)], width=600 / 86400 * 0.85, color=ORANGE, align="edge")
        axes[0].set_ylabel("Point: outlier\nrounds / 10 min", rotation=0, ha="right", va="center")
        if cs:
            axes[1].plot([c[0] for c in cs], [c[1] for c in cs], color=BLUE, lw=1.5, marker="o", ms=3)
        axes[1].axhline(2.22, color=INK2, lw=0.8, ls="--")
        axes[1].set_ylabel("Context: main\nscore (z)", rotation=0, ha="right", va="center")
        axes[0].set_title("#%d %s %s %s %s (grey = injection; dashed = threshold 2.22)"
                          % (r["no"], dv, t["channel"], t["type"], r["level"].replace("轮", " rounds")), loc="left")
        axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
        fig.tight_layout()
        fig.savefig(os.path.join(a.out_dir, "figs", "inject_%02d_%s.png" % (r["no"], t["type"])), dpi=130)
        plt.close(fig)


if __name__ == "__main__":
    sys.exit(main())
