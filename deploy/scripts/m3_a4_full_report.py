#!/usr/bin/env python3
"""
m3_a4_full_report.py —— 补遗四全量重放的汇总：A4-1（重放完整性与检查点）、A4-3（缓冲与水位线）、A4-5（训练时长与性能），
并把 A4-2（逐位等值）与 A4-4（守卫在线核对）的结论并进同一份报告，与 10 月 9 日注入运行并排。
Summary of the addendum-4 full replay (A4-1, A4-3, A4-5), merging the A4-2 and A4-4 results, side by side with the
injection run.

判据（2026-10-16 裁决，开发设计日志 0.84 版第一节）：训练期间每一次检查点的端到端时长不超过检查点间隔三十秒；零失败、
零重启；并给出训练期与非训练期的时长分布（中位数、最大值）及注入运行同期的对照。

---------------------------- 脚本交付五要素 ----------------------------
1. 执行环境 / Environment: master 或本地 Mac，仓库根目录，python3（只用标准库）。通常由 syn-m3-a4-full.sh 在 master 上调用。
2. 调用命令 / Invocation:
     python3 deploy/scripts/m3_a4_full_report.py --run docs/m3_a4/full --ref docs/m3_inject --out docs/m3_a4/full/a4_full_report.md
   可选 --ckpt-interval-sec 30（判据上限）、--buffer-cap 77760（缓冲上限，轮）、--tz-offset-hours（采样所在机器的时区偏移，默认本机）。
3. 前置条件 / Preconditions: --run 目录里有 m3_tm_log.txt、ckpt_timeline.csv、replay_verify.txt，最好还有 equality.md、
   guard_check.md、perf/perf_summary.md；--ref 目录（注入运行）里有 m3_tm_log.txt、ckpt_timeline.csv、perf/perf_summary.md。
4. 期望产出 / Expected output: --out 的 Markdown，按 A4-1 至 A4-5 分节，每节给出判定。
5. 失败兜底 / Failure fallback: 缺少的输入在对应小节标「无数据」；退出码 0 表示报告已生成（判定写在报告里），2 表示目录不存在。
"""
import argparse
import os
import statistics
import sys
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import m3_a4_stepc_report as base  # noqa: E402  共用日志与时间线的解析 / shared parsers


def ckpt_split(rows, dev):
    """把检查点按「训练与排空期间」与「其余时段」分开（排空期间缓冲仍非空，属于训练造成的保持时段）。
    Split checkpoints into the training-and-draining window and the rest."""
    starts = [d["enter"] for d in dev.values() if "enter" in d]
    ends = [(d["collected"] + timedelta(seconds=d.get("drainsec", 0.0))) if "collected" in d
            else d.get("trained") for d in dev.values()]
    ends = [e for e in ends if e]
    t0, t1 = (min(starts), max(ends)) if starts and ends else (None, None)
    seen, train, other = set(), [], []
    for r in rows:
        cid, e2e, at = r.get("last_completed_id"), r.get("last_completed_e2e_sec"), r.get("last_completed_at")
        if not cid or not e2e or cid in seen:
            continue
        seen.add(cid)
        when = base.ts(r["wall_clock"][:10] + " " + at) if at else base.ts(r["wall_clock"])
        when -= timedelta(seconds=float(e2e))         # 按触发时刻归类 / classify by trigger time
        (train if t0 and t0 <= when <= t1 else other).append(float(e2e))
    failed = max((int(r["failed"]) for r in rows if r.get("failed", "").isdigit()), default=0)
    restarts = max((int(r["num_restarts"]) for r in rows if r.get("num_restarts", "").isdigit()), default=0)
    return train, other, failed, restarts


def dist(xs):
    return ("%d 次，中位数 %.1f 秒，最大 %.1f 秒" % (len(xs), statistics.median(xs), max(xs))) if xs else "无"


def read(path):
    return open(path, encoding="utf-8").read() if os.path.exists(path) else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--ref", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--ckpt-interval-sec", type=float, default=30.0)
    ap.add_argument("--buffer-cap", type=int, default=77760)
    ap.add_argument("--tz-offset-hours", type=float, default=None)
    a = ap.parse_args()
    for d in (a.run, a.ref):
        if not os.path.isdir(d):
            print("ERROR: 目录不存在：%s" % d, file=sys.stderr)
            return 2
    if a.tz_offset_hours is not None:
        base.LOCAL_OFFSET = timedelta(hours=a.tz_offset_hours)
    dev, hold, threads = base.parse_log(os.path.join(a.run, "m3_tm_log.txt"))
    rdev, _, _ = base.parse_log(os.path.join(a.ref, "m3_tm_log.txt"))
    L = ["# 补遗四全量重放汇总（A4-1 至 A4-5）", "",
         "本次运行：`%s`；对照：10 月 9 日注入运行 `%s`。训练线程数：%s。" % (a.run, a.ref, threads or "?"), ""]

    # ---- A4-1 ----
    L += ["## A4-1 重放完整性与检查点", ""]
    rv = read(os.path.join(a.run, "replay_verify.txt"))
    if rv:
        lines = [x.strip() for x in rv.splitlines() if x.strip().startswith("[断言") or "七条断言" in x]
        L += ["重放完整性七条断言（`replay_verify.txt`）：", ""] + ["- " + x for x in lines] + [""]
    else:
        L += ["没有 `replay_verify.txt`。", ""]
    tr, ot, failed, restarts = ckpt_split(base.read_ckpt(os.path.join(a.run, "ckpt_timeline.csv")), dev)
    rtr, rot, _, _ = ckpt_split(base.read_ckpt(os.path.join(a.ref, "ckpt_timeline.csv")), rdev)
    over = [x for x in tr if x > a.ckpt_interval_sec]
    ok1 = bool(tr) and not over and failed == 0 and restarts == 0
    L += ["| 运行 | 训练与排空期间 | 其余时段 |", "| --- | --- | --- |",
          "| 本次 | %s | %s |" % (dist(tr), dist(ot)),
          "| 注入运行 | %s | %s |" % (dist(rtr), dist(rot)), "",
          "检查点失败 %d 次，作业重启 %d 次；训练与排空期间超过检查点间隔 %.0f 秒的检查点 %d 次。判定：%s。" % (
              failed, restarts, a.ckpt_interval_sec, len(over), "通过" if ok1 else "**未通过**"), ""]

    # ---- A4-2 ----
    eq = read(os.path.join(a.run, "equality.md"))
    L += ["## A4-2 逐位等值", ""]
    if eq:
        verdict = "全部相同" if "**全部相同。**" in eq else "**存在不同**"
        L += ["详见 `equality.md`。判定：%s。" % verdict, ""]
        prev = read(os.path.join(a.run, "equality_prev.md"))   # 修正后的重跑：再与上一次全量重放比较（0.86 版裁决）
        if prev:
            L += ["与上一次全量重放（修正前，作业 `9218af22…`）的比较详见 `equality_prev.md`。判定：%s。" % (
                "全部相同" if "**全部相同。**" in prev else "**存在不同**"), ""]
    else:
        L += ["没有 `equality.md`。", ""]

    # ---- A4-3 ----
    L += ["## A4-3 缓冲与水位线", "",
          "| 设备 | 子任务 | 收回时缓冲（轮，即峰值） | 占上限 %d 轮 | 排空（秒） | 提交到收回（秒） | 输出水位线保持（秒） |" % a.buffer_cap,
          "| --- | --- | --- | --- | --- | --- | --- |"]
    peak_pct = 0.0
    for name in sorted(dev):
        x = dev[name]
        span = (x["collected"] - x["enter"]).total_seconds() if "collected" in x and "enter" in x else None
        b = x.get("buffered", 0)
        peak_pct = max(peak_pct, 100.0 * b / a.buffer_cap)
        L.append("| %s | %s | %d | %.1f%% | %s | %s | %s |" % (
            name, x.get("subtask", "-"), b, 100.0 * b / a.buffer_cap, "%.1f" % x["drainsec"] if "drainsec" in x else "-",
            "%.0f" % span if span is not None else "-", "、".join("%.1f" % h for h in hold.get(x.get("subtask"), [])) or "-"))
    log = read(os.path.join(a.run, "m3_tm_log.txt")) or ""
    full = log.count("training buffer full")
    L += ["", "缓冲溢出上报 %d 次；逐台缓冲峰值最高占上限 %.1f%%（倍速公式按二倍余量预期不超过 50%%）。判定：%s。" % (
        full, peak_pct, "通过" if full == 0 else "**未通过**"), ""]

    # ---- A4-4 ----
    gc = read(os.path.join(a.run, "guard_check.md"))
    L += ["## A4-4 守卫在线核对", ""]
    L += (["详见 `guard_check.md`。判定：%s。" % ("通过" if "**通过。**" in gc else "**未通过**"), ""] if gc
          else ["没有 `guard_check.md`。", ""])

    # ---- A4-5 ----
    L += ["## A4-5 性能", "", "逐台训练时长（含排队等待），与注入运行（同步训练、最多六台同时）并排：", "",
          "| 设备 | 排队等待（秒） | 训练秒数（本次） | 训练秒数（注入运行） | 轮数（本次 / 注入运行） |",
          "| --- | --- | --- | --- | --- |"]
    for name in sorted(set(dev) | set(rdev)):
        x, y = dev.get(name, {}), rdev.get(name, {})
        L.append("| %s | %s | %s | %s | %s / %s |" % (
            name, "%.1f" % x["wait"] if "wait" in x else "-", "%.1f" % x["trainsec"] if "trainsec" in x else "-",
            "%.1f" % y["trainsec"] if "trainsec" in y else "-", x.get("epochs", "-"), y.get("epochs", "-")))
    for title, p in (("本次", os.path.join(a.run, "perf", "perf_summary.md")),
                     ("注入运行", os.path.join(a.ref, "perf", "perf_summary.md"))):
        s = read(p)
        L += ["", "### 推理时延、转发延迟与任务管理器内存（%s，`%s`）" % (title, p), ""]
        L += [s.strip() if s else "无数据。", ""]

    text = "\n".join(L) + "\n"
    with open(a.out, "w", encoding="utf-8") as f:
        f.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
