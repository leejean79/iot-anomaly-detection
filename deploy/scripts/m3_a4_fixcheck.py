#!/usr/bin/env python3
"""
m3_a4_fixcheck.py —— 排空缺陷修正后的步骤 C 验证（2026-10-17 裁决，开发设计日志 0.86 版第二节第 3 条第 2 步）。
Step-C verification of the drain fix (ruling 0.86, section 2.3 step 2).

四项核对：
  1. 排空期间的每一次检查点（触发到完成的区间与某台设备的排空区间重叠）端到端不超过 1 秒；
  2. 排空中的子任务，其输入水位线与其他子任务中最快者之差（折成墙钟秒，即事件时间差 ÷ 倍速）不超过一个采样间隔；
  3. 八台训练产物与全部评分窗与步骤 C 的 t2 逐位相同（读 m3_a4_equality.py 生成的 Markdown 的总判定）；
  4. 监测主题中训练态状态快照的条数等于设计值：每台每 30 秒一条，即 floor(提交到收回 ÷ 30)，容差一条。
第 1、2 项若没有任何检查点或采样落在排空区间内，判为「未覆盖」（不是通过）。
Four checks: checkpoints overlapping a drain finish within 1 s; the draining subtask's input watermark stays within one
sampling interval of the fastest subtask; bit-exact equality with t2; one training status snapshot per device per 30 s.

---------------------------- 脚本交付五要素 ----------------------------
1. 执行环境 / Environment: master 或本地 Mac，仓库根目录，python3（只用标准库）。通常由 syn-m3-a4-stepc.sh 的 fix 运行调用。
2. 调用命令 / Invocation:
     python3 deploy/scripts/m3_a4_fixcheck.py --run docs/m3_a4/stepc_fix --speed 400 \\
         --equality docs/m3_a4/stepc_fix/equality_t2.md --out docs/m3_a4/stepc_fix/fix_check.md
   可选：--tz-offset-hours 8（在时区不是 master 所在时区的机器上重算时指定）、--sample-sec 10、--poll-sec 30、
   --max-ckpt-sec 1.0、--skip-snapshots（第四项不判定，只用于没有监测转储的反向对照，例如修正前的全量重放）。
3. 前置条件 / Preconditions: --run 目录里有 m3_tm_log.txt、ckpt_history.csv（缺失时退回 ckpt_timeline.csv）、
   wm_buffer.csv、status_snapshots.csv（编排在删除监测转储之前从 master 统计），以及 --equality 指定的文件。
4. 期望产出 / Expected output: --out 指定的 Markdown（四项判定与逐台明细），终端打印同样内容。
5. 失败兜底 / Failure fallback: 退出码 0 表示四项全部通过；1 表示有未通过或未覆盖的项（表中标出）；2 表示输入缺失。
"""
import argparse
import csv
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import m3_a4_stepc_report as base  # noqa: E402  共用日志解析 / shared log parser


def t(s):
    s = s.strip()
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f" if "." in s else "%Y-%m-%d %H:%M:%S")


def checkpoints(run):
    """[(编号, 触发, 完成, 端到端秒)]；优先用不漏的 ckpt_history.csv / checkpoints with trigger and completion."""
    out = []
    hist = os.path.join(run, "ckpt_history.csv")
    if os.path.exists(hist):
        with open(hist, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r["status"] == "COMPLETED" and r["trigger_time"] and r["end_to_end_sec"]:
                    e2e = float(r["end_to_end_sec"])
                    trig = t(r["trigger_time"])
                    out.append((r["id"], trig, trig + timedelta(seconds=e2e), e2e))
        return out, "ckpt_history.csv"
    seen = set()
    for r in base.read_ckpt(os.path.join(run, "ckpt_timeline.csv")):
        cid, e2e, at = r.get("last_completed_id"), r.get("last_completed_e2e_sec"), r.get("last_completed_at")
        if cid and e2e and at and cid not in seen:
            seen.add(cid)
            done = t(r["wall_clock"][:10] + " " + at)
            out.append((cid, done - timedelta(seconds=float(e2e)), done, float(e2e)))
    return out, "ckpt_timeline.csv（每 15 秒只记最近一个，可能漏记）"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--speed", type=float, required=True)
    ap.add_argument("--equality", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--tz-offset-hours", type=float, default=None)
    ap.add_argument("--sample-sec", type=float, default=10.0)
    ap.add_argument("--poll-sec", type=float, default=30.0)
    ap.add_argument("--max-ckpt-sec", type=float, default=1.0)
    ap.add_argument("--skip-snapshots", action="store_true")
    a = ap.parse_args()
    if a.tz_offset_hours is not None:
        base.LOCAL_OFFSET = timedelta(hours=a.tz_offset_hours)
    need = ["m3_tm_log.txt", "wm_buffer.csv"] + ([] if a.skip_snapshots else ["status_snapshots.csv"])
    miss = [f for f in need if not os.path.exists(os.path.join(a.run, f))] + \
        ([a.equality] if not os.path.exists(a.equality) else [])
    if miss:
        print("ERROR: 缺少 " + "、".join(miss), file=sys.stderr)
        return 2

    dev, _, _ = base.parse_log(os.path.join(a.run, "m3_tm_log.txt"))
    # 排空区间：收回时刻起、加排空秒数；日志时刻只到秒，区间两端各放宽一秒 / drain windows, widened by 1 s each side
    drains = {}
    for d, x in dev.items():
        if "collected" in x and x.get("buffered", 0) > 0:
            drains[d] = (x["collected"] - timedelta(seconds=1),
                         x["collected"] + timedelta(seconds=x.get("drainsec", 0.0) + 1), x["subtask"])
    cks, ck_src = checkpoints(a.run)

    # 1. 检查点 / checkpoints overlapping a drain
    rows1, over, covered = [], [], 0
    for d, (s, e, _) in sorted(drains.items()):
        hit = [c for c in cks if c[1] <= e and c[2] >= s]
        covered += len(hit)
        over += [c for c in hit if c[3] > a.max_ckpt_sec]
        rows1.append("| %s | %s 至 %s | %.1f | %s |" % (
            d, s.strftime("%H:%M:%S"), e.strftime("%H:%M:%S"), dev[d].get("drainsec", 0.0),
            "、".join("%s（%.3f 秒）" % (c[0], c[3]) for c in hit) or "无"))
    v1 = "未覆盖" if covered == 0 else ("通过" if not over else "**未通过**")

    # 2. 输入水位线 / input watermark of the draining subtask versus the fastest other subtask
    wm = {}
    with open(os.path.join(a.run, "wm_buffer.csv"), encoding="utf-8") as f:
        for r in csv.DictReader(f):
            try:
                iw = int(r["input_wm_ms"])
            except ValueError:
                continue
            if iw > 0:
                wm.setdefault(r["wall_clock"], {})[int(r["subtask"])] = iw
    rows2, worst, samples = [], 0.0, 0
    for d, (s, e, sub) in sorted(drains.items()):
        lags = []
        for wc, m in wm.items():
            if s <= t(wc) <= e and sub in m and len(m) > 1:
                others = max(v for k, v in m.items() if k != sub)
                lags.append(max(0.0, (others - m[sub]) / 1000.0 / a.speed))
        samples += len(lags)
        worst = max([worst] + lags)
        rows2.append("| %s | %d | %d | %s |" % (d, sub, len(lags), "%.1f" % max(lags) if lags else "—"))
    v2 = "未覆盖" if samples == 0 else ("通过" if worst <= a.sample_sec else "**未通过**")

    # 3. 逐位等值 / equality verdict
    with open(a.equality, encoding="utf-8") as f:
        eq = f.read()
    v3 = "通过" if "**全部相同。**" in eq else "**未通过**"

    # 4. 训练态状态快照条数 / training status snapshots per device
    snaps = {}
    if not a.skip_snapshots:
        with open(os.path.join(a.run, "status_snapshots.csv"), encoding="utf-8") as f:
            for r in csv.DictReader(f):
                snaps[r["device"]] = int(r["count"])
    rows4, bad4 = [], 0
    for d, x in sorted(dev.items()):
        if "enter" not in x or "collected" not in x:
            continue
        span = (x["collected"] - x["enter"]).total_seconds()
        exp = int(span // a.poll_sec)
        got = snaps.get(d, 0)
        ok = a.skip_snapshots or abs(got - exp) <= 1
        bad4 += 0 if ok else 1
        rows4.append("| %s | %.0f | %d | %s | %s |" % (d, span, exp, "—" if a.skip_snapshots else got,
                                                        "未判定" if a.skip_snapshots else ("相同" if ok else "**不符**")))
    v4 = "未判定（没有监测转储）" if a.skip_snapshots else ("通过" if rows4 and not bad4 else "**未通过**")

    L = ["# 排空缺陷修正的步骤 C 验证（0.86 版裁决第二节第 3 条）", "",
         "运行目录：`%s`；倍速 %g；检查点来源：%s。" % (a.run, a.speed, ck_src), "",
         "| 核对项 | 判据 | 结果 |", "| --- | --- | --- |",
         "| 一、排空期间的检查点 | 每次端到端不超过 %.1f 秒 | %s（重叠 %d 次，超过 %d 次） |" % (a.max_ckpt_sec, v1, covered, len(over)),
         "| 二、排空中的输入水位线 | 与最快子任务之差不超过 %.0f 秒（墙钟） | %s（%d 个采样，最大 %.1f 秒） |" % (a.sample_sec, v2, samples, worst),
         "| 三、与 t2 逐位等值 | 训练产物与全部评分窗相同 | %s（见 `%s`） |" % (v3, os.path.basename(a.equality)),
         "| 四、训练态状态快照 | 每台每 %.0f 秒一条，容差一条 | %s |" % (a.poll_sec, v4), "",
         "## 一、排空区间与重叠的检查点", "", "| 设备 | 排空区间（本机时间，两端各放宽 1 秒） | 排空（秒） | 重叠的检查点 |",
         "| --- | --- | --- | --- |"] + rows1 + \
        ["", "## 二、排空中的子任务输入水位线落后（墙钟秒）", "", "| 设备 | 子任务 | 落在排空区间内的采样数 | 最大落后 |",
         "| --- | --- | --- | --- |"] + rows2 + \
        ["", "## 四、训练态状态快照", "", "| 设备 | 提交到收回（秒） | 设计值（条） | 实测（条） | 判定 |",
         "| --- | --- | --- | --- | --- |"] + rows4
    allok = all(v == "通过" for v in (v1, v2, v3, v4))
    L += ["", "## 总判定", "", "**四项全部通过。**" if allok else "**有未通过或未覆盖的项，见上表；按「不迎合」约定如实上报。**"]
    text = "\n".join(L) + "\n"
    with open(a.out, "w", encoding="utf-8") as f:
        f.write(text)
    print(text)
    return 0 if allok else 1


if __name__ == "__main__":
    sys.exit(main())
