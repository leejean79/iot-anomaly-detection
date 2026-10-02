#!/usr/bin/env python3
# ============================================================================
# m3_coldstart_report.py
# 从 TaskManager 日志里的 M3 训练记录与检查点时间线，整理出冷启动的逐设备、逐子任务与检查点三张表；
# 短重放验证（补遗三步骤 C）时再按「每天折合轮数」的缩小倍数把耗时外推到全尺寸，给出检查点超时的建议值。
# Summarise the M3 cold start from the TaskManager log lines and the checkpoint timeline: per device,
# per subtask, and checkpoint behaviour. For the probe (addendum 3 step C) it also scales the timings to
# full size and proposes the checkpoint timeout.
#
# 依据：2026-09-30 裁决（冷启动检查点配置与步骤 C）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.8+，只用标准库。
# 2. 调用命令：
#      python3 deploy/scripts/m3_coldstart_report.py --dir docs/m3_coldstart --rounds-per-day 864 \
#          --offline-csv docs/m3_grid_rerun.csv,docs/m3_grid.csv
#    --dir 下需要 m3_tm_log.txt（syn-m3-march-collect.sh 产出）与 ckpt_timeline.csv
#    （syn-m3-coldstart-watch.sh 产出）。--rounds-per-day 为作业所用的值，默认 8640；小于 8640 时输出
#    外推一节。--offline-csv 为离线网格结果（逗号分隔多份，先出现者优先），只取隐藏层 60、窗口 60 的行。
#    可选 --max-epochs 300、--timeout-min 60。
# 3. 前置条件：两份输入文件存在；日志来自 2026-09-30 之后的 jar（「entering TRAINING」行带子任务编号）。
# 4. 期望产出：终端打印报告，并写到 --dir 下的 coldstart_report.md。
# 5. 失败兜底：日志里一条训练记录也没有时退出码 2（多半是 --since 取得太晚，或作业还没进入训练）；
#    检查点时间线缺失时只跳过检查点一节。
# ============================================================================
import argparse
import csv
import math
import os
import re
import sys
from datetime import datetime

FULL_RPD = 8640
TS = r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+"
RE_HDR = re.compile(r"^===== (\S+) / (\S+) =====")
RE_ENTER = re.compile(TS + r".*\[M3\] Device (\S+) \(subtask (\d+)\) entering TRAINING: (\d+) train windows "
                      r"\((\d+) excluded.*?\), (\d+) early-stop windows")
RE_TRAINED = re.compile(TS + r".*\[M3\] Device (\S+) trained: .*?, (\d+) epochs \(last improvement at epoch (\d+), "
                        r"longest plateau (\d+)\), early-stop loss=([0-9.eE+-]+), ([0-9.]+)s")
RE_ONLINE = re.compile(TS + r".*\[M3\] Device (\S+) entering ONLINE")
RE_REPORT = re.compile(r"\[M3\] Device (\S+) REPORT: (.*)")
RE_EXCLUDED = re.compile(r"\[M3\] Device (\S+) excluded training window ending at round (\d+)")


def ts(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


excl = {}   # 在线算子日志中的剔除窗口标识（窗口最后一轮的时间戳）/ online excluded window ids


def parse_log(path):
    dev, tm = {}, "?"
    for line in open(path, encoding="utf-8", errors="replace"):
        m = RE_HDR.match(line)
        if m:
            tm = m.group(2)
            continue
        m = RE_ENTER.search(line)
        if m:
            d = dev.setdefault(m.group(2), {"reports": []})
            d.setdefault("enters", []).append(m.group(1))
            d.update(tm=tm, enter=ts(m.group(1)), subtask=int(m.group(3)), train=int(m.group(4)),
                     excluded=int(m.group(5)), es=int(m.group(6)))
            continue
        m = RE_TRAINED.search(line)
        if m:
            d = dev.setdefault(m.group(2), {"reports": []})
            d.update(trained=ts(m.group(1)), epochs=int(m.group(3)), best=int(m.group(4)),
                     plateau=int(m.group(5)), esloss=float(m.group(6)), esloss_str=m.group(6),
                     sec=float(m.group(7)))
            continue
        m = RE_ONLINE.search(line)
        if m:
            dev.setdefault(m.group(2), {"reports": []})["online"] = ts(m.group(1))
            continue
        m = RE_EXCLUDED.search(line)
        if m:
            excl.setdefault(m.group(1), set()).add(int(m.group(2)))
            continue
        m = RE_REPORT.search(line)
        if m:
            dev.setdefault(m.group(1), {"reports": []})["reports"].append(m.group(2).strip())
    return {k: v for k, v in dev.items() if "enter" in v}


def load_offline(paths):
    ref = {}
    for p in paths:
        if not p or not os.path.exists(p):
            continue
        for r in csv.DictReader(open(p, encoding="utf-8")):
            if r.get("hiddenSize") == "60" and r.get("windowLength") == "60" and r["device"] not in ref:
                ref[r["device"]] = {"epochs": int(r["epochs"]), "sec": float(r["trainSeconds"]),
                                    "spe": float(r["secPerEpoch"]), "esloss": float(r["esLoss"]),
                                    "esloss_str": r.get("esLossExact", ""), "train": int(r["trainWindows"]),
                                    "excluded": int(r["trainExcluded"]), "src": p}
    return ref


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--rounds-per-day", type=int, default=FULL_RPD)
    ap.add_argument("--offline-csv", default="")
    ap.add_argument("--offline-excluded", default="",
                    help="离线网格的剔除窗口清单（syn-m3-grid.sh 拉回的 *_excluded.csv）")
    ap.add_argument("--max-epochs", type=int, default=300)
    ap.add_argument("--timeout-min", type=float, default=60.0)
    a = ap.parse_args()

    dev = parse_log(os.path.join(a.dir, "m3_tm_log.txt"))
    if not dev:
        print("ERROR: m3_tm_log.txt 里没有「entering TRAINING」记录。", file=sys.stderr)
        return 2
    ref = load_offline(a.offline_csv.split(","))
    out = []
    p = out.append

    p("# M3 冷启动报告（每天折合轮数 %d%s）\n" % (a.rounds_per_day,
      "，短重放验证" if a.rounds_per_day < FULL_RPD else ""))
    p("## 一、逐设备\n")
    p("| 设备 | TaskManager | 子任务 | 训练窗/剔除/早停窗 | 进入训练 | 训练完成 | 轮数 | 最后改善 | 最长平台 | 早停集误差 | 训练秒数 | 秒/轮 | 上报 |")
    p("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for k in sorted(dev):
        d = dev[k]
        done = "sec" in d
        p("| %s | %s | %d | %d/%d/%d | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            k, d["tm"], d["subtask"], d["train"], d["excluded"], d["es"], d["enter"].strftime("%H:%M:%S"),
            d["trained"].strftime("%H:%M:%S") if done else "未完成", d.get("epochs", "-"), d.get("best", "-"),
            d.get("plateau", "-"), ("%.6f" % d["esloss"]) if done else "-", ("%.1f" % d["sec"]) if done else "-",
            ("%.2f" % (d["sec"] / d["epochs"])) if done and d["epochs"] else "-", "；".join(d["reports"]) or "无"))

    # 同一设备多次进入训练：要么作业重启过（状态回滚后再次触发训练），要么 --since 早于本次提交、混入了
    # 之前作业的记录。表中一律取最后一次。/ Multiple TRAINING entries: a restart, or --since too early.
    multi = {k: d["enters"] for k, d in dev.items() if len(d.get("enters", [])) > 1}
    if multi:
        p("\n**注意**：以下设备多次进入训练，表中取最后一次：%s。这说明作业重启过，或者收集时的 --since 早于"
          "本次提交、混入了之前作业的记录，须核对。" % "；".join(
              "%s（%s）" % (k, "、".join(v)) for k, v in sorted(multi.items())))

    # 逐子任务：同一子任务上的设备只能依次训练，任务线程被占住的时长是从第一台进入训练到最后一台完成。
    p("\n## 二、逐子任务（同一子任务上的设备依次训练）\n")
    p("| 子任务 | TaskManager | 设备 | 训练秒数之和 | 线程被占住的墙钟跨度（秒） |")
    p("| --- | --- | --- | --- | --- |")
    subs = {}
    for k, d in dev.items():
        subs.setdefault(d["subtask"], []).append(k)
    spans = {}
    for s in sorted(subs):
        ds = sorted(subs[s], key=lambda k: dev[k]["enter"])
        tot = sum(dev[k].get("sec", 0.0) for k in ds)
        ends = [dev[k].get("online") or dev[k].get("trained") for k in ds]
        span = (max(ends) - dev[ds[0]]["enter"]).total_seconds() if all(ends) else float("nan")
        spans[s] = span
        p("| %d | %s | %s | %.1f | %s |" % (s, dev[ds[0]]["tm"], "、".join(ds), tot,
                                            "未完成" if math.isnan(span) else "%.0f" % span))
    # 2026-09-30 裁决：任一子任务的线程占用跨度超过检查点超时的一半，说明外推偏乐观，须上报设计会话。
    # Ruling of 2026-09-30: a subtask span above half the checkpoint timeout must be reported.
    half = a.timeout_min * 60 / 2
    over = [s for s, v in spans.items() if not math.isnan(v) and v > half]
    p("\n%s" % ("**须上报设计会话**：子任务 %s 的跨度超过检查点超时的一半（%.0f 分钟），外推偏乐观。"
                 % ("、".join("%d（%.0f 分钟）" % (s, spans[s] / 60) for s in over), half / 60)
                 if over else "各子任务的跨度都不超过检查点超时的一半（%.0f 分钟）。" % (half / 60)))
    # 同时在训练的设备数：用「进入训练」到「训练完成」的区间求最大重叠。
    ev = []
    for d in dev.values():
        if "trained" in d:
            ev += [(d["enter"], 1), (d["trained"], -1)]
    cur = peak = 0
    for _, e in sorted(ev, key=lambda x: (x[0], x[1])):
        cur += e
        peak = max(peak, cur)
    p("\n同时处于训练中的设备数最多为 %d 台。" % peak)

    # 检查点时间线
    tl = os.path.join(a.dir, "ckpt_timeline.csv")
    if os.path.exists(tl):
        rows = list(csv.DictReader(open(tl, encoding="utf-8")))
        p("\n## 三、检查点与重启\n")
        # 挂起时长按检查点编号分别取最大值。若某个编号一直显示「进行中」，而更大编号的检查点已经完成，
        # 那么它在协调器里已不再挂起（最大并发为 1），只是统计接口没有把它结掉，单独列出，不计入挂起时长。
        # A pending id seen while a larger id has completed is a statistics leftover (max concurrency is 1).
        pend_by, stale = {}, set()
        for r in rows:
            if r["pending_id"] and r["pending_sec"]:
                pid = int(r["pending_id"])
                pend_by[pid] = max(pend_by.get(pid, 0.0), float(r["pending_sec"]))
                if r["last_completed_id"] and int(r["last_completed_id"]) > pid:
                    stale.add(pid)
        pend = [v for k, v in pend_by.items() if k not in stale]
        rst = [int(r["num_restarts"]) for r in rows if r["num_restarts"].isdigit()]
        failed = [int(r["failed"]) for r in rows if r["failed"].isdigit()]
        long_done = {}
        for r in rows:
            if r["last_completed_e2e_sec"] and float(r["last_completed_e2e_sec"]) > 60:
                long_done[r["last_completed_id"]] = (float(r["last_completed_e2e_sec"]), r["last_completed_at"])
        p("- 记录 %d 行，从 %s 到 %s。" % (len(rows), rows[0]["wall_clock"], rows[-1]["wall_clock"]) if rows else "- 时间线为空。")
        p("- 观察到的最长挂起时间：%s 秒（检查点超时为 %.0f 分钟，即 %.0f 秒）。"
          % ("%.0f" % max(pend) if pend else "无挂起", a.timeout_min, a.timeout_min * 60))
        if stale:
            p("- 统计上悬挂的检查点：编号 %s。它们一直显示「进行中」，而更大编号的检查点已经陆续完成；最大并发为 1，"
              "所以它们在协调器里已不再挂起，只是统计接口没有结掉。「进行中」计数因此一直停在 1，"
              "上面的最长挂起时间不含它们。" % "、".join(str(i) for i in sorted(stale)))
        p("- 端到端耗时超过 60 秒、最终完成的检查点：%s。" % ("；".join(
            "编号 %s，%.0f 秒，完成于 %s" % (i, v[0], v[1]) for i, v in sorted(long_done.items())) or "无"))
        p("- 失败的检查点累计：%s 次；重启次数：%s。" % (max(failed) if failed else "?",
                                              ("最大 %d" % max(rst)) if rst else "读不到"))

    # 外推（仅短重放）
    if a.rounds_per_day < FULL_RPD:
        scale = FULL_RPD / a.rounds_per_day
        p("\n## 四、外推到全尺寸（近似）\n")
        p("每轮的代价与窗口数近似成正比（训练更新次数与早停集评估批次都随窗口数增长），故全尺寸每轮秒数取"
          "短重放实测值乘以 %.1f。批次数其实按向上取整增长、每批另有固定开销，所以这样外推偏保守。"
          "全尺寸的轮数：离线网格测过的设备取实测值，其余设备分别按「已测设备中最多的轮数」与「上限 %d 轮」"
          "给出两个值。" % (scale, a.max_epochs))
        known_max = max([r["epochs"] for r in ref.values()], default=a.max_epochs)
        p("\n| 子任务 | 设备（全尺寸轮数来源） | 外推秒数（未测设备按已测最多轮数） | 外推秒数（未测设备按上限） |")
        p("| --- | --- | --- | --- |")
        worst = [0.0, 0.0]
        for s in sorted(subs):
            lo = hi = 0.0
            parts = []
            for k in sorted(subs[s]):
                d = dev[k]
                if "sec" not in d or not d["epochs"]:
                    parts.append("%s（未完成）" % k)
                    continue
                spe = d["sec"] / d["epochs"] * scale
                if k in ref:
                    lo += ref[k]["epochs"] * spe
                    hi += ref[k]["epochs"] * spe
                    parts.append("%s（离线 %d 轮）" % (k, ref[k]["epochs"]))
                else:
                    lo += known_max * spe
                    hi += a.max_epochs * spe
                    parts.append("%s（未测）" % k)
            worst = [max(worst[0], lo), max(worst[1], hi)]
            p("| %d | %s | %.0f | %.0f |" % (s, "、".join(parts), lo, hi))
        p("\n最慢子任务外推为 %.0f 至 %.0f 秒（%.0f 至 %.0f 分钟）。按裁决「取实测最慢时长的两倍」，"
          "检查点超时应为 %.0f 至 %.0f 分钟；当前配置 %.0f 分钟。"
          % (worst[0], worst[1], worst[0] / 60, worst[1] / 60, 2 * worst[0] / 60, 2 * worst[1] / 60, a.timeout_min))

    # 与离线单台训练相比的减速比（全尺寸运行）
    if a.rounds_per_day == FULL_RPD and ref:
        p("\n## 四、与离线单台训练相比的减速比（每轮秒数之比）\n")
        p("| 设备 | 在线秒/轮 | 离线秒/轮 | 减速比 |")
        p("| --- | --- | --- | --- |")
        for k in sorted(set(dev) & set(ref)):
            d, r = dev[k], ref[k]
            if "sec" in d:
                spe = d["sec"] / d["epochs"]
                p("| %s | %.2f | %.2f | %.2f |" % (k, spe, r["spe"], spe / r["spe"]))
        p("\n离线参照来自 %s。离线在 master 上运行，在线在 worker 上运行，两者 CPU 不同，减速比同时包含"
          "机器差异与并行争用。" % "、".join(sorted({r["src"] for r in ref.values()})))

        # 等值核验（2026-10-02 裁决第二节第 2 条）：先核对两边训练集完全相同（窗口数与剔除集合逐一相等），
        # 再要求早停集误差逐位相同；输入不同时不套 5%，报告差异所在。
        # Parity check: identical training sets first, then a bit-identical early-stop loss.
        off_excl = {}
        if a.offline_excluded and os.path.exists(a.offline_excluded):
            for r in csv.DictReader(open(a.offline_excluded, encoding="utf-8")):
                if r.get("windowLength", "60") == "60":
                    off_excl.setdefault(r["device"], set()).add(int(r["lastRoundTs"]))
        p("\n## 五、等值核验（先核训练集，再核早停集误差是否逐位相同）\n")
        p("| 设备 | 训练窗（在线/离线） | 剔除数（在线/离线） | 剔除集合 | 早停集误差（在线/离线） | 结论 |")
        p("| --- | --- | --- | --- | --- | --- |")
        fmt = lambda t: datetime.utcfromtimestamp(t).strftime("%m-%d %H:%M:%S")
        notes = []
        for k in sorted(set(dev) & set(ref)):
            d, r = dev[k], ref[k]
            if "sec" not in d:
                continue
            same_n = d["train"] == r["train"] and d["excluded"] == r["excluded"]
            if k in excl and k in off_excl:
                only_on, only_off = sorted(excl[k] - off_excl[k]), sorted(off_excl[k] - excl[k])
                same_set = not only_on and not only_off
                set_txt = "相同" if same_set else "不同（仅在线 %d，仅离线 %d）" % (len(only_on), len(only_off))
                if not same_set:
                    notes.append("%s 仅在线剔除：%s；仅离线剔除：%s" % (
                        k, "、".join(fmt(t) for t in only_on[:10]) or "无",
                        "、".join(fmt(t) for t in only_off[:10]) or "无"))
            else:
                same_set = None
                set_txt = "缺少清单，无法核对"
            if same_n and same_set:
                bit = r["esloss_str"] != "" and d["esloss_str"] == r["esloss_str"]
                verdict = "通过：训练集相同，误差逐位相同" if bit else (
                    "不通过：训练集相同但误差不逐位相同" if r["esloss_str"] else "离线缺少完整精度误差，无法逐位比对")
            elif same_set is None:
                verdict = "训练集无法完整核对，不作判定"
            else:
                verdict = "输入不同，不作判定（见差异）"
            p("| %s | %d/%d | %d/%d | %s | %s / %s | %s |" % (
                k, d["train"], r["train"], d["excluded"], r["excluded"], set_txt,
                d["esloss_str"], r["esloss_str"] or ("%.8f" % r["esloss"]), verdict))
        for n in notes:
            p("\n- " + n)
        p("\n剔除窗口以窗口最后一轮的时间戳（UTC）标识。")

    text = "\n".join(out) + "\n"
    print(text)
    with open(os.path.join(a.dir, "coldstart_report.md"), "w", encoding="utf-8") as f:
        f.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
