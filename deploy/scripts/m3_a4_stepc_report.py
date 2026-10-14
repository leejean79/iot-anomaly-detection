#!/usr/bin/env python3
"""
m3_a4_stepc_report.py —— 补遗四步骤 C（短重放预验证）的汇总：从每次运行的日志、检查点时间线与水位线采样里
算出五件事要的数字，并对「杀任务管理器的那次」与「未被打断的参照」做逐位比较。
Summarises addendum-4 step C runs: per-run numbers for the five items from the logs, checkpoint timeline and
watermark samples, plus a bit-exact comparison of the killed run against its uninterrupted reference.

---------------------------- 脚本交付五要素 ----------------------------
1. 执行环境 / Environment: 本地 Mac，仓库根目录，python3（只用标准库）。
2. 调用命令 / Invocation:
     python3 deploy/scripts/m3_a4_stepc_report.py \\
         --runs t1=docs/m3_a4/stepc_t1,t2=docs/m3_a4/stepc_t2,t4=docs/m3_a4/stepc_t4,kill=docs/m3_a4/stepc_kill \\
         --compare kill:t2 --cancel-log docs/m3_a4/stepc_cancel/cancel.txt --out docs/m3_a4/stepc_report.md
   --runs 逗号分隔的「标签=目录」；--compare 「被比较:参照」，可省略；--cancel-log 取消计时的输出，可省略；
   --buffer-cap 缓冲上限（轮），默认 7776（步骤 C：9 × 864），用于给出缓冲峰值占上限的比例。
3. 前置条件 / Preconditions: 每个目录里有 syn-m3-march-collect.sh 生成的 m3_tm_log.txt 与 m3_scores.jsonl、
   syn-m3-coldstart-watch.sh 生成的 ckpt_timeline.csv、m3_a4_watch.py 生成的 wm_buffer.csv（缺哪个就跳过哪一节）。
4. 期望产出 / Expected output: --out 指定的 Markdown 文件，五节分别对应步骤 C 的五件事；终端打印同样内容。
5. 失败兜底 / Failure fallback: 退出码 2 表示某个目录不存在或没有日志；某次运行缺少训练记录时，该运行在表里
   标「无训练记录」，请检查收集时的 --since 是否早于提交时刻。
"""
import argparse
import collections
import csv
import json
import os
import re
import statistics
import sys
from datetime import datetime, timedelta

TS = r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+"
RE = {
    "enter": re.compile(TS + r".*\[M3\] Device (\S+) \(subtask (\d+)\) entering TRAINING"),
    "started": re.compile(TS + r".*\[M3\] Device (\S+) training started on (\S+) after ([0-9.]+)s"),
    "trained": re.compile(TS + r".*\[M3\] Device (\S+) trained: .*?, (\d+) epochs .*early-stop loss=(\S+), ([0-9.]+)s"),
    "calibrated": re.compile(TS + r".*\[M3\] Device (\S+) calibrated: median=(\S+), IQR=(\S+), threshold=(\S+)"),
    "collected": re.compile(TS + r".*\[M3\] Device (\S+) result collected .*?; (\d+) buffered rounds"),
    "drained": re.compile(TS + r".*\[M3\] Device (\S+) buffer drained in ([0-9.]+)s"),
    "resubmit": re.compile(TS + r".*\[M3\] Device (\S+) is in TRAINING without a model"),
    "released": re.compile(TS + r".*\[M3\] Output watermark hold released after ([0-9.]+)s \(subtask (\d+)\)"),
    "threads": re.compile(r"\[M3\] Training executor started: (\d+) thread"),
}


def ts(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


# 任务管理器容器的日志是世界协调时，两个采样脚本记的是本机时间；日志时刻加上本机时区偏移后再比较。
# TaskManager logs are UTC while both watchers record local time; shift log times by the local offset.
LOCAL_OFFSET = timedelta(minutes=round((datetime.now() - datetime.utcnow()).total_seconds() / 60))


def ts_log(s):
    return ts(s) + LOCAL_OFFSET


def parse_log(path):
    dev = collections.defaultdict(dict)
    hold = collections.defaultdict(list)
    threads = None
    if not os.path.exists(path):
        return dev, hold, threads
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            for k, rx in RE.items():
                m = rx.search(line)
                if not m:
                    continue
                if k == "threads":
                    threads = int(m.group(1))
                elif k == "released":
                    hold[int(m.group(3))].append(float(m.group(2)))
                else:
                    d = dev[m.group(2)]
                    d[k] = ts_log(m.group(1))
                    if k == "enter":
                        d["subtask"] = int(m.group(3))
                    elif k == "started":
                        d["thread"], d["wait"] = m.group(3), float(m.group(4))
                    elif k == "trained":
                        d["epochs"], d["esloss"], d["trainsec"] = m.group(3), m.group(4), float(m.group(5))
                    elif k == "calibrated":
                        d["median"], d["iqr"], d["thr"] = m.group(3), m.group(4), m.group(5)
                    elif k == "collected":
                        d["buffered"] = int(m.group(3))
                    elif k == "drained":
                        d["drainsec"] = float(m.group(3))
                    elif k == "resubmit":
                        d["resubmits"] = d.get("resubmits", 0) + 1
    return dev, hold, threads


def read_ckpt(path):
    rows = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows.append(r)
    return rows


def ckpt_section(rows, dev):
    """训练期间与训练前的检查点端到端时长 / checkpoint e2e before vs during training."""
    if not rows or not dev:
        return None
    starts = [d["enter"] for d in dev.values() if "enter" in d]
    ends = [d.get("collected") or d.get("trained") for d in dev.values()]
    ends = [e for e in ends if e]
    if not starts or not ends:
        return None
    t0, t1 = min(starts), max(ends)
    seen, before, during = set(), [], []
    for r in rows:
        cid, e2e, at = r.get("last_completed_id"), r.get("last_completed_e2e_sec"), r.get("last_completed_at")
        if not cid or not e2e or cid in seen:
            continue
        seen.add(cid)
        when = ts(r["wall_clock"][:10] + " " + at) if at else ts(r["wall_clock"])
        (during if t0 <= when <= t1 else before if when < t0 else []).append(float(e2e))
    failed = max((int(r["failed"]) for r in rows if r.get("failed", "").isdigit()), default=0)
    restarts = max((int(r["num_restarts"]) for r in rows if r.get("num_restarts", "").isdigit()), default=0)
    return {
        "before_median": statistics.median(before) if before else None,
        "during_max": max(during) if during else None,
        "during_n": len(during), "failed": failed, "restarts": restarts,
    }


def wm_section(path, dev):
    """训练期间各子任务输入水位线最长停滞（墙钟秒）/ longest input-watermark stall during training."""
    if not os.path.exists(path) or not dev:
        return None
    starts = [d["enter"] for d in dev.values() if "enter" in d]
    ends = [d.get("collected") for d in dev.values() if d.get("collected")]
    if not starts or not ends:
        return None
    t0, t1 = min(starts), max(ends)
    series = collections.defaultdict(list)
    peak = collections.defaultdict(int)
    overflow = 0
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            w = ts(r["wall_clock"])
            sub = int(r["subtask"])
            try:
                peak[sub] = max(peak[sub], int(float(r["buffer_rounds"] or 0)))
                overflow = max(overflow, int(float(r["overflow"] or 0)))
            except ValueError:
                pass
            if t0 <= w <= t1 and r["input_wm_ms"]:
                series[sub].append((w, float(r["input_wm_ms"])))
    stall = {}
    for sub, pts in series.items():
        longest, last_change = 0.0, pts[0][0] if pts else None
        for (w0, v0), (w1, v1) in zip(pts, pts[1:]):
            if v1 > v0:
                last_change = w1
            longest = max(longest, (w1 - last_change).total_seconds())
        stall[sub] = longest
    return {"stall": stall, "peak": dict(peak), "overflow": overflow}


def scores_by_device(path):
    out = collections.defaultdict(dict)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    o = json.loads(line)
                except ValueError:
                    continue
                out[o.get("device")][o.get("windowEnd")] = (o.get("mainScore"), o.get("wmse"))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", required=True)
    ap.add_argument("--compare", default="")
    ap.add_argument("--cancel-log", default="")
    ap.add_argument("--out", required=True)
    ap.add_argument("--buffer-cap", type=int, default=7776,
                    help="缓冲上限（轮）：(标定天数 + 2) × 每天折合轮数，步骤 C 为 9 × 864 = 7776")
    a = ap.parse_args()
    runs = []
    for item in a.runs.split(","):
        tag, d = item.split("=", 1)
        if not os.path.isdir(d):
            print("ERROR: 目录不存在：%s" % d, file=sys.stderr)
            return 2
        runs.append((tag, d))
    L = ["# 补遗四步骤 C：短重放预验证汇总", ""]
    parsed = {}
    for tag, d in runs:
        dev, hold, threads = parse_log(os.path.join(d, "m3_tm_log.txt"))
        parsed[tag] = (d, dev, hold, threads)

    L += ["## 一、任务线程可用性（检查点端到端时长）", "",
          "| 运行 | 线程数 | 训练前中位数（秒） | 训练期间最大（秒） | 训练期间完成数 | 是否不超过两倍 | 失败次数 | 重启次数 |",
          "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for tag, (d, dev, hold, threads) in parsed.items():
        c = ckpt_section(read_ckpt(os.path.join(d, "ckpt_timeline.csv")), dev)
        if not c:
            L.append("| %s | %s | 无数据 | | | | | |" % (tag, threads or "?"))
            continue
        ok = (c["before_median"] and c["during_max"] is not None
              and c["during_max"] <= 2 * c["before_median"])
        L.append("| %s | %s | %s | %s | %d | %s | %d | %d |" % (
            tag, threads or "?", "%.1f" % c["before_median"] if c["before_median"] else "-",
            "%.1f" % c["during_max"] if c["during_max"] is not None else "-", c["during_n"],
            "是" if ok else "否", c["failed"], c["restarts"]))

    L += ["", "## 二、水位线与缓冲", "",
          "| 运行 | 子任务 | 训练期间输入水位线最长停滞（墙钟秒） | 缓冲峰值（轮） | 峰值占上限 %d 轮的比例 | 输出水位线保持时长（秒） |" % a.buffer_cap,
          "| --- | --- | --- | --- | --- | --- |"]
    for tag, (d, dev, hold, threads) in parsed.items():
        w = wm_section(os.path.join(d, "wm_buffer.csv"), dev)
        if not w:
            L.append("| %s | 无数据 | | | | |" % tag)
            continue
        for sub in sorted(set(w["stall"]) | set(w["peak"])):
            L.append("| %s | %d | %s | %d | %.1f%% | %s |" % (
                tag, sub, "%.0f" % w["stall"][sub] if sub in w["stall"] else "-", w["peak"].get(sub, 0),
                100.0 * w["peak"].get(sub, 0) / a.buffer_cap,
                "、".join("%.1f" % x for x in hold.get(sub, [])) or "-"))
        L.append("| %s | 缓冲溢出计数（最大） | %d | | | |" % (tag, w["overflow"]))

    L += ["", "## 三、逐设备训练（含排队等待）与排空", "",
          "| 运行 | 设备 | 子任务 | 训练线程 | 排队等待（秒） | 训练秒数 | 轮数 | 提交到收回（秒） | 收回时缓冲轮数 | 排空（秒） | 重新提交 |",
          "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for tag, (d, dev, hold, threads) in parsed.items():
        if not dev:
            L.append("| %s | 无训练记录 | | | | | | | | | |" % tag)
        for name in sorted(dev):
            x = dev[name]
            span = (x["collected"] - x["enter"]).total_seconds() if "collected" in x and "enter" in x else None
            L.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
                tag, name, x.get("subtask", "-"), x.get("thread", "-"),
                "%.1f" % x["wait"] if "wait" in x else "-", "%.1f" % x["trainsec"] if "trainsec" in x else "-",
                x.get("epochs", "-"), "%.0f" % span if span is not None else "-", x.get("buffered", "-"),
                "%.1f" % x["drainsec"] if "drainsec" in x else "-", x.get("resubmits", 0)))

    L += ["", "## 四、取消", ""]
    if a.cancel_log and os.path.exists(a.cancel_log):
        with open(a.cancel_log, encoding="utf-8") as f:
            L += ["```", f.read().strip(), "```"]
    else:
        L.append("没有提供取消计时的输出。")

    L += ["", "## 五、重启后的逐位比较", ""]
    if a.compare:
        x, y = a.compare.split(":")
        dx, dy = parsed[x][1], parsed[y][1]
        L += ["| 设备 | 重新提交次数 | 轮数 | 早停集误差 | 标定中位数 | 标定 IQR | 阈值 | 评分窗数（%s/%s） | 逐窗相同 |" % (x, y),
              "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
        sx = scores_by_device(os.path.join(parsed[x][0], "m3_scores.jsonl"))
        sy = scores_by_device(os.path.join(parsed[y][0], "m3_scores.jsonl"))
        for name in sorted(set(dx) | set(dy)):
            p, q = dx.get(name, {}), dy.get(name, {})
            same = lambda k: "相同" if p.get(k) is not None and p.get(k) == q.get(k) else "不同（%s / %s）" % (
                p.get(k), q.get(k))
            common = set(sx[name]) & set(sy[name])
            eq = all(sx[name][k] == sy[name][k] for k in common)
            L.append("| %s | %s | %s | %s | %s | %s | %s | %d/%d | %s |" % (
                name, p.get("resubmits", 0), same("epochs"), same("esloss"), same("median"), same("iqr"),
                same("thr"), len(sx[name]), len(sy[name]),
                ("是（共同 %d 窗）" % len(common)) if eq and len(sx[name]) == len(sy[name]) else "否"))
    else:
        L.append("没有指定 --compare。")

    text = "\n".join(L) + "\n"
    with open(a.out, "w", encoding="utf-8") as f:
        f.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
