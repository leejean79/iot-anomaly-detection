#!/usr/bin/env python3
"""
m3_a4_guard_check.py —— 补遗四全量重放的验收项 A4-4（守卫在线核对）：从监测转储中读出数据接入模块的重复值游程标记，
核对三次温度卡死注入是否在阈值长度处被标为疑似卡死，以及自然数据中普通游程是否零触阈。
A4-4 online check of the repeated-value run-length guard from the monitoring dump.

判读 / rules（守卫收口裁决第三节）：
  - 每段卡死注入：期望在注入区间内出现该通道的疑似卡死标记，游程起点在注入开始前后一轮之内，首次标记时的游程长度
    等于或略大于阈值（监测快照每 60 秒一次，所以首次看到时可能已多出几轮）。
  - 自然数据：注入区间之外不应出现任何疑似卡死标记（普通游程零触阈）；底值游程只计数，单列供参考。

---------------------------- 脚本交付五要素 ----------------------------
1. 执行环境 / Environment: master（监测转储较大，留在 master 上），由 syn-m3-a4-full.sh 调用；也可在有转储的任何机器上运行。
2. 调用命令 / Invocation:
     python3 deploy/scripts/m3_a4_guard_check.py --monitoring /opt/fa-iforest/m3march/monitoring.jsonl \
         --truth docs/m3_a4/full/inject-truth.csv --thresholds docs/stuck_guard/thresholds.env --out docs/m3_a4/full/guard_check.md
3. 前置条件 / Preconditions: 作业启动时带了守卫阈值（启动横幅 Stuck guard: ON）；监测转储来自本次运行。
4. 期望产出 / Expected output: --out 的 Markdown：三段卡死注入逐段的首次标记时刻、游程起点与长度、判定；注入区间外的
   疑似卡死标记清单（期望为空）；底值游程计数。
5. 失败兜底 / Failure fallback: 退出码 0 通过；1 未通过（某段注入没被标记，或自然数据有标记）；2 输入缺失。
"""
import argparse
import collections
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

CHANNELS = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]


def utc(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%m-%d %H:%M:%S")


def thresholds(path):
    with open(path, encoding="utf-8") as f:
        m = re.search(r'SYN_STUCK_RUN_THRESHOLDS="([^"]*)"', f.read())
    out = {}
    for item in (m.group(1) if m else "").split(","):
        if "=" in item:
            d, v = item.split("=", 1)
            out[d.strip()] = [int(x) for x in v.split("/")]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--monitoring", required=True)
    ap.add_argument("--truth", required=True)
    ap.add_argument("--thresholds", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    for p in (a.monitoring, a.truth, a.thresholds):
        if not os.path.exists(p):
            print("ERROR: 缺少 %s" % p, file=sys.stderr)
            return 2
    thr = thresholds(a.thresholds)
    stuck_inj = []
    with open(a.truth, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["type"] == "stuck":
                stuck_inj.append((r["device"], CHANNELS.index(r["channel"]), int(r["start_ts"]), int(r["end_ts"])))

    # 逐台逐通道收集被标记的快照：疑似卡死与底值游程分开 / flagged snapshots, stuck and floor separately
    flagged = collections.defaultdict(list)     # (device, ch) -> [(ts, runLen, runStart)]
    floor = collections.Counter()               # (device, ch) -> snapshots with a floor run
    with open(a.monitoring, encoding="utf-8") as f:
        for line in f:
            if '"stuckRounds"' not in line:
                continue
            try:
                o = json.loads(line)
            except ValueError:
                continue
            if o.get("windowEnd", 0) != 0:      # 只看数据接入模块的快照 / ingestion snapshots only
                continue
            lens, starts = o.get("guardRunLength") or [0] * 5, o.get("guardRunStartTs") or [0] * 5
            for c in range(5):
                if o.get("stuckRounds", 0) and (o.get("stuckChannelsMask", 0) >> c) & 1:
                    flagged[(o["device"], c)].append((o["ts"], lens[c], starts[c]))
                if o.get("floorRounds", 0) and (o.get("floorChannelsMask", 0) >> c) & 1:
                    floor[(o["device"], c)] += 1

    ok = True
    L = ["# A4-4 守卫在线核对", "", "## 一、三段卡死注入", "",
         "| 段 | 设备与通道 | 注入区间（UTC） | 阈值（轮） | 首次标记 | 游程起点 | 首次标记时游程长度 | 判定 |",
         "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    used = set()
    for i, (d, c, s, e) in enumerate(stuck_inj, 1):
        hits = [h for h in flagged.get((d, c), []) if s <= h[0] <= e + 60]
        used.update(((d, c), h) for h in hits)
        t = thr.get(d, [0] * 5)[c]
        if hits:
            ts, ln, st = min(hits)
            good = abs(st - s) <= 20 and ln >= t
            L.append("| %d | %s %s | %s 至 %s | %d | %s | %s | %d | %s |" % (
                i, d, CHANNELS[c], utc(s), utc(e), t, utc(ts), utc(st) if st else "-", ln, "通过" if good else "**未通过**"))
        else:
            good = False
            L.append("| %d | %s %s | %s 至 %s | %d | 未被标记 | - | - | **未通过** |" % (i, d, CHANNELS[c], utc(s), utc(e), t))
        ok &= good

    L += ["", "## 二、注入区间之外的疑似卡死标记（期望为空）", ""]
    natural = [(k, h) for k, hs in flagged.items() for h in hs if (k, h) not in used]
    if natural:
        ok = False
        L += ["| 设备与通道 | 快照时刻（UTC） | 游程起点 | 游程长度 |", "| --- | --- | --- | --- |"]
        for (d, c), (ts, ln, st) in sorted(natural)[:50]:
            L.append("| %s %s | %s | %s | %d |" % (d, CHANNELS[c], utc(ts), utc(st) if st else "-", ln))
        L.append("")
        L.append("共 %d 个快照带疑似卡死标记（表中最多列 50 个）。" % len(natural))
    else:
        L.append("没有。自然数据中普通游程零触阈。")

    L += ["", "## 三、底值游程（只计数，供参考）", ""]
    if floor:
        L += ["| 设备与通道 | 带底值游程标记的快照数 |", "| --- | --- |"]
        for (d, c), n in sorted(floor.items()):
            L.append("| %s %s | %d |" % (d, CHANNELS[c], n))
    else:
        L.append("没有底值游程达到阈值。")
    L += ["", "## 判定", "", "**通过。**" if ok else "**未通过，见上表。**"]
    text = "\n".join(L) + "\n"
    with open(a.out, "w", encoding="utf-8") as f:
        f.write(text)
    print(text)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
