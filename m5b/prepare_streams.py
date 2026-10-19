#!/usr/bin/env python3
# ============================================================================
# m5b/prepare_streams.py
# 性能层离线实验的输入（交接文档 handover_m5b_stage_zh.md 第四节「数据」）：从上下文通道的评分转储整理逐台的元素流——
# 十分钟粒度即每个评分窗一个元素（主分），小时粒度即每小时主分的中位数；剔除注入设备的注入区间及其后一个窗长。
# Element streams for the M5b offline experiment: one element per scoring window (main score) and hourly medians,
# with the injected intervals (plus one window length after each) removed.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac 或任意有 python3 的机器，仓库根目录；只用标准库。
# 2. 调用命令 / Invocation:
#      python3 m5b/prepare_streams.py
#    可选：--scores docs/m3_a4/full_fix/m3_scores.jsonl（三次逐位相同运行之一的上下文通道评分）、
#          --inject docs/m3_a4/full_fix/inject-truth.csv（注入真值）、--window-sec 600（评分窗长度，秒）、
#          --out-dir docs/m5b。
# 3. 前置条件 / Preconditions: 上述两个输入文件存在（随修正后全量重跑入库）。
# 4. 期望产出 / Expected output: docs/m5b/streams_10min.csv、docs/m5b/streams_hourly.csv 与 docs/m5b/streams_summary.md
#    （逐台元素数、剔除数、时间范围与相邻元素间隔分布）；终端打印同样的摘要。
# 5. 失败兜底 / Failure fallback: 退出码 2 表示输入文件缺失。
# ============================================================================
import argparse
import collections
import csv
import json
import os
import statistics
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

LOCAL = ZoneInfo("Europe/London")


def fmt(ts, tz):
    return datetime.fromtimestamp(ts, tz).strftime("%Y-%m-%d %H:%M")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scores", default="docs/m3_a4/full_fix/m3_scores.jsonl")
    ap.add_argument("--inject", default="docs/m3_a4/full_fix/inject-truth.csv")
    ap.add_argument("--window-sec", type=int, default=600)
    ap.add_argument("--out-dir", default="docs/m5b")
    a = ap.parse_args()
    for f in (a.scores, a.inject):
        if not os.path.exists(f):
            print("ERROR: 缺少 %s" % f, file=sys.stderr)
            return 2

    # 注入区间：评分窗 [windowEnd − 窗长, windowEnd) 与 [开始, 结束 + 窗长) 有重叠即剔除
    # Exclude a window [end − w, end) that overlaps [start, stop + w).
    excl = collections.defaultdict(list)
    with open(a.inject, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            excl[r["device"]].append((int(r["start_ts"]), int(r["end_ts"]) + a.window_sec))

    rows, dropped = collections.defaultdict(list), collections.Counter()
    with open(a.scores, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if r.get("channel") != "m3_context":
                continue
            d, we = r["device"], int(r["windowEnd"])
            if any(we > s and we - a.window_sec < e for s, e in excl.get(d, [])):
                dropped[d] += 1
                continue
            rows[d].append((we, float(r["mainScore"]), bool(r["aboveThreshold"])))

    os.makedirs(a.out_dir, exist_ok=True)
    with open(os.path.join(a.out_dir, "streams_10min.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["device", "window_end", "window_end_utc", "window_end_local", "main_score", "above_threshold"])
        for d in sorted(rows):
            for we, ms, ab in sorted(rows[d]):
                w.writerow([d, we, fmt(we, timezone.utc), fmt(we, LOCAL), repr(ms), int(ab)])

    # 小时粒度：按窗口末所在的世界协调时整点小时分组，取主分中位数，并记下该小时的元素数
    # Hourly: group by the UTC hour of the window end; median main score and element count.
    with open(os.path.join(a.out_dir, "streams_hourly.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["device", "hour_start", "hour_start_utc", "hour_start_local", "n", "median_main_score"])
        for d in sorted(rows):
            hours = collections.defaultdict(list)
            for we, ms, _ in rows[d]:
                hours[we // 3600 * 3600].append(ms)
            for h in sorted(hours):
                w.writerow([d, h, fmt(h, timezone.utc), fmt(h, LOCAL), len(hours[h]), repr(statistics.median(hours[h]))])

    L = ["# M5b 离线实验的元素流摘要", "",
         "由 `m5b/prepare_streams.py` 从 `%s` 生成；注入区间来自 `%s`，评分窗与「注入区间加其后一个窗长（%d 秒）」有重叠即剔除。"
         % (a.scores, a.inject, a.window_sec), "",
         "| 设备 | 十分钟元素数 | 剔除 | 小时元素数 | 首个窗口末（当地） | 最后窗口末（当地） | 相邻间隔 600 秒的占比 | 超过 1 小时的间隔数 |",
         "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for d in sorted(rows):
        v = sorted(x[0] for x in rows[d])
        gaps = [b - x for x, b in zip(v, v[1:])]
        L.append("| %s | %d | %d | %d | %s | %s | %.1f%% | %d |" % (
            d, len(v), dropped[d], len({x // 3600 for x in v}), fmt(v[0], LOCAL), fmt(v[-1], LOCAL),
            100.0 * sum(g == 600 for g in gaps) / max(len(gaps), 1), sum(g > 3600 for g in gaps)))
    text = "\n".join(L) + "\n"
    with open(os.path.join(a.out_dir, "streams_summary.md"), "w", encoding="utf-8") as f:
        f.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
