#!/usr/bin/env python3
# ============================================================================
# m7/prepare_inputs.py
# 聚合模块决策部分（M7）离线实验的输入整理：把三次逐位相同全量运行的转储与前两个阶段的离线产出（出处见离线操作口径第一节），
# 整理成决策函数直接读取的七张表。Python 参考实现 `m7/decide.py` 与 Java 离线运行器读同一份整理结果，所以两者的比较不受整理方式影响。
# Input preparation for the M7 offline experiment: seven tidy tables read by both the Python reference and the Java runner.
#
# 产出（--out-dir，默认 docs/m7/inputs）：
#   hours.csv          每台每当地小时：轮数、入口轮数、守卫打标轮数、到达滑动步的离群轮数、上下文窗口数与超阈数与主分最大值、
#                      温度气体光照冻结之后的变换域读数数与和；
#   perf_events.csv    性能层候选、确认、撤销（确认带门限与幅度）；
#   perf_elements.csv  性能层元素（每台每十分钟主分），供确认之后判回落；
#   data_days.csv      每台每通道每个当地日：可判、日末是否偏离、当日转移、偏移、宽度倍数；
#   bursts.csv         离群率爆发：报警元素、发出时刻（按数据层在线实现的规则模拟）、大小、峰值、持续长度；
#   refs.csv           每台温度、气体、光照参照直方图的 P10 与 P90（变换域）；
#   freeze.csv         每台冻结时刻。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3（只用标准库）。
# 2. 调用命令 / Invocation: python3 m7/prepare_inputs.py [--out-dir docs/m7/inputs]
# 3. 前置条件 / Preconditions: docs/m7/extract/hourly.csv 含 pn_ 与 ps_ 列（用户在 Mac 上用新版 eda/m7_extract.py 重跑过），以及
#    freeze.csv；docs/m3_a4/full_fix/scores.jsonl、docs/m5b/streams_10min_with_injection.csv、docs/m5b/grid/events/
#    c0.99_r0.5_10min_k36_with_injection.csv、docs/m5a/lm_transitions.csv、docs/m5a/bursts.csv、docs/m5a/extract/{daily,quality,
#    denominators,refs}.csv。
# 4. 期望产出 / Expected output: 上述七张表；终端打印每张表的行数，以及模拟的爆发与 docs/m5a/bursts.csv 是否逐条一致。
# 5. 失败兜底 / Failure fallback: 缺输入时退出码 2；hourly.csv 没有 pn_、ps_ 列时退出码 3（提示在 Mac 上重跑提取）；
#    模拟的爆发与数据层离线结果不一致时退出码 4。
# ============================================================================
import argparse
import collections
import csv
import json
import os
import sys

SYNC = ("Temperature", "Gas", "Light")
CHANNELS = ("Temperature", "Humidity", "Pressure", "Gas", "Light")
BIN, HORIZON, SLIDE = 600, 3600, 60
FIRST_DAY, LAST_DAY = "2022-03-08", "2022-04-30"


def rows(path):
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def write(path, header, data):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        for r in data:
            w.writerow([repr(v) if isinstance(v, float) else v for v in r])
    print("  %s：%d 行" % (os.path.basename(path), len(data)))


def outliers(path):
    """到达滑动步的离群判定，按轮去重 / arrival-slide outlier rounds, deduplicated."""
    seen = set()
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            r = json.loads(line)
            if r.get("channel") != "m2_point" or not r.get("outlier"):
                continue
            rt, we = int(r["roundTs"]), int(r["windowEnd"])
            if we - SLIDE <= rt < we:
                seen.add((r["device"], rt))
    return seen


def simulate_bursts(den, num, table_path):
    """按数据层在线实现（M5aCore.element）的规则重放离群率元素，得到每次爆发的大小、峰值、持续长度与发出时刻。
    Replays the outlier-rate elements as M5aCore does, giving each burst's size, peak, duration and emission time."""
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "m5b"))
    import optwin_offline as ow   # noqa: E402
    table = ow.load_table(table_path)
    out = []
    for dev in sorted(den):
        det = ow.Detector(table.wmax)
        pending, last_run = [], 0
        for b in sorted(den[dev]):
            a = den[dev][b]
            if a <= 0:
                continue
            k = num.get((dev, b), 0)
            rate = k / a
            keep = []
            for q in pending:
                if b <= q["ts"] + HORIZON:
                    q["size"] += k
                    q["peak"] = max(q["peak"], rate)
                if q["run_open"]:
                    if rate > 0:
                        q["duration"] += 1
                    else:
                        q["run_open"] = False
                if b > q["ts"] + HORIZON and not q["run_open"]:
                    out.append((dev, q["ts"], b + SLIDE, q["size"], q["peak"], q["duration"]))
                else:
                    keep.append(q)
            pending = keep
            alarm = det.update(rate, table)
            if alarm is not None:
                pending.append(dict(ts=b, size=k, peak=rate, run_open=rate > 0,
                                    duration=last_run + 1 if rate > 0 else last_run))
            last_run = last_run + 1 if rate > 0 else 0
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--extract-dir", default="docs/m7/extract")
    ap.add_argument("--m5a-dir", default="docs/m5a")
    ap.add_argument("--scores", default="docs/m3_a4/full_fix/scores.jsonl")
    ap.add_argument("--streams", default="docs/m5b/streams_10min_with_injection.csv")
    ap.add_argument("--perf", default="docs/m5b/grid/events/c0.99_r0.5_10min_k36_with_injection.csv")
    ap.add_argument("--table", default="docs/m5b/tables/thesis_0.99_0.5_2016.csv")
    ap.add_argument("--out-dir", default="docs/m7/inputs")
    a = ap.parse_args()
    ex = a.extract_dir
    m5 = a.m5a_dir
    need = [os.path.join(ex, "hourly.csv"), os.path.join(ex, "freeze.csv"), a.scores, a.streams, a.perf, a.table,
            os.path.join(m5, "lm_transitions.csv"), os.path.join(m5, "bursts.csv")]
    need += [os.path.join(m5, "extract", f) for f in ("daily.csv", "quality.csv", "denominators.csv", "refs.csv")]
    miss = [f for f in need if not os.path.exists(f)]
    if miss:
        print("ERROR: 缺少输入：\n  " + "\n  ".join(miss), file=sys.stderr)
        return 2
    hourly = rows(os.path.join(ex, "hourly.csv"))
    if "ps_Light" not in hourly[0]:
        print("ERROR: hourly.csv 没有 pn_、ps_ 列；请在 Mac 上用新版 eda/m7_extract.py 重跑提取。", file=sys.stderr)
        return 3
    os.makedirs(a.out_dir, exist_ok=True)
    print("整理到 %s/：" % a.out_dir)

    # ---- hours.csv
    out_rounds = collections.Counter()
    hour_of = {}
    starts = collections.defaultdict(list)
    for r in hourly:
        starts[r["device"]].append(int(r["hour_start"]))
    for dev in starts:
        starts[dev].sort()
    import bisect
    outl = outliers(a.scores)
    for dev, rt in outl:
        s = starts.get(dev, [])
        i = bisect.bisect_right(s, rt) - 1
        if i >= 0 and rt < s[i] + 3600:
            out_rounds[(dev, s[i])] += 1
        else:
            hour_of[(dev, rt)] = None                         # 落在没有读数的小时里（不应发生）/ should not happen
    ctx = collections.defaultdict(lambda: [0, 0, None])
    for r in rows(a.streams):
        dev, we = r["device"], int(r["window_end"])
        s = starts.get(dev, [])
        i = bisect.bisect_left(s, we) - 1                     # 窗口末落在 (h 起点, h 终点] / window end in (start, end]
        if i < 0 or we > s[i] + 3600:
            continue
        c = ctx[(dev, s[i])]
        c[0] += 1
        c[1] += int(r["above_threshold"])
        v = float(r["main_score"])
        c[2] = v if c[2] is None or v > c[2] else c[2]
    hours = []
    for r in hourly:
        key = (r["device"], int(r["hour_start"]))
        c = ctx.get(key, [0, 0, None])
        row = [r["device"], key[1], r["local_hour"], int(r["rounds"]), int(r["admitted"]), int(r["guard_rounds"]),
               out_rounds.get(key, 0), c[0], c[1], "" if c[2] is None else c[2]]
        for ch in SYNC:
            row += [int(r["pn_" + ch]), float(r["ps_" + ch])]
        hours.append(row)
    head = ["device", "hour_start", "local_hour", "rounds", "admitted", "guard_rounds", "outlier_rounds", "ctx_windows",
            "ctx_above", "ctx_max"]
    for ch in SYNC:
        head += ["n_" + ch, "sum_" + ch]
    write(os.path.join(a.out_dir, "hours.csv"), head, sorted(hours, key=lambda x: (x[0], x[1])))
    if hour_of:
        print("  注意：%d 条离群轮落在提取没有覆盖的小时里" % len(hour_of))

    # ---- perf_events.csv、perf_elements.csv
    pe = [[r["device"], int(r["ts"]), r["level"], float(r["threshold"]), float(r["magnitude"]), float(r["hist_sd"])]
          for r in rows(a.perf)]
    write(os.path.join(a.out_dir, "perf_events.csv"), ["device", "ts", "level", "threshold", "magnitude", "hist_sd"],
          sorted(pe, key=lambda x: (x[0], x[1])))
    el = [[r["device"], int(r["window_end"]), float(r["main_score"])] for r in rows(a.streams)]
    write(os.path.join(a.out_dir, "perf_elements.csv"), ["device", "ts", "main_score"], sorted(el, key=lambda x: (x[0], x[1])))

    # ---- data_days.csv
    quality = {(r["device"], r["day"]): r["evaluable"] == "1" for r in rows(os.path.join(m5, "extract", "quality.csv"))
               if r["variant"] == "inj"}
    stats = {(r["device"], r["day"], r["channel"]): r for r in rows(os.path.join(m5, "extract", "daily.csv"))
             if r["variant"] == "inj" and r["daykind"] == "local"}
    trans = {(r["device"], r["day"], r["channel"]): r["kind"] for r in rows(os.path.join(m5, "lm_transitions.csv"))
             if r["variant"] == "inj"}
    days = sorted({d for (_, d) in quality})
    dd = []
    for dev in sorted({d for (d, _) in quality}):
        for ch in CHANNELS:
            state = False
            for day in days:
                if (dev, day) not in quality:
                    continue
                k = trans.get((dev, day, ch), "")
                if k == "进入偏离":
                    state = True
                elif k == "回落":
                    state = False
                st = stats.get((dev, day, ch))
                dd.append([dev, day, ch, int(quality[(dev, day)]), int(state),
                           {"进入偏离": "DEVIATE", "回落": "RECOVER"}.get(k, ""),
                           "" if st is None else float(st["shift"]), "" if st is None else float(st["width_x"])])
    write(os.path.join(a.out_dir, "data_days.csv"),
          ["device", "day", "channel", "evaluable", "deviated", "transition", "shift", "width_x"], dd)

    # ---- bursts.csv（模拟发出时刻并与数据层离线结果核对）
    den = collections.defaultdict(dict)
    for r in rows(os.path.join(m5, "extract", "denominators.csv")):
        den[r["device"]][int(r["bin_end"])] = int(r["admitted"])
    num = collections.Counter()
    for dev, rt in outl:
        num[(dev, (rt // BIN) * BIN + BIN)] += 1
    sim = simulate_bursts(den, num, a.table)
    ref = {(r["device"], int(r["ts"])): r for r in rows(os.path.join(m5, "bursts.csv")) if r["variant"] == "inj"}
    simk = {(d, t): (s, p, du) for d, t, _, s, p, du in sim}
    tail = [k for k in ref if k not in simk]                  # 数据末尾、在线不会发出的爆发 / never emitted at the end
    bad = [k for k in simk if k not in ref or (int(ref[k]["size"]), float(ref[k]["peak"]), int(ref[k]["duration"])) != simk[k]]
    print("  爆发：模拟发出 %d 次，数据层离线 %d 次（其中 %d 次在数据末尾、在线不会发出），不一致 %d 次"
          % (len(sim), len(ref), len(tail), len(bad)))
    if bad:
        print("ERROR: 模拟的爆发与 docs/m5a/bursts.csv 不一致，例如 %s" % bad[:3], file=sys.stderr)
        return 4
    write(os.path.join(a.out_dir, "bursts.csv"), ["device", "ts", "emitted_at", "size", "peak", "duration"],
          sorted(sim, key=lambda x: (x[0], x[1])))

    # ---- refs.csv、freeze.csv
    rf = [[r["device"], r["channel"], float(r["p10"]), float(r["p90"])] for r in rows(os.path.join(m5, "extract", "refs.csv"))
          if r["channel"] in SYNC]
    write(os.path.join(a.out_dir, "refs.csv"), ["device", "channel", "p10", "p90"], sorted(rf))
    fz = [[r["device"], int(r["freeze_ts"])] for r in rows(os.path.join(ex, "freeze.csv"))]
    write(os.path.join(a.out_dir, "freeze.csv"), ["device", "freeze_ts"], sorted(fz))
    return 0


if __name__ == "__main__":
    sys.exit(main())
