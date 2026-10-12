#!/usr/bin/env python3
# ============================================================================
# repeat_run_lengths.py
# 重复值游程守卫（开发决策第十条，2026-10-09 裁决第二节第 1 条）的阈值标定与离线预言核对。
# Threshold calibration and offline prediction check for the repeated-value run-length guard (decision 10).
#
# 做四件事 / four things（2026-10-12 裁决第二节起区分「底值游程」与「普通游程」）：
#   1. 底值集合：对每个检测通道，在八台设备合并的直方图里，取落在量程两端（最小值或最大值附近 --floor-edge 比例的
#      范围内，默认 5%）、且单个取值占比超过 --floor-share（默认 1%）的取值。预计温度、湿度、气压为空集；光照的
#      夜间底值与气体的饱和值落在其中。底值集合由设计会话在守卫报告里核对后冻结。
#   2. 逐设备逐检测通道，统计原始数据中「连续完全相同读数」的游程（以该通道的读数计，与在线守卫相同：缺失的轮
#      不计入；相邻两条读数相隔超过冷启动判据 cacheDepth × 10 秒 = 10000 秒时游程清零），分成普通游程（读数不在
#      底值集合中）与底值游程（读数在底值集合中）两类。
#   3. 阈值 = 普通游程的自然最长值 × 倍数（默认 2，向上取整），写出可直接放进 .env 的 SYN_STUCK_RUN_THRESHOLDS 与
#      SYN_STUCK_FLOOR_VALUES；底值游程只统计（最长值与达到阈值的次数），不参与定阈值。
#   4. 离线核对设计会话的预言「三次卡死注入在一个阈值游程内被标记，自然数据零误触」：按注入器的语义（区间内的
#      读数冻结为区间内第一条读数的值）改写真值文件中的卡死注入，重算游程，给出每次卡死在开始后多少轮、多少秒
#      被标记；同时统计自然数据中普通游程达到阈值的次数。
#   默认排除拷贝段 2022-06-12 23:00 至 06-20 00:00（数据事实第二十九条：六月中旬一周是平移十四天的近似拷贝，按
#   「非数据」处理），其余覆盖期即「干净参照段」。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt（原始 CSV 只在本地）。
# 2. 调用命令：
#      python3 eda/repeat_run_lengths.py \
#          --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
#          --inject-truth docs/m3_inject/inject-truth.csv --out-dir docs/stuck_guard
#    可选：--start 2022-02-11、--end 2022-07-26（不含；默认取数据事实第十条的真实覆盖期）、--multiplier 2、
#    --reset-gap-sec 10000（冷启动清零的间隔，等于缓存深度 1000 轮 × 标称周期 10 秒）、
#    --exclude 2022-06-12T23:00,2022-06-20T00:00（排除的时段，空串表示不排除）、--floor-share 0.01、--floor-edge 0.05。
# 3. 前置条件：原始 CSV 目录可读；--inject-truth 为注入运行的真值文件（可省略，省略时不做第 3 项）。
# 4. 期望产出：终端打印逐设备逐通道的自然最长游程（轮数、起始时刻、持续时间）与建议阈值、卡死注入的标记时刻；
#    --out-dir 下写出 floor_values.csv（各通道的底值集合与占比）、natural_runs.csv（普通游程与底值游程）、
#    stuck_check.csv、thresholds.env（SYN_STUCK_RUN_THRESHOLDS 与 SYN_STUCK_FLOOR_VALUES 两行）。
# 5. 失败兜底：没有可识别的数据文件时退出码 2；某台设备某通道没有读数时该格阈值为 0（守卫关闭）并在终端注明。
# ============================================================================
import argparse
import csv
import io
import math
import os
import sys
import warnings
from datetime import datetime, timezone

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from edalib import config                            # noqa: E402
from edalib.inventory import list_all_files          # noqa: E402
from edalib.scan import sniff_schema                 # noqa: E402

CHANNELS = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]
iso = lambda t: datetime.fromtimestamp(int(t), timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def utc_epoch(day):
    return int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def load(data_dir, lo, hi):
    """读取时段内五个检测通道的全部读数 / read all detection-channel readings in [lo, hi)."""
    parts, n_data = [], 0
    for path in list_all_files(data_dir):
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
        except OSError:
            continue
        has_header, looks_like_data, _ = sniff_schema(raw[: config.SNIFF_BYTES])
        if not looks_like_data:
            continue
        n_data += 1
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = pd.read_csv(io.BytesIO(raw), header=0 if has_header else None,
                                 names=config.CSV_COLUMNS, on_bad_lines="skip", engine="c",
                                 encoding="utf-8", encoding_errors="replace")
        except Exception:  # noqa: BLE001
            continue
        t = pd.to_numeric(df["Time"], errors="coerce")
        v = pd.to_numeric(df["Value"], errors="coerce")
        sens = df["Sensor"].astype(str).str.strip()
        keep = t.notna() & v.notna() & df["DeviceId"].notna() & sens.isin(CHANNELS) & (t >= lo) & (t < hi)
        if keep.any():
            parts.append(pd.DataFrame({"dev": df["DeviceId"][keep].astype(str).str.strip().to_numpy(),
                                       "ch": sens[keep].to_numpy(), "t": t[keep].astype("int64").to_numpy(),
                                       "v": v[keep].to_numpy()}))
    if n_data == 0:
        return None
    d = pd.concat(parts, ignore_index=True)
    # 同一设备同一通道同一时刻只留第一条（与装配器「保留首值」一致）/ keep the first per (dev, ch, t)
    return d.drop_duplicates(subset=["dev", "ch", "t"], keep="first").sort_values(["dev", "ch", "t"], kind="stable")


def runs(t, v, reset_gap):
    """逐条读数的游程长度（与在线守卫相同的规则）/ run length at each reading, same rules as the online guard."""
    n = len(v)
    if n == 0:
        return np.empty(0, dtype=np.int64)
    brk = np.ones(n, dtype=bool)                       # 新游程的起点 / start of a new run
    brk[1:] = (v[1:] != v[:-1]) | ((t[1:] - t[:-1]) > reset_gap)
    idx = np.arange(n)
    start = np.maximum.accumulate(np.where(brk, idx, 0))
    return idx - start + 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--start", default="2022-02-11")
    ap.add_argument("--end", default="2022-07-26")
    ap.add_argument("--multiplier", type=float, default=2.0)
    ap.add_argument("--reset-gap-sec", type=int, default=10000)
    ap.add_argument("--inject-truth", default="")
    ap.add_argument("--exclude", default="2022-06-12T23:00,2022-06-20T00:00")
    ap.add_argument("--floor-share", type=float, default=0.01)
    ap.add_argument("--floor-edge", type=float, default=0.05)
    ap.add_argument("--out-dir", required=True)
    a = ap.parse_args()
    os.makedirs(a.out_dir, exist_ok=True)

    d = load(a.data_dir, utc_epoch(a.start), utc_epoch(a.end))
    if d is None:
        print("ERROR: %s 下没有可识别的数据文件。" % a.data_dir, file=sys.stderr)
        return 2
    if a.exclude:
        x0, x1 = [int(datetime.strptime(x, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc).timestamp())
                  for x in a.exclude.split(",")]
        d = d[(d["t"] < x0) | (d["t"] >= x1)]
        print("已排除 %s 至 %s（拷贝段，按非数据处理）" % tuple(a.exclude.split(",")))
    devices = sorted(d["dev"].unique())
    series = {(dv, ch): (g["t"].to_numpy(), g["v"].to_numpy()) for (dv, ch), g in d.groupby(["dev", "ch"])}

    # 一、底值集合 / floor-value sets
    floors, frows = {}, []
    print("\n== 底值集合（量程两端 %.0f%% 范围内、单值占比超过 %.1f%%）==" % (100 * a.floor_edge, 100 * a.floor_share))
    for ch in CHANNELS:
        vals = d.loc[d["ch"] == ch, "v"]
        lo_v, hi_v = float(vals.min()), float(vals.max())
        span = hi_v - lo_v
        share = vals.value_counts(normalize=True)
        sel = share[(share > a.floor_share)
                    & ((share.index <= lo_v + a.floor_edge * span) | (share.index >= hi_v - a.floor_edge * span))]
        floors[ch] = sorted(float(x) for x in sel.index)
        for val in floors[ch]:
            frows.append({"channel": ch, "value": "%.10g" % val, "share": round(float(share[val]), 5),
                          "range_min": lo_v, "range_max": hi_v})
        print("  %-11s 量程 %g 至 %g；底值：%s" % (ch, lo_v, hi_v, "、".join(
            "%.10g（%.1f%%）" % (x, 100 * share[x]) for x in floors[ch]) or "空集"))
    with open(os.path.join(a.out_dir, "floor_values.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["channel", "value", "share", "range_min", "range_max"])
        w.writeheader()
        w.writerows(frows)

    # 二、普通游程的自然最长值与阈值；底值游程只统计 / ordinary-run thresholds; floor runs only counted
    rows, thr = [], {}
    print("\n== 普通游程的自然最长值（轮）与建议阈值（× %.1f，向上取整），时段 %s 至 %s（不含） =="
          % (a.multiplier, a.start, a.end))
    for dv in devices:
        thr[dv] = []
        for ch in CHANNELS:
            if (dv, ch) not in series:
                thr[dv].append(0)
                print("  %s %-11s 没有读数，阈值记 0（守卫关闭）" % (dv, ch))
                continue
            t, v = series[(dv, ch)]
            r = runs(t, v, a.reset_gap_sec)
            is_floor = np.isin(v, floors[ch])
            ro = np.where(is_floor, 0, r)
            i = int(np.argmax(ro))
            mx = int(ro[i])
            t0 = t[i - mx + 1]
            th = int(math.ceil(mx * a.multiplier))
            thr[dv].append(th)
            rf = np.where(is_floor, r, 0)
            mf = int(rf.max()) if is_floor.any() else 0
            rows.append({"device": dv, "channel": ch, "readings": len(v), "max_run": mx,
                         "run_start_utc": iso(t0), "run_duration_sec": int(t[i] - t0), "run_value": v[i],
                         "p999_run": int(np.percentile(r[~is_floor], 99.9)) if (~is_floor).any() else 0,
                         "threshold": th, "floor_share": round(float(is_floor.mean()), 4), "max_floor_run": mf,
                         "floor_runs_reaching_threshold": int(np.sum(rf == th)) if th > 0 else 0})
            print("  %s %-11s 普通游程最长 %6d 轮（%s 起，持续 %.1f 小时，值 %g）→ 阈值 %d；底值游程最长 %d 轮，达到阈值 %d 次"
                  % (dv, ch, mx, iso(t0), (t[i] - t0) / 3600.0, v[i], th, mf,
                     rows[-1]["floor_runs_reaching_threshold"]))
    with open(os.path.join(a.out_dir, "natural_runs.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    spec = ",".join("%s=%s" % (dv, "/".join(str(x) for x in thr[dv])) for dv in devices)
    fspec = ",".join("%s=%s" % (ch, "/".join("%.10g" % x for x in floors[ch])) for ch in CHANNELS if floors[ch])
    with open(os.path.join(a.out_dir, "thresholds.env"), "w", encoding="utf-8") as fh:
        fh.write('SYN_STUCK_RUN_THRESHOLDS="%s"\n' % spec)
        fh.write('SYN_STUCK_FLOOR_VALUES="%s"\n' % fspec)
    print("\n.env 两行：\nSYN_STUCK_RUN_THRESHOLDS=\"%s\"\nSYN_STUCK_FLOOR_VALUES=\"%s\"" % (spec, fspec))

    # 三、自然数据中普通游程达到阈值的次数（按构造应为 0）/ ordinary runs reaching the threshold (0 by construction)
    natural_hits = 0
    for (dv, ch), (t, v) in series.items():
        th = thr[dv][CHANNELS.index(ch)]
        if th > 0:
            natural_hits += int(np.sum((runs(t, v, a.reset_gap_sec) == th) & ~np.isin(v, floors[ch])))
    print("\n自然数据中普通游程达到阈值的次数：%d（阈值取普通游程自然最长值的 %.1f 倍，按构造应为 0）"
          % (natural_hits, a.multiplier))

    # 四、卡死注入的离线核对 / offline check of the stuck injections
    if a.inject_truth:
        out = []
        stuck = [r for r in csv.DictReader(open(a.inject_truth, encoding="utf-8")) if r["type"] == "stuck"]
        print("\n== 卡死注入的离线核对（%d 次）==" % len(stuck))
        for k, s in enumerate(stuck, 1):
            dv, ch, s0, s1 = s["device"], s["channel"], int(s["start_ts"]), int(s["end_ts"])
            t, v = series[(dv, ch)]
            v2 = v.copy()
            inside = (t >= s0) & (t < s1)
            if inside.any():
                v2[inside] = v[np.argmax(inside)]          # 冻结为区间内第一条读数 / freeze at the first reading
            r = runs(t, v2, a.reset_gap_sec)
            th = thr[dv][CHANNELS.index(ch)]
            if np.isin(v2[inside][:1], floors[ch]).any():
                print("  第 %d 次 %s %s：冻结值落在底值集合里，按底值游程处理，设备层面不打标" % (k, dv, ch))
                th = 0
            hit = np.where(inside & (r >= th))[0] if th > 0 else np.array([], dtype=int)
            n_in = int(inside.sum())
            if len(hit):
                i = int(hit[0])
                rec = {"no": k, "device": dv, "channel": ch, "start_utc": iso(s0), "readings_in_interval": n_in,
                       "threshold": th, "flagged": True, "rounds_to_flag": int(np.sum(inside[: i + 1])),
                       "sec_to_flag": int(t[i] - s0)}
                print("  第 %d 次 %s %s %s：区间内 %d 条读数，阈值 %d，开始后第 %d 轮（%.1f 分钟）被标记"
                      % (k, dv, ch, iso(s0), n_in, th, rec["rounds_to_flag"], rec["sec_to_flag"] / 60.0))
            else:
                rec = {"no": k, "device": dv, "channel": ch, "start_utc": iso(s0), "readings_in_interval": n_in,
                       "threshold": th, "flagged": False, "rounds_to_flag": "", "sec_to_flag": ""}
                print("  第 %d 次 %s %s %s：区间内 %d 条读数，阈值 %d，**未被标记**（区间内的读数不足一个阈值游程）"
                      % (k, dv, ch, iso(s0), n_in, th))
            out.append(rec)
        with open(os.path.join(a.out_dir, "stuck_check.csv"), "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(out[0].keys()))
            w.writeheader()
            w.writerows(out)
    print("\n输出：%s" % a.out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
