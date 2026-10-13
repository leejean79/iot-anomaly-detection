#!/usr/bin/env python3
# ============================================================================
# repeat_run_lengths.py
# 重复值游程守卫（开发决策第十条，2026-10-09 裁决第二节第 1 条）的阈值标定与离线预言核对。
# Threshold calibration and offline prediction check for the repeated-value run-length guard (decision 10).
#
# 做四件事（2026-10-12 裁决第二节区分「底值游程」与「普通游程」；2026-10-13 裁决第四节改为逐设备统计）：
#   1. 底值集合，逐设备、逐通道：取值落在该台该通道干净段实测范围两端各 --floor-edge（默认 5%）之内，且单个取值占该台
#      该通道记录数的比例超过 --floor-share（默认 1%）。光照的夜间底值落在其中；为空集的通道只走普通游程规则。
#   2. 游程：连续完全相同读数的长度（以该通道的读数计；相邻两条读数相隔超过 --gap-sec（默认 60 秒，探索性分析的缺口
#      定义）即断开）。读数在该台该通道底值集合里的是底值游程，其余是普通游程。
#   3. 十大游程表与阈值：逐设备逐通道列出最长的十个普通游程（起始时刻、长度、读数值）；若第一名超过第二名的
#      --outlier-ratio 倍（默认 3），它是一次真实卡死的候选，不参与阈值推导，另行登记。阈值 = 剩余普通游程最大值 ×
#      --multiplier（默认 2，向上取整）。底值游程只统计，不参与定阈值。
#   4. 离线核对「三次卡死注入在一个阈值游程内被标记，自然数据零误触」：按注入器的语义（区间内的读数冻结为区间内第一条
#      读数的值）改写真值文件中的卡死注入，重算游程，给出每次卡死在开始后多少轮、多少秒被标记。
#   默认排除拷贝段 2022-06-12 23:00 至 06-20 00:00（数据事实第二十九条，按「非数据」处理），其余覆盖期即「干净参照段」。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt（原始 CSV 只在本地）。
# 2. 调用命令：
#      python3 eda/repeat_run_lengths.py \
#          --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
#          --inject-truth docs/m3_inject/inject-truth.csv --out-dir docs/stuck_guard
#    可选：--start 2022-02-11、--end 2022-07-26（不含；默认取数据事实第十条的真实覆盖期）、--multiplier 2、
#    --gap-sec 60（缺口断开游程的间隔）、--exclude 2022-06-12T23:00,2022-06-20T00:00（排除的时段，空串表示不排除）、
#    --floor-share 0.01、--floor-edge 0.05、--outlier-ratio 3。
# 3. 前置条件：原始 CSV 目录可读；--inject-truth 为注入运行的真值文件（可省略，省略时不做第 3 项）。
# 4. 期望产出：终端打印逐设备逐通道的自然最长游程（轮数、起始时刻、持续时间）与建议阈值、卡死注入的标记时刻；
#    --out-dir 下写出 floor_values.csv（逐设备逐通道的底值集合与占比）、top10_runs.csv（十大普通游程，标出真实卡死候选）、
#    natural_runs.csv（阈值与底值游程统计）、stuck_check.csv、thresholds.env（SYN_STUCK_RUN_THRESHOLDS 与
#    SYN_STUCK_FLOOR_VALUES 两行）。
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


def runs(t, v, gap):
    """逐条读数的游程长度（与在线守卫相同的规则）/ run length at each reading, same rules as the online guard."""
    n = len(v)
    if n == 0:
        return np.empty(0, dtype=np.int64)
    brk = np.ones(n, dtype=bool)                       # 新游程的起点 / start of a new run
    brk[1:] = (v[1:] != v[:-1]) | ((t[1:] - t[:-1]) > gap)
    idx = np.arange(n)
    start = np.maximum.accumulate(np.where(brk, idx, 0))
    return idx - start + 1


def run_ends(t, v, r, gap):
    """每个游程的末位下标 / index of the last reading of each run."""
    n = len(v)
    nxt_break = np.ones(n, dtype=bool)
    nxt_break[:-1] = (v[1:] != v[:-1]) | ((t[1:] - t[:-1]) > gap)
    return np.where(nxt_break)[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--start", default="2022-02-11")
    ap.add_argument("--end", default="2022-07-26")
    ap.add_argument("--multiplier", type=float, default=2.0)
    ap.add_argument("--gap-sec", type=int, default=60)
    ap.add_argument("--inject-truth", default="")
    ap.add_argument("--exclude", default="2022-06-12T23:00,2022-06-20T00:00")
    ap.add_argument("--floor-share", type=float, default=0.01)
    ap.add_argument("--floor-edge", type=float, default=0.05)
    ap.add_argument("--outlier-ratio", type=float, default=3.0)
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

    # 一、逐设备逐通道的底值集合 / per-device floor sets
    floors, frows = {}, []
    print("\n== 底值集合（逐设备逐通道：该台实测范围两端 %.0f%% 内、单值占比超过 %.1f%%）=="
          % (100 * a.floor_edge, 100 * a.floor_share))
    for (dv, ch), (t, v) in sorted(series.items()):
        lo_v, hi_v = float(v.min()), float(v.max())
        span = hi_v - lo_v
        vals, cnt = np.unique(v, return_counts=True)
        share = cnt / len(v)
        sel = (share > a.floor_share) & ((vals <= lo_v + a.floor_edge * span) | (vals >= hi_v - a.floor_edge * span))
        floors[(dv, ch)] = [float(x) for x in vals[sel]]
        for x, sh in zip(vals[sel], share[sel]):
            frows.append({"device": dv, "channel": ch, "value": "%.10g" % x, "share": round(float(sh), 5),
                          "range_min": lo_v, "range_max": hi_v})
        if floors[(dv, ch)]:
            print("  %s %-11s 范围 %g 至 %g；底值：%s" % (dv, ch, lo_v, hi_v, "、".join(
                "%.10g（%.1f%%）" % (x, 100 * sh) for x, sh in zip(vals[sel], share[sel]))))
    print("  其余设备通道为空集。")
    with open(os.path.join(a.out_dir, "floor_values.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["device", "channel", "value", "share", "range_min", "range_max"])
        w.writeheader()
        w.writerows(frows)

    # 二、十大普通游程、真实卡死候选与阈值 / top-10 ordinary runs, stuck candidates, thresholds
    rows, top_rows, thr, cand = [], [], {}, []
    for dv in devices:
        thr[dv] = []
        for ch in CHANNELS:
            if (dv, ch) not in series:
                thr[dv].append(0)
                continue
            t, v = series[(dv, ch)]
            r = runs(t, v, a.gap_sec)
            ends = run_ends(t, v, r, a.gap_sec)
            is_floor_end = np.isin(v[ends], floors[(dv, ch)])
            ord_ends = ends[~is_floor_end]
            order = ord_ends[np.argsort(-r[ord_ends], kind="stable")]
            top = order[:10]
            lens = [int(r[i]) for i in top]
            excluded = len(lens) >= 2 and lens[0] > a.outlier_ratio * lens[1]
            for k, i in enumerate(top, 1):
                is_cand = excluded and k == 1
                top_rows.append({"device": dv, "channel": ch, "rank": k, "length": int(r[i]),
                                 "start_utc": iso(t[i - r[i] + 1]), "duration_sec": int(t[i] - t[i - r[i] + 1]),
                                 "value": "%.10g" % v[i], "stuck_candidate": is_cand})
                if is_cand:
                    cand.append(top_rows[-1])
            base = lens[1] if excluded else (lens[0] if lens else 0)
            th = int(math.ceil(base * a.multiplier))
            thr[dv].append(th)
            fl_ends = ends[is_floor_end]
            mf = int(r[fl_ends].max()) if len(fl_ends) else 0
            rows.append({"device": dv, "channel": ch, "readings": len(v), "max_ordinary_run": lens[0] if lens else 0,
                         "second_ordinary_run": lens[1] if len(lens) > 1 else 0, "top1_excluded": excluded,
                         "threshold": th, "floor_values": "/".join("%.10g" % x for x in floors[(dv, ch)]),
                         "floor_share": round(float(np.isin(v, floors[(dv, ch)]).mean()), 4),
                         "max_floor_run": mf, "floor_runs_reaching_threshold": int(np.sum(r[fl_ends] >= th)) if th else 0})
    print("\n== 阈值（剩余普通游程最大值 × %.1f，向上取整；轮）==" % a.multiplier)
    for ch in CHANNELS:
        print("  %-11s %s" % (ch, "  ".join("%s=%d" % (dv, thr[dv][CHANNELS.index(ch)]) for dv in devices)))
    print("\n真实卡死候选（第一名超过第二名 %.0f 倍、读数不在底值集合里，不参与阈值推导）：%s"
          % (a.outlier_ratio, "；".join("%s %s %s 起 %d 轮 值 %s" % (c["device"], c["channel"], c["start_utc"],
                                                                 c["length"], c["value"]) for c in cand) or "无"))
    for name, data in (("top10_runs.csv", top_rows), ("natural_runs.csv", rows)):
        with open(os.path.join(a.out_dir, name), "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(data[0].keys()))
            w.writeheader()
            w.writerows(data)
    spec = ",".join("%s=%s" % (dv, "/".join(str(x) for x in thr[dv])) for dv in devices)
    fspec = ",".join("%s.%s=%s" % (dv, ch, "/".join("%.10g" % x for x in floors[(dv, ch)]))
                     for dv in devices for ch in CHANNELS if floors.get((dv, ch)))
    with open(os.path.join(a.out_dir, "thresholds.env"), "w", encoding="utf-8") as fh:
        fh.write('SYN_STUCK_RUN_THRESHOLDS="%s"\n' % spec)
        fh.write('SYN_STUCK_FLOOR_VALUES="%s"\n' % fspec)
    print("\n.env 两行：\nSYN_STUCK_RUN_THRESHOLDS=\"%s\"\nSYN_STUCK_FLOOR_VALUES=\"%s\"" % (spec, fspec))

    # 三、自然数据中普通游程达到阈值的次数（排除候选后按构造应只剩候选本身）/ natural hits
    hits = 0
    for (dv, ch), (t, v) in series.items():
        th = thr[dv][CHANNELS.index(ch)]
        if th > 0:
            hits += int(np.sum((runs(t, v, a.gap_sec) == th) & ~np.isin(v, floors[(dv, ch)])))
    print("\n自然数据中普通游程达到阈值的次数：%d（应等于真实卡死候选的个数 %d）" % (hits, len(cand)))

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
            r = runs(t, v2, a.gap_sec)
            th = thr[dv][CHANNELS.index(ch)]
            if np.isin(v2[inside][:1], floors[(dv, ch)]).any():
                print("  第 %d 次 %s %s：冻结值落在底值集合里，按底值游程处理，设备层面不打标" % (k, dv, ch))
                th = 0
            hit = np.where(inside & (r >= th))[0] if th > 0 else np.array([], dtype=int)
            n_in = int(inside.sum())
            rec = {"no": k, "device": dv, "channel": ch, "start_utc": iso(s0), "readings_in_interval": n_in,
                   "threshold": th, "flagged": bool(len(hit)),
                   "rounds_to_flag": int(np.sum(inside[: hit[0] + 1])) if len(hit) else "",
                   "sec_to_flag": int(t[hit[0]] - s0) if len(hit) else ""}
            print("  第 %d 次 %s %s %s：区间内 %d 条读数，阈值 %d，%s" % (
                k, dv, ch, iso(s0), n_in, th, ("开始后第 %d 轮（%.1f 分钟）被标记" % (rec["rounds_to_flag"],
                                                                         rec["sec_to_flag"] / 60.0))
                if len(hit) else "**未被标记**"))
            out.append(rec)
        with open(os.path.join(a.out_dir, "stuck_check.csv"), "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(out[0].keys()))
            w.writeheader()
            w.writerows(out)
    print("\n输出：%s" % a.out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
