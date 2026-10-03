#!/usr/bin/env python3
# ============================================================================
# m3_window_gap_check.py
# 把上下文通道的告警窗口与缺轮位置对齐，看告警是否集中在跨越缺口的窗口上（设计会话 2026-10-03 回复：
# B 训练最短、缺轮最多、告警率最高，怀疑「缝合」窗口——窗口由连续收到的轮拼成，缺轮处相邻两轮之间有
# 时间跳跃——既污染训练，也在评分时制造虚假告警）。
# Align context-channel alarm windows with missing rounds: are alarms concentrated on windows that
# span a gap? (Design-session reply of 2026-10-03: "stitched" windows across missing rounds.)
#
# 两种分辨率 / two resolutions:
#   粗：只用评分记录。每个窗口恰好 60 个轮，不缺轮时跨 600 秒；相邻两条记录的窗口末之差减 600 秒就是
#       窗口内缺失的时长，误差为一个滑动步（60 秒），所以只看得见缺失不少于 60 秒的窗口。
#   细：另给原始 CSV 目录（只在本地 Mac 上有），逐窗口找出相邻两轮之间的最大间隔。窗口 k 的轮落在
#       [上一窗口末 − 60 秒, 本窗口末) 内（轮按到达的滑动步记窗口末），区间两端的滑动步可能混入相邻窗口
#       的至多 5 个轮，这一近似只影响缺口恰好落在边界滑动步上的少数窗口。
#   Coarse: from score records only (a window holds exactly 60 rounds, 600 s without gaps; resolution
#   one slide, 60 s). Fine: with the raw CSV directory, the largest gap between consecutive rounds in
#   each window's time range [previous window end - 60 s, window end).
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt。不需要集群，也不需要重放。
# 2. 调用命令：
#      python3 eda/m3_window_gap_check.py \
#          --scores docs/m3_march/m3_scores.jsonl \
#          --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
#          --out-dir docs/reports/m3_window_gap_check
#    --data-dir 省略时只做粗分辨率。可选：--devices B（默认全部八台，便于拿其他设备作对照）、
#    --gap-sec 15（相邻两轮间隔超过它记为缺口；正常间隔约 10 秒）。
# 3. 前置条件：--scores 是某次运行的上下文通道记录（m3_context）；--data-dir 是 EDA 用的原始 CSV 目录。
# 4. 期望产出：终端与 --out-dir/gap_check.md 打印逐设备表（跨缺口窗口数、两类窗口的告警率、告警落在
#    跨缺口窗口上的比例、按最大间隔分档的告警率）；--out-dir/gap_windows.csv 为逐窗口明细。
# 5. 失败兜底：--scores 不存在或没有上下文通道记录时退出码 2；--data-dir 下没有可识别的数据文件时
#    退出码 3。
# ============================================================================
import argparse
import bisect
import collections
import csv
import io
import json
import os
import sys
import warnings

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

WINDOW_SEC = 600      # 60 轮 × 10 秒 / 60 rounds x 10 s
SLIDE_SEC = 60
BINS = [(0, 15, "≤15 秒"), (15, 60, "15–60 秒"), (60, 600, "1–10 分钟"), (600, 10 ** 12, "超过 10 分钟")]


def load_scores(path, devices):
    recs = collections.defaultdict(list)
    for line in open(path, encoding="utf-8", errors="replace"):
        try:
            o = json.loads(line)
        except ValueError:
            continue
        if o.get("channel") != "m3_context" or (devices and o["device"] not in devices):
            continue
        recs[o["device"]].append((int(o["windowEnd"]), bool(o["aboveThreshold"])))
    return {d: sorted(v) for d, v in recs.items()}


def load_rounds(data_dir, devices, lo, hi):
    """原始 CSV 中的唯一 (设备, 时间戳)，口径与 count_rounds.py 相同 / unique (device, ts), as count_rounds.py."""
    import pandas as pd
    from edalib import config
    from edalib.inventory import list_all_files
    from edalib.scan import sniff_schema
    out, n_data = collections.defaultdict(set), 0
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
        dev = df["DeviceId"].astype(str).str.strip()
        sel = t.notna() & (t >= lo) & (t < hi) & dev.isin(devices)
        for d, tt in zip(dev[sel], t[sel].astype("int64")):
            out[d].add(int(tt))
    return {d: sorted(v) for d, v in out.items()}, n_data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scores", required=True)
    ap.add_argument("--data-dir", default="")
    ap.add_argument("--devices", default="")
    ap.add_argument("--gap-sec", type=int, default=15)
    ap.add_argument("--out-dir", required=True)
    a = ap.parse_args()
    if not os.path.isfile(a.scores):
        print("ERROR: 找不到 %s" % a.scores, file=sys.stderr)
        return 2
    want = set(a.devices.split(",")) if a.devices else set()
    recs = load_scores(a.scores, want)
    if not recs:
        print("ERROR: %s 中没有上下文通道记录" % a.scores, file=sys.stderr)
        return 2
    devices = sorted(recs)
    fine = bool(a.data_dir)
    rounds = {}
    if fine:
        lo = min(v[0][0] for v in recs.values()) - 3600
        hi = max(v[-1][0] for v in recs.values()) + 1
        rounds, n_data = load_rounds(a.data_dir, set(devices), lo, hi)
        if n_data == 0:
            print("ERROR: %s 下没有可识别的数据文件" % a.data_dir, file=sys.stderr)
            return 3

    os.makedirs(a.out_dir, exist_ok=True)
    detail = []
    for d in devices:
        r = recs[d]
        ts = rounds.get(d, [])
        for (prev, _), (we, al) in zip(r, r[1:]):
            missing = max(0, we - prev - WINDOW_SEC)      # 粗：窗口内缺失的时长（±60 秒）/ coarse missing time
            max_gap, n_in = None, None
            if fine:
                i0, i1 = bisect.bisect_left(ts, prev - SLIDE_SEC), bisect.bisect_left(ts, we)
                seg = ts[i0:i1]
                n_in = len(seg)
                max_gap = max((y - x for x, y in zip(seg, seg[1:])), default=None)
            detail.append({"device": d, "windowEnd": we, "alarm": int(al), "coarse_missing_sec": missing,
                           "rounds_in_range": n_in, "max_gap_sec": max_gap})

    with open(os.path.join(a.out_dir, "gap_windows.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(detail[0].keys()))
        w.writeheader()
        w.writerows(detail)

    def table(title, is_gap, note):
        L = ["## %s" % title, "", note, "",
             "| 设备 | 窗口数 | 跨缺口窗口 | 跨缺口窗口告警率 | 其余窗口告警率 | 比值 | 告警落在跨缺口窗口上的比例 |",
             "| --- | --- | --- | --- | --- | --- | --- |"]
        for d in devices:
            rows = [x for x in detail if x["device"] == d]
            g = [x for x in rows if is_gap(x)]
            n = [x for x in rows if not is_gap(x)]
            rg = sum(x["alarm"] for x in g) / len(g) if g else None
            rn = sum(x["alarm"] for x in n) / len(n) if n else None
            tot = sum(x["alarm"] for x in rows)
            L.append("| %s | %d | %d（%.1f%%） | %s | %s | %s | %s |" % (
                d, len(rows), len(g), 100.0 * len(g) / len(rows) if rows else 0,
                "-" if rg is None else "%.3f" % rg, "-" if rn is None else "%.3f" % rn,
                "%.2f" % (rg / rn) if rg is not None and rn else "-",
                "%.2f" % (sum(x["alarm"] for x in g) / tot) if tot else "-"))
        return L + [""]

    L = ["# 上下文通道告警窗口与缺轮位置的对齐", "",
         "- 评分记录：%s；设备：%s。" % (a.scores, "、".join(devices)),
         "- 判读口径（暂定）：跨缺口窗口的告警率达到其余窗口的 2 倍以上，记为「告警集中在跨缺口窗口上」。", ""]
    L += table("粗分辨率：窗口内缺失不少于 60 秒", lambda x: x["coarse_missing_sec"] >= 120,
               "窗口末之差减 600 秒为缺失时长；滑动步取整带来 ±60 秒误差，所以门槛取 120 秒（至少缺 60 秒）。")
    if fine:
        L += table("细分辨率：窗口内相邻两轮的最大间隔超过 %d 秒" % a.gap_sec,
                   lambda x: x["max_gap_sec"] is not None and x["max_gap_sec"] > a.gap_sec,
                   "按原始 CSV 的唯一 (设备, 时间戳) 计算；正常间隔约 10 秒。")
        L += ["## 细分辨率：按窗口内最大间隔分档的告警率", "",
              "| 设备 | " + " | ".join(b[2] for b in BINS) + " |", "| --- | " + " | ".join("---" for _ in BINS) + " |"]
        for d in devices:
            cells = []
            for lo_, hi_, _ in BINS:
                xs = [x for x in detail if x["device"] == d and x["max_gap_sec"] is not None
                      and lo_ < x["max_gap_sec"] <= hi_] if lo_ else \
                     [x for x in detail if x["device"] == d and x["max_gap_sec"] is not None and x["max_gap_sec"] <= hi_]
                cells.append("%.3f（%d 窗）" % (sum(x["alarm"] for x in xs) / len(xs), len(xs)) if xs else "-")
            L.append("| %s | %s |" % (d, " | ".join(cells)))
        L.append("")
    else:
        L += ["未提供 --data-dir，只做了粗分辨率；缺失不足 60 秒的窗口在粗分辨率下看不见。", ""]
    text = "\n".join(L) + "\n"
    open(os.path.join(a.out_dir, "gap_check.md"), "w", encoding="utf-8").write(text)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
