#!/usr/bin/env python3
# ============================================================================
# check_shift_copy.py
# 核查原始数据里一段日期是否是另一段日期平移后的拷贝：把较早一段的时间戳加上平移量，与较晚一段
# 按「设备 + 时间戳 + 传感器」对齐，统计时间戳能对上的比例、数值相同的比例、逐传感器的相关系数，
# 以及逐传感器的差值分布（用来区分「取整精度不同」与「加了扰动」）。
# Check whether one date range of the raw data is a time-shifted copy of another: align on
# (device, time, sensor) after shifting, then report alignment, equality, per-sensor correlation and
# the per-sensor difference distribution (rounding versus added perturbation).
#
# 起因：六月段逐日轮数表中 06-13 至 06-18 与 05-30 至 06-04 逐日相等；2026-09-28 裁决书要求对全数据集
# 排查（第二节）并表征差值分布（第三节）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt（含 matplotlib）。
# 2. 调用命令（两种用法）：
#    单对：
#      python3 eda/check_shift_copy.py --data-dir <原始 CSV 目录> \
#          --a-start 2022-05-30 --b-start 2022-06-13 --days 7
#    多对（读一遍原始文件核查候选清单里的全部日期对，写出拷贝段清单与差值图）：
#      python3 eda/check_shift_copy.py --data-dir <原始 CSV 目录> \
#          --pairs-csv docs/copy_candidates.csv \
#          --list-out docs/copy_segments.csv --diff-plot docs/figures/copy_diff_hist.png
#    --a-start / --b-start 为较早、较晚一段的起始日（UTC 日期零点），--days 为天数，平移量 = 两者之差。
#    --pairs-csv 取 find_copy_candidates.py 的输出（列 source_start、target_start、days）。
# 3. 前置条件：原始 CSV 目录可读；多对用法须先跑 count_rounds.py 与 find_copy_candidates.py。
# 4. 期望产出：每一对打印逐设备与逐传感器的对齐、相同比例、相关系数、差值统计；多对用法另写出
#    拷贝段清单 CSV 与差值直方图 PNG。时间戳全对上且连续通道相关系数接近 1 即为平移拷贝；时间戳
#    全对上而相关系数不高时，须与对照平移比较。
# 5. 失败兜底：目录下没有可识别的数据文件时退出码 2；某一对的任一段没有数据时跳过该对并注明。
# ============================================================================
import argparse
import csv
import io
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

DAY = 86400
# 连续通道：判断拷贝的依据。Accelerometer、MIC、IR 只有两三种取值，相同与否说明不了什么。
# Continuous channels decide; Accelerometer, MIC and IR take only two or three values.
CONTINUOUS = ["Gas", "Humidity", "Light", "Pressure", "Temperature", "RSSI"]


def utc_epoch(day: str) -> int:
    return int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def day_str(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d")


def read_rows(data_dir, ranges):
    """读一遍原始文件，只保留落在任一区间内的行 / one pass over the raw files, keeping rows in any range."""
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
        keep = t.notna() & df["DeviceId"].notna() & df["Sensor"].notna()
        df, t = df[keep], t[keep].astype("int64")
        inside = np.zeros(len(t), dtype=bool)
        for lo, hi in ranges:
            inside |= ((t >= lo) & (t < hi)).to_numpy()
        if inside.any():
            parts.append(pd.DataFrame({
                "dev": df["DeviceId"][inside].astype(str).str.strip().to_numpy(),
                "time": t[inside].to_numpy(),
                "sensor": df["Sensor"][inside].astype(str).str.strip().to_numpy(),
                "value": pd.to_numeric(df["Value"][inside], errors="coerce").to_numpy(),
            }))
    rows = (pd.concat(parts, ignore_index=True).drop_duplicates(["dev", "time", "sensor"])
            if parts else pd.DataFrame(columns=["dev", "time", "sensor", "value"]))
    return rows, n_data


def compare(rows, a0, b0, span):
    """对齐一对区间，返回较晚一段逐行的对齐结果 / align one pair; one row per later-range row."""
    early = rows[(rows["time"] >= a0) & (rows["time"] < a0 + span)].copy()
    late = rows[(rows["time"] >= b0) & (rows["time"] < b0 + span)].copy()
    if early.empty or late.empty:
        return None
    early["time"] += b0 - a0                        # 平移到较晚一段 / shift onto the later range
    m = late.merge(early, on=["dev", "time", "sensor"], how="left", suffixes=("", "_a"),
                   indicator=True)
    m["key_ok"] = m["_merge"] == "both"
    m["same"] = m["key_ok"] & ((m["value"] == m["value_a"])
                               | (m["value"].isna() & m["value_a"].isna()))
    m["diff"] = m["value"] - m["value_a"]
    return m


def corr_of(g):
    ok = g["value"].notna() & g["value_a"].notna()
    return g.loc[ok, "value"].corr(g.loc[ok, "value_a"]) if ok.sum() > 100 else float("nan")


def report(m, a0, b0, days):
    """逐设备判定是否拷贝，再对被判为拷贝的设备做逐传感器统计。返回 (清单行, 拷贝设备的行)。
    Decide per device, then give per-sensor statistics over the copied devices only.

    判定必须逐设备：各设备基线水平不同，混在一起算相关系数会虚高；而一台按完美网格采样、却未被拷贝的
    设备，又会把混算的最小值拉低、掩盖其他设备的拷贝。
    The verdict is per device: pooling inflates correlation through level differences, while one
    uncopied device on a perfect sampling grid would mask the copied ones in a pooled minimum.
    """
    shift = (b0 - a0) // DAY
    print(f"\n==== {day_str(a0)} 起 {days} 天，平移 {shift} 天后对齐 {day_str(b0)} 起 {days} 天 ====")
    both = m[m["key_ok"]]
    print(f"{'设备':<4}{'较晚一段行数':>12}{'时间戳对上':>12}{'数值相同':>12}"
          f"{'连续通道相关系数最小':>14}  判定")
    copied = []
    for d, g in m.groupby("dev"):
        key = g["key_ok"].mean()
        cs = [corr_of(x) for sen, x in both[both["dev"] == d].groupby("sensor") if sen in CONTINUOUS]
        cs = [c for c in cs if c == c]
        cmin = min(cs) if cs else float("nan")
        is_copy = key > 0.99 and bool(cs) and cmin > 0.99
        if is_copy:
            copied.append(d)
        print(f"{d:<6}{len(g):>12}{key:>12.2%}{g['same'].mean():>12.2%}{cmin:>14.3f}  "
              f"{'拷贝' if is_copy else '-'}")
    key, same = m["key_ok"].mean(), m["same"].mean()
    print(f"合计：{len(m)} 行，时间戳对上 {key:.2%}，数值相同 {same:.2%}；"
          f"判为拷贝的设备：{'、'.join(copied) or '无'}")

    out = {"source_start": day_str(a0), "source_end": day_str(a0 + days * DAY),
           "target_start": day_str(b0), "target_end": day_str(b0 + days * DAY),
           "shift_days": shift, "rows": len(m), "key_pct": round(100 * key, 3),
           "same_pct": round(100 * same, 3), "copied_devices": ";".join(copied),
           "copy": bool(copied)}
    cp = both[both["dev"].isin(copied)]
    if cp.empty:
        return out, cp
    # 以下只统计被判为拷贝的设备；差值只看数值不同的行，它们刻画的是「改动」本身。
    # Over copied devices only; differences over differing rows, which characterise the change.
    print(f"\n（仅被判为拷贝的设备）\n{'传感器':<16}{'行数':>9}{'数值相同':>9}{'相关系数最小':>10}"
          f"{'差值中位数':>12}{'最大绝对差':>12}{'最常见的绝对差':>24}")
    for sen, g in cp.groupby("sensor"):
        cs = [c for c in (corr_of(x) for _, x in g.groupby("dev")) if c == c]
        cmin = min(cs) if cs else float("nan")
        ok = g["value"].notna() & g["value_a"].notna()
        dif = g.loc[ok & ~g["same"], "diff"]
        med = dif.median() if len(dif) else 0.0
        mx = dif.abs().max() if len(dif) else 0.0
        common = dif.abs().round(6).value_counts().head(3)
        common_s = ", ".join(f"{v:g}({c})" for v, c in common.items()) or "-"
        print(f"{sen:<18}{len(g):>9}{g['same'].mean():>9.2%}{cmin:>10.3f}"
              f"{med:>12.4g}{mx:>12.4g}  {common_s}")
        if sen in CONTINUOUS:
            out[f"corr_{sen}"] = round(cmin, 4) if cmin == cmin else ""
            out[f"maxabsdiff_{sen}"] = round(float(mx), 6)
    return out, cp


def plot_diffs(frames, out_png):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    both = pd.concat(frames, ignore_index=True)
    both = both[both["value"].notna() & both["value_a"].notna()]
    sensors = [s for s in CONTINUOUS if s in set(both["sensor"])]
    fig, axes = plt.subplots(1, len(sensors), figsize=(3.2 * len(sensors), 3.2))
    axes = np.atleast_1d(axes)
    for ax, sen in zip(axes, sensors):
        d = both.loc[both["sensor"] == sen, "diff"]
        lim = d.abs().quantile(0.999) or 1e-9
        ax.hist(d.clip(-lim, lim), bins=81, color="#2a78d6")
        ax.set_title(f"{sen}  (n={len(d)}, {(d == 0).mean():.0%} zero)", fontsize=9, loc="left")
        ax.set_xlabel("copy - source")
        ax.set_yscale("log")
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
    axes[0].set_ylabel("rows (log)")
    # 图中文字用英文：matplotlib 默认字体没有中文字形 / English labels: no CJK glyphs by default
    fig.suptitle("Value difference, copy minus shifted source, per continuous sensor", fontsize=10)
    fig.tight_layout()
    os.makedirs(os.path.dirname(out_png) or ".", exist_ok=True)
    fig.savefig(out_png, dpi=150)
    print(f"\n差值直方图已写出：{out_png}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--a-start")
    ap.add_argument("--b-start")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--pairs-csv")
    ap.add_argument("--list-out")
    ap.add_argument("--diff-plot")
    a = ap.parse_args()

    pairs = []
    if a.pairs_csv:
        with open(a.pairs_csv, newline="") as fh:
            for r in csv.DictReader(fh):
                pairs.append((utc_epoch(r["source_start"]), utc_epoch(r["target_start"]),
                              int(r["days"])))
    elif a.a_start and a.b_start:
        pairs.append((utc_epoch(a.a_start), utc_epoch(a.b_start), a.days))
    else:
        ap.error("须给出 --a-start 与 --b-start，或 --pairs-csv")
    ranges = [(s, s + d * DAY) for s, _, d in pairs] + [(t, t + d * DAY) for _, t, d in pairs]

    rows, n_data = read_rows(a.data_dir, ranges)
    if n_data == 0:
        print(f"ERROR: {a.data_dir} 下没有可识别的数据文件。", file=sys.stderr)
        return 2

    results, frames = [], []
    for a0, b0, days in pairs:
        m = compare(rows, a0, b0, days * DAY)
        if m is None:
            print(f"\n{day_str(a0)} → {day_str(b0)}：较早或较晚的一段没有数据，跳过。")
            continue
        row, cp = report(m, a0, b0, days)
        results.append(row)
        frames.append(cp)

    print("\n判据（逐设备）：时间戳对上 > 99% 且各连续通道相关系数全部 > 0.99 记为拷贝。")
    for r in results:
        print(f"  {r['source_start']} → {r['target_start']}（平移 {r['shift_days']} 天）："
              f"{'拷贝设备 ' + r['copied_devices'].replace(';', '、') if r['copy'] else '不满足判据'}")

    if a.list_out and results:
        cols = sorted({k for r in results for k in r}, key=lambda k: (k.startswith(("corr_", "max")), k))
        os.makedirs(os.path.dirname(a.list_out) or ".", exist_ok=True)
        with open(a.list_out, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols)
            w.writeheader()
            w.writerows(results)
        print(f"拷贝段清单已写出：{a.list_out}")
    if a.diff_plot:
        copied = [f for f, r in zip(frames, results) if r["copy"]]   # 只含拷贝设备的行 / copied rows
        if copied:
            plot_diffs(copied, a.diff_plot)
        else:
            print("没有满足判据的拷贝段，不画差值图。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
