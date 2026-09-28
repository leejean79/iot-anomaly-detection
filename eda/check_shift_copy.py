#!/usr/bin/env python3
# ============================================================================
# check_shift_copy.py
# 核查原始数据里一段日期是否是另一段日期平移后的逐行拷贝：把前一段的时间戳加上平移量，
# 与后一段按「设备 + 时间戳 + 传感器」对齐，统计时间戳能对上的比例与数值完全相同的比例。
# Check whether one date range of the raw data is a row-for-row copy of another, shifted in time:
# shift the earlier range's timestamps and align with the later range on (device, time, sensor).
#
# 起因：六月段逐日轮数表中，8 台设备 06-13 至 06-18 的轮数与 05-30 至 06-04 逐日完全相等，
# 06-19 恰为 06-05 减去 262 或 263 轮（即数据在 23:16 结束所少的约 44 分钟）。仅凭轮数不能断定是拷贝，
# 本脚本比较时间戳与数值本身。
# Motivation: per-day round counts for 06-13..06-18 equal those for 05-30..06-04 on all eight
# devices; counts alone cannot prove a copy, so this compares timestamps and values.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt。
# 2. 调用命令：
#      python3 eda/check_shift_copy.py \
#          --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
#          --a-start 2022-05-30 --b-start 2022-06-13 --days 7
#    --a-start 为较早一段的起始日，--b-start 为较晚一段的起始日（均为 UTC 日期零点），--days 为比较的
#    天数；平移量 = b-start − a-start。
# 3. 前置条件：原始 CSV 目录可读（与 count_rounds.py 相同的目录）。
# 4. 期望产出：逐设备打印较晚一段的行数、平移后时间戳能对上的比例、数值完全相同的比例；再逐传感器
#    打印数值相同的比例、两段数值的相关系数与不同取值数。两个比例都接近 100% 即为逐行拷贝；时间戳
#    对不上则不是平移拷贝；时间戳全对上而数值部分相同时，须与一段不怀疑拷贝的对照平移比较。
# 5. 失败兜底：目录下没有可识别的数据文件时退出码 2；任一段没有数据时退出码 3。
# ============================================================================
import argparse
import io
import os
import sys
import warnings
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from edalib import config                            # noqa: E402
from edalib.inventory import list_all_files          # noqa: E402
from edalib.scan import sniff_schema                 # noqa: E402

DAY = 86400


def utc_epoch(day: str) -> int:
    return int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--a-start", required=True)
    ap.add_argument("--b-start", required=True)
    ap.add_argument("--days", type=int, default=7)
    a = ap.parse_args()
    a0, b0 = utc_epoch(a.a_start), utc_epoch(a.b_start)
    span, shift = a.days * DAY, b0 - a0

    parts, n_data = [], 0
    for path in list_all_files(a.data_dir):
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
        keep = t.notna() & df["DeviceId"].notna() & df["Sensor"].notna()
        t = t[keep].astype("int64")
        inside = ((t >= a0) & (t < a0 + span)) | ((t >= b0) & (t < b0 + span))
        if inside.any():
            parts.append(pd.DataFrame({
                "dev": df["DeviceId"][keep][inside].astype(str).str.strip().to_numpy(),
                "time": t[inside].to_numpy(),
                "sensor": df["Sensor"][keep][inside].astype(str).str.strip().to_numpy(),
                "value": v[keep][inside].to_numpy(),
            }))
    if n_data == 0:
        print(f"ERROR: {a.data_dir} 下没有可识别的数据文件。", file=sys.stderr)
        return 2

    rows = pd.concat(parts, ignore_index=True).drop_duplicates(["dev", "time", "sensor"])
    early = rows[rows["time"] < a0 + span].copy()
    late = rows[rows["time"] >= b0].copy()
    if early.empty or late.empty:
        print("ERROR: 较早或较晚的一段没有数据。", file=sys.stderr)
        return 3
    early["time"] += shift                           # 平移到较晚一段 / shift onto the later range
    m = late.merge(early, on=["dev", "time", "sensor"], how="left", suffixes=("", "_a"),
                   indicator=True)
    m["key_ok"] = m["_merge"] == "both"             # 平移后能找到同一行 / the shifted row exists
    m["same"] = m["key_ok"] & ((m["value"] == m["value_a"]) | (m["value"].isna() & m["value_a"].isna()))

    print(f"比较：{a.a_start} 起 {a.days} 天，平移 {shift // DAY} 天后对齐 {a.b_start} 起 {a.days} 天")
    print(f"{'设备':<4}{'较晚一段行数':>12}{'时间戳对上':>12}{'数值相同':>12}")
    for d, g in m.groupby("dev"):
        n = len(g)
        print(f"{d:<6}{n:>12}{g['key_ok'].mean():>12.2%}{g['same'].mean():>12.2%}")
    key, same = m["key_ok"].mean(), m["same"].mean()
    print(f"合计：{len(m)} 行，时间戳对上 {key:.2%}，数值相同 {same:.2%}")

    # 逐传感器：数值相同的比例与两段数值的相关系数。准常数或粗量化的通道天然容易相同，
    # 连续通道若也大比例相同或相关系数接近 1，才说明数值被拷贝。
    # Per sensor: near-constant or coarse channels repeat naturally; copied values show up as
    # high equality or near-1 correlation on the continuous channels.
    both = m[m["key_ok"]]
    print(f"\n{'传感器':<16}{'行数':>10}{'数值相同':>10}{'相关系数':>10}{'不同取值数':>10}")
    for sen, g in both.groupby("sensor"):
        ok = g["value"].notna() & g["value_a"].notna()
        corr = g.loc[ok, "value"].corr(g.loc[ok, "value_a"]) if ok.sum() > 2 else float("nan")
        print(f"{sen:<18}{len(g):>10}{g['same'].mean():>10.2%}{corr:>10.3f}{g['value'].nunique():>10}")

    # 不自动下结论：时间戳全对上而数值部分相同时，需与一段不怀疑拷贝的对照平移比较才能判断。
    # No automatic verdict: when timestamps all align but values only partly match, the numbers must
    # be compared against a control shift between two periods not suspected of copying.
    if key > 0.99 and same > 0.99:
        print("\n结论：逐行拷贝（时间戳与数值都相同）。")
    elif key > 0.99:
        print("\n时间戳全部对上而数值部分相同：须与对照平移的结果比较后再判断，本脚本不下结论。")
    else:
        print("\n时间戳没有全部对上：不是整段平移拷贝。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
