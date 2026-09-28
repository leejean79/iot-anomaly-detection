#!/usr/bin/env python3
# ============================================================================
# check_dense_days.py
# 核查某台设备「一天远超 8,640 轮」的日子：多出来的轮是不是叠进来的拷贝块。做法是按时间戳对 10 取余
# 把当天的轮分成若干条「相位流」（正常设备每 10 秒采一次，相位固定），再看每条相位流的时间戳平移
# 若干整天后，能否在该设备其余数据里原样找到。
# Check a device's over-dense days: split the day's rounds into phase streams by timestamp mod 10,
# then test whether each stream's timestamps reappear, shifted by whole days, in the rest of the data.
#
# 依据：2026-09-28 裁决书第二节第 4 条（数据事实第十九条：H 设备恢复后日记录数约为三倍）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt。
# 2. 调用命令：
#      python3 eda/check_dense_days.py --data-dir <原始 CSV 目录> --device H \
#          --daily docs/dataset_daily_rounds.csv --search-days 90
#    不给 --days 时，自动取逐日表中该设备轮数超过 8,640 的日子；也可用 --days 2022-05-29,2022-05-30 指定。
#    --search-days 为搜索平移的范围（正负天数）。
# 3. 前置条件：原始 CSV 目录可读；自动选日时须先有 count_rounds.py 写出的全数据集逐日表。
# 4. 期望产出：每个密集日打印相位分布、每条相位流的轮数，以及该流平移多少天时与其余数据对上的
#    比例最高。某条流在某个平移下对上接近 100%，即为从那一天叠进来的拷贝块；可再用
#    check_shift_copy.py 核对数值。
# 5. 失败兜底：没有密集日时说明并退出码 0；目录下没有可识别的数据文件时退出码 2。
# ============================================================================
import argparse
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


def utc_epoch(day: str) -> int:
    return int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--device", required=True)
    ap.add_argument("--days")
    ap.add_argument("--daily")
    ap.add_argument("--search-days", type=int, default=90)
    ap.add_argument("--min-stream", type=int, default=500, help="相位流少于此轮数不单独核查")
    a = ap.parse_args()

    if a.days:
        days = a.days.split(",")
    elif a.daily:
        t = pd.read_csv(a.daily).set_index("device")
        days = [c for c in t.columns if t.loc[a.device, c] > 8640]
    else:
        ap.error("须给出 --days 或 --daily")
    if not days:
        print(f"设备 {a.device} 没有轮数超过 8,640 的日子。")
        return 0

    # 只读该设备的时间戳，全数据集 / timestamps of this device only, whole dataset
    stamps, n_data = [], 0
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
                                 names=config.CSV_COLUMNS, usecols=[0, 1], on_bad_lines="skip",
                                 engine="c", encoding="utf-8", encoding_errors="replace")
        except Exception:  # noqa: BLE001
            continue
        sel = df["DeviceId"].astype(str).str.strip() == a.device
        tt = pd.to_numeric(df["Time"][sel], errors="coerce").dropna().astype("int64")
        if len(tt):
            stamps.append(tt.to_numpy())
    if n_data == 0:
        print(f"ERROR: {a.data_dir} 下没有可识别的数据文件。", file=sys.stderr)
        return 2
    all_ts = np.unique(np.concatenate(stamps)) if stamps else np.array([], dtype=np.int64)
    print(f"设备 {a.device}：全数据集共 {len(all_ts)} 轮；核查 {len(days)} 个密集日")

    for d in days:
        lo = utc_epoch(d)
        day_ts = all_ts[(all_ts >= lo) & (all_ts < lo + DAY)]
        rest = np.setdiff1d(all_ts, day_ts, assume_unique=True)
        phases = pd.Series(day_ts % 10).value_counts().sort_index()
        print(f"\n== {d}：{len(day_ts)} 轮；时间戳对 10 取余的分布 "
              + "，".join(f"{p}:{n}" for p, n in phases.items()))
        for p, n in phases.items():
            if n < a.min_stream:
                continue
            s = day_ts[day_ts % 10 == p]
            best = []
            for k in range(-a.search_days, a.search_days + 1):
                if k == 0:
                    continue
                hit = np.isin(s + k * DAY, rest, assume_unique=True).mean()
                best.append((hit, k))
            best.sort(reverse=True)
            top = "；".join(f"平移 {k:+d} 天对上 {h:.1%}" for h, k in best[:3])
            print(f"  相位 {p}（{n} 轮）：{top}")
    print("\n读法：某条相位流**只在一个**平移下对上接近 100%，说明它的时间骨架来自那一天，可再用 "
          "check_shift_copy.py 核对数值；在许多平移下都对上，只说明采样网格很规则，没有诊断意义；"
          "各平移都只对上一成左右，说明不是拷贝块。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
