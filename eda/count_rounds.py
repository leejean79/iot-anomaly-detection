#!/usr/bin/env python3
# ============================================================================
# count_rounds.py
# 按协调世界时（UTC）窗口，从原始 CSV 数出每台设备每天的采样轮数（唯一的 设备 + 时间戳），
# 作为重放完整性核验断言一的参照值。
# Count sampling rounds (unique device + timestamp) per device and UTC day in a UTC window,
# as the reference for assertion one of the replay-integrity check.
#
# 为什么需要它：EDA 已有的产出里，逐日表（uptime_matrix.csv）数的是传感器行数，不是轮数；
# 三月的参照值是用逐月表拼出来的，而六月段不是整月。本脚本读同一批原始文件，口径与 EDA 相同
# （同一个格式嗅探、同一套 read_csv 参数），只是把统计量换成逐日的唯一 (设备, 时间戳)。
# Why: the existing EDA daily table counts sensor rows, not rounds, and the June segment is not a
# whole month. This reads the same raw files with the same sniffing and parsing as the EDA.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt（pandas、numpy）。
# 2. 调用命令：
#      python3 eda/count_rounds.py \
#          --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
#          --start 2022-05-29 --end 2022-06-20 \
#          --measured 1103663 --tol 0.002 \
#          --out docs/june_segment_eda_rounds.csv
#    --start 含、--end 不含，均按 UTC 日期零点；--measured 为重放后实测的总轮数，给了就做对账；
#    --tol 为相对容差，默认千分之二。
# 3. 前置条件：原始 CSV 目录可读（即 EDA 全量扫描用的同一个目录）。
# 4. 期望产出：终端打印总轮数、逐设备轮数与窗口内最后一轮的时刻、对账结论；--out 处写出「设备 × UTC 日期 → 轮数」表。
# 5. 失败兜底：目录下没有可识别的数据文件时退出码 2；对账超出容差时退出码 1；个别文件解析失败
#    只计数并在末尾报告，不中断。
# ============================================================================
import argparse
import csv
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


def utc_epoch(day: str) -> int:
    return int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--start", required=True, help="UTC 日期，含 / inclusive, YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="UTC 日期，不含 / exclusive, YYYY-MM-DD")
    ap.add_argument("--measured", type=int, help="重放后实测总轮数 / measured total after replay")
    ap.add_argument("--tol", type=float, default=0.002)
    ap.add_argument("--out")
    a = ap.parse_args()
    lo, hi = utc_epoch(a.start), utc_epoch(a.end)

    parts, n_data, n_failed = [], 0, 0
    for path in list_all_files(a.data_dir):
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
        except OSError:
            n_failed += 1
            continue
        has_header, looks_like_data, _ = sniff_schema(raw[: config.SNIFF_BYTES])
        if not looks_like_data:
            continue
        n_data += 1
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                # 与 EDA 扫描相同的解析参数 / same parsing as the EDA scan
                df = pd.read_csv(io.BytesIO(raw), header=0 if has_header else None,
                                 names=config.CSV_COLUMNS, on_bad_lines="skip", engine="c",
                                 encoding="utf-8", encoding_errors="replace")
        except Exception:  # noqa: BLE001
            n_failed += 1
            continue
        t = pd.to_numeric(df["Time"], errors="coerce")
        keep = t.notna() & df["DeviceId"].notna()
        t = t[keep].astype("int64")
        dev = df["DeviceId"][keep].astype(str).str.strip()
        inside = (t >= lo) & (t < hi)
        if inside.any():
            parts.append(pd.DataFrame({"dev": dev[inside].to_numpy(), "time": t[inside].to_numpy()}))

    if n_data == 0:
        print(f"ERROR: {a.data_dir} 下没有可识别的数据文件。", file=sys.stderr)
        return 2
    rounds = (pd.concat(parts, ignore_index=True).drop_duplicates()
              if parts else pd.DataFrame({"dev": [], "time": []}))
    total = len(rounds)
    print(f"数据文件 {n_data} 个，解析失败 {n_failed} 个；窗口 [{a.start}, {a.end}) UTC")
    print(f"总轮数（唯一的 设备 + 时间戳）：{total}")
    # 每台设备窗口内的最后一轮：用来判断重放结束处是数据自然结束还是提前停止。
    # Each device's last round in the window: tells a natural end of data from an early stop.
    last = rounds.groupby("dev")["time"].max()
    for d, n in rounds.groupby("dev").size().items():
        t_last = datetime.fromtimestamp(int(last[d]), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"  设备 {d}：{n} 轮，窗口内最后一轮 {t_last}")

    if a.out:
        rounds["day"] = pd.to_datetime(rounds["time"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
        table = rounds.groupby(["dev", "day"]).size().unstack(fill_value=0)
        os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
        with open(a.out, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["device"] + list(table.columns))
            for d, row in table.iterrows():
                w.writerow([d] + [int(v) for v in row])
        print(f"逐日表已写出：{a.out}")

    if a.measured is not None:
        rel = (a.measured - total) / total if total else float("inf")
        ok = abs(rel) <= a.tol
        print(f"对账：实测 {a.measured}，参照 {total}，相对偏差 {rel:+.4%}，容差 ±{a.tol:.2%} → "
              f"{'通过' if ok else '不通过'}")
        return 0 if ok else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
