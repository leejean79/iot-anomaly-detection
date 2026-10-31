#!/usr/bin/env python3
# ============================================================================
# gap_check.py
# 核对原始数据集在一个世界协调时窗口内是否本来就缺轮：逐台列出相邻两轮间隔超过门限的缺口，并按分钟给出全机队的轮数。
# 用于 1.25 版裁决第八项：三月三十日 20:20（世界协调时）前后全机队缺轮约十二分半钟，是数据集的缺口还是重放的缺口。
# Check whether the raw dataset itself has gaps in a UTC window: per-device gaps longer than a threshold, fleet rounds per minute.
#
# 读法与 eda/count_rounds.py 相同（同一个格式嗅探、同一套 read_csv 参数），一轮 = 唯一的（设备, 时间戳）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt（pandas）。
# 2. 调用命令：
#      python3 eda/gap_check.py \
#          --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
#          --start 2022-03-30T19:00:00 --end 2022-03-30T22:00:00 --gap 30 \
#          --out docs/m7/gap_0330_v2.md
#    --start 含、--end 不含，均为世界协调时；--gap 为判为缺口的相邻轮间隔（秒，名义间隔十秒）；--margin 为窗口两端多读的
#    秒数（默认 3600），跨窗口边界的缺口据此找到起止。
# 3. 前置条件：原始 CSV 目录可读（EDA 全量扫描用的同一个目录）。
# 4. 期望产出：终端与 --out 处的 Markdown：逐台缺口清单（起止时刻与时长）、全机队逐分钟轮数表。若原始数据在 20:00 至 21:00 之间
#    每台都有一个约十二分半钟的缺口，即为数据集本身的缺口。
# 5. 失败兜底：目录下没有可识别的数据文件时退出码 2；窗口内没有任何读数时退出码 1。
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


def epoch(s):
    return int(datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp())


def iso(t):
    return datetime.fromtimestamp(int(t), tz=timezone.utc).strftime("%m-%d %H:%M:%S")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--gap", type=int, default=30)
    ap.add_argument("--margin", type=int, default=3600, help="窗口两端多读的秒数，用来找到跨窗口边界的缺口的起止")
    ap.add_argument("--out")
    a = ap.parse_args()
    lo, hi = epoch(a.start), epoch(a.end)
    parts, n_data = [], 0
    for path in list_all_files(a.data_dir):
        with open(path, "rb") as fh:
            raw = fh.read()
        has_header, looks_like_data, _ = sniff_schema(raw[: config.SNIFF_BYTES])
        if not looks_like_data:
            continue
        n_data += 1
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_csv(io.BytesIO(raw), header=0 if has_header else None, names=config.CSV_COLUMNS,
                             on_bad_lines="skip", engine="c", encoding="utf-8", encoding_errors="replace")
        t = pd.to_numeric(df["Time"], errors="coerce")
        keep = t.notna() & df["DeviceId"].notna()
        t = t[keep].astype("int64")
        dev = df["DeviceId"][keep].astype(str).str.strip()
        inside = (t >= lo - a.margin) & (t < hi + a.margin)   # 两端多读一段，跨边界的缺口也能找到起止
        if inside.any():
            parts.append(pd.DataFrame({"dev": dev[inside].to_numpy(), "time": t[inside].to_numpy()}))
    if n_data == 0:
        print("ERROR: %s 下没有可识别的数据文件。" % a.data_dir, file=sys.stderr)
        return 2
    if not parts:
        print("窗口内没有任何读数。", file=sys.stderr)
        return 1
    rounds = pd.concat(parts, ignore_index=True).drop_duplicates().sort_values(["dev", "time"])
    out = ["# 原始数据缺口核对（eda/gap_check.py）\n", "窗口 [%s, %s) UTC，缺口门限 %d 秒，数据文件 %d 个。\n" % (a.start, a.end, a.gap, n_data),
           "## 逐台缺口\n", "| 设备 | 缺口开始（上一轮，UTC） | 缺口结束（下一轮，UTC） | 时长（秒） |", "| --- | --- | --- | --- |"]
    for d, g in rounds.groupby("dev"):
        ts = g["time"].to_numpy()
        if ts[0] > lo:                                   # 读入范围内找不到缺口前的一轮 / no round before the gap within the margin
            out.append("| %s | 早于 %s（读入范围内无读数） | %s | 至少 %d |" % (d, iso(lo - a.margin), iso(ts[0]), ts[0] - (lo - a.margin)))
        for prev, nxt in zip(ts[:-1], ts[1:]):
            if nxt - prev > a.gap and nxt > lo and prev < hi:
                out.append("| %s | %s | %s | %d |" % (d, iso(prev), iso(nxt), nxt - prev))
    inw = rounds[(rounds["time"] >= lo) & (rounds["time"] < hi)].copy()
    inw["minute"] = (inw["time"] // 60) * 60
    tab = inw.groupby(["minute", "dev"]).size().unstack(fill_value=0)
    devs = list(tab.columns)
    out += ["\n## 逐分钟轮数（名义每台每分钟六轮）\n", "| 分钟（UTC） | " + " | ".join(devs) + " |",
            "| --- | " + " | ".join("---" for _ in devs) + " |"]
    for m in range(lo - lo % 60, hi, 60):
        row = tab.loc[m] if m in tab.index else None
        out.append("| %s | " % iso(m)[6:11] + " | ".join(str(int(row[d])) if row is not None else "0" for d in devs) + " |")
    text = "\n".join(out) + "\n"
    print(text)
    if a.out:
        os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
        with open(a.out, "w", encoding="utf-8") as fh:
            fh.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
