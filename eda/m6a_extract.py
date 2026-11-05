#!/usr/bin/env python3
# ============================================================================
# m6a_extract.py
# 适应执行第一批离线模拟的原始数据提取（M6a 交接文档第五节）：把指定设备的全部采样轮写成紧凑的轮表，供 Python 参考实现
# （m6a/reestimate.py）与 Java 离线运行器读取，两边由同一份轮表出发做零级重估并逐位比较；离线模拟的后续计算（数据层在新参照
# 上的状态、点通道在新参数上的离群率等）都从这份轮表出发，不再需要原始数据。
# Raw-data extraction for the M6a offline simulation: a compact per-round table for the chosen devices.
#
# 轮的构造与数据接入模块的成轮器（RoundAssembler）相同：同一设备任何一条可解析的读数（含质量通道 MIC、RSSI、Accelerometer 与
# 未知传感器）都使其时间戳成为一轮，所以只有这类读数的时间戳是一条五个检测通道全部缺失的轮（2026-11-05 修正：第一版只看检测通道，
# 漏掉这类轮，G 的冻结因此比在线晚一轮）；同一设备、通道、时间戳的重复读数保留首条；缺一个通道就是该通道缺失；光照等于 65,536 为右删失。读数按原始文本由 Python 的 float 解析（正确舍入，与 Java 的
# Double.parseDouble 相同），再以最短往返表示写出，保证两边读到的双精度数逐位相同。守卫按 RepeatRunGuard 的规则逐轮计算。
#
# 输出 docs/m6a/extract/：
#   rounds_<设备>.csv.gz：ts（秒）、Temperature、Humidity、Pressure、Gas、Light（缺失为空）、stuck（守卫是否打标，0/1）、cold（冷启动，0/1）；
#   meta.md：每台的轮数、冻结时刻（第 60,480 轮）、读数文件数与每个轮表的 SHA-256。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt（pandas、numpy）。
# 2. 调用命令 / Invocation:
#      python3 eda/m6a_extract.py --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv
#    可选：--devices BG（默认 B 与 G，即离线模拟的两个用例）、--out-dir docs/m6a/extract、--start 2022-03-01、--end 2022-05-01
#    （世界协调时，含首不含尾，结束日多读一天以覆盖 04-30 当地日）、--guard docs/stuck_guard/thresholds.env。
# 3. 前置条件 / Preconditions: 原始 CSV 目录可读；仓库里有守卫阈值文件。
# 4. 期望产出 / Expected output: 终端逐台打印轮数与冻结时刻；docs/m6a/extract/ 下每台一个 rounds_<设备>.csv.gz（每台约 50 万轮，
#    压缩后几 MB）与 meta.md。运行时长与 eda/m5a_extract.py 相近（读一遍全部文件）。
# 5. 失败兜底 / Failure fallback: 没有读到读数时退出码 2（多半是 --data-dir 不对）；缺守卫文件时退出码 3；某台不足 60,480 轮时
#    照常写出轮表，并在 meta.md 注明「不足以冻结」。
# ============================================================================
import argparse
import gzip
import hashlib
import io
import os
import sys
import warnings

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from edalib import config                            # noqa: E402
from edalib.inventory import list_all_files          # noqa: E402
from edalib.scan import sniff_schema                 # noqa: E402
from m5a_extract import CALIB_ROUNDS, CHANNELS, COLD_GAP_SEC, guard_flags, read_guard   # noqa: E402


def to_float(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return np.nan


def load(data_dir, t0, t1, devices):
    """读数保留原始文本、由 float 逐个解析；另返回每台设备所有成轮时间戳（任何传感器的可解析读数）/ exact parsing; round stamps."""
    parts, stamps, nfiles = [], [], 0
    for path in list_all_files(data_dir):
        try:
            with open(path, "rb") as fh:
                raw = fh.read()
        except OSError:
            continue
        has_header, looks_like_data, _ = sniff_schema(raw[: config.SNIFF_BYTES])
        if not looks_like_data:
            continue
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = pd.read_csv(io.BytesIO(raw), header=0 if has_header else None, names=config.CSV_COLUMNS,
                                 on_bad_lines="skip", engine="c", encoding="utf-8", encoding_errors="replace",
                                 dtype={"Value": str})
        except Exception:  # noqa: BLE001
            continue
        sensor = df["Sensor"].astype(str).str.strip()
        dev = df["DeviceId"].astype(str).str.strip()
        t = pd.to_numeric(df["Time"], errors="coerce")
        base = dev.isin(list(devices)) & (t >= t0) & (t < t1)
        if not base.any():
            continue
        anyv = df["Value"][base].astype(str).str.strip()
        parsable = {u: to_float(u) == to_float(u) for u in pd.unique(anyv)}   # 去重后逐个解析 / parse each distinct text once
        ok = pd.Series(False, index=df.index)
        ok[base] = anyv.map(parsable).astype(bool)
        if ok.any():                                   # 成轮：任何传感器的可解析读数 / any parsable reading opens a round
            stamps.append(pd.DataFrame({"dev": dev[ok].astype(str).to_numpy(), "t": t[ok].astype("int64").to_numpy()}))
        sel = sensor.isin(CHANNELS) & ok
        if not sel.any():
            continue
        nfiles += 1
        text = df["Value"][sel].astype(str).str.strip()
        uniq = pd.unique(text)
        lut = dict(zip(uniq, (to_float(u) for u in uniq)))
        parts.append(pd.DataFrame({"dev": dev[sel].astype(str).to_numpy(),
                                   "ch": sensor[sel].map({c: i for i, c in enumerate(CHANNELS)}).astype("int8").to_numpy(),
                                   "t": t[sel].astype("int64").to_numpy(),
                                   "v": text.map(lut).to_numpy(dtype=float)}).dropna())
    if not parts:
        return None, None, nfiles
    d = pd.concat(parts, ignore_index=True)
    st = pd.concat(stamps, ignore_index=True).drop_duplicates()
    return d.drop_duplicates(["dev", "ch", "t"], keep="first"), st, nfiles


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--out-dir", default="docs/m6a/extract")
    ap.add_argument("--start", default="2022-03-01")
    ap.add_argument("--end", default="2022-05-01")
    ap.add_argument("--guard", default="docs/stuck_guard/thresholds.env")
    ap.add_argument("--devices", default="BG")
    a = ap.parse_args()
    if not os.path.exists(a.guard):
        print("ERROR: 缺少 %s" % a.guard, file=sys.stderr)
        return 3
    thr, floors = read_guard(a.guard)
    t0 = int(pd.Timestamp(a.start, tz="UTC").timestamp())
    t1 = int(pd.Timestamp(a.end, tz="UTC").timestamp()) + 86400
    d, st, nfiles = load(a.data_dir, t0, t1, a.devices)
    if d is None or d.empty:
        print("ERROR: 没有读到读数（检查 --data-dir）", file=sys.stderr)
        return 2
    os.makedirs(a.out_dir, exist_ok=True)
    meta = ["# 适应执行第一批离线模拟的轮表（eda/m6a_extract.py 生成）\n",
            "读数文件 %d 个；范围 %s 至 %s（世界协调时，结束日多读一天）。\n" % (nfiles, a.start, a.end)]
    for dev, g in d.groupby("dev"):
        g = g.sort_values("t", kind="mergesort")
        wide = g.pivot(index="t", columns="ch", values="v").reindex(columns=range(5))
        all_t = np.unique(st.loc[st["dev"] == dev, "t"].to_numpy().astype("int64"))
        empty = int(len(all_t) - len(wide))            # 只有质量通道或未知传感器读数的轮 / rounds without detection readings
        wide = wide.reindex(all_t)
        ts = wide.index.to_numpy().astype("int64")
        raw = wide.to_numpy(dtype=float)
        stuck = guard_flags(ts, raw, thr.get(dev, [0] * 5), floors.get(dev, [set() for _ in CHANNELS]))
        cold = np.zeros(len(ts), dtype=int)
        cold[1:] = (np.diff(ts) > COLD_GAP_SEC).astype(int)
        path = os.path.join(a.out_dir, "rounds_%s.csv.gz" % dev)
        buf = io.StringIO()
        buf.write("ts," + ",".join(CHANNELS) + ",stuck,cold\n")
        for i in range(len(ts)):
            vals = ["" if v != v else repr(float(v)) for v in raw[i]]
            buf.write("%d,%s,%d,%d\n" % (ts[i], ",".join(vals), int(stuck[i]), cold[i]))
        data = buf.getvalue().encode("utf-8")
        with gzip.GzipFile(path, "wb", mtime=0) as fh:          # mtime=0：同样的输入得到同样的字节 / reproducible bytes
            fh.write(data)
        sha = hashlib.sha256(open(path, "rb").read()).hexdigest()
        if len(ts) >= CALIB_ROUNDS:
            tf = int(ts[CALIB_ROUNDS - 1])
            fz = "冻结于 %s UTC（第 %d 轮）" % (pd.Timestamp(tf, unit="s", tz="UTC").strftime("%Y-%m-%d %H:%M:%S"), CALIB_ROUNDS)
        else:
            fz = "只有 %d 轮，不足以冻结" % len(ts)
        line = "%s：轮数 %d（其中无检测通道读数的轮 %d）；%s；守卫打标 %d 轮；冷启动 %d 次；SHA-256 %s" % (
            dev, len(ts), empty, fz, int(stuck.sum()), int(cold.sum()), sha)
        print(line)
        meta.append("- " + line + "\n")
    with open(os.path.join(a.out_dir, "meta.md"), "w", encoding="utf-8") as fh:
        fh.write("".join(meta))
    print("已写出 %s" % a.out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
