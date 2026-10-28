#!/usr/bin/env python3
# ============================================================================
# m7_extract.py
# 聚合模块决策部分（M7）离线实验的原始数据提取（裁决书 `ruling_m7_kickoff_zh.md` 第一项、第九项）：从原始读数算出每台每个当地小时
# 的轮数、入口轮数、守卫打标轮数与五个通道的小时均值，以及标定周各当地小时的均值（「同步变化」只作记录的对照版本）。只做计算，
# 不判读；决策函数的离线运行不需要原始数据。
# Raw-data extraction for the M7 offline experiment: per device and local hour, round counts, admitted rounds, guard-flagged
# rounds and channel means; plus calibration-week means per local hour of day (record-only comparison).
#
# 处理要点（与数据层离线第一步 eda/m5a_extract.py 相同，并直接复用它的函数）：
#   - 轮 = 同一设备的不同时间戳；第 60,480 轮处冻结，标定窗口为冻结之前（含）；
#   - 注入取「不剔除」的一遍（按 Injector 的语义改写读数，交接文档第七节）；
#   - 守卫：RepeatRunGuard 的移植；入口轮：冻结之后五个通道都有读数（含删失光照）；
#   - 均值在变换域：光照取 log1p，等于 65,536 的右删失读数不计入；缺失读数不计入；
#   - 当地小时：布里斯托当地时间的整点（03-27 夏令时开始，当地 01:00 这一小时不存在）；小时用其起点的世界协调时秒数标识。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt（只用 pandas、numpy）。
# 2. 调用命令：
#      python3 eda/m7_extract.py --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv
#    可选：--out-dir docs/m7/extract（输出目录）、--start 2022-03-01、--end 2022-05-01（读入的世界协调时范围，含首不含尾）、
#    --inject docs/m3_a4/full_fix/inject-truth.csv、--guard docs/stuck_guard/thresholds.env、--devices ABCDEFGH。
# 3. 前置条件：原始 CSV 目录可读（与数据层离线第一步同一个目录）；仓库里有注入真值与守卫阈值文件。
# 4. 期望产出：docs/m7/extract/ 下 hourly.csv（设备、小时起点、当地小时、轮数、入口轮数、守卫打标轮数、五个通道的读数数与均值）、
#    calib_hourly.csv（设备、当地钟点 0 至 23、五个通道在标定窗口内该钟点的读数数与均值）、meta.md（冻结时刻、读数数、行数，以及
#    每个通道不能被单精度精确表示的读数数，供适应执行模块的紧凑缓存参考）。
#    运行时长与数据层离线第一步相近（读一遍全部文件，再逐台计算）。
# 5. 失败兜底：没有读到读数时退出码 2（多半是 --data-dir 不对）；缺注入或守卫文件时退出码 3；某台设备不足 60,480 轮时跳过该台并在
#    meta.md 中注明。
# ============================================================================
import argparse
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import m5a_extract as ex                              # noqa: E402

CHANNELS = ex.CHANNELS


def local_hour_start(ts):
    """每个时刻所在当地整点的世界协调时秒数 / UTC epoch of the local hour containing each timestamp."""
    loc = pd.to_datetime(ts, unit="s", utc=True).tz_convert(ex.TZ)
    start = (loc.floor("h") - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(seconds=1)    # 与 pandas 的时间单位无关 / unit-agnostic
    return np.asarray(start, dtype="int64"), np.asarray(loc.hour)


def process_device(dev, g, specs, thr, floors, out):
    g = g.sort_values("t", kind="mergesort")
    wide_df = g.pivot(index="t", columns="ch", values="v").reindex(columns=range(5))
    ts = wide_df.index.to_numpy().astype("int64")
    raw = wide_df.to_numpy(dtype=float)
    if len(ts) < ex.CALIB_ROUNDS:
        out["meta"].append("%s：只有 %d 轮，不足 %d，跳过。" % (dev, len(ts), ex.CALIB_ROUNDS))
        return
    tf = int(ts[ex.CALIB_ROUNDS - 1])
    my_specs = [s for s in specs if s["device"] == dev]
    w = ex.apply_injections(ts, raw, my_specs) if my_specs else raw
    stuck = ex.guard_flags(ts, w, thr.get(dev, [0] * 5), floors.get(dev, [set() for _ in CHANNELS]))
    adm = (ts > tf) & ~np.isnan(w).any(axis=1)
    tv = w.copy()
    light = tv[:, ex.LIGHT]
    light[light == ex.LIGHT_CENSOR] = np.nan
    tv[:, ex.LIGHT] = np.log1p(light)
    hs, hod = local_hour_start(ts)
    df = pd.DataFrame({"hour_start": hs, "rounds": 1, "admitted": adm.astype(int), "guard_rounds": stuck.astype(int)})
    agg = {"rounds": "sum", "admitted": "sum", "guard_rounds": "sum"}
    for c, name in enumerate(CHANNELS):
        df["n_" + name] = (~np.isnan(tv[:, c])).astype(int)
        df["s_" + name] = np.nan_to_num(tv[:, c])
        agg["n_" + name] = "sum"
        agg["s_" + name] = "sum"
    h = df.groupby("hour_start", sort=True).agg(agg).reset_index()
    for name in CHANNELS:
        h["mean_" + name] = np.where(h["n_" + name] > 0, h["s_" + name] / h["n_" + name].where(h["n_" + name] > 0, 1), np.nan)
        h = h.drop(columns="s_" + name)
    h.insert(0, "device", dev)
    h.insert(2, "local_hour", pd.to_datetime(h["hour_start"], unit="s", utc=True).dt.tz_convert(ex.TZ).dt.strftime("%Y-%m-%d %H"))
    out["hourly"].append(h)
    cal = ts <= tf
    for hour in range(24):
        sel = cal & (hod == hour)
        row = dict(device=dev, hour_of_day=hour)
        for c, name in enumerate(CHANNELS):
            x = tv[sel, c]
            x = x[~np.isnan(x)]
            row["n_" + name] = len(x)
            row["mean_" + name] = float(x.mean()) if len(x) else np.nan
        out["calib"].append(row)
    out["meta"].append("%s：冻结于 %s UTC；轮数 %d；小时 %d 个。" % (
        dev, pd.Timestamp(tf, unit="s", tz="UTC").strftime("%Y-%m-%d %H:%M:%S"), len(ts), len(h)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--out-dir", default="docs/m7/extract")
    ap.add_argument("--start", default="2022-03-01")
    ap.add_argument("--end", default="2022-05-01")
    ap.add_argument("--inject", default="docs/m3_a4/full_fix/inject-truth.csv")
    ap.add_argument("--guard", default="docs/stuck_guard/thresholds.env")
    ap.add_argument("--devices", default="ABCDEFGH")
    a = ap.parse_args()
    if not (os.path.exists(a.inject) and os.path.exists(a.guard)):
        print("ERROR: 缺少 %s 或 %s" % (a.inject, a.guard), file=sys.stderr)
        return 3
    specs = ex.read_injections(a.inject)
    thr, floors = ex.read_guard(a.guard)
    t0 = int(pd.Timestamp(a.start, tz="UTC").timestamp())
    t1 = int(pd.Timestamp(a.end, tz="UTC").timestamp()) + 86400        # 多读一天，覆盖 04-30 当地日的最后一小时
    d, nfiles = ex.load(a.data_dir, t0, t1, a.devices)
    if d is None or d.empty:
        print("ERROR: 没有读到读数（检查 --data-dir）", file=sys.stderr)
        return 2
    print("读入文件 %d 个；去重后读数 %d 条" % (nfiles, len(d)))
    # 适应执行模块第一批的紧凑缓存按单精度存原始读数；这里核对每个通道有多少读数不能被单精度精确表示（M6 开工核对第四节）
    # The M6 compact cache stores raw readings as float32; count readings per channel that float32 cannot represent exactly.
    v = d["v"].to_numpy(dtype=float)
    # pandas 默认的快速解析器偶尔差一个最低有效位，所以允许两个最低有效位 / pandas' fast parser may be off by one ulp
    inexact = np.abs(v.astype(np.float32).astype(float) - v) > 2 * np.spacing(np.abs(v))
    f32 = {CHANNELS[c]: (int(inexact[d["ch"].to_numpy() == c].sum()), int((d["ch"].to_numpy() == c).sum())) for c in range(5)}
    out = {"hourly": [], "calib": [], "meta": []}
    for dev, g in d.groupby("dev"):
        print("处理设备 %s（%d 条读数）……" % (dev, len(g)))
        process_device(dev, g, specs, thr, floors, out)
    os.makedirs(a.out_dir, exist_ok=True)
    hourly = pd.concat(out["hourly"], ignore_index=True)
    hourly.to_csv(os.path.join(a.out_dir, "hourly.csv"), index=False)
    pd.DataFrame(out["calib"]).to_csv(os.path.join(a.out_dir, "calib_hourly.csv"), index=False)
    with open(os.path.join(a.out_dir, "meta.md"), "w", encoding="utf-8") as fh:
        fh.write("# M7 离线实验原始数据提取的元数据\n\n读入文件 %d 个，读数 %d 条（%s 至 %s，世界协调时）；hourly.csv %d 行。\n\n"
                 % (nfiles, len(d), a.start, a.end, len(hourly)))
        fh.write("\n".join("- " + x for x in out["meta"]) + "\n")
        fh.write("\n## 单精度能否精确表示原始读数（适应执行模块紧凑缓存）\n\n")
        fh.write("\n".join("- %s：%d / %d 条读数不能被单精度精确表示。" % (k, a_, b_) for k, (a_, b_) in f32.items()) + "\n")
    print("完成：%s/ 下 hourly.csv（%d 行）、calib_hourly.csv、meta.md" % (a.out_dir, len(hourly)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
