#!/usr/bin/env python3
# ============================================================================
# g_light_profile.py
# 设备 G 光照通道的全程逐日剖面（0.89 版裁决第三节，纯分析、只报事实）：按布里斯托当地时间逐日给出最小值、
# 中位数、最大值与日内形态（逐小时中位数），与其他七台同日对照；按明示的描述性规则标出各日状态与 G 的状态
# 变化日期；核对三月头两周 G 的读数是否落在该台底值集合内、重复值游程多长，用来区分「传感器黑暗」与「读数卡住」。
# Full-period per-local-day profile of device G's Light channel versus the other seven devices, state markers by
# a stated descriptive rule, and the floor/run check for early March. Facts only, no conclusions.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt（只用 pandas、numpy）。
# 2. 调用命令：
#      python3 eda/g_light_profile.py \
#          --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv
#    可选：--focus-start 2022-03-01 --focus-end 2022-03-15（核对底值与游程的时段，当地日期，含首不含尾）、
#    --run-gap-sec 60（相邻读数间隔超过它就断开游程，与守卫一致）、--min-run 30（列入游程表的最短长度，读数）、
#    --out-dir docs/g_light（CSV 输出目录）、--report docs/reports/g_light_profile.md。
# 3. 前置条件：原始 CSV 目录可读（与 daily_channel_profile.py 相同）；仓库里有 docs/stuck_guard/floor_values.csv
#    （各台底值集合）与 docs/copy_segments.csv（六月复制段，用来标注）。
# 4. 期望产出：docs/reports/g_light_profile.md（报告）；docs/g_light/daily_light.csv（八台逐日长表，含逐小时中位数）、
#    docs/g_light/g_runs_focus.csv（核对时段内 G 的游程表）。终端打印读入的文件数、读数与设备数。运行时长与
#    daily_channel_profile.py 全程扫描相近（读一遍全部文件）。
# 5. 失败兜底：没有读到任何光照读数时退出码 2（多半是 --data-dir 不对）；缺少底值或复制段文件时退出码 3。
# ============================================================================
import argparse
import csv
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

TZ = "Europe/London"          # 布里斯托：冬令时为世界协调时，夏令时（2022-03-27 01:00 UTC 起）加一小时
DAY_HOURS = range(11, 15)     # 「白天」取当地 11 至 14 时 / local 11:00-14:59
NIGHT_HOURS = range(0, 4)     # 「夜间」取当地 0 至 3 时 / local 00:00-03:59
SHORT = {"底值为主": "底", "近乎恒定": "恒", "有日内变化": "变"}


def load_light(data_dir):
    """读全部文件里的光照读数，按（设备，时间）去重 / all Light readings, deduplicated by (device, time)."""
    parts, nfiles = [], 0
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
                df = pd.read_csv(io.BytesIO(raw), header=0 if has_header else None,
                                 names=config.CSV_COLUMNS, on_bad_lines="skip", engine="c",
                                 encoding="utf-8", encoding_errors="replace")
        except Exception:  # noqa: BLE001
            continue
        sel = df["Sensor"].astype(str).str.strip() == "Light"
        if not sel.any():
            continue
        nfiles += 1
        parts.append(pd.DataFrame({"dev": df["DeviceId"].astype(str).str.strip()[sel],
                                   "t": pd.to_numeric(df["Time"][sel], errors="coerce"),
                                   "v": pd.to_numeric(df["Value"][sel], errors="coerce")}).dropna())
    if not parts:
        return None, nfiles
    d = pd.concat(parts, ignore_index=True)
    d["t"] = d["t"].astype("int64")
    d = d.drop_duplicates(["dev", "t"], keep="first").sort_values(["dev", "t"], kind="mergesort")
    local = pd.to_datetime(d["t"], unit="s", utc=True).dt.tz_convert(TZ)
    d["day"] = local.dt.strftime("%Y-%m-%d")
    d["hour"] = local.dt.hour
    return d.reset_index(drop=True), nfiles


def runs(d, gap):
    """重复值游程：值不变且相邻间隔不超过 gap 秒 / runs of identical values with gaps <= gap seconds."""
    new = (d["dev"] != d["dev"].shift()) | (d["v"] != d["v"].shift()) | (d["t"] - d["t"].shift() > gap)
    rid = new.cumsum()
    g = d.groupby(rid, sort=False)
    return pd.DataFrame({"dev": g["dev"].first(), "start_t": g["t"].first(), "end_t": g["t"].last(),
                         "day": g["day"].first(), "value": g["v"].first(), "length": g.size()})


def state_of(r, floor):
    """描述性状态（规则写在报告里，只是标记，不是结论）/ descriptive state marker."""
    if r["floor_share"] >= 0.5:
        return "底值为主"
    if r["median"] > max(floor or [0]) and (r["p90"] - r["p10"]) <= 0.1 * r["median"]:
        return "近乎恒定"
    return "有日内变化"


def fmt(x, nd=0):
    return "—" if x is None or (isinstance(x, float) and np.isnan(x)) else ("%.*f" % (nd, x))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--focus-start", default="2022-03-01")
    ap.add_argument("--focus-end", default="2022-03-15")
    ap.add_argument("--run-gap-sec", type=int, default=60)
    ap.add_argument("--min-run", type=int, default=30)
    ap.add_argument("--floor-values", default="docs/stuck_guard/floor_values.csv")
    ap.add_argument("--copy-segments", default="docs/copy_segments.csv")
    ap.add_argument("--out-dir", default="docs/g_light")
    ap.add_argument("--report", default="docs/reports/g_light_profile.md")
    a = ap.parse_args()
    if not (os.path.exists(a.floor_values) and os.path.exists(a.copy_segments)):
        print("ERROR: 缺少 %s 或 %s" % (a.floor_values, a.copy_segments), file=sys.stderr)
        return 3
    floors = {}
    with open(a.floor_values, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["channel"] == "Light":
                floors.setdefault(r["device"], []).append(float(r["value"]))
    copy_days = set()
    with open(a.copy_segments, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            copy_days |= {x.strftime("%Y-%m-%d") for x in pd.date_range(r["target_start"], r["target_end"])}

    d, nfiles = load_light(a.data_dir)
    if d is None or d.empty:
        print("ERROR: 没有读到任何光照读数（检查 --data-dir）", file=sys.stderr)
        return 2
    print("读入含光照读数的文件 %d 个；去重后读数 %d 条；设备 %s" % (nfiles, len(d), ",".join(sorted(d["dev"].unique()))))
    d["floor"] = [v in floors.get(dev, []) for dev, v in zip(d["dev"], d["v"])]

    rs = runs(d, a.run_gap_sec)
    longest = rs.sort_values("length", ascending=False).drop_duplicates(["dev", "day"]).set_index(["dev", "day"])
    rows = []
    for (dev, day), g in d.groupby(["dev", "day"], sort=True):
        v = g["v"].to_numpy()
        hm = g.groupby("hour")["v"].median()
        dayv = g.loc[g["hour"].isin(DAY_HOURS), "v"]
        night = g.loc[g["hour"].isin(NIGHT_HOURS), "v"]
        lr = longest.loc[(dev, day)] if (dev, day) in longest.index else None
        r = {"device": dev, "day": day, "n": len(v), "min": float(v.min()), "p10": float(np.percentile(v, 10)),
             "median": float(np.median(v)), "p90": float(np.percentile(v, 90)), "max": float(v.max()),
             "distinct": int(len(np.unique(v))), "floor_share": float(g["floor"].mean()),
             "longest_run": int(lr["length"]) if lr is not None else 0,
             "longest_run_value": float(lr["value"]) if lr is not None else float("nan"),
             "day_minus_night": float(dayv.median() - night.median()) if len(dayv) and len(night) else float("nan"),
             "copy_segment": day in copy_days}
        r["state"] = state_of(r, floors.get(dev))
        for h in range(24):
            r["h%02d" % h] = float(hm[h]) if h in hm.index else float("nan")
        rows.append(r)
    daily = pd.DataFrame(rows)
    os.makedirs(a.out_dir, exist_ok=True)
    daily.round(3).to_csv(os.path.join(a.out_dir, "daily_light.csv"), index=False)

    gfloor = floors.get("G", [])
    gruns = rs[(rs["dev"] == "G") & (rs["day"] >= a.focus_start) & (rs["day"] < a.focus_end) & (rs["length"] >= a.min_run)].copy()
    gruns["start_local"] = pd.to_datetime(gruns["start_t"], unit="s", utc=True).dt.tz_convert(TZ).dt.strftime("%Y-%m-%d %H:%M:%S")
    gruns["duration_sec"] = gruns["end_t"] - gruns["start_t"]
    gruns["in_floor_set"] = gruns["value"].isin(gfloor)
    gruns = gruns.sort_values("length", ascending=False)
    gruns[["start_local", "length", "duration_sec", "value", "in_floor_set"]].to_csv(
        os.path.join(a.out_dir, "g_runs_focus.csv"), index=False)

    write_report(a, daily, gruns, floors, nfiles, len(d))
    print("报告：%s；长表：%s/daily_light.csv（%d 行）" % (a.report, a.out_dir, len(daily)))
    return 0


def write_report(a, daily, gruns, floors, nfiles, nreadings):
    g = daily[daily["device"] == "G"].set_index("day")
    devs = sorted(daily["device"].unique())
    L = ["# 设备 G 光照通道的全程逐日剖面（0.89 版裁决第三节）", "",
         "本报告只报事实，不下结论；成因由设计会话结合随数据集发布的材料判定。由 `eda/g_light_profile.py` 从原始 CSV 生成"
         "（读入含光照读数的文件 %d 个，按设备与时间去重后 %d 条读数）。" % (nfiles, nreadings), "",
         "## 〇、口径", "",
         "- 日期与小时一律为布里斯托当地时间（`Europe/London`）：2022-03-27 01:00 UTC 之前为世界协调时，之后为世界协调时加一小时。",
         "- 「白天减夜间」= 当地 11 至 14 时读数的中位数减去当地 0 至 3 时读数的中位数。",
         "- 底值集合取自 `docs/stuck_guard/floor_values.csv`（G 的光照为 %s）；「底值占比」是当日读数落在该台底值集合内的比例。"
         % "、".join(fmt(x) for x in floors.get("G", [])),
         "- 游程：值不变且相邻读数间隔不超过 %d 秒（与守卫一致）；跨午夜的游程计入起始那一天。" % a.run_gap_sec,
         "- 描述性状态标记（只是标记，不是结论）：底值占比不低于 50% 记「底值为主」；否则若中位数高于该台底值集合的最大值、且 "
         "P10 至 P90 宽度不超过中位数的 10%，记「近乎恒定」；其余记「有日内变化」。",
         "- 「复制段」指 `docs/copy_segments.csv` 中六月复制的目标日期，那几天的数据是复制来的。", ""]

    # 一、G 的逐日表
    L += ["## 一、设备 G 逐日统计", "",
          "| 当地日期 | 读数 | 最小 | P10 | 中位数 | P90 | 最大 | 不同取值数 | 底值占比 | 最长游程（读数）与取值 | 白天减夜间 | 状态 |",
          "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for day, r in g.iterrows():
        L.append("| %s%s | %d | %s | %s | %s | %s | %s | %d | %.1f%% | %d（%s） | %s | %s |" % (
            day, "（复制段）" if r["copy_segment"] else "", r["n"], fmt(r["min"]), fmt(r["p10"]), fmt(r["median"]),
            fmt(r["p90"]), fmt(r["max"]), r["distinct"], 100 * r["floor_share"], r["longest_run"],
            fmt(r["longest_run_value"]), fmt(r["day_minus_night"]), r["state"]))

    # 二、状态变化日期
    L += ["", "## 二、设备 G 的状态变化日期（按上面的描述性规则）", ""]
    prev, changes = None, []
    for day, r in g.iterrows():
        if prev is not None and r["state"] != prev:
            changes.append("| %s | %s | %s |" % (day, prev, r["state"]))
        prev = r["state"]
    L += (["| 当地日期 | 前一日状态 | 当日状态 |", "| --- | --- | --- |"] + changes) if changes else ["全程状态没有变化。"]
    cnt = g["state"].value_counts()
    L += ["", "各状态天数：" + "；".join("%s %d 天" % (k, v) for k, v in cnt.items()) + "。", ""]

    # 三、与其他七台同日对照
    L += ["## 三、与其他七台同日对照", "", "每格为「当日中位数 / 白天减夜间」，后附状态的首字（底、恒、变）。", "",
          "| 当地日期 | " + " | ".join(devs) + " |", "| --- | " + " | ".join("---" for _ in devs) + " |"]
    piv = daily.set_index(["day", "device"])
    for day in sorted(daily["day"].unique()):
        cells = []
        for dev in devs:
            if (day, dev) in piv.index:
                r = piv.loc[(day, dev)]
                cells.append("%s / %s %s" % (fmt(r["median"]), fmt(r["day_minus_night"]), SHORT[r["state"]]))
            else:
                cells.append("—")
        L.append("| %s | %s |" % (day, " | ".join(cells)))

    # 四、日内形态
    L += ["", "## 四、设备 G 的日内形态（当地各小时读数的中位数）", "",
          "| 当地日期 | " + " | ".join("%02d" % h for h in range(24)) + " |", "| --- | " + " | ".join("---" for _ in range(24)) + " |"]
    for day, r in g.iterrows():
        L.append("| %s | %s |" % (day, " | ".join(fmt(r["h%02d" % h]) for h in range(24))))

    # 五、核对时段的底值与游程
    f = g[(g.index >= a.focus_start) & (g.index < a.focus_end)]
    L += ["", "## 五、%s 至 %s（不含）设备 G 的底值与游程" % (a.focus_start, a.focus_end), "",
          "逐日底值占比、不同取值数与最长游程见第一节同日的行。下表列出这一时段内长度不少于 %d 个读数的游程（共 %d 段），"
          "按长度降序，最多 40 段；全部在 `%s/g_runs_focus.csv`。" % (a.min_run, len(gruns), a.out_dir), "",
          "这一时段 G 的读数合计 %d 条，其中落在底值集合内的 %.1f%%；不同取值数逐日为 %s。" % (
              int(f["n"].sum()), 100 * float((f["floor_share"] * f["n"]).sum() / max(f["n"].sum(), 1)),
              "、".join(str(int(x)) for x in f["distinct"])), "",
          "| 起始（当地） | 长度（读数） | 持续（秒） | 取值 | 是否属于底值集合 |", "| --- | --- | --- | --- | --- |"]
    for _, r in gruns.head(40).iterrows():
        L.append("| %s | %d | %d | %s | %s |" % (r["start_local"], r["length"], r["duration_sec"], fmt(r["value"], 1),
                                                 "是" if r["in_floor_set"] else "否"))
    os.makedirs(os.path.dirname(a.report) or ".", exist_ok=True)
    with open(a.report, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")


if __name__ == "__main__":
    sys.exit(main())
