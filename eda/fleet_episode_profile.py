#!/usr/bin/env python3
# ============================================================================
# fleet_episode_profile.py
# 跨设备同时确认漂移的时段的成因核查（0.98 版裁决第二节第四项，纯分析、只报事实）：从网格的确认清单
# （docs/m5b/grid/confirms.csv，默认组合）按「6 小时内至少三台设备都有候选并被确认」找出时段，略去三月事件尾段内的；
# 对每个时段给出八台设备五个检测通道按布里斯托当地时间的逐小时剖面（相对参照期同一当地小时的偏移、宽度倍数）、
# 机队逐小时对照与主分，按明示的描述性规则标出哪些通道在动、是否全机队同步、持续多久、是否回落。
# Fleet-wide drift episodes: hourly local-time profile of five channels across the fleet around each episode.
# Facts only, no conclusions.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt（只用 pandas、numpy）。
# 2. 调用命令：
#      python3 eda/fleet_episode_profile.py \
#          --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv
#    可选：--ref-start 2022-03-08 --ref-end 2022-03-17（参照期，当地日期，首尾都含）、--pad-hours 12（时段前后各留的小时数）、
#    --min-devices 3、--cluster-hours 6（成簇规则）、--skip-before 2022-03-28（略去此前的时段，即三月事件尾段）、
#    --out-dir docs/fleet_episodes（CSV 输出目录）、--report docs/reports/fleet_drift_episodes.md。
# 3. 前置条件：原始 CSV 目录可读（与 daily_channel_profile.py 相同）；仓库里有 docs/m5b/grid/confirms.csv（run_grid.py 的输出）
#    与 docs/m5b/streams_hourly.csv（主分的逐小时中位数）。
# 4. 期望产出：docs/reports/fleet_drift_episodes.md（报告）；docs/fleet_episodes/hourly.csv（各时段逐台逐通道逐小时长表）、
#    docs/fleet_episodes/episodes.csv（时段清单）。终端打印读入的文件数、读数与时段数。运行时长与 daily_channel_profile.py
#    全程扫描相近（读一遍全部文件）。
# 5. 失败兜底：没有读到读数时退出码 2（多半是 --data-dir 不对）；缺少 confirms.csv 或 streams_hourly.csv 时退出码 3；
#    按规则一个时段也没找到时退出码 4。
# ============================================================================
import argparse
import csv
import io
import os
import sys
import warnings
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from edalib import config                            # noqa: E402
from edalib.inventory import list_all_files          # noqa: E402
from edalib.scan import sniff_schema                 # noqa: E402

TZ = "Europe/London"          # 布里斯托：冬令时为世界协调时，夏令时（2022-03-27 01:00 UTC 起）加一小时
CHANNELS = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]
SHORT = {"Temperature": "温", "Humidity": "湿", "Pressure": "压", "Gas": "气", "Light": "光"}
DEVICES = "ABCDEFGH"
MOVE = 1.0                    # 描述性标记：|偏移| ≥ 1 个参照宽度记为「在动」/ marker: |shift| >= 1 reference width
BACK = 0.5                    # 时段末 |偏移| < 0.5 记为「已回落」/ marker: back below 0.5 at the window end
MIN_N = 30                    # 一小时内读数少于 30 条时不计 / hours with fewer readings are skipped
ALARM = 2.22                  # 主分报警阈值 / main-score alarm threshold


def parse_utc(s):
    return datetime.strptime("2022-" + s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)


def find_episodes(path, min_dev, hours, skip_before):
    """按候选时刻排序，以每簇第一个候选为起点、hours 小时内的确认成簇；至少 min_dev 台设备的簇为一个时段。"""
    with open(path, encoding="utf-8") as fh:
        rows = [r for r in csv.DictReader(fh) if r["combo"] == "c0.99_r0.5_10min_k36"]
    rows.sort(key=lambda r: parse_utc(r["candidate_utc"]))
    clusters, cur = [], []
    for r in rows:
        if cur and parse_utc(r["candidate_utc"]) - parse_utc(cur[0]["candidate_utc"]) > timedelta(hours=hours):
            clusters.append(cur)
            cur = []
        cur.append(r)
    if cur:
        clusters.append(cur)
    out = []
    for c in clusters:
        if len({r["device"] for r in c}) >= min_dev and parse_utc(c[0]["candidate_utc"]) >= skip_before:
            out.append(dict(first_candidate=parse_utc(c[0]["candidate_utc"]),
                            last_candidate=parse_utc(c[-1]["candidate_utc"]),
                            last_confirm=max(parse_utc(r["confirm_utc"]) for r in c), confirms=c))
    return out


def load(data_dir, keep):
    """读全部文件里五个检测通道的读数，只留 keep(t) 为真的时刻，按（设备，通道，时间）去重。"""
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
        sensor = df["Sensor"].astype(str).str.strip()
        t = pd.to_numeric(df["Time"], errors="coerce")
        sel = sensor.isin(CHANNELS) & t.notna() & keep(t)
        if not sel.any():
            continue
        nfiles += 1
        parts.append(pd.DataFrame({"dev": df["DeviceId"].astype(str).str.strip()[sel], "ch": sensor[sel],
                                   "t": t[sel], "v": pd.to_numeric(df["Value"][sel], errors="coerce")}).dropna())
    if not parts:
        return None, nfiles
    d = pd.concat(parts, ignore_index=True)
    d["t"] = d["t"].astype("int64")
    d = d.drop_duplicates(["dev", "ch", "t"], keep="first")
    loc = pd.to_datetime(d["t"], unit="s", utc=True).dt.tz_convert(TZ)
    d["day"] = loc.dt.strftime("%Y-%m-%d")
    d["hod"] = loc.dt.hour
    d["hour_utc"] = (d["t"] // 3600) * 3600
    return d.reset_index(drop=True), nfiles


def width(v):
    return float(np.percentile(v, 90) - np.percentile(v, 10)) if len(v) else float("nan")


def local_str(ts):
    return pd.Timestamp(ts, unit="s", tz="UTC").tz_convert(TZ).strftime("%m-%d %H:%M")


def utc_str(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%m-%d %H:%M")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--ref-start", default="2022-03-08")
    ap.add_argument("--ref-end", default="2022-03-17")
    ap.add_argument("--pad-hours", type=int, default=12)
    ap.add_argument("--min-devices", type=int, default=3)
    ap.add_argument("--cluster-hours", type=int, default=6)
    ap.add_argument("--skip-before", default="2022-03-28")
    ap.add_argument("--confirms", default="docs/m5b/grid/confirms.csv")
    ap.add_argument("--scores", default="docs/m5b/streams_hourly.csv")
    ap.add_argument("--out-dir", default="docs/fleet_episodes")
    ap.add_argument("--report", default="docs/reports/fleet_drift_episodes.md")
    a = ap.parse_args()
    if not (os.path.exists(a.confirms) and os.path.exists(a.scores)):
        print("ERROR: 缺少 %s 或 %s" % (a.confirms, a.scores), file=sys.stderr)
        return 3
    skip = datetime.strptime(a.skip_before, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    eps = find_episodes(a.confirms, a.min_devices, a.cluster_hours, skip)
    if not eps:
        print("ERROR: 按规则没有找到时段", file=sys.stderr)
        return 4
    pad = a.pad_hours * 3600
    for e in eps:                                       # 时段窗口按整点小时取 / whole hours
        e["w0"] = (int(e["first_candidate"].timestamp()) - pad) // 3600 * 3600
        e["w1"] = (int(e["last_confirm"].timestamp()) + pad) // 3600 * 3600 + 3600
        e["core_end"] = int(e["last_confirm"].timestamp())
    # 参照期按当地日期，前后各放宽一天读入，再按当地日期筛 / reference by local date
    r0 = int(datetime.strptime(a.ref_start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) - 86400
    r1 = int(datetime.strptime(a.ref_end, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) + 2 * 86400
    spans = [(r0, r1)] + [(e["w0"], e["w1"]) for e in eps]

    def keep(t):
        m = pd.Series(False, index=t.index)
        for s0, s1 in spans:
            m |= (t >= s0) & (t < s1)
        return m

    d, nfiles = load(a.data_dir, keep)
    if d is None or d.empty:
        print("ERROR: 没有读到读数（检查 --data-dir）", file=sys.stderr)
        return 2
    print("读入文件 %d 个；去重后读数 %d 条；设备 %s；时段 %d 个" % (nfiles, len(d), ",".join(sorted(d["dev"].unique())), len(eps)))

    # 参照：同一当地小时的中位数、整个参照期的 P10–P90 宽度、参照期逐小时宽度的中位数（与 c_alarm 分析相同）
    ref = d[(d["day"] >= a.ref_start) & (d["day"] <= a.ref_end)]
    ref_hod = ref.groupby(["dev", "ch", "hod"])["v"].median()
    ref_w = ref.groupby(["dev", "ch"])["v"].apply(lambda v: width(v.to_numpy()))
    ref_hw = ref.groupby(["dev", "ch", "day", "hod"])["v"].apply(lambda v: width(v.to_numpy())).groupby(["dev", "ch"]).median()

    scores = pd.read_csv(a.scores)
    sc = scores.set_index(["device", "hour_start"])["median_main_score"]

    rows = []
    for i, e in enumerate(eps, 1):
        w = d[(d["t"] >= e["w0"]) & (d["t"] < e["w1"])]
        for (dev, ch, hu), g in w.groupby(["dev", "ch", "hour_utc"]):
            v = g["v"].to_numpy()
            if len(v) < MIN_N:
                continue
            hod = int(pd.Timestamp(hu, unit="s", tz="UTC").tz_convert(TZ).hour)
            rm, rw, rhw = ref_hod.get((dev, ch, hod), np.nan), ref_w.get((dev, ch), np.nan), ref_hw.get((dev, ch), np.nan)
            rows.append(dict(episode=i, device=dev, channel=ch, hour_utc=utc_str(hu), hour_local=local_str(hu), hour_ts=hu,
                             n=len(v), median=float(np.median(v)), p10=float(np.percentile(v, 10)),
                             p90=float(np.percentile(v, 90)), ref_hod_median=rm, ref_width=rw,
                             shift=(float(np.median(v)) - rm) / rw if rw and rw > 0 else np.nan,
                             width_x=width(v) / rhw if rhw and rhw > 0 else np.nan,
                             main_score=sc.get((dev, hu), np.nan)))
    hourly = pd.DataFrame(rows)
    os.makedirs(a.out_dir, exist_ok=True)
    hourly.drop(columns=["hour_ts"]).round(4).to_csv(os.path.join(a.out_dir, "hourly.csv"), index=False)
    ep_rows = []
    for i, e in enumerate(eps, 1):
        ep_rows.append(dict(episode=i, first_candidate_utc=e["first_candidate"].strftime("%m-%d %H:%M"),
                            last_candidate_utc=e["last_candidate"].strftime("%m-%d %H:%M"),
                            last_confirm_utc=e["last_confirm"].strftime("%m-%d %H:%M"),
                            devices="".join(sorted({r["device"] for r in e["confirms"]})),
                            window_utc="%s 至 %s" % (utc_str(e["w0"]), utc_str(e["w1"]))))
    pd.DataFrame(ep_rows).to_csv(os.path.join(a.out_dir, "episodes.csv"), index=False)
    write_report(a, eps, hourly, nfiles, len(d))
    print("报告：%s；长表：%s/hourly.csv（%d 行）" % (a.report, a.out_dir, len(hourly)))
    return 0


def fmt(x, nd=2):
    return "—" if x is None or (isinstance(x, float) and np.isnan(x)) else ("%.*f" % (nd, x))


def longest_run(flags):
    best = cur = 0
    for f in flags:
        cur = cur + 1 if f else 0
        best = max(best, cur)
    return best


def write_report(a, eps, hourly, nfiles, nreadings):
    L = ["# 跨设备同时确认漂移的时段：机队五通道逐小时剖面（0.98 版裁决第二节第四项）", "",
         "由 `eda/fleet_episode_profile.py` 生成。纯分析、只报事实，不下结论；成因由设计会话判定。按数据出处优先约定（CLAUDE.md 第 8 节），"
         "出处材料中没有这类事件的记录（自述文件只有部署描述，0.98 版裁决已核实），所以直接查数据本身。", "",
         "**时段的找法**：默认组合（置信度 0.99、鲁棒度 0.5、十分钟粒度、K = 36）的确认清单按候选时刻排序，以每簇第一个候选为起点、"
         "%d 小时内的确认成簇，至少 %d 台设备的簇为一个时段；%s 之前的（三月事件尾段，成因已知）略去。"
         % (a.cluster_hours, a.min_devices, a.skip_before), "",
         "**剖面的口径**：逐小时按布里斯托当地时间给出（表中也给出世界协调时）。「偏移」= 该小时中位数减参照期（%s 至 %s，当地日期）"
         "同一当地小时的中位数，再除以该台该通道整个参照期的 P10–P90 宽度；「宽度倍数」= 该小时 P10–P90 宽度 ÷ 参照期逐小时宽度的中位数。"
         "一小时内读数少于 %d 条不计。主分为该台该世界协调时小时的主分中位数（`docs/m5b/streams_hourly.csv`）。"
         % (a.ref_start, a.ref_end, MIN_N), "",
         "**描述性标记（只是标记，不是结论）**：|偏移| ≥ %.1f 记为该小时「在动」；「持续」= 时段窗口内连续在动的最长小时数；"
         "「已回落」= 窗口最后一个有数据的小时 |偏移| < %.1f。峰值在 [第一个候选前 %d 小时, 最后一次确认] 内取。"
         % (MOVE, BACK, a.pad_hours), "",
         "读入文件 %d 个，读数 %d 条（只读参照期与各时段窗口）。逐台逐通道逐小时的完整数据见 `docs/fleet_episodes/hourly.csv`。" % (nfiles, nreadings), ""]
    for i, e in enumerate(eps, 1):
        h = hourly[hourly["episode"] == i]
        L += ["## 时段 %d：候选 %s 至 %s（UTC），当地 %s 至 %s" % (
            i, e["first_candidate"].strftime("%m-%d %H:%M"), e["last_candidate"].strftime("%m-%d %H:%M"),
            local_str(int(e["first_candidate"].timestamp())), local_str(int(e["last_candidate"].timestamp()))), ""]
        L += ["确认（候选 → 确认，UTC；检验；最近六个的中位数 / 参照值）：" + "；".join(
            "%s %s → %s，%s，%.2f / %.2f" % (r["device"], r["candidate_utc"], r["confirm_utc"], r["test"],
                                           float(r["last_median"]), float(r["reference"])) for r in e["confirms"]) + "。", ""]
        # 机队逐小时对照 / fleet hour by hour
        L += ["### 机队逐小时对照（各通道：八台偏移的中位数，括号内为在动的设备数）", "",
              "| 当地 | UTC | " + " | ".join("%s %s" % (SHORT[c], c) for c in CHANNELS) + " | 主分中位数（≥ 2.22 的设备数） |",
              "| --- | --- |" + " --- |" * (len(CHANNELS) + 1)]
        for hu, g in h.groupby("hour_ts"):
            cells = []
            for c in CHANNELS:
                s = g[g["channel"] == c]["shift"].dropna()
                cells.append("%s（%d）" % (fmt(float(s.median())), int((s.abs() >= MOVE).sum())) if len(s) else "—")
            ms = g.drop_duplicates("device")["main_score"].dropna()
            cells.append("%s（%d）" % (fmt(float(ms.median())), int((ms >= ALARM).sum())) if len(ms) else "—")
            L.append("| %s | %s | %s |" % (local_str(hu), utc_str(hu), " | ".join(cells)))
        L.append("")
        # 逐台逐通道摘要 / per device and channel
        L += ["### 逐台逐通道摘要（只列峰值 |偏移| ≥ %.1f 或最大宽度倍数 ≥ 3 的组合）" % MOVE, "",
              "| 设备 | 通道 | 峰值偏移 | 峰值时刻（当地） | 在动小时数 | 持续（小时） | 最大宽度倍数 | 已回落 | 该台在本时段有确认 |",
              "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
        conf_dev = {r["device"] for r in e["confirms"]}
        core0 = int(e["first_candidate"].timestamp()) - a.pad_hours * 3600
        for dev in DEVICES:
            for c in CHANNELS:
                g = h[(h["device"] == dev) & (h["channel"] == c)].sort_values("hour_ts")
                if g.empty:
                    continue
                core = g[(g["hour_ts"] >= core0 // 3600 * 3600) & (g["hour_ts"] <= e["core_end"])]
                s = core["shift"].dropna()
                if s.empty:
                    continue
                pk = s.abs().idxmax()
                moving = (g["shift"].abs() >= MOVE).tolist()
                wx = g["width_x"].max()
                if abs(s[pk]) < MOVE and not (wx >= 3):
                    continue
                last = g["shift"].dropna()
                L.append("| %s | %s | %s | %s | %d | %d | %s | %s | %s |" % (
                    dev, c, fmt(float(s[pk])), core.loc[pk, "hour_local"], sum(moving), longest_run(moving), fmt(float(wx)),
                    ("是" if abs(last.iloc[-1]) < BACK else "否") if len(last) else "—", "是" if dev in conf_dev else "否"))
        L.append("")
    with open(a.report, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")


if __name__ == "__main__":
    sys.exit(main())
