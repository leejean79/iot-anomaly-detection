#!/usr/bin/env python3
# ============================================================================
# make_injection_specs.py
# 为 V-M3-5（注入召回）生成重放器 --inject 的注入规格：四种类型 × 三档强度，排在注入窗口内、彼此
# 不重叠。强度以该设备该通道在标定期内原始值的 P10 至 P90 宽度为单位，再换算成原始单位——
# 重放器的注入是直接加到原始值上的（Injector.java：spike/step 为 原值 + 幅度，ramp 为 原值 + 幅度 ×
# 进度，stuck 冻结在注入开始时的值）。
# Build the replayer's --inject specs for V-M3-5: four types x three strengths, non-overlapping inside
# the injection window. Strength is in units of the channel's raw P10-P90 width over the calibration period,
# converted to raw units because the replayer adds the magnitude to the raw value.
#
# 依据：2026-10-02 裁决第六节（注入方案的要求）：注入设备 E；注入放在四月的平稳日里；四种类型各三档，
# 幅度以标定期 P10 至 P90 宽度的倍数表达（1、2、4 倍）；时长为尖峰 3 轮（30 秒）、阶跃 2 小时、爬坡 6 小时、
# 卡死 2 小时；相邻注入之间至少间隔一个窗长加一个滑动步；真值写入日志（重放器的 --inject-log）。
#
# 「标定期」取阈值校准期（默认 2022-03-17 至 03-24），与 V-M3-4 的「标定期宽度」同一口径。卡死没有幅度，
# 「三档」无法按幅度取，本脚本的做法是卡死三次、各 2 小时，排在不同时刻，这一点须送交设计会话确认。
# Per the ruling of 2026-10-02 section 6. "Calibration period" is the threshold-calibration period,
# the same reference as V-M3-4. Stuck has no magnitude; it is injected three times for 2 hours each,
# which the design session must confirm.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt。
# 2. 调用命令：
#      python3 eda/make_injection_specs.py \
#          --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
#          --days 2022-04-10,2022-04-11,2022-04-12,2022-04-13 \
#          --out docs/m3_inject_plan.csv
#    --days 为平稳日（取自 m3_v34_report.py 报告的平稳日清单，须是四月的日子）。
#    可选：--device E、--channel Temperature、--multipliers 1,2,4、--calib-start 2022-03-17、
#    --calib-end 2022-03-24、--gap-sec 7200（相邻注入的间隔，不得小于窗长加滑动步，即 660 秒）、
#    --lead-hours 2（每个平稳日从 UTC 几点开始排）。
# 3. 前置条件：原始 CSV 目录可读；--days 已由 V-M3-4 报告确定。
# 4. 期望产出：终端打印标定期的中位数与 P10–P90 宽度、12 条注入的计划表；--out 处写出计划表 CSV，同目录
#    写出 m3_inject_spec.txt（规格串，供 syn-replay.sh --inject-file 读取）。
# 5. 失败兜底：标定期内该设备该通道没有数据时退出码 2；平稳日排不下全部注入时退出码 3；间隔小于
#    660 秒或 --days 中有不在四月的日子时退出码 4。
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

DUR_SEC = {"spike": 30, "step": 2 * 3600, "ramp": 6 * 3600, "stuck": 2 * 3600}
MIN_GAP_SEC = 60 * 10 + 60      # 一个窗长（60 轮 × 10 秒）加一个滑动步（60 秒）/ one window plus one slide


def utc(day: str) -> datetime:
    return datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--device", default="E")
    ap.add_argument("--channel", default="Temperature")
    ap.add_argument("--multipliers", default="1,2,4")
    ap.add_argument("--calib-start", default="2022-03-17")
    ap.add_argument("--calib-end", default="2022-03-24")
    ap.add_argument("--days", required=True, help="四月的平稳日，逗号分隔，如 2022-04-10,2022-04-11")
    ap.add_argument("--gap-sec", type=int, default=7200)
    ap.add_argument("--lead-hours", type=int, default=2)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    days = [d.strip() for d in a.days.split(",") if d.strip()]
    if a.gap_sec < MIN_GAP_SEC or any(not d.startswith("2022-04-") for d in days):
        print("ERROR: --gap-sec 不得小于 %d 秒（窗长加滑动步），--days 必须都是四月的日子。" % MIN_GAP_SEC,
              file=sys.stderr)
        return 4
    lo, hi = int(utc(a.calib_start).timestamp()), int(utc(a.calib_end).timestamp())
    vals = []
    for path in list_all_files(a.data_dir):
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
        t = pd.to_numeric(df["Time"], errors="coerce")
        sel = ((df["DeviceId"].astype(str).str.strip() == a.device)
               & (df["Sensor"].astype(str).str.strip() == a.channel) & (t >= lo) & (t < hi))
        if sel.any():
            vals.append(pd.to_numeric(df["Value"][sel], errors="coerce").dropna().to_numpy())
    if not vals:
        print(f"ERROR: {a.calib_start} 至 {a.calib_end} 内没有设备 {a.device} 通道 {a.channel} 的数据。",
              file=sys.stderr)
        return 2
    v = np.concatenate(vals)
    med, p10, p90 = np.median(v), np.percentile(v, 10), np.percentile(v, 90)
    width = p90 - p10
    print(f"标定期 {a.calib_start} 至 {a.calib_end}，设备 {a.device} 通道 {a.channel}："
          f"{len(v)} 个读数，中位数 {med:.4f}，P10–P90 宽度 {width:.4f}（P10 {p10:.4f}，P90 {p90:.4f}）")

    # 计划：尖峰、阶跃、爬坡各三档幅度；卡死没有幅度，三次各 2 小时（须设计会话确认）。
    mults = [float(x) for x in a.multipliers.split(",")]
    plan = []
    for typ in ("spike", "step", "ramp"):
        for m in mults:
            plan.append((typ, f"{m:g}xP10P90", round(m * width, 4), DUR_SEC[typ]))
    for k in range(3):
        plan.append(("stuck", f"2h#{k + 1}", 0.0, DUR_SEC["stuck"]))

    # 依次排进平稳日：每天从 lead-hours 开始；一条注入必须整条落在同一个平稳日之内；相邻两条之间至少 gap-sec。
    # Greedy placement inside the stable days; an injection must fit within one day; gaps >= gap-sec.
    rows, specs = [], []
    di = 0
    cursor = utc(days[0]) + timedelta(hours=a.lead_hours)
    for typ, level, mag, dur in plan:
        while True:
            day_end = utc(days[di]) + timedelta(days=1)
            if cursor + timedelta(seconds=dur) <= day_end:
                break
            di += 1
            if di >= len(days):
                print(f"ERROR: 平稳日 {a.days} 排不下全部 {len(plan)} 条注入（卡在 {typ} {level}）。"
                      f"请多给几个平稳日，或减小 --gap-sec。", file=sys.stderr)
                return 3
            cursor = utc(days[di]) + timedelta(hours=a.lead_hours)
        st = cursor
        ts = int(st.timestamp())
        specs.append(f"{a.device}:{a.channel}:{ts}:{dur}:{typ}:{mag}")
        rows.append({"device": a.device, "channel": a.channel, "type": typ, "level": level,
                     "magnitude_raw": mag, "unit": "P10-P90 width %.4f" % width,
                     "start_utc": st.strftime("%Y-%m-%dT%H:%M:%SZ"), "start_ts": ts, "duration_sec": dur,
                     "end_utc": (st + timedelta(seconds=dur)).strftime("%Y-%m-%dT%H:%M:%SZ")})
        cursor = st + timedelta(seconds=dur + a.gap_sec)

    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    spec = ";".join(specs)
    spec_path = os.path.join(os.path.dirname(a.out) or ".", "m3_inject_spec.txt")
    with open(spec_path, "w") as fh:
        fh.write(spec + "\n")
    print(f"\n{'类型':<6}{'档位':<10}{'原始幅度':>10}  {'开始（UTC）':<22}{'持续秒数':>8}")
    for r in rows:
        print(f"{r['type']:<8}{r['level']:<10}{r['magnitude_raw']:>10}  {r['start_utc']:<22}{r['duration_sec']:>8}")
    print(f"\n计划表：{a.out}\n规格串：{spec_path}\n--inject \"{spec}\"")
    return 0


if __name__ == "__main__":
    sys.exit(main())
