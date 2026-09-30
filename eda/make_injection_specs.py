#!/usr/bin/env python3
# ============================================================================
# make_injection_specs.py
# 为 V-M3-5（注入召回）生成重放器 --inject 的注入规格：四种类型 × 三档强度，排在注入窗口内、彼此
# 不重叠。强度以该设备该通道在 M1 标定期内原始值的四分位距（IQR）为单位，再换算成原始单位——
# 重放器的注入是直接加到原始值上的（Injector.java：spike/step 为 原值 + 幅度，ramp 为 原值 + 幅度 ×
# 进度，stuck 冻结在注入开始时的值）。
# Build the replayer's --inject specs for V-M3-5: four types x three strengths, non-overlapping inside
# the injection window. Strength is in units of the channel's raw IQR over the M1 calibration period,
# converted to raw units because the replayer adds the magnitude to the raw value.
#
# 依据：交接文档 §4 与 §5 V-M3-5；2026-09-28 裁决书第三节（注入窗口 2022-03-26 至 03-31，设备 E）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt。
# 2. 调用命令：
#      python3 eda/make_injection_specs.py \
#          --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
#          --out docs/m3_march_injection_plan.csv
#    可选：--device E、--channel Temperature、--multipliers 1,3,6、--stuck-minutes 30,120,360、
#    --calib-start 2022-03-01、--calib-end 2022-03-08、--window-start 2022-03-26、--slot-hours 10。
# 3. 前置条件：原始 CSV 目录可读。
# 4. 期望产出：终端打印标定期的中位数与 IQR、12 条注入的计划表，以及一整行 --inject 规格串；
#    --out 处写出计划表 CSV（同时写出 docs/m3_march_inject_spec.txt，内含规格串，供重放命令直接读取）。
# 5. 失败兜底：标定期内该设备该通道没有数据时退出码 2；注入排不进窗口时退出码 3。
#
# 默认方案（须设计会话确认）：通道 Temperature；spike 持续 30 秒（3 轮），step 持续 2 小时，ramp 持续
# 6 小时（结束时达到满幅），三档强度为 1、3、6 倍 IQR；stuck 没有「幅度」，三档改为冻结 30 分钟、
# 2 小时、6 小时。每条注入占一个 10 小时的时段，从时段开头 2 小时处开始，留出前后间隔。
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

DUR_SEC = {"spike": 30, "step": 2 * 3600, "ramp": 6 * 3600}


def utc(day: str) -> datetime:
    return datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--device", default="E")
    ap.add_argument("--channel", default="Temperature")
    ap.add_argument("--multipliers", default="1,3,6")
    ap.add_argument("--stuck-minutes", default="30,120,360")
    ap.add_argument("--calib-start", default="2022-03-01")
    ap.add_argument("--calib-end", default="2022-03-08")
    ap.add_argument("--window-start", default="2022-03-26")
    ap.add_argument("--window-end", default="2022-03-31")
    ap.add_argument("--slot-hours", type=int, default=10)
    ap.add_argument("--lead-hours", type=int, default=2)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

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
    med, q1, q3 = np.median(v), np.percentile(v, 25), np.percentile(v, 75)
    iqr = q3 - q1
    print(f"标定期 {a.calib_start} 至 {a.calib_end}，设备 {a.device} 通道 {a.channel}："
          f"{len(v)} 个读数，中位数 {med:.4f}，IQR {iqr:.4f}（P25 {q1:.4f}，P75 {q3:.4f}）")

    mults = [float(x) for x in a.multipliers.split(",")]
    stuck = [int(x) for x in a.stuck_minutes.split(",")]
    plan = []
    for typ in ("spike", "step", "ramp"):
        for i, m in enumerate(mults):
            plan.append((typ, f"{m:g}xIQR", round(m * iqr, 4), DUR_SEC[typ]))
    for i, mins in enumerate(stuck):
        plan.append(("stuck", f"{mins}min", 0.0, mins * 60))

    start0 = utc(a.window_start)
    end = utc(a.window_end)
    rows, specs = [], []
    for k, (typ, level, mag, dur) in enumerate(plan):
        st = start0 + timedelta(hours=k * a.slot_hours + a.lead_hours)
        if st + timedelta(seconds=dur) > end:
            print(f"ERROR: 第 {k + 1} 条注入（{typ} {level}）排不进窗口，结束于 "
                  f"{(st + timedelta(seconds=dur)).isoformat()}，晚于 {a.window_end}。", file=sys.stderr)
            return 3
        ts = int(st.timestamp())
        specs.append(f"{a.device}:{a.channel}:{ts}:{dur}:{typ}:{mag}")
        rows.append({"device": a.device, "channel": a.channel, "type": typ, "level": level,
                     "magnitude_raw": mag, "start_utc": st.strftime("%Y-%m-%dT%H:%M:%SZ"),
                     "start_ts": ts, "duration_sec": dur,
                     "end_utc": (st + timedelta(seconds=dur)).strftime("%Y-%m-%dT%H:%M:%SZ")})

    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    spec = ";".join(specs)
    spec_path = os.path.join(os.path.dirname(a.out) or ".", "m3_march_inject_spec.txt")
    with open(spec_path, "w") as fh:
        fh.write(spec + "\n")
    print(f"\n{'类型':<6}{'档位':<10}{'原始幅度':>10}  {'开始（UTC）':<22}{'持续秒数':>8}")
    for r in rows:
        print(f"{r['type']:<8}{r['level']:<10}{r['magnitude_raw']:>10}  {r['start_utc']:<22}{r['duration_sec']:>8}")
    print(f"\n计划表：{a.out}\n规格串：{spec_path}\n--inject \"{spec}\"")
    return 0


if __name__ == "__main__":
    sys.exit(main())
