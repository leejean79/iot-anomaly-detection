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
# 依据：2026-10-06 设计会话《注入实验参数（书面版）》（整理自 10-02 裁决第六节与 10-03 确认）：
#   - 注入设备 E；主通道温度；若排程有余，在气体通道上补做阶跃两倍、爬坡两倍各一次。
#   - 幅度：阈值校准期（03-17 至 03-24）该通道 P10 至 P90 宽度的 1、2、4 倍（卡死无幅度）。
#   - 尖峰：一、二、三轮各对应一档幅度（1 倍一轮、2 倍两轮、4 倍三轮）；阶跃 2 小时；爬坡 6 小时，
#     加量从零线性增长到标称幅度；卡死在三个不同时刻各一次，每次 2 小时，值冻结为注入开始时刻的读数。
#   - 相邻注入间隔下限 3660 秒（点通道窗长 3600 秒加滑动步 60 秒），默认 7200 秒。
#   - 排程范围：按新参照期（03-08 至 03-17）重算后的四月平稳日。
# 尖峰要恰好覆盖 k 轮：脚本读取该设备该通道在平稳日里的真实轮时间戳，尖峰从某一轮开始，到第 k+1 轮
# 之前结束（注入区间左闭右开），而不是简单取 10k 秒——实际轮间隔并不恒为 10 秒。
# Per the design session's written injection parameters of 2026-10-06. Spikes cover exactly k rounds by
# aligning to the device's real round timestamps.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境：本地 Mac，仓库根目录，Python 3.9+，已装 eda/requirements.txt。
# 2. 调用命令：
#      python3 eda/make_injection_specs.py \
#          --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
#          --days 2022-04-10,2022-04-11,2022-04-12 --out docs/m3_inject_plan.csv
#    --days 为重算后的四月平稳日（取自 m3_v34_report.py 报告）。
#    可选：--device E、--channel Temperature、--extra-channel Gas（设为空串则不补做）、--multipliers 1,2,4、
#    --calib-start 2022-03-17、--calib-end 2022-03-24、--gap-sec 7200（不得小于 3660）、
#    --lead-hours 2（每个平稳日从 UTC 几点开始排）。
# 3. 前置条件：原始 CSV 目录可读；--days 已由 V-M3-4 报告确定。
# 4. 期望产出：终端打印两个通道标定期的中位数与 P10–P90 宽度、注入计划表（主通道 12 条，排得下时另加
#    气体通道 2 条）；--out 处写出计划表 CSV，同目录写出 m3_inject_spec.txt（供 syn-replay.sh --inject-file）。
# 5. 失败兜底：标定期内主通道没有数据时退出码 2；平稳日排不下主通道的 12 条时退出码 3；间隔小于 3660 秒或
#    --days 中有不在四月的日子时退出码 4。气体通道的补做排不下时只打印说明，不算失败。
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

DUR_SEC = {"step": 2 * 3600, "ramp": 6 * 3600, "stuck": 2 * 3600}
SPIKE_ROUNDS = (1, 2, 3)        # 第 i 档幅度对应的尖峰轮数 / rounds per spike tier
MIN_GAP_SEC = 3600 + 60         # 点通道窗长加一个滑动步 / point-channel window plus one slide


def utc(day: str) -> datetime:
    return datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def load(data_dir, device, channels, ranges):
    """读取某设备若干通道在若干时段内的 (时间戳, 值) / read (ts, value) for channels within ranges."""
    out = {c: [] for c in channels}
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
        t = pd.to_numeric(df["Time"], errors="coerce")
        in_range = np.logical_or.reduce([(t >= lo) & (t < hi) for lo, hi in ranges])
        dev = df["DeviceId"].astype(str).str.strip() == device
        sens = df["Sensor"].astype(str).str.strip()
        for c in channels:
            sel = dev & (sens == c) & in_range
            if sel.any():
                v = pd.to_numeric(df["Value"][sel], errors="coerce")
                ok = v.notna()
                out[c].append(np.column_stack([t[sel][ok].to_numpy(), v[ok].to_numpy()]))
    return {c: (np.concatenate(a) if a else np.empty((0, 2))) for c, a in out.items()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--device", default="E")
    ap.add_argument("--channel", default="Temperature")
    ap.add_argument("--extra-channel", default="Gas")
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
        print("ERROR: --gap-sec 不得小于 %d 秒（点通道窗长加滑动步），--days 必须都是四月的日子。" % MIN_GAP_SEC,
              file=sys.stderr)
        return 4
    channels = [a.channel] + ([a.extra_channel] if a.extra_channel else [])
    calib = (int(utc(a.calib_start).timestamp()), int(utc(a.calib_end).timestamp()))
    day_ranges = [(int(utc(d).timestamp()), int((utc(d) + timedelta(days=1)).timestamp())) for d in days]

    cal = load(a.data_dir, a.device, channels, [calib])
    width = {}
    for c in channels:
        v = cal[c][:, 1] if len(cal[c]) else np.empty(0)
        if len(v) == 0:
            if c == a.channel:
                print(f"ERROR: {a.calib_start} 至 {a.calib_end} 内没有设备 {a.device} 通道 {c} 的数据。", file=sys.stderr)
                return 2
            print(f"说明：标定期内没有通道 {c} 的数据，不做该通道的补做注入。")
            continue
        med, p10, p90 = np.median(v), np.percentile(v, 10), np.percentile(v, 90)
        width[c] = p90 - p10
        print(f"标定期 {a.calib_start} 至 {a.calib_end}，设备 {a.device} 通道 {c}：{len(v)} 个读数，"
              f"中位数 {med:.4f}，P10–P90 宽度 {width[c]:.4f}（P10 {p10:.4f}，P90 {p90:.4f}）")

    # 主通道在平稳日里的真实轮时间戳，用来让尖峰恰好覆盖 k 轮 / real round timestamps for exact spikes
    rounds = np.unique(load(a.data_dir, a.device, [a.channel], day_ranges)[a.channel][:, 0].astype(np.int64))

    mults = [float(x) for x in a.multipliers.split(",")]
    plan = []                                   # (通道, 类型, 档位, 原始幅度, 时长或轮数, 是否补做)
    for i, m in enumerate(mults):
        plan.append((a.channel, "spike", f"{m:g}xP10P90/{SPIKE_ROUNDS[i]}轮", round(m * width[a.channel], 4),
                     ("rounds", SPIKE_ROUNDS[i]), False))
    for typ in ("step", "ramp"):
        for m in mults:
            plan.append((a.channel, typ, f"{m:g}xP10P90", round(m * width[a.channel], 4), ("sec", DUR_SEC[typ]), False))
    for k in range(3):
        plan.append((a.channel, "stuck", f"2h#{k + 1}", 0.0, ("sec", DUR_SEC["stuck"]), False))
    if a.extra_channel and a.extra_channel in width:
        for typ in ("step", "ramp"):
            plan.append((a.extra_channel, typ, "2xP10P90", round(2 * width[a.extra_channel], 4),
                         ("sec", DUR_SEC[typ]), True))

    # 依次排进平稳日：每天从 lead-hours 开始；一条注入整条落在同一个平稳日之内；相邻两条之间至少 gap-sec。
    # Greedy placement; an injection must fit within one stable day; gaps >= gap-sec.
    rows, specs = [], []
    di = 0
    cursor = utc(days[0]) + timedelta(hours=a.lead_hours)
    for ch, typ, level, mag, (kind, n), extra in plan:
        placed = False
        while di < len(days):
            day_end = utc(days[di]) + timedelta(days=1)
            if kind == "rounds":
                idx = int(np.searchsorted(rounds, int(cursor.timestamp())))
                if idx + n < len(rounds) and rounds[idx + n] <= day_end.timestamp():
                    start_ts, dur = int(rounds[idx]), int(rounds[idx + n] - rounds[idx])
                    placed = True
                    break
            elif cursor + timedelta(seconds=n) <= day_end:
                start_ts, dur = int(cursor.timestamp()), n
                placed = True
                break
            di += 1
            if di < len(days):
                cursor = utc(days[di]) + timedelta(hours=a.lead_hours)
        if not placed:
            if extra:
                print(f"说明：平稳日已排满，气体通道的补做（{typ} {level}）不再安排。")
                continue
            print(f"ERROR: 平稳日 {a.days} 排不下主通道的全部注入（卡在 {typ} {level}）。"
                  f"请多给几个平稳日，或减小 --gap-sec。", file=sys.stderr)
            return 3
        st = datetime.fromtimestamp(start_ts, timezone.utc)
        specs.append(f"{a.device}:{ch}:{start_ts}:{dur}:{typ}:{mag}")
        rows.append({"device": a.device, "channel": ch, "type": typ, "level": level,
                     "magnitude_raw": mag, "unit": "P10-P90 width %.4f" % width[ch],
                     "start_utc": st.strftime("%Y-%m-%dT%H:%M:%SZ"), "start_ts": start_ts, "duration_sec": dur,
                     "end_utc": (st + timedelta(seconds=dur)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                     "extra": "是" if extra else ""})
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
    print(f"\n{'通道':<12}{'类型':<6}{'档位':<16}{'原始幅度':>10}  {'开始（UTC）':<22}{'持续秒数':>8}")
    for r in rows:
        print(f"{r['channel']:<14}{r['type']:<8}{r['level']:<18}{r['magnitude_raw']:>10}  "
              f"{r['start_utc']:<22}{r['duration_sec']:>8}")
    print(f"\n共 {len(rows)} 条。计划表：{a.out}\n规格串：{spec_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
