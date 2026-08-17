#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_test_data.py —— 生成结构与 Erol/SYNERGIA 一致的合成数据集，用于自测。
make_test_data.py -- generate a synthetic dataset shaped like Erol/SYNERGIA for self-testing.

用途 / purpose:
真实数据集在用户本地（约 22 GB），开发环境拿不到。本脚本按交接文档 §3 的数据事实
合成一个小规模等价数据集，并**刻意注入全部边界情形**，用于验收标准中的
"畸形行/异常文件有计数入报告"与"断点续跑实测有效"两项。
The real 22 GB dataset lives on the user's machine. This generator reproduces its shape
per handover §3 and deliberately injects every edge case, so the acceptance criteria
(malformed rows counted, resume verified) can be exercised without it.

注入的边界情形 / injected edge cases:
  1. 空文件（0 字节）、截断文件（行中间被切断）
  2. 缺表头文件；两种文件名模式（HH-MM-SS 与紧凑 HHMMSS）；子目录（测试递归）
  3. 畸形行：列数不足、列数过多、Value 非数字、Time 非数字
  4. 哨兵值 -999；Pressure 越界尖峰；重复 (Device, Time, Sensor) 行
  5. 设备 H 仅在前半段在场（对应 DEV-Q2 的在场区间问题）
  6. Accelerometer 绝大多数为 0，少量非零事件（对应 DEV-Q1）
  7. MIC 取值 {1, 2, 3}；采样间隔 10 s 带抖动，含长缺口
  8. 温度带月度漂移（供 E5 漂移预览层出非平凡结果）

============================== 脚本交付五要素 ==============================
1. 执行环境：Python 3.9+，仅标准库（random/os/datetime）
2. 调用命令：python3 eda/tests/make_test_data.py --out-dir /tmp/eda_synth --days 12
3. 前置条件：--out-dir 可写（已存在的同名目录内容会被覆盖同名文件）
4. 期望产出：<out-dir>/ 下若干 CSV（含 sub/ 子目录）+ 控制台打印的注入清单
5. 失败兜底：目录不可写 → 报错退出码 2；生成过程无副作用，可重复执行
==========================================================================
"""

import argparse
import datetime as _dt
import os
import random
import sys

SENSORS = ["Temperature", "Humidity", "Pressure", "Gas", "Accelerometer", "Light", "MIC", "RSSI"]
DEVICES = ["A", "B", "C", "D", "E", "F", "G", "H"]
HEADER = "Time,DeviceId,Sensor,Value\n"


def sensor_value(sensor: str, rng: random.Random, day_index: int, hour: int) -> float:
    """
    按通道生成一个物理上说得通的读数；温度/光照带日内与月度趋势。
    Produce a physically plausible reading; temperature and light carry diurnal and
    monthly trends so the drift-preview layer has something non-trivial to show.
    """
    season = day_index / 30.0                      # 月度漂移 / monthly drift
    diurnal = max(0.0, 1.0 - abs(hour - 13) / 8.0)  # 日内形态 / diurnal shape
    if sensor == "Temperature":
        return 20.0 + 2.0 * season + 3.0 * diurnal + rng.gauss(0, 0.3)
    if sensor == "Humidity":
        return 45.0 - 1.5 * season - 5.0 * diurnal + rng.gauss(0, 1.0)
    if sensor == "Pressure":
        return 101500.0 + 300.0 * rng.gauss(0, 1)
    if sensor == "Gas":
        return 600.0 + 120.0 * season + rng.gauss(0, 40)
    if sensor == "Accelerometer":
        return 0.0
    if sensor == "Light":
        return max(0.0, 200.0 + 3500.0 * diurnal + rng.gauss(0, 120))
    if sensor == "MIC":
        return float(rng.choice([1, 1, 1, 2, 3, 3]))
    if sensor == "RSSI":
        return float(rng.randint(45, 95))
    return 0.0


def build_file(path: str, start_epoch: int, n_rounds: int, devices, rng: random.Random,
               day_index: int, opts: dict) -> None:
    """
    写出一个文件：n_rounds 轮 × devices × 8 传感器，间隔 10 s 带抖动。
    Write one file: n_rounds rounds x devices x 8 sensors, 10 s nominal with jitter.
    """
    lines = []
    if opts.get("header", True):
        lines.append(HEADER)
    t = start_epoch
    for r in range(n_rounds):
        # 缺口注入：中途跳过一大段时间 / inject a gap by skipping ahead
        if opts.get("gap_at") == r:
            t += opts.get("gap_len", 900)
        for di, dev in enumerate(devices):
            # 跨设备相位：各设备在同一轮内相差 0–2 s / cross-device phase offset
            dev_t = t + (di % 3)
            for sensor in SENSORS:
                val = sensor_value(sensor, rng, day_index, (dev_t // 3600) % 24)
                if sensor == "Accelerometer" and rng.random() < opts.get("accel_nonzero_p", 0.0):
                    val = round(rng.uniform(0.5, 2.0), 4)
                if sensor == "Pressure" and rng.random() < opts.get("oor_p", 0.0):
                    val = 89000.0        # 越界尖峰 / out-of-range spike
                if rng.random() < opts.get("sentinel_p", 0.0):
                    val = -999.0         # 哨兵值 / sentinel
                lines.append("%d,%s,%s,%s\n" % (dev_t, dev, sensor, repr(float(val))))
        t += 10 + rng.randint(-3, 8)     # 抖动 / jitter

    # 畸形行注入 / malformed row injection
    if opts.get("malformed"):
        lines.insert(min(len(lines), 5), "%d,A,Temperature\n" % start_epoch)              # 列数不足
        lines.insert(min(len(lines), 9), "%d,A,Temperature,21.0,extra\n" % start_epoch)   # 列数过多
        lines.insert(min(len(lines), 13), "%d,A,Temperature,not_a_number\n" % start_epoch)  # 值非数字
        lines.insert(min(len(lines), 17), "not_a_time,A,Temperature,21.0\n")              # 时间非数字
    # 重复行注入 / duplicate row injection
    if opts.get("duplicates") and len(lines) > 3:
        lines.append(lines[2])
        lines.append(lines[3])

    text = "".join(lines)
    if opts.get("truncate"):
        text = text[: int(len(text) * 0.7)].rsplit("\n", 1)[0] + "\n" + "16455873"  # 半截行
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="生成合成测试数据 / generate synthetic test data")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--days", type=int, default=12)
    p.add_argument("--files-per-day", type=int, default=3)
    p.add_argument("--rounds", type=int, default=120, help="每文件采样轮数 / rounds per file")
    p.add_argument("--start", default="2022-02-01", help="起始日期 (YYYY-MM-DD)")
    p.add_argument("--seed", type=int, default=20260817)
    args = p.parse_args(argv)

    out_dir = os.path.abspath(os.path.expanduser(args.out_dir))
    try:
        os.makedirs(os.path.join(out_dir, "sub"), exist_ok=True)
    except OSError as exc:
        print("[错误 / error] 无法创建目录 / cannot create directory: %s" % exc, file=sys.stderr)
        return 2

    rng = random.Random(args.seed)
    start = _dt.datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=_dt.timezone.utc)
    injected = []
    n_files = 0

    for day in range(args.days):
        for slot in range(args.files_per_day):
            stamp = start + _dt.timedelta(days=day, hours=slot * 8, minutes=3)
            epoch = int(stamp.timestamp())
            # 设备 H 只在前半段在场 / device H present in the first half only
            devices = DEVICES if day < args.days // 2 else DEVICES[:-1]
            idx = day * args.files_per_day + slot

            compact = (idx % 5 == 0)     # 紧凑文件名模式 / compact filename pattern
            name = stamp.strftime("%Y_%m_%d_") + (
                stamp.strftime("%H%M%S") if compact else stamp.strftime("%H-%M-%S"))
            name += "_data.csv" if idx % 3 else ".csv"
            sub = "sub" if idx % 7 == 3 else ""   # 部分文件放子目录 / some files in a subdir
            path = os.path.join(out_dir, sub, name)

            opts = {
                "header": idx != 4,                       # 一个文件缺表头 / one file has no header
                "malformed": idx % 6 == 1,
                "duplicates": idx % 9 == 2,
                "truncate": idx == 7,
                "gap_at": args.rounds // 2 if idx % 8 == 5 else None,
                "gap_len": 3600 + idx * 60,
                "accel_nonzero_p": 0.0002 if idx % 4 == 0 else 0.0,
                "oor_p": 0.0005 if idx % 11 == 0 else 0.0,
                "sentinel_p": 0.0002 if idx % 13 == 0 else 0.0,
            }
            build_file(path, epoch, args.rounds, devices, rng, day, opts)
            n_files += 1
            for k in ("malformed", "duplicates", "truncate"):
                if opts.get(k):
                    injected.append("%s: %s" % (os.path.join(sub, name), k))
            if not opts["header"]:
                injected.append("%s: no header" % os.path.join(sub, name))
            if opts["gap_at"] is not None:
                injected.append("%s: gap %ds" % (os.path.join(sub, name), opts["gap_len"]))

    # 空文件 / empty file
    empty = os.path.join(out_dir, "2022_02_02_09-00-00_data.csv")
    open(empty, "w").close()
    injected.append("%s: empty file" % os.path.basename(empty))
    n_files += 1

    print("生成完成 / generated: %d 个 CSV → %s" % (n_files, out_dir))
    print("注入的边界情形 / injected edge cases:")
    for line in injected:
        print("  - %s" % line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
