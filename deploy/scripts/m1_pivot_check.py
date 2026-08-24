#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
m1_pivot_check.py —— V-M1-3 装配抽检：从 synergia-m1-out 抽 N 个归一化 DeviceRound，
逐值与原始 CSV 比对（交接文档 §6 V-M1-3）。
m1_pivot_check.py -- V-M1-3 pivot spot check: sample N normalized DeviceRounds from
synergia-m1-out and compare every value against the original CSV (handover §6 V-M1-3).

================================ 脚本交付五要素 / Five delivery elements ================================
1. 执行环境 / Environment: Python 3.8+（仅标准库）；在能读数据集 CSV 的机器上运行（如 master，
   或本地留存的数据集副本）。
2. 调用命令 / Invocation:
     # 先在 master 上把若干 m1-out 消息转储为 JSONL（每行一个 DeviceRound）:
     #   docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server <b> \
     #       --topic synergia-m1-out --from-beginning --max-messages 2000 > m1out.jsonl
     python3 deploy/scripts/m1_pivot_check.py --rounds-jsonl m1out.jsonl \
             --data-dir /opt/fa-iforest/datasets/synergia/files_csv --n 50
3. 前置条件 / Preconditions: m1out.jsonl 为每行一个 DeviceRound 的 JSON；数据集 CSV 可读。
4. 期望产出 / Expected output: 逐轮比对结果与总通过率；退出码 0=全通过，1=有不符（详情打印）。
5. 失败兜底 / Failure fallback: 找不到某轮对应原始行 → 计为 UNMATCHED 并打印（不崩溃）；
   数值比较用容差（默认 1e-6 相对/绝对）。
=====================================================================================================

比对口径 / comparison basis:
  - DeviceRound.x[5] 为**原始** F_det 值（Temperature,Humidity,Pressure,Gas,Light），直接与 CSV 原值比。
    DeviceRound.x[5] are the RAW F_det values, compared directly to the CSV.
  - 归一化值 xNorm 不在此比对（其正确性由单测 M1PipelineTest 覆盖）。
    Normalized xNorm is not compared here (covered by the unit test M1PipelineTest).
  - 缺失通道（missingMask）跳过；右删失 Light（censoredMask）按原值 65536 比对。
"""

import argparse
import json
import os
import random
import sys

CHANNELS = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]


def build_file_index(data_dir):
    """
    建立 文件名时间 → 文件路径 的有序索引（供按 round ts 就近定位候选文件）。
    Build an ordered (filename-time → path) index to locate candidate files near a round ts.
    """
    import re
    name_re = re.compile(r"(\d{4})_(\d{2})_(\d{2})_(\d{2})[-_]?(\d{2})[-_]?(\d{2})(?:_data)?\.csv$")
    import datetime as dt
    idx = []
    for root, _dirs, files in os.walk(data_dir):
        for fn in files:
            m = name_re.search(fn)
            if not m:
                continue
            try:
                t = int(dt.datetime(*(int(m.group(i)) for i in range(1, 7)),
                                    tzinfo=dt.timezone.utc).timestamp())
            except ValueError:
                continue
            idx.append((t, os.path.join(root, fn)))
    idx.sort()
    return idx


def candidate_files(idx, ts, window_sec=7200):
    """文件名时间在 [ts-2h, ts] 内的文件（一份文件约含此后 1h 的数据）。"""
    return [p for (t, p) in idx if ts - window_sec <= t <= ts + window_sec]


def original_values(paths, device, ts):
    """
    在候选文件中查 (device, ts) 各传感器原值 → {sensor: value}。
    Find raw per-sensor values for (device, ts) in the candidate files.
    """
    found = {}
    target = str(ts)
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    # 快速前缀过滤 / fast prefix filter
                    if not line.startswith(target + ","):
                        continue
                    f = line.rstrip("\n").split(",")
                    if len(f) != 4 or f[1] != device:
                        continue
                    try:
                        found[f[2]] = float(f[3])
                    except ValueError:
                        pass
        except OSError:
            continue
    return found


def compare_round(rnd, idx, tol=1e-6):
    """
    比对单个 DeviceRound.x 与原始 CSV；返回 (status, detail)。
    Compare one DeviceRound.x against the original CSV; return (status, detail).
    """
    device = rnd.get("device")
    ts = rnd.get("ts")
    x = rnd.get("x") or []
    missing = rnd.get("missingMask") or [False] * len(CHANNELS)
    paths = candidate_files(idx, ts)
    if not paths:
        return "UNMATCHED", "no candidate file near ts=%s" % ts
    orig = original_values(paths, device, ts)
    if not orig:
        return "UNMATCHED", "no CSV rows for (%s, %s)" % (device, ts)

    mismatches = []
    for i, ch in enumerate(CHANNELS):
        if i < len(missing) and missing[i]:
            continue  # 缺失通道跳过 / skip missing channels
        if ch not in orig:
            mismatches.append("%s: present in round but not in CSV" % ch)
            continue
        got = x[i] if i < len(x) else None
        exp = orig[ch]
        if got is None or abs(got - exp) > tol + tol * abs(exp):
            mismatches.append("%s: round=%s csv=%s" % (ch, got, exp))
    if mismatches:
        return "MISMATCH", "(%s, %s) " % (device, ts) + "; ".join(mismatches)
    return "OK", "(%s, %s)" % (device, ts)


def main(argv=None):
    p = argparse.ArgumentParser(description="V-M1-3 pivot spot check")
    p.add_argument("--rounds-jsonl", required=True, help="每行一个 DeviceRound JSON / one DeviceRound JSON per line")
    p.add_argument("--data-dir", required=True, help="原始数据集 CSV 目录 / original dataset CSV dir")
    p.add_argument("--n", type=int, default=50, help="抽样轮数 / number of rounds to sample")
    p.add_argument("--seed", type=int, default=12345)
    p.add_argument("--tol", type=float, default=1e-6)
    args = p.parse_args(argv)

    if not os.path.isdir(args.data_dir):
        print("ERROR: data-dir not found: %s" % args.data_dir, file=sys.stderr)
        return 2
    try:
        with open(args.rounds_jsonl, encoding="utf-8") as fh:
            rounds = [json.loads(line) for line in fh if line.strip()]
    except (OSError, ValueError) as exc:
        print("ERROR: cannot read rounds jsonl: %s" % exc, file=sys.stderr)
        return 2
    if not rounds:
        print("ERROR: no rounds in %s" % args.rounds_jsonl, file=sys.stderr)
        return 2

    rng = random.Random(args.seed)
    sample = rng.sample(rounds, min(args.n, len(rounds)))
    print("[index] scanning dataset filenames under %s ..." % args.data_dir)
    idx = build_file_index(args.data_dir)
    print("[index] %d data files indexed; sampling %d of %d rounds"
          % (len(idx), len(sample), len(rounds)))

    ok = mism = unmatched = 0
    for rnd in sample:
        status, detail = compare_round(rnd, idx, args.tol)
        if status == "OK":
            ok += 1
        elif status == "MISMATCH":
            mism += 1
            print("  [MISMATCH] " + detail)
        else:
            unmatched += 1
            print("  [UNMATCHED] " + detail)

    print("=" * 50)
    print("V-M1-3 pivot check: OK=%d  MISMATCH=%d  UNMATCHED=%d  (of %d sampled)"
          % (ok, mism, unmatched, len(sample)))
    print("=" * 50)
    return 0 if mism == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
