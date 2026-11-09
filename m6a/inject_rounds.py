#!/usr/bin/env python3
# ============================================================================
# m6a/inject_rounds.py
# 给一台设备的轮表施加注入并重算守卫标志（M6b 交接文档第四节，链式离线实验的输入）：全量运行在数据接入之前由注入器（Injector）改写
# 原始读数，守卫（RepeatRunGuard）在注入之后计算，所以 E 的轮表要先注入再算守卫。注入与守卫都用离线提取已有的移植
# （eda/m5a_extract.py 的 apply_injections 与 guard_flags，聚合模块收口基线的输入就是用它们生成的），读数以最短往返表示写出，
# Java 与 Python 读到的双精度数逐位相同。
# Applies the injection spec to one device's round table and recomputes the guard flags.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+，pandas、numpy。
# 2. 调用命令 / Invocation:
#      python3 m6a/inject_rounds.py --rounds docs/m6a/extract/rounds_E.csv.gz --device E --out docs/m6b/extract/rounds_E_inj.csv.gz
#    可选：--inject docs/m3_a4/full_fix/inject-truth.csv、--guard docs/stuck_guard/thresholds.env
# 3. 前置条件 / Preconditions: 轮表已由 eda/m6a_extract.py 生成。
# 4. 期望产出 / Expected output: out 处的轮表（格式与输入相同）；终端打印注入改写的读数数、注入前后守卫打标的轮数，以及在原始读数
#    上重算的守卫与轮表自带的守卫是否一致（自检）。
# 5. 失败兜底 / Failure fallback: 自检不一致时退出码 1（说明守卫移植或阈值文件与提取时不同），不写输出。
# ============================================================================
import argparse
import gzip
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "eda"))
from m5a_extract import CHANNELS, apply_injections, guard_flags, read_guard, read_injections   # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", required=True)
    ap.add_argument("--device", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--inject", default="docs/m3_a4/full_fix/inject-truth.csv")
    ap.add_argument("--guard", default="docs/stuck_guard/thresholds.env")
    a = ap.parse_args()
    df = pd.read_csv(a.rounds, dtype={c: str for c in CHANNELS})
    ts = df["ts"].to_numpy(dtype="int64")
    raw = np.array([[float(v) if isinstance(v, str) and v != "" else np.nan for v in df[c]] for c in CHANNELS]).T
    thr, floors = read_guard(a.guard)
    t, f = thr.get(a.device, [0] * 5), floors.get(a.device, [set() for _ in CHANNELS])
    before = guard_flags(ts, raw, t, f)
    if not np.array_equal(before.astype(int), df["stuck"].to_numpy(dtype=int)):
        print("ERROR: 在原始读数上重算的守卫与轮表自带的不一致", file=sys.stderr)
        return 1
    specs = [s for s in read_injections(a.inject) if s["device"] == a.device]
    inj = apply_injections(ts, raw, specs)
    changed = int(np.sum((inj != raw) & ~np.isnan(raw)))
    after = guard_flags(ts, inj, t, f)
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    lines = ["ts," + ",".join(CHANNELS) + ",stuck,cold\n"]
    cold = df["cold"].to_numpy(dtype=int)
    for i in range(len(ts)):
        vals = ["" if v != v else repr(float(v)) for v in inj[i]]
        lines.append("%d,%s,%d,%d\n" % (ts[i], ",".join(vals), int(after[i]), cold[i]))
    with gzip.GzipFile(a.out, "wb", mtime=0) as fh:
        fh.write("".join(lines).encode("utf-8"))
    print("%s：注入 %d 段，改写读数 %d 个；守卫打标 注入前 %d 轮、注入后 %d 轮；已写出 %s"
          % (a.device, len(specs), changed, int(before.sum()), int(after.sum()), a.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
