#!/usr/bin/env python3
# ============================================================================
# m5b/check_tables.py
# 核对 Java 生成的最优窗口漂移检测器常量表（OptWinTables）：用 scipy 在每一个窗长上对整数切点逐个穷举式 4.1 的最大根，
# 与 Java 的切点逐一比较（须完全相同），并比较 F、t 临界值（相对误差不超过一百万分之一）；作者代码口径的表还可与作者
# 预先算好的切点文件逐窗长比较（裁决补充 0.93 版第一节的数值核对办法）。
# Check the Java OPTWIN tables: exhaustive scipy search of the highest root of Eq. 4.1 on the integer grid, exact
# cut equality and 1e-6 relative tolerance on the critical values; optionally compare author-code tables with the
# authors' precomputed cut files.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac 或开发机，仓库根目录；python3，已装 numpy 与 scipy。
# 2. 调用命令 / Invocation:
#      python3 m5b/check_tables.py --table <Java 生成的表.csv> [--authors-dir <作者仓库的 pre_computed_cuts 目录>]
#    Java 表的生成：java -cp target/classes:<commons-math3-3.5.jar> com.leejean.m5.OptWinTables 0.99 0.5 2016 out.csv [AUTHOR_CODE]
# 3. 前置条件 / Preconditions: 已编译 Java；作者文件只在核对 AUTHOR_CODE 口径时需要（git clone github.com/maurodlt/OPTWIN，
#    本报告使用的提交为 f43731b）。
# 4. 期望产出 / Expected output: 终端打印切点不同的窗长数、临界值的最大相对误差；全部一致时退出码 0。
# 5. 失败兜底 / Failure fallback: 退出码 1 表示有不一致（打印前十个窗长）；退出码 2 表示输入缺失。
# ============================================================================
import argparse
import csv
import math
import os
import sys

import numpy as np
from scipy.stats import f as F, t as T


def read_table(path):
    meta, rows = {}, []
    with open(path, encoding="utf-8") as fh:
        first = fh.readline()
        for kv in first.lstrip("# ").split():
            k, v = kv.split("=")
            meta[k] = v
        for r in csv.DictReader(fh):
            rows.append(r)
    return meta, rows


def scipy_table(conf, rho, wmax, mode):
    dp = conf ** 0.25
    out = {}
    for w in range(30, wmax + 1):
        c = np.arange(2, w - 1, dtype=float)
        nu = c / w
        f = F.ppf(dp, c - 1, w - c - 1)
        nh, nn = nu * w, (1 - nu) * w
        r = f if mode == "THESIS" else 1.0 / (f * f)
        a, b = 1.0 / nh, r / nn
        df = (a + b) ** 2 / (a * a / (nh - 1) + b * b / (nn - 1))
        g = rho - T.ppf(dp, df) * np.sqrt(1.0 / nh + f / nn)
        ok = np.nonzero(g >= 0)[0]
        found = len(ok) > 0
        cut = int(c[ok[-1]]) if found else (w // 2 if mode == "THESIS" else w // 2 + 1)
        fc = F.ppf(dp, cut - 1, w - cut - 1)
        nu1 = cut / w
        nh1, nn1 = nu1 * w, (1 - nu1) * w
        f1 = F.ppf(dp, nh1 - 1, nn1 - 1)
        r1 = f1 if mode == "THESIS" else 1.0 / (f1 * f1)
        a1, b1 = 1.0 / nh1, r1 / nn1
        tc = T.ppf(dp, (a1 + b1) ** 2 / (a1 * a1 / (nh1 - 1) + b1 * b1 / (nn1 - 1)))
        out[w] = (cut, found, float(fc), float(tc))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    ap.add_argument("--authors-dir", default="")
    a = ap.parse_args()
    if not os.path.exists(a.table):
        print("ERROR: 缺少 %s" % a.table, file=sys.stderr)
        return 2
    meta, rows = read_table(a.table)
    conf, rho, wmax, mode = float(meta["confidence"]), float(meta["robustness"]), int(meta["wmax"]), meta["mode"]
    ref = scipy_table(conf, rho, wmax, mode)
    bad_cut, max_rel_f, max_rel_t = [], 0.0, 0.0
    for r in rows:
        w = int(r["w"])
        cut, found, fc, tc = ref[w]
        if int(r["cut"]) != cut or bool(int(r["root_found"])) != found:
            bad_cut.append((w, int(r["cut"]), cut))
            continue
        max_rel_f = max(max_rel_f, abs(float(r["f_crit"]) - fc) / abs(fc))
        max_rel_t = max(max_rel_t, abs(float(r["t_crit"]) - tc) / abs(tc))
    first = next((int(r["w"]) for r in rows if int(r["root_found"])), None)
    print("%s：置信度 %s，鲁棒度 %s，窗上限 %d，口径 %s；首个有根窗长 %s" % (os.path.basename(a.table), conf, rho, wmax, mode, first))
    print("  与 scipy 逐个穷举相比：切点不同 %d 个窗长；F 临界值最大相对误差 %.2e，t 临界值最大相对误差 %.2e"
          % (len(bad_cut), max_rel_f, max_rel_t))
    ok = not bad_cut and max_rel_f <= 1e-6 and max_rel_t <= 1e-6
    if bad_cut:
        print("  前十个不同（窗长, Java, scipy）：%s" % bad_cut[:10])
    if a.authors_dir and mode == "AUTHOR_CODE":   # 作者文件只对作者代码口径有意义 / author files only match that mode
        name = "cut_30-10000_%s_%sr.csv" % (round(1 - conf, 6), rho)
        path = os.path.join(a.authors_dir, name)
        if not os.path.exists(path):
            print("  作者目录里没有 %s，跳过作者文件核对" % name)
        else:
            vals = [float(x) for x in list(csv.reader(open(path)))[0][4:]]
            diff = [(int(r["w"]), int(r["cut"]), int(vals[int(r["w"])])) for r in rows if int(vals[int(r["w"])]) != int(r["cut"])]
            print("  与作者文件 %s 相比：切点不同 %d 个窗长%s" % (name, len(diff), "；前十个（窗长, Java, 作者）：%s" % diff[:10] if diff else ""))
            ok = ok and not diff
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
