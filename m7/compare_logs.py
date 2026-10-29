#!/usr/bin/env python3
# ============================================================================
# m7/compare_logs.py
# 验收项一（V-M7-1）与验收项三（V-M7-3）的日志比较：两个目录下的 alerts.csv、commands.csv、review.csv 逐行比较；数值列解析成双精度
# 后比较（Python 与 Java 的浮点写法不同，但值须逐位相同），其余列逐字比较。
# Compares alerts/commands/review logs of two runs row by row; numeric cells compared as exact doubles.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 任意有 python3 的机器，仓库根目录；只用标准库。
# 2. 调用命令 / Invocation: python3 m7/compare_logs.py <目录甲> <目录乙>
# 3. 前置条件 / Preconditions: 两个目录下都有三份日志（m7/decide.py 或 M7OfflineRunner 的输出）。
# 4. 期望产出 / Expected output: 每份日志的条数与「逐条相同」或第一处不同；全部相同时退出码 0。
# 5. 失败兜底 / Failure fallback: 退出码 1 表示不同，2 表示文件缺失。
# ============================================================================
import csv
import os
import sys

NUMERIC = {"point_rate", "ctx_max", "perf_magnitude"}


def same(k, a, b):
    if k in NUMERIC and a != "" and b != "":
        return float(a) == float(b)
    return a == b


def main() -> int:
    if len(sys.argv) != 3:
        print("用法：compare_logs.py <目录甲> <目录乙>", file=sys.stderr)
        return 2
    ok = True
    for name in ("alerts.csv", "commands.csv", "review.csv"):
        pa, pb = (os.path.join(d, name) for d in sys.argv[1:])
        if not (os.path.exists(pa) and os.path.exists(pb)):
            print("缺少 %s" % name, file=sys.stderr)
            return 2
        a, b = (list(csv.DictReader(open(x, encoding="utf-8"))) for x in (pa, pb))
        diff = None
        for i, (x, y) in enumerate(zip(a, b)):
            for k in x:
                if not same(k, x[k], y.get(k)):
                    diff = "第 %d 行的 %s 不同：%s / %s" % (i, k, x[k], y.get(k))
                    break
            if diff:
                break
        if diff is None and len(a) != len(b):
            diff = "前 %d 行相同，但条数不同（%d / %d）" % (min(len(a), len(b)), len(a), len(b))
        print("%s：%d / %d 条，%s" % (name, len(a), len(b), diff or "逐条相同"))
        ok = ok and diff is None
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
