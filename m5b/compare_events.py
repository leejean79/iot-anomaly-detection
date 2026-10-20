#!/usr/bin/env python3
# ============================================================================
# m5b/compare_events.py
# 逐条比较两份漂移事件清单（DriftEvent.CSV_HEADER 格式）：文本列逐字比较，数值列按双精度逐位比较（解析后相等即相同）。
# 用于 Java 离线运行器与 Python 参考实现的等值核对，以及验收项 V-M5b-3（离线与在线等值）。
# Compare two drift-event lists row by row: text columns literally, numeric columns as exact doubles.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 任意有 python3 的机器，仓库根目录；只用标准库。
# 2. 调用命令 / Invocation: python3 m5b/compare_events.py <清单甲.csv> <清单乙.csv>
# 3. 前置条件 / Preconditions: 两份清单都带 DriftEvent.CSV_HEADER 表头。
# 4. 期望产出 / Expected output: 打印两边的事件数与第一处不同；完全相同时退出码 0。
# 5. 失败兜底 / Failure fallback: 退出码 1 表示不同；2 表示文件缺失。
# ============================================================================
import csv
import os
import sys

TEXT = {"device", "level", "test"}


def main() -> int:
    if len(sys.argv) != 3 or not all(os.path.exists(p) for p in sys.argv[1:]):
        print("用法：compare_events.py <甲.csv> <乙.csv>（两个文件都须存在）", file=sys.stderr)
        return 2
    a, b = (list(csv.DictReader(open(p, encoding="utf-8"))) for p in sys.argv[1:])
    print("事件数：%d / %d" % (len(a), len(b)))
    for i, (x, y) in enumerate(zip(a, b)):
        for k in x:
            same = x[k] == y[k] if k in TEXT else float(x[k]) == float(y[k])
            if not same:
                print("第 %d 条事件的 %s 不同：%s / %s" % (i, k, x[k], y[k]))
                return 1
    if len(a) != len(b):
        print("前 %d 条相同，但条数不同" % min(len(a), len(b)))
        return 1
    print("逐条相同。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
