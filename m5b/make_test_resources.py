#!/usr/bin/env python3
# ============================================================================
# m5b/make_test_resources.py
# 生成 Java 单元测试用的参照数据（M5b 交接文档第三节「测试」）：scipy 逐个穷举得到的常量表、带均值漂移与方差漂移的合成流、
# 平稳合成流，以及 Python 参考实现在这些合成流上算出的报警清单。
# Generates the reference data for the Java unit tests: scipy table, synthetic mean/variance-shift and stationary
# streams, and the Python reference's alarms on them.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机，仓库根目录；python3，已装 numpy 与 scipy。
# 2. 调用命令 / Invocation: python3 m5b/make_test_resources.py
# 3. 前置条件 / Preconditions: docs/m5b/tables/thesis_0.99_0.5_2016.csv 已由 Java 生成（OptWinOfflineRunner --table-out）。
# 4. 期望产出 / Expected output: src/test/resources/m5b/ 下的 scipy_thesis_0.99_0.5_2016.csv、synthetic_streams.csv、
#    synthetic_events.csv；终端打印各合成流的报警数。
# 5. 失败兜底 / Failure fallback: 缺少 Java 常量表时退出码 2。
# ============================================================================
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_tables                 # noqa: E402
import optwin_offline as ow         # noqa: E402

OUT = "src/test/resources/m5b"
TABLE = "docs/m5b/tables/thesis_0.99_0.5_2016.csv"


def main() -> int:
    if not os.path.exists(TABLE):
        print("ERROR: 缺少 %s" % TABLE, file=sys.stderr)
        return 2
    os.makedirs(OUT, exist_ok=True)
    ref = check_tables.scipy_table(0.99, 0.5, 2016, "THESIS")
    with open(os.path.join(OUT, "scipy_thesis_0.99_0.5_2016.csv"), "w") as fh:
        fh.write("w,cut,root_found,f_crit,t_crit\n")
        for w in sorted(ref):
            c, found, fc, tc = ref[w]
            fh.write("%d,%d,%d,%r,%r\n" % (w, c, int(found), fc, tc))
    rng = np.random.default_rng(20261020)
    streams = {
        "mean_shift": np.concatenate([rng.normal(0, 1, 600), rng.normal(1.0, 1, 600)]),
        "var_shift": np.concatenate([rng.normal(0, 1, 600), rng.normal(0.2, 3, 600)]),
        "stationary": rng.normal(0, 1, 20000),
    }
    rows = [(name, i, float(x)) for name, v in streams.items() for i, x in enumerate(v)]
    with open(os.path.join(OUT, "synthetic_streams.csv"), "w") as fh:
        fh.write("device,ts,value\n")
        for d, i, x in rows:
            fh.write("%s,%d,%r\n" % (d, i, x))
    events = ow.run_stream(rows, ow.load_table(TABLE), 36)
    ow.write_events(events, os.path.join(OUT, "synthetic_events.csv"))
    for name in streams:
        c = [e for e in events if e["device"] == name and e["level"] == "CANDIDATE"]
        print("%s：候选 %d 个，首个在第 %s 个元素（%s 检验）" % (name, len(c), c[0]["ts"] if c else "—", c[0]["test"] if c else "—"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
