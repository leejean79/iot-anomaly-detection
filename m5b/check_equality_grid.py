#!/usr/bin/env python3
# ============================================================================
# m5b/check_equality_grid.py
# 网格全部组合上 Java 离线运行器与 Python 参考实现的等值核对（交接文档第三节「等值测试」）：对每种（置信度，鲁棒度，粒度）由 Java
# 生成常量表，再对每个 K 分别用 Java 与 Python 算事件清单，逐条比较。只报告是否相同，不做任何判读。
# Java/Python equality over the whole grid; reports identity only, no evaluation.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机，仓库根目录；python3、JDK（11 或以上）；已执行 mvn compile。
# 2. 调用命令 / Invocation: python3 m5b/check_equality_grid.py [--with-injection] [--work-dir /tmp/m5b_eq]
#    --with-injection 用不剔除注入的元素流（判读口径第七条的输入）。
# 3. 前置条件 / Preconditions: target/classes 已编译；~/.m2 中有 commons-math3-3.5.jar；docs/m5b 下有元素流。
# 4. 期望产出 / Expected output: 每种组合一行「相同」或「不同」；常量表写到 docs/m5b/tables/；全部相同时退出码 0。
# 5. 失败兜底 / Failure fallback: 退出码 1 表示有组合不同（打印 compare_events.py 的第一处不同）；2 表示缺 jar 或类。
# ============================================================================
import argparse
import itertools
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import optwin_offline as ow   # noqa: E402

JAR = os.path.expanduser("~/.m2/repository/org/apache/commons/commons-math3/3.5/commons-math3-3.5.jar")
GRAN = {"10min": ("streams_10min", "window_end", "main_score", 2016),
        "hourly": ("streams_hourly", "hour_start", "median_main_score", 336)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--with-injection", action="store_true")
    ap.add_argument("--work-dir", default="/tmp/m5b_eq")
    a = ap.parse_args()
    if not (os.path.exists(JAR) and os.path.exists("target/classes/com/leejean/m5/OptWinOfflineRunner.class")):
        print("ERROR: 缺少 %s 或未编译 target/classes" % JAR, file=sys.stderr)
        return 2
    java = os.path.join(os.environ.get("JAVA_HOME", ""), "bin", "java") if os.environ.get("JAVA_HOME") else "java"
    cp = "target/classes:" + JAR
    os.makedirs(a.work_dir, exist_ok=True)
    suffix = "_with_injection" if a.with_injection else ""
    bad = 0
    for conf, rho, gran in itertools.product((0.99, 0.999), (0.1, 0.3, 0.5), ("10min", "hourly")):
        base, tcol, vcol, wmax = GRAN[gran]
        streams = "docs/m5b/%s%s.csv" % (base, suffix)
        table = "docs/m5b/tables/thesis_%s_%s_%d.csv" % (conf, rho, wmax)
        rows = ow.read_stream(streams, tcol, vcol)
        for k in (18, 36, 72):
            jout = os.path.join(a.work_dir, "java_%s_%s_%s_%d%s.csv" % (conf, rho, gran, k, suffix))
            pout = os.path.join(a.work_dir, "py_%s_%s_%s_%d%s.csv" % (conf, rho, gran, k, suffix))
            subprocess.run([java, "-cp", cp, "com.leejean.m5.OptWinOfflineRunner", "--streams", streams,
                            "--time-col", tcol, "--value-col", vcol, "--confidence", str(conf), "--robustness", str(rho),
                            "--wmax", str(wmax), "--k", str(k), "--table-out", table, "--out", jout],
                           check=True, capture_output=True)
            ow.write_events(ow.run_stream(rows, ow.load_table(table), k), pout)
            r = subprocess.run([sys.executable, "m5b/compare_events.py", jout, pout], capture_output=True, text=True)
            same = r.returncode == 0
            bad += not same
            print("置信度 %-5s 鲁棒度 %s 粒度 %-6s K=%-2d：%s" % (conf, rho, gran, k, "相同" if same else "不同\n" + r.stdout))
    print("全部组合相同。" if not bad else "有 %d 种组合不同。" % bad)
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
