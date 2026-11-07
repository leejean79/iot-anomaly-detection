#!/usr/bin/env python3
# ============================================================================
# m7/equality_configs.py
# 聚合模块验收项一（V-M7-1）的逐条相同核对：在 23 种配置上分别运行 Python 参考实现 m7/decide.py 与 Java 离线运行器
# com.leejean.m7.M7OfflineRunner，用 m7/compare_logs.py 比较告警、命令、复核三份日志。2026-11-07 按 1.31 版裁决第一节在修正后的
# 输入上重做一遍并留证据（此前这一核对没有留成脚本）。
# V-M7-1 equality over the 23 configurations, Python reference vs Java offline runner.
#
# 23 种配置：默认；不带「同一段偏离只发一次」；三个单层消融；共模比例二分之一与全体；K_site 二与五；p_ctx 六分之一与六分之二；
# 决策节拍（一小时、一天）× 等待时长（六小时、十二小时、一个日界）× 生效提前量（一小时、一天）的十二种组合（其中一种与默认相同）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+，JDK 11；先 mvn package。
# 2. 调用命令 / Invocation: python3 m7/equality_configs.py --cp target/iot-anomaly-detection-1.0-SNAPSHOT.jar
#    可选 --inputs docs/m7/inputs、--work <临时目录>、--out docs/m7/equality.md。
# 3. 前置条件 / Preconditions: docs/m7/inputs 在库。
# 4. 期望产出 / Expected output: 逐配置打印比较结果；out 处一张表；全部逐条相同时退出码 0，否则 1。
# 5. 失败兜底 / Failure fallback: 某一侧运行失败时该配置记为失败并写出命令；Java 找不到类时检查 --cp。
# ============================================================================
import argparse
import os
import subprocess
import sys
import tempfile

BASE = [("default", "默认配置", []), ("no_once", "不带同一段偏离只发一次", ["--no-once"]),
        ("only_data", "只用数据层", ["--layers", "data"]), ("only_perf", "只用性能层", ["--layers", "perf"]),
        ("only_detect", "只用检测通道", ["--layers", "detect"]),
        ("common_1_2", "共模比例二分之一", ["--common-ratio", "1/2"]), ("common_1_1", "共模比例全体", ["--common-ratio", "1/1"]),
        ("ksite_2", "K_site 二", ["--k-site", "2"]), ("ksite_5", "K_site 五", ["--k-site", "5"]),
        ("pctx_1", "p_ctx 六分之一", ["--p-ctx", repr(1 / 6)]), ("pctx_2", "p_ctx 六分之二", ["--p-ctx", repr(2 / 6)])]
TIME = []
for cmd, cn in ((3600, "决策节拍一小时"), (0, "决策节拍一天")):
    for wait, wn in ((21600, "等待六小时"), (43200, "等待十二小时"), (0, "等待一个日界")):
        for delta, dn in ((3600, "提前量一小时"), (0, "提前量一天")):
            d = 86400 if (cmd and not delta) else delta          # 与 m7/scan_time_params.py 相同 / as in the scan
            TIME.append(("t_%s_%s_%s" % (cmd, wait, d), "%s、%s、%s" % (cn, wn, dn),
                         ["--cmd-sec", str(cmd), "--wait-sec", str(wait), "--delta-sec", str(d)]))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cp", required=True)
    ap.add_argument("--inputs", default="docs/m7/inputs")
    ap.add_argument("--work", default=None)
    ap.add_argument("--out", default="docs/m7/equality.md")
    a = ap.parse_args()
    work = a.work or tempfile.mkdtemp(prefix="m7eq_")
    rows, bad = [], 0
    for key, name, args in BASE + TIME:
        py, jv = os.path.join(work, key, "py"), os.path.join(work, key, "java")
        r1 = subprocess.run([sys.executable, "m7/decide.py", "--inputs", a.inputs, "--out-dir", py] + args, capture_output=True, text=True)
        r2 = subprocess.run(["java", "-cp", a.cp, "com.leejean.m7.M7OfflineRunner", "--inputs", a.inputs, "--out-dir", jv] + args,
                            capture_output=True, text=True)
        if r1.returncode or r2.returncode:
            bad += 1
            rows.append((name, "失败", "运行失败：Python 退出码 %d，Java 退出码 %d" % (r1.returncode, r2.returncode)))
            continue
        c = subprocess.run([sys.executable, "m7/compare_logs.py", py, jv], capture_output=True, text=True)
        lines = [l for l in c.stdout.splitlines() if l.endswith("csv") or "：" in l]
        ok = c.returncode == 0 and all("逐条相同" in l for l in lines if l.split("：")[0].endswith(".csv"))
        bad += 0 if ok else 1
        rows.append((name, "逐条相同" if ok else "不同", "；".join(l.strip() for l in lines if ".csv" in l)))
        print("%-12s %s %s" % (key, "逐条相同" if ok else "不同", rows[-1][2]))
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write("# 聚合模块 Java 离线运行器与 Python 参考实现的逐条相同核对（m7/equality_configs.py 生成）\n\n输入：`%s`。\n\n"
                 "| 配置 | 结果 | 三份日志 |\n| --- | --- | --- |\n" % a.inputs)
        for r in rows:
            fh.write("| %s | %s | %s |\n" % r)
        fh.write("\n共 %d 种配置，%d 种不同或失败。\n" % (len(rows), bad))
    print("共 %d 种配置，%d 种不同或失败；表格写入 %s" % (len(rows), bad, a.out))
    return 0 if bad == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
