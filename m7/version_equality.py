#!/usr/bin/env python3
# ============================================================================
# m7/version_equality.py
# 聚合模块按版本过滤（M6b 交接文档第三节，验收项 V-M6b-1）的 Java 与 Python 逐条相同核对：开环输入没有版本变化，二十三种配置的
# 核对（m7/equality_configs.py）走不到版本过滤的分支，所以本脚本在 docs/m7/inputs 上合成几次换版本（hours.csv 与 data_days.csv
# 加 version 列，换版本时刻为当地零点），再用若干配置分别跑 Python 参考实现与 Java 离线运行器，比较三份日志。合成的换版本只为走到
# 分支，不代表任何真实命令。
# Java-vs-Python check of the M7 version filter on inputs with synthetic version switches.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+，JDK 11；先 mvn package。
# 2. 调用命令 / Invocation: python3 m7/version_equality.py --cp target/iot-anomaly-detection-1.0-SNAPSHOT.jar
#    可选：--inputs docs/m7/inputs、--work <临时目录>（默认系统临时目录）、--out docs/m7/version_equality.md
# 3. 前置条件 / Preconditions: docs/m7/inputs 已由 m7/prepare_inputs.py 生成；jar 已打包。
# 4. 期望产出 / Expected output: 终端逐个配置打印三份日志是否逐条相同及条数；表格写入 --out。
# 5. 失败兜底 / Failure fallback: 任何配置不同或运行失败时退出码 1；缺输入时退出码 2。
# ============================================================================
import argparse
import csv
import filecmp
import os
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import decide as D  # noqa: E402

# 合成的换版本（当地日零点起为下一个版本）/ synthetic switches: a new version from local midnight of each day
SWITCHES = {"B": ["2022-03-16"], "G": ["2022-03-12", "2022-03-25"], "E": ["2022-03-29", "2022-04-06"], "C": ["2022-04-10"]}
CONFIGS = [[], ["--no-once"], ["--cmd-sec", "3600", "--wait-sec", "21600", "--delta-sec", "3600"], ["--layers", "data"],
           ["--common-ratio", "1/2", "--k-site", "2"]]
LOGS = ("alerts", "commands", "review")


def version(dev, day):
    return 1 + sum(1 for d in SWITCHES.get(dev, []) if day >= d)


def make_inputs(src, dst):
    shutil.rmtree(dst, ignore_errors=True)
    shutil.copytree(src, dst)
    for name, day_of in (("hours.csv", lambda r: D.local_of(int(r["hour_start"]))[0]), ("data_days.csv", lambda r: r["day"])):
        with open(os.path.join(src, name), encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        with open(os.path.join(dst, name), "w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, list(rows[0].keys()) + ["version"], lineterminator="\n")
            w.writeheader()
            for r in rows:
                r["version"] = version(r["device"], day_of(r))
                w.writerow(r)


def count(path):
    with open(path, encoding="utf-8") as fh:
        return sum(1 for _ in fh) - 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cp", required=True)
    ap.add_argument("--inputs", default="docs/m7/inputs")
    ap.add_argument("--work", default=None)
    ap.add_argument("--out", default="docs/m7/version_equality.md")
    a = ap.parse_args()
    if not os.path.exists(os.path.join(a.inputs, "hours.csv")):
        print("ERROR: %s 下缺少输入；先运行 m7/prepare_inputs.py" % a.inputs, file=sys.stderr)
        return 2
    work = a.work or tempfile.mkdtemp(prefix="m7ver_")
    inp = os.path.join(work, "inputs")
    make_inputs(a.inputs, inp)
    java = os.path.join(os.environ.get("JAVA_HOME", ""), "bin", "java") if os.environ.get("JAVA_HOME") else "java"
    rows, bad = [], 0
    for cfg in CONFIGS:
        tag = "_".join(x.strip("-").replace("/", "") for x in cfg) or "default"
        py, jv = os.path.join(work, "py_" + tag), os.path.join(work, "java_" + tag)
        r1 = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "decide.py"), "--inputs", inp, "--out-dir", py]
                            + cfg, capture_output=True)
        r2 = subprocess.run([java, "-cp", a.cp, "com.leejean.m7.M7OfflineRunner", "--inputs", inp, "--out-dir", jv] + cfg,
                            capture_output=True)
        if r1.returncode or r2.returncode:
            cells = ["运行失败"] * 3
            bad += 1
        else:
            cells = []
            for log in LOGS:
                same = filecmp.cmp(os.path.join(py, log + ".csv"), os.path.join(jv, log + ".csv"), shallow=False)
                bad += 0 if same else 1
                cells.append("%d 条，%s" % (count(os.path.join(py, log + ".csv")), "逐条相同" if same else "不同"))
        rows.append((" ".join(cfg) or "默认", cells))
        print("%-50s %s" % (" ".join(cfg) or "默认", "；".join(cells)))
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write("# 聚合模块按版本过滤的 Java 与 Python 核对（m7/version_equality.py 生成）\n\n")
        fh.write("输入为 docs/m7/inputs 加合成的换版本：%s（当地日零点起为下一个版本）。\n\n" % "；".join(
            "%s 于 %s" % (k, "、".join(v)) for k, v in sorted(SWITCHES.items())))
        fh.write("| 配置 | 告警 | 命令 | 复核 |\n| --- | --- | --- | --- |\n")
        for cfg, cells in rows:
            fh.write("| %s | %s |\n" % (cfg, " | ".join(cells)))
    print("共 %d 种配置，%d 项不同或失败；表格写入 %s" % (len(CONFIGS), bad, a.out))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
