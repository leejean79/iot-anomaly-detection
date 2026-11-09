#!/usr/bin/env python3
# ============================================================================
# m6b/q12_report.py
# 开放问题第十二条离线实验的报告表格（M6b 交接文档第五节）：读 m6b/q12_run.sh 的结果行，列出三项度量（重训耗时与轮数、重训后平稳段
# 超阈率、对四月五日六段注入的召回），按运行前写定的判据给出热冷启动与 N 的配对比较。本脚本只计数与列表，选定由设计会话宣布。
# Tables for the open-question-12 experiment; counts and paired comparisons only.
#
# 判据的操作写法（2026-11-09 在结果出来之前写定并提交；交接文档只写了「无系统差别」与「不再改善」）：
#   一、度量：平稳段超阈率取两个口径（口径一：重训后七个当地日 03-28 至 04-03；口径二：E 在 v34 日表中重训后的平稳日，不含注入日
#       04-05），越低越好；召回取两个口径（检出的注入段数，满分六；注入窗口中超阈的比例），越高越好。
#   二、系统差别：同一 N、同一度量，五个种子逐一配对（热启动减冷启动），差值同号（不计相等）的种子不少于四个，记为有系统差别，
#       方向为该符号；否则记为无系统差别。
#   三、N 的「不再改善」：相邻两档（三对七、七对十四）按同样的五种子配对，较大的 N 在某一度量上有系统改善，记为该度量仍在改善。
#       N 取最小的一档，使比它大的各档在后两项的四个口径上都没有系统改善。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m6b/q12_report.py [--dir docs/m6b/q12] [--out docs/m6b/q12/report_tables.md]
# 3. 前置条件 / Preconditions: m6b/q12_run.sh 已跑完（Q12_DIR/ALL_DONE 存在）。
# 4. 期望产出 / Expected output: --out 处的 Markdown 表格；终端打印缺失的运行（若有）。
# 5. 失败兜底 / Failure fallback: 缺少结果行时照常出表并在表下列出缺失项，退出码 1。
# ============================================================================
import argparse
import csv
import glob
import os
import statistics
import sys

SEEDS = ["42", "7", "1", "2", "3"]
NS = ["3", "7", "14"]
STARTS = [("warm", "热启动"), ("cold", "冷启动")]
# (列名, 中文名, 越大越好)
METRICS = [("s1_rate", "平稳段口径一超阈率", False), ("s2_rate", "平稳段口径二超阈率", False),
           ("inj_detected", "检出的注入段（满分六）", True), ("inj_rate", "注入窗口超阈比例", True)]


def load(d):
    rows = {}
    for p in glob.glob(os.path.join(d, "rows", "*.csv")):
        for r in csv.DictReader(open(p, encoding="utf-8")):
            r["s1_rate"] = int(r["s1_above"]) / int(r["s1_windows"])
            r["s2_rate"] = int(r["s2_above"]) / int(r["s2_windows"])
            r["inj_rate"] = int(r["inj_above"]) / int(r["inj_windows"])
            r["inj_detected"] = int(r["inj_detected"])
            r["other_rate"] = int(r["other_above"]) / int(r["other_windows"])
            rows[(r["seed"], r["phase"], r["n_days"], r["start"])] = r
    return rows


def table(head, rows):
    out = ["| " + " | ".join(head) + " |", "| " + " | ".join("---" for _ in head) + " |"]
    out += ["| " + " | ".join(str(x) for x in r) + " |" for r in rows]
    return "\n".join(out) + "\n"


def fmt(v, key):
    return "%d" % v if key == "inj_detected" else "%.3f" % v


def paired(rows, a, b, key, better_high):
    """a 减 b 的逐种子差值与是否有系统差别（同号不少于四个）/ per-seed differences and the systematic flag."""
    diffs = []
    for s in SEEDS:
        if a(s) in rows and b(s) in rows:
            diffs.append(rows[a(s)][key] - rows[b(s)][key])
    pos = sum(1 for x in diffs if x > 0)
    neg = sum(1 for x in diffs if x < 0)
    sysdir = "+" if pos >= 4 else "-" if neg >= 4 else ""
    if sysdir:
        good = (sysdir == "+") == better_high
        verdict = "有系统差别（前者%s）" % ("更好" if good else "更差")
    else:
        verdict = "无系统差别"
    return diffs, verdict, sysdir, len(diffs)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="docs/m6b/q12")
    ap.add_argument("--out", default="docs/m6b/q12/report_tables.md")
    a = ap.parse_args()
    R = load(a.dir)
    missing = [k for k in [(s, "init", "", "") for s in SEEDS]
               + [(s, "retrain", n, st) for s in SEEDS for n in NS for st, _ in STARTS] if k not in R]
    out = []
    w = out.append
    w("# 开放问题第十二条离线实验报告表格（m6b/q12_report.py 生成）\n\n"
      "用例：设备 E，一级在 03-28 当地零点生效；训练集为生效前 N 天的入口轮，按当前版本（链式实验乙档 E 的版本三）标准化，"
      "七比二比七划成训练、早停、标定三段；初始模型按全量运行的方式在版本一上训练。种子 %s。\n\n" % "、".join(SEEDS))

    # ---------------- 逐次运行
    w("## 一、逐次运行\n\n")
    rows = []
    for s in SEEDS:
        k = (s, "init", "", "")
        if k in R:
            r = R[k]
            rows.append([s, "初始模型（不重训）", "—", r["train_windows"], r["epochs"], "%.0f" % float(r["train_seconds"]),
                         fmt(r["s1_rate"], ""), fmt(r["s2_rate"], ""), r["inj_detected"], "%s/%s" % (r["spike_detected"], r["step_detected"]),
                         fmt(r["inj_rate"], ""), fmt(r["other_rate"], "")])
        for n in NS:
            for st, sn in STARTS:
                k = (s, "retrain", n, st)
                if k not in R:
                    continue
                r = R[k]
                rows.append([s, sn, n, r["train_windows"], r["epochs"], "%.0f" % float(r["train_seconds"]), fmt(r["s1_rate"], ""),
                             fmt(r["s2_rate"], ""), r["inj_detected"], "%s/%s" % (r["spike_detected"], r["step_detected"]),
                             fmt(r["inj_rate"], ""), fmt(r["other_rate"], "")])
    w(table(["种子", "模型", "N（天）", "训练窗", "轮数", "耗时（秒）", "平稳段口径一超阈率", "平稳段口径二超阈率", "检出注入段",
             "尖峰/阶跃检出", "注入窗口超阈比例", "04-05 其余窗口超阈比例"], rows))
    w("\n初始模型按版本一评分，重训模型按版本三评分（选项一：上下文模块按训练版本标准化）。\n")

    # ---------------- 汇总
    w("\n## 二、五个种子的汇总（中位数，括号内为最小至最大）\n\n")
    rows = []
    for n in NS:
        for st, sn in STARTS:
            rs = [R[(s, "retrain", n, st)] for s in SEEDS if (s, "retrain", n, st) in R]
            if not rs:
                continue

            def m(key, f="%.3f"):
                v = [float(r[key]) for r in rs]
                return (f + "（" + f + " 至 " + f + "）") % (statistics.median(v), min(v), max(v))
            rows.append([n, sn, len(rs), m("epochs", "%.0f"), m("train_seconds", "%.0f"), m("s1_rate"), m("s2_rate"),
                         m("inj_detected", "%.0f"), m("inj_rate")])
    rs = [R[(s, "init", "", "")] for s in SEEDS if (s, "init", "", "") in R]
    if rs:
        med = lambda key: "%.3f" % statistics.median(float(r[key]) for r in rs)
        rows.append(["—", "初始模型（不重训）", len(rs), "—", "—", med("s1_rate"), med("s2_rate"), med("inj_detected"), med("inj_rate")])
    w(table(["N（天）", "启动", "种子数", "轮数", "耗时（秒）", "平稳段口径一", "平稳段口径二", "检出注入段", "注入窗口超阈比例"], rows))

    # ---------------- 判据一：热冷启动
    w("\n## 三、判据一：热启动与冷启动（逐种子配对，热启动减冷启动）\n\n")
    rows = []
    warm_sys = False
    for n in NS:
        for key, name, hi in METRICS:
            d, v, sd, k = paired(R, lambda s: (s, "retrain", n, "warm"), lambda s: (s, "retrain", n, "cold"), key, hi)
            warm_sys = warm_sys or bool(sd)
            rows.append([n, name, "、".join(fmt(x, key) for x in d), k, v])
    w(table(["N（天）", "度量", "逐种子差值", "配对数", "判定"], rows))
    w("\n按判据：热启动与冷启动在后两项（四个口径）上%s。\n" % ("至少有一处有系统差别，见上表" if warm_sys else "都无系统差别"))

    # ---------------- 判据二：N
    w("\n## 四、判据二：N 是否仍在改善（逐种子配对，较大的 N 减较小的 N）\n\n")
    rows = []
    for st, sn in STARTS:
        for small, big in (("3", "7"), ("7", "14")):
            for key, name, hi in METRICS:
                d, v, sd, k = paired(R, lambda s: (s, "retrain", big, st), lambda s: (s, "retrain", small, st), key, hi)
                rows.append([sn, "%s 对 %s" % (big, small), name, "、".join(fmt(x, key) for x in d), k, v])
    w(table(["启动", "比较", "度量", "逐种子差值", "配对数", "判定"], rows))
    if missing:
        w("\n缺失的运行：%s\n" % "；".join("种子 %s %s %s %s" % k for k in missing))
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write("".join(out))
    if missing:
        print("缺失 %d 次运行：%s" % (len(missing), missing), file=sys.stderr)
        return 1
    print("已写出 %s" % a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
