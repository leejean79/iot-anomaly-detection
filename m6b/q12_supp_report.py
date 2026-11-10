#!/usr/bin/env python3
# ============================================================================
# m6b/q12_supp_report.py
# 开放问题第十二条补跑两档（1.39 版裁决第二项）的报告表格：档乙（湿度与气压用冻结参数，训练集取生效前 N 天）与档丙（在档乙之上，
# 训练集取生效后 N 个当地日）。本脚本只计数、列表并按下面写定的判定规则给出机械结论；成因与选定由设计会话裁决。
# Tables for the supplementary arms of ruling 1.39; counts, paired comparisons and the pre-registered judgements only.
#
# 判定规则（2026-11-10 在任何补跑重训结果出来之前写定并提交；数值门槛取自 1.39 版裁决第二项的预期）：
#   零、配对与系统差别：沿用 m6b/q12_report.py——同一格五个种子逐一配对，差值同号（不计相等）不少于四个记为有系统差别。
#   一、档乙的回落程度：档乙每一格（N × 启动方式）取五个种子平稳段口径一与口径二超阈率的中位数；两个口径都不超过 0.10 记为
#       「回落到初始模型的量级」，任一口径不低于 0.20 记为「仍明显高于初始模型」，其余记为「介于两者之间」。同时列出气压的逐通道
#       重构误差在口径二与标定段之比（五个种子的中位数），对照第一轮网格的约一百倍（N=14）与约九百倍（N=3）。
#   二、档丙与档乙：同 N、同启动方式，按种子配对比较口径三（04-17、04-23、04-24，两档共同且都在训练集之后的平稳日）超阈率，
#       档丙减档乙；有系统差别且为负记为「档丙更低」。口径一两档的日子不同（档乙 03-28 至 04-03；档丙为训练集之后七个当地日，
#       不含注入日 04-05），只作描述。
#   三、召回的区分度：每一格取五个种子 04-05 非注入窗口超阈比例的中位数，不超过 0.20 且注入窗口超阈比例的中位数高于它，记为
#       「召回有区分度」；否则记为「召回仍饱和」。另列检出的注入段数（尖峰/阶跃），对照初始模型的基线四段（尖峰一、阶跃三）。
#   四、热启动与冷启动：各档、各 N，按种子配对比较口径一、口径二、口径三、检出的注入段数、注入窗口超阈比例（热减冷）。
#       1.39 版裁决第三项：若热启动在平稳段与召回上都不系统地优于冷启动，一级取冷启动；本脚本只列出逐项判定。
#   五、N 的「不再改善」：各档内相邻两档 N 按种子配对（较大减较小），度量同第一轮（口径一、口径二、检出段数、注入窗口超阈比例）；
#       N 取最小的一档，使比它大的各档在这四个口径上都没有系统改善。档丙只有三与七两档。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m6b/q12_supp_report.py [--out docs/m6b/q12_supp_report_tables.md]
# 3. 前置条件 / Preconditions: m6b/q12_supp.sh 已跑完（docs/m6b/q12_supp_DONE 存在）；第一轮网格 docs/m6b/q12/ 的结果行在。
# 4. 期望产出 / Expected output: --out 处的 Markdown 表格；终端打印缺失的运行（若有）。
# 5. 失败兜底 / Failure fallback: 缺少结果行时照常出表并列出缺失项，退出码 1。
# ============================================================================
import argparse
import csv
import glob
import os
import statistics
import sys

SEEDS = ["42", "7", "1", "2", "3"]
STARTS = [("warm", "热启动"), ("cold", "冷启动")]
ARMS = [("B", "档乙", "docs/m6b/q12b", ["3", "7", "14"]), ("C", "档丙", "docs/m6b/q12c", ["3", "7"])]
CH = ["T", "H", "P", "G", "L"]
CH_ZH = {"T": "温度", "H": "湿度", "P": "气压", "G": "气体", "L": "光照"}


def rate(r, k):
    return int(r[k + "_above"]) / int(r[k + "_windows"]) if int(r[k + "_windows"]) else float("nan")


def load_rows(d):
    R = {}
    for p in glob.glob(os.path.join(d, "rows", "*.csv")):
        for r in csv.DictReader(open(p, encoding="utf-8")):
            for k in ("s1", "s2", "s3", "inj", "other"):
                r[k + "_rate"] = rate(r, k)
            r["inj_detected"] = int(r["inj_detected"])
            tag = os.path.basename(p)[:-4]
            r["diag"] = load_diag(os.path.join(d, "diag", tag + ".csv"))
            R[(r["seed"], r["phase"], r["n_days"], r["start"])] = r
    return R


def load_diag(p):
    if not os.path.exists(p):
        return {}
    return {r["span"]: {c: float(r["mse_" + c]) for c in CH} for r in csv.DictReader(open(p, encoding="utf-8"))}


def table(head, rows):
    out = ["| " + " | ".join(head) + " |", "| " + " | ".join("---" for _ in head) + " |"]
    out += ["| " + " | ".join(str(x) for x in r) + " |" for r in rows]
    return "\n".join(out) + "\n"


def f3(v):
    return "%.3f" % v


def med(vals, f="%.3f"):
    vals = [v for v in vals if v == v]
    if not vals:
        return "—"
    return (f + "（" + f + " 至 " + f + "）") % (statistics.median(vals), min(vals), max(vals))


def paired(R, ka, kb, key, better_high):
    diffs = [R[ka(s)][key] - R[kb(s)][key] for s in SEEDS if ka(s) in R and kb(s) in R]
    pos = sum(1 for x in diffs if x > 0)
    neg = sum(1 for x in diffs if x < 0)
    sd = "+" if pos >= 4 else "-" if neg >= 4 else ""
    if sd:
        verdict = "有系统差别（前者%s）" % ("更好" if (sd == "+") == better_high else "更差")
    else:
        verdict = "无系统差别"
    return diffs, verdict, sd


def ratio(r, span, c):
    d = r["diag"]
    if span not in d or "calibration" not in d or d["calibration"][c] == 0:
        return float("nan")
    return d[span][c] / d["calibration"][c]


def fmtd(x, key):
    return "%d" % x if key == "inj_detected" else "%.3f" % x


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="docs/m6b/q12_supp_report_tables.md")
    a = ap.parse_args()
    data = {arm: load_rows(d) for arm, _, d, _ in ARMS}
    missing = []
    for arm, name, d, ns in ARMS:
        R = data[arm]
        init_ns = ["7"] if arm == "B" else ns
        missing += ["%s 初始模型 种子 %s N=%s" % (name, s, n) for s in SEEDS for n in init_ns if (s, "init", n, "") not in R]
        missing += ["%s 种子 %s N=%s %s" % (name, s, n, st) for s in SEEDS for n in ns for st, _ in STARTS
                    if (s, "retrain", n, st) not in R]
    out = []
    w = out.append
    w("# 开放问题第十二条补跑两档的报告表格（m6b/q12_supp_report.py 生成）\n\n"
      "档乙：湿度与气压的标准化参数换成冻结参数（版本一），其余三个通道仍用版本三，训练集取生效前 N 天。"
      "档丙：在档乙之上，训练集取生效之后的 N 个当地日（03-28 起）。初始模型沿用第一轮网格，只做评价。"
      "口径一：训练集之后的七个当地日（档乙 03-28 至 04-03；档丙不含注入日 04-05）；口径二：v34 日表中 E 的平稳日 03-29、03-30、"
      "04-17、04-23、04-24 中不落在训练集内的；口径三：04-17、04-23、04-24（两档共同）。判定规则见脚本文件头。\n\n")

    for arm, name, d, ns in ARMS:
        R = data[arm]
        w("## %s：逐次运行\n\n" % name)
        rows = []
        for s in SEEDS:
            for n in (["7"] if arm == "B" else ns):
                r = R.get((s, "init", n, ""))
                if r:
                    rows.append([s, "初始模型", n if arm == "C" else "—", "—", "—", f3(r["s1_rate"]), f3(r["s2_rate"]),
                                 f3(r["s3_rate"]), r["inj_detected"], "%s/%s" % (r["spike_detected"], r["step_detected"]),
                                 f3(r["inj_rate"]), f3(r["other_rate"]), "%.1f" % ratio(r, "s2", "P")])
            for n in ns:
                for st, sn in STARTS:
                    r = R.get((s, "retrain", n, st))
                    if r:
                        rows.append([s, sn, n, r["epochs"], "%.0f" % float(r["train_seconds"]), f3(r["s1_rate"]), f3(r["s2_rate"]),
                                     f3(r["s3_rate"]), r["inj_detected"], "%s/%s" % (r["spike_detected"], r["step_detected"]),
                                     f3(r["inj_rate"]), f3(r["other_rate"]), "%.1f" % ratio(r, "s2", "P")])
        w(table(["种子", "模型", "N（天）", "轮数", "耗时（秒）", "口径一", "口径二", "口径三", "检出注入段", "尖峰/阶跃",
                 "注入窗口超阈", "04-05 其余窗口超阈", "气压误差 口径二/标定段"], rows))

        w("\n## %s：五个种子的汇总（中位数，括号内为最小至最大）与判定一、判定三\n\n" % name)
        rows = []
        cells = [("init", n, "") for n in (["7"] if arm == "B" else ns)] + [("retrain", n, st) for n in ns for st, _ in STARTS]
        for ph, n, st in cells:
            rs = [R[(s, ph, n, st)] for s in SEEDS if (s, ph, n, st) in R]
            if not rs:
                continue
            m1 = statistics.median(r["s1_rate"] for r in rs)
            m2 = statistics.median(r["s2_rate"] for r in rs)
            j1 = "—"
            if ph == "retrain":
                j1 = "回落到初始模型的量级" if m1 <= 0.10 and m2 <= 0.10 else \
                    "仍明显高于初始模型" if m1 >= 0.20 or m2 >= 0.20 else "介于两者之间"
            mo = statistics.median(r["other_rate"] for r in rs)
            mi = statistics.median(r["inj_rate"] for r in rs)
            j3 = "召回有区分度" if mo <= 0.20 and mi > mo else "召回仍饱和"
            label = "初始模型" if ph == "init" else dict(STARTS)[st]
            rows.append([n if not (ph == "init" and arm == "B") else "—", label, len(rs), med([r["s1_rate"] for r in rs]),
                         med([r["s2_rate"] for r in rs]), med([r["s3_rate"] for r in rs]),
                         med([float(r["inj_detected"]) for r in rs], "%.0f"), med([r["other_rate"] for r in rs]),
                         med([ratio(r, "s2", "P") for r in rs], "%.1f"), med([ratio(r, "s2", "T") for r in rs], "%.1f"),
                         j1 if arm == "B" else "—", j3])
        w(table(["N（天）", "模型", "种子数", "口径一", "口径二", "口径三", "检出注入段", "04-05 其余窗口超阈",
                 "气压误差 口径二/标定段", "温度误差 口径二/标定段", "判定一（仅档乙）", "判定三"], rows))

        w("\n## %s：逐通道重构误差（五个种子的中位数；标定段与口径二、口径三）\n\n" % name)
        rows = []
        for ph, n, st in cells:
            rs = [R[(s, ph, n, st)] for s in SEEDS if (s, ph, n, st) in R and R[(s, ph, n, st)]["diag"]]
            if not rs:
                continue
            label = "初始模型" if ph == "init" else dict(STARTS)[st]
            for span, sz in (("calibration", "标定段"), ("s2", "口径二"), ("s3", "口径三")):
                rows.append([n if not (ph == "init" and arm == "B") else "—", label, sz]
                            + ["%.4g" % statistics.median(r["diag"][span][c] for r in rs) for c in CH])
        w(table(["N（天）", "模型", "时段"] + [CH_ZH[c] for c in CH], rows))

        w("\n## %s：判定四，热启动与冷启动（逐种子配对，热减冷）\n\n" % name)
        rows = []
        for n in ns:
            for key, nm, hi in (("s1_rate", "口径一", False), ("s2_rate", "口径二", False), ("s3_rate", "口径三", False),
                                ("inj_detected", "检出注入段", True), ("inj_rate", "注入窗口超阈", True)):
                dd, v, _ = paired(R, lambda s: (s, "retrain", n, "warm"), lambda s: (s, "retrain", n, "cold"), key, hi)
                rows.append([n, nm, "、".join(fmtd(x, key) for x in dd), len(dd), v])
        w(table(["N（天）", "度量", "逐种子差值", "配对数", "判定"], rows))

        w("\n## %s：判定五，N 是否仍在改善（逐种子配对，较大的 N 减较小的 N）\n\n" % name)
        rows = []
        for st, sn in STARTS:
            for small, big in zip(ns, ns[1:]):
                for key, nm, hi in (("s1_rate", "口径一", False), ("s2_rate", "口径二", False),
                                    ("inj_detected", "检出注入段", True), ("inj_rate", "注入窗口超阈", True)):
                    dd, v, _ = paired(R, lambda s: (s, "retrain", big, st), lambda s: (s, "retrain", small, st), key, hi)
                    rows.append([sn, "%s 对 %s" % (big, small), nm, "、".join(fmtd(x, key) for x in dd), len(dd), v])
        w(table(["启动", "比较", "度量", "逐种子差值", "配对数", "判定"], rows))
        w("\n")

    w("## 判定二：档丙与档乙（同 N、同启动方式，逐种子配对，档丙减档乙）\n\n")
    rows = []
    for n in ("3", "7"):
        for st, sn in STARTS:
            ka = lambda s: (s, "retrain", n, st)
            for key, nm in (("s3_rate", "口径三（判定所用）"), ("s1_rate", "口径一（日子不同，只作描述）")):
                diffs = [data["C"][ka(s)][key] - data["B"][ka(s)][key] for s in SEEDS if ka(s) in data["C"] and ka(s) in data["B"]]
                neg = sum(1 for x in diffs if x < 0)
                pos = sum(1 for x in diffs if x > 0)
                v = "档丙更低" if neg >= 4 else "档丙更高" if pos >= 4 else "无系统差别"
                rows.append([n, sn, nm, "、".join(f3(x) for x in diffs), len(diffs), v if key == "s3_rate" else "—（%s）" % v])
    w(table(["N（天）", "启动", "度量", "逐种子差值", "配对数", "判定"], rows))

    if missing:
        w("\n缺失的运行：%s\n" % "；".join(missing))
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write("".join(out))
    if missing:
        print("缺失 %d 次运行：%s" % (len(missing), missing), file=sys.stderr)
        return 1
    print("已写出 %s" % a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
