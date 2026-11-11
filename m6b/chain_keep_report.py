#!/usr/bin/env python3
# ============================================================================
# m6b/chain_keep_report.py
# 链式离线实验补跑一档（1.41 版裁决第一节，开发决策第十二条）：乙档（重估七天）加「湿度与气压不随零级重估」的开关，作为新的闭环
# 基线。本脚本读 ChainDriver 的 D7（原乙档）与 D7K（乙档加开关）两档输出，按下面写定的三项预期给出机械判定，并列出两档命令的
# 逐条差别。只计数与判定，不下成因结论。
# Pre-registered checks of the chained run with Humidity and Pressure kept at the freeze parameters (ruling 1.41 §1).
#
# 预期与判定规则（2026-11-11 在 D7K 运行之前写定并提交；前三项的文字取自 1.41 版裁决第一节）：
#   一、命令总数不多于六十：D7K 的命令总数（commands.csv 行数，含未执行的）不超过 60 记为「相符」，否则「不符」。
#   二、每条命令生效后七天内湿度与气压的连带偏离日数明显低于二百八十四：口径与原报告判读口径三的「其中湿度与气压」完全相同
#       （m6b/chain_report.py：每条已执行命令，生效当地日起七个当地日内，湿度、气压中不属于该命令触发通道的那几个通道的日末偏离
#       日数之和）。裁决没有给出「明显」的数值，这里写定为：不超过 284 的一半（142）记为「明显低于」；介于 142 与 284 之间（不含两端）
#       记为「有所降低但未达明显」；不低于 284 记为「未降低」。另列每条命令的平均值，以及不论是否触发通道的湿度与气压偏离日数（描述）。
#   三、三月事件与 G 光照的命令时刻不变：
#       （1）三月事件：命令日在 2022-03-22 至 2022-03-27 的命令，（设备，命令日）的集合在两档中相同记为「不变」；
#       （2）G 光照：G 上触发通道含光照的命令，命令日的集合在两档中相同记为「不变」。
#       两者都不变记为「相符」。触发通道的变化另列（描述）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m6b/chain_keep_report.py [--chain-dir docs/m6b/chain] [--out docs/m6b/chain/keep_check.md]
# 3. 前置条件 / Preconditions: 已运行 ChainDriver 的 D7 与 D7K 两档（D7K：--variant D7 --keep-channels Humidity,Pressure
#    --out-dir docs/m6b/chain/D7K）。
# 4. 期望产出 / Expected output: --out 处的 Markdown；终端打印三项判定。
# 5. 失败兜底 / Failure fallback: 缺少某一档的输出时退出码 2。
# ============================================================================
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import chain_report as CR       # noqa: E402

HP = ("Humidity", "Pressure")
BASE = 284


def executed(run):
    return [c for c in run.commands if "%s_%s" % (c["device"], c["seq"]) in run.executed]


def hp_days(run):
    """返回（非触发的湿度与气压偏离日之和，不论触发的湿度与气压偏离日之和，执行的命令数）/ HP co-deviation days."""
    non = allhp = n = 0
    for c in executed(run):
        n += 1
        e = CR.lday(c["effective_at"])
        tr = [t for t in c["triggers"].split("|") if t != "perf"]
        for ch in HP:
            k = run.dev_days(c["device"], ch, e, 7)
            allhp += k
            if ch not in tr:
                non += k
    return non, allhp, n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain-dir", default="docs/m6b/chain")
    ap.add_argument("--out", default="docs/m6b/chain/keep_check.md")
    a = ap.parse_args()
    for v in ("D7", "D7K"):
        if not os.path.exists(os.path.join(a.chain_dir, v, "commands.csv")):
            print("ERROR: 缺少 %s/%s 的输出" % (a.chain_dir, v), file=sys.stderr)
            return 2
    b = CR.Run(os.path.join(a.chain_dir, "D7"))
    k = CR.Run(os.path.join(a.chain_dir, "D7K"))
    out = ["# 链式离线实验：乙档加「湿度与气压不随零级重估」开关的预期核对（m6b/chain_keep_report.py 生成）\n\n"]
    w = out.append

    # 一、命令总数
    n_b, n_k = len(b.commands), len(k.commands)
    j1 = "相符" if n_k <= 60 else "不符"
    # 二、湿度与气压的连带偏离
    non_b, all_b, e_b = hp_days(b)
    non_k, all_k, e_k = hp_days(k)
    j2 = "明显低于" if non_k <= BASE / 2 else ("有所降低但未达明显" if non_k < BASE else "未降低")
    # 三、命令时刻
    march = lambda run: sorted((c["device"], c["day"]) for c in run.commands if "2022-03-22" <= c["day"] <= "2022-03-27")
    glight = lambda run: sorted(c["day"] for c in run.commands if c["device"] == "G" and "Light" in c["triggers"].split("|"))
    m_same = march(b) == march(k)
    g_same = glight(b) == glight(k)
    j3 = "相符" if m_same and g_same else "不符"

    w("## 三项预期的判定\n\n")
    w(CR.table(["预期", "原乙档（D7）", "加开关（D7K）", "判定"], [
        ["一、命令总数不多于六十", n_b, n_k, j1],
        ["二、生效后七天内非触发的湿度与气压偏离日数明显低于 284", "%d（执行 %d 条，每条 %.2f）" % (non_b, e_b, non_b / e_b),
         "%d（执行 %d 条，每条 %.2f）" % (non_k, e_k, non_k / e_k if e_k else 0), j2],
        ["三（1）、三月事件命令的（设备，命令日）不变", "%d 条" % len(march(b)), "%d 条" % len(march(k)), "不变" if m_same else "有变化"],
        ["三（2）、G 光照命令的命令日不变", "、".join(d[5:] for d in glight(b)), "、".join(d[5:] for d in glight(k)),
         "不变" if g_same else "有变化"],
        ["三、合并", "", "", j3],
    ]))
    w("\n描述：不论是否触发通道，生效后七天内湿度与气压的偏离日数之和，原乙档 %d，加开关 %d。\n" % (all_b, all_k))

    # 逐条差别
    key = lambda c: (c["device"], c["day"])
    kb = {key(c): c for c in b.commands}
    kk = {key(c): c for c in k.commands}
    rows = []
    for x in sorted(set(kb) | set(kk)):
        cb, ck = kb.get(x), kk.get(x)
        if cb and ck and cb["triggers"] == ck["triggers"]:
            continue
        rows.append([x[0], x[1][5:], cb["triggers"] if cb else "—", ck["triggers"] if ck else "—"])
    w("\n## 两档命令的逐条差别（按设备与命令日对齐；触发通道相同的不列）\n\n")
    w(CR.table(["设备", "命令日", "原乙档触发", "加开关触发"], rows) if rows else "无差别。\n")
    only_hp = [c for c in k.commands if set(t for t in c["triggers"].split("|") if t != "perf") <= set(HP)
               and c["triggers"] != "perf"]
    w("\n加开关一档中触发通道只有湿度或气压的命令：%d 条%s。\n" % (
        len(only_hp), ("（" + "；".join("%s %s %s" % (c["device"], c["day"][5:], c["triggers"]) for c in only_hp) + "）") if only_hp else ""))
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write("".join(out))
    print("一、命令总数 %d（原乙档 %d）：%s" % (n_k, n_b, j1))
    print("二、非触发的湿度与气压偏离日 %d（原乙档 %d）：%s" % (non_k, non_b, j2))
    print("三、三月事件%s，G 光照%s：%s" % ("不变" if m_same else "有变化", "不变" if g_same else "有变化", j3))
    return 0


if __name__ == "__main__":
    sys.exit(main())
