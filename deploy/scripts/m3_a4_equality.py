#!/usr/bin/env python3
"""
m3_a4_equality.py —— 补遗四全量重放的验收项 A4-2（逐位等值）：把本次运行与 10 月 9 日注入运行逐项比较。
A4-2 bit-exact equality of the addendum-4 full replay against the injection run of 2026-10-09.

比较三类内容，任何一处不同都判为「不同」（缺陷，上报而不是解释）：
  1. 逐台训练与标定：训练轮数、早停集误差、标定中位数、四分位距、阈值（读两次运行的 m3_tm_log.txt）；
  2. 上下文通道评分：逐台逐窗的主分（到小数点后九位，并另报完全相同的窗数）、马氏分、加权均方误差、是否超阈；
  3. 点通道输出：逐条离群记录（设备、轮时间戳、窗口末）构成的多重集合。
Compares training/calibration per device, every context-channel window, and the multiset of point-channel outliers.

---------------------------- 脚本交付五要素 ----------------------------
1. 执行环境 / Environment: 本地 Mac 或 master，仓库根目录，python3（只用标准库）。通常由 syn-m3-a4-full.sh 在 master 上调用。
2. 调用命令 / Invocation:
     python3 deploy/scripts/m3_a4_equality.py --run docs/m3_a4/full --ref docs/m3_inject --out docs/m3_a4/full/equality.md
     可选 --ref-label <名称>：对照运行在表中的名称，默认「注入运行」。
3. 前置条件 / Preconditions: 两个目录里都有 syn-m3-march-collect.sh 收集的 m3_tm_log.txt，以及 scores.jsonl（全部输出）
   或 m3_scores.jsonl（只有上下文通道）。任何一边只有 m3_scores.jsonl 时（例如步骤 C 的运行），不比较点通道，并在表中注明。
4. 期望产出 / Expected output: --out 指定的 Markdown（逐台对照表与总判定），终端打印同样内容。
5. 失败兜底 / Failure fallback: 退出码 0 表示全部相同；1 表示有不同（表中标出）；2 表示输入文件缺失。
"""
import argparse
import collections
import json
import os
import re
import sys

RE_TRAINED = re.compile(r"\[M3\] Device (\S+) trained: .*?, (\d+) epochs .*early-stop loss=(\S+), ")
RE_CALIB = re.compile(r"\[M3\] Device (\S+) calibrated: median=(\S+), IQR=(\S+), threshold=(\S+)")


def train_lines(path):
    """每台设备最后一次训练与标定的数值（以字符串比较，即逐位比较）/ last training and calibration per device."""
    out = collections.defaultdict(dict)
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = RE_TRAINED.search(line)
            if m:
                out[m.group(1)].update(epochs=m.group(2), esloss=m.group(3).rstrip(","))
            m = RE_CALIB.search(line)
            if m:
                out[m.group(1)].update(median=m.group(2).rstrip(","), iqr=m.group(3).rstrip(","), thr=m.group(4))
    return out


def scores(path):
    ctx = collections.defaultdict(dict)                 # device -> windowEnd -> record
    point = collections.Counter()                       # (device, roundTs, windowEnd) -> count
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                o = json.loads(line)
            except ValueError:
                continue
            if o.get("channel") == "m3_context":
                ctx[o["device"]][o["windowEnd"]] = o
            elif o.get("channel") == "m2_point" and o.get("outlier"):
                point[(o["device"], o["roundTs"], o["windowEnd"])] += 1
    return ctx, point


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--ref", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--ref-label", default="注入运行")
    a = ap.parse_args()
    files = {}
    for d in (a.run, a.ref):
        files[d] = next((os.path.join(d, f) for f in ("scores.jsonl", "m3_scores.jsonl")
                         if os.path.exists(os.path.join(d, f))), None)
        if not files[d] or not os.path.exists(os.path.join(d, "m3_tm_log.txt")):
            print("ERROR: %s 缺少 m3_tm_log.txt，或 scores.jsonl 与 m3_scores.jsonl 都没有" % d, file=sys.stderr)
            return 2
    # 只有上下文通道评分时不比较点通道 / no point-channel comparison when either side has context scores only
    point = all(os.path.basename(files[d]) == "scores.jsonl" for d in (a.run, a.ref))
    tr, tf = train_lines(os.path.join(a.run, "m3_tm_log.txt")), train_lines(os.path.join(a.ref, "m3_tm_log.txt"))
    cr, pr = scores(files[a.run])
    cf, pf = scores(files[a.ref])
    ok = True
    L = ["# 逐位等值：本次运行（%s）与%s（%s）" % (a.run, a.ref_label, a.ref), "",
         "## 一、逐台训练与标定", "",
         "| 设备 | 训练轮数 | 早停集误差 | 标定中位数 | 标定四分位距 | 阈值 | 判定 |", "| --- | --- | --- | --- | --- | --- | --- |"]
    for d in sorted(set(tr) | set(tf)):
        cells, same = [], True
        for k in ("epochs", "esloss", "median", "iqr", "thr"):
            x, y = tr[d].get(k), tf[d].get(k)
            if x is not None and x == y:
                cells.append(x)
            else:
                cells.append("**%s / %s**" % (x, y))
                same = False
        ok &= same
        L.append("| %s | %s | %s |" % (d, " | ".join(cells), "相同" if same else "**不同**"))

    L += ["", "## 二、上下文通道评分（逐窗）", "",
          "| 设备 | 窗数（本次 / %s） | 主分九位小数相同 | 主分完全相同 | 马氏分、误差、超阈相同 | 判定 |" % a.ref_label,
          "| --- | --- | --- | --- | --- | --- |"]
    for d in sorted(set(cr) | set(cf)):
        x, y = cr.get(d, {}), cf.get(d, {})
        common = set(x) & set(y)
        r9 = sum(1 for w in common if round(x[w]["mainScore"], 9) == round(y[w]["mainScore"], 9))
        ex = sum(1 for w in common if x[w]["mainScore"] == y[w]["mainScore"])
        oth = sum(1 for w in common if all(x[w].get(k) == y[w].get(k) for k in ("mahaScore", "wmse", "aboveThreshold")))
        same = len(x) == len(y) == len(common) == r9 == oth
        ok &= same
        L.append("| %s | %d / %d | %d | %d | %d | %s |" % (d, len(x), len(y), r9, ex, oth, "相同" if same else "**不同**"))

    L += ["", "## 三、点通道离群记录", ""]
    only_r, only_f = (pr - pf, pf - pr) if point else (collections.Counter(), collections.Counter())
    if point:
        same = not only_r and not only_f
        ok &= same
        L.append("本次 %d 条、%s %d 条；只在本次出现 %d 条，只在%s出现 %d 条。判定：%s。" % (
            sum(pr.values()), a.ref_label, sum(pf.values()), sum(only_r.values()), a.ref_label,
            sum(only_f.values()), "相同" if same else "**不同**"))
    else:
        L.append("至少一边只有上下文通道评分（m3_scores.jsonl），不比较点通道。")
    for title, c in (("只在本次出现（前 10 条）", only_r), ("只在%s出现（前 10 条）" % a.ref_label, only_f)):
        if c:
            L += ["", title + "：", ""] + ["- %s %s %s ×%d" % (k[0], k[1], k[2], n) for k, n in list(c.items())[:10]]

    L += ["", "## 总判定", "", "**全部相同。**" if ok else "**存在不同，按逐位等值约定属于缺陷，须上报。**"]
    text = "\n".join(L) + "\n"
    with open(a.out, "w", encoding="utf-8") as f:
        f.write(text)
    print(text)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
