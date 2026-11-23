#!/usr/bin/env python3
# 第三批交接文档第二节：一级切换复算表与运行前写定的判据 / Batch 3 §2: switch recompute table and the pre-written criterion.
#
# 执行环境：开发机，仓库根目录，Python 3.8+（只用标准库）。
# 调用命令：python3 m6c/recompute_table.py [--dir docs/m6c/recompute] [--limit 3.0]
# 前置条件：M6cSwitchRecompute 已对 rerun_a 与 replay1 各跑两遍，生成 {label}_pass1.csv 与 {label}_pass2.csv。
# 期望产出：在 --dir 下写 recompute_table.md（两遍逐位核对、二十三行表、判据计数与结论），并在终端打印同样内容。
# 失败兜底：缺文件时退出码 2；两遍不逐位相同时表照常生成，但第一行写明不相同、退出码 1。
import argparse
import csv
import math
import os
import sys

LABELS = ("rerun_a", "replay1")
RUN_NAME = {"rerun_a": "第二批重跑运行 a", "replay1": "第一次全量重放运行 a"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="docs/m6c/recompute")
    ap.add_argument("--limit", type=float, default=3.0)
    a = ap.parse_args()
    out, rows, same = [], [], True
    for lb in LABELS:
        p1, p2 = (os.path.join(a.dir, "%s_pass%d.csv" % (lb, k)) for k in (1, 2))
        if not (os.path.exists(p1) and os.path.exists(p2)):
            print("缺少 %s 或 %s" % (p1, p2), file=sys.stderr)
            sys.exit(2)
        b1, b2 = open(p1, "rb").read(), open(p2, "rb").read()
        eq = b1 == b2
        same &= eq
        n = len(list(csv.DictReader(open(p1, encoding="utf-8"))))
        out.append("- %s：两遍复算%s，共 %d 行。" % (RUN_NAME[lb], "逐位相同" if eq else "**不逐位相同**", n))
        rows += list(csv.DictReader(open(p1, encoding="utf-8")))
    out.append("")
    hdr = ("| 运行 | 设备 | 命令 | 切换（世界协调时） | 在线比值 | 离线比值 | 相对差 | 新模型窗口数 | 新模型超阈率 | 新模型中位数 | 新模型 P95 "
           "| 旧模型窗口数 | 旧模型超阈率 | 旧模型中位数 | 旧模型 P95 | 超阈率倍数 | 七天内新一级或二级 |")
    out += [hdr, "|" + "---|" * 17]
    rej, others = [], []
    import datetime
    for r in rows:
        on = float(r["online_ratio"]) if r["online_ratio"] else None
        off = float(r["offline_ratio"]) if r["offline_ratio"] else None
        rel = abs(off - on) / on if on and off is not None else None
        na, oa = float(r["new_above_rate"]), float(r["old_above_rate"])
        zero = oa == 0.0
        mult = (math.inf if na > 0 else float("nan")) if zero else na / oa
        r["_on"], r["_mult"], r["_zero"] = on, mult, zero
        (rej if on is not None and on > a.limit else others).append(r)
        sw = datetime.datetime.utcfromtimestamp(int(r["switch_at"])).strftime("%m-%d %H:%M")
        ms = ("分母为零（新模型大于零）" if na > 0 else "两者都为零") if zero else "%.2f" % mult
        out.append("| %s | %s | %s | %s | %s | %s | %s | %s | %.4f | %.3f | %.3f | %s | %.4f | %.3f | %.3f | %s | %s |" % (
            RUN_NAME[r["run"]], r["device"], r["seq"], sw, "%.4f" % on if on else "无", "%.4f" % off if off is not None else "无",
            "%.3f%%" % (100 * rel) if rel is not None else "无", r["new_windows"], na, float(r["new_median"]), float(r["new_p95"]),
            r["old_windows"], oa, float(r["old_median"]), float(r["old_p95"]), ms, "是" if r["new_level12_within_7d"] == "1" else "否"))
    out.append("")
    c1 = [r for r in rej if r["_mult"] > 2]
    c1z = [r for r in c1 if r["_zero"]]
    c2 = [r for r in c1 if r["new_level12_within_7d"] == "1"]
    out.append("**判据计数。**在线比值超过 %.1f 的「会拒绝」共 %d 例（%s）。其中新模型切换后七天的超阈率高于旧模型两倍以上的有 %d 例"
               "（%s），其中靠「分母为零」约定成立的 %d 例；这些例中七天内触发了新的一级或二级的有 %d 例（%s）。" % (
                   a.limit, len(rej), "、".join("%s %s %.2f" % (r["device"], RUN_NAME[r["run"]], r["_on"]) for r in rej),
                   len(c1), "、".join(r["device"] for r in c1) or "无", len(c1z), len(c2), "、".join(r["device"] for r in c2) or "无"))
    ok = len(c1) >= 4 and len(c2) >= 3
    if ok:
        lo = max(r["_on"] for r in others) if others else 0.0
        hi = min(r["_on"] for r in rej)
        n = math.floor(lo) + 1 if lo == math.floor(lo) else math.ceil(lo)
        n = max(n, 1)
        fits = n < hi
        out.append("**结论。**判据满足：验证默认开启。其余 %d 例的在线比值最大为 %.4f，「会拒绝」五例最小为 %.4f；使五例被拒而其余全部通过的"
                   "最小整数倍数为 %d%s。" % (len(others), lo, hi, n, "" if fits else "，但它不小于五例的最小比值，不存在这样的整数"))
    else:
        out.append("**结论。**判据不满足（需要至少四例超阈率高于两倍，且其中至少三例七天内有新的一级或二级）：验证保持关闭，比值只记录。")
    if not same:
        out.insert(0, "**两遍复算不逐位相同，下表取第一遍。**\n")
    text = "\n".join(out) + "\n"
    open(os.path.join(a.dir, "recompute_table.md"), "w", encoding="utf-8").write(text)
    print(text)
    sys.exit(0 if same else 1)


if __name__ == "__main__":
    main()
