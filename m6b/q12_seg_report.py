#!/usr/bin/env python3
# ============================================================================
# m6b/q12_seg_report.py
# 1.40 版裁决补的逐段检出核对：读 m6b/q12_seg.sh 重跑的档丙二十次重训与五个初始模型的逐段检出（docs/m6b/q12seg/seg/），列出注入日
# 六段（三段尖峰、三段阶跃，幅度 4.56、9.12、18.24）每一段在每个模型下是否检出、重叠窗口的最大窗口误差与门槛之比；并核对重跑的
# 结果行与 docs/m6b/q12c/rows/ 逐字段相同（耗时除外），即训练逐位可复现、逐段结果属于原来那二十次重训。只计数与列表，不下结论。
# Per-segment detection tables for arm C and the initial models (ruling 1.40), plus a reproduction check of the rerun rows.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m6b/q12_seg_report.py [--out docs/m6b/q12seg/seg_report_tables.md]
# 3. 前置条件 / Preconditions: m6b/q12_seg.sh 已跑完（docs/m6b/q12seg/ALL_DONE 存在）；docs/m6b/q12c/rows/ 在库。
# 4. 期望产出 / Expected output: --out 处的 Markdown 表格；终端打印复现核对与缺失的运行。
# 5. 失败兜底 / Failure fallback: 有缺失的运行或复现不一致时照常出表并列出，退出码 1。
# ============================================================================
import argparse
import csv
import glob
import os
import statistics
import sys

SEEDS = ["42", "7", "1", "2", "3"]
DIR = "docs/m6b/q12seg"
ORIG = "docs/m6b/q12c/rows"
SKIP = {"train_seconds"}


def rows(path):
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def table(head, body):
    out = "| " + " | ".join(head) + " |\n| " + " | ".join("---" for _ in head) + " |\n"
    return out + "".join("| " + " | ".join(str(c) for c in r) + " |\n" for r in body)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(DIR, "seg_report_tables.md"))
    a = ap.parse_args()
    tags = ["init_%s_3" % s for s in SEEDS] + ["r_%s_%s_%s" % (s, n, st) for n in ("3", "7") for s in SEEDS for st in ("warm", "cold")]
    missing, mismatch = [], []
    seg = {}
    for t in tags:
        sp, rp, op = (os.path.join(DIR, "seg", t + ".csv"), os.path.join(DIR, "rows", t + ".csv"), os.path.join(ORIG, t + ".csv"))
        if not (os.path.exists(sp) and os.path.exists(rp)):
            missing.append(t)
            continue
        seg[t] = rows(sp)
        if os.path.exists(op):
            x, y = rows(rp)[0], rows(op)[0]
            diff = [k for k in x if k not in SKIP and x.get(k) != y.get(k)]
            if diff:
                mismatch.append("%s：%s" % (t, "、".join(diff)))
        else:
            mismatch.append("%s：原结果行不存在" % t)
    out = ["# 逐段检出核对（m6b/q12_seg_report.py 生成）\n\n",
           "档丙（湿度与气压用冻结参数、训练集取生效后 N 天）二十次重训与五个初始模型，在注入日 04-05 的六段注入上逐段检出。"
           "「比值」为重叠窗口中最大窗口误差除以门槛，不低于一即检出。\n\n",
           "复现核对：重跑的结果行与 `docs/m6b/q12c/rows/` 逐字段相同（耗时除外）的运行 %d 个，不一致 %d 个，缺失 %d 个。\n\n" % (
               len(seg) - len(mismatch), len(mismatch), len(missing))]
    if mismatch:
        out.append("不一致：" + "；".join(mismatch) + "\n\n")
    if missing:
        out.append("缺失：" + "、".join(missing) + "\n\n")
    if not seg:
        open(a.out, "w", encoding="utf-8").write("".join(out))
        print("没有逐段结果")
        return 1
    segs = seg[next(iter(seg))]
    names = ["%s %s %s" % (r["segment"], r["type"], r["magnitude"]) for r in segs]
    groups = [("初始模型", ["init_%s_3" % s for s in SEEDS])] + [
        ("N=%s %s" % (n, "热启动" if st == "warm" else "冷启动"), ["r_%s_%s_%s" % (s, n, st) for s in SEEDS])
        for n in ("3", "7") for st in ("warm", "cold")]
    # 汇总：每组每段检出的种子数与比值中位数 / detections per segment and median ratio
    body = []
    for g, ts in groups:
        cells = []
        for k in range(len(segs)):
            got = [seg[t][k] for t in ts if t in seg]
            det = sum(1 for r in got if int(r["above"]) > 0)
            ratio = statistics.median(float(r["max_wmse"]) / float(r["threshold"]) for r in got) if got else float("nan")
            cells.append("%d/%d（%.2f）" % (det, len(got), ratio))
        body.append([g] + cells)
    out.append("## 各组每段的检出（检出的种子数 / 种子数，括号内为比值的中位数）\n\n")
    out.append(table(["模型"] + ["第 " + n for n in names], body))
    # 逐次 / per run
    body = []
    for g, ts in groups:
        for t in ts:
            if t not in seg:
                continue
            body.append([g, t.split("_")[1]] + ["%s %.2f" % ("检出" if int(r["above"]) > 0 else "—", float(r["max_wmse"]) / float(r["threshold"]))
                                                for r in seg[t]] + ["%.4g" % float(seg[t][0]["threshold"])])
    out.append("\n## 逐次运行（每段：是否检出、比值）\n\n")
    out.append(table(["模型", "种子"] + ["第 " + n for n in names] + ["门槛"], body))
    open(a.out, "w", encoding="utf-8").write("".join(out))
    print("已写出 %s；复现一致 %d，不一致 %d，缺失 %d" % (a.out, len(seg) - len(mismatch), len(mismatch), len(missing)))
    return 1 if (missing or mismatch) else 0


if __name__ == "__main__":
    sys.exit(main())
