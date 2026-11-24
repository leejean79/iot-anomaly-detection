#!/usr/bin/env python3
# ============================================================================
# m6c/cross_level.py
# 第三批第三节「跨级冷却」的事后统计（只统计、不实现）：从第二批全量重放中数出每台在一级切换完成后三天内收到的零级命令，以及零级
# 生效后三天内提交的一级命令，列表并注明各自的触发与命中的格。按交接文档：两类合计少于五次则跨级冷却不做，否则第四批再议。
# After-the-fact count of cross-level triggers (batch 3, section 3); counts only.
#
# 口径：「收到」与「提交」取命令的决定时刻；「三天内」为（起点，起点 + 3 × 86,400 秒］；一级切换完成取上下文模块日志中的
# 「model event switch at」时刻；时间按布里斯托当地时间显示（夏令时自 2022-03-27 01:00 UTC 起）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m6c/cross_level.py [--run-dir docs/m6b/full/a] [--out docs/m6c/chain/cross_level.md]
# 3. 前置条件 / Preconditions: run-dir 下有 adapt.jsonl（命令主题）与 tm_log.txt（上下文模块日志）。
# 4. 期望产出 / Expected output: --out 处的 Markdown 表格；终端打印两类的条数与合计。
# 5. 失败兜底 / Failure fallback: 缺少输入时退出码 2。
# ============================================================================
import argparse
import datetime
import json
import os
import re
import sys

DST = 1648342800


def loc(t):
    return datetime.datetime.utcfromtimestamp(t + (3600 if t >= DST else 0)).strftime("%m-%d %H:%M")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", default="docs/m6b/full/a")
    ap.add_argument("--out", default="docs/m6c/chain/cross_level.md")
    a = ap.parse_args()
    adapt, log = os.path.join(a.run_dir, "adapt.jsonl"), os.path.join(a.run_dir, "tm_log.txt")
    if not (os.path.exists(adapt) and os.path.exists(log)):
        print("ERROR: 缺少 adapt.jsonl 或 tm_log.txt", file=sys.stderr)
        return 2
    cmds = [json.loads(l) for l in open(adapt, encoding="utf-8") if '"type":"command"' in l]
    sw = [(m.group(1), int(m.group(2))) for m in
          re.finditer(r"Device (\S+) model event switch at (\d+)", open(log, encoding="utf-8").read())]
    A = [[dev, loc(t), c["seq"], loc(c["decidedAt"]), c["triggers"], c["cell"]]
         for dev, t in sw for c in cmds
         if c["device"] == dev and c["level"] == 0 and t < c["decidedAt"] <= t + 3 * 86400]
    B = [[c0["device"], c0["seq"], loc(c0["effectiveAt"]), c0["triggers"], c["seq"], loc(c["decidedAt"]), c["triggers"], c["cell"]]
         for c0 in cmds if c0["level"] == 0 for c in cmds
         if c["device"] == c0["device"] and c["level"] == 1 and c0["effectiveAt"] < c["decidedAt"] <= c0["effectiveAt"] + 3 * 86400]
    tab = lambda h, rows: "| " + " | ".join(h) + " |\n| " + " | ".join("---" for _ in h) + " |\n" + "".join(
        "| " + " | ".join(str(x) for x in r) + " |\n" for r in rows)
    out = ["# 跨级交叉触发的事后统计（m6c/cross_level.py 生成；材料 %s，一级切换 %d 次）\n\n" % (a.run_dir, len(sw)),
           "## 一级切换完成后三天内收到的零级命令：%d 条\n\n" % len(A),
           tab(["设备", "切换完成（当地）", "零级序号", "零级决定（当地）", "触发", "格"], A),
           "\n## 零级生效后三天内提交的一级命令：%d 条\n\n" % len(B),
           tab(["设备", "零级序号", "零级生效（当地）", "零级触发", "一级序号", "一级决定（当地）", "一级触发", "格"], B),
           "\n合计 %d 次。交接文档写定：少于五次则跨级冷却不做；否则第四批再议。\n" % (len(A) + len(B))]
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write("".join(out))
    print("切换后三天内的零级 %d 条；零级后三天内的一级 %d 条；合计 %d" % (len(A), len(B), len(A) + len(B)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
