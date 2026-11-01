#!/usr/bin/env python3
# ============================================================================
# m7/perf_summary.py
# 验收项五（V-M7-5，性能）的汇总：从打开聚合算子那次运行的 TaskManager 日志里取每个当地日的节拍耗时与日界之后的状态大小
# （`[M7] Day … closed: boundary tick … us; … ticks total … us, max … us; … state … bytes`），从两次运行的检查点体积采样
# （syn-m3-watch.sh 的 ckpt_sizes.csv）里取聚合算子的检查点体积与整次检查点合计的差，写一份 Markdown 表。
# V-M7-5 summary: tick cost and state size from the TaskManager log, checkpoint sizes from both runs.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 任意有 python3 的机器（集群 master 上由 syn-m7-verify.sh 调用）；只用标准库。
# 2. 调用命令 / Invocation: python3 m7/perf_summary.py --on docs/m7/verify/on --off docs/m7/verify/off --out perf.md
# 3. 前置条件 / Preconditions: on/m7_tm_log.txt、on/ckpt_sizes.csv、off/ckpt_sizes.csv（缺哪个，表里注明缺失）。
# 4. 期望产出 / Expected output: perf.md（日界节拍与整点节拍的耗时、状态大小、检查点体积与增量）。
# 5. 失败兜底 / Failure fallback: 输入缺失时对应行写「缺失」，退出码仍为 0。
# ============================================================================
import argparse
import collections
import csv
import os
import re
import statistics

LINE = re.compile(r"\[M7\] Day (\S+) closed: boundary tick (\d+) us; (\d+) ticks total (\d+) us, max (\d+) us; "
                  r"alerts (\d+) commands (\d+) review (\d+); state (-?\d+) bytes")


def ckpt(path):
    """每次已完成检查点的合计与聚合算子的体积 / per completed checkpoint: total and the M7 operator."""
    if not os.path.exists(path):
        return None
    tot, m7 = collections.OrderedDict(), {}
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if r["ckpt_status"] != "COMPLETED":
                continue
            k = int(r["ckpt_id"])
            tot[k] = tot.get(k, 0) + int(r["state_size_bytes"])
            if "M7-Decision" in r["operator"]:
                m7[k] = int(r["state_size_bytes"])
    return tot, m7


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--on", required=True)
    ap.add_argument("--off", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    rows = []
    log = os.path.join(a.on, "m7_tm_log.txt")
    recs = []
    if os.path.exists(log):
        with open(log, encoding="utf-8", errors="replace") as fh:
            recs = [m.groups() for m in map(LINE.search, fh) if m]
    if recs:
        bt = [int(r[1]) for r in recs]
        ticks = [int(r[2]) for r in recs]
        tot = [int(r[3]) for r in recs]
        mx = [int(r[4]) for r in recs]
        st = [int(r[8]) for r in recs]
        rows.append(("日界节拍的耗时（告警节拍加决策节拍加复核）",
                     "%d 个日界；中位数 %.2f 毫秒，最大 %.2f 毫秒" % (len(bt), statistics.median(bt) / 1000, max(bt) / 1000)))
        mean = [t / n for t, n in zip(tot, ticks) if n]
        rows.append(("各节拍的平均耗时（按当地日）与单个节拍的最大耗时",
                     "平均的中位数 %.2f 毫秒；单个节拍最大 %.2f 毫秒" % (statistics.median(mean) / 1000, max(mx) / 1000)))
        rows.append(("日界之后的算子状态（Java 序列化字节数）",
                     "首日 %d 字节，末日 %d 字节，最大 %d 字节" % (st[0], st[-1], max(st))))
    else:
        rows.append(("节拍耗时与状态大小", "缺失（没有 %s 或其中没有 [M7] 行）" % log))
    on, off = ckpt(os.path.join(a.on, "ckpt_sizes.csv")), ckpt(os.path.join(a.off, "ckpt_sizes.csv"))
    if on and on[1]:
        v = list(on[1].values())
        rows.append(("聚合算子的检查点体积（含链接的 Kafka 输出）", "最小 %d 字节，最大 %d 字节，最后一次 %d 字节" % (min(v), max(v), v[-1])))
    else:
        rows.append(("聚合算子的检查点体积", "缺失"))
    if on and off and on[0] and off[0]:
        a1, b1 = list(on[0].values())[-1], list(off[0].values())[-1]
        rows.append(("收尾时整次检查点合计", "打开 %.3f MB，关闭 %.3f MB，相差 %.3f MB" % (a1 / 1e6, b1 / 1e6, (a1 - b1) / 1e6)))
        rows.append(("整次检查点合计的最大值", "打开 %.3f MB，关闭 %.3f MB" % (max(on[0].values()) / 1e6, max(off[0].values()) / 1e6)))
    else:
        rows.append(("检查点合计", "缺失"))
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write("| 项目 | 实测 |\n| --- | --- |\n")
        for k, v in rows:
            fh.write("| %s | %s |\n" % (k, v))
    print("已写出 %s（%d 行）" % (a.out, len(rows)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
