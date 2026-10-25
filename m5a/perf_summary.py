#!/usr/bin/env python3
# ============================================================================
# m5a/perf_summary.py
# 验收项 V-M5a-5（性能）的汇总：从打开数据层那次运行的 TaskManager 日志里取日界处理的耗时与关闭后的状态大小（`[M5a] Device …
# day … closed in … us: … values, state … bytes`），从两次运行的检查点体积采样（syn-m3-watch.sh 的 ckpt_sizes.csv）里取数据层算子
# 的检查点体积与整次检查点的合计，写一份 Markdown 表。
# V-M5a-5 summary: day-close cost and state size from the TaskManager log, checkpoint sizes from both runs.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 任意有 python3 的机器（集群 master 上由 syn-m5a-verify.sh 调用）；只用标准库。
# 2. 调用命令 / Invocation: python3 m5a/perf_summary.py --on docs/m5a/verify/on --off docs/m5a/verify/off --out perf.md
# 3. 前置条件 / Preconditions: on/m5a_tm_log.txt、on/ckpt_sizes.csv、off/ckpt_sizes.csv（缺哪个，表里注明缺失）。
# 4. 期望产出 / Expected output: perf.md（日界耗时的中位数与最大值、状态大小、检查点体积与增量）。
# 5. 失败兜底 / Failure fallback: 输入缺失时对应行写「缺失」，退出码仍为 0。
# ============================================================================
import argparse
import collections
import csv
import os
import re
import statistics

LINE = re.compile(r"\[M5a\] Device (\S+) day (\S+) closed in (\d+) us: (\d+) values, state (-?\d+) bytes")


def ckpt(path):
    """每次已完成检查点的合计与数据层算子的体积 / per completed checkpoint: total and the M5a operator."""
    if not os.path.exists(path):
        return None
    tot, m5a = collections.OrderedDict(), {}
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if r["ckpt_status"] != "COMPLETED":
                continue
            k = int(r["ckpt_id"])
            tot[k] = tot.get(k, 0) + int(r["state_size_bytes"])
            if "M5a" in r["operator"]:
                m5a[k] = int(r["state_size_bytes"])
    return tot, m5a


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--on", required=True)
    ap.add_argument("--off", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    rows = []
    log = os.path.join(a.on, "m5a_tm_log.txt")
    recs = []
    if os.path.exists(log):
        with open(log, encoding="utf-8", errors="replace") as fh:
            recs = [m.groups() for m in map(LINE.search, fh) if m]
    if recs:
        us = [int(r[2]) for r in recs]
        st = [int(r[4]) for r in recs]
        vals = [int(r[3]) for r in recs]
        rows.append(("日界处理耗时（一台设备关闭一个当地日，含五个通道的排序与检验）",
                     "%d 次；中位数 %.1f 毫秒，最大 %.1f 毫秒" % (len(us), statistics.median(us) / 1000, max(us) / 1000)))
        rows.append(("当日读数（五个通道合计）", "中位数 %d，最大 %d" % (statistics.median(vals), max(vals))))
        rows.append(("日界关闭后一台设备的状态（Java 序列化字节数；含参照直方图、检测器窗口与未发出的爆发）",
                     "中位数 %d 字节，最大 %d 字节" % (statistics.median(st), max(st))))
        peak = [b + 8 * v for b, v in zip(st, vals)]
        rows.append(("日界之前一台设备的状态峰值（估计：关闭后的状态加上当日读数乘八字节）",
                     "中位数 %d 字节，最大 %d 字节" % (statistics.median(peak), max(peak))))
    else:
        rows.append(("日界处理耗时与状态大小", "缺失（没有 %s 或其中没有 [M5a] 行）" % log))
    on, off = ckpt(os.path.join(a.on, "ckpt_sizes.csv")), ckpt(os.path.join(a.off, "ckpt_sizes.csv"))
    if on and on[1]:
        v = list(on[1].values())
        rows.append(("数据层算子的检查点体积（含链接的 Kafka 输出）", "最小 %d 字节，最大 %d 字节，最后一次 %d 字节" % (min(v), max(v), v[-1])))
    else:
        rows.append(("数据层算子的检查点体积", "缺失"))
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
