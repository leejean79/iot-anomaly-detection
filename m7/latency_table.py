#!/usr/bin/env python3
# ============================================================================
# m7/latency_table.py
# 验收项四（V-M7-4）的时延表：三月事件、G 光照、气体抬高三段，逐台列出从上游第一条信号到第一条告警、到第一条零级命令生效的时延。
# 取全量离线结果（docs/m7/inputs 与 docs/m7/run，按 1.25 版的结束规则）；在线节拍在小时终点与日界之后六十秒触发（1.23 版裁决第三点），
# 所以告警与命令在线发出的时刻比事件时间各晚六十秒，表里的「告警发出」已含这六十秒；命令的生效时刻是事件时间，不受这六十秒影响。
# Latency table for V-M7-4 from the full offline run; online ticks fire one slide (60 s) after the hour end and day boundary.
#
# 三段的取法（报告中写明）：
#   三月事件：03-22 至 03-27 的当地日；上游信号取全部五类（点通道或上下文通道标志所在小时的终点、性能层候选或确认、离群率爆发的
#             发出时刻、数据层进入偏离的发出时刻即该日日界）；告警取信号之后第一条非质量问题的告警；命令取命令日在该段内的第一条。
#   G 光照：信号取 G 光照第一次进入偏离的发出时刻；告警取之后第一条数据层通道含光照的 G 告警；命令取第一条带光照触发的 G 命令。
#   气体抬高：命令日 03-14 至 03-21；逐台信号取该段第一次气体进入偏离的发出时刻；告警取之后第一条数据层通道含气体的告警；命令取
#             该段内第一条带气体触发的命令。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m7/latency_table.py [--inputs docs/m7/inputs] [--run docs/m7/run] [--out docs/m7/report/latency.md]
# 3. 前置条件 / Preconditions: docs/m7/inputs 与 docs/m7/run 在库（m7/decide.py 的默认运行）。
# 4. 期望产出 / Expected output: out 处的 Markdown 时延表；终端打印行数。
# 5. 失败兜底 / Failure fallback: 缺输入时退出码 2。
# ============================================================================
import argparse
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import decide as D  # noqa: E402

SLIDE = 60


def rows(path):
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def loc(ts):
    if ts is None:
        return "—"
    day, hod = D.local_of(ts)
    return "%s %02d:%02d" % (day[5:], hod, (ts % 3600) // 60)


def hours(a, b):
    return "—" if a is None or b is None else "%.1f" % ((b - a) / 3600)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", default="docs/m7/inputs")
    ap.add_argument("--run", default="docs/m7/run")
    ap.add_argument("--out", default="docs/m7/report/latency.md")
    a = ap.parse_args()
    for f in (os.path.join(a.inputs, "hours.csv"), os.path.join(a.run, "alerts.csv")):
        if not os.path.exists(f):
            print("ERROR: 缺少 %s" % f, file=sys.stderr)
            return 2
    inp = D.Inputs(a.inputs)
    p_point = D.derive_p_point(inp, D.Params())
    alerts = rows(os.path.join(a.run, "alerts.csv"))
    commands = rows(os.path.join(a.run, "commands.csv"))
    devs = inp.devices

    def deviate_emissions(dev, ch, d0, d1):
        """[d0, d1] 内该通道进入偏离的发出时刻（日界）/ emission times of DEVIATE in [d0, d1]."""
        return sorted(D.local_day_start(D.next_day(dd)) for (dv, dd, c), r in inp.days.items()
                      if dv == dev and c == ch and r["transition"] == "DEVIATE" and d0 <= dd <= d1)

    out = ["# 时延表（验收项四，m7/latency_table.py 生成）\n",
           "取全量离线结果（1.25 版结束规则）。时刻均为当地时间；「告警发出」为在线节拍的触发时刻（小时终点加六十秒）；"
           "命令的「决定发出」为日界加六十秒，「生效」为命令的生效事件时间（Δ 为一天）。时延单位为小时，从上游第一条信号算起。\n"]

    # ---------------- 三月事件
    s0, s1 = D.local_day_start("2022-03-22"), D.local_day_start("2022-03-28")
    out.append("\n## 三月事件（03-22 至 03-27）\n\n| 设备 | 第一条信号 | 信号类别 | 第一条告警发出 | 归因 | 到告警 | 第一条命令决定发出 | 生效 | 到生效 |\n"
               "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
    for dev in devs:
        sig = []
        for (dv, hs), r in inp.hours.items():
            if dv != dev or not (s0 <= hs < s1):
                continue
            adm = int(r["admitted"])
            if adm >= 180 and int(r["outlier_rounds"]) / adm > p_point:
                sig.append((hs + 3600, "点通道"))
            if int(r["ctx_above"]) / 6.0 >= 0.5:
                sig.append((hs + 3600, "上下文通道"))
        for t, lvl, *_ in inp.perf_events.get(dev, []):
            if s0 <= t < s1 and lvl in ("CANDIDATE", "CONFIRM"):
                sig.append((t, "性能层" + ("候选" if lvl == "CANDIDATE" else "确认")))
        for b, e in inp.bursts.get(dev, []):
            if s0 <= e < s1 + 86400:
                sig.append((e, "离群率爆发"))
        for ch in D.ACTIVE:
            for t in deviate_emissions(dev, ch, "2022-03-22", "2022-03-27"):
                sig.append((t, "数据层 %s 进入偏离" % ch))
        if not sig:
            out.append("| %s | 无 | | | | | | | |\n" % dev)
            continue
        t0, kind = min(sig)
        al = [x for x in alerts if x["device"] == dev and x["label"] != D.QUALITY
              and int(x["hour_start"]) + 3600 >= t0 and int(x["hour_start"]) < s1 + 86400]
        ta = int(al[0]["hour_start"]) + 3600 + SLIDE if al else None
        cm = [c for c in commands if c["device"] == dev and "2022-03-22" <= c["day"] <= "2022-03-27"]
        td = int(cm[0]["decided_at"]) + SLIDE if cm else None
        te = int(cm[0]["effective_at"]) if cm else None
        out.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s |\n" % (
            dev, loc(t0), kind, loc(ta), al[0]["label"] if al else "—", hours(t0, ta), loc(td), loc(te), hours(t0, te)))

    # ---------------- G 光照
    out.append("\n## G 的光照\n\n| 设备 | 第一条信号（光照进入偏离的发出） | 第一条告警发出 | 归因 | 到告警 | 第一条命令决定发出 | 生效 | 到生效 |\n"
               "| --- | --- | --- | --- | --- | --- | --- | --- |\n")
    em = deviate_emissions("G", "Light", "2022-03-08", "2022-04-30")
    if em:
        t0 = em[0]
        al = [x for x in alerts if x["device"] == "G" and "Light" in x["data_channels"].split("|")
              and int(x["hour_start"]) + 3600 >= t0]
        ta = int(al[0]["hour_start"]) + 3600 + SLIDE if al else None
        cm = [c for c in commands if c["device"] == "G" and "Light" in c["triggers"].split("|")]
        td = int(cm[0]["decided_at"]) + SLIDE if cm else None
        te = int(cm[0]["effective_at"]) if cm else None
        out.append("| G | %s | %s | %s | %s | %s | %s | %s |\n" % (
            loc(t0), loc(ta), al[0]["label"] if al else "—", hours(t0, ta), loc(td), loc(te), hours(t0, te)))

    # ---------------- 气体抬高
    out.append("\n## 气体自三月十四日起的抬高（命令日 03-14 至 03-21）\n\n| 设备 | 第一条信号（气体进入偏离的发出） | 第一条告警发出 | 归因 | 到告警 | "
               "第一条命令决定发出 | 生效 | 到生效 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
    for dev in devs:
        cm = [c for c in commands if c["device"] == dev and "2022-03-14" <= c["day"] <= "2022-03-21"
              and "Gas" in c["triggers"].split("|")]
        em = deviate_emissions(dev, "Gas", "2022-03-12", "2022-03-21")
        if not em:
            out.append("| %s | 无 | | | | | | |\n" % dev)
            continue
        t0 = em[0]
        al = [x for x in alerts if x["device"] == dev and "Gas" in x["data_channels"].split("|")
              and int(x["hour_start"]) + 3600 >= t0]
        ta = int(al[0]["hour_start"]) + 3600 + SLIDE if al else None
        td = int(cm[0]["decided_at"]) + SLIDE if cm else None
        te = int(cm[0]["effective_at"]) if cm else None
        out.append("| %s | %s | %s | %s | %s | %s | %s | %s |\n" % (
            dev, loc(t0), loc(ta), al[0]["label"] if al else "—", hours(t0, ta), loc(td), loc(te), hours(t0, te)))
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write("".join(out))
    print("已写出 %s（%d 行）" % (a.out, sum(s.count("\n") for s in out)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
