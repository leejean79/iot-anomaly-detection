#!/usr/bin/env python3
# ============================================================================
# m6a/sim_report.py
# 适应执行第一批离线模拟报告的表格（验收项二 V-M6a-2，交接文档第五节判读口径第一、二、三、四、六条）：读 M6aSimulator 的输出
# （docs/m6a/sim/{设备}_{off,on_D1,on_D3,on_D7,onx_D1}_*）与零级的 JSON，写 docs/m6a/sim/tables.md。只列事实，不判读。
# Tables for the M6a offline-simulation report, from the simulator outputs.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m6a/sim_report.py [--sim docs/m6a/sim] [--out docs/m6a/sim/tables.md]
# 3. 前置条件 / Preconditions: 两台设备的五次模拟与六份零级 JSON 已生成（见报告的复现方式）。
# 4. 期望产出 / Expected output: out 处的 Markdown 表格；终端打印行数。
# 5. 失败兜底 / Failure fallback: 缺输入文件时退出码 2。
# ============================================================================
import argparse
import csv
import json
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

Z = ZoneInfo("Europe/London")
CH = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]
CASES = [("G", "2022-03-25", "Light"), ("B", "2022-03-16", "Gas")]


def rows(path):
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def day_start(day):
    return int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=Z).timestamp())


def add_day(day, n):
    return (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=n)).strftime("%Y-%m-%d")


def rate(hours, t0, t1):
    a = sum(int(r["admitted"]) for r in hours if t0 <= int(r["hour_start"]) < t1)
    o = sum(int(r["outliers"]) for r in hours if t0 <= int(r["hour_start"]) < t1)
    return a, o, ("%.3f%%" % (100.0 * o / a) if a else "—")


def g(v):
    return "%.6g" % v


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sim", default="docs/m6a/sim")
    ap.add_argument("--out", default="docs/m6a/sim/tables.md")
    a = ap.parse_args()
    out = ["# 适应执行第一批离线模拟的表格（m6a/sim_report.py 生成）\n\n时刻均为当地时间（布里斯托）。"
           "「无命令」与「有命令」是同一份轮表的两次模拟，有命令一次在生效时刻执行零级。\n"]
    for dev, day, focus in CASES:
        p = lambda tag, kind: os.path.join(a.sim, "%s_%s_%s" % (dev, tag, kind))
        need = [p(t, k) for t in ("off", "on_D1", "on_D3", "on_D7", "onx_D1") for k in ("days.csv", "hours.csv", "events.csv")]
        need += [os.path.join(a.sim, "%s_D%d_py.json" % (dev, d)) for d in (1, 3, 7)]
        miss = [f for f in need if not os.path.exists(f)]
        if miss:
            print("ERROR: 缺少 %s" % miss[0], file=sys.stderr)
            return 2
        eff = day_start(day)
        js = {d: json.load(open(os.path.join(a.sim, "%s_D%d_py.json" % (dev, d)))) for d in (1, 3, 7)}
        lx = json.load(open(p("onx_D1", "level0.json")))
        out.append("\n## 设备 %s：生效时刻 %s 当地零点（重点通道 %s）\n" % (dev, day, focus))

        # ---- 判读口径第一条与第六条：参数
        out.append("\n### 判读口径第一条（与第六条）：重估前后的中位数与分母\n\n冻结参数即标定周（冻结之前 60,480 轮）的值；"
                   "光照在 log1p 域。重估窗口的轮数含缺失轮，剔除数为守卫打标的轮。\n\n"
                   "| 通道 | 冻结 中位数 | 冻结 分母 | D=1 中位数 | D=1 分母 | D=3 中位数 | D=3 分母 | D=7 中位数 | D=7 分母 |\n"
                   "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
        for c in range(5):
            f = js[1]["freeze"]
            cells = [g(f["median"][c]), g(f["scale"][c])]
            for d in (1, 3, 7):
                cells += [g(js[d]["new"]["median"][c]), g(js[d]["new"]["scale"][c])]
            out.append("| %s | %s |\n" % (CH[c], " | ".join(cells)))
        out.append("\n重估窗口：" + "；".join("D=%d 为 %d 轮、剔除 %d 轮、回填 %d 轮" % (
            d, js[d]["windowRounds"], js[d]["guardExcluded"], len(js[d]["backfill"]["ts"])) for d in (1, 3, 7)) + "。\n")

        # ---- 判读口径第二条：数据层状态
        out.append("\n### 判读口径第二条：命令之后数据层的状态\n\n「偏离日」是日记录里状态为偏离的当地日；事件列出生效当日及之后的进入偏离"
                   "与回落。\n\n| 通道 | 运行 | 生效日及之后的偏离日数 / 可判日数 | 生效日及之后的事件 |\n| --- | --- | --- | --- |\n")
        for c in CH:
            for tag, name in (("off", "无命令"), ("on_D1", "有命令 D=1"), ("on_D3", "有命令 D=3"), ("on_D7", "有命令 D=7")):
                ds = [r for r in rows(p(tag, "days.csv")) if r["channel"] == c and r["day"] >= day]
                dev_n = sum(1 for r in ds if r["deviated"] == "1")
                ev_n = sum(1 for r in ds if r["evaluable"] == "1")
                ev = ["%s %s" % (r["day"][5:], "进入偏离" if r["level"] == "DEVIATE" else "回落")
                      for r in rows(p(tag, "events.csv")) if r["channel"] == c and r["day"] >= day]
                out.append("| %s | %s | %d / %d | %s |\n" % (c, name, dev_n, ev_n, "、".join(ev) if ev else "无"))
        ds_on = {r["day"]: r for r in rows(p("on_D1", "days.csv")) if r["channel"] == focus}
        ds_off = {r["day"]: r for r in rows(p("off", "days.csv")) if r["channel"] == focus}
        out.append("\n%s 通道逐日（D=1）：\n\n| 当地日 | 无命令 D | 无命令 偏离 | 有命令 D | 有命令 p | 有命令 偏离 | 有命令 可判 |\n"
                   "| --- | --- | --- | --- | --- | --- | --- |\n" % focus)
        for dd in sorted(ds_on):
            if dd < add_day(day, -2):
                continue
            r, o = ds_on[dd], ds_off.get(dd, {})
            out.append("| %s | %s | %s | %s | %s | %s | %s |\n" % (
                dd, g(float(o.get("d", "nan"))), "是" if o.get("deviated") == "1" else "否", g(float(r["d"])), g(float(r["p"])),
                "是" if r["deviated"] == "1" else "否", "是" if r["evaluable"] == "1" else "否（%s）" % r["reason"]))

        # ---- 判读口径第三条与第四条：点通道离群率
        out.append("\n### 判读口径第三条与第四条：点通道离群率\n\n离群率为到达滑动步的离群轮数除以入口轮数（来自离群记录，不含回填点，"
                   "1.29 版裁决第四项）。「剔除离群轮」一列为判读口径第四条：重估时另剔除无命令运行中重估窗口内被点通道判为离群的轮"
                   "（%d 轮）。\n\n| 区间 | 无命令 | 有命令（只剔除守卫轮） | 有命令（另剔除离群轮） |\n| --- | --- | --- | --- |\n"
                   % lx["guardExcluded"])
        spans = [("生效前一日（%s）" % add_day(day, -1), day_start(add_day(day, -1)), eff),
                 ("生效后第一小时（状态重建的瞬态）", eff, eff + 3600),
                 ("生效当日（%s）" % day, eff, day_start(add_day(day, 1))),
                 ("生效后七天", eff, day_start(add_day(day, 7))),
                 ("生效至 04-30 结束", eff, day_start("2022-05-01"))]
        H = {t: rows(p(t, "hours.csv")) for t in ("off", "on_D1", "onx_D1")}
        for name, t0, t1 in spans:
            cells = []
            for t in ("off", "on_D1", "onx_D1"):
                ad, o, rt = rate(H[t], t0, t1)
                cells.append("%s（%d / %d）" % (rt, o, ad))
            out.append("| %s | %s |\n" % (name, " | ".join(cells)))
        out.append("\n判读口径第四条的参数差（另剔除离群轮减只剔除守卫轮，D=1）：\n\n| 通道 | 中位数差 | 分母差 |\n| --- | --- | --- |\n")
        for c in range(5):
            out.append("| %s | %s | %s |\n" % (CH[c], g(lx["median"][c] - js[1]["new"]["median"][c]),
                                              g(lx["scale"][c] - js[1]["new"]["scale"][c])))
    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write("".join(out))
    print("已写出 %s（%d 行）" % (a.out, sum(s.count("\n") for s in out)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
