#!/usr/bin/env python3
# ============================================================================
# m7/scan_time_params.py
# 判读口径第 9 条（交接文档 1.25 版第七节）：时间参数的离线扫描，只作记录、不改默认。决策节拍取一小时与一个当地日两档，
# 等待时长 W 取六小时、十二小时、一个日界三档（前两档按 1.24 版第四点四只用性能层判持续），生效提前量 Δ 取一小时与一天两档；
# 告警节拍保持一小时。每种组合列出零级命令总数、带性能层触发的命令、落在四月五个共模气体时段上的命令，以及三月事件与 G 光照两段从进入
# 偏离到命令生效的延迟；另列性能层每一段「已确认未回落」的起止与时长（这是 W 起作用的唯一途径）。
# Offline scan of W x Delta for judgement item 9; record only.
#
# 共模时段的归属写法（报告中写明）：时段取数据事实第三十五条候选的当地时间；第四段「四月十五日傍晚」取到 04-16 03:00，覆盖该段
# 四台的性能层确认。一条命令算落在某时段上，当且仅当它带性能层触发且这一段确认从该时段内开始（被长瞬态骗到），或它带气体触发
# 且决定日（命令的 day）是该时段所在的当地日（数据层路径，另列，各组合相同与否照实写出）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m7/scan_time_params.py [--inputs docs/m7/inputs] [--out-dir docs/m7/scan_time]
# 3. 前置条件 / Preconditions: 已运行 m7/prepare_inputs.py；docs/m7/inputs 下七张表齐全。
# 4. 期望产出 / Expected output: out-dir 下 summary.csv（每种组合一行）、commands_<组合>.csv、perf_spells.csv、latency.csv、
#    scan.md（三张表的 Markdown 版）；终端打印 summary。
# 5. 失败兜底 / Failure fallback: 缺输入时退出码 2；时区库缺失（Python 3.8）时请改用 3.9 及以上。
# ============================================================================
import argparse
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import decide as D  # noqa: E402

SEGMENTS = [                                                # (名称, 当地起, 当地止, 当地日)
    ("时段一 04-03 00–09", "2022-04-03 00:00", "2022-04-03 09:00", ["2022-04-03"]),
    ("时段二 04-04 05–23", "2022-04-04 05:00", "2022-04-04 23:00", ["2022-04-04"]),
    ("时段三 04-10 00–08", "2022-04-10 00:00", "2022-04-10 08:00", ["2022-04-10"]),
    ("时段四 04-15 傍晚至 04-16 03", "2022-04-15 17:00", "2022-04-16 03:00", ["2022-04-15", "2022-04-16"]),
    ("时段五 04-22 07–14", "2022-04-22 07:00", "2022-04-22 14:00", ["2022-04-22"]),
]
CMDS = [(3600, "1h"), (0, "1d")]
WAITS = [(21600, "6h"), (43200, "12h"), (0, "1日界")]
DELTAS = [(3600, "1h"), (0, "1d")]


def local_ts(s):
    day, hm = s.split(" ")
    return D.local_day_start(day) + 3600 * int(hm[:2]) + 60 * int(hm[3:])


def fmt_local(ts):
    day, hod = D.local_of(ts)
    return "%s %02d:%02d" % (day[5:], hod, (ts % 3600) // 60)


def perf_spells(inp):
    """性能层每一段「已确认未回落」：(设备, 开始, 回落或 None) / confirmed spells per device."""
    out = []
    for dev in inp.devices:
        pf = D.PerfTracker(inp.perf_events.get(dev, []), inp.perf_elements.get(dev, []))
        start = None
        for t in sorted({x[0] for x in pf.items}):
            was = pf.confirmed
            pf.advance(t)
            if pf.confirmed and not was:
                start = pf.since
            if was and not pf.confirmed:
                out.append((dev, start, t))
        if pf.confirmed:
            out.append((dev, start, None))
    return out


def segment_of(ts):
    for name, a, b, _ in SEGMENTS:
        if local_ts(a) <= ts <= local_ts(b):
            return name
    return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", default="docs/m7/inputs")
    ap.add_argument("--out-dir", default="docs/m7/scan_time")
    a = ap.parse_args()
    if not os.path.exists(os.path.join(a.inputs, "hours.csv")):
        print("ERROR: %s 下缺少输入；先运行 m7/prepare_inputs.py" % a.inputs, file=sys.stderr)
        return 2
    os.makedirs(a.out_dir, exist_ok=True)
    inp = D.Inputs(a.inputs)
    p_point = D.derive_p_point(inp, D.Params())
    spells = perf_spells(inp)
    with open(os.path.join(a.out_dir, "perf_spells.csv"), "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["device", "since_local", "fallen_local", "hours", "segment"])
        for dev, s, e in spells:
            w.writerow([dev, fmt_local(s), fmt_local(e) if e else "", "%.1f" % ((e - s) / 3600) if e else "",
                        segment_of(s)])

    seg_days = {d: name for name, _, _, days in SEGMENTS for d in days}
    summary, latency = [], []
    for cmd, cn, wait, wn, delta, dn in [(c, cn, w, wn, d, dn) for c, cn in CMDS for w, wn in WAITS for d, dn in DELTAS]:
        if True:
            tag = "C%s_W%s_D%s" % (cn, wn, dn)
            # Δ 一天：决策节拍一天时取一个当地日（delta 0），决策节拍一小时时取 86,400 秒 / one day of lead time
            p = D.Params(p_point=p_point, cmd_sec=cmd, wait_sec=wait, delta_sec=86400 if (cmd and not delta) else delta)
            _, commands, _ = D.run(inp, p)
            D.write(os.path.join(a.out_dir, "commands_%s.csv" % tag), commands, D.COMMAND_COLS)
            perf_cmds, seg_perf, seg_gas = [], [], []
            for c in commands:
                trig = c["triggers"].split("|")
                if "perf" in trig:
                    since = max(s for dev, s, e in spells if dev == c["device"] and s <= c["decided_at"])
                    perf_cmds.append("%s %s" % (c["device"], c["day"][5:]))
                    if segment_of(since):
                        seg_perf.append("%s %s（%s）" % (c["device"], c["day"][5:], segment_of(since)[:3]))
                if "Gas" in trig and c["day"] in seg_days:
                    seg_gas.append("%s %s（%s）" % (c["device"], c["day"][5:], seg_days[c["day"]][:3]))
            summary.append(dict(config=tag, commands=len(commands), perf_commands=len(perf_cmds),
                                perf_list="；".join(perf_cmds), seg_perf=len(seg_perf), seg_perf_list="；".join(seg_perf),
                                seg_gas=len(seg_gas), seg_gas_list="；".join(seg_gas)))
            # 延迟：三月事件（命令日 03-22 至 03-27）与 G 光照（G 的第一条光照命令）；进入偏离取触发通道最近一次 DEVIATE 的当地日零点
            g_light = [c for c in commands if c["device"] == "G" and "Light" in c["triggers"].split("|")][:1]
            march = [c for c in commands if "2022-03-22" <= c["day"] <= "2022-03-27"]
            for seg, cs in (("三月事件", march), ("G 光照", g_light)):
                for c in cs:
                    chans = [t for t in c["triggers"].split("|") if t != "perf"]
                    if seg == "G 光照":
                        chans = ["Light"]
                    if chans:
                        entry = max(dd for (dev, dd, ch), r in inp.days.items()
                                    if dev == c["device"] and ch in chans and r["transition"] == "DEVIATE" and dd <= c["day"])
                        t0 = D.local_day_start(entry)
                    else:                                         # 只由性能层触发：取这一段确认的时刻 / perf only
                        t0 = max(s for dev, s, e in spells if dev == c["device"] and s <= c["decided_at"])
                        entry = fmt_local(t0)
                    latency.append(dict(config=tag, segment=seg, device=c["device"], triggers=c["triggers"], entry_day=entry,
                                        decided_local=fmt_local(c["decided_at"]), effective_local=fmt_local(c["effective_at"]),
                                        to_decision_h=(c["decided_at"] - t0) // 3600,
                                        to_effective_h=(c["effective_at"] - t0) // 3600))
    D.write(os.path.join(a.out_dir, "summary.csv"), summary, list(summary[0]))
    D.write(os.path.join(a.out_dir, "latency.csv"), latency, list(latency[0]))

    with open(os.path.join(a.out_dir, "scan.md"), "w", encoding="utf-8") as fh:
        fh.write("# 时间参数离线扫描（判读口径第 9 条，m7/scan_time_params.py 生成）\n\n")
        fh.write("告警节拍一小时；组合名 C 为决策节拍、W 为等待时长、D 为生效提前量（决策节拍一小时时，Δ 一天取 86,400 秒）；"
                 "p_point = %r。\n\n" % p_point)
        fh.write("| 组合 | 命令总数 | 带性能层触发的命令 | 性能层确认始于共模时段的命令 | 决定日在共模时段上的气体命令 |\n")
        fh.write("| --- | --- | --- | --- | --- |\n")
        for s in summary:
            fh.write("| %s | %d | %d %s | %d %s | %d %s |\n" % (
                s["config"], s["commands"], s["perf_commands"], s["perf_list"], s["seg_perf"], s["seg_perf_list"],
                s["seg_gas"], s["seg_gas_list"]))
        fh.write("\n| 组合 | 片段 | 设备 | 触发 | 进入偏离（当地日） | 决定（当地） | 生效（当地） | 到决定（小时） | 到生效（小时） |\n")
        fh.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
        for r in latency:
            fh.write("| %s | %s | %s | %s | %s | %s | %s | %d | %d |\n" % (
                r["config"], r["segment"], r["device"], r["triggers"], r["entry_day"], r["decided_local"],
                r["effective_local"], r["to_decision_h"], r["to_effective_h"]))
        fh.write("\n| 设备 | 确认（当地） | 回落（当地） | 时长（小时） | 共模时段 |\n| --- | --- | --- | --- | --- |\n")
        for dev, s, e in spells:
            fh.write("| %s | %s | %s | %s | %s |\n" % (dev, fmt_local(s), fmt_local(e) if e else "未回落",
                                                    "%.1f" % ((e - s) / 3600) if e else "", segment_of(s)))
    for s in summary:
        print(s["config"], s["commands"], "性能层", s["perf_commands"], "时段性能", s["seg_perf"], "时段气体", s["seg_gas"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
