#!/usr/bin/env python3
# ============================================================================
# m7/report_tables.py
# 聚合模块离线实验报告（验收项二）的表格：按交接文档 1.24 版第七节判读口径第 1 至 8 条，从 Python 参考实现的运行结果生成 Markdown 表；
# 第 9 条见 m7/scan_time_params.py。各配置（默认、不带「同一段偏离只发一次」、三个单层消融、共模比例二分之一与全体、K_site 二与五）
# 由本脚本调用 decide.run 现跑，日志写到 out-dir 下各子目录。本脚本只计数与列表，不下结论。
# Tables for the M7 offline report (judgement items 1-8), generated from decide.run; counts and lists only.
#
# 片段的取法（报告中写明）：三月事件取 03-22 至 03-27 的当地日；气体抬高取命令日 03-14 至 03-21；四月五个共模气体时段取数据事实
# 第三十五条候选的当地时间（第四段取 04-15 17:00 至 04-16 03:00）；C 的正午取日照直射日的当地 10 至 15 点；傍晚取当地 16 至 21 点、
# 同小时至少两台有点通道或上下文通道标志的设备小时；平稳日取 docs/m3_inject/v34/v34_daily.csv 的逐台平稳日（与数据层阶段相同，
# 日期按当地日使用）；注入取 docs/m3_a4/full_fix/inject-truth.csv，每段覆盖的当地小时。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m7/report_tables.py [--inputs docs/m7/inputs] [--out-dir docs/m7/report]
# 3. 前置条件 / Preconditions: 已运行 m7/prepare_inputs.py；docs/m3_inject/v34/v34_daily.csv 与 docs/m3_a4/full_fix/inject-truth.csv 在库。
# 4. 期望产出 / Expected output: out-dir 下 tables.md（第 1 至 8 条的表）与各配置子目录（alerts.csv、commands.csv、review.csv）；
#    终端打印各配置的告警与命令条数。
# 5. 失败兜底 / Failure fallback: 缺输入时退出码 2。
# ============================================================================
import argparse
import collections
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import decide as D  # noqa: E402

LABELS = [D.QUALITY, D.SWITCH, D.DRIFT, D.SITE, D.TRANSIENT, D.ENVIRONMENT, D.ANOMALY]
SHORT = {D.QUALITY: "质量", D.SWITCH: "切换", D.DRIFT: "漂移", D.SITE: "现场", D.TRANSIENT: "瞬态", D.ENVIRONMENT: "环境",
         D.ANOMALY: "异常"}
CONFIGS = [
    ("default", "默认（全部信号）", {}),
    ("no_once", "不带同一段偏离只发一次", dict(once_per_episode=False)),
    ("only_data", "只用数据层", dict(layers=("data",))),
    ("only_perf", "只用性能层", dict(layers=("perf",))),
    ("only_detect", "只用检测通道", dict(layers=("detect",))),
    ("common_1_2", "共模比例二分之一", dict(common_ratio_num=1, common_ratio_den=2)),
    ("common_1_1", "共模比例全体", dict(common_ratio_num=1, common_ratio_den=1)),
    ("ksite_2", "K_site 二", dict(k_site=2)),
    ("ksite_5", "K_site 五", dict(k_site=5)),
    ("pctx_1", "p_ctx 六分之一", dict(p_ctx=1 / 6)),
    ("pctx_2", "p_ctx 六分之二", dict(p_ctx=2 / 6)),
]
APRIL = [
    # 1.26 版裁决第二点：时段起点以最早一层为准；时段一的数据层起点为 04-02 12:00（世界协调时），即当地 13 时
    ("时段一", "2022-04-02 13", "2022-04-03 08", ["2022-04-02", "2022-04-03"]),
    ("时段二", "2022-04-04 05", "2022-04-04 22", ["2022-04-04"]),
    ("时段三", "2022-04-10 00", "2022-04-10 07", ["2022-04-10"]),
    # 1.27 版裁决第一项：时段四的起点按数据层前移到 04-14（日分辨率）/ segment four starts on 04-14 (day resolution)
    ("时段四", "2022-04-14 00", "2022-04-16 02", ["2022-04-14", "2022-04-15", "2022-04-16"]),
    ("时段五", "2022-04-22 07", "2022-04-22 13", ["2022-04-22"]),
]
SUN_DAYS = ["2022-04-17", "2022-04-20", "2022-04-21", "2022-04-24", "2022-04-25"]


def read(path):
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def lh(ts):
    day, hod = D.local_of(ts)
    return "%s %02d" % (day, hod)


def fmt_local(ts):
    day, hod = D.local_of(ts)
    return "%s %02d:%02d" % (day[5:], hod, (ts % 3600) // 60)


def counts(alerts, pred=lambda a: True):
    c = collections.Counter(a["label"] for a in alerts if pred(a))
    return "；".join("%s %d" % (SHORT[k], c[k]) for k in LABELS if c[k]) or "无"


def table(head, rows):
    out = ["| " + " | ".join(head) + " |", "| " + " | ".join("---" for _ in head) + " |"]
    out += ["| " + " | ".join(str(x) for x in r) + " |" for r in rows]
    return "\n".join(out) + "\n"


def run_with_drift(inp, p):
    """跑 decide.run，同时记下每个日节拍之后各台的漂移状态与「同一段偏离」及其触发 / records drift and episode books."""
    book = {}
    orig = D.daily_tick

    def wrapped(day, t, inp_, p_, devs, perf, drift, *rest, boundary=True):
        orig(day, t, inp_, p_, devs, perf, drift, *rest, boundary=boundary)
        if boundary:
            book[D.next_day(day)] = {d: "|".join(v["triggers"]) for d, v in drift.items()}
            book["ep " + D.next_day(day)] = {d: "|".join(v) for d, v in rest[0].items()}

    D.daily_tick = wrapped
    try:
        out = D.run(inp, p)
    finally:
        D.daily_tick = orig
    return out, book


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", default="docs/m7/inputs")
    ap.add_argument("--out-dir", default="docs/m7/report")
    ap.add_argument("--stable", default="docs/m3_inject/v34/v34_daily.csv")
    ap.add_argument("--inject", default="docs/m3_a4/full_fix/inject-truth.csv")
    a = ap.parse_args()
    for f in (os.path.join(a.inputs, "hours.csv"), a.stable, a.inject):
        if not os.path.exists(f):
            print("ERROR: 缺少 %s" % f, file=sys.stderr)
            return 2
    os.makedirs(a.out_dir, exist_ok=True)
    inp = D.Inputs(a.inputs)
    devs = inp.devices
    p_point = D.derive_p_point(inp, D.Params())
    R, books = {}, {}
    for key, _, kw in CONFIGS:
        p = D.Params(p_point=p_point, **kw)
        (al, cm, rv), books[key] = run_with_drift(inp, p)
        R[key] = dict(alerts=al, commands=cm, review=rv)
        sub = os.path.join(a.out_dir, key)
        os.makedirs(sub, exist_ok=True)
        D.write(os.path.join(sub, "alerts.csv"), al, D.ALERT_COLS)
        D.write(os.path.join(sub, "commands.csv"), cm, D.COMMAND_COLS)
        D.write(os.path.join(sub, "review.csv"), rv, D.REVIEW_COLS)
        print(key, "告警", len(al), "命令", len(cm), "复核", len(rv))

    stable = {d: set() for d in devs}
    for r in read(a.stable):
        for d in devs:
            if r.get("stable_" + d) == "True":
                stable[d].add("2022-" + r["day"])
    injections = read(a.inject)
    # 检测标志（与 decide.run 相同的写法）/ detection flags as in decide.run
    flags = {}
    for (dev, hs), r in inp.hours.items():
        rounds, adm, guard = int(r["rounds"]), int(r["admitted"]), int(r["guard_rounds"])
        if D.quality_bad(rounds, guard, D.Params()):
            continue
        pf = adm >= 180 and int(r["outlier_rounds"]) / adm > p_point
        cf = int(r["ctx_above"]) / 6.0 >= 0.5
        if pf or cf:
            flags[(dev, hs)] = ("点" if pf else "") + ("上" if cf else "")

    out = []
    w = out.append
    default = R["default"]
    al, cm, rv = default["alerts"], default["commands"], default["review"]
    w("# 聚合模块离线实验报告表格（m7/report_tables.py 生成）\n\n默认参数，p_point = %r；比较期 03-08 至 04-30 当地日。\n\n" % p_point)

    # ---------------- 第 1 条
    w("## 第 1 条：告警按归因与范围的分布\n\n")
    rows = []
    for lab in LABELS:
        c = collections.Counter(x["scope"] for x in al if x["label"] == lab)
        rows.append([lab, c["全局"], c["局部"], c["全局"] + c["局部"]])
    rows.append(["合计", sum(r[1] for r in rows), sum(r[2] for r in rows), len(al)])
    w(table(["归因", "全局", "局部", "合计"], rows))
    w("\n逐台：\n\n")
    w(table(["设备"] + [SHORT[x] for x in LABELS] + ["合计"],
            [[d] + [sum(1 for x in al if x["device"] == d and x["label"] == lab) for lab in LABELS]
             + [sum(1 for x in al if x["device"] == d)] for d in devs]))
    w("\n逐日（设备小时数）：\n\n")
    days = sorted({x["local_hour"][:10] for x in al})
    w(table(["当地日"] + [SHORT[x] for x in LABELS] + ["命令"],
            [[dd[5:]] + [sum(1 for x in al if x["local_hour"].startswith(dd) and x["label"] == lab) or "" for lab in LABELS]
             + [sum(1 for c in cm if c["day"] == dd) or ""] for dd in days]))
    kinds = collections.Counter(r["kind"] for r in rv)
    w("\n日界复核：%s。改记的两条：%s。\n\n" % ("；".join("%s %d 条" % kv for kv in kinds.items()),
                                       "；".join("%s %s" % (r["device"], r["detail"]) for r in rv if r["kind"] != "当日不可判")))

    # ---------------- 第 2 条
    w("## 第 2 条：零级命令清单（%d 条）\n\n" % len(cm))
    w(table(["设备", "命令日", "决定（当地）", "生效（当地）", "序号", "范围", "触发", "时间形态依据", "同报台数", "同报设备", "该段第几次"],
            [[c["device"], c["day"][5:], fmt_local(c["decided_at"]), fmt_local(c["effective_at"]), c["seq"], c["scope"],
              c["triggers"], c["forms"], c["co"], c["co_devices"], c["episode_n"]] for c in cm]))
    w("\n按触发：%s；按范围：%s；按设备：%s。不带同一段偏离只发一次的对照跑 %d 条。\n\n" % (
        dict(collections.Counter(t for c in cm for t in c["triggers"].split("|"))),
        dict(collections.Counter(c["scope"] for c in cm)), dict(sorted(collections.Counter(c["device"] for c in cm).items())),
        len(R["no_once"]["commands"])))

    # ---------------- 第 3 条：片段矩阵
    w("## 第 3 条：片段矩阵的实际归因与命令\n\n")

    def seg_rows(key):
        A, C = R[key]["alerts"], R[key]["commands"]
        rows = []
        m = lambda x: "2022-03-22" <= x["local_hour"][:10] <= "2022-03-27"
        rows.append(["三月事件 03-22 至 03-27", counts(A, m),
                     "；".join("%s %s %s" % (c["device"], c["day"][5:], c["triggers"]) for c in C if "2022-03-22" <= c["day"] <= "2022-03-27") or "无"])
        rows.append(["气体抬高（命令日 03-14 至 03-21）", "—",
                     "；".join("%s %s" % (c["device"], c["day"][5:]) for c in C if "2022-03-14" <= c["day"] <= "2022-03-21"
                              and "Gas" in c["triggers"].split("|")) or "无"])
        for name, s, e, sd in APRIL:
            rows.append(["四月" + name + " %s 时至 %s 时" % (s[5:], e[5:]), counts(A, lambda x: s <= x["local_hour"] <= e),
                         "；".join("%s %s %s" % (c["device"], c["day"][5:], c["triggers"]) for c in C if c["day"] in sd) or "无"])
        rows.append(["C 正午（日照直射日 10 至 15 时）",
                     counts(A, lambda x: x["device"] == "C" and x["local_hour"][:10] in SUN_DAYS and 10 <= int(x["local_hour"][11:]) <= 15),
                     "；".join("C %s %s" % (c["day"][5:], c["triggers"]) for c in C if c["device"] == "C" and c["day"] in SUN_DAYS) or "无"])
        lab = {(x["device"], x["hour_start"]): x["label"] for x in A}
        eve = collections.defaultdict(list)
        for (dev, hs), f in flags.items():
            if 16 <= D.local_of(hs)[1] <= 21:
                eve[hs].append(dev)
        ev_hours = [(dev, hs) for hs, ds in eve.items() if len(ds) >= 2 for dev in ds]
        c = collections.Counter(lab.get(k, "无告警") for k in ev_hours)
        rows.append(["傍晚多台同小时有检测标志（16 至 21 时，%d 个设备小时）" % len(ev_hours),
                     "；".join("%s %d" % (SHORT.get(k, k), v) for k, v in c.most_common()), "—"])
        rows.append(["G 光照", counts(A, lambda x: x["device"] == "G"),
                     "；".join("G %s %s" % (c["day"][5:], c["triggers"]) for c in C if c["device"] == "G" and "Light" in c["triggers"].split("|")) or "无"])
        inj = collections.Counter()
        for r in injections:
            hs0 = int(r["start_ts"]) // 3600 * 3600
            for hs in range(hs0, int(r["end_ts"]), 3600):
                inj[lab.get((r["device"], hs), "无告警")] += 1
        rows.append(["十四段注入（E，所覆盖的设备小时）", "；".join("%s %d" % (SHORT.get(k, k), v) for k, v in inj.most_common()), "—"])
        st = lambda x: x["local_hour"][:10] in stable[x["device"]]
        rows.append(["平稳日（%d 个设备日）" % sum(len(v) for v in stable.values()), counts(A, st),
                     "；".join("%s %s %s" % (c["device"], c["day"][5:], c["triggers"]) for c in C if c["day"] in stable[c["device"]]) or "无"])
        return rows

    w(table(["片段", "告警（设备小时，按归因）", "命令（设备、命令日、触发）"], seg_rows("default")))
    # 三月事件逐日叠加 / March event day by day with and without the once rule
    w("\n三月事件与气体抬高的逐日命令（默认 / 不带同一段偏离只发一次）：\n\n")
    rows = []
    for dd in ["2022-03-%02d" % k for k in range(14, 29)]:
        rows.append([dd[5:]] + ["%s / %s" % (
            next((c["triggers"] for c in R["default"]["commands"] if c["device"] == d and c["day"] == dd), "") or "·",
            next((c["triggers"] for c in R["no_once"]["commands"] if c["device"] == d and c["day"] == dd), "") or "·")
            for d in devs])
    w(table(["命令日"] + devs, rows))
    w("\n03-22 当地零点（03-21 的日节拍之后）各台已在「同一段偏离」中的触发：%s；处于漂移状态的触发：%s。\n\n" % (
        "；".join("%s %s" % (d, t) for d, t in sorted(books["default"].get("ep 2022-03-22", {}).items())) or "无",
        "；".join("%s %s" % (d, t) for d, t in sorted(books["default"].get("2022-03-22", {}).items())) or "无"))
    # 三月三十日的机队级爆发 / the fleet burst on 03-30
    w("三月三十日 20:20（世界协调时，当地 21:20）机队级爆发前后，各台当地 21 至 23 时的检测标志与归因：\n\n")
    lab = {(x["device"], x["local_hour"]): (x["label"], x["burst"]) for x in al}
    rows = []
    for d in devs:
        cells = []
        for h in (21, 22, 23):
            key = "2022-03-30 %02d" % h
            hs = D.local_day_start("2022-03-30") + 3600 * h
            l, b = lab.get((d, key), ("无告警", 0))
            cells.append("%s%s%s" % (SHORT.get(l, l), "，爆发" if int(b) else "", "，" + flags[(d, hs)] if (d, hs) in flags else ""))
        rows.append([d] + cells)
    w(table(["设备", "21 时", "22 时", "23 时"], rows))
    w("（「点」「上」表示该小时有点通道、上下文通道标志。）\n\n")
    ph = sum(1 for (dev, dd, ch), r in inp.days.items() if ch in ("Pressure", "Humidity") and r["deviated"] == "1")
    w("气压与湿度：偏离的设备日 %d 个，命令触发中出现 0 次（按构造只记录）。\n\n" % ph)

    # ---------------- 第 4 条：消融
    w("## 第 4 条：消融（理论第四项验证）\n\n")
    rows = []
    for key in ("default", "only_data", "only_perf", "only_detect"):
        A, C = R[key]["alerts"], R[key]["commands"]
        name = dict((k, n) for k, n, _ in CONFIGS)[key]
        st = sum(1 for x in A if x["local_hour"][:10] in stable[x["device"]])
        st_cmd = sum(1 for c in C if c["day"] in stable[c["device"]])
        march = any("2022-03-22" <= c["day"] <= "2022-03-27" for c in C)
        g = any(c["device"] == "G" and "Light" in c["triggers"].split("|") for c in C)
        gas = any("2022-03-14" <= c["day"] <= "2022-03-21" and "Gas" in c["triggers"].split("|") for c in C)
        rows.append([name, len(A), counts(A), len(C), "%d（%s）" % (st, counts(A, lambda x: x["local_hour"][:10] in stable[x["device"]])),
                     st_cmd, "有" if march else "无", "有" if g else "无", "有" if gas else "无"])
    w(table(["配置", "告警", "按归因", "命令", "平稳日告警（误报侧）", "平稳日命令", "三月事件命令", "G 光照命令", "气体抬高命令"], rows))
    w("\n按片段并排（告警按归因；命令条数）：\n\n")
    segs = {k: seg_rows(k) for k in ("default", "only_data", "only_perf", "only_detect")}
    rows = []
    for i, r in enumerate(segs["default"]):
        rows.append([r[0]] + ["%s；命令 %s" % (segs[k][i][1], "0" if segs[k][i][2] in ("无", "—") else len(segs[k][i][2].split("；")))
                              if segs[k][i][2] != "—" else segs[k][i][1] for k in ("default", "only_data", "only_perf", "only_detect")])
    w(table(["片段", "默认", "只用数据层", "只用性能层", "只用检测通道"], rows))

    # ---------------- 第 5 条
    w("\n## 第 5 条：共模比例的敏感性\n\n")
    rows = []
    for key, name in (("common_1_2", "二分之一"), ("default", "四分之三（默认）"), ("common_1_1", "全体")):
        A, C = R[key]["alerts"], R[key]["commands"]
        cells = []
        for lab in LABELS:
            c = collections.Counter(x["scope"] for x in A if x["label"] == lab)
            cells.append("%d / %d" % (c["全局"], c["局部"]))
        cs = collections.Counter(c["scope"] for c in C)
        rows.append([name] + cells + ["%d / %d" % (cs["全局"], cs["局部"])])
    w(table(["共模比例"] + [SHORT[x] + "（全局 / 局部）" for x in LABELS] + ["命令（全局 / 局部）"], rows))

    # ---------------- 第 6 条
    w("\n## 第 6 条：K_site 取二、三、五天时 C 归因为现场条件的日期（全部钟点；正午指 10 至 15 时）\n\n")
    rows = []
    for key, name in (("ksite_2", "二"), ("default", "三（默认）"), ("ksite_5", "五")):
        A = R[key]["alerts"]
        sites = sorted({x["local_hour"] for x in A if x["device"] == "C" and x["label"] == D.SITE})
        byday = collections.OrderedDict()
        for s in sites:
            byday.setdefault(s[5:10], []).append(s[11:])
        noon = [s for s in sites if 10 <= int(s[11:]) <= 15]
        rows.append([name, len(sites), "；".join("%s（%s 时）" % (k, "、".join(v)) for k, v in byday.items()) or "无",
                     "、".join(s[5:] for s in noon) or "无", sum(1 for x in A if x["label"] == D.SITE)])
    w(table(["K_site", "C 的现场条件小时数", "日期（当地钟点）", "其中正午", "全机队现场条件小时数"], rows))

    # ---------------- 第 7 条
    w("\n## 第 7 条：日界等待的代价\n\n每条零级命令从触发通道进入偏离（该通道最近一次进入偏离的当地日零点；性能层取这一段确认的时刻）到命令生效：\n\n")
    spells = collections.defaultdict(list)
    for dev in devs:
        pf = D.PerfTracker(inp.perf_events.get(dev, []), inp.perf_elements.get(dev, []))
        for t in sorted({x[0] for x in pf.items}):
            was = pf.confirmed
            pf.advance(t)
            if pf.confirmed and not was:
                spells[dev].append(pf.since)
    # 只计每段偏离第一次出现在命令里的那个触发（即开启新一段的触发）；同一段在后续命令里再次列出的不重复计
    # only the trigger that opens a deviation segment counts; a segment listed again in later commands is skipped
    rows, gaps, seen = [], [], set()
    for c in cm:
        parts = []
        for tr in c["triggers"].split("|"):
            if tr == "perf":
                t0 = max(s for s in spells[c["device"]] if s <= c["decided_at"])
            else:
                entry = max(dd for (dev, dd, ch), r in inp.days.items()
                            if dev == c["device"] and ch == tr and r["transition"] == "DEVIATE" and dd <= c["day"])
                t0 = D.local_day_start(entry)
            g = (c["effective_at"] - t0) / 86400
            label = "%s %s 起" % (tr, fmt_local(t0)[:5] if tr != "perf" else fmt_local(t0))
            if (c["device"], tr, t0) in seen:
                parts.append(label + "（该段已在此前命令中）")
                continue
            seen.add((c["device"], tr, t0))
            gaps.append(g)
            parts.append("%s %.1f 天" % (label, g))
        rows.append([c["device"], c["day"][5:], fmt_local(c["effective_at"]), "；".join(parts)])
    w(table(["设备", "命令日", "生效（当地）", "进入偏离到生效"], rows))
    gaps.sort()
    w("\n开启新一段的触发共 %d 个：最短 %.1f 天，中位 %.1f 天，最长 %.1f 天；大于三天的触发逐条见上表。\n\n" % (
        len(gaps), gaps[0], gaps[len(gaps) // 2], gaps[-1]))
    # 瞬态事件：按设备的连续小时段；若按小时节拍立即响应（在段首小时终点发命令，同样带一天冷却期）会多发多少命令
    runs = collections.defaultdict(list)
    for x in sorted((x for x in al if x["label"] == D.TRANSIENT), key=lambda x: (x["device"], x["hour_start"])):
        rr = runs[x["device"]]
        if rr and rr[-1][1] == x["hour_start"]:
            rr[-1][1] = x["hour_start"] + 3600
        else:
            rr.append([x["hour_start"], x["hour_start"] + 3600])
    n_runs = sum(len(v) for v in runs.values())
    immediate, matched = 0, 0
    rows = []
    for dev in devs:
        until = 0
        k_imm = k_match = 0
        for s, e in runs[dev]:
            t = s + 3600
            if t < until:
                continue
            k_imm += 1
            until = D.add_local_days(t, 1)
            if any(c["device"] == dev and s <= c["decided_at"] <= D.add_local_days(e, 2) for c in cm):
                k_match += 1
        immediate += k_imm
        matched += k_match
        rows.append([dev, len(runs[dev]), sum((e - s) // 3600 for s, e in runs[dev]), k_imm, k_match, k_imm - k_match])
    w("「瞬态事件（待日界判定）」按设备的连续小时段 %d 段。若按小时节拍立即响应（段首小时终点即发命令，同样带一天冷却期），逐台：\n\n" % n_runs)
    w(table(["设备", "瞬态段数", "瞬态小时数", "立即响应会发的命令", "其中两天内日界也发了命令", "多发"], rows))
    w("\n合计立即响应 %d 条，其中 %d 条在段结束后两天内日界也发了命令，多发 %d 条。\n\n" % (immediate, matched, immediate - matched))

    # ---------------- 第 8 条
    w("## 第 8 条：C 正午各小时的实际归因（日照直射日，当地 10 至 15 时）\n\n")
    labc = {x["local_hour"]: x for x in al if x["device"] == "C"}
    rows = []
    for dd in SUN_DAYS:
        cells = []
        for h in range(10, 16):
            x = labc.get("%s %02d" % (dd, h))
            hs = D.local_day_start(dd) + 3600 * h
            f = flags.get(("C", hs), "")
            if x is None:
                cells.append("无告警" + ("，" + f if f else ""))
            elif x["label"] == D.DRIFT:
                cells.append("漂移（状态触发 %s；数据层偏离 %s）%s" % (books["default"].get(dd, {}).get("C", "?"), x["data_channels"] or "无",
                                                       "，" + f if f else ""))
            else:
                cells.append(SHORT[x["label"]] + ("，" + f if f else ""))
        rows.append([dd[5:]] + cells)
    w(table(["当地日"] + ["%d 时" % h for h in range(10, 16)], rows))
    w("（「点」「上」表示该小时有点通道、上下文通道标志；「状态触发」是该日零点日节拍之后 C 的漂移状态由哪个通道或性能层触发。）\n")

    # ================ 1.25 版裁决要求的补充记录 / supplements requested by ruling 1.25
    w("\n## 补充一（裁决第一项）：三月事件逐台的命令\n\n")
    rows = []
    for d in devs:
        before = books["default"].get("ep 2022-03-22", {}).get(d, "")
        mine = [c for c in cm if c["device"] == d and "2022-03-22" <= c["day"] <= "2022-03-27"]
        no = [c for c in R["no_once"]["commands"] if c["device"] == d and "2022-03-22" <= c["day"] <= "2022-03-27"]
        rows.append([d, before or "无", "；".join("%s %s" % (c["day"][5:], c["triggers"]) for c in mine) or "无",
                     len(no), "；".join("%s %s" % (c["day"][5:], c["triggers"]) for c in no) or "无"])
    w(table(["设备", "03-22 零点时未结束的段（触发类）", "默认：命令日 03-22 至 03-27 的命令", "对照跑：命令数",
             "对照跑：命令"], rows))

    w("\n## 补充二（裁决第二项）：决定日在四月共模时段上的命令逐条分类\n\n")
    w("判定：触发通道的偏离段在时段所在当地日（时段一含数据层起点 04-02，时段四含数据层起点 04-14）进入、之后回落的，记为长瞬态实例候选；进入日早于时段的，记为与时段重合；进入后到 04-30 未回落的单列。\n\n")

    def segment_of(dev, ch, day):
        ds = sorted(dd for (dv, dd, c), r in inp.days.items() if dv == dev and c == ch and r["transition"] == "DEVIATE" and dd <= day)
        start = ds[-1] if ds else None
        rec = sorted(dd for (dv, dd, c), r in inp.days.items()
                     if dv == dev and c == ch and r["transition"] == "RECOVER" and start and dd > start)
        return start, rec[0] if rec else None

    rows = []
    for name, s0, e0, sd in APRIL:
        for c in cm:
            if c["day"] not in sd:
                continue
            for tr in c["triggers"].split("|"):
                if tr == "perf":
                    rows.append([name, c["device"], c["day"][5:], tr, "—", "—", "性能层触发，另见第 9 条"])
                    continue
                st0, en = segment_of(c["device"], tr, c["day"])
                if st0 in sd:
                    verdict = "长瞬态实例候选（时段后 %s 回落）" % en[5:] if en else "时段内进入，至 04-30 未回落"
                elif st0 and st0 < sd[0]:
                    lag = (D.local_day_start(sd[0]) - D.local_day_start(st0)) // 86400
                    verdict = "与时段重合（偏离段早 %d 天进入）" % lag
                else:
                    verdict = "其他"
                rows.append([name, c["device"], c["day"][5:], tr, st0[5:] if st0 else "—", en[5:] if en else "未回落", verdict])
    w(table(["时段", "设备", "命令日", "触发通道", "偏离段进入（当地日）", "回落（当地日）", "判定"], rows))

    # 各时段各层的时间（1.26 版裁决第二点：表里同时列各层的时间）/ per-layer times of each segment
    w("\n各时段各层的时间（当地）：性能层取数据事实第三十五条候选；数据层爆发取性能层起点前二十四小时至时段末之间的离群率爆发，"
      "同一时刻至少三台的记为机队级；气体进入偏离取时段所在日与前一日的状态转移。\n\n")
    perf_win = {"时段一": "04-03 00–09", "时段二": "04-04 05–23", "时段三": "04-10 00–08", "时段四": "04-15 傍晚至 04-16 03",
                "时段五": "04-22 07–14"}
    perf_start = {"时段一": "2022-04-03 00", "时段二": "2022-04-04 05", "时段三": "2022-04-10 00", "时段四": "2022-04-15 17",
                  "时段五": "2022-04-22 07"}
    rows = []
    for name, s0, e0, sd in APRIL:
        t0 = D.local_day_start(perf_start[name][:10]) + 3600 * int(perf_start[name][11:]) - 86400
        t1 = D.local_day_start(e0[:10]) + 3600 * (int(e0[11:]) + 1)
        bs = collections.defaultdict(list)
        for dev in devs:
            for b, _ in inp.bursts.get(dev, []):
                if t0 <= b <= t1:
                    bs[b].append(dev)
        fleet = [(b, ds) for b, ds in sorted(bs.items()) if len(ds) >= 3]
        d0 = D.add_days(perf_start[name][:10], -1)
        ent = collections.defaultdict(list)
        for (dev, dd, ch), r in inp.days.items():
            if ch == "Gas" and r["transition"] == "DEVIATE" and d0 <= dd <= e0[:10]:
                ent[dd].append(dev)
        rows.append([name, perf_win[name],
                     "；".join("%s（%s）" % (fmt_local(b), "".join(sorted(ds))) for b, ds in fleet) or "无",
                     "；".join("%s（%s）" % (fmt_local(b), "".join(sorted(ds))) for b, ds in sorted(bs.items()) if len(ds) < 3) or "无",
                     "；".join("%s（%s）" % (dd[5:], "".join(sorted(v))) for dd, v in sorted(ent.items())) or "无",
                     "%s 时至 %s 时" % (s0[5:], e0[5:])])
    w(table(["时段", "性能层", "数据层机队级爆发", "数据层其他爆发", "气体进入偏离", "本表采用的时段"], rows))

    w("\n## 补充三之二（1.26 版裁决第三点）：全机队六十一个现场条件小时\n\n")
    base = {}
    for (dev, hs), r in inp.hours.items():
        fd = D.local_of(inp.freeze[dev])[0]
        day, hod = D.local_of(hs)
        if fd <= day <= D.add_days(fd, 9):
            for ch in D.ACTIVE:
                n = int(r["n_" + ch])
                if n > 0:
                    s1, n1 = base.get((dev, ch, hod), (0.0, 0))
                    base[(dev, ch, hod)] = (s1 + float(r["sum_" + ch]), n1 + n)
    site_rows, by = [], collections.Counter()
    for x in al:
        if x["label"] != D.SITE:
            continue
        r = inp.hours[(x["device"], x["hour_start"])]
        hod = int(x["local_hour"][11:])
        chs = []
        for ch in D.ACTIVE:
            n = int(r["n_" + ch])
            b = base.get((x["device"], ch, hod))
            p10, p90 = inp.refs[(x["device"], ch)]
            if n and b:
                dlt = float(r["sum_" + ch]) / n - b[0] / b[1]
                if abs(dlt) > 0.5 * (p90 - p10):
                    chs.append("%s %+.2f 宽" % (ch, dlt / (p90 - p10)))
        flg = ("点" if x["point_flag"] else "") + ("上" if x["ctx_flag"] else "")
        site_rows.append([x["device"], x["local_hour"][5:], flg, "；".join(chs), x["note"] or ""])
        by[(x["device"], hod)] += 1
    w(table(["设备", "当地时", "检测标志", "同步变化的通道（偏离 / 参照 P90−P10 宽度）", "备注"], site_rows))
    w("\n按设备与当地钟点计：%s。\n" % "；".join("%s %02d 时 %d" % (d, h, n) for (d, h), n in sorted(by.items())))

    w("\n## 补充三（裁决第三项）：p_ctx 取六分之一、六分之二、六分之三时 C 正午各小时的标志与归因\n\n")
    w("格内依次为：上下文通道超阈窗口数 / 6；三档 p_ctx 下的归因（标志有无写在括号里，「上」为上下文通道标志）。\n\n")
    rows = []
    for dd in SUN_DAYS:
        for h in range(10, 16):
            hs = D.local_day_start(dd) + 3600 * h
            r = inp.hours.get(("C", hs))
            cells = [dd[5:] + " %02d" % h, r["ctx_above"] if r else "—"]
            for key in ("pctx_1", "pctx_2", "default"):
                x = next((y for y in R[key]["alerts"] if y["device"] == "C" and y["hour_start"] == hs), None)
                cells.append("无告警" if x is None else SHORT[x["label"]] + ("（上）" if x["ctx_flag"] else ""))
            rows.append(cells)
    w(table(["C 当地时", "超阈窗口数", "p_ctx 1/6", "p_ctx 2/6", "p_ctx 3/6（默认）"], rows))
    for key in ("pctx_1", "pctx_2", "default"):
        A = R[key]["alerts"]
        w("\n%s：全机队现场条件 %d 小时，C 的现场条件 %d 小时（正午 %d），命令 %d 条。" % (
            dict((k, n) for k, n, _ in CONFIGS)[key], sum(1 for x in A if x["label"] == D.SITE),
            sum(1 for x in A if x["label"] == D.SITE and x["device"] == "C"),
            sum(1 for x in A if x["label"] == D.SITE and x["device"] == "C" and 10 <= int(x["local_hour"][11:]) <= 15),
            len(R[key]["commands"])))
    w("\n")

    w("\n## 补充四（裁决第五项）：十四段注入所在小时的归因与原因\n\n")
    rows = []
    labd = {(x["device"], x["hour_start"]): x for x in al}
    for r in injections:
        hs0 = int(r["start_ts"]) // 3600 * 3600
        for hs in range(hs0, int(r["end_ts"]), 3600):
            x = labd.get((r["device"], hs))
            h = inp.hours.get((r["device"], hs))
            day = D.local_of(hs)[0]
            if x is None:
                why = "无告警"
            elif x["label"] == D.DRIFT:
                why = "漂移期，状态触发 %s" % books["default"].get(day, {}).get(r["device"], "?")
            elif x["label"] == D.QUALITY:
                miss = 1 - int(h["rounds"]) / 360
                guard = int(h["guard_rounds"]) / 360
                why = "缺轮 %.0f%%，守卫打标 %.0f%%（%s）" % (100 * miss, 100 * guard, "守卫超门槛" if guard > 0.2 else "缺轮超门槛")
            else:
                why = ""
            rows.append(["%s %s %s" % (r["channel"], r["type"], r["magnitude"].rstrip("0").rstrip(".")), x["local_hour"] if x else lh(hs),
                         SHORT.get(x["label"], "") if x else "无告警", why,
                         ("点" if x and x["point_flag"] else "") + ("上" if x and x["ctx_flag"] else "") or "无"])
    w(table(["注入（E）", "当地小时", "归因", "原因", "载荷中的检测标志"], rows))

    with open(os.path.join(a.out_dir, "tables.md"), "w", encoding="utf-8") as fh:
        fh.write("".join(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
