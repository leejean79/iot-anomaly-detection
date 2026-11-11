#!/usr/bin/env python3
# ============================================================================
# m6b/chain_report.py
# 链式离线实验的报告表格（M6b 交接文档第四节「判读口径」六条与片段矩阵）：读 com.leejean.m6.ChainDriver 四档（开环、D1、D7、只重估
# 位置）的输出，生成驱动正确性核对、片段矩阵三档并排、每条命令生效后七天的偏离日数、点通道离群率的变化、丙型实例清单与选定判据的
# 两项计数。本脚本只计数与列表，不下结论；片段的时段、平稳日与日照直射日沿用聚合模块报告表格（m7/report_tables.py）的定义。
# Tables for the chained offline experiment report; counts and lists only.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m6b/chain_report.py [--chain-dir docs/m6b/chain] [--out docs/m6b/chain/report_tables.md]
#    可选：--inputs docs/m7/inputs、--baseline docs/m7/run、--stable docs/m3_inject/v34/v34_daily.csv、
#    --inject docs/m3_a4/full_fix/inject-truth.csv、--variants open,D7,D7K（只出这几档；第一档须为 open）
# 3. 前置条件 / Preconditions: 已运行 ChainDriver 的 open、D1、D7、pos 四档。
# 4. 期望产出 / Expected output: --out 处的 Markdown 表格；终端打印每档的命令数与丙型实例数。
# 5. 失败兜底 / Failure fallback: 缺少某一档的输出时退出码 2。
# ============================================================================
import argparse
import collections
import csv
import filecmp
import os
import sys

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(HERE, "m7"))
import decide as D                     # noqa: E402
from report_tables import APRIL, SHORT, SUN_DAYS, LABELS, counts, table   # noqa: E402

NAMES = {"open": "开环对照", "D1": "甲：重估一天", "D7": "乙：重估七天", "pos": "丙：只重估位置",
         "D7K": "乙档加开关：湿度与气压不重估"}
VARIANTS = [(v, NAMES[v]) for v in ("open", "D1", "D7", "pos")]
ARMS = VARIANTS[1:]
CH = list(D.CHANNELS)


def read(path):
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def lday(ts):
    return D.local_of(int(ts))[0]


def fmt_local(ts):
    day, hod = D.local_of(int(ts))
    return "%s %02d:%02d" % (day[5:], hod, (int(ts) % 3600) // 60)


class Run:
    def __init__(self, d):
        self.alerts = read(os.path.join(d, "alerts.csv"))
        self.commands = read(os.path.join(d, "commands.csv"))
        self.review = read(os.path.join(d, "review.csv"))
        self.days = {(r["device"], r["day"], r["channel"]): r for r in read(os.path.join(d, "data_days.csv"))}
        self.hours = {(r["device"], int(r["hour_start"])): (int(r["outlier_rounds"]), int(r["points"]), int(r["version"]))
                      for r in read(os.path.join(d, "hours.csv"))}
        self.executed = {(os.path.splitext(f)[0]) for f in os.listdir(os.path.join(d, "level0"))}

    def dev_days(self, dev, ch, first, n):
        """从 first 起 n 个当地日内该通道日末偏离的日数 / deviated days in n local days from first."""
        k = 0
        for i in range(n):
            r = self.days.get((dev, D.add_days(first, i), ch))
            k += 1 if r is not None and r["deviated"] == "1" else 0
        return k

    def rate(self, dev, t0, t1):
        o = p = 0
        for hs in range(t0 // 3600 * 3600, t1, 3600):
            v = self.hours.get((dev, hs))
            if v:
                o, p = o + v[0], p + v[1]
        return None if p == 0 else o / p


def pct(x):
    return "—" if x is None else "%.2f%%" % (100 * x)


def cmd_list(C, pred):
    return "；".join("%s %s %s" % (c["device"], c["day"][5:], c["triggers"]) for c in C if pred(c)) or "无"


def type_c(run):
    """丙型实例：同一台、同一触发通道的相邻两条命令，触发日的偏移（中位数相对参照）符号相反 / opposite-sign successive commands."""
    out = []
    by = collections.defaultdict(list)
    for c in run.commands:
        for tr in c["triggers"].split("|"):
            if tr != "perf":
                by[(c["device"], tr)].append(c)
    for (dev, tr), cs in sorted(by.items()):
        for a, b in zip(cs, cs[1:]):
            sa = float(run.days[(dev, a["day"], tr)]["shift"])
            sb = float(run.days[(dev, b["day"], tr)]["shift"])
            if sa * sb < 0:
                back = next((dd for dd in sorted({k[1] for k in run.days if k[0] == dev})
                             if lday(a["effective_at"]) <= dd <= b["day"] and run.days[(dev, dd, tr)]["transition"] == "DEVIATE"),
                            "")
                out.append((dev, tr, a, sa, back, b, sb))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain-dir", default="docs/m6b/chain")
    ap.add_argument("--inputs", default="docs/m7/inputs")
    ap.add_argument("--baseline", default="docs/m7/run")
    ap.add_argument("--stable", default="docs/m3_inject/v34/v34_daily.csv")
    ap.add_argument("--inject", default="docs/m3_a4/full_fix/inject-truth.csv")
    ap.add_argument("--out", default="docs/m6b/chain/report_tables.md")
    ap.add_argument("--variants", default="open,D1,D7,pos", help="第一档须为 open；D7K 为 1.41 版裁决第一节的开关档")
    a = ap.parse_args()
    global VARIANTS, ARMS
    VARIANTS = [(v, NAMES[v]) for v in a.variants.split(",")]
    ARMS = VARIANTS[1:]
    for v, _ in VARIANTS:
        if not os.path.exists(os.path.join(a.chain_dir, v, "commands.csv")):
            print("ERROR: 缺少 %s/%s 的输出" % (a.chain_dir, v), file=sys.stderr)
            return 2
    R = {v: Run(os.path.join(a.chain_dir, v)) for v, _ in VARIANTS}
    devs = sorted({c["device"] for c in R["open"].commands} | {k[0] for k in R["open"].days})
    stable = {d: set() for d in devs}
    for r in read(a.stable):
        for d in devs:
            if r.get("stable_" + d) == "True":
                stable[d].add("2022-" + r["day"])
    injections = read(a.inject)
    out = []
    w = out.append
    w("# 链式离线实验报告表格（m6b/chain_report.py 生成）\n\n比较期 03-08 至 04-30 当地日；命令日为决定所在的当地日，"
      "决定时刻为该日结束的日界，生效时刻为再下一个日界。\n\n")

    # ---------------- 判读口径一：驱动正确性
    w("## 判读口径一：开环对照与聚合模块收口基线\n\n")
    rows = []
    for f in ("alerts", "commands", "review"):
        same = filecmp.cmp(os.path.join(a.chain_dir, "open", f + ".csv"), os.path.join(a.baseline, f + ".csv"), shallow=False)
        rows.append([f + ".csv", len(read(os.path.join(a.baseline, f + ".csv"))), "逐字节相同" if same else "不同"])
    base_h = {(r["device"], int(r["hour_start"])): int(r["outlier_rounds"]) for r in read(os.path.join(a.inputs, "hours.csv"))}
    dh = sum(1 for k, v in base_h.items() if R["open"].hours.get(k, (0,))[0] != v)
    base_d = {(r["device"], r["day"], r["channel"]): (r["evaluable"], r["deviated"], r["transition"])
              for r in read(os.path.join(a.inputs, "data_days.csv"))}
    dd = sum(1 for k, v in base_d.items() if k not in R["open"].days
             or (R["open"].days[k]["evaluable"], R["open"].days[k]["deviated"], R["open"].days[k]["transition"]) != v)
    base_b = {(r["device"], r["ts"], r["emitted_at"], r["size"], r["duration"], float(r["peak"])) for r in read(os.path.join(a.inputs, "bursts.csv"))}
    sim_b = {(r["device"], r["ts"], r["emitted_at"], r["size"], r["duration"], float(r["peak"]))
             for r in read(os.path.join(a.chain_dir, "open", "bursts.csv"))}
    rows.append(["小时离群轮数（收口输入 hours.csv）", len(base_h), "不同 %d 个设备小时" % dh])
    rows.append(["数据层日结论（data_days.csv：可判、偏离、转移）", len(base_d), "不同 %d 条" % dd])
    rows.append(["离群率爆发（bursts.csv）", len(base_b), "不同 %d 条" % len(base_b ^ sim_b)])
    w(table(["核对项", "收口基线条数", "开环对照"], rows))

    # ---------------- 判读口径二：命令清单与片段矩阵
    w("\n## 判读口径二：三档命令清单与片段矩阵\n\n### 逐日命令（触发；· 为无命令）\n\n")
    days = sorted({c["day"] for v, _ in VARIANTS for c in R[v].commands})
    rows = []
    for d in days:
        rows.append([d[5:]] + ["；".join("%s %s" % (c["device"], c["triggers"]) for c in R[v].commands if c["day"] == d) or "·"
                               for v, _ in VARIANTS])
    w(table(["命令日"] + [n for _, n in VARIANTS], rows))

    w("\n### 片段矩阵\n\n")
    lab = {v: {(x["device"], int(x["hour_start"])): x["label"] for x in R[v].alerts} for v, _ in VARIANTS}
    segs = []
    segs.append(("气体抬高（命令日 03-14 至 03-21，触发含气体）", None,
                 lambda C: cmd_list(C, lambda c: "2022-03-14" <= c["day"] <= "2022-03-21" and "Gas" in c["triggers"].split("|"))))
    segs.append(("三月事件 03-22 至 03-27", lambda x: "2022-03-22" <= x["local_hour"][:10] <= "2022-03-27",
                 lambda C: cmd_list(C, lambda c: "2022-03-22" <= c["day"] <= "2022-03-27")))
    for name, s, e, sd in APRIL:
        segs.append(("四月%s %s 时至 %s 时" % (name, s[5:], e[5:]), (lambda s, e: lambda x: s <= x["local_hour"] <= e)(s, e),
                     (lambda sd: lambda C: cmd_list(C, lambda c: c["day"] in sd))(sd)))
    segs.append(("C 正午（日照直射日 10 至 15 时）",
                 lambda x: x["device"] == "C" and x["local_hour"][:10] in SUN_DAYS and 10 <= int(x["local_hour"][11:]) <= 15,
                 lambda C: cmd_list(C, lambda c: c["device"] == "C" and c["day"] in SUN_DAYS)))
    segs.append(("G 光照", lambda x: x["device"] == "G",
                 lambda C: cmd_list(C, lambda c: c["device"] == "G" and "Light" in c["triggers"].split("|"))))
    segs.append(("平稳日（%d 个设备日）" % sum(len(v) for v in stable.values()), lambda x: x["local_hour"][:10] in stable[x["device"]],
                 lambda C: cmd_list(C, lambda c: c["day"] in stable[c["device"]])))
    rows = []
    for name, ap_, cp in segs:
        rows.append([name] + ["%s；命令：%s" % (counts(R[v].alerts, ap_) if ap_ else "—", cp(R[v].commands)) for v, _ in VARIANTS])
    inj = {v: collections.Counter() for v, _ in VARIANTS}
    for r in injections:
        for hs in range(int(r["start_ts"]) // 3600 * 3600, int(r["end_ts"]), 3600):
            for v, _ in VARIANTS:
                inj[v][lab[v].get((r["device"], hs), "无告警")] += 1
    rows.append(["十四段注入（E，所覆盖的设备小时）"] + ["；".join("%s %d" % (SHORT.get(k, k), n) for k, n in inj[v].most_common())
                                                for v, _ in VARIANTS])
    w(table(["片段"] + [n for _, n in VARIANTS], rows))

    w("\n### 全程\n\n")
    rows = []
    for v, n in VARIANTS:
        C = R[v].commands
        per = collections.Counter(c["device"] for c in C)
        gaps = []
        by = collections.defaultdict(list)
        for c in C:
            by[c["device"]].append(int(c["decided_at"]))
        for d, ts in by.items():
            gaps += [(t2 - t1) / 86400 for t1, t2 in zip(ts, ts[1:])]
        st = [c for c in C if c["day"] in stable[c["device"]]]
        rows.append([n, len(C), " ".join("%s=%d" % (d, per[d]) for d in devs), "%.0f 天" % min(gaps) if gaps else "—",
                     len(type_c(R[v])) if v != "open" else "—", len(st), len(R[v].alerts), counts(R[v].alerts)])
    w(table(["档", "命令", "每台", "同一台命令的最短间隔", "丙型实例", "平稳日命令", "告警", "告警按归因"], rows))
    w("\n（开环对照的命令只发出不执行，丙型实例不适用。丙型实例的判别见判读口径五。）\n")

    # ---------------- 判读口径三：命令生效后七天的偏离日数
    w("\n## 判读口径三：每条命令生效后七天内触发通道与非触发通道的数据层偏离日数\n\n"
      "七天为生效当地日起的七个当地日；偏离日为日末偏离的日记录（换版本后为新版本的记录）。非触发通道含只记录的湿度与气压。"
      "同一台在七天内又执行下一条命令时照常计数。\n\n")
    rows = []
    detail = {}
    for v, n in ARMS:
        run = R[v]
        trig = non = hp = cmds = 0
        lst = []
        for c in run.commands:
            if "%s_%s" % (c["device"], c["seq"]) not in run.executed:
                continue
            cmds += 1
            e = lday(c["effective_at"])
            tr = [t for t in c["triggers"].split("|") if t != "perf"]
            kt = sum(run.dev_days(c["device"], ch, e, 7) for ch in tr)
            kn = {ch: run.dev_days(c["device"], ch, e, 7) for ch in CH if ch not in tr}
            trig += kt
            non += sum(kn.values())
            hp += kn.get("Humidity", 0) + kn.get("Pressure", 0)
            lst.append([c["device"], c["day"][5:], e[5:], c["triggers"], kt,
                        "；".join("%s %d" % (ch, k) for ch, k in kn.items() if k) or "0"])
        detail[v] = lst
        rows.append([n, cmds, trig, "%.2f" % (trig / cmds), non, "%.2f" % (non / cmds), hp])
    rows3 = {r[0]: r for r in rows}
    w(table(["档", "执行的命令", "触发通道偏离日", "每条命令", "非触发通道偏离日", "每条命令", "其中湿度与气压"], rows))
    w("\n气体抬高的命令（命令日 03-14 至 03-16、触发含气体）生效后七天内湿度与气压的偏离日数（片段矩阵第一行的连带偏离）：\n\n")
    gas_rows = []
    for v, n in ARMS:
        run = R[v]
        cells = []
        for c in run.commands:
            if "2022-03-14" <= c["day"] <= "2022-03-16" and "Gas" in c["triggers"].split("|"):
                e = lday(c["effective_at"])
                cells.append("%s %d/%d" % (c["device"], run.dev_days(c["device"], "Humidity", e, 7), run.dev_days(c["device"], "Pressure", e, 7)))
        gas_rows.append([n, "；".join(cells)])
    w(table(["档", "设备 湿度偏离日/气压偏离日"], gas_rows))
    w("\n平稳日上的命令与该台上一条已执行命令的重估窗口（乙、丙两档的重估窗口为生效前 7 × 86,400 秒，甲档为生效前 86,400 秒，所以跨过夏令时开始日的窗口起点为当地 23 时）：\n\n")
    st_rows = []
    for v, n in ARMS:
        run = R[v]
        dsec = 86400 if v == "D1" else 7 * 86400
        for c in run.commands:
            if c["day"] not in stable[c["device"]]:
                continue
            prev = [x for x in run.commands if x["device"] == c["device"] and int(x["effective_at"]) <= int(c["decided_at"])
                    and "%s_%s" % (x["device"], x["seq"]) in run.executed]
            if prev:
                x = prev[-1]
                win = "%s 至 %s 24:00" % (fmt_local(int(x["effective_at"]) - dsec), D.add_days(lday(x["effective_at"]), -1)[5:])
                pv = "%s 命令（%s），生效 %s，重估窗口 %s" % (x["day"][5:], x["triggers"], lday(x["effective_at"])[5:], win)
            else:
                pv = "无（参照为标定周）"
            st_rows.append([n, c["device"], c["day"][5:], c["triggers"], pv])
    w(table(["档", "设备", "命令日", "触发", "上一条已执行命令"], st_rows))
    for v, n in ARMS:
        w("\n<details><summary>%s：逐条命令</summary>\n\n" % n)
        w(table(["设备", "命令日", "生效日", "触发", "触发通道偏离日", "非触发通道偏离日"], detail[v]))
        w("\n</details>\n")

    # ---------------- 判读口径四：点通道离群率
    w("\n## 判读口径四：每条命令生效前后点通道离群率与状态重建瞬态\n\n"
      "离群率为到达滑动步的离群轮数除以进入点通道的轮数。生效前取生效前二十四小时；生效后取生效后第一个小时（状态重建瞬态）与"
      "生效后二十四小时；开环同窗为同一台在开环对照中同一时间窗的离群率（命令不执行时的对照）。表中为逐条命令的中位数与均值。\n\n")
    rows = []
    for v, n in ARMS:
        run = R[v]
        cols = collections.defaultdict(list)
        for c in run.commands:
            if "%s_%s" % (c["device"], c["seq"]) not in run.executed:
                continue
            t, d = int(c["effective_at"]), c["device"]
            for key, rr, t0, t1 in (("pre", run, t - 86400, t), ("h1", run, t, t + 3600), ("post", run, t, t + 86400),
                                    ("open", R["open"], t, t + 86400)):
                x = rr.rate(d, t0, t1)
                if x is not None:
                    cols[key].append(x)

        def stat(xs):
            xs = sorted(xs)
            return "—" if not xs else "%s / %s" % (pct(xs[len(xs) // 2]), pct(sum(xs) / len(xs)))
        rows.append([n, stat(cols["pre"]), stat(cols["h1"]), stat(cols["post"]), stat(cols["open"])])
    w(table(["档", "生效前 24 小时", "生效后第 1 小时", "生效后 24 小时", "开环同窗（生效后 24 小时）"], rows))
    w("\n（每格为「中位数 / 均值」。p_point = %r，即小时离群率超过它时点通道有标志。）\n" % D.derive_p_point(D.Inputs(a.inputs), D.Params()))

    # ---------------- 判读口径五：丙型实例
    w("\n## 判读口径五：丙型实例（命令后回落再命令）\n\n"
      "判别：同一台、同一触发通道的相邻两条命令，前一条与后一条在各自命令日的偏移（当日中位数相对参照）符号相反，即生效后"
      "数据层在新参照上看到反向偏离并以第二条命令映回。「反向进入偏离日」为两条命令之间该通道在新参照上进入偏离的第一个当地日。\n\n")
    for v, n in ARMS:
        tc = type_c(R[v])
        gap = lambda x, y: (int(y["decided_at"]) - int(x["effective_at"])) / 86400
        near = sum(1 for _, _, x, _, _, y, _ in tc if gap(x, y) <= 7)
        w("**%s**：%d 例；其中第二条在第一条生效后七天内决定的 %d 例。\n\n" % (n, len(tc), near))
        if tc:
            w(table(["设备", "通道", "第一条（命令日、偏移）", "生效日", "反向进入偏离日", "第二条（命令日、偏移）", "生效到第二条决定（天）"],
                    [[d, ch, "%s，%+.3g" % (x["day"][5:], sa), lday(x["effective_at"])[5:], back[5:] or "—",
                      "%s，%+.3g" % (y["day"][5:], sb), "%.0f" % gap(x, y)] for d, ch, x, sa, back, y, sb in tc]))
            w("\n")

    # ---------------- 判读口径六：选定判据的两项
    w("## 判读口径六：重估窗口选定判据的两项计数\n\n判据（交接文档第四节，运行前写定）：以命令总数最少且非触发通道连带偏离日数最少为优先，"
      "两者冲突时以连带偏离日数为准；若丙与乙在两项上都相当，取乙。本表只列两项计数，选定由设计会话宣布。\n\n")
    w(table(["档", "命令总数", "执行的命令", "非触发通道连带偏离日数", "其中湿度与气压"],
            [[n, len(R[v].commands), rows3[n][1], rows3[n][4], rows3[n][6]] for v, n in ARMS]))
    out_s = "".join(out)
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write(out_s)
    for v, n in VARIANTS:
        print("%s：命令 %d 条；丙型实例 %s" % (n, len(R[v].commands), len(type_c(R[v])) if v != "open" else "—"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
