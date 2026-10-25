#!/usr/bin/env python3
# ============================================================================
# m5a/run_offline.py
# M5a 离线实验第二步（验收项 V-M5a-2）：读第一步 `eda/m5a_extract.py` 的输出、注入运行的点通道离群记录与性能层的事件清单，按
# 运行前写定的操作口径 `docs/reports/m5a_offline_protocol.md` 算出三族信号的事件、三种统计方案、九条判读口径、时延表、两层同报表与
# 两种参照的对照表，写报告。只计数，不调任何参数；门槛常量注明出处。
# Step 2 of the M5a offline experiment: events of the three signal families, the three statistical schemes, the nine
# protocol items, latency, the two-layer table and the reference comparison. Counting only.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 任意有 python3 的机器，仓库根目录；只用标准库（第三族复用 m5b/optwin_offline.py）。
# 2. 调用命令 / Invocation:
#      python3 m5a/run_offline.py                     # 读 docs/m5a/extract/，写 docs/m5a/ 与 docs/reports/m5a_offline_report.md
#      python3 m5a/run_offline.py --extract-dir /tmp/x --out-dir /tmp/y --report /tmp/y/report.md   # 试运行
# 3. 前置条件 / Preconditions: 第一步的四个 CSV；docs/m3_a4/full_fix/scores.jsonl 与 inject-truth.csv；docs/m5b/tables/ 的默认常量表；
#    docs/m5b/grid/events/ 的默认组合事件清单；平稳日、尺度漂移日、共模时段三份清单。
# 4. 期望产出 / Expected output: docs/m5a/ 下 lm_events.csv、crossings.csv、rate_events_excl.csv、rate_events_inj.csv、schemes.csv、
#    segments.csv、two_layer.csv；报告 docs/reports/m5a_offline_report.md。终端打印各族事件数。
# 5. 失败兜底 / Failure fallback: 退出码 2 表示输入缺失（终端列出缺哪些）。
# ============================================================================
import argparse
import collections
import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "m5b"))
import stats                  # noqa: E402
import optwin_offline as ow   # noqa: E402

try:
    from zoneinfo import ZoneInfo
    LONDON = ZoneInfo("Europe/London")
except Exception:  # noqa: BLE001
    LONDON = None

DEVICES = "ABCDEFGH"
CHANNELS = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]
CN = {"Temperature": "温度", "Humidity": "湿度", "Pressure": "气压", "Gas": "气体", "Light": "光照"}
D0 = {"Temperature": 0.38, "Humidity": 0.73, "Pressure": 0.84, "Gas": 0.43, "Light": 0.51}   # 1.05 版冻结
G0 = {"Temperature": 0.75, "Humidity": 1.57, "Pressure": 1.84, "Gas": 0.77, "Light": 1.24}   # 1.05 版平滑规则重算后冻结
ALPHA, ALPHA_B = 0.01, 0.01 / 5
POS, SCALE = 0.5, 3.0
ATTR_TAIL = 6 * 3600
MARCH = (1647907200, 1648425600)                     # 03-22 00:00 至 03-28 00:00 UTC（第 2 条）
MARCH_DAYS = ["2022-03-%02d" % d for d in range(22, 28)]
C_SUN_DAYS = ("04-17", "04-20", "04-21", "04-24", "04-25")
EVENING = (1650218400, 1650240000)
BST_START = 1648342800


# ---------------------------------------------------------------- 时间 / time
def utc(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%m-%d %H:%M")


def local(ts):
    return datetime.fromtimestamp(ts + (3600 if ts >= BST_START else 0), timezone.utc).strftime("%m-%d %H:%M")


def local_day_of(ts):
    return datetime.fromtimestamp(ts + (3600 if ts >= BST_START else 0), timezone.utc).strftime("%Y-%m-%d")


def utc_day_label(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%m-%d")


def local_day_bounds(day):
    """当地日的世界协调时起止 / UTC bounds of a local day."""
    d = datetime.strptime(day, "%Y-%m-%d")
    start = d.replace(tzinfo=timezone.utc).timestamp()
    end = (d + timedelta(days=1)).replace(tzinfo=timezone.utc).timestamp()
    off = lambda t: 3600 if t >= BST_START + 3600 else 0           # 当地 02:00 起为夏令时 / BST from local 02:00
    return int(start - off(start)), int(end - off(end))


def utc_ts(s):
    return int(datetime.strptime("2022-" + s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp())


# ---------------------------------------------------------------- 读入 / inputs
def read_csv(path):
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def load_extract(d):
    daily = read_csv(os.path.join(d, "daily.csv"))
    for r in daily:
        for k in ("n", "m", "D_bin", "D_exact", "p_ks", "G", "G_df", "G_kl", "median", "p10", "p90", "shift", "width_x"):
            r[k] = fnum(r[k])
        r["p_G"] = stats.chi2_sf(r["G"], int(r["G_df"]))
    quality = {(r["device"], r["variant"], r["day"]): r for r in read_csv(os.path.join(d, "quality.csv"))}
    den = collections.defaultdict(dict)
    for r in read_csv(os.path.join(d, "denominators.csv")):
        den[r["device"]][int(r["bin_end"])] = int(r["admitted"])
    refs = read_csv(os.path.join(d, "refs.csv"))
    return daily, quality, den, refs


def outlier_bins(path):
    """点通道离群记录只取到达滑动步的判定（窗口末 − 60 秒 ≤ 轮时刻 < 窗口末），按十分钟计数（与 M3Grid 的还原规则相同）。"""
    seen, out = set(), collections.defaultdict(lambda: collections.Counter())
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            r = json.loads(line)
            if r.get("channel") != "m2_point" or not r.get("outlier"):
                continue
            rt, we = int(r["roundTs"]), int(r["windowEnd"])
            if not (we - 60 <= rt < we) or (r["device"], rt) in seen:
                continue
            seen.add((r["device"], rt))
            out[r["device"]][(rt // 600) * 600 + 600] += 1
    return out


def load_stable(path):
    st = {d: set() for d in DEVICES}
    for r in read_csv(path):
        for d in DEVICES:
            if r.get("stable_" + d) == "True":
                st[d].add(r["day"])
    return st


def load_scale_days(path):
    out = {d: {} for d in DEVICES}
    mult = {}
    for r in read_csv(path):
        ref = fnum(r["ref_p10_p90_width"])
        if ref > 0 and r["channel"] in CHANNELS:
            mult[(r["device"], r["day"], r["channel"])] = (fnum(r["p90"]) - fnum(r["p10"])) / ref
    for (dv, day, ch), v in mult.items():
        if "03-24" <= day <= "04-30" and v > 3.0:
            out[dv].setdefault(day, []).append("%s %.2f" % (ch, v))
    return out


def load_injections(path):
    return [dict(item="6", name="%s %s %s %s" % (r["channel"], r["type"], r["magnitude"], utc(int(r["start_ts"]))), device=r["device"],
                 channel=r["channel"], start=int(r["start_ts"]), end=int(r["end_ts"])) for r in read_csv(path)]


def load_m5b_events(path):
    ev = []
    for r in read_csv(path):
        ev.append(dict(device=r["device"], ts=int(r["ts"]), level=r["level"], test=r["test"],
                       candidate_ts=int(r["candidate_ts"]) if r["candidate_ts"] else int(r["ts"])))
    return ev


# ---------------------------------------------------------------- 三族信号 / the three families
def evaluable(quality, dev, var, day):
    q = quality.get((dev, var, day))
    return q is not None and q["evaluable"] == "1"


def lm_events(daily, quality, var):
    """长记忆事件（主方案）：p < 0.002 且 D > D 零 / long-memory events."""
    return [r for r in daily if r["variant"] == var and r["daykind"] == "local" and evaluable(quality, r["device"], var, r["day"])
            and r["p_ks"] < ALPHA_B and r["D_bin"] > D0[r["channel"]]]


def crossings(daily, quality, var):
    out = []
    for r in daily:
        if r["variant"] != var or r["daykind"] != "local" or not evaluable(quality, r["device"], var, r["day"]):
            continue
        if abs(r["shift"]) >= POS:
            out.append(dict(r, kind="位置"))
        if r["width_x"] > SCALE:
            out.append(dict(r, kind="尺度"))
    return out


def rate_stream(num, den, injections, exclude):
    rows = []
    for dev in sorted(den):
        for b in sorted(den[dev]):
            a = den[dev][b]
            if a <= 0:
                continue
            if exclude and any(s["device"] == dev and b > s["start"] and b - 600 < s["end"] + 600 for s in injections):
                continue
            rows.append((dev, b, num.get(dev, {}).get(b, 0) / a))
    return rows


def scheme_alarm(r, scheme):
    if scheme == "甲":
        return r["p_ks"] < ALPHA
    if scheme == "乙":
        return r["p_ks"] < ALPHA_B and r["D_bin"] > D0[r["channel"]]
    if scheme == "乙′":
        return r["p_ks"] < ALPHA_B
    if scheme == "丙":
        return r["p_G"] < ALPHA_B and r["G_kl"] > G0[r["channel"]]
    if scheme == "丙′":
        return r["p_G"] < ALPHA_B
    raise ValueError(scheme)


SCHEMES = ("甲", "乙", "乙′", "丙", "丙′")


# ---------------------------------------------------------------- 归属 / attribution
def attributed(ts, seg):
    return seg["start"] <= ts <= seg["end"] + ATTR_TAIL


def seg_rate(seg, events):
    dev = seg["device"]
    c = [e for e in events if e["device"] == dev and e["level"] == "CANDIDATE" and attributed(e["ts"], seg)]
    f = [e for e in events if e["device"] == dev and e["level"] == "CONFIRM" and attributed(e["candidate_ts"], seg)]
    fc = min(c, key=lambda e: e["ts"]) if c else None
    ff = min(f, key=lambda e: e["ts"]) if f else None
    return dict(cand=len(c), first_cand=fc["ts"] if fc else 0, first_cand_test=fc["test"] if fc else "",
                confirm=len(f), first_confirm=ff["ts"] if ff else 0, first_confirm_test=ff["test"] if ff else "")


def seg_days(seg):
    """与段有交集的当地日 / local days intersecting the segment."""
    days, t = set(), seg["start"]
    while t <= seg["end"]:
        days.add(local_day_of(t))
        t += 3600
    days.add(local_day_of(seg["end"]))
    return sorted(days)


def day_end_ts(day):
    return local_day_bounds(day)[1]


# ---------------------------------------------------------------- 主程序 / main
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--extract-dir", default="docs/m5a/extract")
    ap.add_argument("--scores", default="docs/m3_a4/full_fix/scores.jsonl")
    ap.add_argument("--inject", default="docs/m3_a4/full_fix/inject-truth.csv")
    ap.add_argument("--table", default="docs/m5b/tables/thesis_0.99_0.5_2016.csv")
    ap.add_argument("--m5b-events", default="docs/m5b/grid/events/c0.99_r0.5_10min_k36.csv")
    ap.add_argument("--m5b-events-inj", default="docs/m5b/grid/events/c0.99_r0.5_10min_k36_with_injection.csv")
    ap.add_argument("--stable", default="docs/m3_inject/v34/v34_daily.csv")
    ap.add_argument("--profile", default="docs/m3_marapr/daily_channel_profile_ref0308.csv")
    ap.add_argument("--episodes", default="docs/fleet_episodes/episodes.csv")
    ap.add_argument("--out-dir", default="docs/m5a")
    ap.add_argument("--report", default="docs/reports/m5a_offline_report.md")
    a = ap.parse_args()
    need = [os.path.join(a.extract_dir, f) for f in ("daily.csv", "quality.csv", "denominators.csv", "refs.csv")]
    need += [a.scores, a.inject, a.table, a.m5b_events, a.m5b_events_inj, a.stable, a.profile, a.episodes]
    miss = [f for f in need if not os.path.exists(f)]
    if miss:
        print("ERROR: 缺少输入：\n  " + "\n  ".join(miss), file=sys.stderr)
        return 2

    daily, quality, den, refs = load_extract(a.extract_dir)
    injections = load_injections(a.inject)
    stable, scale = load_stable(a.stable), load_scale_days(a.profile)
    num = outlier_bins(a.scores)
    table = ow.load_table(a.table)
    rate_ev = {}
    rate_elems = {}
    for var, excl in (("excl", True), ("inj", False)):
        rows = rate_stream(num, den, injections, excl)
        rate_elems[var] = collections.defaultdict(list)
        for dv, b, _ in rows:
            rate_elems[var][dv].append(b)
        rate_ev[var] = ow.run_stream(rows, table, 36)
    m5b = {"excl": load_m5b_events(a.m5b_events), "inj": load_m5b_events(a.m5b_events_inj)}
    lm = {v: lm_events(daily, quality, v) for v in ("excl", "inj")}
    cr = {v: crossings(daily, quality, v) for v in ("excl", "inj")}
    print("长记忆事件 %d 条（剔除）/ %d 条（不剔除）；越门槛 %d / %d；离群率事件 %d / %d" % (
        len(lm["excl"]), len(lm["inj"]), len(cr["excl"]), len(cr["inj"]), len(rate_ev["excl"]), len(rate_ev["inj"])))

    os.makedirs(a.out_dir, exist_ok=True)
    keys = ["variant", "device", "day", "channel", "n", "D_bin", "D_exact", "p_ks", "G_kl", "p_G", "shift", "width_x"]
    write(os.path.join(a.out_dir, "lm_events.csv"), [{k: r[k] for k in keys} for v in lm for r in lm[v]])
    write(os.path.join(a.out_dir, "crossings.csv"), [dict({k: r[k] for k in keys}, kind=r["kind"]) for v in cr for r in cr[v]])
    for var in rate_ev:
        ow.write_events(rate_ev[var], os.path.join(a.out_dir, "rate_events_%s.csv" % var))

    # 片段 / segments
    episodes = []
    for r in read_csv(a.episodes):
        episodes.append(dict(name="时段 %s（%s）" % (r["episode"], r["devices"]), start=utc_ts(r["first_candidate_utc"]) - 12 * 3600,
                             end=utc_ts(r["last_confirm_utc"])))
    segs = [dict(item="2", name="三月事件", device=d, start=MARCH[0], end=MARCH[1]) for d in DEVICES]
    for e in episodes:
        segs += [dict(item="4", name=e["name"], device=d, start=e["start"], end=e["end"]) for d in DEVICES]
    for day in C_SUN_DAYS:
        t0 = int(datetime.strptime("2022-" + day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        segs.append(dict(item="5", name="C 日照直射 %s 11–14 时（UTC）" % day, device="C", start=t0 + 11 * 3600, end=t0 + 14 * 3600))
    segs += [dict(item="5", name="04-17 傍晚气体上升", device=d, start=EVENING[0], end=EVENING[1]) for d in DEVICES]
    inj_segs = injections
    for d in DEVICES:
        for day, why in sorted(scale[d].items()):
            t0 = int(datetime.strptime("2022-" + day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
            segs.append(dict(item="3", name="尺度漂移日 " + day, device=d, start=t0, end=t0 + 86400, why="、".join(why)))
    for d in DEVICES:
        scale[d] = {k: "、".join(v) for k, v in scale[d].items()}

    R = Report(a, daily, quality, refs, stable, scale, lm, cr, rate_ev, rate_elems, m5b, segs, inj_segs, episodes)
    R.write()
    write(os.path.join(a.out_dir, "segments.csv"), R.seg_rows)
    write(os.path.join(a.out_dir, "two_layer.csv"), R.two_layer_rows)
    write(os.path.join(a.out_dir, "schemes.csv"), R.scheme_rows)
    print("报告 → %s；长表 → %s/" % (a.report, a.out_dir))
    return 0


def write(path, rows):
    if not rows:
        open(path, "w").close()
        return
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def f2(x, nd=2):
    return "—" if x != x else ("%.*f" % (nd, x))


class Report:
    def __init__(self, a, daily, quality, refs, stable, scale, lm, cr, rate_ev, rate_elems, m5b, segs, inj_segs, episodes):
        self.a, self.daily, self.quality, self.refs = a, daily, quality, refs
        self.stable, self.scale, self.lm, self.cr = stable, scale, lm, cr
        self.rate_ev, self.rate_elems, self.m5b = rate_ev, rate_elems, m5b
        self.segs, self.inj_segs, self.episodes = segs, inj_segs, episodes
        self.seg_rows, self.two_layer_rows, self.scheme_rows = [], [], []
        self.L = []

    def p(self, s=""):
        self.L.append(s)

    def table(self, head, rows):
        self.p("| " + " | ".join(head) + " |")
        self.p("|" + " --- |" * len(head))
        for r in rows:
            self.p("| " + " | ".join(str(x) for x in r) + " |")
        self.p()

    def stable_local_days(self, d):
        return {"2022-" + x for x in self.stable[d]}

    # ------------------------------------------------------------ 各节
    def write(self):
        p = self.p
        p("# M5a 离线实验结果（V-M5a-2）")
        p()
        p("由 `m5a/run_offline.py` 生成（%s）。口径：`docs/reports/m5a_offline_protocol.md`。第一、二族按当地日；第三族按世界协调时日期归属"
          "平稳日与尺度漂移日。除第 6 条外均为剔除注入的一遍。长表在 `%s/`。" % (datetime.now().strftime("%Y-%m-%d %H:%M"), self.a.out_dir))
        p()
        self.thresholds()
        self.quality_section()
        self.item1()
        self.item2()
        self.item3()
        self.item4()
        self.item5()
        self.item6()
        self.item7()
        self.item8()
        self.item9()
        self.latency()
        self.refs_section()
        self.deviations()
        self.timeline()
        with open(self.a.report, "w", encoding="utf-8") as fh:
            fh.write("\n".join(self.L) + "\n")

    def thresholds(self):
        self.p("## 〇、冻结的门槛（1.05 版）")
        self.p()
        self.table(["通道", "D 零", "G 零（G ÷ 2n）"], [(CN[c], D0[c], G0[c]) for c in CHANNELS])
        errs = [abs(r["D_bin"] - r["D_exact"]) for r in self.daily if r["variant"] == "excl" and r["daykind"] == "local"]
        self.p("显著性 0.01，五通道邦费罗尼校正为 0.002。分箱 D 与精确 D 之差：最大 %s，中位数 %s（%d 个设备日通道）。G ÷ 2n 带正偏差，约为"
               "（非空箱数 − 1）÷ 2n，不做校正。" % (f2(max(errs), 4) if errs else "—", f2(sorted(errs)[len(errs) // 2], 4) if errs else "—", len(errs)))
        self.p()

    def quality_section(self):
        self.p("## 〇之二、质量门控：不可判的设备日")
        self.p()
        rows = [r for r in self.quality.values() if r["evaluable"] != "1" and r["variant"] == "excl" and r["reason"] != "标定日"]
        rows.sort(key=lambda r: (r["device"], r["day"]))
        self.table(["设备", "日（当地）", "原因", "缺轮比例", "守卫打标比例"],
                   [(r["device"], r["day"], r["reason"], r["missing_frac"], r["guard_frac"]) for r in rows] or [("—",) * 5])
        inj_only = [r for r in self.quality.values() if r["variant"] == "inj" and r["evaluable"] != "1"
                    and self.quality.get((r["device"], "excl", r["day"]), {}).get("evaluable") == "1"]
        self.p("只在不剔除注入的一遍中不可判的设备日：%s。03-08 为标定日，全部不可判。" % (
            "；".join("%s %s（%s）" % (r["device"], r["day"], r["reason"]) for r in inj_only) or "无"))
        self.p()

    def item1(self):
        p = self.p
        p("## 一、平稳日上的事件数（按通道分列）")
        p()
        rows, unexpected = [], []
        for d in DEVICES:
            sd = self.stable_local_days(d)
            ne = sum(1 for day in sd if not evaluable(self.quality, d, "excl", day))
            lm = [r for r in self.lm["excl"] if r["device"] == d and r["day"] in sd]
            pos = [r for r in self.cr["excl"] if r["device"] == d and r["day"] in sd and r["kind"] == "位置"]
            sca = [r for r in self.cr["excl"] if r["device"] == d and r["day"] in sd and r["kind"] == "尺度"]
            rc = [e for e in self.rate_ev["excl"] if e["device"] == d and utc_day_label(e["ts"]) in self.stable[d]]
            per = lambda xs: "、".join("%s %d" % (CN[c], sum(1 for r in xs if r["channel"] == c)) for c in CHANNELS
                                       if any(r["channel"] == c for r in xs)) or "0"
            rows.append((d, len(sd), ne, per(lm), per(pos), per(sca), sum(e["level"] == "CONFIRM" for e in rc),
                         sum(e["level"] == "CANDIDATE" for e in rc)))
            unexpected += [r for r in lm if r["channel"] in ("Temperature", "Gas")]
        self.table(["设备", "平稳日", "其中不可判", "长记忆事件（按通道）", "越位置门槛（按通道）", "越尺度门槛（按通道）", "离群率确认", "离群率候选"], rows)
        g_light = [r for r in self.lm["excl"] if r["device"] == "G" and r["channel"] == "Light" and r["day"] in self.stable_local_days("G")]
        p("**分类说明**（运行前写定）：")
        p()
        p("- G 的光照（参照污染，数据事实第三十四条，不计入未预期）：平稳日上长记忆事件 %d 条。" % len(g_light))
        others = collections.Counter(r["day"] for r in self.lm["excl"] if r["channel"] == "Light" and r["device"] != "G"
                                     and r["day"] in self.stable_local_days(r["device"]))
        p("- 其余设备的光照（季节趋势，理论回传第十五条）：平稳日上长记忆事件 %d 条，按日：%s。" % (
            sum(others.values()), "；".join("%s %d 台" % (k[5:], v) for k, v in sorted(others.items())) or "无"))
        for ch in ("Pressure", "Humidity"):
            c = collections.Counter(r["day"] for r in self.lm["excl"] if r["channel"] == ch and r["day"] in self.stable_local_days(r["device"]))
            p("- %s（天气驱动、全机队同步，理论回传第二十一条）：平稳日上长记忆事件 %d 条；同一天报警的设备数：%s。" % (
                CN[ch], sum(c.values()), "；".join("%s %d 台" % (k[5:], v) for k, v in sorted(c.items())) or "无"))
        p("- 温度与气体（未预期的平稳日事件）：%d 条%s" % (len(unexpected), "。" if not unexpected else "："))
        p()
        if unexpected:
            self.table(["设备", "日", "通道", "D（分箱）", "D 零", "G ÷ 2n", "偏移", "宽度倍数"],
                       [(r["device"], r["day"], CN[r["channel"]], f2(r["D_bin"], 3), D0[r["channel"]], f2(r["G_kl"], 3), f2(r["shift"]),
                         f2(r["width_x"])) for r in sorted(unexpected, key=lambda r: (r["device"], r["day"]))])

    def first_day(self, rows, dev, ch, days, kind=None):
        xs = sorted(r["day"] for r in rows if r["device"] == dev and r["channel"] == ch and r["day"] in days
                    and (kind is None or r["kind"] == kind))
        return xs[0][5:] if xs else "—"

    def item2(self):
        self.p("## 二、三月事件（03-22 至 03-27）")
        self.p()
        rows = []
        for d in DEVICES:
            for c in CHANNELS:
                rows.append((d, CN[c], self.first_day(self.lm["excl"], d, c, MARCH_DAYS), self.first_day(self.cr["excl"], d, c, MARCH_DAYS, "位置"),
                             self.first_day(self.cr["excl"], d, c, MARCH_DAYS, "尺度")))
        self.table(["设备", "通道", "第一次长记忆事件", "第一次越位置门槛", "第一次越尺度门槛"], rows)
        rows = []
        for s in [x for x in self.segs if x["item"] == "2"]:
            st = seg_rate(s, self.rate_ev["excl"])
            rows.append((s["device"], "%s（%s）" % (utc(st["first_cand"]), st["first_cand_test"]) if st["first_cand"] else "—",
                         "%s（%s）" % (utc(st["first_confirm"]), st["first_confirm_test"]) if st["first_confirm"] else "—", st["cand"], st["confirm"]))
            self.seg_rows.append(dict(item="2", segment=s["name"], device=s["device"], **st))
        self.p("离群率（归属于 [03-22 00:00, 03-28 00:00) UTC 加 6 小时尾长）：")
        self.p()
        self.table(["设备", "第一个候选（UTC，检验）", "第一次确认（UTC，检验）", "候选数", "确认数"], rows)

    def item3(self):
        self.p("## 三、尺度漂移日：逐日统计量的宽度倍数与长记忆事件")
        self.p()
        cat = collections.Counter()
        rows = []
        for d in DEVICES:
            for day, why in sorted(self.scale[d].items()):
                full = "2022-" + day
                if not evaluable(self.quality, d, "excl", full):
                    rows.append((d, day, why, "不可判", "", ""))
                    continue
                wx = {r["channel"]: r["width_x"] for r in self.daily if r["variant"] == "excl" and r["daykind"] == "local"
                      and r["device"] == d and r["day"] == full}
                over = any(v > SCALE for v in wx.values())
                lmc = [CN[r["channel"]] for r in self.lm["excl"] if r["device"] == d and r["day"] == full]
                cat[("越尺度" if over else "未越尺度", "有长记忆" if lmc else "无长记忆")] += 1
                rows.append((d, day, why, "、".join("%s %s" % (CN[c], f2(wx.get(c, float("nan")))) for c in CHANNELS),
                             "是" if over else "否", "、".join(lmc) or "无"))
        self.table(["设备", "日", "已登记口径的超标通道", "本阶段参照的宽度倍数", "越尺度门槛", "长记忆事件"], rows)
        self.p("汇总（设备日）：越尺度且有长记忆 %d；只越尺度 %d；只有长记忆 %d；两者都没有 %d。" % (
            cat[("越尺度", "有长记忆")], cat[("越尺度", "无长记忆")], cat[("未越尺度", "有长记忆")], cat[("未越尺度", "无长记忆")]))
        self.p()

    def item4(self):
        self.p("## 四、四月五个共模气体时段")
        self.p()
        rows = []
        for e in self.episodes:
            seg = dict(start=e["start"], end=e["end"])
            days = seg_days(seg)
            per_day = collections.Counter(r["day"] for r in self.lm["excl"] if r["channel"] == "Gas" and r["day"] in days)
            for d in DEVICES:
                s = dict(seg, device=d, item="4", name=e["name"])
                st = seg_rate(s, self.rate_ev["excl"])
                self.seg_rows.append(dict(item="4", segment=e["name"], device=d, **st))
                lmd = sorted(r["day"][5:] for r in self.lm["excl"] if r["device"] == d and r["channel"] == "Gas" and r["day"] in days)
                crd = sorted("%s %s" % (r["day"][5:], r["kind"]) for r in self.cr["excl"] if r["device"] == d and r["channel"] == "Gas" and r["day"] in days)
                rows.append((e["name"], d, "、".join(lmd) or "无", "、".join(crd) or "无",
                             utc(st["first_cand"]) if st["first_cand"] else "—", utc(st["first_confirm"]) if st["first_confirm"] else "—"))
            rows.append((e["name"], "同日气体长记忆报警的设备数", "；".join("%s %d 台" % (k[5:], v) for k, v in sorted(per_day.items())) or "无", "", "", ""))
        self.table(["时段", "设备", "气体长记忆事件（当地日）", "气体越门槛", "离群率第一个候选（UTC）", "离群率第一次确认（UTC）"], rows)

    def item5(self):
        self.p("## 五、C 的日照直射与 04-17 傍晚气体上升")
        self.p()
        rows = []
        for s in [x for x in self.segs if x["item"] == "5"]:
            st = seg_rate(s, self.rate_ev["excl"])
            self.seg_rows.append(dict(item="5", segment=s["name"], device=s["device"], **st))
            days = seg_days(s)
            chs = ("Light", "Temperature") if s["name"].startswith("C 日照") else ("Gas",)
            lmc = sorted("%s %s" % (r["day"][5:], CN[r["channel"]]) for r in self.lm["excl"] if r["device"] == s["device"]
                         and r["channel"] in chs and r["day"] in days)
            crc = sorted("%s %s %s" % (r["day"][5:], CN[r["channel"]], r["kind"]) for r in self.cr["excl"] if r["device"] == s["device"]
                         and r["channel"] in chs and r["day"] in days)
            rows.append((s["name"], s["device"], "、".join(lmc) or "无", "、".join(crc) or "无", st["cand"], st["confirm"]))
        self.table(["段", "设备", "长记忆事件", "越门槛", "离群率候选", "离群率确认"], rows)

    def item6(self):
        self.p("## 六、注入（不剔除的一遍）")
        self.p()
        rows = []
        for s in self.inj_segs:
            st = seg_rate(s, self.rate_ev["inj"])
            self.seg_rows.append(dict(item="6", segment=s["name"], device=s["device"], **st))
            day = local_day_of(s["start"])
            cell = lambda var: "、".join(sorted(
                ["长记忆"] * any(r["device"] == s["device"] and r["channel"] == s["channel"] and r["day"] == day for r in self.lm[var])
                + [r["kind"] for r in self.cr[var] if r["device"] == s["device"] and r["channel"] == s["channel"] and r["day"] == day])) or "无"
            ok = lambda var: "" if evaluable(self.quality, s["device"], var, day) else "（不可判）"
            rows.append((s["name"], st["cand"], st["confirm"], cell("inj") + ok("inj"), cell("excl") + ok("excl")))
        self.table(["注入段", "离群率候选", "离群率确认", "注入当日（不剔除）", "注入当日（剔除）"], rows)

    def item7(self):
        self.p("## 七、夏令时切换日：按当地日与按世界协调时日的长记忆")
        self.p()
        by = {(r["device"], r["channel"], r["daykind"], r["day"]): r for r in self.daily if r["variant"] == "excl"}
        rows = []
        for day in ("2022-03-25", "2022-03-26", "2022-03-27", "2022-03-28", "2022-03-29"):
            for c in CHANNELS:
                diffs, ev_l, ev_u = [], 0, 0
                for d in DEVICES:
                    l, u = by.get((d, c, "local", day)), by.get((d, c, "utc", day))
                    if l and u:
                        diffs.append(abs(l["D_bin"] - u["D_bin"]))
                        ev_l += l["p_ks"] < ALPHA_B and l["D_bin"] > D0[c]
                        ev_u += u["p_ks"] < ALPHA_B and u["D_bin"] > D0[c]
                rows.append((day[5:], CN[c], f2(max(diffs), 4) if diffs else "—", f2(sum(diffs) / len(diffs), 4) if diffs else "—", ev_l, ev_u))
        self.table(["日", "通道", "两种日界 D 之差的最大值", "平均值", "当地日事件台数", "世界协调时日事件台数"], rows)
        q = [r for r in self.quality.values() if r["day"] == "2022-03-27" and r["variant"] == "excl"]
        self.p("03-27 当地日应有轮数 %s（23 小时）。" % (q[0]["expected"] if q else "—"))
        self.p()

    def item8(self):
        self.p("## 八、两层同报表")
        self.p()
        self.p("时刻为世界协调时；长记忆与越门槛以当地日结束的时刻计（在线算子在日界处发出）。性能层为默认组合的事件。")
        self.p()
        rows = []
        segs = [s for s in self.segs if s["item"] in ("2", "3", "4", "5")] + list(self.inj_segs)
        for s in segs:
            var = "inj" if s["item"] == "6" else "excl"
            items = []
            for e in self.m5b[var]:
                if e["device"] != s["device"]:
                    continue
                if e["level"] == "CANDIDATE" and attributed(e["ts"], s):
                    items.append((e["ts"], "性能层候选（%s）" % e["test"]))
                if e["level"] == "CONFIRM" and attributed(e["candidate_ts"], s):
                    items.append((e["ts"], "性能层确认（%s）" % e["test"]))
            for e in self.rate_ev[var]:
                if e["device"] != s["device"]:
                    continue
                if e["level"] == "CANDIDATE" and attributed(e["ts"], s):
                    items.append((e["ts"], "离群率候选（%s）" % e["test"]))
                if e["level"] == "CONFIRM" and attributed(e["candidate_ts"], s):
                    items.append((e["ts"], "离群率确认（%s）" % e["test"]))
            days = seg_days(s)
            for r in self.lm[var]:
                if r["device"] == s["device"] and r["day"] in days:
                    items.append((day_end_ts(r["day"]), "长记忆 %s" % CN[r["channel"]]))
            for r in self.cr[var]:
                if r["device"] == s["device"] and r["day"] in days:
                    items.append((day_end_ts(r["day"]), "越%s %s" % (r["kind"], CN[r["channel"]])))
            items.sort()
            text = "；".join("%s %s" % (utc(t), x) for t, x in items) or "无"
            self.two_layer_rows.append(dict(item=s["item"], segment=s["name"], device=s["device"], sequence=text))
            rows.append((s["item"], s["name"], s["device"], text))
        self.table(["条", "段", "设备", "按时刻排序的报警"], rows)

    def item9(self):
        self.p("## 九、理论第五项验证：三种统计方案")
        self.p()
        by_dd = collections.defaultdict(list)
        for r in self.daily:
            if r["variant"] == "excl" and r["daykind"] == "local" and evaluable(self.quality, r["device"], "excl", r["day"]):
                by_dd[(r["device"], r["day"])].append(r)
        rows = []
        for sc in SCHEMES:
            alarm = {k for k, rs in by_dd.items() if any(scheme_alarm(r, sc) for r in rs)}
            st_n = sum(1 for (d, day) in alarm if day in self.stable_local_days(d))
            st_tot = sum(1 for (d, day) in by_dd if day in self.stable_local_days(d))
            march = []
            for d in DEVICES:
                xs = sorted(day for (dv, day) in alarm if dv == d and day in MARCH_DAYS)
                march.append("%s %s" % (d, xs[0][5:] if xs else "—"))
            ep = []
            for e in self.episodes:
                days = seg_days(dict(start=e["start"], end=e["end"]))
                gas = collections.Counter(day for (d, day), rs in by_dd.items() if day in days and any(r["channel"] == "Gas" and scheme_alarm(r, sc) for r in rs))
                ep.append(max(gas.values()) if gas else 0)
            rows.append((sc, "%d / %d" % (st_n, st_tot), "；".join(march), " / ".join(str(x) for x in ep)))
            self.scheme_rows.append(dict(scheme=sc, stable_alarm=st_n, stable_total=st_tot, march="；".join(march), episodes_gas_max=" / ".join(map(str, ep))))
        self.table(["方案", "平稳日报警的设备日 / 可判的平稳设备日", "三月事件第一次报警（当地日）", "五个共模时段同日气体报警设备数的最大值"], rows)
        self.p("方案：甲 = p < 0.01 无效应量门槛；乙 = p < 0.002 且 D > D 零（主方案）；乙′ = 只有 p < 0.002；丙 = G 检验 p < 0.002 且 G ÷ 2n > G 零；"
               "丙′ = 只有 G 检验 p < 0.002。平稳日一栏含 G 的光照等第一条所列的各类，分类见第一条。")
        self.p()

    def latency(self):
        self.p("## 十、时延表（验收项四）：三月事件起点 03-22 00:00 UTC 到各族第一次报警")
        self.p()
        rows = []
        for d in DEVICES:
            lmd = sorted(r["day"] for r in self.lm["excl"] if r["device"] == d and r["day"] >= "2022-03-22")
            crd = sorted(r["day"] for r in self.cr["excl"] if r["device"] == d and r["day"] >= "2022-03-22")
            dd = lambda xs: "%s（第 %d 天）" % (xs[0][5:], (datetime.strptime(xs[0], "%Y-%m-%d") - datetime(2022, 3, 22)).days + 1) if xs else "—"
            s = dict(start=MARCH[0], end=MARCH[1], device=d)
            st = seg_rate(s, self.rate_ev["excl"])
            el = self.rate_elems["excl"].get(d, [])
            cnt = lambda t: sum(1 for b in el if MARCH[0] < b <= t)
            mn = lambda t: "%d 分钟 / %d 个元素" % ((t - MARCH[0]) // 60, cnt(t)) if t else "—"
            rows.append((d, dd(lmd), dd(crd), mn(st["first_cand"]), mn(st["first_confirm"])))
        self.table(["设备", "第一次长记忆事件", "第一次越门槛", "离群率第一个候选", "离群率第一次确认"], rows)

    def refs_section(self):
        self.p("## 十一、两种参照的对照（1.03 版）")
        self.p()
        rows = []
        for r in self.refs:
            w, ow_ = fnum(r["p90"]) - fnum(r["p10"]), fnum(r["old_p90"]) - fnum(r["old_p10"])
            rows.append((r["device"], CN[r["channel"]], f2(fnum(r["median"]), 3), f2(w, 3), f2(fnum(r["old_median"]), 3), f2(ow_, 3),
                         f2(ow_ / w, 2) if w > 0 else "—"))
        self.table(["设备", "通道", "本阶段参照中位数", "本阶段参照宽度", "已登记参照中位数", "已登记参照宽度", "宽度之比（已登记 ÷ 本阶段）"], rows)
        self.p("数值在变换域（光照为 log1p）。本阶段参照为标定窗口（03-01 至冻结），已登记口径的参照为 03-08 至 03-17。")


    def deviations(self):
        p = self.p
        p("## 十二、与口径或交接文档预期不符的实测（只报事实）")
        p()
        ev = self.rate_ev["excl"]
        lv = collections.Counter(e["level"] for e in ev)
        elems = sum(len(v) for v in self.rate_elems["excl"].values())
        rv = [e for e in ev if e["level"] == "REVOKE"]
        p("1. **离群率一族全程没有确认。** 剔除注入的一遍共 %d 个十分钟元素，候选 %d 条、确认 %d 条、撤销 %d 条；撤销中 %d 条在候选之后第 6 个元素即撤销，"
          "%d 条撤销时最近六个元素的中位数为 0。不剔除的一遍候选 %d 条、确认 %d 条。交接文档第四节第 2 条预期「起点由离群率先报」，第 6 条预期"
          "「离群率对尖峰与阶跃候选、持续性核对不确认」。" % (
              elems, lv["CANDIDATE"], lv["CONFIRM"], lv["REVOKE"], sum(e["checked"] == 6 for e in rv), sum(e["last_median"] == 0 for e in rv),
              sum(e["level"] == "CANDIDATE" for e in self.rate_ev["inj"]), sum(e["level"] == "CONFIRM" for e in self.rate_ev["inj"])))
        errs = sorted(((abs(r["D_bin"] - r["D_exact"]), r) for r in self.daily if r["variant"] == "excl" and r["daykind"] == "local"),
                      key=lambda x: -x[0])
        viol = sum(1 for e, r in errs if e > max(fnum(r.get("max_cell_mass_day")), 1.0 / 400) + 1e-12)
        big = [x for x in errs if x[0] > 0.005]
        p("2. **分箱 D 的误差在少数设备日上大于参照核查时的值。** 参照核查（标定日）的最大误差为 0.0019；实验日上有 %d 个设备日通道的误差超过 0.005，"
          "最大 %s（%s）。这些设备日当天落在单个箱里的读数比例为 %s；全部设备日通道中误差超过「当日单箱最大质量与参照单箱质量 1/400 的"
          "较大者」的有 %d 个。" % (
              len(big), f2(errs[0][0], 4) if errs else "—",
              "、".join("%s %s %s" % (r["device"], r["day"][5:], CN[r["channel"]]) for _, r in big[:3]),
              "、".join(f2(fnum(r.get("max_cell_mass_day")), 3) for _, r in big[:3]), viol))
        reg, now = [], []
        for d in DEVICES:
            for day, why in self.scale[d].items():
                for item in why.split("、"):
                    if item.startswith("Light"):
                        reg.append(float(item.split()[1]))
                        now += [r["width_x"] for r in self.daily if r["variant"] == "excl" and r["daykind"] == "local" and r["device"] == d
                                and r["day"] == "2022-" + day and r["channel"] == "Light"]
        p("3. **已登记的尺度漂移日清单中光照的条目与本阶段不可比。** 已登记口径在原始光照值上算宽度倍数，本阶段在 log1p 域上算；已登记清单中因光照"
          "超标的 %d 个设备日（已登记口径 %s 至 %s 倍），在本阶段参照下光照宽度倍数为 %s 至 %s。" % (
              len(reg), f2(min(reg)) if reg else "—", f2(max(reg)) if reg else "—", f2(min(now)) if now else "—", f2(max(now)) if now else "—"))
        pre = collections.Counter(r["channel"] for r in self.lm["excl"] if r["day"] < "2022-03-22")
        p("4. **三月事件之前已有长记忆事件。** 03-09 至 03-21 共有 %s 条长记忆事件；第二条表中「第一次」只在 03-22 至 03-27 内取，不表示事件的起点。"
          "逐日的设备数见附表。" % "、".join("%s %d" % (CN[c], pre[c]) for c in CHANNELS if pre[c]))
        st = collections.Counter(r["channel"] for r in self.lm["excl"] if r["day"] in self.stable_local_days(r["device"]))
        alarms = sum(1 for d in DEVICES for day in self.stable_local_days(d) if evaluable(self.quality, d, "excl", day)
                     and any(r["device"] == d and r["day"] == day for r in self.lm["excl"]))
        total = sum(1 for d in DEVICES for day in self.stable_local_days(d) if evaluable(self.quality, d, "excl", day))
        p("5. **平稳日上的长记忆事件远多于交接文档的理论预期（零）。** 可判的平稳设备日 %d 个，其中 %d 个至少有一个通道的长记忆事件；按通道：%s。"
          "分类按第一条（运行前写定）。" % (total, alarms, "、".join("%s %d" % (CN[c], st[c]) for c in CHANNELS if st[c])))
        sh = {c: [r["shift"] for r in self.lm["excl"] if r["channel"] == c] for c in CHANNELS}
        p("6. **长记忆事件的方向。** 全程（剔除注入）各通道事件中偏移为正的比例：%s。" % "、".join(
            "%s %d / %d" % (CN[c], sum(x > 0 for x in v), len(v)) for c, v in sh.items() if v))
        p()

    def timeline(self):
        self.p("## 附：逐日长记忆事件的设备数（剔除注入的一遍）")
        self.p()
        days = sorted({r["day"] for r in self.daily if r["daykind"] == "local"})
        ev = collections.Counter((r["channel"], r["day"]) for r in self.lm["excl"])
        ok = collections.Counter(day for (d, v, day), q in self.quality.items() if v == "excl" and q["evaluable"] == "1")
        self.table(["当地日", "温度", "湿度", "气压", "气体", "光照", "可判设备数"],
                   [(day[5:],) + tuple(ev[(c, day)] for c in CHANNELS) + (ok[day],) for day in days])


if __name__ == "__main__":
    sys.exit(main())
