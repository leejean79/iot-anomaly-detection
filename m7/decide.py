#!/usr/bin/env python3
# ============================================================================
# m7/decide.py
# 聚合模块决策函数的 Python 参考实现（交接文档 `handover_m7_decision_stage_zh.md` 1.23 版第四节）：读 `m7/prepare_inputs.py` 整理的
# 七张表，逐个当地小时做小时节拍（五个标志、机队计数、六级归因），逐个当地日界做日节拍（时间形态、决策矩阵、冷却期与同一段偏离
# 只发一次）与日界复核，写告警、命令、复核三份日志。Java 离线运行器读同一份输入，日志须逐条相同（验收项一）。
# Python reference of the M7 decision function: hourly alerts, daily level-0 commands and the daily review log.
#
# 交接文档未写死、此处选定的写法（报告中逐条列出）：
#   a. 质量问题告警的范围：按有读数的设备数计门槛；分布切换的范围：按有爆发标志的设备数计门槛；
#   b. 当日不可判时，数据层两条日规则在该日不成立（没有新的证据），状态仍按数据层的规则延续；
#   c. 一台设备同时有多个触发信号时，其中任一回落即结束「同一段偏离」与「漂移」状态；
#   d. 复核只用发出时刻不晚于日节拍触发时刻（日界后六十秒）的爆发，与在线一致。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation:
#      python3 m7/decide.py [--inputs docs/m7/inputs] [--out-dir docs/m7/run] [--no-once] [--layers data,perf,detect]
#        [--common-ratio 0.75] [--k-site 3]
#    --no-once 关闭「同一段偏离只发一次」（对照跑）；--layers 只用部分信号（消融）；其余参数见 Params。
# 3. 前置条件 / Preconditions: 已运行 m7/prepare_inputs.py。
# 4. 期望产出 / Expected output: out-dir 下 alerts.csv、commands.csv、review.csv、params.json；终端打印三份日志的条数与按归因的分布。
# 5. 失败兜底 / Failure fallback: 缺输入时退出码 2。
# ============================================================================
import argparse
import bisect
import collections
import csv
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:                                       # Python 3.8
    ZoneInfo = None

TZ = "Europe/London"
CHANNELS = ("Temperature", "Humidity", "Pressure", "Gas", "Light")
ACTIVE = ("Temperature", "Gas", "Light")                  # 气压与湿度只记录 / pressure and humidity are record-only
SLIDE = 60
NOMINAL_PER_HOUR = 360

QUALITY, SWITCH, DRIFT, SITE, TRANSIENT, ENVIRONMENT, ANOMALY = (
    "质量问题", "分布切换", "漂移", "现场条件", "瞬态事件（待日界判定）", "环境事件", "异常")


class Params:
    def __init__(self, **kw):
        self.p_point = kw.get("p_point")                  # 由 derive_p_point 推导 / derived
        self.p_ctx = kw.get("p_ctx", 0.5)
        self.k_site = kw.get("k_site", 3)
        self.site_window_days = kw.get("site_window_days", 7)
        self.k_site_drift = kw.get("k_site_drift", 7)
        self.site_drift_window_days = kw.get("site_drift_window_days", 14)
        self.sync_frac = kw.get("sync_frac", 0.5)
        self.baseline_days = kw.get("baseline_days", 10)
        self.common_ratio_num = kw.get("common_ratio_num", 3)
        self.common_ratio_den = kw.get("common_ratio_den", 4)
        self.min_global = kw.get("min_global", 2)
        self.quality_max = kw.get("quality_max", 0.2)
        self.point_min_frac = kw.get("point_min_frac", 0.5)
        self.once_per_episode = kw.get("once_per_episode", True)
        self.layers = kw.get("layers", ("data", "perf", "detect"))
        self.first_day = kw.get("first_day", "2022-03-08")
        self.last_day = kw.get("last_day", "2022-04-30")

    def threshold(self, n):
        """共模门槛：max(最少台数, ⌈n·比例⌉)，整数运算 / common-mode threshold with integer arithmetic."""
        num, den = self.common_ratio_num, self.common_ratio_den
        return max(self.min_global, (n * num + den - 1) // den)


# ---------------------------------------------------------------- 时间 / time
def local_day_start(day):
    """当地日零点的世界协调时秒数 / UTC epoch of local midnight."""
    d = datetime.strptime(day, "%Y-%m-%d")
    if ZoneInfo is not None:
        return int(d.replace(tzinfo=ZoneInfo(TZ)).timestamp())
    raise RuntimeError("需要 Python 3.9+（zoneinfo）")


def next_day(day):
    return (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")


def add_days(day, k):
    return (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=k)).strftime("%Y-%m-%d")


def local_hours(day):
    """当地日内的各个整点起点（夏令时开始那天少一个小时）/ hour starts of a local day."""
    s, e = local_day_start(day), local_day_start(next_day(day))
    return list(range(s, e, 3600))


def local_of(ts):
    t = datetime.fromtimestamp(ts, ZoneInfo(TZ))
    return t.strftime("%Y-%m-%d"), t.hour


# ---------------------------------------------------------------- 输入 / inputs
def read(path):
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


class Inputs:
    def __init__(self, d):
        self.hours = {}
        for r in read(os.path.join(d, "hours.csv")):
            self.hours[(r["device"], int(r["hour_start"]))] = r
        self.devices = sorted({k[0] for k in self.hours})
        self.perf_events = collections.defaultdict(list)
        for r in read(os.path.join(d, "perf_events.csv")):
            self.perf_events[r["device"]].append((int(r["ts"]), r["level"], float(r["threshold"]), float(r["magnitude"])))
        self.perf_elements = collections.defaultdict(list)
        for r in read(os.path.join(d, "perf_elements.csv")):
            self.perf_elements[r["device"]].append((int(r["ts"]), float(r["main_score"])))
        self.days = {}
        for r in read(os.path.join(d, "data_days.csv")):
            self.days[(r["device"], r["day"], r["channel"])] = r
        self.evaluable = {}
        for (dev, day, ch), r in self.days.items():
            self.evaluable[(dev, day)] = r["evaluable"] == "1"
        self.bursts = collections.defaultdict(list)
        for r in read(os.path.join(d, "bursts.csv")):
            self.bursts[r["device"]].append((int(r["ts"]), int(r["emitted_at"])))
        self.refs = {(r["device"], r["channel"]): (float(r["p10"]), float(r["p90"])) for r in read(os.path.join(d, "refs.csv"))}
        self.freeze = {r["device"]: int(r["freeze_ts"]) for r in read(os.path.join(d, "freeze.csv"))}


def derive_p_point(inp, p):
    """平稳日参照期（03-08 至 03-17）可算设备小时的离群率第九十九百分位（全机队合并，numpy 线性插值）。"""
    rates = []
    for (dev, hs), r in inp.hours.items():
        day, _ = local_of(hs)
        if not ("2022-03-08" <= day <= "2022-03-17"):
            continue
        rounds, adm, guard = int(r["rounds"]), int(r["admitted"]), int(r["guard_rounds"])
        if quality_bad(rounds, guard, p) or adm < p.point_min_frac * NOMINAL_PER_HOUR:
            continue
        rates.append(int(r["outlier_rounds"]) / adm)
    rates.sort()
    pos = (len(rates) - 1) * 0.99
    lo = int(pos)
    hi = min(lo + 1, len(rates) - 1)
    g = pos - lo
    a, b = rates[lo], rates[hi]
    return b - (b - a) * (1 - g) if g >= 0.5 else a + (b - a) * g


def quality_bad(rounds, guard, p):
    missing = 1.0 - rounds / NOMINAL_PER_HOUR
    return missing > p.quality_max or guard / NOMINAL_PER_HOUR > p.quality_max


# ---------------------------------------------------------------- 性能层的确认与回落 / perf confirmation and fallback
class PerfTracker:
    """确认之后只用确认之后的主分元素，凑满六个后每到一个元素取最近六个的中位数，不高于确认的门限即回落。"""

    def __init__(self, events, elements):
        self.items = sorted([(t, 0, lvl, thr, mag) for t, lvl, thr, mag in events] +
                            [(t, 1, None, v, None) for t, v in elements], key=lambda x: (x[0], x[1]))
        self.i = 0
        self.confirmed = False
        self.confirm_ts = None
        self.since = None                                 # 这一段「已确认未回落」的开始 / start of the confirmed spell
        self.threshold = None
        self.magnitude = None
        self.post = []
        self.fallen_at = []

    def advance(self, t):
        """处理时刻不晚于 t 的事件与元素 / process items with ts <= t."""
        while self.i < len(self.items) and self.items[self.i][0] <= t:
            ts, kind, lvl, val, mag = self.items[self.i]
            self.i += 1
            if kind == 0:
                if lvl == "CONFIRM":
                    if not self.confirmed:
                        self.since = ts
                    self.confirmed, self.confirm_ts, self.threshold, self.magnitude, self.post = True, ts, val, mag, []
            elif self.confirmed and ts > self.confirm_ts:
                self.post.append(val)
                if len(self.post) >= 6:
                    s = sorted(self.post[-6:])
                    if (s[2] + s[3]) / 2.0 <= self.threshold:
                        self.confirmed = False
                        self.fallen_at.append(ts)


# ---------------------------------------------------------------- 决策 / decision
def run(inp, p):
    alerts, commands, review = [], [], []
    devs = inp.devices
    perf = {d: PerfTracker(inp.perf_events.get(d, []) if "perf" in p.layers else [], inp.perf_elements.get(d, [])) for d in devs}
    bursts = {d: inp.bursts.get(d, []) if "data" in p.layers else [] for d in devs}
    flag_hist = collections.defaultdict(set)      # (dev, 钟点) → 有点通道或上下文标志的日 / days with a detection flag
    site_hist = collections.defaultdict(set)      # (dev, 钟点) → 归为现场条件的日 / days labelled site condition
    base = {}                                      # (dev, ch, 钟点) → (和, 数) / baseline sums
    base_ready = {}
    drift = {}                                     # dev → dict(scope, triggers, since)
    episode = {}                                   # dev → dict(triggers, count)
    cooldown_until = {}
    seq = collections.Counter()
    day_alerts = collections.defaultdict(list)
    freeze_day = {d: local_of(inp.freeze[d])[0] for d in devs if d in inp.freeze}

    def data_dev(dev, day, ch):
        if "data" not in p.layers:
            return False
        r = inp.days.get((dev, day, ch))
        return r is not None and r["deviated"] == "1"

    def data_trans(dev, day, ch):
        if "data" not in p.layers:
            return ""
        r = inp.days.get((dev, day, ch))
        return "" if r is None else r["transition"]

    day = p.first_day
    while day <= p.last_day:
        d1, d2 = add_days(day, -1), add_days(day, -2)
        for hs in local_hours(day):
            he = hs + 3600
            hod = local_of(hs)[1]
            info = {}
            for dev in devs:
                r = inp.hours.get((dev, hs))
                if r is None:
                    continue
                rounds, adm, guard = int(r["rounds"]), int(r["admitted"]), int(r["guard_rounds"])
                bad = quality_bad(rounds, guard, p)
                det = "detect" in p.layers
                point = None
                if det and adm >= p.point_min_frac * NOMINAL_PER_HOUR:
                    point = int(r["outlier_rounds"]) / adm
                pflag = point is not None and point > p.p_point
                cflag = det and int(r["ctx_above"]) / 6.0 >= p.p_ctx
                perf[dev].advance(he)
                fperf = perf[dev].confirmed
                fdata = {ch: data_dev(dev, d1, ch) or data_dev(dev, d2, ch) for ch in CHANNELS}
                fburst = any(hs - 7200 < b <= hs and e <= he for b, e in bursts[dev])
                means = {}
                for ch in ACTIVE:
                    n = int(r["n_" + ch])
                    if n > 0:
                        means[ch] = float(r["sum_" + ch]) / n
                info[dev] = dict(bad=bad, point=point, pflag=pflag, cflag=cflag, perf=fperf, data=fdata, burst=fburst,
                                 means=means, ctx_max=r["ctx_max"], row=r)
                # 基线累积：冻结日起十个当地日 / baseline accumulation over ten local days from the freeze day
                fd = freeze_day.get(dev)
                if fd is not None and fd <= day <= add_days(fd, p.baseline_days - 1):
                    for ch in ACTIVE:
                        n = int(r["n_" + ch])
                        if n > 0:
                            s0, n0 = base.get((dev, ch, hod), (0.0, 0))
                            base[(dev, ch, hod)] = (s0 + float(r["sum_" + ch]), n0 + n)
            present = [d for d in info if not info[d]["bad"]]
            T = p.threshold(len(present))
            cnt = collections.Counter()
            for d in present:
                x = info[d]
                cnt["point"] += x["pflag"]
                cnt["ctx"] += x["cflag"]
                cnt["perf"] += x["perf"]
                cnt["burst"] += x["burst"]
                for ch in CHANNELS:
                    cnt["data_" + ch] += x["data"][ch]
            glob = {k: v >= T for k, v in cnt.items()}
            T_rows = p.threshold(len(info))
            n_bad = sum(1 for d in info if info[d]["bad"])
            for dev in sorted(info):
                x = info[dev]
                label, scope, note, co = None, "局部", "", []
                active_data = [ch for ch in ACTIVE if x["data"][ch]]
                if x["bad"]:
                    label, scope = QUALITY, ("全局" if n_bad >= T_rows else "局部")
                    co = [d for d in info if info[d]["bad"]]
                elif x["burst"] and any(data_trans(dev, dd, ch) == "DEVIATE" for dd in (d1, d2) for ch in ACTIVE):
                    label, scope = SWITCH, ("全局" if glob.get("burst") else "局部")
                    co = [d for d in present if info[d]["burst"]]
                elif dev in drift:
                    label, scope = DRIFT, drift[dev]["scope"]
                    co = sorted(drift)
                elif (x["pflag"] or x["cflag"]) and site_condition(dev, hod, day, x, flag_hist, base, inp, p, freeze_day):
                    label = SITE
                    if len([dd for dd in site_hist[(dev, hod)] if add_days(day, -(p.site_drift_window_days - 1)) <= dd < day]) + 1 \
                            >= p.k_site_drift:
                        note = "局部漂移候选，留待适应执行第二批"
                elif x["perf"] or active_data:
                    label = TRANSIENT
                    scope = "全局" if (x["perf"] and glob.get("perf")) or any(glob.get("data_" + ch) for ch in active_data) else "局部"
                    co = [d for d in present if info[d]["perf"] or any(info[d]["data"][ch] for ch in ACTIVE)]
                elif (x["pflag"] and glob.get("point")) or (x["cflag"] and glob.get("ctx")):
                    label, scope = ENVIRONMENT, "全局"
                    co = [d for d in present if info[d]["pflag"] or info[d]["cflag"]]
                elif x["pflag"] or x["cflag"]:
                    label = ANOMALY
                    co = [dev]
                if label == SITE:
                    site_hist[(dev, hod)].add(day)
                if label is None:
                    continue
                a = dict(device=dev, hour_start=hs, local_hour="%s %02d" % (day, hod), label=label, scope=scope,
                         co="|".join(sorted(co)), point_rate=x["point"], ctx_max=x["ctx_max"],
                         perf_magnitude=perf[dev].magnitude if x["perf"] else None,
                         data_channels="|".join(active_data), burst=int(x["burst"]), note=note)
                alerts.append(a)
                day_alerts[day].append(a)
            # 现场条件用的按日标志记录在日界更新；这里先记下当日该钟点是否有标志 / per-day flag record
            for dev in info:
                if info[dev]["pflag"] or info[dev]["cflag"]:
                    flag_hist[(dev, hod)].add(day)
        # ---------------- 日节拍：日界 t = 次日零点 / daily tick at the boundary
        t = local_day_start(next_day(day))
        daily_tick(day, t, inp, p, devs, perf, drift, episode, cooldown_until, seq, commands, data_dev, data_trans)
        # ---------------- 日界复核 / daily review
        for dev in devs:
            if (dev, day) in inp.evaluable and not inp.evaluable[(dev, day)]:
                review.append(dict(device=dev, day=day, kind="当日不可判", hour_start="", detail=""))
            if any(data_trans(dev, day, ch) == "DEVIATE" for ch in ACTIVE):
                for a in day_alerts[day]:
                    if a["device"] != dev or a["label"] != ANOMALY:
                        continue
                    hs = a["hour_start"]
                    if any(hs < b <= hs + 3600 and e <= t + SLIDE for b, e in bursts[dev]):
                        review.append(dict(device=dev, day=day, kind="异常改记为分布切换", hour_start=hs, detail=a["local_hour"]))
        day = next_day(day)
    return alerts, commands, review


def site_condition(dev, hod, day, x, flag_hist, base, inp, p, freeze_day):
    """现场条件：最近七天（不含当日）该钟点有标志的天数 ≥ K_site − 1，且温度、气体、光照任一个的变换域小时均值相对同钟点基线
    偏离超过参照宽度的 sync_frac 倍；基线在冻结日起第十个当地日结束后才可用。"""
    past = [dd for dd in flag_hist[(dev, hod)] if add_days(day, -p.site_window_days) <= dd < day]
    if len(past) < p.k_site - 1:
        return False
    fd = freeze_day.get(dev)
    if fd is None or day <= add_days(fd, p.baseline_days - 1):
        return False                                  # 基线累积中 / baseline still accumulating
    for ch in ACTIVE:
        if ch not in x["means"] or (dev, ch, hod) not in base or (dev, ch) not in inp.refs:
            continue
        s, n = base[(dev, ch, hod)]
        p10, p90 = inp.refs[(dev, ch)]
        if abs(x["means"][ch] - s / n) > p.sync_frac * (p90 - p10):
            return True
    return False


def daily_tick(day, t, inp, p, devs, perf, drift, episode, cooldown_until, seq, commands, data_dev, data_trans):
    prev = add_days(day, -1)
    day_start = local_day_start(day)
    present = [d for d in devs if inp.evaluable.get((d, day), False)]
    T = p.threshold(len(present))
    st = {}
    for dev in devs:
        perf[dev].advance(t)
        pf = perf[dev]
        perf_at_t = pf.confirmed
        perf_cross = pf.confirmed and pf.since is not None and pf.since < day_start
        ev = inp.evaluable.get((dev, day), False)
        chans = {}
        for ch in CHANNELS:
            dd = data_dev(dev, day, ch)
            persistent = ev and dd and (data_dev(dev, prev, ch) or perf_at_t)
            chans[ch] = dict(dev=ev and dd, persistent=persistent)
        st[dev] = dict(perf_at_t=perf_at_t, perf_cross=perf_cross, chans=chans)
    cnt = collections.Counter()
    for dev in present:
        for ch in ACTIVE:
            cnt[ch] += st[dev]["chans"][ch]["persistent"]
        cnt["perf"] += st[dev]["perf_cross"]
    # 回落：结束漂移状态与同一段偏离 / fallbacks end drift status and episodes
    for dev in devs:
        for book in (drift, episode):
            if dev not in book:
                continue
            fell = False
            for trig in book[dev]["triggers"]:
                if trig == "perf":
                    fell = fell or not st[dev]["perf_at_t"]
                elif inp.evaluable.get((dev, day), False) and not data_dev(dev, day, trig):
                    fell = True
            if fell:
                del book[dev]
    for dev in devs:
        s = st[dev]
        triggers, forms = [], []
        for ch in ACTIVE:
            c = s["chans"][ch]
            if c["persistent"]:
                triggers.append(ch)
                forms.append("%s：%s" % (ch, "两层同日同报" if s["perf_at_t"] and not data_dev(dev, prev, ch) else "连续两日偏离"))
        if s["perf_cross"]:
            triggers.append("perf")
            forms.append("性能层确认跨过日界")
        if not triggers:
            continue
        scope = "全局" if any(cnt[tr] >= T for tr in triggers) else "局部"
        co = sorted(d for d in present if any(st[d]["chans"][ch]["persistent"] for ch in ACTIVE if ch in triggers)
                    or ("perf" in triggers and st[d]["perf_cross"]))
        drift.setdefault(dev, dict(scope=scope, triggers=triggers, since=t))
        if cooldown_until.get(dev, 0) > t:
            continue
        if dev in episode and p.once_per_episode:
            continue
        ep = episode.setdefault(dev, dict(triggers=triggers, count=0))
        ep["count"] += 1
        seq[dev] += 1
        eff = local_day_start(next_day(next_day(day)))      # t 之后的下一个当地日界 / next local boundary after t
        cooldown_until[dev] = local_day_start(next_day(next_day(next_day(day))))
        commands.append(dict(device=dev, decided_at=t, effective_at=eff, seq=seq[dev], level=0, scope=scope,
                             label=DRIFT, triggers="|".join(triggers), forms="；".join(forms), co=len(co),
                             co_devices="|".join(co), episode_n=ep["count"], day=day))


# ---------------------------------------------------------------- 输出 / output
def fmt(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return repr(v)
    return str(v)


def write(path, rows, cols):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(cols)
        for r in rows:
            w.writerow([fmt(r[c]) for c in cols])


ALERT_COLS = ["device", "hour_start", "local_hour", "label", "scope", "co", "point_rate", "ctx_max", "perf_magnitude",
              "data_channels", "burst", "note"]
COMMAND_COLS = ["device", "day", "decided_at", "effective_at", "seq", "level", "scope", "label", "triggers", "forms", "co",
                "co_devices", "episode_n"]
REVIEW_COLS = ["device", "day", "kind", "hour_start", "detail"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", default="docs/m7/inputs")
    ap.add_argument("--out-dir", default="docs/m7/run")
    ap.add_argument("--no-once", action="store_true")
    ap.add_argument("--layers", default="data,perf,detect")
    ap.add_argument("--common-ratio", default="3/4")
    ap.add_argument("--k-site", type=int, default=3)
    ap.add_argument("--p-point", type=float, default=None)
    a = ap.parse_args()
    need = ["hours.csv", "perf_events.csv", "perf_elements.csv", "data_days.csv", "bursts.csv", "refs.csv", "freeze.csv"]
    miss = [f for f in need if not os.path.exists(os.path.join(a.inputs, f))]
    if miss:
        print("ERROR: %s 下缺少 %s；先运行 m7/prepare_inputs.py" % (a.inputs, miss), file=sys.stderr)
        return 2
    num, den = (int(x) for x in a.common_ratio.split("/"))
    p = Params(once_per_episode=not a.no_once, layers=tuple(a.layers.split(",")), common_ratio_num=num,
               common_ratio_den=den, k_site=a.k_site)
    inp = Inputs(a.inputs)
    p.p_point = a.p_point if a.p_point is not None else derive_p_point(inp, Params())
    alerts, commands, review = run(inp, p)
    os.makedirs(a.out_dir, exist_ok=True)
    write(os.path.join(a.out_dir, "alerts.csv"), alerts, ALERT_COLS)
    write(os.path.join(a.out_dir, "commands.csv"), commands, COMMAND_COLS)
    write(os.path.join(a.out_dir, "review.csv"), review, REVIEW_COLS)
    with open(os.path.join(a.out_dir, "params.json"), "w", encoding="utf-8") as fh:
        json.dump(vars(p), fh, ensure_ascii=False, indent=1, default=list)
    print("p_point = %r" % p.p_point)
    print("告警 %d 条：%s" % (len(alerts), dict(collections.Counter(x["label"] for x in alerts))))
    print("命令 %d 条；复核 %d 条" % (len(commands), len(review)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
