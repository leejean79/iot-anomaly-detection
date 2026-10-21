#!/usr/bin/env python3
# ============================================================================
# m5b/run_grid.py
# M5b 离线实验（验收项 V-M5b-2，离线网格）：36 种组合加加权霍夫丁基线，按预先写定的操作口径
# `docs/reports/m5b_offline_protocol.md`（第三节七条、第五节补充项）逐条统计，并按第四节的参数选择规则给出结论。
# 本脚本只计数、不改任何参数；口径与第四节规则都以操作口径文件为准，本文件中的常量注明了出处。
# Offline grid for M5b (V-M5b-2): 36 combinations plus the HDDM_W baseline, counted exactly as the pre-registered protocol.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac 或开发机，仓库根目录；python3，river 0.26（基线用；pip install river==0.26.1）。
#    Java 等值核对（可选）另需 JDK 11 及以上与 mvn compile 后的 target/classes。
# 2. 调用命令 / Invocation:
#      python3 m5b/run_grid.py                       # 全部统计，写 docs/m5b/grid/ 与 docs/reports/m5b_offline_grid.md
#      python3 m5b/run_grid.py --java-check          # 另外对两种元素流各跑一遍 check_equality_grid.py，结果写进报告
#      python3 m5b/run_grid.py --out-dir /tmp/g --report /tmp/g/report.md   # 写到别处（试运行）
#      python3 m5b/run_grid.py --self-test           # 只运行归属、核对结论与盲区等辅助函数的自检
# 3. 前置条件 / Preconditions: docs/m5b 下有四份元素流（prepare_streams.py 生成，含 _with_injection 两份）与 12 张常量表；
#    docs/m3_inject/v34/v34_daily.csv、docs/m3_marapr/daily_channel_profile_ref0308.csv、docs/m3_a4/full_fix/inject-truth.csv 存在。
# 4. 期望产出 / Expected output: docs/m5b/grid/ 下事件清单（events/，72 份）与各口径的长表 CSV、summary.csv；
#    报告 docs/reports/m5b_offline_grid.md（第四节规则的结论、36 种组合与基线的完整表格、默认组合的逐台明细）。
# 5. 失败兜底 / Failure fallback: 退出码 2 表示输入缺失或没装 river；--java-check 有组合不同时退出码 1，报告里写明。
# ============================================================================
import argparse
import collections
import csv
import itertools
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import optwin_offline as ow   # noqa: E402

DEVICES = "ABCDEFGH"
CHANNELS = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]
CONFS, RHOS, GRANS, KS = (0.99, 0.999), (0.1, 0.3, 0.5), ("10min", "hourly"), (18, 36, 72)
DEFAULT = (0.99, 0.5, "10min", 36)                       # 交接文档决定六 / handover decision 6
GRAN = {"10min": ("streams_10min", "window_end", "main_score", 2016, 600),
        "hourly": ("streams_hourly", "hour_start", "median_main_score", 336, 3600)}
ATTR_TAIL = 6 * 3600                                     # 归属尾长 6 小时（口径第二节）/ attribution tail
BLIND_ELEMENTS = 30                                      # 盲区：候选之后到第 30 个元素（口径第三节第 5 条）
SCALE_MAX, GAS_SENS = 3.0, 1.5                           # 尺度门槛与气体敏感性列（第 3 条）
BST_START = 1648342800                                   # 2022-03-27 01:00 UTC 起英国夏令时（CLAUDE.md 第 8 节）
TAIL = (1648080000, 1648425600)                          # 03-24 00:00 至 03-28 00:00 UTC（第 2 条）
C_SUN_DAYS = ("04-17", "04-20", "04-21", "04-24", "04-25")   # c_alarm/evidence.md 条件甲（第 4 条）
EVENING = (1650218400, 1650240000)                       # 04-17 18:00 至 24:00 UTC（第 4 条）
LATE_START = {"B": "03-26 15:12（当地）", "H": "03-26 00:03（当地）"}   # 第 2 条注明


# ---------------------------------------------------------------- 时间 / time
def utc(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%m-%d %H:%M")


def local(ts):
    return datetime.fromtimestamp(ts + (3600 if ts >= BST_START else 0), timezone.utc).strftime("%m-%d %H:%M")


def day_of(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%m-%d")


def day_start(day):
    return int(datetime.strptime("2022-" + day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


# ---------------------------------------------------------------- 组合 / combos
def combo_id(c):
    return "c%s_r%s_%s_k%d" % c


def k_hours(c):
    return c[3] * GRAN[c[2]][4] / 3600.0


def label(c):
    return "%s / %s%s / %s / K=%d（%g 小时）" % (c[0], c[1], "（对半切分）" if c[1] == 0.1 else "",
                                           "十分钟" if c[2] == "10min" else "小时", c[3], k_hours(c))


# ---------------------------------------------------------------- 运行 / runs
def run_combo(rows, table, k):
    """逐台运行检测器与核对；另记下每次「方差显著但均值未升」所在的元素时刻 / also record fWithoutRise per element."""
    det, gate, events, fwr = {}, {}, [], []
    for dev, ts, x in rows:
        d = det.setdefault(dev, ow.Detector(table.wmax))
        g = gate.setdefault(dev, ow.Gate(k, table.robustness))
        before = d.f_without_rise
        events.extend(g.on_element(dev, ts, x, d.update(x, table)))
        if d.f_without_rise > before:
            fwr.append((dev, ts))
    return events, fwr


def run_baseline(path):
    """加权霍夫丁检测器，river 0.26 默认参数，输入超阈标记，逐台独立（口径第一节）/ HDDM_W per device on the 0/1 flag."""
    from river.drift.binary import HDDMW
    det, out = {}, []
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            d = det.setdefault(r["device"], HDDMW(drift_confidence=0.001, warning_confidence=0.005, lambda_val=0.05,
                                                  two_sided_test=False))
            d.update(int(r["above_threshold"]))
            if d.drift_detected:
                out.append((r["device"], int(r["window_end"])))
    return out


# ---------------------------------------------------------------- 口径辅助 / helpers
def attributed(ts, seg):
    """候选时刻落在 [段开始, 段结束 + 6 小时] 之内（口径第二节）/ attribution rule."""
    return seg["start"] <= ts <= seg["end"] + ATTR_TAIL


def gate_outcomes(events):
    """开始核对的候选 →（结论, 结论时刻）；到流末尾仍无结论为 None / outcome of each check-starting candidate."""
    out = {}
    for e in events:
        if e["level"] == "CANDIDATE" and not int(e["during_check"]):
            out[(e["device"], e["ts"])] = None
    for e in events:
        if e["level"] in ("CONFIRM", "REVOKE"):
            out[(e["device"], e["candidate_ts"])] = (e["level"], e["ts"])
    return out


def blind_intervals(events, elem_ts):
    """每个候选到其后第 30 个元素为止 / [candidate, 30th element after] per candidate."""
    out = collections.defaultdict(list)
    for e in events:
        if e["level"] != "CANDIDATE":
            continue
        ts_list = elem_ts[e["device"]]
        i = _index(ts_list, e["ts"])
        j = min(i + BLIND_ELEMENTS, len(ts_list) - 1)
        out[e["device"]].append((e["ts"], ts_list[j]))
    return out


def _index(ts_list, ts):
    lo, hi = 0, len(ts_list)
    while lo < hi:
        mid = (lo + hi) // 2
        if ts_list[mid] < ts:
            lo = mid + 1
        else:
            hi = mid
    return lo


def segment_stats(seg, events, fwr, blind, elem_ts=None):
    """一段的候选、确认、F 计数、盲区内错过与时延 / per-segment statistics including delays.

    时延（验收项四）：从段的开始到第一个归属于它的候选、到第一次归属于它的确认，给出分钟数与其间的元素数
    （元素时刻落在 (段开始, 候选或确认时刻] 之内）。/ delay from the segment start to the first candidate / confirm."""
    dev = seg["device"]
    cands = [e for e in events if e["device"] == dev and e["level"] == "CANDIDATE" and attributed(e["ts"], seg)]
    confs = [e for e in events if e["device"] == dev and e["level"] == "CONFIRM" and attributed(e["candidate_ts"], seg)]
    in_blind = any(a <= seg["end"] and b >= seg["start"] for a, b in blind.get(dev, []))
    return dict(candidates=len(cands), has_candidate=int(bool(cands)), has_confirm=int(bool(confs)),
                first_candidate_test=min(cands, key=lambda e: e["ts"])["test"] if cands else "",
                first_confirm_ts=min(e["ts"] for e in confs) if confs else 0,
                first_confirm_test=min(confs, key=lambda e: e["ts"])["test"] if confs else "",
                fwr=sum(1 for d, t in fwr if d == dev and seg["start"] <= t < seg["end"]),
                blind_miss=int(in_blind and not cands),
                cand_delay=_delay(seg, min(e["ts"] for e in cands) if cands else 0, elem_ts, dev),
                confirm_delay=_delay(seg, min(e["ts"] for e in confs) if confs else 0, elem_ts, dev))


def _delay(seg, ts, elem_ts, dev):
    """（分钟数, 元素数）；没有事件时为空 / (minutes, elements) or blanks."""
    if not ts:
        return "", ""
    n = sum(1 for t in elem_ts[dev] if seg["start"] < t <= ts) if elem_ts else ""
    return "%.0f" % ((ts - seg["start"]) / 60.0), n


# ---------------------------------------------------------------- 输入 / inputs
def load_stable(path):
    st = {d: set() for d in DEVICES}
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            for d in DEVICES:
                if r.get("stable_" + d) == "True":
                    st[d].add(r["day"])
    return st


def load_scale_days(path):
    """尺度漂移日：03-24 至 04-30 中任一检测通道宽度倍数超过 3.0；敏感性列：气体宽度倍数不低于 1.5（第 3 条）。"""
    mult = {}
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            ref = float(r["ref_p10_p90_width"])
            if ref > 0 and r["channel"] in CHANNELS:
                mult[(r["device"], r["day"], r["channel"])] = (float(r["p90"]) - float(r["p10"])) / ref
    days = sorted({k[1] for k in mult if "03-24" <= k[1] <= "04-30"})
    scale, gas = {d: {} for d in DEVICES}, {d: {} for d in DEVICES}
    for dv in DEVICES:
        for day in days:
            over = ["%s %.2f" % (c, mult[(dv, day, c)]) for c in CHANNELS if mult.get((dv, day, c), 0) > SCALE_MAX]
            if over:
                scale[dv][day] = "、".join(over)
            g = mult.get((dv, day, "Gas"))
            if g is not None and g >= GAS_SENS:
                gas[dv][day] = "Gas %.2f" % g
    return scale, gas


def load_injections(path):
    with open(path, encoding="utf-8") as fh:
        return [dict(item="7", name="%s %s %s %s" % (r["channel"], r["type"], r["magnitude"], utc(int(r["start_ts"]))),
                     device=r["device"], start=int(r["start_ts"]), end=int(r["end_ts"]), type=r["type"],
                     channel=r["channel"], magnitude=r["magnitude"]) for r in csv.DictReader(fh)]


def segments(scale, injections):
    segs = [dict(item="2", name="三月尾段", device=d, start=TAIL[0], end=TAIL[1]) for d in DEVICES]
    for d in DEVICES:
        for day in sorted(scale[d]):
            segs.append(dict(item="3", name="尺度漂移日 " + day, device=d, start=day_start(day),
                             end=day_start(day) + 86400, why=scale[d][day]))
    for day in C_SUN_DAYS:
        segs.append(dict(item="4", name="C 日照直射 %s 11–14 时（UTC）" % day, device="C",
                         start=day_start(day) + 11 * 3600, end=day_start(day) + 14 * 3600))
    for d in DEVICES:
        segs.append(dict(item="4", name="04-17 傍晚气体上升 18–24 时（UTC）", device=d, start=EVENING[0], end=EVENING[1]))
    return segs + injections


# ---------------------------------------------------------------- 统计 / statistics
def analyse(c, events, fwr, elem_ts, stable, scale, gas, segs_main, ev_inj, fwr_inj, elem_inj, inj_segs, rows_out):
    cid = combo_id(c)
    S = dict(combo=cid, confidence=c[0], robustness=c[1], granularity=c[2], k=c[3], k_hours=k_hours(c))
    lv = collections.Counter(e["level"] for e in events)
    S.update(candidates=lv["CANDIDATE"], confirms=lv["CONFIRM"], revokes=lv["REVOKE"])
    outcomes = gate_outcomes(events)

    # 第 1 条与第五节第 1 项 / item 1 and addendum 1
    tot = collections.Counter()
    for d in DEVICES:
        sd = stable[d]
        conf_st = [e for e in events if e["device"] == d and e["level"] == "CONFIRM" and day_of(e["ts"]) in sd]
        cand_st = [e for e in events if e["device"] == d and e["level"] == "CANDIDATE" and day_of(e["ts"]) in sd]
        starts = [e for e in cand_st if not int(e["during_check"])]
        oc = collections.Counter((outcomes.get((d, e["ts"])) or ("UNRESOLVED",))[0] for e in starts)
        fw = sum(1 for dv, t in fwr if dv == d and day_of(t) in sd)
        r = dict(combo=cid, device=d, stable_days=len(sd), confirms_on_stable=len(conf_st),
                 candidates_on_stable=len(cand_st), check_starting=len(starts), during_check=len(cand_st) - len(starts),
                 revoked=oc["REVOKE"], confirmed=oc["CONFIRM"], unresolved=oc["UNRESOLVED"],
                 blocked_ratio=("%.3f" % (oc["REVOKE"] / len(starts))) if starts else "",
                 fwr_total=sum(1 for dv, _ in fwr if dv == d), fwr_stable=fw)
        rows_out["item1_per_device"].append(r)
        for k in ("stable_days", "confirms_on_stable", "candidates_on_stable", "check_starting", "during_check",
                  "revoked", "confirmed", "unresolved", "fwr_total", "fwr_stable"):
            tot[k] += r[k]
        for day in sorted(sd):
            nc = sum(1 for e in conf_st if day_of(e["ts"]) == day)
            nn = sum(1 for e in cand_st if day_of(e["ts"]) == day)
            if nc or nn:
                rows_out["item1_per_day"].append(dict(combo=cid, device=d, day=day, confirms=nc, candidates=nn))
    S.update({"i1_" + k: v for k, v in tot.items()})
    S["i1_blocked_ratio"] = ("%.3f" % (tot["revoked"] / tot["check_starting"])) if tot["check_starting"] else ""

    # 第 2、3、4、7 条的各段与盲区（第 5 条）/ segments of items 2, 3, 4, 7 and blind spots (item 5)
    blind, blind_inj = blind_intervals(events, elem_ts), blind_intervals(ev_inj, elem_inj)
    seg_rows = []
    for seg in segs_main:
        st = segment_stats(seg, events, fwr, blind, elem_ts)
        seg_rows.append((seg, st))
    for seg in inj_segs:
        seg_rows.append((seg, segment_stats(seg, ev_inj, fwr_inj, blind_inj, elem_inj)))
    for seg, st in seg_rows:
        rows_out["segments"].append(dict(combo=cid, item=seg["item"], segment=seg["name"], device=seg["device"],
                                         note=seg.get("why", ""), candidates=st["candidates"],
                                         has_candidate=st["has_candidate"], has_confirm=st["has_confirm"],
                                         first_candidate_test=st["first_candidate_test"],
                                         first_confirm_utc=utc(st["first_confirm_ts"]) if st["first_confirm_ts"] else "",
                                         first_confirm_local=local(st["first_confirm_ts"]) if st["first_confirm_ts"] else "",
                                         first_confirm_test=st["first_confirm_test"], fwr=st["fwr"],
                                         blind_miss=st["blind_miss"],
                                         cand_delay_min=st["cand_delay"][0], cand_delay_elements=st["cand_delay"][1],
                                         confirm_delay_min=st["confirm_delay"][0],
                                         confirm_delay_elements=st["confirm_delay"][1]))
    by_item = collections.defaultdict(list)
    for seg, st in seg_rows:
        by_item[seg["item"]].append((seg, st))
    S["i2_devices_confirmed"] = sum(st["has_confirm"] for _, st in by_item["2"])
    # 「这四天内的候选数」按候选时刻落在四天之内计（不加尾长）/ candidates whose time lies within the four days
    S["i2_candidates"] = sum(1 for e in events if e["level"] == "CANDIDATE" and TAIL[0] <= e["ts"] < TAIL[1])
    S["i2_fwr"] = sum(st["fwr"] for _, st in by_item["2"])
    sun = [x for x in by_item["4"] if x[0]["name"].startswith("C 日照")]
    eve = [x for x in by_item["4"] if not x[0]["name"].startswith("C 日照")]
    S.update(i4_sun_candidate=sum(st["has_candidate"] for _, st in sun), i4_sun_confirm=sum(st["has_confirm"] for _, st in sun),
             i4_evening_candidate=sum(st["has_candidate"] for _, st in eve),
             i4_evening_confirm=sum(st["has_confirm"] for _, st in eve),
             i4_fwr=sum(st["fwr"] for _, st in by_item["4"]))
    S["i5_blind_misses"] = sum(st["blind_miss"] for _, st in seg_rows)
    S.update(i7_with_candidate=sum(st["has_candidate"] for _, st in by_item["7"]),
             i7_with_confirm=sum(st["has_confirm"] for _, st in by_item["7"]),
             i7_fwr=sum(st["fwr"] for _, st in by_item["7"]))

    # 第 3 条：按确认时刻所在的日子 / item 3 by the confirm's day
    days_conf = days_conf_g = 0
    f3 = t3 = fg = tg = 0
    for d in DEVICES:
        for day, why in sorted(scale[d].items()):
            cs = [e for e in events if e["device"] == d and e["level"] == "CONFIRM" and day_of(e["ts"]) == day]
            nf = sum(e["test"] == "F" for e in cs)
            f3, t3, days_conf = f3 + nf, t3 + len(cs) - nf, days_conf + bool(cs)
            rows_out["item3_scale_days"].append(dict(combo=cid, device=d, day=day, column="主表（任一通道 > 3.0）",
                                                     channels_over=why, confirms=len(cs), F=nf, T=len(cs) - nf))
        for day, why in sorted(gas[d].items()):
            cs = [e for e in events if e["device"] == d and e["level"] == "CONFIRM" and day_of(e["ts"]) == day]
            nf = sum(e["test"] == "F" for e in cs)
            fg, tg, days_conf_g = fg + nf, tg + len(cs) - nf, days_conf_g + bool(cs)
            rows_out["item3_scale_days"].append(dict(combo=cid, device=d, day=day, column="敏感性（气体 ≥ 1.5）",
                                                     channels_over=why, confirms=len(cs), F=nf, T=len(cs) - nf))
    S.update(i3_days=sum(len(scale[d]) for d in DEVICES), i3_days_confirmed=days_conf, i3_F=f3, i3_T=t3,
             i3g_days=sum(len(gas[d]) for d in DEVICES), i3g_days_confirmed=days_conf_g, i3g_F=fg, i3g_T=tg)

    # 第五节第 2 项：触发检验的分解 / trigger-test decomposition
    fleet_f = fleet_n = 0
    for d in DEVICES:
        cs = [e for e in events if e["device"] == d and e["level"] == "CONFIRM"]
        nf = sum(e["test"] == "F" for e in cs)
        fleet_f, fleet_n = fleet_f + nf, fleet_n + len(cs)
        rows_out["trigger_tests"].append(dict(combo=cid, device=d, confirms=len(cs), F=nf, T=len(cs) - nf,
                                              F_share=("%.3f" % (nf / len(cs))) if cs else ""))
    S["F_share"] = ("%.3f" % (fleet_f / fleet_n)) if fleet_n else ""

    # 每一次确认的幅度与候选时刻的历史标准差（2026-10-21 补充，只报告，不设门限）
    # Per confirm: (median of the last six post-candidate elements − reference) / historical sd at the candidate.
    for e in events:
        if e["level"] == "CONFIRM":
            sd = e["hist_sd"]
            rows_out["confirms"].append(dict(
                combo=cid, device=e["device"], confirm_utc=utc(e["ts"]), confirm_local=local(e["ts"]),
                candidate_utc=utc(e["candidate_ts"]), test=e["test"],
                on_stable_day=int(day_of(e["ts"]) in stable[e["device"]]),
                reference=repr(e["reference"]), last_median=repr(e["last_median"]), hist_sd=repr(sd),
                confirm_magnitude=repr((e["last_median"] - e["reference"]) / sd) if sd > 0 else ""))
    S["fwr_total"], S["fwr_stable"] = tot["fwr_total"], tot["fwr_stable"]
    return S


def selection(summary):
    """第四节的参数选择规则（2026-10-21 设计会话确认）/ section 4 selection rule (confirmed)."""
    by = {(s["confidence"], s["robustness"], s["granularity"], s["k"]): s for s in summary}
    dflt = by[DEFAULT]
    neigh = [((0.999, 0.5, "10min", 36), "置信度 0.999", True), ((0.99, 0.3, "10min", 36), "鲁棒度 0.3", True),
             ((0.99, 0.1, "10min", 36), "鲁棒度 0.1（对半切分，只列出）", False),
             ((0.99, 0.5, "hourly", 36), "小时粒度", True), ((0.99, 0.5, "10min", 18), "K = 18", True),
             ((0.99, 0.5, "10min", 72), "K = 72", True)]
    rows, ok = [], []
    for c, name, eligible in neigh:
        s = by[c]
        a, b = dflt["i1_confirms_on_stable"], s["i1_confirms_on_stable"]
        higher = a >= 1.5 * b and a - b >= 3
        c2 = s["i2_devices_confirmed"] >= dflt["i2_devices_confirmed"]
        c3 = s["i3_days_confirmed"] >= dflt["i3_days_confirmed"]
        hit = eligible and higher and c2 and c3
        if hit:
            ok.append(name)
        rows.append((name, s, higher, c2, c3, hit, eligible))
    if not ok:
        verdict = "没有邻近组合同时满足三条，保持默认组合。"
    elif len(ok) == 1:
        verdict = "只有「%s」同时满足三条，按规则更换为该组合。" % ok[0]
    else:
        verdict = "有 %d 个邻近组合同时满足三条（%s），按规则不自动选择，交给设计会话。" % (len(ok), "、".join(ok))
    return dflt, rows, verdict


# ---------------------------------------------------------------- 报告 / report
def write_csv(path, rows):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def report(path, summary, rows_out, base_rows, base_seg, java_note, scale, gas, stable, out_dir):
    dflt, sel_rows, verdict = selection(summary)
    L = []
    p = L.append
    p("# M5b 离线网格结果（V-M5b-2）\n")
    p("由 `m5b/run_grid.py` 生成（%s）。口径：`docs/reports/m5b_offline_protocol.md` 第三节七条与第五节补充项；日期为世界协调时，"
      "时刻同时给出布里斯托当地时间。事件归属：候选时刻落在 [段开始, 段结束 + 6 小时] 之内；确认与撤销按它所核对的候选归属。"
      "同一个候选可能同时归属于相邻的几段（例如间隔不到 6 小时的注入），各段分别计。表甲的「尾段候选」按候选时刻落在 03-24 至 03-27 "
      "四天之内计；逐台明细表中各段的「候选」按归属规则计。长表在 `%s/`。\n" % (datetime.now().strftime("%Y-%m-%d %H:%M"), out_dir))
    p("Java 与 Python 的等值：%s\n" % java_note)
    p("## 一、第四节参数选择规则的结论（规则于 2026-10-21 经设计会话确认，在运行之前）\n")
    p("默认组合：平稳日确认 %d 条，三月尾段有确认的设备 %d 台，有确认的尺度漂移日 %d 个设备日。\n"
      % (dflt["i1_confirms_on_stable"], dflt["i2_devices_confirmed"], dflt["i3_days_confirmed"]))
    p("| 邻近组合 | 平稳日确认 | 默认明显更高（多 50% 且至少多 3 条） | 尾段有确认的设备数 | 不比默认差 | 有确认的尺度漂移日 | 不比默认差 | 满足三条 |")
    p("| --- | --- | --- | --- | --- | --- | --- | --- |")
    yn = lambda v: "是" if v else "否"
    for name, s, hi, c2, c3, hit, el in sel_rows:
        p("| %s | %d | %s | %d | %s | %d | %s | %s |" % (name, s["i1_confirms_on_stable"], yn(hi), s["i2_devices_confirmed"],
                                                       yn(c2), s["i3_days_confirmed"], yn(c3), yn(hit) if el else "不作为候选"))
    p("\n**%s**\n" % verdict)

    p("## 二、36 种组合的完整表格\n")
    p("鲁棒度 0.1 在窗上限以内没有根，切点退化为对半切分（0.92 版第三点）。K 按元素数，括号内为小时。\n")
    p("### 表甲：事件总数、第 1 条（平稳日）、第 2 条（三月尾段）、第 3 条（尺度漂移日）\n")
    p("| 组合 | 候选 | 确认 | 撤销 | 平稳日确认 | 平稳日候选 | 开始核对 | 撤销 | 确认 | 未有结论 | 挡掉的比例 | 尾段有确认的设备 | 尾段候选 | "
      "有确认的尺度漂移日 / 总数 | 其上确认 F / T | 敏感性：有确认 / 总数 |")
    p("| --- |" + " --- |" * 15)
    for s in summary:
        c = (s["confidence"], s["robustness"], s["granularity"], s["k"])
        p("| %s%s | %d | %d | %d | %d | %d | %d | %d | %d | %d | %s | %d / 8 | %d | %d / %d | %d / %d | %d / %d |" % (
            label(c), "（默认）" if c == DEFAULT else "", s["candidates"], s["confirms"], s["revokes"],
            s["i1_confirms_on_stable"], s["i1_candidates_on_stable"], s["i1_check_starting"], s["i1_revoked"],
            s["i1_confirmed"], s["i1_unresolved"], s["i1_blocked_ratio"] or "—", s["i2_devices_confirmed"], s["i2_candidates"],
            s["i3_days_confirmed"], s["i3_days"], s["i3_F"], s["i3_T"], s["i3g_days_confirmed"], s["i3g_days"]))
    p("\n平稳日确认按确认时刻所在的日子计；平稳日候选及其细分按候选时刻所在的日子计，核对结论归属到候选所在的日子（口径第三节第 1 条）。\n")
    p("### 表乙：第 4 条（C 日照与傍晚气体）、第 5 条（盲区内错过）、第 7 条（注入）、触发检验的分解与「方差显著但均值未升」计数\n")
    p("| 组合 | C 日照段有候选 / 有确认（共 5） | 傍晚气体有候选 / 有确认（共 8） | 盲区内错过 | 注入有候选 / 有确认（共 %d） | "
      "确认中 F 的比例 | 方差显著但均值未升：全程 / 平稳日 / 尾段 / 第 4 条各段 / 注入段 |" % sum(1 for r in rows_out["segments"]
                                                                       if r["item"] == "7" and r["combo"] == summary[0]["combo"]))
    p("| --- | --- | --- | --- | --- | --- | --- |")
    for s in summary:
        c = (s["confidence"], s["robustness"], s["granularity"], s["k"])
        p("| %s%s | %d / %d | %d / %d | %d | %d / %d | %s | %d / %d / %d / %d / %d |" % (
            label(c), "（默认）" if c == DEFAULT else "", s["i4_sun_candidate"], s["i4_sun_confirm"],
            s["i4_evening_candidate"], s["i4_evening_confirm"], s["i5_blind_misses"], s["i7_with_candidate"],
            s["i7_with_confirm"], s["F_share"] or "—", s["fwr_total"], s["fwr_stable"], s["i2_fwr"], s["i4_fwr"], s["i7_fwr"]))
    p("\n「方差显著但均值未升」是检测器的 `fWithoutRise` 计数：F 检验显著、新段均值没有高于历史段，因而没有报警（单侧实现漏掉的情形，"
      "理论线回传第十八条）。全程与平稳日分开给出；各段的计数按段本身的区间，不加 6 小时尾长。\n")

    p("## 三、基线：加权霍夫丁检测器（river 0.26 `HDDMW` 默认参数，输入超阈标记，十分钟粒度）\n")
    p("| 设备 | 基线候选总数 | 三月尾段 | 尺度漂移日命中 / 总数 | C 日照段命中 | 傍晚气体 | 注入命中 / 总数 |")
    p("| --- | --- | --- | --- | --- | --- | --- |")
    for d in DEVICES:
        b = base_seg[d]
        p("| %s | %d | %s | %d / %d | %s | %s | %s |" % (d, b["total"], yn(b["tail"]), b["scale_hit"], b["scale_n"],
                                                     ("%d / 5" % b["sun"]) if d == "C" else "—", yn(b["evening"]),
                                                     ("%d / %d" % (b["inj"], b["inj_n"])) if b["inj_n"] else "—"))
    p("\n基线看超阈标记序列，主检测器看主分，两者输入不同，比较的是对同一段时间的判断（口径第一节）。\n")

    p("## 四、默认组合的逐台明细\n")
    cid = combo_id(DEFAULT)
    p("### 第 1 条：平稳日\n")
    p("| 设备 | 平稳日天数 | 确认 | 候选 | 开始核对 | 核对中到达 | 撤销 | 确认 | 未有结论 | 挡掉的比例 | 方差显著但均值未升（平稳日 / 全程） |")
    p("| --- |" + " --- |" * 10)
    for r in rows_out["item1_per_device"]:
        if r["combo"] == cid:
            p("| %s | %d | %d | %d | %d | %d | %d | %d | %d | %s | %d / %d |" % (
                r["device"], r["stable_days"], r["confirms_on_stable"], r["candidates_on_stable"], r["check_starting"],
                r["during_check"], r["revoked"], r["confirmed"], r["unresolved"], r["blocked_ratio"] or "—",
                r["fwr_stable"], r["fwr_total"]))
    days = [r for r in rows_out["item1_per_day"] if r["combo"] == cid]
    p("\n有事件的平稳日：%s\n" % ("；".join("%s %s 确认 %d 候选 %d" % (r["device"], r["day"], r["confirms"], r["candidates"])
                                       for r in days) or "无"))
    for item, title in (("2", "第 2 条：三月尾段"), ("3", "第 3 条（按段归属）与第 5 条：尺度漂移日"),
                        ("4", "第 4 条：C 日照直射与傍晚气体上升"), ("7", "第 7 条：注入（不剔除注入的元素流）")):
        p("### %s\n" % title)
        p("| 段 | 设备 | 候选 | 第一个候选的检验 | 到候选（分钟 / 元素） | 有确认 | 第一次确认（UTC / 当地） | 检验 | "
          "到确认（分钟 / 元素） | 方差显著但均值未升 | 盲区内错过 | 注 |")
        p("| --- |" + " --- |" * 11)
        for r in rows_out["segments"]:
            if r["combo"] == cid and r["item"] == item:
                note = r["note"] or (("评分从 " + LATE_START[r["device"]] + " 才开始") if item == "2" and r["device"] in LATE_START else "")
                dl = lambda m, n: ("%s / %s" % (m, n)) if m != "" else "—"
                p("| %s | %s | %d | %s | %s | %s | %s | %s | %s | %d | %s | %s |" % (
                    r["segment"], r["device"], r["candidates"], r["first_candidate_test"] or "—",
                    dl(r["cand_delay_min"], r["cand_delay_elements"]), yn(r["has_confirm"]),
                    ("%s / %s" % (r["first_confirm_utc"], r["first_confirm_local"])) if r["first_confirm_utc"] else "—",
                    r["first_confirm_test"] or "—", dl(r["confirm_delay_min"], r["confirm_delay_elements"]),
                    r["fwr"], yn(r["blind_miss"]), note))
        p("")
    p("### 第 3 条：按确认时刻所在的日子\n")
    p("| 列 | 设备 | 日 | 超标通道（宽度倍数） | 确认 | F | T |")
    p("| --- | --- | --- | --- | --- | --- | --- |")
    for r in rows_out["item3_scale_days"]:
        if r["combo"] == cid and r["confirms"]:
            p("| %s | %s | %s | %s | %d | %d | %d |" % (r["column"], r["device"], r["day"], r["channels_over"], r["confirms"],
                                                     r["F"], r["T"]))
    p("\n（只列有确认的日子；全部尺度漂移日见 `item3_scale_days.csv`。）\n")
    p("### 触发检验的分解（逐台）\n")
    p("| 设备 | 确认 | F | T | F 的比例 |")
    p("| --- | --- | --- | --- | --- |")
    for r in rows_out["trigger_tests"]:
        if r["combo"] == cid:
            p("| %s | %d | %d | %d | %s |" % (r["device"], r["confirms"], r["F"], r["T"], r["F_share"] or "—"))
    p("\n### 每一次确认的幅度与候选时刻的历史标准差\n")
    p("确认的幅度 =（确认时候选之后最近六个元素的中位数 − 参照值）÷ 候选时刻的历史段标准差；参照值为候选时刻的历史段中位数。"
      "历史标准差为 0 时幅度留空。确认要求中位数高于参照值 + 鲁棒度 × 历史标准差，所以幅度按构造必然大于鲁棒度；以主分为单位的绝对"
      "幅度是「幅度 × 历史标准差」，即最近六个的中位数减参照值。只报告、不设门限（2026-10-21 补充）。全部组合见 `confirms.csv`。\n")
    p("| 设备 | 确认（UTC / 当地） | 候选（UTC） | 检验 | 平稳日 | 参照值 | 最近六个的中位数 | 历史标准差 | 确认的幅度 |")
    p("| --- |" + " --- |" * 8)
    for r in rows_out["confirms"]:
        if r["combo"] == cid:
            p("| %s | %s / %s | %s | %s | %s | %.4g | %.4g | %.4g | %s |" % (
                r["device"], r["confirm_utc"], r["confirm_local"], r["candidate_utc"], r["test"],
                yn(r["on_stable_day"]), float(r["reference"]), float(r["last_median"]), float(r["hist_sd"]),
                ("%.3f" % float(r["confirm_magnitude"])) if r["confirm_magnitude"] else "—"))
    p("\n## 五、输入摘要\n")
    p("平稳日（逐台）：%s。\n" % "；".join("%s %d 天" % (d, len(stable[d])) for d in DEVICES))
    p("尺度漂移日（逐台，主表 / 敏感性）：%s。\n" % "；".join("%s %d / %d" % (d, len(scale[d]), len(gas[d])) for d in DEVICES))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")


# ---------------------------------------------------------------- 自检 / self-test
def self_test():
    seg = dict(start=1000, end=2000, device="X")
    assert attributed(1000, seg) and attributed(2000 + ATTR_TAIL, seg) and not attributed(999, seg)
    assert not attributed(2001 + ATTR_TAIL, seg)
    u = 1000                                              # 时刻单位放大，使 6 小时尾长不覆盖后面的候选 / scale
    ev = [dict(device="X", ts=100 * u, level="CANDIDATE", during_check=0, candidate_ts=100 * u, test="F"),
          dict(device="X", ts=130 * u, level="CANDIDATE", during_check=1, candidate_ts=130 * u, test="T"),
          dict(device="X", ts=160 * u, level="REVOKE", during_check=0, candidate_ts=100 * u, test="F"),
          dict(device="X", ts=300 * u, level="CANDIDATE", during_check=0, candidate_ts=300 * u, test="T")]
    oc = gate_outcomes(ev)
    assert oc[("X", 100 * u)] == ("REVOKE", 160 * u) and oc[("X", 300 * u)] is None and ("X", 130 * u) not in oc
    elem = {"X": list(range(0, 1000 * u, 10 * u))}
    b = blind_intervals(ev, elem)
    assert b["X"][0] == (100 * u, 400 * u) and b["X"][2] == (300 * u, 600 * u), b
    st = segment_stats(dict(start=620 * u, end=700 * u, device="X"), ev, [("X", 650 * u)], b)
    assert st["blind_miss"] == 0 and st["candidates"] == 0 and st["fwr"] == 1
    st = segment_stats(dict(start=420 * u, end=500 * u, device="X"), ev, [], b)
    assert st["blind_miss"] == 1 and st["candidates"] == 0
    st = segment_stats(dict(start=50 * u, end=120 * u, device="X"), ev, [], b)
    assert st["candidates"] == 2 and st["blind_miss"] == 0 and st["first_candidate_test"] == "F"
    assert st["has_confirm"] == 0 and st["cand_delay"][1] == "" and st["confirm_delay"] == ("", "")
    st = segment_stats(dict(start=50 * u, end=120 * u, device="X"), ev, [], b, elem)
    assert st["cand_delay"] == ("%.0f" % (50 * u / 60.0), 5), st["cand_delay"]
    assert local(BST_START - 60).endswith("00:59") and local(BST_START).endswith("02:00")
    print("自检通过。")
    return 0


# ---------------------------------------------------------------- 主程序 / main
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--streams-dir", default="docs/m5b")
    ap.add_argument("--tables-dir", default="docs/m5b/tables")
    ap.add_argument("--stable", default="docs/m3_inject/v34/v34_daily.csv")
    ap.add_argument("--profile", default="docs/m3_marapr/daily_channel_profile_ref0308.csv")
    ap.add_argument("--inject", default="docs/m3_a4/full_fix/inject-truth.csv")
    ap.add_argument("--out-dir", default="docs/m5b/grid")
    ap.add_argument("--report", default="docs/reports/m5b_offline_grid.md")
    ap.add_argument("--java-check", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    if a.self_test:
        return self_test()
    need = [a.stable, a.profile, a.inject] + [os.path.join(a.streams_dir, "%s%s.csv" % (GRAN[g][0], s))
                                              for g in GRANS for s in ("", "_with_injection")]
    need += [os.path.join(a.tables_dir, "thesis_%s_%s_%d.csv" % (c, r, GRAN[g][3])) for c in CONFS for r in RHOS for g in GRANS]
    miss = [f for f in need if not os.path.exists(f)]
    try:
        import river  # noqa: F401
    except ImportError:
        miss.append("python 包 river（pip install river==0.26.1）")
    if miss:
        print("ERROR: 缺少输入：\n  " + "\n  ".join(miss), file=sys.stderr)
        return 2

    java_note, java_rc = "本次未运行（加 --java-check 运行 `m5b/check_equality_grid.py`）。", 0
    if a.java_check:
        res = []
        for flag in ([], ["--with-injection"]):
            r = subprocess.run([sys.executable, "m5b/check_equality_grid.py"] + flag, capture_output=True, text=True)
            res.append("%s：%s" % ("不剔除注入" if flag else "剔除注入", (r.stdout.strip().splitlines() or ["无输出"])[-1]))
            java_rc |= r.returncode != 0
        java_note = "；".join(res) + "（`m5b/check_equality_grid.py`）。"

    stable = load_stable(a.stable)
    scale, gas = load_scale_days(a.profile)
    inj_segs = load_injections(a.inject)
    segs_main = segments(scale, [])
    ev_dir = os.path.join(a.out_dir, "events")
    os.makedirs(ev_dir, exist_ok=True)

    data = {}
    for g in GRANS:
        base, tcol, vcol, _, _ = GRAN[g]
        for suf in ("", "_with_injection"):
            rows = ow.read_stream(os.path.join(a.streams_dir, base + suf + ".csv"), tcol, vcol)
            elem = collections.defaultdict(list)
            for dv, ts, _ in rows:
                elem[dv].append(ts)
            data[(g, suf)] = (rows, elem)

    rows_out = collections.defaultdict(list)
    summary = []
    for c in itertools.product(CONFS, RHOS, GRANS, KS):
        table = ow.load_table(os.path.join(a.tables_dir, "thesis_%s_%s_%d.csv" % (c[0], c[1], GRAN[c[2]][3])))
        rows, elem = data[(c[2], "")]
        rows_i, elem_i = data[(c[2], "_with_injection")]
        events, fwr = run_combo(rows, table, c[3])
        ev_inj, fwr_inj = run_combo(rows_i, table, c[3])
        ow.write_events(events, os.path.join(ev_dir, combo_id(c) + ".csv"))
        ow.write_events(ev_inj, os.path.join(ev_dir, combo_id(c) + "_with_injection.csv"))
        summary.append(analyse(c, events, fwr, elem, stable, scale, gas, segs_main, ev_inj, fwr_inj, elem_i, inj_segs,
                               rows_out))
        print("完成 %s：事件 %d 条" % (combo_id(c), len(events)))

    # 基线 / baseline
    base = run_baseline(os.path.join(a.streams_dir, "streams_10min.csv"))
    base_i = run_baseline(os.path.join(a.streams_dir, "streams_10min_with_injection.csv"))
    base_rows = [dict(device=d, window_end=t, utc=utc(t), local=local(t), stream="剔除注入") for d, t in base]
    base_rows += [dict(device=d, window_end=t, utc=utc(t), local=local(t), stream="不剔除注入") for d, t in base_i]
    base_seg = {}
    for d in DEVICES:
        ts = [t for dv, t in base if dv == d]
        hit = lambda seg, pool=ts: any(attributed(t, seg) for t in pool)
        mine = [s for s in segs_main if s["device"] == d]
        inj = [s for s in inj_segs if s["device"] == d]
        base_seg[d] = dict(total=len(ts), tail=hit([s for s in mine if s["item"] == "2"][0]),
                           scale_hit=sum(hit(s) for s in mine if s["item"] == "3"),
                           scale_n=sum(1 for s in mine if s["item"] == "3"),
                           sun=sum(hit(s) for s in mine if s["name"].startswith("C 日照")),
                           evening=hit([s for s in mine if s["name"].startswith("04-17 傍晚")][0]),
                           inj=sum(any(attributed(t, s) for dv, t in base_i if dv == d) for s in inj), inj_n=len(inj))
        for s in mine + inj:
            pool = ts if s["item"] != "7" else [t for dv, t in base_i if dv == d]
            rows_out["baseline_segments"].append(dict(item=s["item"], segment=s["name"], device=d,
                                                      hit=int(any(attributed(t, s) for t in pool))))

    write_csv(os.path.join(a.out_dir, "summary.csv"), summary)
    for name, rows in rows_out.items():
        write_csv(os.path.join(a.out_dir, name + ".csv"), rows)
    write_csv(os.path.join(a.out_dir, "baseline_hddmw_events.csv"), base_rows)
    os.makedirs(os.path.dirname(a.report) or ".", exist_ok=True)
    report(a.report, summary, rows_out, base_rows, base_seg, java_note, scale, gas, stable, a.out_dir)
    print("报告 → %s；长表 → %s/" % (a.report, a.out_dir))
    return 1 if java_rc else 0


if __name__ == "__main__":
    sys.exit(main())
