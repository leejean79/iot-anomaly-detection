#!/usr/bin/env python3
# ============================================================================
# m6b/full_check.py
# 适应执行第二批 V-M6b-4（全量重放）的核对与报告表格（交接文档第七节；1.43、1.44、1.45 版裁决）。读 deploy/scripts/syn-m6b-full.sh
# 收集的两次运行（a、b）与链式离线对照（docs/m6b/chain/D7K_up，D7K 打开升级格、在第一条一级或二级命令的决定时刻停止），给出：
#   一、断言八条：每次运行的重放完整性核验（syn-replay-verify.sh --adapt）退出码为零，作业重启次数为零；
#   二、等值第一段：在链式离线的停止时刻（第一条一级或二级命令的决定时刻）之前，聚合模块三份日志（自 03-08 起）与链式离线逐条相同，
#       点通道到达滑动步的离群轮集合相同，数据层日记录的离散字段（可判、原因、偏离、读数数、版本）逐条相同、连续字段报最大相对差
#       （方法手册第五条：跨主机只保证离散决策量相同）；在线第一条一级或二级命令与链式的停止命令相同；这一段内上下文评分条数（应为零，
#       上下文通道仍在收集）；
#   三、等值第二段：两次在线运行逐位相同。评分、漂移事件、告警与命令主题、数据层日记录、影子评分快照、上下文模块的标记快照逐行相同
#       （各快照去掉随墙钟变化的训练缓冲占用），m1-out 摘要相同，监测快照的规范化摘要相同（不含按处理时间发出的训练态快照
#       windowEnd = -1 与性能层快照 windowEnd = -2，去掉训练缓冲占用；1.43 版裁决接受的排除口径）；
#   四、影子评分表：每条零级命令生效后七天内该台的正式超阈率（按训练版本）、影子超阈率（按当前版本）与性能层在正式评分上的确认数；
#   五、时延表：三月事件、G 的光照、气体抬高三个片段，逐台从第一条信号到零级生效、到一级命令生效、到一级切换；
#   六、性能表：检查点体积、一级与二级重训的训练耗时与轮数、冷启动与重训之后的排空时长、切换前验证的比值。
# 只计数、比较与列表，不下结论。另有一个子命令 --normalize-monitoring，由编排在 master 上对完整的监测快照转储计算规范化摘要。
# Checks and tables for the V-M6b-4 full replays; counts and comparisons only.
#
# 写定的口径（运行前写定，随本脚本提交）：
#   - 第一条信号：该台在片段窗口内第一条告警，类别不是「质量问题」，且带数据层通道、点通道标志或上下文标志之一；
#   - 片段窗口（当地日）：三月事件 03-21 至 03-31（八台）；G 的光照 03-08 至 03-20（G）；气体抬高 03-13 至 03-20（B、C、D、E、G、H，
#     且第一条信号的数据层通道含气体）；
#   - 零级生效、一级命令生效：窗口内、不早于第一条信号的第一条相应级别命令的生效时刻；一级切换：该台一级命令生效之后，上下文评分中
#     模型代数第一次增加的那一条评分的窗口末。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 集群 master（由编排调用）或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation:
#      python3 m6b/full_check.py --dir docs/m6b/full --chain docs/m6b/chain/D7K_up        # 核对与报告表格
#      python3 m6b/full_check.py --normalize-monitoring <monitoring.jsonl>                  # 打印「行数 摘要」
# 3. 前置条件 / Preconditions: --dir 下 a/、b/ 为编排收集的运行目录（scores.jsonl、drift.jsonl、alerts.jsonl、adapt.jsonl、
#    m5a_records.jsonl、shadow.jsonl、m3_markers.jsonl、m1out.sha、monitoring_norm.sha、tm_log.txt、online/、replay_verify.rc、
#    jm_states.txt、ckpt_sizes.csv）；--chain 下有链式离线的三份日志、outliers.csv、data_days.csv 与 stopped_at.txt。
# 4. 期望产出 / Expected output: --dir/full_report_tables.md；终端逐项打印「[通过]」或「[失败]」。
# 5. 失败兜底 / Failure fallback: 任何一项失败时退出码 1；缺文件时该项记为失败并写明缺什么。
# ============================================================================
import argparse
import bisect
import csv
import hashlib
import json
import os
import re
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(HERE, "m7"))
import compare_logs as CL      # noqa: E402

ZONE = ZoneInfo("Europe/London")
WALL = ("m3BufferRounds",)
PROC_TIME = (-1, -2)
DEVICES = list("ABCDEFGH")


def day_start(day):
    return int(datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=ZONE).timestamp())


def local(ts):
    return datetime.fromtimestamp(int(ts), ZONE).strftime("%m-%d %H:%M")


def lines(path):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return [l.rstrip("\n") for l in fh if l.strip()]


def jsonl(path):
    out = []
    for l in lines(path) or []:
        try:
            out.append(json.loads(l))
        except json.JSONDecodeError:
            pass
    return out


def strip_wall(line):
    try:
        x = json.loads(line)
    except json.JSONDecodeError:
        return line
    if isinstance(x, dict):
        for k in WALL:
            x.pop(k, None)
        return json.dumps(x, sort_keys=True)
    return line


def table(head, body):
    out = "| " + " | ".join(head) + " |\n| " + " | ".join("---" for _ in head) + " |\n"
    return out + "".join("| " + " | ".join(str(c).replace("|", "\\|") for c in r) + " |\n" for r in body)


def normalize_monitoring(path):
    h = []
    with open(path, encoding="utf-8") as fh:
        for l in fh:
            try:
                x = json.loads(l)
            except json.JSONDecodeError:
                continue
            if x.get("windowEnd") in PROC_TIME:
                continue
            for k in WALL:
                x.pop(k, None)
            h.append(json.dumps(x, sort_keys=True))
    h.sort()
    print("%d %s" % (len(h), hashlib.sha256("\n".join(h).encode("utf-8")).hexdigest()))
    return 0


def csv_rows(path):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="docs/m6b/full")
    ap.add_argument("--chain", default="docs/m6b/chain/D7K_up")
    ap.add_argument("--normalize-monitoring")
    a = ap.parse_args()
    if a.normalize_monitoring:
        return normalize_monitoring(a.normalize_monitoring)
    runs = [r for r in ("a", "b") if os.path.isdir(os.path.join(a.dir, r))]
    results, out = [], []

    def check(name, ok, detail):
        results.append((name, ok, detail))
        print("[%s] %s：%s" % ("通过" if ok else "失败", name, detail))

    # ---------------- 一、断言八条
    for r in runs:
        d = os.path.join(a.dir, r)
        rc = (lines(os.path.join(d, "replay_verify.rc")) or ["缺失"])[0]
        tail = (lines(os.path.join(d, "replay_verify.txt")) or ["（无输出）"])[-1]
        restarts = sum(1 for l in lines(os.path.join(d, "jm_states.txt")) or [] if "RESTARTING" in l)
        check("运行 %s：断言八条与重启次数" % r, rc == "0" and restarts == 0,
              "核验退出码 %s（%s）；作业转入重启 %d 次" % (rc, tail, restarts))

    # ---------------- 二、等值第一段
    stop = (lines(os.path.join(a.chain, "stopped_at.txt")) or [""])[0].split(",")
    if len(stop) < 5:
        check("等值第一段", False, "链式离线缺少 stopped_at.txt")
        cut = None
    else:
        decided = int(stop[2])
        cut = decided + 60
        start = day_start("2022-03-08")
        out.append("链式离线停止在 %s 的 %s 级命令（%s 决定，%s 生效；%s）。第一段比较节拍时刻在 %s 至 %s（当地）之间的行。\n\n" % (
            stop[0], stop[4], local(decided), local(int(stop[3])), stop[5] if len(stop) > 5 else "", local(start), local(decided)))
        for r in runs:
            d = os.path.join(a.dir, r)
            # 聚合模块三份日志 / the three M7 logs
            parts, ok = [], True
            for name in ("alerts.csv", "commands.csv", "review.csv"):
                x, y = csv_rows(os.path.join(d, "online", name)), csv_rows(os.path.join(a.chain, name))
                if x is None or y is None:
                    parts.append("%s 缺失" % name)
                    ok = False
                    continue
                x, y = ([w for w in rows if start < CL.tick_time(name, w) <= cut] for rows in (x, y))
                diff = None
                for i, (p, q) in enumerate(zip(x, y)):
                    for k in q:
                        if not CL.same(k, p.get(k), q[k]):
                            diff = "第 %d 行的 %s 不同（%s / %s）" % (i, k, p.get(k), q[k])
                            break
                    if diff:
                        break
                if diff is None and len(x) != len(y):
                    diff = "条数不同（%d / %d）" % (len(x), len(y))
                parts.append("%s %d / %d 条%s" % (name, len(x), len(y), "逐条相同" if diff is None else "，" + diff))
                ok &= diff is None
            # 第一条一级或二级命令 / the first level-1/2 command online
            cmds = csv_rows(os.path.join(d, "online", "commands.csv")) or []
            first = next((c for c in sorted(cmds, key=lambda c: int(c["decided_at"])) if c["level"] != "0"), None)
            same_stop = first is not None and first["device"] == stop[0] and int(first["decided_at"]) == decided \
                and first["level"] == stop[4]
            parts.append("在线第一条一级或二级命令：%s" % (
                "%s %s %s 级（%s）" % (first["device"], first["day"], first["level"], "与链式相同" if same_stop else "与链式不同")
                if first else "没有"))
            ok &= same_stop
            check("运行 %s：等值第一段，聚合模块三份日志" % r, ok, "；".join(parts))
            # 点通道 / point channel
            on = set()
            for s in jsonl(os.path.join(d, "scores.jsonl")):
                if s.get("channel") == "m2_point" and s.get("outlier") and s["windowEnd"] - 60 <= s["roundTs"] < s["windowEnd"] \
                        and s["roundTs"] < decided:
                    on.add((s["device"], int(s["roundTs"])))
            off = {(w["device"], int(w["round_ts"])) for w in csv_rows(os.path.join(a.chain, "outliers.csv")) or []
                   if int(w["round_ts"]) < decided}
            check("运行 %s：等值第一段，点通道到达滑动步的离群轮" % r, on == off,
                  "在线 %d 个，链式 %d 个，只在在线 %d 个，只在链式 %d 个" % (len(on), len(off), len(on - off), len(off - on)))
            # 数据层日记录 / data-layer day records
            stop_day = stop[1]
            ch = {(w["device"], w["day"], w["channel"]): w for w in csv_rows(os.path.join(a.chain, "data_days.csv")) or []
                  if w["day"] <= stop_day}
            onl = {(x["device"], x.get("m5aDay"), x.get("m5aChannel")): x for x in jsonl(os.path.join(d, "m5a_records.jsonl"))
                   if (x.get("m5aDay") or "9") <= stop_day}
            disc_bad, rel = 0, 0.0
            for k, w in ch.items():
                x = onl.get(k)
                if x is None:
                    disc_bad += 1
                    continue
                disc = [(w["evaluable"], "1" if x.get("m5aEvaluable") else "0"), (w["reason"], x.get("m5aReason") or ""),
                        (w["deviated"], "1" if x.get("m5aDeviated") else "0"), (w["n"], str(x.get("m5aN", 0))),
                        (w["version"], str(x.get("version") or 1))]
                disc_bad += any(p != q for p, q in disc)
                for f, g in (("d", "m5aD"), ("p", "m5aP"), ("p10", "m5aP10"), ("median", "m5aMedian"), ("p90", "m5aP90"),
                             ("shift", "m5aShift")):
                    u, v = float(w[f]), float(x.get(g, 0.0))
                    if u != v:
                        rel = max(rel, abs(u - v) / max(abs(u), abs(v)))
            extra = len(set(onl) - set(ch))
            check("运行 %s：等值第一段，数据层日记录" % r, disc_bad == 0 and extra == 0,
                  "链式 %d 条，在线 %d 条；离散字段不同或缺失 %d 条，只在在线 %d 条；连续字段最大相对差 %.3g" % (
                      len(ch), len(onl), disc_bad, extra, rel))
            ctx_before = sum(1 for s in jsonl(os.path.join(d, "scores.jsonl"))
                             if s.get("channel") == "m3_context" and s["windowEnd"] < decided)
            out.append("运行 %s：第一段内上下文评分 %d 条（上下文通道仍在收集时为零）。\n\n" % (r, ctx_before))

    # ---------------- 三、等值第二段
    if len(runs) == 2:
        rows, same = [], True
        for f in ("scores.jsonl", "drift.jsonl", "alerts.jsonl", "adapt.jsonl", "m5a_records.jsonl", "shadow.jsonl",
                  "m3_markers.jsonl"):
            la, lb = (lines(os.path.join(a.dir, r, f)) for r in runs)
            if la is None or lb is None:
                rows.append("%s 缺失" % f)
                same = False
                continue
            eq = sorted(map(strip_wall, la)) == sorted(map(strip_wall, lb))
            rows.append("%s %d/%d 行%s" % (f, len(la), len(lb), "相同" if eq else "不同"))
            same &= eq
        for f in ("m1out.sha", "monitoring_norm.sha"):
            sa, sb = ((lines(os.path.join(a.dir, r, f)) or ["缺失"])[0] for r in runs)
            rows.append("%s %s" % (f, "相同" if sa == sb and sa != "缺失" else "不同"))
            same &= sa == sb and sa != "缺失"
        check("等值第二段：两次在线运行逐位相同", same, "；".join(rows))
    else:
        check("等值第二段：两次在线运行逐位相同", False, "只有运行 %s，无法比较" % (runs[0] if runs else "（无）"))

    base = os.path.join(a.dir, runs[0]) if runs else None
    if base:
        cmds = [c for c in jsonl(os.path.join(base, "adapt.jsonl")) if c.get("type") == "command"]
        shadow = defaultdict(list)
        for x in jsonl(os.path.join(base, "shadow.jsonl")):
            shadow[x["device"]].append((x["windowEnd"], bool(x.get("m3Above")), bool(x.get("m3ShadowAbove"))))
        for v in shadow.values():
            v.sort()
        confirms = defaultdict(list)
        for e in jsonl(os.path.join(base, "drift.jsonl")):
            if e.get("layer") == "performance" and e.get("level") == "CONFIRM":
                confirms[e["device"]].append(e["ts"])
        # ---------------- 四、影子评分表
        body, higher, counted = [], 0, 0
        for c in sorted((c for c in cmds if c.get("level") == 0), key=lambda c: (c["effectiveAt"], c["device"])):
            dev, eff = c["device"], c["effectiveAt"]
            w = [s for s in shadow.get(dev, []) if eff <= s[0] < eff + 7 * 86400]
            nconf = sum(1 for t in confirms.get(dev, []) if eff <= t < eff + 7 * 86400)
            if w:
                fr, sr = sum(s[1] for s in w) / len(w), sum(s[2] for s in w) / len(w)
                counted += 1
                higher += sr > fr
                body.append([dev, c["day"][5:], local(eff), c.get("triggers", ""), len(w), "%.3f" % fr, "%.3f" % sr, nconf])
            else:
                body.append([dev, c["day"][5:], local(eff), c.get("triggers", ""), 0, "—", "—", nconf])
        out.append("## 影子评分表（运行 %s；每条零级命令生效后七天）\n\n影子超阈率高于正式超阈率的命令 %d 条，有在线窗口的命令 %d 条。\n\n" % (
            runs[0], higher, counted))
        out.append(table(["设备", "命令日", "生效（当地）", "触发", "在线窗口", "正式超阈率", "影子超阈率", "性能层确认"], body))
        # ---------------- 五、时延表
        alerts = csv_rows(os.path.join(base, "online", "alerts.csv")) or []
        ocmds = csv_rows(os.path.join(base, "online", "commands.csv")) or []
        ctx = defaultdict(list)
        for s in jsonl(os.path.join(base, "scores.jsonl")):
            if s.get("channel") == "m3_context":
                ctx[s["device"]].append((s["windowEnd"], s.get("model") or 0))
        for v in ctx.values():
            v.sort()
        segs = [("三月事件", "2022-03-21", "2022-03-31", DEVICES, None), ("G 的光照", "2022-03-08", "2022-03-20", ["G"], None),
                ("气体抬高", "2022-03-13", "2022-03-20", list("BCDEGH"), "Gas")]
        body = []
        for name, d0, d1, devs, chan in segs:
            t0, t1 = day_start(d0), day_start((datetime.strptime(d1, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"))
            for dev in devs:
                sig = next((x for x in sorted(alerts, key=lambda x: int(x["hour_start"]))
                            if x["device"] == dev and t0 <= int(x["hour_start"]) < t1 and x["label"] != "质量问题"
                            and (x["data_channels"] or x["point_flag"] == "1" or x["ctx_flag"] == "1")
                            and (chan is None or chan in x["data_channels"].split("|"))), None)
                if sig is None:
                    body.append([name, dev, "无", "—", "—", "—"])
                    continue
                st = int(sig["hour_start"])
                dc = [c for c in ocmds if c["device"] == dev and t0 <= int(c["effective_at"]) < t1 + 7 * 86400
                      and int(c["decided_at"]) >= st]
                l0 = min((int(c["effective_at"]) for c in dc if c["level"] == "0"), default=None)
                l1 = min((int(c["effective_at"]) for c in dc if c["level"] == "1"), default=None)
                sw = None
                if l1 is not None:
                    g0 = max((m for w, m in ctx.get(dev, []) if w < l1), default=0)
                    sw = next((w for w, m in ctx.get(dev, []) if w >= l1 and m > g0), None)
                fmt = lambda t: "—" if t is None else "%s（%.1f 小时）" % (local(t), (t - st) / 3600)
                body.append([name, dev, local(st), fmt(l0), fmt(l1), fmt(sw)])
        out.append("\n## 时延表（运行 %s；括号内为距第一条信号的小时数）\n\n" % runs[0])
        out.append(table(["片段", "设备", "第一条信号", "零级生效", "一级命令生效", "一级切换"], body))
    # ---------------- 六、性能表
    body = []
    for r in runs:
        d = os.path.join(a.dir, r)
        ck = csv_rows(os.path.join(d, "ckpt_sizes.csv")) or []
        per = defaultdict(int)
        for w in ck:
            per[w["ckpt_id"]] += int(w["state_size_bytes"] or 0)
        log = lines(os.path.join(d, "tm_log.txt")) or []
        pending, retr, drains, checks = {}, [], [], []
        for l in log:
            m = re.search(r"\[M3\] Device (\w+) (level-1 retrain|level 2 )", l)
            if m:
                pending[m.group(1)] = "一级" if "level-1" in m.group(2) else "二级"
            m = re.search(r"\[M3\] Device (\w+) trained: .* (\d+) epochs .* early-stop loss=([\d.eE+-]+), ([\d.]+)s", l)
            if m and m.group(1) in pending:
                retr.append("%s %s：%s 轮、%s 秒" % (m.group(1), pending.pop(m.group(1)), m.group(2), m.group(4)))
            m = re.search(r"\[M3\] Device (\w+) buffer drained in ([\d.]+)s", l)
            if m:
                drains.append(float(m.group(2)))
            m = re.search(r"\[M3\] Device (\w+) level-1 switch check: .* ratio ([\d.eE+-]+|NaN|Infinity) .*: (would reject|passes)", l)
            if m:
                checks.append("%s %s（%s）" % (m.group(1), m.group(2)[:6], "判为不该切换" if m.group(3) == "would reject" else "通过"))
        body.append([r, "%.1f MB（第 %s 个）" % (max(per.values()) / 1e6, max(per, key=per.get)) if per else "—",
                     "%.1f MB" % (per[max(per, key=int)] / 1e6) if per else "—",
                     "；".join(retr) or "无", "%d 次，最长 %.1f 秒" % (len(drains), max(drains)) if drains else "无",
                     "；".join(checks) or "无"])
    out.append("\n## 性能表\n\n")
    out.append(table(["运行", "检查点体积最大值", "最后一个检查点", "一级与二级重训", "排空", "切换前验证"], body))

    head = ["# V-M6b-4 全量重放核对（m6b/full_check.py 生成）\n\n", "| 核对项 | 结果 | 详情 |\n| --- | --- | --- |\n"]
    for name, ok, detail in results:
        head.append("| %s | %s | %s |\n" % (name, "通过" if ok else "失败", detail.replace("|", "\\|")))
    with open(os.path.join(a.dir, "full_report_tables.md"), "w", encoding="utf-8") as fh:
        fh.write("".join(head) + "\n" + "".join(out))
    return 0 if results and all(ok for _, ok, _ in results) else 1


if __name__ == "__main__":
    sys.exit(main())
