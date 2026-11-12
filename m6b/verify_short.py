#!/usr/bin/env python3
# ============================================================================
# m6b/verify_short.py
# 适应执行第二批 V-M6b-3 短重放的核对（1.41 版裁决第四节）：读 deploy/scripts/syn-m6b-verify.sh 取回的两次运行（a、b），逐项核对
#   一、两次运行逐位相同：全部评分、漂移事件、m1-out 摘要；G 的监测快照中按处理时间发出的状态快照（训练态 windowEnd = -1、性能层
#       windowEnd = -2）只报条数，其余快照去掉训练缓冲占用（排空期间随墙钟变化）后逐位相同，并列出原样不同的字段；
#   二、一级命令转发一次：数据接入模块的「Level 1 forwarded」恰好一条，且没有执行零级；
#   三、一级训练集范围：上下文模块记录的范围为 [生效, 生效 + 3 × 86,400)，生效后侧、冷启动；
#   四、切换：G 的上下文评分在切换前模型代数为空，切换后为一；第一条新模型评分的窗口末不早于生效后三天；版本号前后一致（无零级）；
#   五、每个在线窗口恰好一条评分：每六十个点通道会接纳的 G 的轮恰好一条评分（评分的窗口末为窗口末轮的到达滑动步末端），窗口末互不相同；
#   六、性能层在模型代数改变时清空一次；
#   七、没有一级失败标记。
# 结果写成 verify_report.md 并打印到终端；只报事实，不改任何参数。
# Checks for the V-M6b-3 short replays.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 集群 master（由编排脚本调用）或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m6b/verify_short.py --dir docs/m6b/verify [--effective-day 2022-03-29] [--window 60] [--n-days 3]
# 3. 前置条件 / Preconditions: --dir 下 a/ 与 b/ 各有 scores.jsonl、monitoring_G.jsonl、drift.jsonl、m1out.sha、m1out_G.jsonl、
#    tm_log_G.txt（编排脚本收集）；只有 a 时跳过第一项。
# 4. 期望产出 / Expected output: --dir/verify_report.md；--dir/monitoring_diff.txt（两次运行的监测快照去掉随墙钟变化的字段后
#    仍对不上的行）；终端逐项打印「[通过]」或「[失败]」。
# 5. 失败兜底 / Failure fallback: 任何一项失败时退出码 1；缺文件时退出码 2。
# ============================================================================
import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime
from zoneinfo import ZoneInfo


def lines(path):
    with open(path, encoding="utf-8") as fh:
        return [l.rstrip("\n") for l in fh if l.strip()]


def jsonl(path):
    out = []
    for l in lines(path):
        try:
            out.append(json.loads(l))
        except json.JSONDecodeError:
            pass
    return out


def digest(ls):
    return hashlib.sha256("\n".join(sorted(ls)).encode("utf-8")).hexdigest()


WALL_CLOCK = ("m3BufferRounds",)


# 按处理时间发出的状态快照：训练态快照（windowEnd = -1，训练期间按轮询定时器发出）与性能层快照（windowEnd = -2，每六十秒
# 处理时间发出一次）。它们的条数与采样到的元素随墙钟快慢而变，按设计不参与逐位比较，只报条数。
# Status snapshots emitted on processing-time timers: excluded from the bitwise comparison, counts reported.
PROC_TIME = (-1, -2)


def normalize(x):
    y = {k: v for k, v in x.items() if k not in WALL_CLOCK}
    return json.dumps(y, sort_keys=True)


def compare_monitoring(xa, xb, dump=None):
    """返回（去掉随墙钟变化的字段后是否相同，说明）/ (equal after removing wall-clock fields, text)."""
    from collections import Counter, defaultdict
    status = [sum(1 for x in xs if x.get("windowEnd") in PROC_TIME) for xs in (xa, xb)]
    xa = [x for x in xa if x.get("windowEnd") not in PROC_TIME]
    xb = [x for x in xb if x.get("windowEnd") not in PROC_TIME]
    raw_a = Counter(json.dumps(x, sort_keys=True) for x in xa)
    raw_b = Counter(json.dumps(x, sort_keys=True) for x in xb)
    norm_equal = Counter(normalize(x) for x in xa) == Counter(normalize(x) for x in xb)
    only_a = raw_a - raw_b
    fields = Counter()
    by_norm = defaultdict(list)
    for line in (raw_b - raw_a).elements():
        x = json.loads(line)
        by_norm[normalize(x)].append(x)
    for line in only_a.elements():
        x = json.loads(line)
        cand = by_norm.get(normalize(x))
        if cand:
            y = cand.pop()
            for k in set(x) | set(y):
                if x.get(k) != y.get(k):
                    fields[k] += 1
    raw_diff = sum(only_a.values())
    if dump:
        # 去掉随墙钟变化的字段后仍对不上的行，原样写出（各最多五十行）/ unmatched lines after normalization, verbatim
        na = Counter(normalize(x) for x in xa)
        nb = Counter(normalize(x) for x in xb)
        with open(dump, "w", encoding="utf-8") as fh:
            for tag, extra in (("只在运行 a", na - nb), ("只在运行 b", nb - na)):
                fh.write("## %s（去掉随墙钟变化的字段后），共 %d 行\n" % (tag, sum(extra.values())))
                for line in list(extra.elements())[:50]:
                    fh.write(line + "\n")
    txt = "monitoring_G.jsonl 按处理时间发出的状态快照 %d/%d 条不参与比较；其余 %d/%d 行，原样不同 %d 行%s；去掉%s后%s" % (
        status[0], status[1], len(xa), len(xb), raw_diff,
        ("（不同的字段：" + "、".join("%s %d 行" % kv for kv in fields.most_common()) + "）") if fields else "",
        "、".join(WALL_CLOCK), "相同" if norm_equal else "仍不同")
    return norm_equal, txt


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="docs/m6b/verify")
    ap.add_argument("--effective-day", default="2022-03-29")
    ap.add_argument("--window", type=int, default=60)
    ap.add_argument("--n-days", type=int, default=3)
    ap.add_argument("--slide", type=int, default=60, help="点通道滑动步（秒）")
    a = ap.parse_args()
    eff = int(datetime.strptime(a.effective_day, "%Y-%m-%d").replace(tzinfo=ZoneInfo("Europe/London")).timestamp())
    runs = [r for r in ("a", "b") if os.path.isdir(os.path.join(a.dir, r))]
    need = ["scores.jsonl", "monitoring_G.jsonl", "drift.jsonl", "m1out.sha", "m1out_G.jsonl", "tm_log_G.txt"]
    for r in runs:
        for f in need:
            if not os.path.exists(os.path.join(a.dir, r, f)):
                print("ERROR: 缺少 %s/%s/%s" % (a.dir, r, f), file=sys.stderr)
                return 2
    if not runs:
        print("ERROR: %s 下没有运行目录" % a.dir, file=sys.stderr)
        return 2
    results = []

    def check(name, ok, detail):
        results.append((name, ok, detail))
        print("[%s] %s：%s" % ("通过" if ok else "失败", name, detail))

    # 一、两次运行逐位相同
    if len(runs) == 2:
        rows = []
        same = True
        for f in ("scores.jsonl", "drift.jsonl", "m1out_G.jsonl"):
            da, db = (digest(lines(os.path.join(a.dir, r, f))) for r in runs)
            na, nb = (len(lines(os.path.join(a.dir, r, f))) for r in runs)
            rows.append("%s %d/%d 行%s" % (f, na, nb, "相同" if da == db else "不同"))
            same &= da == db
        # G 的监测快照：按处理时间发出的状态快照只报条数；其余快照的训练缓冲占用（m3BufferRounds）在排空期间取处理时间下的
        # 缓冲大小，按补遗四的设计随训练的墙钟快慢而变；去掉它后要求逐位相同，并列出原样比较时不同的字段。
        # Monitoring snapshots: compared with the two wall-clock-dependent fields removed; differing raw fields are listed.
        ok_m, txt_m = compare_monitoring(*(jsonl(os.path.join(a.dir, r, "monitoring_G.jsonl")) for r in runs),
                                         dump=os.path.join(a.dir, "monitoring_diff.txt"))
        rows.append(txt_m)
        same &= ok_m
        sa, sb = (open(os.path.join(a.dir, r, "m1out.sha"), encoding="utf-8").read().strip() for r in runs)
        rows.append("m1-out 摘要%s" % ("相同" if sa == sb else "不同"))
        same &= sa == sb
        check("两次运行逐位相同", same, "；".join(rows))
    else:
        check("两次运行逐位相同", False, "只有运行 %s，无法比较" % runs[0])

    for r in runs:
        d = os.path.join(a.dir, r)
        log = lines(os.path.join(d, "tm_log_G.txt"))
        # 二、命令转发
        fw = [l for l in log if "[M6a] Level 1 forwarded" in l and "device G " in l]
        l0 = [l for l in log if "[M6a] Level 0 executed" in l]
        check("运行 %s：一级命令转发一次" % r, len(fw) == 1 and not l0, "转发 %d 条，零级执行 %d 条" % (len(fw), len(l0)))
        # 三、训练集范围
        sp = [l for l in log if re.search(r"\[M3\] Device G level 1 \(seq \d+, effective %d\): training set \[" % eff, l)]
        m = re.search(r"training set \[(\d+), (\d+)\)", sp[0]) if sp else None
        ok = bool(m) and int(m.group(1)) == eff and int(m.group(2)) == eff + a.n_days * 86400 and "after" in sp[0] \
            and "cold start" in sp[0]
        check("运行 %s：一级训练集范围" % r, ok, sp[0].split("[M3] ", 1)[1] if sp else "没有找到一级的范围记录")
        sw = [l.split("[M3] ", 1)[1] for l in log if "[M3] Device G level-1 switch" in l]
        # 四、切换
        sc = sorted((s for s in jsonl(os.path.join(d, "scores.jsonl"))
                     if s.get("device") == "G" and s.get("channel") == "m3_context"), key=lambda s: s["windowEnd"])
        first_new = next((s for s in sc if s.get("model") == 1), None)
        bad = [s for s in sc if (s.get("model") is not None) != (first_new is not None and s["windowEnd"] >= first_new["windowEnd"])]
        versions = sorted({s.get("version") for s in sc}, key=str)
        ok = first_new is not None and not bad and first_new["windowEnd"] >= eff + a.n_days * 86400 \
            and all(s.get("model") in (None, 1) for s in sc) and len(versions) == 1
        check("运行 %s：切换前后的模型代数与版本号" % r, ok,
              "评分 %d 条；第一条新模型评分窗口末 %s（生效后 %.2f 天）；不一致 %d 条；版本号 %s；切换日志：%s" % (
                  len(sc), first_new["windowEnd"] if first_new else "无",
                  (first_new["windowEnd"] - eff) / 86400 if first_new else float("nan"), len(bad), versions,
                  sw[0] if sw else "无"))
        # 五、每个在线窗口恰好一条评分。评分的窗口末是窗口最后一轮「到达的滑动步」的末端（点通道按到达滑动步转交，
        # 末端 = ⌊ts / 滑动⌋ × 滑动 + 滑动），不是最后一轮的时间戳。所以按接纳轮的序号核对：存在偏移 c0，使第 k 条评分的窗口末
        # 恰为第 c0 + 60k 个接纳轮的到达滑动步末端（k 从一起），对全部评分成立，即每 60 个接纳轮恰好一条评分、无丢失无重复。
        # Score k ends at the arrival-slide end of admitted round c0 + 60k for one offset c0, for every k.
        adm = sorted(x["ts"] for x in jsonl(os.path.join(d, "m1out_G.jsonl"))
                     if not x.get("warmup") and not any(x.get("missingMask") or []))
        ends = [s["windowEnd"] for s in sc]
        slide_end = lambda t: (t // a.slide + 1) * a.slide
        best = None
        for c0 in range(len(adm)):
            if c0 + a.window - 1 >= len(adm) or slide_end(adm[c0 + a.window - 1]) > ends[0]:
                break
            if slide_end(adm[c0 + a.window - 1]) != ends[0]:
                continue
            miss = next((k for k in range(len(ends)) if c0 + a.window * (k + 1) - 1 >= len(adm)
                         or slide_end(adm[c0 + a.window * (k + 1) - 1]) != ends[k]), None)
            if best is None or (miss if miss is not None else len(ends)) > best[1]:
                best = (c0, miss if miss is not None else len(ends))
            if miss is None:
                break
        ok = best is not None and best[1] == len(ends) and len(set(ends)) == len(ends)
        check("运行 %s：每个在线窗口恰好一条评分" % r, ok,
              "评分 %d 条，窗口末重复 %d 个；%s" % (len(ends), len(ends) - len(set(ends)),
                  "找不到与第一条评分对齐的接纳轮" if best is None else
                  "从第 %d 个接纳轮起每 %d 轮一条，与 %d 条评分中的前 %d 条逐条对上%s" % (
                      best[0] + 1, a.window, len(ends), best[1],
                      "" if best[1] == len(ends) else "，第 %d 条（窗口末 %d）对不上" % (best[1] + 1, ends[best[1]]))))
        # 六、性能层清空
        cl = [l for l in log if "[M5b] Device G model generation 1" in l]
        check("运行 %s：性能层在模型代数改变时清空一次" % r, len(cl) == 1, "清空记录 %d 条" % len(cl))
        # 七、没有一级失败
        fl = [x for x in jsonl(os.path.join(d, "monitoring_G.jsonl")) if x.get("m3Level1Failed")]
        ab = [l for l in log if "level-1 retrain" in l and "abandoned" in l]
        check("运行 %s：没有一级失败" % r, not fl and not ab, "失败标记 %d 条，放弃日志 %d 条" % (len(fl), len(ab)))

    out = ["# V-M6b-3 短重放核对（m6b/verify_short.py 生成）\n\n",
           "一级生效 %s 当地零点（%d），N = %d 天，窗口 %d 轮。\n\n| 核对项 | 结果 | 详情 |\n| --- | --- | --- |\n" % (
               a.effective_day, eff, a.n_days, a.window)]
    for name, ok, detail in results:
        out.append("| %s | %s | %s |\n" % (name, "通过" if ok else "失败", detail.replace("|", "\\|")))
    with open(os.path.join(a.dir, "verify_report.md"), "w", encoding="utf-8") as fh:
        fh.write("".join(out))
    return 0 if all(ok for _, ok, _ in results) else 1


if __name__ == "__main__":
    sys.exit(main())
