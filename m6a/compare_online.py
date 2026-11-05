#!/usr/bin/env python3
# ============================================================================
# m6a/compare_online.py
# 验收项三（V-M6a-3）的核对：短重放中对 G 手工下发的零级命令，在线执行与离线模拟逐位或逐条相同；迟到命令为零；两次在线运行逐位相同。
# 由 deploy/scripts/syn-m6a-verify.sh 在 master 上调用，也可在本地对取回的结果单独运行。只用标准库。
# Checks for V-M6a-3: online execution equals the offline simulation; zero late commands; two online runs identical.
#
# 核对项：
#   一、新参数：TaskManager 日志「[M6a] Level 0 executed」里的新中位数、分母、旁路，与 M6aOfflineRunner 的 JSON 逐位相同；
#   二、新参照直方图：m1-out 中 G 第一条版本二的轮所带的直方图，与离线 JSON 逐位相同；
#   三、标准化轮：m1-out 中 G 冻结之后每一轮的版本号与五个标准化值，与模拟器逐轮导出逐位相同；
#   四、点通道判定：评分主题中 G 到达滑动步的离群轮，与模拟器的离群轮逐条相同；
#   五、数据层日记录：G 的逐日逐通道记录（轮数、可判、原因、样本数、D、p、偏离、版本号）与模拟器逐条相同；
#   六、命令执行：两次运行的日志里零级执行各一次、迟到与冻结前命令各为零；
#   七、两次在线运行：m1-out 校验和、评分、漂移事件与日记录排序后逐行相同。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: master 或本地 Mac，仓库根目录，python3.9+。
# 2. 调用命令 / Invocation: python3 m6a/compare_online.py --dir docs/m6a/verify [--device G]
# 3. 前置条件 / Preconditions: --dir 下有 a/、b/ 两次运行的结果与 offline/ 离线模拟结果（由编排脚本生成）。
# 4. 期望产出 / Expected output: 打印七项核对的通过与否，写 --dir/verify_report.md；全部通过退出码 0，否则 1。
# 5. 失败兜底 / Failure fallback: 缺文件的核对项记为失败并写明缺哪个文件。
# ============================================================================
import argparse
import csv
import json
import os
import re
import sys


def jl(path):
    out = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line.startswith("{"):
                    out.append(json.loads(line))
    return out


def same(a, b):
    """逐位相同（浮点数按位比较）/ bitwise equality for floats."""
    if isinstance(a, float) or isinstance(b, float):
        return float(a).hex() == float(b).hex()
    return a == b


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="docs/m6a/verify")
    ap.add_argument("--device", default="G")
    a = ap.parse_args()
    d, dev = a.dir, a.device
    off = os.path.join(d, "offline")
    res = []

    def check(name, ok, note):
        res.append((name, ok, note))
        print("[%s] %s —— %s" % ("通过" if ok else "失败", name, note))

    # ---------- 一、新参数
    log = open(os.path.join(d, "a", "m6a_tm_log.txt"), encoding="utf-8").read() if os.path.exists(os.path.join(d, "a", "m6a_tm_log.txt")) else ""
    m = re.search(r"Level 0 executed: device %s .*?new median \[([^\]]*)\] scale \[([^\]]*)\] bypass \[([^\]]*)\]" % dev, log)
    runner = json.load(open(os.path.join(off, "level0_java.json"))) if os.path.exists(os.path.join(off, "level0_java.json")) else None
    if m and runner:
        med = [float(x) for x in m.group(1).split(",")]
        sc = [float(x) for x in m.group(2).split(",")]
        bp = [x.strip() == "true" for x in m.group(3).split(",")]
        ok = all(same(x, y) for x, y in zip(med, runner["new"]["median"])) and all(
            same(x, y) for x, y in zip(sc, runner["new"]["scale"])) and bp == runner["new"]["bypass"]
        check("一、新参数", ok, "在线 中位数 %s；分母 %s" % (med, sc))
    else:
        check("一、新参数", False, "缺日志里的执行记录或离线 JSON")

    # ---------- 二、新参照直方图；三、标准化轮
    m1 = [r for r in jl(os.path.join(d, "a", "m1out_%s.jsonl" % dev)) if not r.get("warmup")]
    m1.sort(key=lambda r: r["ts"])
    first_new = next((r for r in m1 if r.get("version") == runner_version(runner)), None) if runner else None
    if first_new and first_new.get("refHistograms") and runner:
        ok = True
        for c, h in enumerate(first_new["refHistograms"]):
            o = runner["hist"][c]
            if (h is None) != (o is None):
                ok = False
                continue
            if h is None:
                continue
            ok &= len(h["edges"]) == len(o["edges"]) and all(same(x, y) for x, y in zip(h["edges"], o["edges"]))
            ok &= h["cells"] == o["cells"] and h["m"] == o["m"]
            ok &= all(same(h[k], o[k]) for k in ("p10", "median", "p90"))
        check("二、新参照直方图", ok, "第一条版本 %s 的轮 ts=%d" % (first_new.get("version"), first_new["ts"]))
    else:
        check("二、新参照直方图", False, "m1-out 里没有带直方图的新版本轮，或缺离线 JSON")
    sim = {}
    p = os.path.join(off, "rounds.csv")
    if os.path.exists(p):
        for r in csv.DictReader(open(p, encoding="utf-8")):
            sim[int(r["ts"])] = (int(r["version"]) if r["version"] else None, [float(r["x%d" % i]) for i in range(5)])
    bad, n = [], 0
    for r in m1:
        n += 1
        s = sim.get(r["ts"])
        if s is None or s[0] != r.get("version") or not all(same(x, y) for x, y in zip(s[1], r["xnorm"])):
            bad.append(r["ts"])
    check("三、标准化轮", n > 0 and not bad and n == len(sim), "在线 %d 轮、离线 %d 轮，不同 %d 轮%s" % (
        n, len(sim), len(bad), ("（首个 ts=%d）" % bad[0]) if bad else ""))

    # ---------- 四、点通道判定
    on = set()
    for r in jl(os.path.join(d, "a", "scores.jsonl")):
        if r.get("channel") == "m2_point" and r.get("device") == dev and r["windowEnd"] - 60 <= r["roundTs"] < r["windowEnd"]:
            on.add(r["roundTs"])
    p = os.path.join(off, "outliers.txt")
    offo = {int(x) for x in open(p)} if os.path.exists(p) else None
    if offo is not None and on:
        lo, hi = min(min(on), min(offo)), max(max(on), max(offo))
        check("四、点通道判定", on == offo, "在线 %d 条、离线 %d 条；只在在线 %d、只在离线 %d" % (
            len(on), len(offo), len(on - offo), len(offo - on)))
    else:
        check("四、点通道判定", False, "缺评分主题或离线离群表")

    # ---------- 五、数据层日记录
    onr = {(r["m5aDay"], r["m5aChannel"]): r for r in jl(os.path.join(d, "a", "m5a_records.jsonl")) if r.get("device") == dev}
    p = os.path.join(off, "days.csv")
    offr = {(r["day"], r["channel"]): r for r in csv.DictReader(open(p, encoding="utf-8"))} if os.path.exists(p) else {}
    bad = []
    for k, r in onr.items():
        o = offr.get(k)
        if o is None:
            bad.append(k)
            continue
        pairs = [(r["m5aRounds"], int(o["rounds"])), (r["m5aEvaluable"], o["evaluable"] == "1"), (r["m5aReason"], o["reason"]),
                 (r["m5aN"], int(o["n"])), (r["m5aD"], float(o["d"])), (r["m5aP"], float(o["p"])),
                 (r["m5aDeviated"], o["deviated"] == "1"), (r.get("version"), int(o["version"]) if o["version"] else None)]
        if not all(same(x, y) for x, y in pairs):
            bad.append(k)
    check("五、数据层日记录", bool(onr) and not bad and set(onr) <= set(offr), "在线 %d 条，不同 %d 条%s" % (
        len(onr), len(bad), ("（首个 %s）" % (bad[0],)) if bad else ""))

    # ---------- 六、命令执行
    notes, ok = [], True
    for run in ("a", "b"):
        t = open(os.path.join(d, run, "m6a_tm_log.txt"), encoding="utf-8").read() if os.path.exists(os.path.join(d, run, "m6a_tm_log.txt")) else ""
        ex, late, pre = t.count("Level 0 executed"), t.count("Late command"), t.count("Pre-freeze command")
        ok &= ex == 1 and late == 0 and pre == 0
        notes.append("%s：执行 %d、迟到 %d、冻结前 %d" % (run, ex, late, pre))
    check("六、命令执行（迟到命令为零）", ok, "；".join(notes))

    # ---------- 七、两次在线运行
    diffs = []
    for f in ("m1out.sha", "scores.jsonl", "drift.jsonl", "m5a_records.jsonl"):
        pa, pb = os.path.join(d, "a", f), os.path.join(d, "b", f)
        if not (os.path.exists(pa) and os.path.exists(pb)):
            diffs.append(f + "（缺）")
            continue
        if sorted(open(pa, encoding="utf-8").read().splitlines()) != sorted(open(pb, encoding="utf-8").read().splitlines()):
            diffs.append(f)
    check("七、两次在线运行逐位相同", not diffs, "不同：" + "、".join(diffs) if diffs else "m1-out 校验和、评分、漂移事件、日记录排序后逐行相同")

    perf = re.findall(r"reestimate (\d+) us, total (\d+) us; cache (\d+) rows (\d+) bytes", log)
    with open(os.path.join(d, "verify_report.md"), "w", encoding="utf-8") as fh:
        fh.write("# 适应执行第一批短重放核对（m6a/compare_online.py 生成）\n\n| 核对项 | 结果 | 说明 |\n| --- | --- | --- |\n")
        for name, ok, note in res:
            fh.write("| %s | %s | %s |\n" % (name, "通过" if ok else "失败", note))
        if perf:
            fh.write("\n零级执行的耗时与缓存（运行 a）：重估 %s 微秒，含回填与换参共 %s 微秒；缓存 %s 行、%s 字节。\n" % perf[0])
    return 0 if all(ok for _, ok, _ in res) else 1


def runner_version(runner):
    return runner.get("version") if runner else None


if __name__ == "__main__":
    sys.exit(main())
