#!/usr/bin/env python3
# ============================================================================
# m6c/chain_anchor_report.py
# 第三批第 2 步：零级窗口锚定的链式三档并排（交接文档第一节）与振荡规则一档（第三节）。读 com.leejean.m6.ChainDriver 的输出，
# 按下面写定的预期给出机械判定，列出选定判据的两项计数。只计数与判定，不下成因结论；选定由设计会话宣布。
# Pre-registered checks of the anchored level-0 window (three arms) and of the oscillation rule (one arm).
#
# 三档（全部为七天窗口、湿度与气压不随零级重估、升级格关闭，即第二批收口的链式基线 D7K）：
#   甲 off：现行默认，窗口为生效前七天（docs/m6c/chain/off，与 docs/m6b/chain/D7K 同一配置）；
#   乙 trigger：只有触发通道用锚定窗口；丙 all：全部被重估的通道用锚定窗口（湿度与气压照旧不重估）。
#   锚定窗口起点取「全部数据层触发通道中最早的进入之日的当地零点」与「生效前七天」中较晚者，不足两天回退到生效前七天。
#
# 预期与判定规则（2026-11-24 在乙、丙两档运行之前写定并提交；文字取自交接文档第一、三节）：
#   第一节，乙、丙两档各自与甲比较：
#   一、三月事件中的同向第二条减少：命令日在 03-22 至 03-27 的命令里，A、B、G 三台中「有两条命令共享同一触发通道、且两条在各自
#       命令日该通道的偏移同号」的台数（甲为 3：A 03-24/03-27 温度，B 03-24/03-27 温度，G 03-22/03-26 温度）。少于甲记为「相符」。
#   二、事件后平稳日的伪命令减少：命令日为 03-30 或 04-17、且该日为该台平稳日（docs/m3_inject/v34/v34_daily.csv，与第二批链式
#       报告同一定义）的命令条数（甲为 5：A、B 03-30，B、C、G 04-17）。少于甲记为「相符」。另列全程平稳日命令数（描述）。
#   三、七天内的丙型实例由十一例下降：丙型实例的判别与第二批链式报告判读口径五相同（m6b/chain_report.py 的 type_c），
#       「七天内」为第二条的决定时刻减第一条的生效时刻不超过七天。少于 11 记为「相符」。
#   四、命令总数都不多于六十：不超过 60 记为「相符」。
#   五、丙档非触发通道的连带偏离日数可能高于乙档：描述项，只列两档数字与大小关系，不判相符与否。连带偏离日数的口径与第二批
#       链式报告判读口径三相同（每条已执行命令，生效当地日起七个当地日内，非触发通道（含湿度与气压）的日末偏离日数之和）。
#   选定判据（沿用第二批）：命令总数最少且非触发通道连带偏离日数最少的档优先；两项指向不同档时以连带偏离日数为准；乙与丙在两项上
#       都相当时取乙。「相当」在这里写定为：命令总数相差不超过 2 条，且连带偏离日数相差不超过两者中较小者的 5%。本脚本给出按此规则
#       指向的档，选定由设计会话宣布。
#   第三节，振荡规则一档（在第一节选定的档上加 --osc-suppress true，十四天）与该档比较：
#   六、丙型实例的第三条及以后的命令消失：「连锁丙型实例」为一个丙型实例的第一条命令恰是同一台同一通道上一个丙型实例的第二条、
#       且两个实例的第二条各自距其第一条的决定时刻不超过十四天。振荡一档中连锁丙型实例为零记为「相符」。
#   七、命令总数再降：少于所在档记为「相符」。
#   八、三月事件与 G 光照的首条命令不变：命令日在 03-22 至 03-27 的命令中每台的第一条（设备、命令日、触发通道），以及 G 上第一条
#       触发含光照的命令（命令日），两者都与所在档相同记为「相符」。
#   另列（描述）：振荡抑制的复核条数、其中部分触发通道被抑制而其余照发的条数、锚定回退的命令条数（各档）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m6c/chain_anchor_report.py [--chain-dir docs/m6c/chain] [--osc-arm trigger_osc --osc-base trigger]
#    [--out docs/m6c/chain/anchor_check.md]
# 3. 前置条件 / Preconditions: 已运行 ChainDriver 的 off、trigger、all 三档（--variant D7 --keep-channels Humidity,Pressure
#    --anchor off|trigger|all --out-dir docs/m6c/chain/<档>）；给出 --osc-arm 时，该档也已运行。
# 4. 期望产出 / Expected output: --out 处的 Markdown；终端打印各项判定与选定判据指向的档。
# 5. 失败兜底 / Failure fallback: 缺少某一档的输出时退出码 2。
# ============================================================================
import argparse
import collections
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(HERE, "m6b"))
import chain_report as CR       # noqa: E402

NAMES = {"off": "甲：生效前七天（现行默认）", "trigger": "乙：只锚定触发通道", "all": "丙：锚定全部重估通道"}
BASE_MARCH, BASE_STABLE, BASE_NEAR, MAX_CMDS = 3, 5, 11, 60


def stable_days(path):
    st = collections.defaultdict(set)
    for r in CR.read(path):
        for k, v in r.items():
            if k.startswith("stable_") and v == "True":
                st[k[len("stable_"):]].add("2022-" + r["day"])
    return st


def gap(x, y):
    return (int(y["decided_at"]) - int(x["effective_at"])) / 86400


def march_second(run):
    """A、B、G 中在三月事件内有同通道同号两条命令的台 / devices with a same-sign second command in the March event."""
    out = []
    for dev in ("A", "B", "G"):
        cs = [c for c in run.commands if c["device"] == dev and "2022-03-22" <= c["day"] <= "2022-03-27"]
        hit = False
        for i, a in enumerate(cs):
            for b in cs[i + 1:]:
                for ch in set(a["triggers"].split("|")) & set(b["triggers"].split("|")) - {"perf"}:
                    sa = float(run.days[(dev, a["day"], ch)]["shift"])
                    sb = float(run.days[(dev, b["day"], ch)]["shift"])
                    hit = hit or sa * sb > 0
        if hit:
            out.append(dev)
    return out


def collateral(run):
    """非触发通道连带偏离日数（第二批判读口径三）/ non-trigger collateral deviation days."""
    non = 0
    for c in run.commands:
        if "%s_%s" % (c["device"], c["seq"]) not in run.executed:
            continue
        e = CR.lday(c["effective_at"])
        tr = [t for t in c["triggers"].split("|") if t != "perf"]
        non += sum(run.dev_days(c["device"], ch, e, 7) for ch in CR.CH if ch not in tr)
    return non


def chained(run):
    """连锁丙型实例数（判定六）/ chained C-type instances."""
    tc = CR.type_c(run)
    dec = lambda x, y: (int(y["decided_at"]) - int(x["decided_at"])) / 86400
    n = 0
    for d1, ch1, a1, _, _, b1, _ in tc:
        for d2, ch2, a2, _, _, b2, _ in tc:
            if (d1, ch1) == (d2, ch2) and a2["seq"] == b1["seq"] and dec(a1, b1) <= 14 and dec(a2, b2) <= 14:
                n += 1
    return n


def fallbacks(d):
    k = n = 0
    for p in glob.glob(os.path.join(d, "level0", "*.json")):
        j = json.load(open(p, encoding="utf-8"))
        if "windowStart" in j:
            n += 1
            k += 1 if j.get("anchorFallback") else 0
    return k, n


def firsts(run):
    m = {}
    for c in run.commands:
        if "2022-03-22" <= c["day"] <= "2022-03-27" and c["device"] not in m:
            m[c["device"]] = (c["day"], c["triggers"])
    g = next((c["day"] for c in run.commands if c["device"] == "G" and "Light" in c["triggers"].split("|")), None)
    return m, g


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain-dir", default="docs/m6c/chain")
    ap.add_argument("--stable", default="docs/m3_inject/v34/v34_daily.csv")
    ap.add_argument("--osc-arm", default="")
    ap.add_argument("--osc-base", default="")
    ap.add_argument("--out", default="docs/m6c/chain/anchor_check.md")
    a = ap.parse_args()
    arms = ["off", "trigger", "all"] + ([a.osc_arm] if a.osc_arm else [])
    for v in arms:
        if not os.path.exists(os.path.join(a.chain_dir, v, "commands.csv")):
            print("ERROR: 缺少 %s/%s 的输出" % (a.chain_dir, v), file=sys.stderr)
            return 2
    R = {v: CR.Run(os.path.join(a.chain_dir, v)) for v in arms}
    st = stable_days(a.stable)
    S = {}
    for v in arms:
        run = R[v]
        tc = CR.type_c(run)
        stab = [c for c in run.commands if c["day"] in st[c["device"]]]
        S[v] = dict(cmds=len(run.commands), march=march_second(run),
                    post=[c for c in stab if c["day"] in ("2022-03-30", "2022-04-17")], stable=stab,
                    tc=len(tc), near=sum(1 for _, _, x, _, _, y, _ in tc if gap(x, y) <= 7), coll=collateral(run),
                    fb=fallbacks(os.path.join(a.chain_dir, v)), chained=chained(run))
    out = ["# 零级窗口锚定的链式三档与振荡规则一档：预期核对（m6c/chain_anchor_report.py 生成）\n\n"]
    w = out.append
    lst = lambda cs: "；".join("%s %s %s" % (c["device"], c["day"][5:], c["triggers"]) for c in cs) or "无"
    rows = []
    for v in ("off", "trigger", "all"):
        s = S[v]
        rows.append([NAMES[v], s["cmds"], "%d（%s）" % (len(s["march"]), "、".join(s["march"]) or "无"),
                     "%d（%s）" % (len(s["post"]), lst(s["post"])), len(s["stable"]), "%d（七天内 %d）" % (s["tc"], s["near"]),
                     s["coll"], "%d / %d" % s["fb"]])
    w("## 第一节：三档并排\n\n")
    w(CR.table(["档", "命令总数", "三月事件同向第二条的台数", "03-30 与 04-17 平稳日命令", "全程平稳日命令", "丙型实例",
                "非触发通道连带偏离日数", "锚定回退 / 锚定执行"], rows))
    w("\n## 第一节：预期判定（乙、丙各自与甲比较）\n\n")
    judge = lambda ok: "相符" if ok else "不符"
    jr = []
    for v in ("trigger", "all"):
        s = S[v]
        jr.append([NAMES[v], judge(len(s["march"]) < BASE_MARCH), judge(len(s["post"]) < BASE_STABLE), judge(s["near"] < BASE_NEAR),
                   judge(s["cmds"] <= MAX_CMDS)])
    w(CR.table(["档", "一、三月事件同向第二条减少", "二、事件后平稳日伪命令减少", "三、七天内丙型实例少于 11", "四、命令总数不多于 60"], jr))
    rel = "高于" if S["all"]["coll"] > S["trigger"]["coll"] else ("等于" if S["all"]["coll"] == S["trigger"]["coll"] else "低于")
    w("\n五（描述）：丙档非触发通道连带偏离日数 %d，%s乙档 %d。\n" % (S["all"]["coll"], rel, S["trigger"]["coll"]))
    # 选定判据 / selection rule
    def best(key):
        m = min(S[v][key] for v in ("off", "trigger", "all"))
        return [v for v in ("off", "trigger", "all") if S[v][key] == m]
    bc, bl = best("cmds"), best("coll")
    t, l = S["trigger"], S["all"]
    comparable = abs(t["cmds"] - l["cmds"]) <= 2 and abs(t["coll"] - l["coll"]) <= 0.05 * min(t["coll"], l["coll"])
    if comparable and set(bl) & {"trigger", "all"}:
        pick = "trigger"
        why = "乙与丙在两项上相当（命令差 %d 条，连带偏离日差 %d 日），取乙" % (abs(t["cmds"] - l["cmds"]), abs(t["coll"] - l["coll"]))
    else:
        pick = bl[0] if len(bl) == 1 else ("trigger" if "trigger" in bl else bl[0])
        why = "命令总数最少：%s；连带偏离日数最少：%s；%s" % ("、".join(NAMES[x] for x in bc), "、".join(NAMES[x] for x in bl),
                                                  "两项一致" if pick in bc else "两项指向不同，以连带偏离日数为准")
    w("\n选定判据指向：**%s**（%s）。选定由设计会话宣布。\n" % (NAMES[pick], why))
    print("第一节：", "；".join("%s 命令 %d、同向第二条 %d、事件后平稳日 %d、七天内丙型 %d、连带 %d" % (
        v, S[v]["cmds"], len(S[v]["march"]), len(S[v]["post"]), S[v]["near"], S[v]["coll"]) for v in ("off", "trigger", "all")))
    print("选定判据指向：%s（%s）" % (pick, why))
    if a.osc_arm:
        o, b = S[a.osc_arm], S[a.osc_base]
        fo, fb = firsts(R[a.osc_arm]), firsts(R[a.osc_base])
        rv = [r for r in R[a.osc_arm].review if r["kind"] == "零级被抑制：基线振荡"]
        part = sum(1 for r in rv if "其余触发通道照常发零级" in r["detail"])
        w("\n## 第三节：振荡规则一档（在 %s 上）\n\n" % NAMES.get(a.osc_base, a.osc_base))
        w(CR.table(["项", "所在档", "加振荡规则", "判定"], [
            ["六、连锁丙型实例为零", b["chained"], o["chained"], judge(o["chained"] == 0)],
            ["七、命令总数再降", b["cmds"], o["cmds"], judge(o["cmds"] < b["cmds"])],
            ["八、三月事件每台首条与 G 光照首条不变", "%d 台；G 光照 %s" % (len(fb[0]), fb[1]), "%d 台；G 光照 %s" % (len(fo[0]), fo[1]),
             judge(fo == fb)],
        ]))
        w("\n描述：振荡抑制的复核 %d 条，其中部分触发通道被抑制而其余照发 %d 条；丙型实例 %d 例（七天内 %d），所在档 %d 例（七天内 %d）。\n"
          % (len(rv), part, o["tc"], o["near"], b["tc"], b["near"]))
        if rv:
            w("\n" + CR.table(["设备", "日", "被抑制的通道与处理"], [[r["device"], r["day"], r["detail"]] for r in rv]))
        print("第三节：连锁丙型 %d→%d，命令 %d→%d，首条%s；抑制 %d 条（部分 %d）" % (
            b["chained"], o["chained"], b["cmds"], o["cmds"], "不变" if fo == fb else "有变化", len(rv), part))
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write("".join(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
