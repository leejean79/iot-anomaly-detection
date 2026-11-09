#!/usr/bin/env python3
# ============================================================================
# m6b/chain_check.py
# 链式离线实验验收项 V-M6b-2 的跨语言部分：「Java 模拟器与 Python 参考在每条命令的新参数上逐位（光照 1e-12）相同」。对链式驱动
# 程序（com.leejean.m6.ChainDriver）每一档 level0/ 下的每一条执行记录，用 Python 参考实现（m6a/reestimate.py 的 freeze 与 level0）
# 从同一份轮表按同一生效时刻与重估窗口独立重算，再用 m6a/compare.py 的同一套逐值比较。零级的新参数只取决于重估窗口里的原始读数与
# 守卫，不取决于之前的命令，所以每条命令可以独立核对。每台轮表只读一次。
# Cross-language check of every executed level-0 in the chained experiment.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m6b/chain_check.py [--chain-dir docs/m6b/chain] [--variants D1,D7,pos]
#    可选：--rounds-dir docs/m6a/extract、--rounds-E docs/m6b/extract/rounds_E_inj.csv.gz、--out docs/m6b/chain/check.md
# 3. 前置条件 / Preconditions: 已运行 ChainDriver 的各档；E 的注入轮表已生成。
# 4. 期望产出 / Expected output: 终端逐档打印核对的命令数与不同的命令数；表格写入 --out。
# 5. 失败兜底 / Failure fallback: 有任何不同时退出码 1，并列出前几处不同。
# ============================================================================
import argparse
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(HERE, "m6a"))
import compare as CMP      # noqa: E402
import reestimate as RE    # noqa: E402

DAYS = {"D1": 1, "D7": 7, "pos": 7}


def python_result(rows, frz, t_freeze, eff, seq, d_days, position_only):
    cal, window, guard, bf = RE.level0([r for r in rows if r[0] < eff], eff, d_days * 86400, 3600, 1e-9)
    if position_only:
        cal = dict(cal, scale=list(frz["scale"]), bypass=list(frz["bypass"]))
        bf = [(t, RE.normalize(x, cal)) for t, x, _ in rows if eff - 3600 <= t < eff and all(v is not None for v in x)]
    return dict(effectiveAt=eff, version=seq + 1, freezeTs=t_freeze, windowRounds=window, guardExcluded=guard,
                freeze=dict(median=frz["median"], scale=frz["scale"], bypass=frz["bypass"]),
                new=dict(median=cal["median"], scale=cal["scale"], bypass=cal["bypass"]), hist=cal["hist"],
                backfill=dict(ts=[t for t, _ in bf], xNorm=[v for _, v in bf]))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain-dir", default="docs/m6b/chain")
    ap.add_argument("--variants", default="D1,D7,pos")
    ap.add_argument("--rounds-dir", default="docs/m6a/extract")
    ap.add_argument("--rounds-E", default="docs/m6b/extract/rounds_E_inj.csv.gz")
    ap.add_argument("--out", default="docs/m6b/chain/check.md")
    a = ap.parse_args()
    jobs = {}
    for v in a.variants.split(","):
        for path in sorted(glob.glob(os.path.join(a.chain_dir, v, "level0", "*.json"))):
            j = json.load(open(path, encoding="utf-8"))
            jobs.setdefault(j["device"], []).append((v, path, j))
    stats = {v: [0, 0, 0.0, 0] for v in a.variants.split(",")}    # 命令数、不同数、光照最大差、比较的值数
    bad = []
    for dev in sorted(jobs):
        path = a.rounds_E if dev == "E" else os.path.join(a.rounds_dir, "rounds_%s.csv.gz" % dev)
        rows = RE.read_rounds(path)
        frz, t_freeze = RE.freeze(rows, 60480, 1e-9)
        for v, jp, j in jobs[dev]:
            p = python_result(rows, frz, t_freeze, j["effectiveAt"], j["seq"], DAYS[v], v == "pos")
            c = CMP.compare(p, j)
            s = stats[v]
            s[0] += 1
            s[1] += 1 if c.diffs else 0
            s[2] = max(s[2], c.light_max)
            s[3] += c.n
            if c.diffs:
                bad.append("%s %s：%s" % (v, os.path.basename(jp), "；".join(c.diffs[:3])))
        print("%s：%d 条命令已核对" % (dev, len(jobs[dev])))
    lines = ["# 链式离线实验每条命令新参数的 Java 与 Python 核对（m6b/chain_check.py 生成）\n\n",
             "| 档 | 核对的命令 | 不同 | 比较的值 | 光照通道最大差 |\n| --- | --- | --- | --- | --- |\n"]
    for v, s in stats.items():
        lines.append("| %s | %d | %d | %d | %.3e |\n" % (v, s[0], s[1], s[3], s[2]))
        print("%s：核对 %d 条命令，不同 %d 条，比较 %d 个值，光照最大差 %.3e" % (v, s[0], s[1], s[3], s[2]))
    if bad:
        lines.append("\n不同之处：\n\n" + "".join("- %s\n" % b for b in bad))
        print("\n".join(bad[:10]))
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write("".join(lines))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
