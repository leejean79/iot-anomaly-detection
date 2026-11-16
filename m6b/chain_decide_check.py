#!/usr/bin/env python3
# ============================================================================
# m6b/chain_decide_check.py
# 链式基线决策层的 Python 对照（1.49 版裁决「顺序」第一条：新基线 D7K_up_s 的 Python 对照）：链式驱动把每小时的离群轮数与参数版本
# （hours.csv）、数据层日记录（data_days.csv）与爆发（bursts.csv）写在链式目录里，这正是聚合模块在闭环中看到的输入；把它们与收口
# 输入的其余各表（小时观测的其余列、性能层事件与元素、参照、冻结）合成一份输入，交给 Python 参考 m7/decide.py（升级格打开、以上下文
# 评分的首条时刻为上线时刻），与链式驱动的三份日志逐条比较，截止到链式驱动停止的那条命令的决定时刻。
# Python cross-check of the chain baseline's decisions: merge the chain's per-hour outliers, versions, data days and bursts into
# the closeout inputs, run m7/decide.py with the upgrade cells, and compare the three logs up to the chain's stop.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m6b/chain_decide_check.py [--chain docs/m6b/chain/D7K_up_s] [--inputs docs/m7/inputs]
# 3. 前置条件 / Preconditions: 已运行链式驱动（--upgrade-cells true）。
# 4. 期望产出 / Expected output: chain 下 py_decide/（合成的输入与 Python 的三份日志）；终端打印三份日志「逐条相同」与条数。
# 5. 失败兜底 / Failure fallback: 不同时退出码 1，并打印第一处不同。
# ============================================================================
import argparse
import csv
import os
import shutil
import subprocess
import sys


def rows(path):
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def write(path, header, rs):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=header, lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows(rs)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain", default="docs/m6b/chain/D7K_up_s")
    ap.add_argument("--inputs", default="docs/m7/inputs")
    a = ap.parse_args()
    out = os.path.join(a.chain, "py_decide")
    inp = os.path.join(out, "inputs")
    os.makedirs(inp, exist_ok=True)
    sim = {(r["device"], r["hour_start"]): r for r in rows(os.path.join(a.chain, "hours.csv"))}
    base = rows(os.path.join(a.inputs, "hours.csv"))
    header = list(base[0].keys()) + ["version"]
    for r in base:
        s = sim.get((r["device"], r["hour_start"]))
        r["outlier_rounds"] = s["outlier_rounds"] if s else "0"
        r["version"] = s["version"] if s else "1"
    write(os.path.join(inp, "hours.csv"), header, base)
    for f in ("data_days.csv", "bursts.csv"):
        shutil.copy(os.path.join(a.chain, f), os.path.join(inp, f))
    for f in ("perf_events.csv", "perf_elements.csv", "refs.csv", "freeze.csv"):
        shutil.copy(os.path.join(a.inputs, f), os.path.join(inp, f))
    stop = open(os.path.join(a.chain, "stopped_at.txt"), encoding="utf-8").read().strip().split(",")
    # 链式驱动的 p_point 取自收口输入（离群轮换成模拟值之前），这里同样从收口输入推导 / p_point from the closeout inputs
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "m7"))
    import decide                                    # noqa: E402
    p_point = decide.derive_p_point(decide.Inputs(a.inputs), decide.Params())
    args = [sys.executable, "m7/decide.py", "--inputs", inp, "--out-dir", out, "--upgrade-cells", "true",
            "--p-point", repr(p_point)]
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL)
    cut = int(stop[2]) + 60
    r = subprocess.run([sys.executable, "m7/compare_logs.py", a.chain, out, "--cut", str(cut)], capture_output=True, text=True)
    print("链式驱动停在 %s %s 的 %s 级命令；比较截止到其决定时刻（%s）" % (stop[0], stop[1], stop[4], stop[2]))
    print(r.stdout.strip())
    return r.returncode


if __name__ == "__main__":
    sys.exit(main())
