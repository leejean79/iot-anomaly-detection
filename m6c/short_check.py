#!/usr/bin/env python3
# ============================================================================
# m6c/short_check.py
# 第三批短重放的核对（交接文档第六节第 3 步；1.54 版裁决第八处、1.57 版裁决第五项）。读 deploy/scripts/syn-m6c-short.sh 收集的两次运行
# （a、b），核对：两次运行各自零重启；启动横幅里第三批开关与本组要求一致；两次运行的九类输出逐位相同（与第二批全量重放的等值第二段
# 同一口径：m6b/full_check.py 的 stage2，排除按处理时间产生的三类诊断量；数据发完时仍有设备保持水位线时，聚合模块的告警、复核与命令
# 从保持日起不参与比较，1.50 版裁决第二项）。
#   第一组：运行 a 用 m6b-closeout 标签构建的 jar、运行 b 用新 jar，第三批全部开关关闭——「第三批全部开关关闭时，新代码与标签 jar
#           在同一配置下逐位相同」；
#   第二组：两次都用新 jar，开启丁档与二级预案——「开启选定项后两次运行逐位相同」。
# 只核对与列表，不下结论。
# Checks of the batch-3 short replays: zero restarts, banner switches, and bit-identical outputs across the two runs.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 集群 master（由编排调用）或本地 Mac，仓库根目录，python3.9+（只用标准库；需要 zoneinfo 与 Europe/London）。
# 2. 调用命令 / Invocation: python3 m6c/short_check.py --dir docs/m6c/short1 --group 1
#                           python3 m6c/short_check.py --dir docs/m6c/short2 --group 2
# 3. 前置条件 / Preconditions: --dir 下 a/、b/ 为编排收集的运行目录（submit.log、jm_states.txt、wm_buffer.csv、replay_end.txt 与
#    九类输出：scores.jsonl、drift.jsonl、alerts.jsonl、adapt.jsonl、m5a_records.jsonl、shadow.jsonl、m3_markers.jsonl、m1out.sha、
#    monitoring_norm.sha）。
# 4. 期望产出 / Expected output: --dir/short_report.md；终端逐项打印「[通过]」或「[失败]」。
# 5. 失败兜底 / Failure fallback: 任何一项失败时退出码 1；缺文件时该项记为失败；没有时区数据时退出码 2（fetch 后在本地运行）。
# ============================================================================
import argparse
import os
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(HERE, "m6b"))
import full_check as FC      # noqa: E402

BANNER = {
    # 第一组运行 b（新 jar）：第三批全部关闭；运行 a 是标签 jar，没有这一行 / group 1 run b: everything off
    "1b": "Batch 3:         anchor=off min=2 d; defer=0 d; oscillation=false; rebuild plan=false; switch validate=false",
    # 第二组：丁档与二级预案打开，验证与振荡关闭（1.57 版裁决第五项第 2 条）/ group 2: arm D and the plan on
    "2a": "Batch 3:         anchor=trigger min=7 d; defer=7 d; oscillation=false; rebuild plan=true; switch validate=false",
    "2b": "Batch 3:         anchor=trigger min=7 d; defer=7 d; oscillation=false; rebuild plan=true; switch validate=false",
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--group", required=True, choices=["1", "2"])
    a = ap.parse_args()
    if FC.ZONE is None:
        print("ERROR: 这里的 python3 没有 zoneinfo 或 Europe/London 时区数据；请 fetch 后在本地运行核对。", file=sys.stderr)
        return 2
    runs = [r for r in ("a", "b") if os.path.isdir(os.path.join(a.dir, r))]
    results, notes = [], []

    def check(name, ok, detail):
        results.append((name, ok, detail))
        print("[%s] %s：%s" % ("通过" if ok else "失败", name, detail))

    for r in runs:
        d = os.path.join(a.dir, r)
        restarts = sum(1 for l in FC.lines(os.path.join(d, "jm_states.txt")) or [] if "to RESTARTING" in l)
        check("运行 %s：零重启" % r, restarts == 0, "作业转入重启 %d 次" % restarts)
        sub = FC.lines(os.path.join(d, "submit.log")) or []
        want = BANNER.get(a.group + r)
        if want:
            check("运行 %s：启动横幅的第三批开关" % r, any(want in l for l in sub), want)
        elif a.group == "1" and r == "a":
            check("运行 a：标签 jar（横幅里没有第三批一行）", not any("Batch 3:" in l for l in sub),
                  "横幅里%s「Batch 3:」一行" % ("有" if any("Batch 3:" in l for l in sub) else "没有"))
    exclude_from = None
    for r in runs:
        held = FC.end_hold(os.path.join(a.dir, r))
        if held is None:
            check("运行 %s：数据发完时没有设备保持水位线" % r, False, "缺少 wm_buffer.csv，无法确认")
            continue
        check("运行 %s：数据发完时没有设备保持水位线" % r, True,
              "无" if not held else "；".join("%s 保持在 %s" % (dv, FC.local(o // 1000)) for dv, o in held))
        for dv, o in held:
            day = datetime.fromtimestamp(o // 1000, FC.ZONE).strftime("%Y-%m-%d")
            exclude_from = day if exclude_from is None else min(exclude_from, day)
    if exclude_from:
        notes.append("数据发完时仍有设备保持水位线：聚合模块的告警、复核与命令从 %s 起不参与比较（1.50 版裁决第二项）。" % exclude_from)
    title = ("第一组：标签 jar 与新 jar（第三批全部关闭）逐位相同" if a.group == "1"
             else "第二组：开启丁档与二级预案后两次运行逐位相同")
    FC.stage2(a.dir, runs, check, title, exclude_from)
    with open(os.path.join(a.dir, "short_report.md"), "w", encoding="utf-8") as fh:
        fh.write("# 第三批短重放第%s组核对（m6c/short_check.py 生成）\n\n" % ("一" if a.group == "1" else "二"))
        fh.write(FC.table(["核对项", "结果", "详情"], [[n, "通过" if ok else "失败", dt] for n, ok, dt in results]))
        for n in notes:
            fh.write("\n" + n + "\n")
    return 0 if results and all(ok for _, ok, _ in results) else 1


if __name__ == "__main__":
    sys.exit(main())
