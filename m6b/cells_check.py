#!/usr/bin/env python3
# ============================================================================
# m6b/cells_check.py
# 升级格与抑制规则的跨语言核对（1.44 版裁决；1.49 版裁决「顺序」第一条）：在合成输入（m6b/synth_cells.py）上按五种配置、在收口输入
# （docs/m7/inputs）上按升级格打开一种配置，分别运行 Java 离线运行器（com.leejean.m7.M7OfflineRunner）与 Python 参考
# （m7/decide.py），用 m7/compare_logs.py 逐条比较三份日志，并统计每种配置命中的格与复核类别，确认每个格与每种抑制至少命中一次。
# Cross-language check of the upgrade cells and suppressions: five configurations on synthetic inputs plus one on the closeout inputs.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+、JDK 11。
# 2. 调用命令 / Invocation: python3 m6b/cells_check.py [--jar target/iot-anomaly-detection-1.0-SNAPSHOT.jar]
#    [--synth docs/m6b/synth_cells/inputs] [--out docs/m6b/synth_cells]
# 3. 前置条件 / Preconditions: 已 mvn package；已运行 python3 m6b/synth_cells.py。
# 4. 期望产出 / Expected output: out 下每种配置的 java/、py/ 两个目录与 cells_check.md（逐条比较结果与命中统计）；终端逐配置打印。
# 5. 失败兜底 / Failure fallback: 任何一种配置不逐条相同、或有格与抑制没有命中时退出码 1。
# ============================================================================
import argparse
import collections
import csv
import os
import subprocess
import sys

CONFIGS = [
    ("syn1_first_ctx", "synth", ["--upgrade-cells", "true"]),
    ("syn2_events", "synth", ["--upgrade-cells", "true", "--model-events", "true"]),
    ("syn3_events_short", "synth", ["--upgrade-cells", "true", "--model-events", "true", "--k-up-days", "3",
                                    "--up-cooldown-days", "3", "--k-fail-days", "5"]),
    ("syn4_before", "synth", ["--upgrade-cells", "true", "--retrain-side", "before"]),
    ("syn5_events_hourly", "synth", ["--upgrade-cells", "true", "--model-events", "true", "--cmd-sec", "3600"]),
    ("real_first_ctx", "real", ["--upgrade-cells", "true"]),
]
CELLS = ["二级：一级之后 K_up 天内性能层新确认段", "一级：只有性能层持续", "一级：两层同报",
         "一级：零级之后 K_up 天内同一通道新持续段", "零级：只有数据层持续", "零级：一级被抑制（模型不在线）",
         "零级：一级被抑制（重训进行中）"]
KINDS = ["一级被抑制：模型不在线", "二级被抑制：模型不在线", "一级被抑制：重训进行中", "REPORT：一级重训超时视为失败"]


def rows(path):
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jar", default="target/iot-anomaly-detection-1.0-SNAPSHOT.jar")
    ap.add_argument("--synth", default="docs/m6b/synth_cells/inputs")
    ap.add_argument("--real", default="docs/m7/inputs")
    ap.add_argument("--out", default="docs/m6b/synth_cells")
    a = ap.parse_args()
    java = os.path.join(os.environ.get("JAVA_HOME", "/usr"), "bin", "java")
    ok_all, cells_hit, kinds_hit, lines = True, collections.Counter(), collections.Counter(), []
    lines.append("| 配置 | 输入 | 参数 | 告警、命令、复核（Java 与 Python） | 逐条比较 | 命中的格 | 抑制与 REPORT |\n")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |\n")
    for name, src, args in CONFIGS:
        inp = a.synth if src == "synth" else a.real
        jd, pd = os.path.join(a.out, name, "java"), os.path.join(a.out, name, "py")
        subprocess.run([java, "-cp", a.jar, "com.leejean.m7.M7OfflineRunner", "--inputs", inp, "--out-dir", jd] + args,
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run([sys.executable, "m7/decide.py", "--inputs", inp, "--out-dir", pd] + args, check=True,
                       stdout=subprocess.DEVNULL)
        cmp = subprocess.run([sys.executable, "m7/compare_logs.py", jd, pd], capture_output=True, text=True)
        same = cmp.returncode == 0
        ok_all &= same
        cmds, rev = rows(os.path.join(jd, "commands.csv")), rows(os.path.join(jd, "review.csv"))
        al = rows(os.path.join(jd, "alerts.csv"))
        cc = collections.Counter(c.get("cell", "") for c in cmds)
        kc = collections.Counter(r["kind"] for r in rev if r["kind"] in KINDS)
        cells_hit.update(cc)
        kinds_hit.update(kc)
        print("%s：%s；格 %s；抑制 %s" % (name, "逐条相同" if same else "不同", dict(cc), dict(kc)))
        if not same:
            print(cmp.stdout)
        lines.append("| %s | %s | %s | %d、%d、%d | %s | %s | %s |\n" % (
            name, src, " ".join(args), len(al), len(cmds), len(rev), "逐条相同" if same else "**不同**",
            "；".join("%s %d" % (k, v) for k, v in sorted(cc.items())), "；".join("%s %d" % (k, v) for k, v in sorted(kc.items())) or "—"))
    miss = [c for c in CELLS if not cells_hit[c]] + [k for k in KINDS if not kinds_hit[k]]
    lines.append("\n未命中的格与抑制：%s\n" % ("无" if not miss else "、".join(miss)))
    with open(os.path.join(a.out, "cells_check.md"), "w", encoding="utf-8") as fh:
        fh.write("# 升级格与抑制规则的跨语言核对（m6b/cells_check.py 生成）\n\n")
        fh.writelines(lines)
    print("未命中：%s" % ("无" if not miss else "、".join(miss)))
    return 0 if ok_all and not miss else 1


if __name__ == "__main__":
    sys.exit(main())
