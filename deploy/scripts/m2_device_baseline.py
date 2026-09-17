#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
m2_device_baseline.py —— 从 synergia-monitoring 转储聚合 M2 的逐设备基线，并与 Java 8 探针值比对。
Aggregate M2's per-device baseline from a synergia-monitoring dump and compare it with the Java 8 probe.

用途 / Purpose
  三月全月重放 runbook 第 4.2 与第 5 节：作业侧的逐设备平均离群率、平均微簇占用率、邻居数 P10/P50
  没有现成工具（syn-m2-metrics.sh 只给全局计数器，m2_probe_compare.py 只吃离线探针 CSV）。本脚本补上
  这一环，并按第 5 节的 1% 相对容差做等值核验。
  Fills the gap for the full-March runbook sections 4.2 and 5: per-device rates from the *job's*
  monitoring snapshots, plus the equality check against the Java 8 probe at each device's final radius.

口径 / Metric definition（第 4.2 节要求显式声明并确认与探针一致）
  meanOutlierRate = 每个滑动步的 (窗口内离群数 ÷ 窗口内点数)，再对所有滑动步取**算术平均**。
  这与 M2Probe 的 meanOutlierRate 完全同口径（见 m2_surge.py 注释"均值与探针 meanOutlierRate 同口径"）。
  注意它**不等于** m2_outliers_total ÷ m2_points_total（后者是按点数加权的总体比率，本脚本另行输出，
  两者都给，避免口径混淆）。
  meanOutlierRate is the per-slide ratio averaged arithmetically over slides — the probe's definition.
  It is NOT the same as the point-weighted overall ratio; both are emitted so the two are never conflated.

================================ 脚本交付五要素 / Five delivery elements ================================
1. 执行环境 / Environment: 本地 python3（仅标准库），工作目录为项目根目录。
2. 调用命令 / Invocation:
     python3 deploy/scripts/m2_device_baseline.py \
         --monitoring-jsonl docs/m2_monitoring_march.jsonl \
         --out-csv docs/reports/m2_java11_march_per_device.csv \
         --out-md  docs/reports/m2_java11_march_per_device.md \
         --out-svg docs/reports/m2_java11_march_rate_cmp.svg
   常用可选参数 / common options:
     --java8-probe <csv>   Java 8 参照探针表（默认 docs/m2_probe_7d_clean.csv）
     --r-per-device <str>  逐设备半径，格式 "A=1.0,B=1.0,..."（默认取 .env 同款终值）
     --tol-rel <pct>       等值核验的相对容差，默认 1.0（%）
     --no-compare          只聚合不比对（换时段、换配置时用）
3. 前置条件 / Preconditions: 已用 syn-m2-baseline.sh（或 kafka-console-consumer）把 synergia-monitoring
   转储成 JSONL；转储须覆盖整个重放时段。Java 8 参照表需存在（仓库已有）。
4. 期望产出 / Expected output: stdout 打印逐设备对照表与核验结论；写出 CSV、Markdown 表、SVG 图。
   退出码 0 = 全部设备在容差内（或 --no-compare）；1 = 有设备超容差；2 = 输入有问题。
5. 失败兜底 / Failure fallback: 转储里没有 M2 快照（全是 windowEnd=0 的 M1 快照）→ 提示 M2 仍在预热期
   或转储时段不对；某设备滑动步数显著少于其它设备 → 提示该设备可能有停机段，需人工判读。

缩写自查 / Abbreviations: PD = pure-data 点（不属于任何微簇的点）；MC = micro-cluster 微簇；
  P10/P50 = 第 10/50 百分位；JSONL = 每行一个 JSON 对象的文本格式。
"""

import argparse
import csv
import json
import os
import sys

DEVICES = ["A", "B", "C", "D", "E", "F", "G", "H"]
# .env 的 SYN_M2_R_PER_DEVICE 终值（M2 收尾任务一裁决）/ final per-device radii from .env
DEFAULT_R = "A=1.0,B=1.0,C=1.0,D=0.75,E=1.0,F=1.0,G=1.5,H=1.0"


def parse_r_per_device(spec):
    """解析 "A=1.0,B=1.0,..." / parse the per-device radius spec."""
    out = {}
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        dev, _, val = part.partition("=")
        out[dev.strip()] = float(val)
    return out


def load_snapshots(path):
    """读 monitoring 转储，只取 M2 快照（windowEnd>0；M1 快照的 windowEnd 恒为 0）。
    Read the dump, keeping only M2 snapshots (M1 snapshots always carry windowEnd = 0)."""
    per_dev = {}
    total, kept, bad = 0, 0, 0
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                o = json.loads(line)
            except ValueError:
                bad += 1
                continue
            if int(o.get("windowEnd", 0) or 0) <= 0:
                continue          # M1 快照，跳过 / M1 snapshot
            dev = o.get("device")
            if not dev:
                continue
            kept += 1
            per_dev.setdefault(dev, []).append((
                float(o.get("m2OutlierRate", 0.0) or 0.0),
                float(o.get("m2McOccupancy", 0.0) or 0.0),
                float(o.get("m2NeighborCountP10", 0.0) or 0.0),
                float(o.get("m2NeighborCountP50", 0.0) or 0.0),
                int(o.get("m2Outliers", 0) or 0),
                int(o.get("m2WindowPoints", 0) or 0),
            ))
    return per_dev, total, kept, bad


def load_java8(path, r_map):
    """从 Java 8 探针 CSV 取每设备在其终值半径、k=10 处的 meanOutlierRate。
    Pick each device's meanOutlierRate at its final radius (k = 10) from the Java 8 probe CSV.

    该文件是纳入版本管理的验收交付物（提交 2fa8d10）。缺失通常是本地工作副本的问题，不是仓库没有，
    因此这里给出可操作的恢复指令，而不是抛出原始回溯。
    The file is a tracked acceptance deliverable; a miss is a local working-copy problem, so give the
    operator a recovery command instead of a raw traceback."""
    if not os.path.isfile(path):
        raise SystemExit(
            "ERROR: 找不到 Java 8 参照表：%s\n"
            "  该文件是被版本管理跟踪的验收交付物，缺失通常是本地工作副本的问题。恢复方式：\n"
            "      git checkout -- %s          # 本地被删\n"
            "      git pull origin dev-claude  # 分支落后\n"
            "  恢复后可复用已有转储重跑，无需重新转储：\n"
            "      bash deploy/scripts/syn-m2-baseline.sh --tag <tag> --reuse-dump\n"
            "  若只想先看聚合结果、暂不比对，可加 --no-compare。\n"
            "ERROR: Java 8 reference table not found: %s (tracked file; restore it with git checkout,\n"
            "  then re-run with --reuse-dump, or pass --no-compare to skip the comparison)."
            % (path, path, path))
    ref = {}
    with open(path, "r", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            dev = row.get("device")
            if dev not in r_map:
                continue
            if abs(float(row["R"]) - r_map[dev]) > 1e-9 or int(float(row["k"])) != 10:
                continue
            ref[dev] = {"rate": float(row["meanOutlierRate"]), "slides": int(float(row["slides"]))}
    missing = [d for d in r_map if d not in ref]
    if missing:
        # 参照表存在但缺某设备在其终值半径处的行：点名，不静默少比 / name them, never silently skip
        print("  [WARN] 参照表 %s 缺少以下设备在其终值半径、k=10 处的行：%s" % (path, ", ".join(missing)))
        print("         这些设备将只报实测值、不做等值核验。请核对 --r-per-device 与参照表是否同一轮标定。")
    return ref


def mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def build_svg(rows, path):
    """逐设备平均离群率：Java 8 与 Java 11 并排（runbook 第 6 节要求的那张图）。
    Per-device mean outlier rate, Java 8 beside Java 11 (the figure section 6 asks for)."""
    if not rows:
        return
    peak = max(max(r["rate11"], r["rate8"] or 0.0) for r in rows) or 1e-9
    x0, plot_w, row_h = 118, 470, 34
    height = 56 + row_h * len(rows) + 30
    px = plot_w / (peak * 1.18)
    p = ['<svg xmlns="http://www.w3.org/2000/svg" width="640" height="%d" viewBox="0 0 640 %d" '
         'font-family="system-ui,-apple-system,Segoe UI,sans-serif">' % (height, height),
         '<rect width="640" height="%d" fill="#ffffff"/>' % height,
         '<text x="12" y="24" font-size="13" font-weight="600" fill="#16191f">'
         '逐设备平均离群率 / mean outlier rate per device</text>',
         '<rect x="%d" y="34" width="11" height="9" fill="#3d6ea8"/>' % x0,
         '<text x="%d" y="42" font-size="10" fill="#414a56">Java 8 (probe)</text>' % (x0 + 16),
         '<rect x="%d" y="34" width="11" height="9" fill="#0f6d78"/>' % (x0 + 118),
         '<text x="%d" y="42" font-size="10" fill="#414a56">Java 11 (job)</text>' % (x0 + 134)]
    for i, r in enumerate(rows):
        y = 56 + i * row_h
        p.append('<text x="10" y="%d" font-size="11" fill="#16191f">%s (R=%g)</text>'
                 % (y + 16, r["device"], r["R"]))
        w8 = max((r["rate8"] or 0.0) * px, 0.6)
        w11 = max(r["rate11"] * px, 0.6)
        p.append('<rect x="%d" y="%d" width="%.2f" height="11" fill="#3d6ea8"/>' % (x0, y, w8))
        p.append('<rect x="%d" y="%d" width="%.2f" height="11" fill="#0f6d78"/>' % (x0, y + 13, w11))
        p.append('<text x="%.2f" y="%d" font-size="9.5" fill="#6b7480">%.4f%%</text>'
                 % (x0 + w8 + 5, y + 9, (r["rate8"] or 0.0) * 100))
        p.append('<text x="%.2f" y="%d" font-size="9.5" fill="#6b7480">%.4f%%</text>'
                 % (x0 + w11 + 5, y + 22, r["rate11"] * 100))
    p.append('<line x1="%d" y1="48" x2="%d" y2="%d" stroke="#c2cad6"/>'
             % (x0 - 3, x0 - 3, 56 + row_h * len(rows)))
    p.append('</svg>')
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(p))


def main():
    ap = argparse.ArgumentParser(description="Aggregate M2 per-device baseline and compare with Java 8.")
    ap.add_argument("--monitoring-jsonl", required=True)
    ap.add_argument("--java8-probe", default="docs/m2_probe_7d_clean.csv")
    ap.add_argument("--r-per-device", default=os.environ.get("SYN_M2_R_PER_DEVICE", DEFAULT_R))
    ap.add_argument("--tol-rel", type=float, default=1.0, help="相对容差（%%）/ relative tolerance in percent")
    ap.add_argument("--no-compare", action="store_true")
    ap.add_argument("--out-csv"); ap.add_argument("--out-md"); ap.add_argument("--out-svg")
    a = ap.parse_args()

    r_map = parse_r_per_device(a.r_per_device.strip('"').strip("'"))
    per_dev, total, kept, bad = load_snapshots(a.monitoring_jsonl)
    print("转储行数 total=%d，M2 快照 kept=%d，解析失败 bad=%d" % (total, kept, bad))
    if not kept:
        print("ERROR: 转储里没有 M2 快照（windowEnd 全为 0）。可能整段仍在预热期，或转储时段不对。", file=sys.stderr)
        print("ERROR: no M2 snapshots in the dump — still in warm-up, or the wrong window was dumped.", file=sys.stderr)
        return 2

    ref = {} if a.no_compare else load_java8(a.java8_probe, r_map)
    rows = []
    for dev in DEVICES:
        recs = per_dev.get(dev, [])
        if not recs:
            print("  [WARN] 设备 %s 没有任何 M2 快照 —— 若该时段该设备停机则属预期，否则需人工判读。" % dev)
            continue
        out_sum = sum(x[4] for x in recs)
        pts_sum = sum(x[5] for x in recs)
        rows.append({
            "device": dev, "R": r_map.get(dev, float("nan")), "slides": len(recs),
            "rate11": mean([x[0] for x in recs]),
            "occ": mean([x[1] for x in recs]),
            "p10": mean([x[2] for x in recs]),
            "p50": mean([x[3] for x in recs]),
            "outliers": out_sum, "points": pts_sum,
            "weighted": (out_sum / pts_sum) if pts_sum else 0.0,
            "rate8": ref.get(dev, {}).get("rate"),
            "slides8": ref.get(dev, {}).get("slides"),
        })

    med = sorted(r["slides"] for r in rows)[len(rows) // 2] if rows else 0
    fail = []
    print("\n%-4s %-5s %8s %11s %11s %9s %8s %7s %7s" %
          ("dev", "R", "slides", "J11 rate%", "J8 rate%", "相对差%", "MC占用", "P10", "P50"))
    for r in rows:
        if r["rate8"]:
            rel = (r["rate11"] - r["rate8"]) / r["rate8"] * 100.0
            r["rel"] = rel
            if abs(rel) > a.tol_rel:
                fail.append(r)
            reld, j8 = "%+.2f" % rel, "%.4f" % (r["rate8"] * 100)
        else:
            r["rel"] = None
            reld, j8 = "—", "—"
        print("%-4s %-5g %8d %11.4f %11s %9s %8.3f %7.2f %7.2f" %
              (r["device"], r["R"], r["slides"], r["rate11"] * 100, j8, reld,
               r["occ"], r["p10"], r["p50"]))
        if med and r["slides"] < med * 0.9:
            print("       [WARN] 滑动步数明显少于中位数 %d —— 该设备可能有停机段，需人工判读。" % med)

    print("\n补充口径 / secondary definition（按点数加权的总体比率，勿与上表的 meanOutlierRate 混用）：")
    for r in rows:
        print("  %s  outliers=%d / points=%d = %.4f%%" %
              (r["device"], r["outliers"], r["points"], r["weighted"] * 100))

    if a.out_csv:
        os.makedirs(os.path.dirname(a.out_csv) or ".", exist_ok=True)
        with open(a.out_csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["device", "R", "slides", "meanOutlierRate_java11", "meanOutlierRate_java8",
                        "relDiffPct", "meanMcOccupancy", "meanNeighborP10", "meanNeighborP50",
                        "outliersTotal", "pointsTotal", "weightedRate"])
            for r in rows:
                w.writerow([r["device"], r["R"], r["slides"], "%.8f" % r["rate11"],
                            "" if r["rate8"] is None else "%.8f" % r["rate8"],
                            "" if r["rel"] is None else "%.4f" % r["rel"],
                            "%.6f" % r["occ"], "%.4f" % r["p10"], "%.4f" % r["p50"],
                            r["outliers"], r["points"], "%.8f" % r["weighted"]])
        print("\n-> %s" % a.out_csv)

    if a.out_md:
        os.makedirs(os.path.dirname(a.out_md) or ".", exist_ok=True)
        with open(a.out_md, "w", encoding="utf-8") as fh:
            fh.write("| 设备 | R | 滑动步 | Java 11 离群率 | Java 8 离群率 | 相对差 | 微簇占用 | 邻居 P10 | 邻居 P50 |\n")
            fh.write("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
            for r in rows:
                fh.write("| %s | %g | %d | %.4f%% | %s | %s | %.3f | %.2f | %.2f |\n" % (
                    r["device"], r["R"], r["slides"], r["rate11"] * 100,
                    "—" if r["rate8"] is None else "%.4f%%" % (r["rate8"] * 100),
                    "—" if r["rel"] is None else "%+.2f%%" % r["rel"],
                    r["occ"], r["p10"], r["p50"]))
        print("-> %s" % a.out_md)

    if a.out_svg:
        build_svg(rows, a.out_svg)
        print("-> %s" % a.out_svg)

    if a.no_compare:
        print("\n(--no-compare：只聚合未比对)")
        return 0
    if fail:
        print("\n[FAIL] 以下设备超出 %.1f%% 相对容差：%s" % (a.tol_rel, ", ".join(r["device"] for r in fail)))
        print("       按 runbook 第 5 节：**不要调参**。先查清是探针与作业的机制差异（窗口边界、标定切换时刻），")
        print("       还是真实偏差；无法用机制差异解释的偏差是需要上报设计会话的发现。")
        print("[FAIL] Do not tune. Identify the probe-versus-job mechanism first; an unexplained deviation is a finding.")
        return 1
    # 结论必须写明实际比对了几台，绝不能把"没比"说成"等值"。
    # The verdict must say how many were actually compared; never report "equal" for a device not compared.
    compared = [r for r in rows if r["rate8"] is not None]
    skipped = [r["device"] for r in rows if r["rate8"] is None]
    if not compared:
        print("\n[FAIL] 没有任何设备完成比对（参照表缺少全部设备的对应行）——不能据此下等值结论。")
        print("[FAIL] No device was compared; the reference table matched none of them.")
        return 1
    print("\n[PASS] 已比对的 %d 台设备全部在 %.1f%% 相对容差内 —— 与 Java 8 探针等值。"
          % (len(compared), a.tol_rel))
    if skipped:
        print("[PART] 但以下设备**未参与比对**（参照表无对应行），其等值性尚未验证：%s"
              % ", ".join(skipped))
        print("[PART] These devices were NOT compared and their equality is unverified: %s"
              % ", ".join(skipped))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
