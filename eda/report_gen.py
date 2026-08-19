#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
report_gen.py —— 由聚合结果生成 CSV 表格、图表与 EDA 报告（EDA 阶段入口 2/2）。
report_gen.py -- turn the aggregate into CSV tables, figures and the EDA report
(EDA stage entry point 2 of 2).

================================ 脚本交付五要素 ================================
================== Five-element script delivery (per CLAUDE.md) ================

【1. 执行环境 / execution environment】
  - macOS / Linux，Python 3.9+
  - 依赖：matplotlib ≥ 3.3（作图）、numpy；**不读原始数据集**，因此不需要数据目录可访问
  - 图内文字一律英文（Mac 默认字体无中文字形，中文会渲染成方块）；报告正文中英双语

【2. 调用命令 / invocation】
  python3 eda/report_gen.py --output-dir eda_output
  # 指定非默认聚合文件 / custom aggregate location
  python3 eda/report_gen.py --aggregate path/to/aggregate.json --output-dir eda_output

【3. 前置条件 / preconditions】
  - 已执行 run_eda.py 并生成 <output-dir>/aggregate.json（本脚本的唯一输入）
  - <output-dir> 可写；重复执行会覆盖同名产出（幂等，可反复出图）

【4. 期望产出 / expected outputs】
  E2: device_sensor_counts.csv, uptime_matrix.csv, uptime_timeline.png,
      round_completeness.csv, gaps_top100.csv, gap_summary.csv, gap_duration_hist.png
  E3: channel_stats.csv, channel_stats_monthly.csv, accel_nonzero.csv, unit_sanity.csv,
      mic_distribution.csv, degenerate_channels.csv
  E4: interarrival_quantiles.csv, interarrival_distribution.png, clock_skew_sample.csv
  E5: monthly_quantile_drift_<channel>.png, ks_adjacent_months.csv, seasonal_trend.png,
      daily_channel_means.csv
  汇总: eda_report.md（五层结果 + 图表引用 + §6 八问的「M1 设计参数建议」章节）

【5. 失败兜底 / failure fallback】
  - aggregate.json 缺失/损坏 → 明确报错并提示先跑 run_eda.py，退出码 2
  - 某一层数据为空（如未采集时钟相位样本）→ 该层产出空表并在报告中标注
    「数据不足以回答」，不中断其余层
  - 单个图表渲染失败 → 打印警告并继续，报告仍生成（图表引用处保留文件名）
  - 全部产出可重复生成：run_eda.py 的 aggregate.json 不被修改
==============================================================================
"""

import argparse
import json
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from edalib import config, report  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="由 aggregate.json 生成 EDA 表格、图表与报告 / render EDA outputs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--output-dir", default=config.DEFAULT_OUTPUT_DIR,
                   help="产出目录（同时是 aggregate.json 的默认位置）/ output directory")
    p.add_argument("--aggregate", default=None,
                   help="聚合文件路径，默认 <output-dir>/aggregate.json / aggregate file path")
    p.add_argument("--baseline", default=None,
                   help="首轮 aggregate.json 作基线，生成补丁 §7.3 关键指标差异表（可选）"
                        " / a prior aggregate.json as baseline for the patch §7.3 diff table")
    return p


def _step(name: str, fn, *args, **kwargs):
    """
    单步执行包装：失败只警告不中断，保证报告仍能产出。
    Step wrapper: a failure warns instead of aborting so the report is still produced.
    """
    try:
        return fn(*args, **kwargs)
    except Exception:  # noqa: BLE001
        print("[警告 / warning] %s 生成失败，已跳过 / failed, skipped:" % name, file=sys.stderr)
        traceback.print_exc()
        return {}


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    out_dir = os.path.abspath(os.path.expanduser(args.output_dir))
    agg_path = args.aggregate or os.path.join(out_dir, config.AGGREGATE_FILENAME)

    if not os.path.isfile(agg_path):
        print("[错误 / error] 找不到聚合文件 / aggregate not found: %s\n"
              "  请先执行 / run first: python3 eda/run_eda.py --data-dir <CSV_DIR>" % agg_path,
              file=sys.stderr)
        return 2
    try:
        with open(agg_path, encoding="utf-8") as fh:
            agg = json.load(fh)
    except Exception as exc:  # noqa: BLE001
        print("[错误 / error] 聚合文件损坏 / aggregate is corrupt: %s: %s\n"
              "  重新执行 run_eda.py 可重建（逐文件缓存仍有效，扫描会很快）"
              % (type(exc).__name__, exc), file=sys.stderr)
        return 2

    os.makedirs(out_dir, exist_ok=True)
    run = agg.get("run", {})
    if run.get("limit"):
        print("[提示 / note] 本聚合来自 --limit %s 的子集运行，报告结论仅对该子集有效。"
              % run["limit"])

    # 补丁 01：若提供基线聚合，计算关键指标差异并注入 agg["_diff"]（供报告 §7.3）。
    # Patch 01: if a baseline aggregate is given, compute the metric diff for report §7.3.
    if args.baseline:
        base_path = os.path.abspath(os.path.expanduser(args.baseline))
        try:
            with open(base_path, encoding="utf-8") as fh:
                base_agg = json.load(fh)
            diff = report.diff_digests(report.metrics_digest(base_agg),
                                       report.metrics_digest(agg),
                                       os.path.basename(base_path))
            agg["_diff"] = diff
            print("[基线 / baseline] 差异项 %d 个 / %d changed metric(s)" % (len(diff["rows"]), len(diff["rows"])))
        except Exception as exc:  # noqa: BLE001
            print("[警告 / warning] 基线聚合无法用于差异对照，已跳过 §7.3 差异表 / baseline unusable: %s: %s"
                  % (type(exc).__name__, exc), file=sys.stderr)

    print("[1/6] 卷积聚合视角 / rolling up aggregate views")
    by_ds, by_ds_hist, by_ms_hist, by_s_hist, by_mds = report._roll_up(agg)

    print("[2/6] E1 清单层 / inventory")
    e1 = _step("E1", report.emit_e1, agg, out_dir)
    print("[3/6] E2 完整性层 / completeness")
    e2 = _step("E2", report.emit_e2, agg, out_dir, by_ds)
    print("[4/6] E3 数值层 / numeric")
    e3 = _step("E3", report.emit_e3, agg, out_dir, by_ds, by_ds_hist, by_mds, by_s_hist)
    print("[5/6] E4 节奏层 + E5 漂移预览层 / rhythm and drift preview")
    e4 = _step("E4", report.emit_e4, agg, out_dir)
    e5 = _step("E5", report.emit_e5, agg, out_dir, by_ms_hist)

    print("[6/6] 汇总报告 / summary report")
    facts = _step("data-fact cross-check", report.check_data_facts, agg, e2, e3, e4, by_ds) or []
    path = report.write_report(agg, out_dir, e1, e2, e3, e4, e5, by_ds, by_ds_hist, facts)

    conflicts = [f for f in facts if "冲突" in f[3]]
    print("\n===== 报告摘要 / report summary =====")
    print("  报告 / report        : %s" % path)
    print("  产出目录 / outputs   : %s" % out_dir)
    if conflicts:
        print("  ⚠ 与交接文档 §3 冲突的数据事实 / conflicting data facts: %s"
              % ", ".join(f[0] for f in conflicts))
        print("    → 已在报告 §7 标注为新数据事实候选，请交回设计会话登记。")
    else:
        print("  与交接文档 §3 数据事实无冲突 / no conflicts with handover §3")
    return 0


if __name__ == "__main__":
    sys.exit(main())
