#!/usr/bin/env bash
# ============================================================================
# m6b/q12_seg.sh
# 1.40 版裁决的逐段检出核对：按档丙（湿度与气压用冻结参数、训练集取生效后 N 天，N 取三与七，热冷，五种子）原样重跑二十次重训，
# 另加初始模型在同一注入日上的评价，每次运行另写注入日六段的逐段检出（重叠窗口数、超阈数、最大窗口误差与门槛）。训练逐位可复现，
# 重跑的结果行应与 docs/m6b/q12c/rows/ 逐字段相同（耗时除外），由 m6b/q12_seg_report.py 核对。
# Per-segment detection check for arm C (ruling 1.40): rerun the twenty arm-C retrains plus the initial models with --seg-out.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机，仓库根目录，JDK 11。
# 2. 调用命令 / Invocation: setsid nohup bash m6b/q12_seg.sh >> docs/m6b/q12seg.log 2>&1 < /dev/null &
# 3. 前置条件 / Preconditions: target/q12-seg.jar 为含 --seg-out 的 jar 副本；docs/m6b/q12/models/ 下五个初始模型在。
# 4. 期望产出 / Expected output: docs/m6b/q12seg/ 下 rows/、seg/、diag/、logs/ 与 ALL_DONE；约一小时。
# 5. 失败兜底 / Failure fallback: 失败的运行在日志里记为「失败」，重跑本脚本只补缺的运行。
# ============================================================================
cd "$(dirname "$0")/.." || exit 2
export Q12_JAR=target/q12-seg.jar Q12_MODELS=docs/m6b/q12/models Q12_SEG=1
Q12_DIR=docs/m6b/q12seg Q12_NS="7 3" Q12_INIT_NS=3 Q12_ARGS="--freeze-channels 1,2 --train-side after" Q12_INIT_EVAL=1 \
    bash m6b/q12_run.sh
