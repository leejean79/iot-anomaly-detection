#!/usr/bin/env bash
# ============================================================================
# syn-m2-metrics.sh
# 读 .env 的 master 公网 IP，调用 m2_metrics.py 拉 M2/M1 计数器并做 V-M2-2 对账。
# Read the master IP from .env and run m2_metrics.py to fetch counters and reconcile (V-M2-2).
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地（python3）；能访问 http://<master>:8081（Flink UI 同一地址）。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m2-metrics.sh                 # 自动找 RUNNING 的 M2Job
#      bash deploy/scripts/syn-m2-metrics.sh --job <jobid>   # 指定作业
# 3. 前置条件 / Preconditions: M2Job RUNNING；REST 端口 8081 从本机可达。
# 4. 期望产出 / Expected output: 计数器表 + 对账行（admitted = rounds − warmup − missing）。
# 5. 失败兜底 / Failure fallback: REST 不可达 → 改用 Flink UI 手动读；作业非 RUNNING → 先提交。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

MASTER="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
FLINK_URL="http://${MASTER}:${FLINK_UI_PORT:-8081}"

python3 "$SCRIPT_DIR/m2_metrics.py" --flink-url "$FLINK_URL" "$@"
