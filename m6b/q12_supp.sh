#!/usr/bin/env bash
# ============================================================================
# m6b/q12_supp.sh
# 开放问题第十二条的补跑（1.39 版裁决第二项）：顺序跑两档，每档用 m6b/q12_run.sh。
#   档乙：湿度与气压的标准化参数换成冻结参数（--freeze-channels 1,2），训练集仍取生效前 N 天，N 取十四、七、三，三十次重训。
#   档丙：在档乙之上，训练集改取生效之后的 N 个当地日（--train-side after），N 取七、三，二十次重训。
# 两档都沿用 docs/m6b/q12/models 中的初始模型（不重新训练），初始模型按各档的评价口径只做评价（档乙的评价日与 N 无关，只评价一次，行内 N 记为七）；每次运行写逐通道诊断。
# Supplementary arms of ruling 1.39, run one after the other.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机，仓库根目录，JDK 11（JAVA_HOME 指向它）。
# 2. 调用命令 / Invocation: setsid nohup bash m6b/q12_supp.sh > docs/m6b/q12_supp.log 2>&1 < /dev/null &
#    可选环境变量：Q12_PAR（并行进程数，默认 2）、Q12_SEEDS（默认 "42 7 1 2 3"）。
# 3. 前置条件 / Preconditions: 已 mvn package，并把 shaded jar 复制为 target/q12-run.jar（运行期间重新打包不影响它）；
#    docs/m6b/q12/models/init_<种子>.bin 已由第一轮网格生成；m6b/q12_run.sh 的其余前置条件。
# 4. 期望产出 / Expected output: docs/m6b/q12b/ 与 docs/m6b/q12c/ 下各有 rows/、diag/、logs/ 与 ALL_DONE；两档都完成后写
#    docs/m6b/q12_supp_DONE。总时长约七小时。
# 5. 失败兜底 / Failure fallback: 某次运行失败时 rows/ 缺该行、logs/ 中有错误信息；重跑本脚本只补缺失的任务。
# ============================================================================
set -u
export Q12_JAR=target/q12-run.jar Q12_MODELS=docs/m6b/q12/models
rm -f docs/m6b/q12_supp_DONE
echo "$(date -u +%H:%M:%S) 档乙"
Q12_DIR=docs/m6b/q12b Q12_NS="14 7 3" Q12_ARGS="--freeze-channels 1,2" Q12_INIT_EVAL=1 Q12_INIT_NS=7 bash m6b/q12_run.sh
echo "$(date -u +%H:%M:%S) 档丙"
Q12_DIR=docs/m6b/q12c Q12_NS="7 3" Q12_ARGS="--freeze-channels 1,2 --train-side after" Q12_INIT_EVAL=1 bash m6b/q12_run.sh
date -u > docs/m6b/q12_supp_DONE
echo "$(date -u +%H:%M:%S) 两档完成"
