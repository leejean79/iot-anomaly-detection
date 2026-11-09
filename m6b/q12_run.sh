#!/usr/bin/env bash
# ============================================================================
# m6b/q12_run.sh
# 开放问题第十二条离线实验的批量运行（M6b 交接文档第五节）：五个种子的初始模型，再对每个种子跑 N 取三、七、十四天，热启动与冷启动
# 各一次，共三十次重训；每次一个 com.leejean.m3.M6bRetrainEval 进程，两个进程并行，每个进程 OMP_NUM_THREADS=2（开发机四核；
# 不限线程时 DL4J 训练慢约五十倍）。已有结果行的任务跳过，所以中断后重跑会接着做。
# Batch run of the open-question-12 experiment: five initial models, then thirty retrains, two processes at a time.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机，仓库根目录，JDK 11（JAVA_HOME 指向它），已 mvn package。
# 2. 调用命令 / Invocation: nohup bash m6b/q12_run.sh > docs/m6b/q12/run.log 2>&1 &
#    可选环境变量：Q12_SEEDS（默认 "42 7 1 2 3"）、Q12_PAR（并行进程数，默认 2）、Q12_DIR（默认 docs/m6b/q12）。
# 3. 前置条件 / Preconditions: docs/m6b/extract/rounds_E_inj.csv.gz、docs/m6b/chain/D7/level0/E_1.json 与 E_2.json、
#    docs/m6b/chain/D7/outliers.csv 已生成（见 m6a/inject_rounds.py 与 ChainDriver）。
# 4. 期望产出 / Expected output: Q12_DIR/rows/ 下每次运行一个结果行文件，Q12_DIR/logs/ 下逐个日志，Q12_DIR/models/ 下初始模型；
#    全部完成后写 Q12_DIR/ALL_DONE。总时长约三到四小时。
# 5. 失败兜底 / Failure fallback: 某次运行失败时其日志末尾有错误信息，结果行缺失；重跑本脚本只补缺失的任务。
# ============================================================================
set -u
DIR="${Q12_DIR:-docs/m6b/q12}"
SEEDS="${Q12_SEEDS:-42 7 1 2 3}"
PAR="${Q12_PAR:-2}"
JAR=target/iot-anomaly-detection-1.0-SNAPSHOT.jar
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
export OMP_NUM_THREADS=2
mkdir -p "$DIR/rows" "$DIR/logs" "$DIR/models"
rm -f "$DIR/ALL_DONE"

job() {   # 参数：种子 阶段 N 启动方式 / seed phase n start
    local seed=$1 phase=$2 n=$3 start=$4 tag
    if [ "$phase" = init ]; then tag="init_$seed"; else tag="r_${seed}_${n}_${start}"; fi
    [ -s "$DIR/rows/$tag.csv" ] && return 0
    "$JAVA" -Xmx3g -cp "$JAR" com.leejean.m3.M6bRetrainEval --phase "$phase" --seed "$seed" --n "$n" --start "$start" \
        --model-dir "$DIR/models" --out "$DIR/rows/$tag.csv.tmp" > "$DIR/logs/$tag.log" 2>&1 \
        && mv "$DIR/rows/$tag.csv.tmp" "$DIR/rows/$tag.csv"
    echo "$(date -u +%H:%M:%S) $tag $([ -s "$DIR/rows/$tag.csv" ] && echo 完成 || echo 失败)"
}
export -f job
export DIR JAR JAVA

echo "$(date -u +%H:%M:%S) 初始模型：种子 $SEEDS"
for s in $SEEDS; do echo "$s init 0 none"; done | xargs -P "$PAR" -L 1 bash -c 'job "$@"' _
echo "$(date -u +%H:%M:%S) 重训"
for n in 14 7 3; do for s in $SEEDS; do for st in warm cold; do echo "$s retrain $n $st"; done; done; done \
    | xargs -P "$PAR" -L 1 bash -c 'job "$@"' _
date -u > "$DIR/ALL_DONE"
echo "$(date -u +%H:%M:%S) 全部完成"
