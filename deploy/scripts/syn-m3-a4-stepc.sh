#!/usr/bin/env bash
# ============================================================================
# syn-m3-a4-stepc.sh
# 补遗四步骤 C（短重放预验证）的五次运行一键串行执行：t1、t2、t4、kill、cancel 依次「复位 → 提交 → master 上
# 启动采样 → 重放 →（扰动）→ 等重放结束并排空 → 收集 → 取消作业 → 取回采样」，全部完成后生成汇总报告。
# Runs the five step-C runs of addendum 4 back to back, unattended, then writes the summary report.
#
# 为什么在本地 Mac 上运行：扰动（强杀任务管理器）要从 Mac 登录 worker，收集结果要写进本地仓库。所以 start
# 用 `caffeinate -i nohup` 把自己放到后台，本地终端可以关闭；但全程（约四个半小时）Mac 须接电源、不睡眠、不断网
# （`caffeinate -i` 阻止空闲睡眠，合上屏幕仍会睡眠）。
# Runs on the Mac because the kill disruption must log in to a worker and results land in the local repo;
# start backgrounds itself with caffeinate/nohup, so the terminal may be closed but the Mac must stay awake.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录；ssh 别名 fa-master、fa-worker1、fa-worker2 可用；python3。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m3-a4-stepc.sh start                     # 依次跑五次，立即返回
#      bash deploy/scripts/syn-m3-a4-stepc.sh start --runs kill,cancel  # 只跑其中几次（例如中断后续跑）
#      bash deploy/scripts/syn-m3-a4-stepc.sh status                    # 随时查看进度，不需要一直开着
#      bash deploy/scripts/syn-m3-a4-stepc.sh stop                      # 中止：停编排、停重放、取消本项目作业
# 3. 前置条件 / Preconditions: 运行手册第一节已完成（任务管理器 5120 兆、jar 已构建核对并上传）；集群上没有其他
#    项目（FA-iForest）的作业；没有本项目的作业在运行。
# 4. 期望产出 / Expected output: start 打印「已在后台启动」与日志路径；进度写入 docs/m3_a4/progress.txt，完整日志写入
#    docs/m3_a4/stepc_all.log；每次运行的结果在 docs/m3_a4/stepc_<标签>/；五次完成后生成 docs/m3_a4/stepc_report.md，
#    progress.txt 最后一行为「全部完成」并给出提交命令。
# 5. 失败兜底 / Failure fallback: 任何一步失败时，脚本取消本次作业、停止重放、取回已有采样，在 progress.txt 写明失败
#    的运行与步骤后停止（不继续后面的运行）；排查后用 `start --runs <剩下的标签>` 续跑。退出码：2 参数错误；3 已有
#    编排在运行（先 status 或 stop）。
# ============================================================================
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a
cd "$REPO_DIR" || exit 2

OUT=docs/m3_a4
PROGRESS=$OUT/progress.txt
LOG=$OUT/stepc_all.log
PIDF=$OUT/stepc_all.pid
ALL_RUNS="t1,t2,t4,kill,cancel"
CMD="${1:-}"; shift || true
RUNS="$ALL_RUNS"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --runs) RUNS="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

# 每次运行的参数：线程数、倍速、扰动 / per-run threads, speed and disruption
threads_of() { case "$1" in t1) echo 1 ;; t4) echo 4 ;; *) echo 2 ;; esac; }
speed_of()   { case "$1" in cancel) echo 600 ;; *) echo 120 ;; esac; }
disrupt_of() { case "$1" in kill|cancel) echo "$1" ;; *) echo "" ;; esac; }

prog() { echo "[$(date '+%F %T')] $*" | tee -a "$PROGRESS"; }
alive() { [ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null; }

case "$CMD" in
    start)
        for r in ${RUNS//,/ }; do
            [[ ",$ALL_RUNS," == *",$r,"* ]] || { echo "ERROR: 未知的运行标签 $r（可选 $ALL_RUNS）" >&2; exit 2; }
        done
        alive && { echo "ERROR: 已有编排在运行（PID $(cat "$PIDF")）。先 status 查看，或 stop 中止。" >&2; exit 3; }
        mkdir -p "$OUT"
        caffeinate -i nohup bash "$SCRIPT_DIR/$(basename "$0")" run --runs "$RUNS" >> "$LOG" 2>&1 &
        echo $! > "$PIDF"
        echo "[stepc] 已在后台启动（PID $!），依次运行：$RUNS。本地终端可以关闭，但 Mac 须接电源、不睡眠、不断网。"
        echo "        查看进度：bash $0 status    完整日志：$LOG"
        ;;
    status)
        echo "== 编排：$(alive && echo "运行中（PID $(cat "$PIDF")）" || echo '未运行') =="
        tail -n 15 "$PROGRESS" 2>/dev/null || echo "（还没有进度记录）"
        echo
        bash "$SCRIPT_DIR/syn-m3-watch.sh" status 2>/dev/null | head -n 30
        ;;
    stop)
        if alive; then pkill -P "$(cat "$PIDF")" 2>/dev/null; kill "$(cat "$PIDF")" 2>/dev/null; fi
        pkill -f syn-m3-a4-disrupt.sh 2>/dev/null
        rm -f "$PIDF"
        bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
        if [ -f "$OUT/current_run.env" ]; then
            source "$OUT/current_run.env"
            ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1 && echo "[stepc] 已取消作业 $JID"
        fi
        bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m3watch_aborted >/dev/null 2>&1
        prog "已手动中止（stop）。重放已停止，本项目作业已取消，采样会话已结束。"
        ;;
    run)
        ;;
    *)
        echo "用法 / usage: $0 start [--runs t1,t2,t4,kill,cancel] | status | stop" >&2
        exit 2 ;;
esac
[ "$CMD" = run ] || exit 0

# ============================== 后台编排 / background orchestration ==============================
REST="http://$NODE_MASTER_IP:8081"
EXTRA_BASE="--m3-enabled true --warmup-rounds 8640 --m3-rounds-per-day 864 --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 7 --m3-window-length 60 --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0 --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true --checkpoint-ms 30000 --checkpoint-timeout-min 10 --checkpoint-tolerable-failures 3 --idle-wall 60"

# 失败时的收尾：取消作业、停重放、取回采样，然后停止整个编排。/ on failure: clean up and stop everything.
fail() {
    prog "失败：运行 $RUN，$*。已停止后续运行；排查后用 start --runs <剩下的标签> 续跑。"
    ssh fa-master "docker exec jobmanager flink cancel ${JID:-none}" >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir "$DIR" >/dev/null 2>&1
    rm -f "$PIDF"
    exit 1
}

# 已排空的设备数：日志里出现「buffer drained」或「收回时缓冲为 0」的不同设备 / devices that finished draining
drained_devices() {
    for pair in "fa-worker1 taskmanager-2" "fa-worker2 taskmanager-3"; do
        set -- $pair
        ssh "$1" "docker logs --since $SINCE $2 2>&1 | grep -E 'buffer drained|; 0 buffered rounds to drain'" 2>/dev/null
    done | sed -n 's/.*\[M3\] Device \([^ ]*\) .*/\1/p' | sort -u | wc -l | tr -d ' '
}

run_one() {
    RUN="$1"; THREADS=$(threads_of "$RUN"); SPEED=$(speed_of "$RUN"); DIS=$(disrupt_of "$RUN")
    DIR="$OUT/stepc_$RUN"; JID=""
    rm -rf "$DIR"; mkdir -p "$DIR"
    prog "== 运行 $RUN 开始：线程数 $THREADS，倍速 $SPEED，扰动 ${DIS:-无} =="

    bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$DIR/reset.log" 2>&1 || fail "复位未通过（见 $DIR/reset.log）"
    SINCE=$(ssh fa-master date -u +%Y-%m-%dT%H:%M:%SZ)
    bash "$SCRIPT_DIR/syn-submit-m2.sh" --extra "$EXTRA_BASE --train-threads $THREADS" > "$DIR/submit.log" 2>&1 \
        || fail "提交失败（见 $DIR/submit.log）"
    JID=$(grep -oE 'JobID [a-f0-9]{32}' "$DIR/submit.log" | awk '{print $2}' | head -1)
    [ -n "$JID" ] || fail "提交输出里没有 JobID"
    printf 'RUN=%s\nTHREADS=%s\nSINCE=%s\nJID=%s\n' "$RUN" "$THREADS" "$SINCE" "$JID" | tee "$OUT/current_run.env" > "$DIR/run.env"
    prog "作业已提交：$JID（日志起点 $SINCE）"

    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m3watch_leftover >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-m3-watch.sh" start --jid "$JID" --since "$SINCE" >> "$DIR/watch.log" 2>&1 \
        || fail "master 上的采样没有启动（见 $DIR/watch.log）"
    bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-replay.sh" --speedup "$SPEED" --start 2022-03-01 --end 2022-03-05 > "$DIR/replay_start.log" 2>&1 \
        || fail "重放没有启动（见 $DIR/replay_start.log）"
    prog "采样与重放已启动"

    if [ -n "$DIS" ]; then
        prog "等待第一台设备开始训练后执行扰动：$DIS"
        bash "$SCRIPT_DIR/syn-m3-a4-disrupt.sh" "$DIS" --jid "$JID" --since "$SINCE" --out "$DIR/$RUN.txt" \
            >> "$DIR/disrupt.log" 2>&1 || fail "扰动失败（见 $DIR/$RUN.txt）"
        prog "扰动完成：$(grep -E '取消端到端|重新 RUNNING' "$DIR/$RUN.txt" | tail -1)"
        if [ "$DIS" = cancel ]; then
            bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir "$DIR" >/dev/null 2>&1
            bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
            prog "== 运行 $RUN 完成 =="
            return 0
        fi
    fi

    # 等重放结束（最长 3 小时）/ wait for the replay to finish (at most 3 h)
    for _ in $(seq 1 180); do
        ssh fa-master "tmux has-session -t syn-replay 2>/dev/null" || break
        sleep 60
    done
    ssh fa-master "tmux has-session -t syn-replay 2>/dev/null" && fail "重放 3 小时仍未结束"
    prog "重放结束：$(ssh fa-master "grep -E 'rc=' /opt/fa-iforest/syn-replay.log | tail -1")"

    # 等八台设备排空（最长 60 分钟），再确认计数器连续两次不变 / wait for all eight devices to drain, then stable
    for _ in $(seq 1 60); do
        [ "$(drained_devices)" -ge 8 ] && break
        sleep 60
    done
    N=$(drained_devices)
    [ "$N" -ge 8 ] || prog "警告：60 分钟后只有 $N 台设备排空，仍继续收集（汇总报告会显示缺哪台）"
    for _ in $(seq 1 10); do
        bash "$SCRIPT_DIR/syn-m2-metrics.sh" > /tmp/a4_m1.txt 2>&1; sleep 120
        bash "$SCRIPT_DIR/syn-m2-metrics.sh" > /tmp/a4_m2.txt 2>&1
        diff -q /tmp/a4_m1.txt /tmp/a4_m2.txt >/dev/null && break
    done
    prog "已排空（$N 台设备完成排空）"

    bash "$SCRIPT_DIR/syn-m3-march-collect.sh" --out-dir "$DIR" --since "$SINCE" > "$DIR/collect.log" 2>&1 \
        || fail "收集失败（见 $DIR/collect.log）"
    ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir "$DIR" >/dev/null 2>&1
    rm -f "$DIR/scores.jsonl"
    ssh fa-master 'rm -rf /opt/fa-iforest/m3march'
    prog "== 运行 $RUN 完成：$(grep -E '训练完成的设备数' "$DIR/collect_summary.txt" 2>/dev/null) =="
}

prog "编排开始：$RUNS"
for r in ${RUNS//,/ }; do
    run_one "$r"
done
bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$OUT/reset_final.log" 2>&1

# 汇总：只纳入已有结果的运行 / summary over the runs that have results
ARGS=""
for r in t1 t2 t4 kill; do [ -s "$OUT/stepc_$r/m3_tm_log.txt" ] && ARGS="$ARGS,$r=$OUT/stepc_$r"; done
CMP=""; [ -s "$OUT/stepc_kill/m3_tm_log.txt" ] && [ -s "$OUT/stepc_t2/m3_tm_log.txt" ] && CMP="--compare kill:t2"
CAN=""; [ -s "$OUT/stepc_cancel/cancel.txt" ] && CAN="--cancel-log $OUT/stepc_cancel/cancel.txt"
if [ -n "$ARGS" ]; then
    python3 "$SCRIPT_DIR/m3_a4_stepc_report.py" --runs "${ARGS#,}" $CMP $CAN --out "$OUT/stepc_report.md" \
        > "$OUT/report.log" 2>&1 && prog "汇总报告已生成：$OUT/stepc_report.md" || prog "汇总报告生成失败（见 $OUT/report.log）"
fi
rm -f "$PIDF"
prog "全部完成。请执行：git add -f docs/m3_a4 && git commit -m \"data(m3): 补遗四步骤 C 短重放预验证结果\" && git push origin dev-claude"
