#!/usr/bin/env bash
# ============================================================================
# syn-m5b-verify.sh
# M5b 验收项 V-M5b-3（离线与在线等值）的短重放验证，在 master 上无人值守地一键执行（M5b 交接文档第五节）：
#   两次步骤 C 规模的短重放（2022-03-01 至 03-05，每天折合 864 轮，线程数 2，120 倍速），都在设备 E 上注入一段 12 小时的温度阶跃
#   （03-04 00:00 至 12:00 UTC，超过 K = 36 个元素即 6 小时，用来触发一次确认漂移）：
#     on   打开性能层（--m5b-enabled true，默认组合），收集评分、漂移事件与检查点体积；
#     off  关闭性能层，其余完全相同，只收集评分。
#   然后在 master 上核对三件事：
#     一、在线事件（synergia-drift）与 Java 离线运行器在 on 运行的同一条评分流上算出的事件逐条相同；
#     二、on 与 off 两次运行的评分（上下文通道全部评分窗与点通道离群记录）逐位相同，证明性能层没有改动上游；
#     三、在线事件里至少有一次确认漂移。
# Two step-C-sized replays with a 12-hour step on device E, with and without M5b; checks online/offline event equality,
# bit-identical upstream scores, and at least one confirmed drift. Runs unattended on master.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录；ssh 别名 fa-master、fa-worker1、fa-worker2 可用。编排在 master 的 tmux
#    会话 syn-m5b-verify 里运行，start 之后本地 Mac 可以关机。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m5b-verify.sh start     # 立即返回；两次运行合计约 2 至 3 小时
#      bash deploy/scripts/syn-m5b-verify.sh status    # 随时查看进度
#      bash deploy/scripts/syn-m5b-verify.sh fetch     # 跑完后把 docs/m5b/verify 拉回本地
#      bash deploy/scripts/syn-m5b-verify.sh stop      # 中止：停编排、停重放、取消本项目作业
# 3. 前置条件 / Preconditions: 已执行过 syn-m3-a4-stepc.sh setup-master；含 M5b 的新 jar 已构建核对并上传（check-jar.sh 全部 PASS）；
#    集群上没有任何作业在运行。编排会在 master 上的 .env 副本里补上 synergia-drift:1，并在每次复位后创建缺少的 topic。
# 4. 期望产出 / Expected output: 进度最后一行「全部完成」；fetch 后本地 docs/m5b/verify/ 下有 verify_report.md（三项核对与总判定）、
#    on/ 与 off/ 两个运行目录（评分、日志、检查点体积）、online_events.csv 与 offline_events.csv。
# 5. 失败兜底 / Failure fallback: 某一步失败时编排取消作业、停重放，在 docs/m5b/verify/progress.txt 写明失败的步骤后停止；先 fetch，
#    再把 progress.txt 发给我。退出码：2 参数错误或 setup-master 未完成；3 已有编排在运行。
# ============================================================================
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a
cd "$REPO_DIR" || exit 2

DIR=docs/m5b/verify
PROGRESS=$DIR/progress.txt
LOG=$DIR/verify.log
PIDF=$DIR/verify.pid
TSESSION="syn-m5b-verify"
RH="${REMOTE_HOME:-/opt/fa-iforest}"
RREPO="$RH/m5bverify/repo"
ON_MASTER="${SYN_STEPC_ON_MASTER:-0}"
SPEED=120
SPEC=docs/m5b/verify_inject_spec.txt
CMD="${1:-}"; shift || true

prog() { echo "[$(date '+%F %T')] $*" | tee -a "$PROGRESS"; }
alive() { [ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null; }
remote_alive() { ssh fa-master "tmux has-session -t $TSESSION 2>/dev/null"; }

case "$CMD" in
    start)
        if [ "$ON_MASTER" = 1 ]; then
            alive && { echo "ERROR: 已有编排在运行（PID $(cat "$PIDF")）。" >&2; exit 3; }
            mkdir -p "$DIR"
            nohup bash "$SCRIPT_DIR/$(basename "$0")" run >> "$LOG" 2>&1 &
            echo $! > "$PIDF"
            exit 0
        fi
        ssh fa-master "test -f $RH/stepc.ready" \
            || { echo "ERROR: master 还不能登录三台机器，先执行：bash deploy/scripts/syn-m3-a4-stepc.sh setup-master" >&2; exit 2; }
        remote_alive && { echo "ERROR: master 上已有编排在运行。先 status 查看，或 stop 中止。" >&2; exit 3; }
        # 复制 deploy/、m5b/ 与注入规格；.env 副本让 REST 走内网，并补上漂移主题 / copy and patch the .env copy
        ssh fa-master "mkdir -p $RREPO && rm -rf $RREPO/deploy $RREPO/docs $RREPO/m5b"
        COPYFILE_DISABLE=1 tar czf - deploy m5b "$SPEC" \
            | ssh fa-master "tar --warning=no-unknown-keyword -xzf - -C $RREPO && cd $RREPO && \
                echo 'NODE_MASTER_PUBLIC_IP=$NODE_MASTER_IP' >> deploy/.env && \
                (grep -q 'synergia-drift' deploy/.env || echo 'SYN_EXTRA_TOPICS=\"${SYN_EXTRA_TOPICS:-synergia-scores:4,synergia-monitoring:1,synergia-m1-out:1},synergia-drift:1\"' >> deploy/.env)"
        ssh fa-master "cd $RREPO && tmux new-session -d -s $TSESSION 'SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m5b-verify.sh start; sleep 5; while [ -f $DIR/verify.pid ] && kill -0 \$(cat $DIR/verify.pid) 2>/dev/null; do sleep 30; done'"
        sleep 5
        remote_alive && echo "[m5b] 已在 master 的 tmux 会话 $TSESSION 中启动（两次短重放，合计约 2 至 3 小时）。本地 Mac 现在可以关机。" \
            || { echo "ERROR: master 上的会话没有起来，请把 ssh fa-master 'tail -n 30 $RREPO/$LOG' 的输出发给我。" >&2; exit 1; }
        echo "      查看进度：bash $0 status    跑完后取回：bash $0 fetch"
        ;;
    status)
        echo "== master 上的编排：$(remote_alive && echo 运行中 || echo 未运行) =="
        ssh fa-master "tail -n 15 $RREPO/$PROGRESS 2>/dev/null || echo '（还没有进度记录）'"
        echo
        bash "$SCRIPT_DIR/syn-m3-watch.sh" status 2>/dev/null | head -n 20
        ;;
    fetch)
        remote_alive && echo "[m5b] 注意：master 上的编排仍在运行，现在取回的是部分结果。"
        ssh fa-master "cd $RREPO && tar czf - $DIR" | tar xzf - -C "$REPO_DIR" \
            && echo "[m5b] 已取回到本地 $DIR/：" && ls "$DIR" | sed 's/^/      /'
        ;;
    stop)
        if [ "$ON_MASTER" = 1 ]; then
            if alive; then pkill -P "$(cat "$PIDF")" 2>/dev/null; kill "$(cat "$PIDF")" 2>/dev/null; fi
            rm -f "$PIDF"
            bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
            for r in on off; do
                if [ -f "$DIR/$r/run.env" ]; then
                    source "$DIR/$r/run.env"
                    ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1 && echo "[m5b] 已取消作业 $JID"
                fi
            done
            bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m5bwatch_aborted >/dev/null 2>&1
            prog "已手动中止（stop）。重放已停止，本项目作业已取消，采样会话已结束。"
        else
            ssh fa-master "cd $RREPO 2>/dev/null && SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m5b-verify.sh stop; tmux kill-session -t $TSESSION 2>/dev/null; true"
            echo "[m5b] master 上的编排已中止。"
        fi
        ;;
    run)
        ;;
    *)
        echo "用法 / usage: $0 start | status | fetch | stop" >&2
        exit 2 ;;
esac
[ "$CMD" = run ] || exit 0

# ============================== 后台编排（在 master 上）/ background orchestration on master ==============================
JID=""
JAR="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
EXTRA_BASE="--m3-enabled true --warmup-rounds 8640 --m3-rounds-per-day 864 --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 7 --m3-window-length 60 --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0 --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true --checkpoint-ms 30000 --checkpoint-timeout-min 10 --checkpoint-tolerable-failures 3 --idle-wall 60 --train-threads 2"
M5B="--m5b-confidence 0.99 --m5b-robustness 0.5 --m5b-granularity 10min --m5b-k 36"

fail() {
    prog "失败：$*。已停止；请先 fetch，再把 progress.txt 与相关日志发给我。"
    [ -n "$JID" ] && ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir "$DIR/aborted" >/dev/null 2>&1
    rm -f "$PIDF"
    exit 1
}

drained_devices() {
    for pair in "fa-worker1 taskmanager-2" "fa-worker2 taskmanager-3"; do
        set -- $pair
        ssh "$1" "docker logs --since $SINCE $2 2>&1 | grep -E 'buffer drained|; 0 buffered rounds to drain'" 2>/dev/null
    done | sed -n 's/.*\[M3\] Device \([^ ]*\) .*/\1/p' | sort -u | wc -l | tr -d ' '
}

run_one() {
    RUN="$1"; local m5b_args="$2"; local R="$DIR/$RUN"; JID=""
    rm -rf "$R"; mkdir -p "$R"
    prog "== 运行 $RUN 开始：倍速 $SPEED，性能层 $( [ -n "$m5b_args" ] && echo 打开 || echo 关闭 ) =="
    bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$R/reset.log" 2>&1 || fail "复位未通过（见 $R/reset.log）"
    bash "$SCRIPT_DIR/syn-create-topics.sh" > "$R/topics.log" 2>&1 || fail "创建 topic 失败（见 $R/topics.log）"
    SINCE=$(ssh fa-master date -u +%Y-%m-%dT%H:%M:%SZ)
    bash "$SCRIPT_DIR/syn-submit-m2.sh" --extra "$EXTRA_BASE $m5b_args" > "$R/submit.log" 2>&1 || fail "提交失败（见 $R/submit.log）"
    JID=$(grep -oE 'JobID [a-f0-9]{32}' "$R/submit.log" | awk '{print $2}' | head -1)
    [ -n "$JID" ] || fail "提交输出里没有 JobID"
    printf 'RUN=%s\nSINCE=%s\nJID=%s\n' "$RUN" "$SINCE" "$JID" > "$R/run.env"
    prog "作业已提交：$JID；$(grep -E 'M5b enabled' "$R/submit.log" | tr -s ' ')"
    if [ -n "$m5b_args" ]; then
        grep -q 'M5b enabled:     true' "$R/submit.log" || fail "启动横幅里性能层不是打开状态（jar 可能是旧的，见 $R/submit.log）"
    fi
    # 两次运行都采样，检查点体积的差即性能层的检查点增量（V-M5b-5）/ sample both runs for the checkpoint increment
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m5bwatch_leftover >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-m3-watch.sh" start --jid "$JID" --since "$SINCE" >> "$R/watch.log" 2>&1 || fail "采样没有启动（见 $R/watch.log）"
    bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-replay.sh" --speedup "$SPEED" --start 2022-03-01 --end 2022-03-05 --inject-file "$SPEC" \
        > "$R/replay_start.log" 2>&1 || fail "重放没有启动（见 $R/replay_start.log）"
    for _ in $(seq 1 180); do
        ssh fa-master "tmux has-session -t syn-replay 2>/dev/null" || break
        sleep 60
    done
    ssh fa-master "tmux has-session -t syn-replay 2>/dev/null" && fail "重放 3 小时仍未结束"
    for _ in $(seq 1 60); do
        [ "$(drained_devices)" -ge 8 ] && break
        sleep 60
    done
    N=$(drained_devices)
    [ "$N" -ge 8 ] || prog "警告：60 分钟后只有 $N 台设备排空，仍继续收集"
    for _ in $(seq 1 10); do
        bash "$SCRIPT_DIR/syn-m2-metrics.sh" > /tmp/m5bv_m1.txt 2>&1; sleep 120
        bash "$SCRIPT_DIR/syn-m2-metrics.sh" > /tmp/m5bv_m2.txt 2>&1
        diff -q /tmp/m5bv_m1.txt /tmp/m5bv_m2.txt >/dev/null && break
    done
    prog "重放结束并排空（$N 台设备完成排空）"
    bash "$SCRIPT_DIR/syn-m3-march-collect.sh" --out-dir "$R" --since "$SINCE" > "$R/collect.log" 2>&1 || fail "收集失败（见 $R/collect.log）"
    if [ -n "$m5b_args" ]; then
        ssh fa-master "docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server $BROKERS --topic synergia-drift \
            --from-beginning --timeout-ms 60000 2>/dev/null" > "$R/drift.jsonl" || true
        prog "漂移事件转储：$(wc -l < "$R/drift.jsonl" | tr -d ' ') 条"
    fi
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir "$R" >/dev/null 2>&1
    ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1
    ssh fa-master "rm -rf $RH/m3march"
    prog "== 运行 $RUN 完成：$(grep -E '训练完成的设备数' "$R/collect_summary.txt" 2>/dev/null) =="
}

prog "== 编排开始：两次短重放（打开与关闭性能层），设备 E 注入 12 小时温度阶跃 =="
run_one on "--m5b-enabled true $M5B"
run_one off "--m5b-enabled false"
bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$DIR/reset_final.log" 2>&1

# ---- 核对 / checks ----
python3 m5b/prepare_streams.py --scores "$DIR/on/m3_scores.jsonl" --inject "$DIR/on/inject-truth.csv" --no-exclude \
    --out-dir "$DIR/streams" > "$DIR/streams.log" 2>&1 || fail "整理元素流失败（见 $DIR/streams.log）"
ssh fa-master "docker exec jobmanager mkdir -p /tmp/m5bv && docker cp $RREPO/$DIR/streams/streams_10min_with_injection.csv jobmanager:/tmp/m5bv/streams.csv && \
    docker exec jobmanager java -cp /opt/flink/usrlib/$JAR com.leejean.m5.OptWinOfflineRunner \
      --streams /tmp/m5bv/streams.csv --time-col window_end --value-col main_score \
      --confidence 0.99 --robustness 0.5 --wmax 2016 --k 36 --out /tmp/m5bv/offline.csv && \
    docker cp jobmanager:/tmp/m5bv/offline.csv $RREPO/$DIR/offline_events.csv" > "$DIR/offline.log" 2>&1 \
    || fail "Java 离线运行器失败（见 $DIR/offline.log）"
python3 m5b/drift_json_to_csv.py "$DIR/on/drift.jsonl" "$DIR/online_events.csv" > "$DIR/online.log" 2>&1
python3 m5b/compare_events.py "$DIR/online_events.csv" "$DIR/offline_events.csv" > "$DIR/compare_events.txt" 2>&1
C1=$?
python3 deploy/scripts/m3_a4_equality.py --run "$DIR/on" --ref "$DIR/off" --ref-label "关闭性能层的运行" \
    --out "$DIR/equality_scores.md" > /dev/null 2>&1
C2=$?
rm -f "$DIR/on/scores.jsonl" "$DIR/off/scores.jsonl"      # 体积大，核对后删除 / large; removed after the check
NCONF=$(awk -F, '$3 == "CONFIRM"' "$DIR/online_events.csv" 2>/dev/null | wc -l | tr -d ' ')
{
    echo "# V-M5b-3 离线与在线等值（短重放）"
    echo
    echo "两次步骤 C 规模的短重放（2022-03-01 至 03-05，120 倍速），设备 E 注入 \`$(cat "$SPEC")\`。打开性能层的运行作业 \`$(grep JID "$DIR/on/run.env" | cut -d= -f2)\`，关闭的运行作业 \`$(grep JID "$DIR/off/run.env" | cut -d= -f2)\`。"
    echo
    echo "| 核对项 | 结果 |"
    echo "| --- | --- |"
    echo "| 一、在线事件与 Java 离线运行器逐条相同 | $( [ $C1 -eq 0 ] && echo 通过 || echo '**未通过**' )（$(tr '\n' ' ' < "$DIR/compare_events.txt")） |"
    echo "| 二、打开与关闭性能层的两次运行评分逐位相同 | $( [ $C2 -eq 0 ] && echo 通过 || echo '**未通过**' )（见 \`equality_scores.md\`） |"
    echo "| 三、在线事件中至少有一次确认漂移 | $( [ "$NCONF" -ge 1 ] && echo 通过 || echo '**未通过**' )（确认 $NCONF 条；$(cat "$DIR/online.log")） |"
    echo
    if [ $C1 -eq 0 ] && [ $C2 -eq 0 ] && [ "$NCONF" -ge 1 ]; then echo "**三项全部通过。**"; else echo "**有未通过的项，见上表；按「不迎合」约定如实上报。**"; fi
} > "$DIR/verify_report.md"
rm -f "$PIDF"
prog "核对：$(tail -n 1 "$DIR/verify_report.md")（详见 $DIR/verify_report.md）"
prog "全部完成。请在 Mac 上执行：bash deploy/scripts/syn-m5b-verify.sh fetch，然后 git add -f $DIR && git commit -m \"data(m5b): V-M5b-3 短重放离线与在线等值结果\" && git push origin dev-claude"
