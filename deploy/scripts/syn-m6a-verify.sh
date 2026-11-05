#!/usr/bin/env bash
# ============================================================================
# syn-m6a-verify.sh
# 适应执行第一批验收项三（V-M6a-3）与验收项四（V-M6a-4）的短重放编排：区间默认 2022-03-24 至 03-31（与聚合模块共用），命令回路打开、
# 配置相同的两次在线运行（a、b），每次在作业提交之后、重放开始之前用手工命令工具对 G 下发一条生效时刻为 03-28 当地日界的零级命令
# （开工核对裁决第七项）。跑完之后在 master 上用同一份轮表做离线模拟（M6aSimulator、M6aOfflineRunner），由 m6a/compare_online.py
# 核对七项：新参数、新参照直方图、标准化轮、点通道判定、数据层日记录与离线逐位或逐条相同；零级各执行一次、迟到命令为零；两次在线运行
# 逐位相同。全部步骤在 master 的 tmux 会话里无人值守地执行，提交之后本地 Mac 可以关机。
# Short replays for V-M6a-3/4: two identical runs with the command loop on and a manual level-0 command for G at 03-28.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录；ssh 别名 fa-master、fa-worker1、fa-worker2 可用。编排在 master 的 tmux
#    会话 syn-m6a-verify 里运行（master 上的仓库副本在 /opt/fa-iforest/m6averify/repo）。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m6a-verify.sh start          # 两次短重放加离线核对，合计约 4 至 5 小时
#      bash deploy/scripts/syn-m6a-verify.sh status         # 任何终端随时查看进度
#      bash deploy/scripts/syn-m6a-verify.sh fetch          # 跑完后取回到本地 docs/m6a/verify/
#      bash deploy/scripts/syn-m6a-verify.sh stop           # 中止：停重放、取消本项目作业、结束采样
#    可选 --runs a,b|a|b（默认 a,b）、--start/--end 2022-MM-DD。
# 3. 前置条件 / Preconditions: 已按 syn-m3-a4-stepc.sh setup-master 做过 master 的准备；新 jar 已上传（syn-upload-m1.sh --jar-only），
#    check-jar.sh 通过（含 adapt-enabled、Level 0 executed、m2_state_version_clears 三项）；轮表 docs/m6a/extract/rounds_G.csv.gz 在库。
# 4. 期望产出 / Expected output: docs/m6a/verify/ 下 progress.txt、verify_report.md（七项核对）、a/ 与 b/ 两个运行目录（m1-out 中 G 的
#    轮、评分、漂移事件、日记录、命令主题、[M6a] 日志、检查点体积）、offline/（离线模拟结果）。
# 5. 失败兜底 / Failure fallback: 任何一步失败都会停止重放、取消本项目作业，并在 progress.txt 写明原因；作业失败时 JobManager 记录的原因
#    在 a/jm_failure.log 或 b/jm_failure.log。退出码：2 参数错误或 setup-master 未完成；3 已有编排在运行。
# ============================================================================
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a
cd "$REPO_DIR" || exit 2

DIR=docs/m6a/verify
PROGRESS=$DIR/progress.txt
LOG=$DIR/verify.log
PIDF=$DIR/verify.pid
TSESSION="syn-m6a-verify"
RH="${REMOTE_HOME:-/opt/fa-iforest}"
RREPO="$RH/m6averify/repo"
WORKM="$RH/m6averify/work"
ON_MASTER="${SYN_STEPC_ON_MASTER:-0}"
SPEED=120
SPEC=docs/m5a/verify_inject_spec.txt
TOPICS_M7="synergia-scores:4,synergia-monitoring:1,synergia-m1-out:1,synergia-drift:1,synergia-adapt:1"
CMD="${1:-}"; shift || true
RUNS=a,b
START=2022-03-24
END=2022-03-31
while [ $# -gt 0 ]; do
    case "$1" in
        --runs) RUNS="$2"; shift 2 ;;
        --start) START="$2"; shift 2 ;;
        --end) END="$2"; shift 2 ;;
        *) echo "ERROR: 未知参数 $1" >&2; exit 2 ;;
    esac
done
[[ "$RUNS" =~ ^(a,b|a|b)$ ]] || { echo "ERROR: --runs 只能是 a,b、a 或 b" >&2; exit 2; }
[[ "$START" =~ ^2022-[0-9]{2}-[0-9]{2}$ && "$END" =~ ^2022-[0-9]{2}-[0-9]{2}$ ]] || { echo "ERROR: --start、--end 须为 2022-MM-DD" >&2; exit 2; }

prog() { echo "[$(date '+%F %T')] $*" | tee -a "$PROGRESS"; }
alive() { [ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null; }
remote_alive() { ssh fa-master "tmux has-session -t $TSESSION 2>/dev/null"; }

case "$CMD" in
    start)
        if [ "$ON_MASTER" = 1 ]; then
            alive && { echo "ERROR: 已有编排在运行（PID $(cat "$PIDF")）。" >&2; exit 3; }
            mkdir -p "$DIR"
            nohup bash "$SCRIPT_DIR/$(basename "$0")" run --runs "$RUNS" --start "$START" --end "$END" >> "$LOG" 2>&1 &
            echo $! > "$PIDF"
            exit 0
        fi
        echo "[m6a] 1/4 检查 master 的准备状态……"
        ssh fa-master "test -f $RH/stepc.ready" \
            || { echo "ERROR: master 还不能登录三台机器，先执行：bash deploy/scripts/syn-m3-a4-stepc.sh setup-master" >&2; exit 2; }
        remote_alive && { echo "ERROR: master 上已有编排在运行。先 status 查看，或 stop 中止。" >&2; exit 3; }
        # 只复制受版本管理的 deploy/、m5a/、m5b/、m7/ 文件，加上 .env 与注入规格 / tracked files only, plus .env and the spec
        FILES=$(git ls-files deploy m5a m5b m6a m7; echo deploy/.env; echo "$SPEC"; echo docs/m6a/extract/rounds_G.csv.gz)
        echo "[m6a] 2/4 复制 $(echo "$FILES" | wc -l | tr -d ' ') 个文件到 master 的 $RREPO ……"
        ssh fa-master "mkdir -p $RREPO && rm -rf $RREPO/deploy $RREPO/m5a $RREPO/m5b $RREPO/m6a $RREPO/m7"   # docs/ 保留，里面可能有上一次运行的结果
        # shellcheck disable=SC2086
        COPYFILE_DISABLE=1 tar czf - $FILES \
            | ssh fa-master "tar --warning=no-unknown-keyword -xzf - -C $RREPO && cd $RREPO && \
                echo 'NODE_MASTER_PUBLIC_IP=$NODE_MASTER_IP' >> deploy/.env && \
                echo 'SYN_EXTRA_TOPICS=\"$TOPICS_M7\"' >> deploy/.env" \
            || { echo "ERROR: 复制到 master 失败（见上方 tar 或 ssh 的报错）。" >&2; exit 1; }
        ssh fa-master "test -f $RREPO/deploy/scripts/syn-m6a-verify.sh && test -f $RREPO/$SPEC && test -f $RREPO/m6a/compare_online.py && test -f $RREPO/docs/m6a/extract/rounds_G.csv.gz" \
            || { echo "ERROR: master 上的仓库副本不完整，请把 ssh fa-master 'find $RREPO -maxdepth 3 | head -50' 的输出发给我。" >&2; exit 1; }
        # 进度文件是追加的，只看本次启动之后新写的行 / progress is appended; look only at lines written by this start
        N0=$(ssh fa-master "cat $RREPO/$PROGRESS 2>/dev/null | wc -l" | tr -d ' ')
        NEW="tail -n +$((N0 + 1)) $RREPO/$PROGRESS 2>/dev/null"
        echo "[m6a] 3/4 在 master 上启动 tmux 会话 $TSESSION ……"
        ssh fa-master "cd $RREPO && tmux new-session -d -s $TSESSION 'SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m6a-verify.sh start --runs $RUNS --start $START --end $END; sleep 5; while [ -f $DIR/verify.pid ] && kill -0 \$(cat $DIR/verify.pid) 2>/dev/null; do sleep 30; done'" \
            || { echo "ERROR: tmux 会话创建失败（见上方报错）。" >&2; exit 1; }
        echo "[m6a] 4/4 等待第一行进度……"
        for _ in $(seq 1 12); do
            [ -n "$(ssh fa-master "$NEW")" ] && break
            sleep 5
        done
        if [ -n "$(ssh fa-master "$NEW")" ]; then
            ssh fa-master "$NEW | tail -n 3"
            if ssh fa-master "$NEW | grep -q '失败'"; then
                echo "ERROR: 编排已经失败（见上方进度）。请执行 bash $0 fetch，把 $DIR/ 下的 progress.txt 与相应日志发给我。" >&2
                exit 1
            fi
            echo "[m6a] 已在 master 的 tmux 会话 $TSESSION 中启动（两次短重放加离线核对，合计约 4 至 5 小时）。本地 Mac 现在可以关机。"
        else
            echo "ERROR: 60 秒内没有进度记录。请把 ssh fa-master 'tail -n 30 $RREPO/$LOG; tmux ls' 的输出发给我。" >&2
            exit 1
        fi
        echo "      查看进度：bash $0 status    跑完后取回：bash $0 fetch"
        ;;
    status)
        echo "== master 上的编排：$(remote_alive && echo 运行中 || echo 未运行) =="
        ssh fa-master "tail -n 15 $RREPO/$PROGRESS 2>/dev/null || echo '（还没有进度记录）'"
        if ! remote_alive; then
            echo "== 诊断（未运行时）=="
            ssh fa-master "ls -ld $RREPO $RREPO/$DIR 2>&1; echo '-- verify.log 末尾 --'; tail -n 20 $RREPO/$LOG 2>&1; \
                echo '-- tmux 会话 --'; tmux ls 2>&1; echo '-- stepc.ready --'; ls -l $RH/stepc.ready 2>&1"
        fi
        echo
        bash "$SCRIPT_DIR/syn-m3-watch.sh" status 2>/dev/null | head -n 20
        ;;
    fetch)
        remote_alive && echo "[m6a] 注意：master 上的编排仍在运行，现在取回的是部分结果。"
        ssh fa-master "cd $RREPO && tar czf - $DIR" | tar xzf - -C "$REPO_DIR" \
            && echo "[m6a] 已取回到本地 $DIR/：" && ls "$DIR" | sed 's/^/      /'
        ;;
    stop)
        if [ "$ON_MASTER" = 1 ]; then
            if alive; then pkill -P "$(cat "$PIDF")" 2>/dev/null; kill "$(cat "$PIDF")" 2>/dev/null; fi
            rm -f "$PIDF"
            bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
            for r in a b; do
                if [ -f "$DIR/$r/run.env" ]; then
                    source "$DIR/$r/run.env"
                    ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1 && echo "[m6a] 已取消作业 $JID"
                fi
            done
            bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m6awatch_aborted >/dev/null 2>&1
            prog "已手动中止（stop）。重放已停止，本项目作业已取消，采样会话已结束。"
        else
            ssh fa-master "cd $RREPO 2>/dev/null && SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m6a-verify.sh stop; tmux kill-session -t $TSESSION 2>/dev/null; true"
            echo "[m6a] master 上的编排已中止。"
        fi
        ;;
    run)
        ;;
    *)
        echo "用法 / usage: $0 start [--runs a,b|a|b] [--start 2022-MM-DD --end 2022-MM-DD] | status | fetch | stop" >&2
        exit 2 ;;
esac
[ "$CMD" = run ] || exit 0

# ============================== 后台编排（在 master 上）/ background orchestration on master ==============================
JID=""
JAR="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
EXTRA_BASE="--m3-enabled true --warmup-rounds 8640 --m3-rounds-per-day 864 --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 7 --m3-window-length 60 --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0 --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true --checkpoint-ms 30000 --checkpoint-timeout-min 10 --checkpoint-tolerable-failures 3 --idle-wall 60 --train-threads 2"
# 命令回路打开、数据层打开、重启次数为零（任何重启都会让两次运行的逐位比较失效）；性能层与聚合算子不需要 / loop on, no restarts
EXTRA_BASE="$EXTRA_BASE --m5a-enabled true --adapt-enabled true --restart-attempts 0"
EFF_DAY=2022-03-28

fail() {
    prog "失败：$*。已停止；请先 fetch，再把 progress.txt 与相关日志发给我。"
    # 作业的失败原因（JobManager 日志里第一次转入失败的前后）/ the job's failure cause from the JobManager log
    [ -n "${SINCE:-}" ] && [ -n "${RUN:-}" ] && ssh fa-master "docker logs --since $SINCE jobmanager 2>&1 \
        | grep -m1 -A60 -E 'switched from state RUNNING to (FAILING|RESTARTING|FAILED)'" > "$DIR/$RUN/jm_failure.log" 2>/dev/null
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

# 转储一个 topic 到 master 上的文件；条数上限取各分区末端偏移之和（读满即退出）/ dump a topic, capped at the end-offset sum
dump_topic() {   # $1 = topic, $2 = 输出文件 / output file
    local cap
    cap="$(ssh fa-master "docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list $BROKERS --topic $1 --time -1" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
    [ "${cap:-0}" -gt 0 ] || { : > "$2"; return 0; }
    ssh fa-master "docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server $BROKERS --topic $1 \
        --from-beginning --max-messages $cap --timeout-ms 60000 2>/dev/null" > "$2" || true
}

# 作业是否仍在运行：作业以 --restart-attempts 0 提交，一失败就停止 / the job runs with no restarts
job_running() {   # 查询偶尔失败时重试两次，避免误判 / retry twice so a transient query failure is not mistaken
    for _ in 1 2 3; do
        ssh fa-master "docker exec jobmanager flink list -r 2>/dev/null" | grep -q "$JID" && return 0
        sleep 10
    done
    return 1
}

run_one() {
    RUN="$1"; local R="$DIR/$RUN"; local W="$WORKM/$RUN"; JID=""
    rm -rf "$R" "$W"; mkdir -p "$R" "$W"
    prog "== 运行 $RUN 开始：$START 至 ${END}，倍速 ${SPEED}，命令回路打开，G 的手工零级命令生效于 $EFF_DAY 当地零点 =="
    bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$R/reset.log" 2>&1 || fail "复位未通过（见 $R/reset.log）"
    bash "$SCRIPT_DIR/syn-create-topics.sh" > "$R/topics.log" 2>&1 || fail "创建 topic 失败（见 $R/topics.log）"
    grep -q 'synergia-adapt' "$R/topics.log" || fail "topic 清单里没有 synergia-adapt（见 $R/topics.log）"
    SINCE=$(ssh fa-master date -u +%Y-%m-%dT%H:%M:%SZ)
    bash "$SCRIPT_DIR/syn-submit-m2.sh" --extra "$EXTRA_BASE" > "$R/submit.log" 2>&1 || fail "提交失败（见 $R/submit.log）"
    JID=$(grep -oE 'JobID [a-f0-9]{32}' "$R/submit.log" | awk '{print $2}' | head -1)
    [ -n "$JID" ] || fail "提交输出里没有 JobID"
    printf 'RUN=%s\nSINCE=%s\nJID=%s\n' "$RUN" "$SINCE" "$JID" > "$R/run.env"
    prog "作业已提交：${JID}；$(grep -E 'Adapt \(M6a\)' "$R/submit.log" | tr -s ' ')"
    grep -q 'Restart attempts: 0' "$R/submit.log" || fail "启动横幅里重启次数不是 0（jar 可能是旧的，见 $R/submit.log）"
    grep -q 'M5a enabled:     true' "$R/submit.log" || fail "启动横幅里数据层不是打开状态（见 $R/submit.log）"
    grep -q 'Adapt (M6a):     true' "$R/submit.log" || fail "启动横幅里命令回路不是打开状态（jar 可能是旧的，见 $R/submit.log）"
    # 手工命令：在重放开始之前写入，命令在状态里等到生效时刻 / the manual command, written before the replay starts
    SYN_ON_MASTER=1 bash "$SCRIPT_DIR/syn-adapt-send.sh" --device G --day "$EFF_DAY" --seq 1 > "$R/command.log" 2>&1 \
        || fail "手工命令没有写入（见 $R/command.log）"
    prog "手工命令已写入：$(head -1 "$R/command.log")"
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m6awatch_leftover >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-m3-watch.sh" start --jid "$JID" --since "$SINCE" >> "$R/watch.log" 2>&1 || fail "采样没有启动（见 $R/watch.log）"
    bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-replay.sh" --speedup "$SPEED" --start "$START" --end "$END" --inject-file "$SPEC" \
        > "$R/replay_start.log" 2>&1 || fail "重放没有启动（见 $R/replay_start.log）"
    for _ in $(seq 1 240); do
        ssh fa-master "tmux has-session -t syn-replay 2>/dev/null" || break
        job_running || fail "作业在重放期间停止运行（不自动重启；原因见 $R/jm_failure.log）"
        sleep 60
    done
    ssh fa-master "tmux has-session -t syn-replay 2>/dev/null" && fail "重放 4 小时仍未结束"
    for _ in $(seq 1 60); do
        [ "$(drained_devices)" -ge 8 ] && break
        job_running || fail "作业在排空期间停止运行（原因见 $R/jm_failure.log）"
        sleep 60
    done
    N=$(drained_devices)
    [ "$N" -ge 8 ] || prog "警告：60 分钟后只有 $N 台设备排空，仍继续收集"
    for _ in $(seq 1 10); do
        bash "$SCRIPT_DIR/syn-m2-metrics.sh" > /tmp/m6av_m1.txt 2>&1; sleep 120
        bash "$SCRIPT_DIR/syn-m2-metrics.sh" > /tmp/m6av_m2.txt 2>&1
        diff -q /tmp/m6av_m1.txt /tmp/m6av_m2.txt >/dev/null && break
    done
    prog "重放结束并排空（$N 台设备完成排空）"
    bash "$SCRIPT_DIR/syn-m3-march-collect.sh" --out-dir "$R" --since "$SINCE" > "$R/collect.log" 2>&1 || fail "收集失败（见 $R/collect.log）"
    dump_topic synergia-drift "$R/drift.jsonl"
    dump_topic synergia-adapt "$R/adapt.jsonl"
    dump_topic "${SYN_TOPIC_M1_OUT:-synergia-m1-out}" "$W/m1out.jsonl"
    grep -E '"windowEnd":-3[,}]' "$RH/m3march/monitoring.jsonl" > "$R/m5a_records.jsonl" 2>/dev/null || true
    grep -F '"device":"G"' "$W/m1out.jsonl" > "$R/m1out_G.jsonl"
    echo "$(wc -l < "$W/m1out.jsonl" | tr -d ' ') $(LC_ALL=C sort "$W/m1out.jsonl" | sha256sum | cut -c1-64)" > "$R/m1out.sha"
    : > "$R/m6a_tm_log.txt"
    for pair in "fa-worker1 taskmanager-2" "fa-worker2 taskmanager-3"; do
        set -- $pair
        ssh "$1" "docker logs --since $SINCE $2 2>&1 | grep -F '[M6a]'" >> "$R/m6a_tm_log.txt" 2>/dev/null || true
    done
    prog "收集：m1-out $(cut -d' ' -f1 "$R/m1out.sha") 行（G $(wc -l < "$R/m1out_G.jsonl" | tr -d ' ') 行），漂移事件 $(wc -l < "$R/drift.jsonl" | tr -d ' ') 条，日记录 $(wc -l < "$R/m5a_records.jsonl" | tr -d ' ') 条，命令主题 $(wc -l < "$R/adapt.jsonl" | tr -d ' ') 条，[M6a] 日志 $(wc -l < "$R/m6a_tm_log.txt" | tr -d ' ') 行"
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir "$R" >/dev/null 2>&1
    ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1
    ssh fa-master "rm -rf $RH/m3march"
    rm -f "$W/m1out.jsonl"
    prog "== 运行 $RUN 完成 =="
}

prog "== 编排开始：运行 ${RUNS}（命令回路打开、上下文通道与数据层打开），$START 至 ${END}，设备 E 注入 12 小时温度阶跃 =="
[[ ",$RUNS," == *",a,"* ]] && run_one a
[ -s "$DIR/a/m1out_G.jsonl" ] && [ -s "$DIR/a/scores.jsonl" ] || fail "没有运行 a 的结果（缺 $DIR/a/ 下的 m1out_G.jsonl 或 scores.jsonl），请用 start 跑全部两次"
[[ ",$RUNS," == *",b,"* ]] && run_one b
bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$DIR/reset_final.log" 2>&1

# ---- 离线模拟：同一份轮表，从短重放的第一轮到最后一轮 / offline simulation over the same rounds ----
START_TS=$(python3 -c "import json,sys; print(min(json.loads(l)['ts'] for l in open(sys.argv[1])))" "$DIR/a/m1out_G.jsonl")
END_TS=$(python3 -c "import json,sys; print(max(json.loads(l)['ts'] for l in open(sys.argv[1])))" "$DIR/a/m1out_G.jsonl")
ssh fa-master "docker exec jobmanager rm -rf /tmp/m6av && docker exec jobmanager mkdir -p /tmp/m6av && \
    docker cp $RREPO/docs/m6a/extract/rounds_G.csv.gz jobmanager:/tmp/m6av/rounds_G.csv.gz && \
    docker exec jobmanager java -Xmx2g -cp /opt/flink/usrlib/$JAR com.leejean.m6.M6aSimulator --rounds /tmp/m6av/rounds_G.csv.gz \
      --device G --r 1.5 --warmup-rounds 8640 --start $START_TS --end $END_TS --day $EFF_DAY --out-dir /tmp/m6av --tag G_short \
      --dump-rounds /tmp/m6av/rounds.csv && \
    docker exec jobmanager java -Xmx1g -cp /opt/flink/usrlib/$JAR com.leejean.m6.M6aOfflineRunner --rounds /tmp/m6av/rounds_G.csv.gz \
      --device G --warmup-rounds 8640 --start $START_TS --day $EFF_DAY --out /tmp/m6av/level0_java.json && \
    rm -rf $RREPO/$DIR/offline && docker cp jobmanager:/tmp/m6av $RREPO/$DIR/offline && docker exec jobmanager rm -rf /tmp/m6av" \
    > "$DIR/offline.log" 2>&1 || fail "离线模拟失败（见 $DIR/offline.log）"
mv "$DIR/offline/G_short_days.csv" "$DIR/offline/days.csv"
mv "$DIR/offline/G_short_outliers.txt" "$DIR/offline/outliers.txt"
rm -f "$DIR/offline/rounds_G.csv.gz"
python3 m6a/compare_online.py --dir "$DIR" > "$DIR/compare.txt" 2>&1
RC=$?
prog "核对：$(grep -c '^\[通过\]' "$DIR/compare.txt") 项通过，$(grep -c '^\[失败\]' "$DIR/compare.txt") 项失败（见 $DIR/verify_report.md）"
rm -f "$PIDF"
prog "== 编排结束：$( [ $RC -eq 0 ] && echo 七项核对全部通过 || echo 有核对项未通过 )。请在本地执行 bash deploy/scripts/syn-m6a-verify.sh fetch 取回 =="
