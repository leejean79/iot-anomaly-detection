#!/usr/bin/env bash
# ============================================================================
# syn-m5a-verify.sh
# M5a 验收项三（V-M5a-3，短重放等值）与验收项五（V-M5a-5，性能）的短重放验证，在 master 上无人值守地一键执行（M5a 交接文档第五节）：
#   两次短重放（默认 2022-03-24 至 03-31，覆盖 03-27 夏令时切换日与三月事件；预热 8,640 轮约一天，标定日为 03-25；
#   每天折合 864 轮的 M3 设置与步骤 C 相同；线程数 2，120 倍速），都在设备 E 上注入一段 12 小时的温度阶跃（03-28 00:00 至 12:00 UTC）。
#   两次运行都打开上下文通道与性能层，只有数据层的开关不同：
#     on   打开数据层（--m5a-enabled true，1.05 版冻结的 D 零），收集评分、漂移事件、日记录、m1-out 与检查点体积；
#     off  关闭数据层，其余完全相同。
#   然后在 master 上核对：
#     一、在线数据层的事件与日记录，与 Java 离线运行器在 on 运行的同一份 m1-out 与评分转储上算出的结果逐行相同（截止到最后一条轮
#         之前 6 小时；离线以最后一条轮为最终事件时间，在线的最终水位线停在别处）；截止之前须有日记录与事件；
#     二、on 与 off 两次运行的评分（上下文通道全部评分窗与点通道离群记录）逐位相同；
#     三、on 与 off 两次运行的 m1-out（标准化轮）排序后逐行相同；
#     四、on 与 off 两次运行的性能层事件逐条相同；off 运行的漂移主题里没有数据层事件、监测主题里没有日记录；
#   并汇总验收项五（日界处理的耗时、状态大小、检查点体积与增量）。
# Two short replays (default 2022-03-24..31) with and without the M5a data layer; checks online/offline equality of data-layer
# events and daily records, bit-identical upstream outputs, and summarizes performance. Runs unattended on master.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录；ssh 别名 fa-master、fa-worker1、fa-worker2 可用。编排在 master 的 tmux
#    会话 syn-m5a-verify 里运行，start 之后本地 Mac 可以关机。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m5a-verify.sh start     # 立即返回；两次运行合计约 4 小时
#      bash deploy/scripts/syn-m5a-verify.sh start --runs off   # 只补跑关闭数据层的一次并做核对（打开的一次已有结果时）
#      bash deploy/scripts/syn-m5a-verify.sh start --start 2022-03-01 --end 2022-03-06   # 换重放区间（默认 03-24 至 03-31）
#      bash deploy/scripts/syn-m5a-verify.sh status    # 随时查看进度
#      bash deploy/scripts/syn-m5a-verify.sh fetch     # 跑完后把 docs/m5a/verify 拉回本地
#      bash deploy/scripts/syn-m5a-verify.sh stop      # 中止：停编排、停重放、取消本项目作业
# 3. 前置条件 / Preconditions: 已执行过 syn-m3-a4-stepc.sh setup-master；含 M5a 的新 jar 已构建核对并上传（check-jar.sh 全部 PASS）；
#    集群上没有任何作业在运行。编排会在 master 上的 .env 副本里补上 synergia-drift:1，并在每次复位后创建缺少的 topic。
# 4. 期望产出 / Expected output: 进度最后一行「全部完成」；fetch 后本地 docs/m5a/verify/ 下有 verify_report.md（四项核对、验收项五
#    的表与总判定）、on/ 与 off/ 两个运行目录（评分、漂移事件、日记录、日志、检查点体积）、online_*.csv 与 offline_*.csv。
#    m1-out 转储较大（每次约 400 MB），留在 master 的 ${REMOTE_HOME}/m5averify/work/，核对后删除，只保留行数与校验和。
# 5. 失败兜底 / Failure fallback: 某一步失败时编排取消作业、停重放，在 docs/m5a/verify/progress.txt 写明失败的步骤后停止；先 fetch，
#    再把 progress.txt 发给我。退出码：2 参数错误或 setup-master 未完成；3 已有编排在运行。
# ============================================================================
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a
cd "$REPO_DIR" || exit 2

DIR=docs/m5a/verify
PROGRESS=$DIR/progress.txt
LOG=$DIR/verify.log
PIDF=$DIR/verify.pid
TSESSION="syn-m5a-verify"
RH="${REMOTE_HOME:-/opt/fa-iforest}"
RREPO="$RH/m5averify/repo"
WORKM="$RH/m5averify/work"
ON_MASTER="${SYN_STEPC_ON_MASTER:-0}"
SPEED=120
SPEC=docs/m5a/verify_inject_spec.txt
CMD="${1:-}"; shift || true
RUNS=on,off
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
[[ "$RUNS" =~ ^(on,off|on|off)$ ]] || { echo "ERROR: --runs 只能是 on,off、on 或 off" >&2; exit 2; }
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
        echo "[m5a] 1/4 检查 master 的准备状态……"
        ssh fa-master "test -f $RH/stepc.ready" \
            || { echo "ERROR: master 还不能登录三台机器，先执行：bash deploy/scripts/syn-m3-a4-stepc.sh setup-master" >&2; exit 2; }
        remote_alive && { echo "ERROR: master 上已有编排在运行。先 status 查看，或 stop 中止。" >&2; exit 3; }
        # 只复制受版本管理的 deploy/、m5a/、m5b/ 文件，加上 .env 与注入规格 / tracked files only, plus .env and the spec
        FILES=$(git ls-files deploy m5a m5b; echo deploy/.env; echo "$SPEC")
        echo "[m5a] 2/4 复制 $(echo "$FILES" | wc -l | tr -d ' ') 个文件到 master 的 $RREPO ……"
        ssh fa-master "mkdir -p $RREPO && rm -rf $RREPO/deploy $RREPO/m5a $RREPO/m5b"     # docs/ 保留，里面可能有上一次运行的结果
        # shellcheck disable=SC2086
        COPYFILE_DISABLE=1 tar czf - $FILES \
            | ssh fa-master "tar --warning=no-unknown-keyword -xzf - -C $RREPO && cd $RREPO && \
                echo 'NODE_MASTER_PUBLIC_IP=$NODE_MASTER_IP' >> deploy/.env && \
                (grep -q '^SYN_EXTRA_TOPICS=.*synergia-drift' deploy/.env || echo 'SYN_EXTRA_TOPICS=\"${SYN_EXTRA_TOPICS:-synergia-scores:4,synergia-monitoring:1,synergia-m1-out:1},synergia-drift:1\"' >> deploy/.env)" \
            || { echo "ERROR: 复制到 master 失败（见上方 tar 或 ssh 的报错）。" >&2; exit 1; }
        ssh fa-master "test -f $RREPO/deploy/scripts/syn-m5a-verify.sh && test -f $RREPO/$SPEC && test -f $RREPO/m5a/compare_online.py" \
            || { echo "ERROR: master 上的仓库副本不完整，请把 ssh fa-master 'find $RREPO -maxdepth 3 | head -50' 的输出发给我。" >&2; exit 1; }
        # 进度文件是追加的，只看本次启动之后新写的行 / progress is appended; look only at lines written by this start
        N0=$(ssh fa-master "cat $RREPO/$PROGRESS 2>/dev/null | wc -l" | tr -d ' ')
        NEW="tail -n +$((N0 + 1)) $RREPO/$PROGRESS 2>/dev/null"
        echo "[m5a] 3/4 在 master 上启动 tmux 会话 $TSESSION ……"
        ssh fa-master "cd $RREPO && tmux new-session -d -s $TSESSION 'SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m5a-verify.sh start --runs $RUNS --start $START --end $END; sleep 5; while [ -f $DIR/verify.pid ] && kill -0 \$(cat $DIR/verify.pid) 2>/dev/null; do sleep 30; done'" \
            || { echo "ERROR: tmux 会话创建失败（见上方报错）。" >&2; exit 1; }
        echo "[m5a] 4/4 等待第一行进度……"
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
            echo "[m5a] 已在 master 的 tmux 会话 $TSESSION 中启动（两次短重放，合计约 4 小时）。本地 Mac 现在可以关机。"
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
        remote_alive && echo "[m5a] 注意：master 上的编排仍在运行，现在取回的是部分结果。"
        ssh fa-master "cd $RREPO && tar czf - $DIR" | tar xzf - -C "$REPO_DIR" \
            && echo "[m5a] 已取回到本地 $DIR/：" && ls "$DIR" | sed 's/^/      /'
        ;;
    stop)
        if [ "$ON_MASTER" = 1 ]; then
            if alive; then pkill -P "$(cat "$PIDF")" 2>/dev/null; kill "$(cat "$PIDF")" 2>/dev/null; fi
            rm -f "$PIDF"
            bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
            for r in on off; do
                if [ -f "$DIR/$r/run.env" ]; then
                    source "$DIR/$r/run.env"
                    ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1 && echo "[m5a] 已取消作业 $JID"
                fi
            done
            bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m5awatch_aborted >/dev/null 2>&1
            prog "已手动中止（stop）。重放已停止，本项目作业已取消，采样会话已结束。"
        else
            ssh fa-master "cd $RREPO 2>/dev/null && SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m5a-verify.sh stop; tmux kill-session -t $TSESSION 2>/dev/null; true"
            echo "[m5a] master 上的编排已中止。"
        fi
        ;;
    run)
        ;;
    *)
        echo "用法 / usage: $0 start [--runs on,off|on|off] [--start 2022-MM-DD --end 2022-MM-DD] | status | fetch | stop" >&2
        exit 2 ;;
esac
[ "$CMD" = run ] || exit 0

# ============================== 后台编排（在 master 上）/ background orchestration on master ==============================
JID=""
JAR="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
EXTRA_BASE="--m3-enabled true --warmup-rounds 8640 --m3-rounds-per-day 864 --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 7 --m3-window-length 60 --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0 --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true --checkpoint-ms 30000 --checkpoint-timeout-min 10 --checkpoint-tolerable-failures 3 --idle-wall 60 --train-threads 2"
EXTRA_BASE="$EXTRA_BASE --m5b-enabled true --m5b-confidence 0.99 --m5b-robustness 0.5 --m5b-granularity 10min --m5b-k 36"

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

# 转储一个 topic 到 master 上的文件；条数上限取各分区末端偏移之和（读满即退出）/ dump a topic, capped at the end-offset sum
dump_topic() {   # $1 = topic, $2 = 输出文件 / output file
    local cap
    cap="$(ssh fa-master "docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list $BROKERS --topic $1 --time -1" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
    [ "${cap:-0}" -gt 0 ] || { : > "$2"; return 0; }
    ssh fa-master "docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server $BROKERS --topic $1 \
        --from-beginning --max-messages $cap --timeout-ms 60000 2>/dev/null" > "$2" || true
}

run_one() {
    RUN="$1"; local m5a_args="$2"; local R="$DIR/$RUN"; local W="$WORKM/$RUN"; JID=""
    rm -rf "$R" "$W"; mkdir -p "$R" "$W"
    prog "== 运行 $RUN 开始：$START 至 ${END}，倍速 ${SPEED}，数据层 $( [ "$RUN" = on ] && echo 打开 || echo 关闭 ) =="
    bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$R/reset.log" 2>&1 || fail "复位未通过（见 $R/reset.log）"
    bash "$SCRIPT_DIR/syn-create-topics.sh" > "$R/topics.log" 2>&1 || fail "创建 topic 失败（见 $R/topics.log）"
    SINCE=$(ssh fa-master date -u +%Y-%m-%dT%H:%M:%SZ)
    bash "$SCRIPT_DIR/syn-submit-m2.sh" --extra "$EXTRA_BASE $m5a_args" > "$R/submit.log" 2>&1 || fail "提交失败（见 $R/submit.log）"
    JID=$(grep -oE 'JobID [a-f0-9]{32}' "$R/submit.log" | awk '{print $2}' | head -1)
    [ -n "$JID" ] || fail "提交输出里没有 JobID"
    printf 'RUN=%s\nSINCE=%s\nJID=%s\n' "$RUN" "$SINCE" "$JID" > "$R/run.env"
    prog "作业已提交：${JID}；$(grep -E 'M5a enabled' "$R/submit.log" | tr -s ' ')"
    # 启动横幅须与本次运行的开关一致；两次运行都须打开性能层 / the banner must match this run's switches
    grep -q 'M5b enabled:     true' "$R/submit.log" || fail "启动横幅里性能层不是打开状态（见 $R/submit.log）"
    if [ "$RUN" = on ]; then
        grep -q 'M5a enabled:     true' "$R/submit.log" || fail "启动横幅里数据层不是打开状态（jar 可能是旧的，见 $R/submit.log）"
    else
        grep -q 'M5a enabled:     false' "$R/submit.log" || fail "启动横幅里数据层不是关闭状态（jar 可能是旧的，见 $R/submit.log）"
    fi
    # 两次运行都采样，检查点体积的差即数据层的检查点增量（V-M5a-5）/ sample both runs for the checkpoint increment
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m5awatch_leftover >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-m3-watch.sh" start --jid "$JID" --since "$SINCE" >> "$R/watch.log" 2>&1 || fail "采样没有启动（见 $R/watch.log）"
    bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-replay.sh" --speedup "$SPEED" --start "$START" --end "$END" --inject-file "$SPEC" \
        > "$R/replay_start.log" 2>&1 || fail "重放没有启动（见 $R/replay_start.log）"
    for _ in $(seq 1 240); do
        ssh fa-master "tmux has-session -t syn-replay 2>/dev/null" || break
        sleep 60
    done
    ssh fa-master "tmux has-session -t syn-replay 2>/dev/null" && fail "重放 4 小时仍未结束"
    for _ in $(seq 1 60); do
        [ "$(drained_devices)" -ge 8 ] && break
        sleep 60
    done
    N=$(drained_devices)
    [ "$N" -ge 8 ] || prog "警告：60 分钟后只有 $N 台设备排空，仍继续收集"
    for _ in $(seq 1 10); do
        bash "$SCRIPT_DIR/syn-m2-metrics.sh" > /tmp/m5av_m1.txt 2>&1; sleep 120
        bash "$SCRIPT_DIR/syn-m2-metrics.sh" > /tmp/m5av_m2.txt 2>&1
        diff -q /tmp/m5av_m1.txt /tmp/m5av_m2.txt >/dev/null && break
    done
    prog "重放结束并排空（$N 台设备完成排空）"
    bash "$SCRIPT_DIR/syn-m3-march-collect.sh" --out-dir "$R" --since "$SINCE" > "$R/collect.log" 2>&1 || fail "收集失败（见 $R/collect.log）"
    dump_topic synergia-drift "$R/drift.jsonl"
    dump_topic "${SYN_TOPIC_M1_OUT:-synergia-m1-out}" "$W/m1out.jsonl"
    # 日记录（windowEnd = -3）从监测主题的转储里取出；监测转储本身较大，随后删除 / daily records from the monitoring dump
    grep -E '"windowEnd":-3[,}]' "$RH/m3march/monitoring.jsonl" > "$R/m5a_records.jsonl" 2>/dev/null || true
    # m1-out 排序后的行数与校验和，用于两次运行的逐行比较 / sorted line count and checksum of m1-out
    echo "$(wc -l < "$W/m1out.jsonl" | tr -d ' ') $(LC_ALL=C sort "$W/m1out.jsonl" | sha256sum | cut -c1-64)" > "$R/m1out.sha"
    : > "$R/m5a_tm_log.txt"
    for pair in "fa-worker1 taskmanager-2" "fa-worker2 taskmanager-3"; do
        set -- $pair
        ssh "$1" "docker logs --since $SINCE $2 2>&1 | grep -F '[M5a]'" >> "$R/m5a_tm_log.txt" 2>/dev/null || true
    done
    prog "收集：漂移事件 $(wc -l < "$R/drift.jsonl" | tr -d ' ') 条，日记录 $(wc -l < "$R/m5a_records.jsonl" | tr -d ' ') 条，m1-out $(cut -d' ' -f1 "$R/m1out.sha") 行，[M5a] 日志 $(wc -l < "$R/m5a_tm_log.txt" | tr -d ' ') 行"
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir "$R" >/dev/null 2>&1
    ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1
    ssh fa-master "rm -rf $RH/m3march"
    [ "$RUN" = on ] || rm -f "$W/m1out.jsonl"            # 关闭的一次只需要校验和 / the off run only needs the checksum
    prog "== 运行 $RUN 完成：$(grep -E '训练完成的设备数' "$R/collect_summary.txt" 2>/dev/null) =="
}

prog "== 编排开始：运行 ${RUNS}（on 打开数据层、off 关闭；两次都打开上下文通道与性能层），$START 至 ${END}，设备 E 注入 12 小时温度阶跃 =="
[[ ",$RUNS," == *",on,"* ]] && run_one on "--m5a-enabled true"
[ -s "$DIR/on/drift.jsonl" ] && [ -s "$DIR/on/scores.jsonl" ] && [ -s "$WORKM/on/m1out.jsonl" ] \
    || fail "没有打开数据层那次运行的结果（缺 $DIR/on/drift.jsonl、$DIR/on/scores.jsonl 或 $WORKM/on/m1out.jsonl），请用 start 跑全部两次"
[[ ",$RUNS," == *",off,"* ]] && run_one off "--m5a-enabled false"
bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$DIR/reset_final.log" 2>&1

# ---- 核对一：在线与离线 / check 1: online vs offline ----
ssh fa-master "docker exec jobmanager rm -rf /tmp/m5av && docker exec jobmanager mkdir -p /tmp/m5av && \
    docker cp $WORKM/on/m1out.jsonl jobmanager:/tmp/m5av/m1out.jsonl && \
    docker cp $RREPO/$DIR/on/scores.jsonl jobmanager:/tmp/m5av/scores.jsonl && \
    docker cp $RREPO/$DIR/on/drift.jsonl jobmanager:/tmp/m5av/drift.jsonl && \
    docker cp $RREPO/$DIR/on/m5a_records.jsonl jobmanager:/tmp/m5av/records.jsonl && \
    docker exec jobmanager java -Xmx1g -cp /opt/flink/usrlib/$JAR com.leejean.m5a.M5aOfflineRunner \
      --rounds /tmp/m5av/m1out.jsonl --scores /tmp/m5av/scores.jsonl \
      --events-out /tmp/m5av/offline_events.csv --records-out /tmp/m5av/offline_records.csv && \
    docker exec jobmanager java -Xmx1g -cp /opt/flink/usrlib/$JAR com.leejean.m5a.M5aOfflineRunner \
      --online-drift /tmp/m5av/drift.jsonl --online-monitoring /tmp/m5av/records.jsonl \
      --events-out /tmp/m5av/online_events.csv --records-out /tmp/m5av/online_records.csv && \
    for f in offline_events offline_records online_events online_records; do docker cp jobmanager:/tmp/m5av/\$f.csv $RREPO/$DIR/\$f.csv; done && \
    docker exec jobmanager rm -rf /tmp/m5av" > "$DIR/offline.log" 2>&1 \
    || fail "Java 离线运行器或转换失败（见 $DIR/offline.log）"
LAST_TS=$(sed -n 's/^最后一条轮时刻 //p' "$DIR/offline.log" | tail -1)
[ -n "$LAST_TS" ] || fail "离线运行器的输出里没有最后一条轮时刻（见 $DIR/offline.log）"
CUT=$((LAST_TS - 6 * 3600))
python3 m5a/compare_online.py --online-events "$DIR/online_events.csv" --offline-events "$DIR/offline_events.csv" \
    --online-records "$DIR/online_records.csv" --offline-records "$DIR/offline_records.csv" --cut "$CUT" > "$DIR/compare_online.txt" 2>&1
C1=$?
# ---- 核对二：评分 / check 2: scores ----
python3 deploy/scripts/m3_a4_equality.py --run "$DIR/on" --ref "$DIR/off" --ref-label "关闭数据层的运行" \
    --out "$DIR/equality_scores.md" > /dev/null 2>&1
C2=$?
# ---- 核对三：m1-out / check 3: m1-out ----
[ -s "$DIR/on/m1out.sha" ] && cmp -s "$DIR/on/m1out.sha" "$DIR/off/m1out.sha"
C3=$?
# ---- 核对四：性能层事件不变；关闭时没有数据层输出 / check 4: performance events unchanged; nothing from M5a when off ----
python3 m5b/drift_json_to_csv.py "$DIR/on/drift.jsonl" "$DIR/perf_events_on.csv" > "$DIR/perf_on.log" 2>&1
python3 m5b/drift_json_to_csv.py "$DIR/off/drift.jsonl" "$DIR/perf_events_off.csv" > "$DIR/perf_off.log" 2>&1
python3 m5b/compare_events.py "$DIR/perf_events_on.csv" "$DIR/perf_events_off.csv" > "$DIR/compare_perf.txt" 2>&1
C4=$?
OFF_DATA=$(grep -c '"layer":"data"' "$DIR/off/drift.jsonl" 2>/dev/null)
OFF_REC=$(wc -l < "$DIR/off/m5a_records.jsonl" 2>/dev/null | tr -d ' ')
[ "${OFF_DATA:-1}" -eq 0 ] && [ "${OFF_REC:-1}" -eq 0 ] || C4=1
# ---- 验收项五 / V-M5a-5 ----
python3 m5a/perf_summary.py --on "$DIR/on" --off "$DIR/off" --out "$DIR/perf.md" > "$DIR/perf.log" 2>&1
rm -f "$DIR/on/scores.jsonl" "$DIR/off/scores.jsonl"      # 体积大，核对后删除 / large; removed after the check
rm -rf "$WORKM"
{
    echo "# V-M5a-3 短重放等值与 V-M5a-5 性能"
    echo
    echo "两次短重放（$START 至 ${END}，120 倍速，预热 8,640 轮），设备 E 注入 \`$(cat "$SPEC")\`；两次都打开上下文通道与性能层。打开数据层的运行作业 \`$(grep JID "$DIR/on/run.env" | cut -d= -f2)\`，关闭的运行作业 \`$(grep JID "$DIR/off/run.env" | cut -d= -f2)\`。"
    echo
    echo "| 核对项 | 结果 |"
    echo "| --- | --- |"
    echo "| 一、在线数据层的事件与日记录与 Java 离线运行器逐行相同（截止到最后一条轮之前 6 小时） | $( [ $C1 -eq 0 ] && echo 通过 || echo '**未通过**' )（$(tr '\n' ' ' < "$DIR/compare_online.txt")） |"
    echo "| 二、打开与关闭数据层的两次运行评分逐位相同 | $( [ $C2 -eq 0 ] && echo 通过 || echo '**未通过**' )（见 \`equality_scores.md\`） |"
    echo "| 三、两次运行的 m1-out 排序后逐行相同 | $( [ $C3 -eq 0 ] && echo 通过 || echo '**未通过**' )（行数与校验和：打开 \`$(cat "$DIR/on/m1out.sha" 2>/dev/null)\`，关闭 \`$(cat "$DIR/off/m1out.sha" 2>/dev/null)\`） |"
    echo "| 四、两次运行的性能层事件逐条相同；关闭时没有数据层输出 | $( [ $C4 -eq 0 ] && echo 通过 || echo '**未通过**' )（$(tr '\n' ' ' < "$DIR/compare_perf.txt")；关闭的运行中数据层事件 ${OFF_DATA:-?} 条、日记录 ${OFF_REC:-?} 条） |"
    echo
    echo "## 验收项五（性能）"
    echo
    cat "$DIR/perf.md" 2>/dev/null || echo "（perf.md 缺失，见 perf.log）"
    echo
    if [ $C1 -eq 0 ] && [ $C2 -eq 0 ] && [ $C3 -eq 0 ] && [ $C4 -eq 0 ]; then echo "**四项全部通过。**"; else echo "**有未通过的项，见上表；按「不迎合」约定如实上报。**"; fi
} > "$DIR/verify_report.md"
rm -f "$PIDF"
prog "核对：$(tail -n 1 "$DIR/verify_report.md")（详见 $DIR/verify_report.md）"
prog "全部完成。请在 Mac 上执行：bash deploy/scripts/syn-m5a-verify.sh fetch，然后 git add -f $DIR && git commit -m \"data(m5a): V-M5a-3 短重放等值与 V-M5a-5 性能结果\" && git push origin dev-claude"
