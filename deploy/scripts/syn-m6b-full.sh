#!/usr/bin/env bash
# ============================================================================
# syn-m6b-full.sh
# 适应执行第二批验收项 V-M6b-4（全量重放）的编排（交接文档第七节；1.45 版裁决第四项）：2022-03-01 至 05-01（三月一日至四月三十日），
# 一百五十倍速，有限重启三次，检查点三十秒；命令回路、上下文通道、性能层、数据层、聚合模块、决策矩阵升级格与影子评分全部打开；零级
# 默认七天窗口、湿度与气压不随零级重估；一级默认生效后三天冷启动，切换前验证只记录；设备 E 的注入段保留。配置相同的两次在线运行
# （a、b），每次跑完收集评分、漂移事件、告警与命令主题、数据层日记录、影子评分快照、m1-out 摘要、监测快照的规范化摘要、TaskManager
# 日志与性能指标，并做重放完整性八条断言。两次都跑完后由 m6b/full_check.py 核对：断言八条；等值第一段（第一条一级或二级命令的决定
# 时刻之前，与链式离线 D7K 打开升级格的一档 docs/m6b/chain/D7K_up 逐条相同）；等值第二段（两次在线运行逐位相同，排除按处理时间
# 产生的三类诊断量）；影子评分表；时延表；性能表。全部步骤在 master 的 tmux 会话里无人值守地执行，提交之后本地 Mac 可以关机。
# Full replays for V-M6b-4: two identical runs with every module, the loop, M7 upgrade cells and shadow scoring on.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录；ssh 别名 fa-master、fa-worker1、fa-worker2 可用。编排在 master 的 tmux
#    会话 syn-m6b-full 里运行（master 上的仓库副本在 /opt/fa-iforest/m6bfull/repo）。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m6b-full.sh start          # 两次全量重放加核对，合计约 21 至 23 小时
#      bash deploy/scripts/syn-m6b-full.sh status         # 任何终端随时查看进度
#      bash deploy/scripts/syn-m6b-full.sh fetch          # 跑完后取回到本地 docs/m6b/full/
#      bash deploy/scripts/syn-m6b-full.sh stop           # 中止：停重放、取消本项目作业、结束采样
#    可选 --runs a,b|a|b（默认 a,b；只跑 b 时沿用 master 上已有的 a 的结果）。
# 3. 前置条件 / Preconditions: 已按 syn-m3-a4-stepc.sh setup-master 做过 master 的准备；新 jar 已上传（syn-upload-m1.sh --jar-only），
#    check-jar.sh 全部 PASS；三台节点磁盘可用不少于 25 GB（每次复位门槛为 20 GB，一次全量重放在 master 上约占二至三 GB）；运行期间
#    不做其他集群操作。启动横幅须含「switch check 3.0 record only; shadow scoring true」与「M7 enabled:      true」。
# 4. 期望产出 / Expected output: docs/m6b/full/ 下 progress.txt、full_report_tables.md（核对与四张表）、a/ 与 b/ 两个运行目录。
#    m1-out 与监测快照的完整转储较大，留在 master 上算摘要后删除，只保留摘要与核对需要的子集。
# 5. 失败兜底 / Failure fallback: 某一步失败时编排取消作业、停重放，在 progress.txt 写明原因后停止；作业失败的原因在
#    a/jm_failure.log 或 b/jm_failure.log。重放期间作业重启（有限重启三次之内）不中止编排，但断言五会记为不通过，第二段等值也会失效，
#    须重跑该次运行（start --runs a 或 --runs b）。退出码：2 参数错误或 setup-master 未完成；3 已有编排在运行。
# ============================================================================
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a
cd "$REPO_DIR" || exit 2

DIR=docs/m6b/full
PROGRESS=$DIR/progress.txt
LOG=$DIR/verify.log
PIDF=$DIR/verify.pid
TSESSION="syn-m6b-full"
RH="${REMOTE_HOME:-/opt/fa-iforest}"
RREPO="$RH/m6bfull/repo"
WORKM="$RH/m6bfull/work"
ON_MASTER="${SYN_STEPC_ON_MASTER:-0}"
SPEED=150
SPEC=docs/m3_inject_spec.txt
TOPICS_M7="synergia-scores:4,synergia-monitoring:1,synergia-m1-out:1,synergia-drift:1,synergia-alerts:1,synergia-adapt:1"
CMD="${1:-}"; shift || true
RUNS=a,b
START=2022-03-01
END=2022-05-01
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
        echo "[m6b-full] 1/4 检查 master 的准备状态……"
        ssh fa-master "test -f $RH/stepc.ready" \
            || { echo "ERROR: master 还不能登录三台机器，先执行：bash deploy/scripts/syn-m3-a4-stepc.sh setup-master" >&2; exit 2; }
        remote_alive && { echo "ERROR: master 上已有编排在运行。先 status 查看，或 stop 中止。" >&2; exit 3; }
        # 只复制受版本管理的 deploy/、m5a/、m5b/、m7/ 文件，加上 .env 与注入规格 / tracked files only, plus .env and the spec
        FILES=$(git ls-files deploy m5a m5b m6a m6b m7 docs/m6b/chain/D7K_up; echo deploy/.env; echo "$SPEC"; echo docs/m3_marapr_eda_rounds.csv)
        echo "[m6b-full] 2/4 复制 $(echo "$FILES" | wc -l | tr -d ' ') 个文件到 master 的 $RREPO ……"
        ssh fa-master "mkdir -p $RREPO && rm -rf $RREPO/deploy $RREPO/m5a $RREPO/m5b $RREPO/m6a $RREPO/m6b $RREPO/m7"   # docs/ 保留，里面可能有上一次运行的结果
        # shellcheck disable=SC2086
        COPYFILE_DISABLE=1 tar czf - $FILES \
            | ssh fa-master "tar --warning=no-unknown-keyword -xzf - -C $RREPO && cd $RREPO && \
                echo 'NODE_MASTER_PUBLIC_IP=$NODE_MASTER_IP' >> deploy/.env && \
                echo 'SYN_EXTRA_TOPICS=\"$TOPICS_M7\"' >> deploy/.env" \
            || { echo "ERROR: 复制到 master 失败（见上方 tar 或 ssh 的报错）。" >&2; exit 1; }
        ssh fa-master "test -f $RREPO/deploy/scripts/syn-m6b-full.sh && test -f $RREPO/$SPEC && test -f $RREPO/m6b/full_check.py && test -f $RREPO/docs/m6b/chain/D7K_up/stopped_at.txt" \
            || { echo "ERROR: master 上的仓库副本不完整，请把 ssh fa-master 'find $RREPO -maxdepth 3 | head -50' 的输出发给我。" >&2; exit 1; }
        # 进度文件是追加的，只看本次启动之后新写的行 / progress is appended; look only at lines written by this start
        N0=$(ssh fa-master "cat $RREPO/$PROGRESS 2>/dev/null | wc -l" | tr -d ' ')
        NEW="tail -n +$((N0 + 1)) $RREPO/$PROGRESS 2>/dev/null"
        echo "[m6b-full] 3/4 在 master 上启动 tmux 会话 $TSESSION ……"
        ssh fa-master "cd $RREPO && tmux new-session -d -s $TSESSION 'SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m6b-full.sh start --runs $RUNS --start $START --end $END; sleep 5; while [ -f $DIR/verify.pid ] && kill -0 \$(cat $DIR/verify.pid) 2>/dev/null; do sleep 30; done'" \
            || { echo "ERROR: tmux 会话创建失败（见上方报错）。" >&2; exit 1; }
        echo "[m6b-full] 4/4 等待第一行进度……"
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
            echo "[m6b-full] 已在 master 的 tmux 会话 $TSESSION 中启动（两次全量重放加核对，合计约 21 至 23 小时）。本地 Mac 现在可以关机。"
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
        remote_alive && echo "[m6b-full] 注意：master 上的编排仍在运行，现在取回的是部分结果。"
        ssh fa-master "cd $RREPO && tar czf - $DIR" | tar xzf - -C "$REPO_DIR" \
            && echo "[m6b-full] 已取回到本地 $DIR/：" && ls "$DIR" | sed 's/^/      /'
        ;;
    stop)
        if [ "$ON_MASTER" = 1 ]; then
            if alive; then pkill -P "$(cat "$PIDF")" 2>/dev/null; kill "$(cat "$PIDF")" 2>/dev/null; fi
            rm -f "$PIDF"
            bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
            for r in a b; do
                if [ -f "$DIR/$r/run.env" ]; then
                    source "$DIR/$r/run.env"
                    ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1 && echo "[m6b-full] 已取消作业 $JID"
                fi
            done
            bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m6bfwatch_aborted >/dev/null 2>&1
            prog "已手动中止（stop）。重放已停止，本项目作业已取消，采样会话已结束。"
        else
            ssh fa-master "cd $RREPO 2>/dev/null && SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m6b-full.sh stop; tmux kill-session -t $TSESSION 2>/dev/null; true"
            echo "[m6b-full] master 上的编排已中止。"
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
# 生产参数（与补遗四全量重放相同）/ production parameters as in the A4 full replay
EXTRA_BASE="--m3-enabled true --window-sec 3600 --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 7 --m3-window-length 60 --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0 --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true --checkpoint-ms 30000 --checkpoint-timeout-min 10 --checkpoint-tolerable-failures 3 --idle-wall 60 --train-threads 2"
# 全部模块、命令回路与聚合模块打开，有限重启三次（1.45 版裁决第四项）/ every module, loop and M7 on; three restarts
EXTRA_BASE="$EXTRA_BASE --m5b-enabled true --m5b-confidence 0.99 --m5b-robustness 0.5 --m5b-granularity 10min --m5b-k 36 --m5a-enabled true --adapt-enabled true --m7-enabled true --restart-attempts 3"
# 默认参数写明，防止旧 jar 的默认值混入 / defaults spelled out
EXTRA_BASE="$EXTRA_BASE --adapt-reestimate-days 7 --adapt-keep-channels Humidity,Pressure --m7-upgrade-cells true --m3-retrain-days 3 --m3-retrain-side after --m3-retrain-warm false --m3-retrain-day-sec 86400 --m3-switch-check-ratio 3.0 --m3-switch-check-enforce false --m3-shadow-scoring true"

fail() {
    prog "失败：$*。已停止；请先 fetch，再把 progress.txt 与相关日志发给我。"
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

dump_topic() {   # $1 = topic, $2 = 输出文件；条数上限取各分区末端偏移之和 / capped at the end-offset sum
    local cap
    cap="$(ssh fa-master "docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list $BROKERS --topic $1 --time -1" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
    [ "${cap:-0}" -gt 0 ] || { : > "$2"; return 0; }
    ssh fa-master "docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server $BROKERS --topic $1 \
        --from-beginning --max-messages $cap --timeout-ms 120000 2>/dev/null" > "$2" || true
}

job_alive() {   # 作业是否还在（运行或重启中）/ is the job still there (running or restarting)
    for _ in 1 2 3; do
        ssh fa-master "docker exec jobmanager flink list -r 2>/dev/null" | grep -q "$JID" && return 0
        sleep 10
    done
    return 1
}

disk_ok() {   # 三台节点可用空间不少于 25 GB / at least 25 GB free on every node
    local bad="" h g
    for h in fa-master fa-worker1 fa-worker2; do
        g=$(ssh "$h" "df -BG / | awk 'NR==2{gsub(\"G\",\"\",\$4); print \$4}'" 2>/dev/null)
        [ "${g:-0}" -ge 25 ] || bad="$bad $h(${g:-?}GB)"
    done
    [ -z "$bad" ] || { echo "$bad"; return 1; }
}

run_one() {
    RUN="$1"; local R="$DIR/$RUN"; local W="$WORKM/$RUN"; JID=""
    rm -rf "$R" "$W"; mkdir -p "$R" "$W"
    prog "== 运行 $RUN 开始：$START 至 ${END}，倍速 ${SPEED}，全部模块、命令回路、聚合模块、升级格与影子评分打开 =="
    LOW=$(disk_ok) || fail "磁盘可用空间不足 25 GB：$LOW（见 docs/cluster_disk_cleanup_zh.md）"
    bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$R/reset.log" 2>&1 || fail "复位未通过（见 $R/reset.log）"
    bash "$SCRIPT_DIR/syn-create-topics.sh" > "$R/topics.log" 2>&1 || fail "创建 topic 失败（见 $R/topics.log）"
    grep -q 'synergia-adapt' "$R/topics.log" && grep -q 'synergia-alerts' "$R/topics.log" \
        || fail "topic 清单里缺少 synergia-adapt 或 synergia-alerts（见 $R/topics.log）"
    SINCE=$(ssh fa-master date -u +%Y-%m-%dT%H:%M:%SZ)
    bash "$SCRIPT_DIR/syn-submit-m2.sh" --extra "$EXTRA_BASE" > "$R/submit.log" 2>&1 || fail "提交失败（见 $R/submit.log）"
    JID=$(grep -oE 'JobID [a-f0-9]{32}' "$R/submit.log" | awk '{print $2}' | head -1)
    [ -n "$JID" ] || fail "提交输出里没有 JobID"
    printf 'RUN=%s\nSINCE=%s\nJID=%s\n' "$RUN" "$SINCE" "$JID" > "$R/run.env"
    prog "作业已提交：${JID}；$(grep -E 'M3 level 1' "$R/submit.log" | tr -s ' ')"
    grep -q 'Restart attempts: 3' "$R/submit.log" || fail "启动横幅里重启次数不是 3（见 $R/submit.log）"
    grep -q 'Adapt (M6a):     true' "$R/submit.log" || fail "启动横幅里命令回路不是打开状态（见 $R/submit.log）"
    grep -q 'keep=\[false, true, true, false, false\]' "$R/submit.log" || fail "启动横幅里湿度与气压不是不重估（jar 可能是旧的，见 $R/submit.log）"
    grep -q 'M7 enabled:      true' "$R/submit.log" || fail "启动横幅里聚合算子不是打开状态（见 $R/submit.log）"
    grep -q 'M3 level 1:      3 days, cold start, window after the effective time; day 86400 s; switch check 3.0 record only; shadow scoring true' \
        "$R/submit.log" || fail "启动横幅里一级参数或影子评分不对（jar 可能是旧的，见 $R/submit.log）"
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m6bfwatch_leftover >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-m3-watch.sh" start --jid "$JID" --since "$SINCE" >> "$R/watch.log" 2>&1 || fail "采样没有启动（见 $R/watch.log）"
    bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-replay.sh" --speedup "$SPEED" --start "$START" --end "$END" --inject-file "$SPEC" \
        > "$R/replay_start.log" 2>&1 || fail "重放没有启动（见 $R/replay_start.log）"
    for _ in $(seq 1 900); do
        ssh fa-master "tmux has-session -t syn-replay 2>/dev/null" || break
        job_alive || fail "作业在重放期间停止（重启次数用尽或被取消；原因见 $R/jm_failure.log）"
        sleep 60
    done
    ssh fa-master "tmux has-session -t syn-replay 2>/dev/null" && fail "重放 15 小时仍未结束"
    for _ in $(seq 1 120); do
        [ "$(drained_devices)" -ge 8 ] && break
        job_alive || fail "作业在排空期间停止（原因见 $R/jm_failure.log）"
        sleep 60
    done
    N=$(drained_devices)
    [ "$N" -ge 8 ] || prog "警告：120 分钟后只有 $N 台设备完成冷启动排空，仍继续收集"
    for _ in $(seq 1 15); do
        bash "$SCRIPT_DIR/syn-m2-metrics.sh" > /tmp/m6bf_m1.txt 2>&1; sleep 120
        bash "$SCRIPT_DIR/syn-m2-metrics.sh" > "$R/m2_metrics_final.txt" 2>&1
        diff -q /tmp/m6bf_m1.txt "$R/m2_metrics_final.txt" >/dev/null && break
    done
    prog "重放结束并排空（$N 台设备完成冷启动排空）"
    # 重放完整性八条断言（含迟到命令为零）/ the eight replay-integrity assertions, late commands included
    TOTAL=$(python3 -c "import csv; r=list(csv.reader(open('docs/m3_marapr_eda_rounds.csv'))); print(sum(int(x) for row in r[1:] for x in row[1:]))")
    bash "$SCRIPT_DIR/syn-replay-verify.sh" --start-utc 2022-03-01T00:00:00Z --end-utc 2022-05-01T00:00:00Z --expected-total "$TOTAL" \
        --tol-pct 0.2 --max-messages 6000000 --report-name "m2_replay_verify_m6bfull_$RUN.csv" --since "$SINCE" --adapt \
        > "$R/replay_verify.txt" 2>&1
    echo $? > "$R/replay_verify.rc"
    cp -f "docs/m2_replay_verify_m6bfull_$RUN.csv" "$R/" 2>/dev/null
    prog "八条断言：退出码 $(cat "$R/replay_verify.rc")（$(tail -n 1 "$R/replay_verify.txt")）"
    bash "$SCRIPT_DIR/syn-m3-march-collect.sh" --out-dir "$R" --since "$SINCE" --max-messages 8000000 > "$R/collect.log" 2>&1 \
        || fail "收集失败（见 $R/collect.log）"
    dump_topic synergia-drift "$R/drift.jsonl"
    dump_topic synergia-alerts "$R/alerts.jsonl"
    dump_topic synergia-adapt "$R/adapt.jsonl"
    dump_topic "${SYN_TOPIC_M1_OUT:-synergia-m1-out}" "$W/m1out.jsonl"
    echo "$(wc -l < "$W/m1out.jsonl" | tr -d ' ') $(LC_ALL=C sort "$W/m1out.jsonl" | sha256sum | cut -c1-64)" > "$R/m1out.sha"
    MON="$RH/m3march/monitoring.jsonl"
    grep -E '"windowEnd":-3[,}]' "$MON" > "$R/m5a_records.jsonl" 2>/dev/null || true
    grep -F '"m3ShadowAbove"' "$MON" > "$R/shadow.jsonl" 2>/dev/null || true
    grep -E '"m3SwitchCheckRatio"|"m3Level1Failed"|"m3Retraining"' "$MON" > "$R/m3_markers.jsonl" 2>/dev/null || true
    python3 m6b/full_check.py --normalize-monitoring "$MON" > "$R/monitoring_norm.sha" 2>"$R/monitoring_norm.err" \
        || fail "监测快照的规范化摘要失败（见 $R/monitoring_norm.err）"
    : > "$R/tm_log.txt"
    for pair in "fa-worker1 taskmanager-2" "fa-worker2 taskmanager-3"; do
        set -- $pair
        ssh "$1" "docker logs --since $SINCE $2 2>&1 | grep -E '\[(M3|M6a|M5b|M7)\]'" >> "$R/tm_log.txt" 2>/dev/null || true
    done
    ssh fa-master "docker logs --since $SINCE jobmanager 2>&1 | grep -E 'switched from state|RESTARTING'" > "$R/jm_states.txt" 2>/dev/null || true
    bash "$SCRIPT_DIR/syn-m3-perf-collect.sh" --since "$SINCE" --out-dir "$R/perf" > "$R/perf_collect.log" 2>&1
    # 在线三份日志转成 CSV（聚合模块 V-M7-3 的转换模式）；运行目录在本次开头已新建，所以 online/ 由 docker cp 新建
    # Convert the online M7 logs to CSV; the run directory is fresh, so docker cp creates online/.
    ssh fa-master "docker exec jobmanager rm -rf /tmp/m6bf && docker exec jobmanager mkdir -p /tmp/m6bf && \
        docker cp $RREPO/$R/alerts.jsonl jobmanager:/tmp/m6bf/alerts.jsonl && \
        docker cp $RREPO/$R/adapt.jsonl jobmanager:/tmp/m6bf/adapt.jsonl && \
        docker exec jobmanager java -Xmx1g -cp /opt/flink/usrlib/$JAR com.leejean.m7.M7DumpRunner \
          --online-alerts /tmp/m6bf/alerts.jsonl --online-adapt /tmp/m6bf/adapt.jsonl --out-dir /tmp/m6bf/online && \
        docker cp jobmanager:/tmp/m6bf/online $RREPO/$R/online && docker exec jobmanager rm -rf /tmp/m6bf" \
        > "$R/convert.log" 2>&1 || fail "在线三份日志的转换失败（见 $R/convert.log）"
    prog "收集：评分 $(wc -l < "$R/scores.jsonl" | tr -d ' ') 条，漂移事件 $(wc -l < "$R/drift.jsonl" | tr -d ' ') 条，告警主题 $(wc -l < "$R/alerts.jsonl" | tr -d ' ') 条，命令主题 $(wc -l < "$R/adapt.jsonl" | tr -d ' ') 条，日记录 $(wc -l < "$R/m5a_records.jsonl" | tr -d ' ') 条，影子快照 $(wc -l < "$R/shadow.jsonl" | tr -d ' ') 条，m1-out $(cut -d' ' -f1 "$R/m1out.sha") 行"
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir "$R" >/dev/null 2>&1
    ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1
    ssh fa-master "rm -rf $RH/m3march"
    rm -f "$W/m1out.jsonl"
    prog "== 运行 $RUN 完成 =="
}

prog "== 编排开始：运行 ${RUNS}，$START 至 ${END}，倍速 ${SPEED}，有限重启三次 =="
[[ ",$RUNS," == *",a,"* ]] && run_one a
[ -s "$DIR/a/scores.jsonl" ] || fail "没有运行 a 的结果（缺 $DIR/a/scores.jsonl），请用 start 跑全部两次"
[[ ",$RUNS," == *",b,"* ]] && run_one b
bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$DIR/reset_final.log" 2>&1
python3 m6b/full_check.py --dir "$DIR" --chain docs/m6b/chain/D7K_up > "$DIR/check.txt" 2>&1
RC=$?
prog "核对：$(grep -c '^\[通过\]' "$DIR/check.txt") 项通过，$(grep -c '^\[失败\]' "$DIR/check.txt") 项失败（见 $DIR/full_report_tables.md）"
rm -f "$PIDF"
prog "== 编排结束：$( [ $RC -eq 0 ] && echo 全部核对通过 || echo 有核对项未通过 )。请在本地执行 bash deploy/scripts/syn-m6b-full.sh fetch 取回 =="
