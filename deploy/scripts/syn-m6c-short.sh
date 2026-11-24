#!/usr/bin/env bash
# ============================================================================
# syn-m6c-short.sh
# 适应执行第三批的两组短重放（交接文档第六节第 3 步；1.54 版裁决第八处、1.57 版裁决第五项）：2022-03-01 至 04-03，一百五十倍速，有限重启
# 三次，检查点三十秒；命令回路、上下文通道、性能层、数据层、聚合模块与升级格打开，影子评分关闭；设备 E 的注入段保留。
#   第一组（--group 1）：运行 a 用 m6b-closeout 标签构建的 jar，运行 b 用新 jar，第三批全部开关关闭；核对「第三批全部开关关闭时，新代码
#       与标签 jar 在同一配置下逐位相同」。
#   第二组（--group 2）：两次都用新 jar，开启丁档（--adapt-anchor trigger --adapt-anchor-min-days 7 --m7-anchor-defer-days 7）与二级预案
#       （--m7-rebuild-plan true），切换验证与振荡规则关闭；核对两次运行逐位相同。
# 每次运行收集与第二批全量重放相同的九类输出，跑完后由 m6c/short_check.py 核对。全部步骤在 master 的 tmux 会话里无人值守地执行，
# 提交之后本地 Mac 可以关机。
# The two batch-3 short replays: closeout-tag jar versus new jar with every batch-3 switch off, then two runs with arm D and the plan on.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录，JDK 11 与 Maven（prepare 要打两个 jar）；ssh 别名 fa-master、fa-worker1、fa-worker2
#    可用。编排在 master 的 tmux 会话 syn-m6c-short1 或 syn-m6c-short2 里运行（master 上的仓库副本在 /opt/fa-iforest/m6cshort<组>/repo）。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m6c-short.sh prepare                 # 打新 jar 与标签 jar，核对标记，上传到 master（约 5 至 10 分钟）
#      bash deploy/scripts/syn-m6c-short.sh start --group 1         # 第一组两次短重放加核对，合计约 11 至 12 小时
#      bash deploy/scripts/syn-m6c-short.sh status --group 1        # 任何终端随时查看进度
#      bash deploy/scripts/syn-m6c-short.sh fetch --group 1         # 跑完后取回到本地 docs/m6c/short1/
#      bash deploy/scripts/syn-m6c-short.sh stop --group 1          # 中止：停重放、取消本项目作业、结束采样
#    第二组把 --group 1 换成 --group 2（结果在 docs/m6c/short2/）。可选 --runs a,b|a|b（默认 a,b）。
# 3. 前置条件 / Preconditions: 已按 syn-m3-a4-stepc.sh setup-master 做过 master 的准备；本地仓库能取到标签 m6b-closeout（prepare 会
#    git fetch 它）；JobManager 进程内存不少于 3,072 MB；三台节点复位之后磁盘可用不少于 20 GB；运行期间不做其他集群操作；
#    两组不能同时运行（共用集群与 Kafka 主题）。
# 4. 期望产出 / Expected output: docs/m6c/short<组>/ 下 progress.txt、short_report.md（核对结果）、a/ 与 b/ 两个运行目录。
#    master 上的 python3 没有时区数据时核对会退出码 2，fetch 后在本地执行 python3 m6c/short_check.py --dir docs/m6c/short1 --group 1。
# 5. 失败兜底 / Failure fallback: 某一步失败时编排取消作业、停重放，在 progress.txt 写明原因后停止；作业失败的原因在 a/jm_failure.log
#    或 b/jm_failure.log。重放期间作业重启（有限重启三次之内）不中止编排，但核对会记为不通过，须重跑该次运行（start --runs a 或 b）。
#    退出码：2 参数错误、setup-master 未完成或 prepare 失败；3 已有编排在运行。
# ============================================================================
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a
cd "$REPO_DIR" || exit 2

CMD="${1:-}"; shift || true
GROUP=""
RUNS=a,b
START=2022-03-01
END=2022-04-03
while [ $# -gt 0 ]; do
    case "$1" in
        --group) GROUP="$2"; shift 2 ;;
        --runs) RUNS="$2"; shift 2 ;;
        *) echo "ERROR: 未知参数 $1" >&2; exit 2 ;;
    esac
done
RH="${REMOTE_HOME:-/opt/fa-iforest}"
NEWJAR="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"
TAGJAR="iot-anomaly-detection-m6b-closeout.jar"
TAG=m6b-closeout
MARK_CLASS=com/leejean/m1/AdaptiveScalerFunction.class
MARK=anchoredStarts                                      # 第三批新增的方法名：新 jar 有、标签 jar 没有 / batch-3 marker

# ============================== prepare（本地 Mac）/ build and upload both jars ==============================
if [ "$CMD" = prepare ]; then
    command -v mvn >/dev/null || { echo "ERROR: 没有 mvn" >&2; exit 2; }
    echo "[m6c] 1/5 打新 jar（mvn clean package -DskipTests）……"
    mvn -q clean package -DskipTests || { echo "ERROR: 新 jar 构建失败（见上方 Maven 输出）。" >&2; exit 2; }
    LOCAL_NEW="$REPO_DIR/target/iot-anomaly-detection-1.0-SNAPSHOT.jar"
    [ "$(unzip -p "$LOCAL_NEW" "$MARK_CLASS" | grep -a -c "$MARK")" -gt 0 ] \
        || { echo "ERROR: 新 jar 里没有第三批的标记 $MARK（jar 是旧的？）" >&2; exit 2; }
    echo "[m6c] 2/5 取标签 $TAG 并在临时工作区打标签 jar……"
    git fetch -q origin "refs/tags/$TAG:refs/tags/$TAG" 2>/dev/null || git rev-parse -q --verify "refs/tags/$TAG" >/dev/null \
        || { echo "ERROR: 取不到标签 $TAG" >&2; exit 2; }
    WT="$(mktemp -d "${TMPDIR:-/tmp}/m6c-tagjar.XXXXXX")"
    git worktree add -q --detach "$WT/src" "$TAG" || { echo "ERROR: git worktree add 失败" >&2; exit 2; }
    ( cd "$WT/src" && mvn -q clean package -DskipTests ) || { git worktree remove --force "$WT/src"; echo "ERROR: 标签 jar 构建失败。" >&2; exit 2; }
    LOCAL_TAG="$WT/$TAGJAR"
    cp "$WT/src/target/iot-anomaly-detection-1.0-SNAPSHOT.jar" "$LOCAL_TAG"
    git worktree remove --force "$WT/src"
    [ "$(unzip -p "$LOCAL_TAG" "$MARK_CLASS" | grep -a -c "$MARK")" -eq 0 ] \
        || { echo "ERROR: 标签 jar 里竟有第三批的标记 $MARK（标签不对？）" >&2; exit 2; }
    echo "[m6c] 3/5 上传新 jar 为 $RH/jars/$NEWJAR ……"
    scp -q "$LOCAL_NEW" "fa-master:$RH/jars/$NEWJAR" || { echo "ERROR: 上传新 jar 失败" >&2; exit 2; }
    echo "[m6c] 4/5 上传标签 jar 为 $RH/jars/$TAGJAR ……"
    scp -q "$LOCAL_TAG" "fa-master:$RH/jars/$TAGJAR" || { echo "ERROR: 上传标签 jar 失败" >&2; exit 2; }
    rm -rf "${WT:?}"
    echo "[m6c] 5/5 在 jobmanager 容器里核对两个 jar 的标记……"
    N1=$(ssh fa-master "docker exec jobmanager sh -c 'unzip -p /opt/flink/usrlib/$NEWJAR $MARK_CLASS | grep -a -c $MARK'" 2>/dev/null)
    N2=$(ssh fa-master "docker exec jobmanager sh -c 'unzip -p /opt/flink/usrlib/$TAGJAR $MARK_CLASS | grep -a -c $MARK'" 2>/dev/null)
    if [ "${N1:-0}" -gt 0 ] && [ "${N2:-1}" = 0 ]; then
        echo "[m6c] 完成：容器里新 jar 有标记（$N1）、标签 jar 没有（0）。接着执行：bash $0 start --group 1"
        exit 0
    fi
    echo "ERROR: 容器里的核对不符（新 jar ${N1:-读不到}、标签 jar ${N2:-读不到}）。请把这两行发给我。" >&2
    exit 2
fi

[[ "$GROUP" =~ ^(1|2)$ ]] || { echo "ERROR: 须给 --group 1 或 --group 2" >&2; exit 2; }
[[ "$RUNS" =~ ^(a,b|a|b)$ ]] || { echo "ERROR: --runs 只能是 a,b、a 或 b" >&2; exit 2; }
DIR=docs/m6c/short$GROUP
PROGRESS=$DIR/progress.txt
LOG=$DIR/verify.log
PIDF=$DIR/verify.pid
TSESSION="syn-m6c-short$GROUP"
RREPO="$RH/m6cshort$GROUP/repo"
WORKM="$RH/m6cshort$GROUP/work"
ON_MASTER="${SYN_STEPC_ON_MASTER:-0}"
SPEED=150
SPEC=docs/m3_inject_spec.txt
TOPICS_M7="synergia-scores:4,synergia-monitoring:1,synergia-m1-out:1,synergia-drift:1,synergia-alerts:1,synergia-adapt:1"

prog() { echo "[$(date '+%F %T')] $*" | tee -a "$PROGRESS"; }
alive() { [ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null; }
remote_alive() { ssh fa-master "tmux has-session -t $TSESSION 2>/dev/null"; }
other_alive() { local o=$((3 - GROUP)); ssh fa-master "tmux has-session -t syn-m6c-short$o 2>/dev/null || tmux has-session -t syn-m6b-full 2>/dev/null"; }

case "$CMD" in
    start)
        if [ "$ON_MASTER" = 1 ]; then
            alive && { echo "ERROR: 已有编排在运行（PID $(cat "$PIDF")）。" >&2; exit 3; }
            mkdir -p "$DIR"
            nohup bash "$SCRIPT_DIR/$(basename "$0")" run --group "$GROUP" --runs "$RUNS" >> "$LOG" 2>&1 &
            echo $! > "$PIDF"
            exit 0
        fi
        echo "[m6c] 1/4 检查 master 的准备状态与两个 jar……"
        ssh fa-master "test -f $RH/stepc.ready" \
            || { echo "ERROR: master 还不能登录三台机器，先执行：bash deploy/scripts/syn-m3-a4-stepc.sh setup-master" >&2; exit 2; }
        ssh fa-master "test -f $RH/jars/$NEWJAR" || { echo "ERROR: master 上没有新 jar，先执行：bash $0 prepare" >&2; exit 2; }
        [ "$GROUP" = 2 ] || ssh fa-master "test -f $RH/jars/$TAGJAR" || { echo "ERROR: master 上没有标签 jar，先执行：bash $0 prepare" >&2; exit 2; }
        remote_alive && { echo "ERROR: master 上已有本组编排在运行。先 status 查看，或 stop 中止。" >&2; exit 3; }
        other_alive && { echo "ERROR: master 上另一组短重放或全量重放正在运行，两者共用集群，不能同时跑。" >&2; exit 3; }
        FILES=$(git ls-files deploy m5a m5b m6a m6b m6c m7; echo deploy/.env; echo "$SPEC")
        echo "[m6c] 2/4 复制 $(echo "$FILES" | wc -l | tr -d ' ') 个文件到 master 的 $RREPO ……"
        ssh fa-master "mkdir -p $RREPO && rm -rf $RREPO/deploy $RREPO/m5a $RREPO/m5b $RREPO/m6a $RREPO/m6b $RREPO/m6c $RREPO/m7"
        # shellcheck disable=SC2086
        COPYFILE_DISABLE=1 tar czf - $FILES \
            | ssh fa-master "tar --warning=no-unknown-keyword -xzf - -C $RREPO && cd $RREPO && \
                echo 'NODE_MASTER_PUBLIC_IP=$NODE_MASTER_IP' >> deploy/.env && \
                echo 'SYN_EXTRA_TOPICS=\"$TOPICS_M7\"' >> deploy/.env" \
            || { echo "ERROR: 复制到 master 失败（见上方 tar 或 ssh 的报错）。" >&2; exit 1; }
        ssh fa-master "test -f $RREPO/deploy/scripts/syn-m6c-short.sh && test -f $RREPO/$SPEC && test -f $RREPO/m6c/short_check.py && test -f $RREPO/m6b/full_check.py" \
            || { echo "ERROR: master 上的仓库副本不完整，请把 ssh fa-master 'find $RREPO -maxdepth 3 | head -50' 的输出发给我。" >&2; exit 1; }
        N0=$(ssh fa-master "cat $RREPO/$PROGRESS 2>/dev/null | wc -l" | tr -d ' ')
        NEW="tail -n +$((N0 + 1)) $RREPO/$PROGRESS 2>/dev/null"
        echo "[m6c] 3/4 在 master 上启动 tmux 会话 $TSESSION ……"
        ssh fa-master "cd $RREPO && tmux new-session -d -s $TSESSION 'SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m6c-short.sh start --group $GROUP --runs $RUNS; sleep 5; while [ -f $DIR/verify.pid ] && kill -0 \$(cat $DIR/verify.pid) 2>/dev/null; do sleep 30; done'" \
            || { echo "ERROR: tmux 会话创建失败（见上方报错）。" >&2; exit 1; }
        echo "[m6c] 4/4 等待第一行进度……"
        for _ in $(seq 1 12); do
            [ -n "$(ssh fa-master "$NEW")" ] && break
            sleep 5
        done
        if [ -n "$(ssh fa-master "$NEW")" ]; then
            ssh fa-master "$NEW | tail -n 3"
            if ssh fa-master "$NEW | grep -q '失败'"; then
                echo "ERROR: 编排已经失败（见上方进度）。请执行 bash $0 fetch --group $GROUP，把 $DIR/ 下的 progress.txt 与相应日志发给我。" >&2
                exit 1
            fi
            echo "[m6c] 已在 master 的 tmux 会话 $TSESSION 中启动（第${GROUP}组两次短重放加核对，合计约 11 至 12 小时）。本地 Mac 现在可以关机。"
        else
            echo "ERROR: 60 秒内没有进度记录。请把 ssh fa-master 'tail -n 30 $RREPO/$LOG; tmux ls' 的输出发给我。" >&2
            exit 1
        fi
        echo "      查看进度：bash $0 status --group $GROUP    跑完后取回：bash $0 fetch --group $GROUP"
        ;;
    status)
        echo "== master 上的第${GROUP}组编排：$(remote_alive && echo 运行中 || echo 未运行) =="
        ssh fa-master "tail -n 15 $RREPO/$PROGRESS 2>/dev/null || echo '（还没有进度记录）'"
        if ! remote_alive; then
            echo "== 诊断（未运行时）=="
            ssh fa-master "ls -ld $RREPO $RREPO/$DIR 2>&1; echo '-- verify.log 末尾 --'; tail -n 20 $RREPO/$LOG 2>&1; echo '-- tmux 会话 --'; tmux ls 2>&1"
        fi
        echo
        bash "$SCRIPT_DIR/syn-m3-watch.sh" status 2>/dev/null | head -n 20
        ;;
    fetch)
        remote_alive && echo "[m6c] 注意：master 上的编排仍在运行，现在取回的是部分结果。"
        ssh fa-master "cd $RREPO && tar czf - $DIR" | tar xzf - -C "$REPO_DIR" \
            && echo "[m6c] 已取回到本地 $DIR/：" && ls "$DIR" | sed 's/^/      /'
        ;;
    stop)
        if [ "$ON_MASTER" = 1 ]; then
            if alive; then pkill -P "$(cat "$PIDF")" 2>/dev/null; kill "$(cat "$PIDF")" 2>/dev/null; fi
            rm -f "$PIDF"
            bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
            for r in a b; do
                if [ -f "$DIR/$r/run.env" ]; then
                    source "$DIR/$r/run.env"
                    ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1 && echo "[m6c] 已取消作业 $JID"
                fi
            done
            bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m6cshort_aborted >/dev/null 2>&1
            prog "已手动中止（stop）。重放已停止，本项目作业已取消，采样会话已结束。"
        else
            ssh fa-master "cd $RREPO 2>/dev/null && SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m6c-short.sh stop --group $GROUP; tmux kill-session -t $TSESSION 2>/dev/null; true"
            echo "[m6c] master 上的第${GROUP}组编排已中止。"
        fi
        ;;
    run)
        ;;
    *)
        echo "用法 / usage: $0 prepare | start --group 1|2 [--runs a,b|a|b] | status --group N | fetch --group N | stop --group N" >&2
        exit 2 ;;
esac
[ "$CMD" = run ] || exit 0


# ============================== 后台编排（在 master 上）/ background orchestration on master ==============================
JID=""
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
# 生产参数（与第二批全量重放相同）/ production parameters as in the batch-2 full replay
EXTRA_BASE="--m3-enabled true --window-sec 3600 --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 7 --m3-window-length 60 --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0 --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true --checkpoint-ms 30000 --checkpoint-timeout-min 10 --checkpoint-tolerable-failures 3 --checkpoint-min-pause-ms 30000 --idle-wall 60 --train-threads 2"
EXTRA_BASE="$EXTRA_BASE --m5b-enabled true --m5b-confidence 0.99 --m5b-robustness 0.5 --m5b-granularity 10min --m5b-k 36 --m5a-enabled true --adapt-enabled true --m7-enabled true --restart-attempts 3"
# 第二批默认参数写明；影子评分关闭（1.52 版裁决，1.54 版第八处）/ batch-2 defaults spelled out; shadow scoring off
EXTRA_BASE="$EXTRA_BASE --adapt-reestimate-days 7 --adapt-keep-channels Humidity,Pressure --m7-upgrade-cells true --m3-retrain-days 3 --m3-retrain-side after --m3-retrain-warm false --m3-retrain-day-sec 86400 --m3-switch-check-ratio 3.0 --m3-switch-check-enforce false --m3-shadow-scoring false"
# 第三批开关：标签 jar 不认识这些参数，只给新 jar / batch-3 switches, for the new jar only
B3_OFF="--adapt-anchor off --adapt-anchor-min-days 2 --m7-anchor-defer-days 0 --m7-osc-suppress false --m7-rebuild-plan false"
B3_D="--adapt-anchor trigger --adapt-anchor-min-days 7 --m7-anchor-defer-days 7 --m7-osc-suppress false --m7-rebuild-plan true"

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

job_alive() {
    for _ in 1 2 3; do
        ssh fa-master "docker exec jobmanager flink list -r 2>/dev/null" | grep -q "$JID" && return 0
        sleep 10
    done
    return 1
}

disk_ok() {
    local bad="" h g
    for h in fa-master fa-worker1 fa-worker2; do
        g=$(ssh "$h" "df -BG / | awk 'NR==2{gsub(\"G\",\"\",\$4); print \$4}'" 2>/dev/null)
        [ "${g:-0}" -ge 20 ] || bad="$bad $h(${g:-?}GB)"
    done
    [ -z "$bad" ] || { echo "$bad"; return 1; }
}

run_one() {   # $1 = 运行名，$2 = jar 名，$3 = 第三批参数（标签 jar 为空）/ run, jar, batch-3 options
    RUN="$1"; local JAR="$2" B3="$3"; local R="$DIR/$RUN"; local W="$WORKM/$RUN"; JID=""
    rm -rf "$R" "$W"; mkdir -p "$R" "$W"
    prog "== 运行 $RUN 开始：$START 至 ${END}，倍速 ${SPEED}，jar $JAR；第三批参数：${B3:-（标签 jar，无）} =="
    JMMEM=$(ssh fa-master "docker exec jobmanager grep -E '^jobmanager.memory.process.size' /opt/flink/conf/flink-conf.yaml | tail -1" 2>/dev/null \
        | sed -E 's/.*: *([0-9]+)m.*/\1/')
    [ "${JMMEM:-0}" -ge 3072 ] 2>/dev/null || fail "JobManager 进程内存为 ${JMMEM:-读不到} MB，不足 3,072 MB"
    bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$R/reset.log" 2>&1 || fail "复位未通过（见 $R/reset.log）"
    LOW=""
    for _ in $(seq 1 10); do
        if LOW=$(disk_ok); then LOW=""; break; fi
        sleep 30
    done
    [ -z "$LOW" ] || fail "复位之后磁盘可用空间仍不足 20 GB：$LOW（见 docs/cluster_disk_cleanup_zh.md）"
    bash "$SCRIPT_DIR/syn-create-topics.sh" > "$R/topics.log" 2>&1 || fail "创建 topic 失败（见 $R/topics.log）"
    grep -q 'synergia-adapt' "$R/topics.log" && grep -q 'synergia-alerts' "$R/topics.log" \
        || fail "topic 清单里缺少 synergia-adapt 或 synergia-alerts（见 $R/topics.log）"
    SINCE=$(ssh fa-master date -u +%Y-%m-%dT%H:%M:%SZ)
    SYN_JOB_JAR_OVERRIDE="$JAR" bash "$SCRIPT_DIR/syn-submit-m2.sh" --extra "$EXTRA_BASE${B3:+ $B3}" > "$R/submit.log" 2>&1 \
        || fail "提交失败（见 $R/submit.log）"
    JID=$(grep -oE 'JobID [a-f0-9]{32}' "$R/submit.log" | awk '{print $2}' | head -1)
    [ -n "$JID" ] || fail "提交输出里没有 JobID"
    printf 'RUN=%s\nSINCE=%s\nJID=%s\nJAR=%s\n' "$RUN" "$SINCE" "$JID" "$JAR" > "$R/run.env"
    grep -q "/opt/flink/usrlib/$JAR" "$R/submit.log" || prog "警告：提交输出里没有看到 jar 路径 /opt/flink/usrlib/$JAR（以横幅核对为准）"
    prog "作业已提交：${JID}；$(grep -E 'M3 level 1' "$R/submit.log" | tr -s ' ')"
    grep -q 'Restart attempts: 3' "$R/submit.log" || fail "启动横幅里重启次数不是 3（见 $R/submit.log）"
    grep -q 'min pause 30000 ms' "$R/submit.log" || fail "启动横幅里检查点最短间隔不是 30 秒（见 $R/submit.log）"
    grep -q 'Adapt (M6a):     true' "$R/submit.log" || fail "启动横幅里命令回路不是打开状态（见 $R/submit.log）"
    grep -q 'keep=\[false, true, true, false, false\]' "$R/submit.log" || fail "启动横幅里湿度与气压不是不重估（见 $R/submit.log）"
    grep -q 'M7 enabled:      true' "$R/submit.log" || fail "启动横幅里聚合算子不是打开状态（见 $R/submit.log）"
    grep -q 'M7 model state:  events from the context module' "$R/submit.log" || fail "启动横幅里没有模型状态事件（见 $R/submit.log）"
    grep -q 'M3 level 1:      3 days, cold start, window after the effective time; day 86400 s; switch check 3.0 record only; shadow scoring false' \
        "$R/submit.log" || fail "启动横幅里一级参数或影子评分不对（见 $R/submit.log）"
    if [ -z "$B3" ]; then
        grep -q 'Batch 3:' "$R/submit.log" && fail "标签 jar 的横幅里竟有第三批一行（jar 不对，见 $R/submit.log）"
    else
        grep -q 'Batch 3:' "$R/submit.log" || fail "新 jar 的横幅里没有第三批一行（jar 是旧的？见 $R/submit.log）"
        prog "$(grep -E 'Batch 3:' "$R/submit.log" | tr -s ' ')"
    fi
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m6cshort_leftover >/dev/null 2>&1
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
    date '+%F %T' > "$R/replay_end.txt"
    awk '/^Max skew:/{b=""} /^Max skew:|^Skew pauses:|^\[skew-warn\]/{b=b $0 "\n"} END{printf "%s", b}' "$RH/syn-replay.log" 2>/dev/null \
        | tr -s ' ' > "$R/throttle.txt"
    prog "重放器限速：$(tr '\n' ' ' < "$R/throttle.txt")"
    for _ in $(seq 1 120); do
        [ "$(drained_devices)" -ge 8 ] && break
        job_alive || fail "作业在排空期间停止（原因见 $R/jm_failure.log）"
        sleep 60
    done
    N=$(drained_devices)
    [ "$N" -ge 8 ] || prog "警告：120 分钟后只有 $N 台设备完成冷启动排空，仍继续收集"
    for _ in $(seq 1 15); do
        bash "$SCRIPT_DIR/syn-m2-metrics.sh" > /tmp/m6cs_m1.txt 2>&1; sleep 120
        bash "$SCRIPT_DIR/syn-m2-metrics.sh" > "$R/m2_metrics_final.txt" 2>&1
        diff -q /tmp/m6cs_m1.txt "$R/m2_metrics_final.txt" >/dev/null && break
    done
    prog "重放结束并排空（$N 台设备完成冷启动排空）"
    bash "$SCRIPT_DIR/syn-m3-march-collect.sh" --out-dir "$R" --since "$SINCE" --max-messages 8000000 > "$R/collect.log" 2>&1 \
        || fail "收集失败（见 $R/collect.log）"
    dump_topic synergia-drift "$R/drift.jsonl"
    dump_topic synergia-alerts "$R/alerts.jsonl"
    dump_topic synergia-adapt "$R/adapt.jsonl"
    dump_topic "${SYN_TOPIC_M1_OUT:-synergia-m1-out}" "$W/m1out.jsonl"
    echo "$(wc -l < "$W/m1out.jsonl" | tr -d ' ') $(LC_ALL=C sort "$W/m1out.jsonl" | sha256sum | cut -c1-64)" > "$R/m1out.sha"
    local MON="$RH/m3march/monitoring.jsonl"
    grep -E '"windowEnd":-3[,}]' "$MON" > "$R/m5a_records.jsonl" 2>/dev/null || true
    grep -F '"m3ShadowAbove"' "$MON" > "$R/shadow.jsonl" 2>/dev/null || true
    grep -E '"m3SwitchCheckRatio"|"m3Level1Failed"|"m3Retraining"|"m3SwitchHeld"' "$MON" > "$R/m3_markers.jsonl" 2>/dev/null || true
    [ -s "$MON" ] && [ -s "$R/scores.jsonl" ] && [ -s "$R/adapt.jsonl" ] || fail "收集的文件不全（$MON、$R/scores.jsonl 或 $R/adapt.jsonl）"
    python3 m6b/full_check.py --normalize-monitoring "$MON" > "$R/monitoring_norm.sha" 2>"$R/monitoring_norm.err" \
        || fail "监测快照的规范化摘要失败（见 $R/monitoring_norm.err）"
    : > "$R/tm_log.txt"
    for pair in "fa-worker1 taskmanager-2" "fa-worker2 taskmanager-3"; do
        set -- $pair
        ssh "$1" "docker logs --since $SINCE $2 2>&1 | grep -E '\[(M3|M6a|M5b|M7)\]'" >> "$R/tm_log.txt" 2>/dev/null || true
    done
    ssh fa-master "docker logs --since $SINCE jobmanager 2>&1 | grep -E 'switched from state|RESTARTING'" > "$R/jm_states.txt" 2>/dev/null || true
    ssh fa-master "docker exec jobmanager rm -rf /tmp/m6cs && docker exec jobmanager mkdir -p /tmp/m6cs && \
        docker cp $RREPO/$R/alerts.jsonl jobmanager:/tmp/m6cs/alerts.jsonl && \
        docker cp $RREPO/$R/adapt.jsonl jobmanager:/tmp/m6cs/adapt.jsonl && \
        docker exec jobmanager java -Xmx1g -cp /opt/flink/usrlib/$JAR com.leejean.m7.M7DumpRunner \
          --online-alerts /tmp/m6cs/alerts.jsonl --online-adapt /tmp/m6cs/adapt.jsonl --out-dir /tmp/m6cs/online && \
        docker cp jobmanager:/tmp/m6cs/online $RREPO/$R/online && docker exec jobmanager rm -rf /tmp/m6cs" \
        > "$R/convert.log" 2>&1 || fail "在线三份日志的转换失败（见 $R/convert.log）"
    prog "收集：评分 $(wc -l < "$R/scores.jsonl" | tr -d ' ') 条，漂移事件 $(wc -l < "$R/drift.jsonl" | tr -d ' ') 条，告警主题 $(wc -l < "$R/alerts.jsonl" | tr -d ' ') 条，命令主题 $(wc -l < "$R/adapt.jsonl" | tr -d ' ') 条，日记录 $(wc -l < "$R/m5a_records.jsonl" | tr -d ' ') 条，m1-out $(cut -d' ' -f1 "$R/m1out.sha") 行"
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir "$R" >/dev/null 2>&1
    ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1
    ssh fa-master "rm -rf $RH/m3march"
    rm -f "$W/m1out.jsonl"
    prog "磁盘可用空间：$(for h in fa-master fa-worker1 fa-worker2; do printf '%s %s ' "$h" "$(ssh "$h" "df -h / | awk 'NR==2{print \$4}'")"; done)"
    prog "== 运行 $RUN 完成 =="
}

prog "== 编排开始：第${GROUP}组，运行 ${RUNS}，$START 至 ${END}，倍速 ${SPEED}，有限重启三次 =="
if [ "$GROUP" = 1 ]; then
    [[ ",$RUNS," == *",a,"* ]] && run_one a "$TAGJAR" ""
    [[ ",$RUNS," == *",b,"* ]] && run_one b "$NEWJAR" "$B3_OFF"
else
    [[ ",$RUNS," == *",a,"* ]] && run_one a "$NEWJAR" "$B3_D"
    [[ ",$RUNS," == *",b,"* ]] && run_one b "$NEWJAR" "$B3_D"
fi
bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$DIR/reset_final.log" 2>&1
python3 m6c/short_check.py --dir "$DIR" --group "$GROUP" > "$DIR/check.txt" 2>&1
RC=$?
if [ $RC -eq 2 ]; then
    prog "核对：master 上的 python3 没有时区数据，未核对；请 fetch 后在本地执行 python3 m6c/short_check.py --dir $DIR --group $GROUP"
else
    prog "核对：$(grep -c '^\[通过\]' "$DIR/check.txt") 项通过，$(grep -c '^\[失败\]' "$DIR/check.txt") 项失败（见 $DIR/short_report.md）"
fi
rm -f "$PIDF"
prog "== 编排结束：$( [ $RC -eq 0 ] && echo 全部核对通过 || echo 有核对项未通过或未核对 )。请在本地执行 bash deploy/scripts/syn-m6c-short.sh fetch --group $GROUP 取回 =="
