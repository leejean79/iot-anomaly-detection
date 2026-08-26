#!/usr/bin/env bash
# ============================================================================
# syn-replay.sh
# 在 master 的 tmux 会话中运行 CsvKafkaReplayer，把数据集重放进 synergia-source。
# Run CsvKafkaReplayer inside a tmux session on master, replaying the dataset into synergia-source.
#
# 为什么用 tmux（这是相对早前版本的关键修正）:
#   重放器是长时进程（全量重放约一小时）。交接文档 §3.5 要求“前台运行”（避免用 `&` 后台化被
#   SIGHUP 杀死），但若直接 `ssh -t docker run` 前台跑，本地 Mac 一旦断连、SSH 会话结束，
#   docker run 同样会收到 SIGHUP 而中断。tmux 同时满足两点：进程在 tmux 内拥有真实前台伪终端
#   （不会因后台化被杀），而 tmux 会话与 SSH 分离（断连不波及）。这与老项目 FA-iForest 的实验
#   脚本一致（其注释建议 `tmux new -s ...` 后 Ctrl+B D 脱离）。
#   处理端作业由 syn-submit-m1.sh 以 `flink run -d` 分离提交，常驻集群、本就不惧断连，无需 tmux。
#
# 运行载体沿用 5-load-data.sh 的既有模式：master 上 `docker run --rm --network host` 起一个临时
# flink 镜像容器（镜像自带 JDK），挂载 jars（只读）与数据集（读写，供 offset 落盘）。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash），ssh 免密到 fa-master；master 上已安装 tmux；
#    jar 与数据集已上传（syn-upload-m1.sh）。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-replay.sh --speedup 600 --start 2022-05-21 --end 2022-05-22  # 启动（默认，tmux 后台常驻）
#      bash deploy/scripts/syn-replay.sh --speedup 3600                                       # 全量压力（约一小时）
#      bash deploy/scripts/syn-replay.sh attach     # 附着到运行中的会话观看（Ctrl+B D 脱离）
#      bash deploy/scripts/syn-replay.sh status     # 查看是否在跑 + 最近日志
#      bash deploy/scripts/syn-replay.sh logs       # 持续跟踪日志（Ctrl+C 只停跟踪，不停重放）
#      bash deploy/scripts/syn-replay.sh stop       # 停止重放（停容器 + 杀 tmux 会话）
#      bash deploy/scripts/syn-replay.sh fg --dry-run   # 前台阻塞运行（供快速 dry-run；断连即止）
# 3. 前置条件 / Preconditions: M1Job 已 RUNNING（先提交作业再重放）；synergia-source 已建（8 分区）；
#    数据集在 ${SYN_DATASET_DIR}；fa-iforest/flink:${FLINK_VERSION} 镜像在 master。
# 4. 期望产出 / Expected output: 消息落 synergia-source（显式分区）；进度与空闲压缩审计日志同时
#    写入 master 的 ${REMOTE_HOME}/syn-replay.log（容器被 --rm 清理后仍可回看结果摘要）。
# 5. 失败兜底 / Failure fallback: tmux 会话在 Mac 断连后仍继续；--resume 从 offset 续跑；
#    start 前检测到已有会话/容器则拒绝并提示先 stop；master 无 tmux 时明确报错并给安装提示。
#
# 缩写自查 / Abbreviations: tmux = terminal multiplexer（终端复用器，可脱离/重连的持久会话）；
#   k = speedup 加速倍数；offset = 位移/进度；rc = return code 返回码。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
JAR_NAME="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"
MAIN="${SYN_REPLAYER_MAIN:-com.leejean.source.CsvKafkaReplayer}"
DATASET_DIR="${SYN_DATASET_DIR:-/opt/fa-iforest/datasets/synergia/files_csv}"

SESSION="syn-replay"                       # tmux 会话名 / tmux session name
CONTAINER="syn-replay"                     # 容器名 / container name
LOG="${REMOTE_HOME}/syn-replay.log"        # master 上的日志文件 / log file on master
LAUNCH="${REMOTE_HOME}/.syn-replay-launch.sh"

on_master() { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "$@"; }
on_master_tty() { ssh -t $SSH_OPTS "$SSH_USER@$MASTER_SSH" "$@"; }

# 子命令分派：首参为 attach/status/stop/logs/fg/start 则消费之，否则默认 start（其余全作重放器参数）。
# Sub-command dispatch: consume a leading attach/status/stop/logs/fg/start; otherwise default to start.
CMD="start"
case "${1:-}" in
    attach|status|stop|logs|fg|start) CMD="$1"; shift ;;
esac
REPLAY_ARGS="$*"

# 组装重放器命令（供 start/fg 共用）/ assemble the replayer command (shared by start/fg)
replayer_docker_cmd() {
    local name_flag="$1"   # 容器名标志或空 / --name flag or empty
    echo "docker run --rm $name_flag --network host --user root \
        -v ${REMOTE_HOME}/jars:/jars:ro \
        -v $DATASET_DIR:/data \
        fa-iforest/flink:${FLINK_VERSION} \
        java -cp /jars/$JAR_NAME $MAIN \
            --data-dir /data \
            --offset-file /data/.replayer.offset \
            --brokers $BROKERS \
            --topic ${SYN_TOPIC_SOURCE:-synergia-source} \
            --num-partitions ${SYN_SOURCE_PARTITIONS:-8} \
            $REPLAY_ARGS"
}

case "$CMD" in
    start)
        # 前置检查：tmux 是否可用 / preflight: tmux present
        if ! on_master "command -v tmux >/dev/null 2>&1"; then
            echo "FATAL: master 上未安装 tmux。请先安装（Ubuntu: sudo apt-get install -y tmux）。" >&2
            echo "FATAL: tmux is not installed on master; install it first." >&2
            exit 2
        fi
        # 已有在跑的会话 → 拒绝 / an existing session → refuse
        if on_master "tmux has-session -t $SESSION 2>/dev/null"; then
            echo "已有名为 '$SESSION' 的重放会话在运行。先 'bash $0 stop' 或 'bash $0 attach'。" >&2
            echo "A replay session '$SESSION' already exists. Stop or attach to it first." >&2
            exit 1
        fi
        # 残留同名容器（上次崩溃/异常退出遗留）→ 拒绝并提示 stop 清理 / a leftover container → refuse
        if [ -n "$(on_master "docker ps -a --filter name=^/${CONTAINER}\$ -q" 2>/dev/null)" ]; then
            echo "master 上残留名为 '$CONTAINER' 的容器（可能上次异常退出）。先运行 'bash $0 stop' 清理。" >&2
            echo "A leftover container '$CONTAINER' exists on master; run 'bash $0 stop' to clean it up first." >&2
            exit 1
        fi

        # 把启动命令写成 master 上的 launcher 脚本，launcher 自身把输出 tee 到日志文件。
        # 这样 tmux 命令只是 `bash LAUNCH`，没有管道/多层引号，杜绝一类引号解析问题。
        # Write a launcher on master that self-tees its output to the log; the tmux command is then a
        # bare `bash LAUNCH` with no pipe/nested quotes, eliminating a class of quoting bugs.
        DOCKER_CMD="$(replayer_docker_cmd "--name $CONTAINER")"
        on_master "cat > $LAUNCH" <<EOF
#!/usr/bin/env bash
set -uo pipefail
# 所有输出既进 tmux 面板又落日志文件（容器被 --rm 清理后仍可回看）。
exec > >(tee -a "$LOG") 2>&1
echo "[syn-replay] start \$(date '+%F %T %Z')  args: $REPLAY_ARGS"
$DOCKER_CMD
rc=\$?
echo "[syn-replay] exit \$(date '+%F %T %Z') rc=\$rc"
EOF
        on_master "chmod +x $LAUNCH"

        # 在分离的 tmux 会话中运行 launcher / run the launcher in a detached tmux session
        on_master "tmux new-session -d -s $SESSION 'bash $LAUNCH'"

        echo "===================================="
        echo "[replay] 已在 master 的 tmux 会话 '$SESSION' 中启动（Mac 断连不影响，控制台在此立即返回）。"
        echo "  数据集(host)=$DATASET_DIR  topic=${SYN_TOPIC_SOURCE:-synergia-source}"
        echo "  参数: ${REPLAY_ARGS:-<none>}"
        echo "  观看:   bash $0 attach     （Ctrl+B 然后 D 脱离，不停重放）"
        echo "  日志:   bash $0 logs       （或 ssh 到 master: tail -f ${LOG}）"
        echo "  状态:   bash $0 status ；  停止: bash $0 stop"
        echo "===================================="

        # 启动后即时反馈：等 3 秒回读会话状态与首几行日志，帮助立刻判断是否真的跑起来了。
        # Immediate feedback: after 3s, echo session state and the first log lines so a failed launch
        # (e.g. image/mount error) is visible right away instead of looking like "no output".
        sleep 3
        echo "[启动自检 / launch self-check]"
        on_master "tmux has-session -t $SESSION 2>/dev/null && echo '  tmux 会话存活 / session alive' || echo '  ⚠ tmux 会话已结束——很可能启动即失败，见下方日志 / session already ended — likely failed to start'"
        on_master "tail -n 8 $LOG 2>/dev/null | sed 's/^/  | /' || true"
        echo "  （若上面报错，多半是镜像名/挂载/端口问题；修正后重跑 start）"
        ;;

    attach)
        on_master_tty "tmux attach -t $SESSION" \
            || { echo "没有正在运行的 '$SESSION' 会话（可能已结束）。查看结果: bash $0 status" >&2; exit 1; }
        ;;

    status)
        echo "== tmux 会话 / session =="
        on_master "tmux has-session -t $SESSION 2>/dev/null && echo '  RUNNING (session $SESSION)' || echo '  未运行 / not running'"
        echo "== 容器 / container =="
        on_master "docker ps --filter name=$CONTAINER --format '  {{.Names}}  {{.Status}}' || true"
        echo "== 日志尾部 / log tail ($LOG) =="
        on_master "tail -n 20 $LOG 2>/dev/null || echo '  （暂无日志 / no log yet）'"
        ;;

    logs)
        echo "跟踪 ${LOG}（Ctrl+C 仅停止跟踪，不影响重放）/ following $LOG (Ctrl+C stops following only)"
        on_master_tty "tail -f $LOG"
        ;;

    stop)
        echo "停止重放：停并删容器 + 杀 tmux 会话 / stop+remove container + kill tmux session"
        on_master "docker rm -f $CONTAINER >/dev/null 2>&1 || true; tmux kill-session -t $SESSION 2>/dev/null || true"
        echo "已停止 / stopped. 结果日志仍在 master: $LOG"
        ;;

    fg)
        # 前台阻塞运行（供快速 dry-run/交互）；断连即止，不用于长跑。
        # Foreground blocking run (for quick dry-run/interactive); dies on disconnect, not for long runs.
        echo "[replay:fg] 前台运行（断连即止；长跑请用默认 start）/ foreground (dies on disconnect)"
        on_master_tty "$(replayer_docker_cmd "")"
        ;;
esac
