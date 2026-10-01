#!/usr/bin/env bash
# ============================================================================
# syn-m3-watch.sh
# 把三月运行的两个监测脚本放到 master 的 tmux 会话里无人值守地运行，并提供查看进度与取回结果的子命令。
# Run the two monitoring scripts of a March run unattended in a tmux session on master, with
# subcommands to check progress and to fetch the results.
#
# 为什么放在 master：监测要从提交作业一直记录到冷启动结束、积压追平，约一个半小时以上。放在本地 Mac 上，
# Mac 一旦睡眠、断网或换了出口 IP，记录就会中断；放在 master 的 tmux 里则与本地完全无关（重放器也是这样
# 运行的）。两个监测脚本：
#   syn-m3-coldstart-watch.sh  检查点时间线、重启次数，统计悬挂的检查点自动取回明细；
#   syn-ckpt-watch.sh          逐算子与逐子任务的检查点大小（裁决要求的检查点峰值）。
# Why master: the monitoring spans well over an hour; on the Mac it would stop whenever the Mac sleeps,
# loses network or changes its exit IP. In a tmux session on master it is independent of the Mac.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录；ssh 别名 fa-master、fa-worker1、fa-worker2 可用；
#    master 上需有 tmux、curl 与 python3（start 会先检查）。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m3-watch.sh start --jid <JobID> --since <提交前记下的 UTC 时刻>
#      bash deploy/scripts/syn-m3-watch.sh status          # 随时查看，不需要一直开着
#      bash deploy/scripts/syn-m3-watch.sh stop --out-dir docs/m3_march
# 3. 前置条件 / Preconditions: 作业已提交且处于 RUNNING；没有同名 tmux 会话在运行。
# 4. 期望产出 / Expected output:
#      start  打印「监测已在 master 的 tmux 会话 syn-m3-watch 中启动」。
#      status 打印两个监测是否仍在运行、各自记录的最后几行、已进入在线的设备数、是否出现 REPORT、重放器状态。
#      stop   结束会话，把 ckpt_timeline.csv、ckpt_sizes.csv、ckpt_stale_*.json（若有）与两份日志拉回 --out-dir。
# 5. 失败兜底 / Failure fallback: master 缺少 python3 或 tmux 时 start 退出码 2，并给出在本地 Mac 上
#    用 caffeinate 后台运行的替代命令；会话已存在时退出码 3（先 stop）；作业取消或结束后两个监测会自行退出，
#    stop 照常取回已记录的部分。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

CMD="${1:-}"; shift || true
JID=""; SINCE=""; OUT_DIR=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --jid) JID="$2"; shift 2 ;;
        --since) SINCE="$2"; shift 2 ;;
        --out-dir) OUT_DIR="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

RD="${REMOTE_HOME:-/opt/fa-iforest}/m3watch"
SESSION="syn-m3-watch"

case "$CMD" in
    start)
        [ -z "$JID" ] || [ -z "$SINCE" ] && { echo "ERROR: start 需要 --jid 与 --since" >&2; exit 2; }
        if ! ssh fa-master 'command -v python3 >/dev/null && command -v tmux >/dev/null && command -v curl >/dev/null'; then
            echo "ERROR: master 上缺少 python3、tmux 或 curl，无法在 master 上运行监测。" >&2
            echo "       替代做法：在本地 Mac 上后台运行（Mac 须接电源、全程不睡眠、不断网）：" >&2
            echo "       caffeinate -i nohup bash deploy/scripts/syn-m3-coldstart-watch.sh --jid $JID --out docs/m3_march/ckpt_timeline.csv > /tmp/timeline.log 2>&1 &" >&2
            echo "       caffeinate -i nohup bash deploy/scripts/syn-ckpt-watch.sh --jid $JID --interval 20 --framesize-mb 64 --out docs/m3_march/ckpt_sizes.csv > /tmp/sizes.log 2>&1 &" >&2
            exit 2
        fi
        if ssh fa-master "tmux has-session -t $SESSION 2>/dev/null"; then
            echo "ERROR: master 上已有 tmux 会话 ${SESSION}。先执行 stop 取回上一次的结果。" >&2
            exit 3
        fi
        ssh fa-master "rm -rf $RD && mkdir -p $RD/scripts"
        scp -q "$SCRIPT_DIR/syn-m3-coldstart-watch.sh" "$SCRIPT_DIR/syn-ckpt-watch.sh" "fa-master:$RD/scripts/"
        # 两个监测脚本按自身位置找上一级目录里的 .env，因此把本地 .env 放到 $RD 下。
        # The scripts source ../.env relative to themselves, so the local .env goes to $RD.
        scp -q "$DEPLOY_DIR/.env" "fa-master:$RD/.env"
        LAUNCH="$(mktemp)"
        cat > "$LAUNCH" <<EOF
#!/usr/bin/env bash
printf 'JID=%s\nSINCE=%s\n' '$JID' '$SINCE' > $RD/meta.env
tmux new-session -d -s $SESSION -n timeline "SYN_ON_MASTER=1 bash $RD/scripts/syn-m3-coldstart-watch.sh --jid $JID --out $RD/ckpt_timeline.csv 2>&1 | tee -a $RD/timeline.log"
tmux new-window -t $SESSION -n sizes "SYN_ON_MASTER=1 bash $RD/scripts/syn-ckpt-watch.sh --jid $JID --interval 20 --framesize-mb 64 --out $RD/ckpt_sizes.csv 2>&1 | tee -a $RD/sizes.log"
EOF
        scp -q "$LAUNCH" "fa-master:$RD/launch.sh"; rm -f "$LAUNCH"
        ssh fa-master "bash $RD/launch.sh"
        sleep 3
        if ssh fa-master "tmux has-session -t $SESSION 2>/dev/null"; then
            echo "[watch] 监测已在 master 的 tmux 会话 $SESSION 中启动（作业 ${JID}），本地终端可以关闭。"
            echo "        查看进度：bash deploy/scripts/syn-m3-watch.sh status"
        else
            echo "ERROR: 会话没有起来，日志如下：" >&2
            ssh fa-master "tail -n 20 $RD/timeline.log $RD/sizes.log 2>/dev/null" >&2
            exit 1
        fi
        ;;
    status)
        META="$(ssh fa-master "cat $RD/meta.env 2>/dev/null")"
        [ -z "$META" ] && { echo "master 上没有监测记录（还没有 start，或已被复位清理）。"; exit 0; }
        eval "$META"
        echo "== 作业 ${JID}，日志起点 ${SINCE} =="
        ssh fa-master "tmux has-session -t $SESSION 2>/dev/null && echo '监测：运行中' || echo '监测：已结束（作业取消或结束后会自行退出）'"
        echo "== 检查点时间线（最后 2 行）=="
        ssh fa-master "tail -n 2 $RD/timeline.log 2>/dev/null; ls $RD/ckpt_stale_*.json 2>/dev/null | sed 's/^/统计悬挂明细：/'"
        echo "== 检查点大小（最后 3 行）=="
        ssh fa-master "tail -n 3 $RD/sizes.log 2>/dev/null"
        echo "== M3 冷启动进度 =="
        TR=0; ON=0; RP=0
        for pair in "fa-worker1 taskmanager-2" "fa-worker2 taskmanager-3"; do
            set -- $pair
            C="$(ssh "$1" "docker logs --since $SINCE $2 2>&1 | grep -E '\[M3\] Device .* (entering TRAINING|entering ONLINE|REPORT)'" 2>/dev/null)"
            TR=$((TR + $(printf '%s\n' "$C" | grep -c 'entering TRAINING')))
            ON=$((ON + $(printf '%s\n' "$C" | grep -c 'entering ONLINE')))
            RP=$((RP + $(printf '%s\n' "$C" | grep -c 'REPORT')))
        done
        echo "已进入训练 ${TR} 台，已进入在线 ${ON} 台（共 8 台），REPORT 警告 ${RP} 条"
        echo "== 重放器 =="
        bash "$SCRIPT_DIR/syn-replay.sh" status 2>/dev/null | tail -n 4
        ;;
    stop)
        [ -z "$OUT_DIR" ] && { echo "ERROR: stop 需要 --out-dir" >&2; exit 2; }
        ssh fa-master "tmux kill-session -t $SESSION 2>/dev/null; true"
        mkdir -p "$OUT_DIR"
        scp -q "fa-master:$RD/ckpt_timeline.csv" "fa-master:$RD/ckpt_sizes.csv" \
            "fa-master:$RD/timeline.log" "fa-master:$RD/sizes.log" "$OUT_DIR/" \
            || echo "[watch] ⚠ 部分文件没有取回，请检查 master 上的 $RD" >&2
        scp -q "fa-master:$RD/ckpt_stale_*.json" "$OUT_DIR/" 2>/dev/null \
            && echo "[watch] 取回了统计悬挂检查点的明细" || true
        echo "[watch] 监测已停止，结果在 ${OUT_DIR}："
        ls -l "$OUT_DIR" | grep -E 'ckpt_|\.log' | sed 's/^/        /'
        ;;
    *)
        echo "用法 / usage: $0 start --jid <JobID> --since <UTC 时刻> | status | stop --out-dir <目录>" >&2
        exit 2 ;;
esac
