#!/usr/bin/env bash
# ============================================================================
# syn-m3-a4-stepc.sh
# 补遗四步骤 C（短重放预验证）的五次运行一键串行执行：t1、t2、t4、kill、cancel 依次「复位 → 提交 → 启动采样 →
# 重放 →（扰动）→ 等重放结束并排空 → 收集 → 取消作业 → 取回采样」，全部完成后生成汇总报告。
# Runs the five step-C runs of addendum 4 back to back, unattended, then writes the summary report.
#
# 默认在 master 的 tmux 会话里运行整个编排：start 把 deploy/ 目录复制到 master 后立即返回，之后本地 Mac 可以关机；
# 全部跑完后用 fetch 把结果拉回本地仓库。前提是 master 能用 ssh 别名 fa-master、fa-worker1、fa-worker2 登录三台
# 机器（强杀任务管理器与收集日志要登录 worker），这由一次性的 setup-master 建立。
# By default the whole orchestration runs in a tmux session on master; start copies deploy/ there and returns, the
# Mac may then be switched off, and fetch pulls the results back. master needs ssh aliases to all three nodes,
# which the one-time setup-master creates.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录；Mac 上 ssh 别名 fa-master、fa-worker1、fa-worker2 可用。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m3-a4-stepc.sh setup-master                # 一次性：让 master 能登录三台机器
#      bash deploy/scripts/syn-m3-a4-stepc.sh start                       # 在 master 上依次跑五次，立即返回
#      bash deploy/scripts/syn-m3-a4-stepc.sh start --runs kill,cancel    # 只跑其中几次（中断后续跑）
#      bash deploy/scripts/syn-m3-a4-stepc.sh start --runs fix --speed <倍速> [--ckpt-ms <毫秒>]
#                                     # 排空缺陷修正后的验证运行（0.86 版裁决第二节第 3 条）：线程数 2、不加扰动；
#                                     # --speed 必须给出（倍速由设计会话裁定），--ckpt-ms 默认 30000
#      bash deploy/scripts/syn-m3-a4-stepc.sh status                      # 随时查看进度
#      bash deploy/scripts/syn-m3-a4-stepc.sh fetch                       # 跑完后把 docs/m3_a4 拉回本地
#      bash deploy/scripts/syn-m3-a4-stepc.sh stop                        # 中止：停编排、停重放、取消本项目作业
#    在 start 后加 --local 则改为在本地 Mac 后台运行（caffeinate，Mac 须全程开机），一般不需要。
# 3. 前置条件 / Preconditions: 运行手册第一节已完成（任务管理器 5120 兆、jar 已构建核对并上传）；集群上没有其他项目
#    （FA-iForest）的作业，也没有本项目的作业在运行；在 master 上运行时须先成功执行过一次 setup-master。
# 4. 期望产出 / Expected output: start 打印「已在 master 的 tmux 会话 syn-m3-a4-stepc 中启动」；进度写入 master 上的
#    docs/m3_a4/progress.txt（status 显示最后几行）；五次完成后最后一行为「全部完成」；fetch 后本地 docs/m3_a4/ 下有
#    五个 stepc_<标签>/ 目录与 stepc_report.md。fix 运行另在 docs/m3_a4/stepc_fix/ 下生成 fix_check.md（四项核对与
#    总判定）、equality_t2.md（与 t2 的逐位比较）、status_snapshots.csv（训练态状态快照的逐台条数）。
# 5. 失败兜底 / Failure fallback: 某次运行失败时，编排取消该次作业、停止重放、取回已有采样，在 progress.txt 写明失败的
#    运行与步骤后停止；排查后用 start --runs <剩下的标签> 续跑。退出码：2 参数错误或 setup-master 未完成；3 已有编排在运行；
#    setup-master 的连通性测试失败时退出码 4（多半是 master 到 worker 的内网 22 端口未放行，请把输出发给我）。
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
TSESSION="syn-m3-a4-stepc"
RREPO="${REMOTE_HOME:-/opt/fa-iforest}/stepc/repo"   # master 上的仓库副本 / repo copy on master
ON_MASTER="${SYN_STEPC_ON_MASTER:-0}"
CMD="${1:-}"; shift || true
RUNS="$ALL_RUNS"; LOCAL=0; FIX_SPEED=""; CKPT_MS=30000
while [[ $# -gt 0 ]]; do
    case "$1" in
        --runs) RUNS="$2"; shift 2 ;;
        --speed) FIX_SPEED="$2"; shift 2 ;;
        --ckpt-ms) CKPT_MS="$2"; shift 2 ;;
        --local) LOCAL=1; shift ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

# 每次运行的参数：线程数、倍速、扰动 / per-run threads, speed and disruption
threads_of() { case "$1" in t1) echo 1 ;; t4) echo 4 ;; *) echo 2 ;; esac; }
speed_of()   { case "$1" in cancel) echo 600 ;; fix) echo "$FIX_SPEED" ;; *) echo 120 ;; esac; }
PASS="--speed ${FIX_SPEED:-0} --ckpt-ms $CKPT_MS"   # 转交给后台编排的参数 / forwarded to the background run
disrupt_of() { case "$1" in kill|cancel) echo "$1" ;; *) echo "" ;; esac; }

prog() { echo "[$(date '+%F %T')] $*" | tee -a "$PROGRESS"; }
alive() { [ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null; }
remote_alive() { ssh fa-master "tmux has-session -t $TSESSION 2>/dev/null"; }

# 在当前机器上清理：停重放、取消本项目作业、结束采样会话 / local cleanup on whichever host runs the orchestration
cleanup_here() {
    pkill -f syn-m3-a4-disrupt.sh 2>/dev/null
    bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
    if [ -f "$OUT/current_run.env" ]; then
        source "$OUT/current_run.env"
        ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1 && echo "[stepc] 已取消作业 $JID"
    fi
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m3watch_aborted >/dev/null 2>&1
    prog "已手动中止（stop）。重放已停止，本项目作业已取消，采样会话已结束。"
}

case "$CMD" in
    setup-master)
        # 在 master 上生成一把只供本项目编排使用的密钥，把公钥加入三台机器的 authorized_keys，并在 master 上写入
        # 与 Mac 同名的 ssh 别名（走内网地址）。不复制 Mac 上的任何私钥。
        # Create a dedicated key on master, authorise it on all three nodes, and write same-named aliases on master.
        echo "[setup] 在 master 上生成专用密钥 ~/.ssh/fa_cluster（已存在则沿用）"
        PUB="$(ssh fa-master 'test -f ~/.ssh/fa_cluster || ssh-keygen -q -t ed25519 -N "" -C fa-master-stepc -f ~/.ssh/fa_cluster; cat ~/.ssh/fa_cluster.pub')"
        [ -n "$PUB" ] || { echo "ERROR: 没有读到 master 上的公钥" >&2; exit 4; }
        for h in fa-master fa-worker1 fa-worker2; do
            ssh "$h" "mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys && (grep -qxF '$PUB' ~/.ssh/authorized_keys || echo '$PUB' >> ~/.ssh/authorized_keys)" \
                && echo "[setup] 已授权到 $h"
        done
        U_M=$(ssh -G fa-master | awk '/^user /{print $2}'); U_1=$(ssh -G fa-worker1 | awk '/^user /{print $2}')
        U_2=$(ssh -G fa-worker2 | awk '/^user /{print $2}')
        CFG="# >>> fa-stepc >>>
Host fa-master
  HostName $NODE_MASTER_IP
  User $U_M
Host fa-worker1
  HostName $NODE_WORKER1_IP
  User $U_1
Host fa-worker2
  HostName $NODE_WORKER2_IP
  User $U_2
Host fa-master fa-worker1 fa-worker2
  IdentityFile ~/.ssh/fa_cluster
  StrictHostKeyChecking accept-new
  BatchMode yes
# <<< fa-stepc <<<"
        ssh fa-master "touch ~/.ssh/config && chmod 600 ~/.ssh/config && sed -i '/# >>> fa-stepc >>>/,/# <<< fa-stepc <<</d' ~/.ssh/config && cat >> ~/.ssh/config" <<< "$CFG"
        echo "[setup] 已在 master 的 ~/.ssh/config 写入 fa-master、fa-worker1、fa-worker2 三个别名（内网地址）"
        echo "[setup] 连通性测试（从 master 登录三台机器并执行 docker ps）："
        OK=1
        for h in fa-master fa-worker1 fa-worker2; do
            if ssh fa-master "ssh $h 'hostname && docker ps --format {{.Names}} | head -3'" 2>&1 | sed "s/^/        $h | /"; then :; fi
            ssh fa-master "ssh $h true" 2>/dev/null || { echo "        $h：失败" ; OK=0; }
        done
        for t in python3 tmux curl; do ssh fa-master "command -v $t >/dev/null" || { echo "        master 缺少 $t"; OK=0; }; done
        [ "$OK" = 1 ] && { ssh fa-master "touch ${REMOTE_HOME:-/opt/fa-iforest}/stepc.ready 2>/dev/null || (mkdir -p ${REMOTE_HOME:-/opt/fa-iforest} && touch ${REMOTE_HOME:-/opt/fa-iforest}/stepc.ready)"; echo "[setup] 全部通过，可以执行 start。"; } \
            || { echo "ERROR: 连通性测试未全部通过，请把以上输出发给我。" >&2; exit 4; }
        ;;
    start)
        for r in ${RUNS//,/ }; do
            [[ ",$ALL_RUNS,fix," == *",$r,"* ]] || { echo "ERROR: 未知的运行标签 $r（可选 $ALL_RUNS,fix）" >&2; exit 2; }
        done
        if [[ ",$RUNS," == *",fix,"* ]] && ! [[ "$FIX_SPEED" =~ ^[0-9]+$ && "$FIX_SPEED" -gt 0 ]]; then
            echo "ERROR: fix 运行需要 --speed <倍速>（由设计会话裁定）" >&2; exit 2
        fi
        if [ "$LOCAL" = 1 ] || [ "$ON_MASTER" = 1 ]; then
            alive && { echo "ERROR: 已有编排在运行（PID $(cat "$PIDF")）。先 status 查看，或 stop 中止。" >&2; exit 3; }
            mkdir -p "$OUT"
            if [ "$ON_MASTER" = 1 ]; then
                nohup bash "$SCRIPT_DIR/$(basename "$0")" run --runs "$RUNS" $PASS >> "$LOG" 2>&1 &
            else
                caffeinate -i nohup bash "$SCRIPT_DIR/$(basename "$0")" run --runs "$RUNS" $PASS >> "$LOG" 2>&1 &
            fi
            echo $! > "$PIDF"
            echo "[stepc] 已在后台启动（PID $!），依次运行：$RUNS。"
            exit 0
        fi
        ssh fa-master "test -f ${REMOTE_HOME:-/opt/fa-iforest}/stepc.ready" \
            || { echo "ERROR: master 还不能登录三台机器，先执行：bash $0 setup-master" >&2; exit 2; }
        remote_alive && { echo "ERROR: master 上已有编排在运行。先 status 查看，或 stop 中止。" >&2; exit 3; }
        # 把 deploy/ 复制到 master 的仓库副本；副本里的 .env 让 REST 地址走内网（master 访问自己的公网地址可能不通）。
        # Copy deploy/ to the repo copy on master; its .env points REST calls at the private address.
        # fix 运行要与 t2 逐位比较，t2 的日志与评分一并复制 / the fix run compares against t2
        ssh fa-master "mkdir -p $RREPO && rm -rf $RREPO/deploy"
        COPYFILE_DISABLE=1 tar czf - deploy docs/m3_a4/stepc_t2/m3_tm_log.txt docs/m3_a4/stepc_t2/m3_scores.jsonl | ssh fa-master "tar --warning=no-unknown-keyword -xzf - -C $RREPO && echo 'NODE_MASTER_PUBLIC_IP=$NODE_MASTER_IP' >> $RREPO/deploy/.env"
        ssh fa-master "cd $RREPO && tmux new-session -d -s $TSESSION 'SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m3-a4-stepc.sh start --runs $RUNS $PASS; sleep 5; while [ -f docs/m3_a4/stepc_all.pid ] && kill -0 \$(cat docs/m3_a4/stepc_all.pid) 2>/dev/null; do sleep 30; done'"
        sleep 5
        remote_alive && echo "[stepc] 已在 master 的 tmux 会话 $TSESSION 中启动，依次运行：$RUNS。本地 Mac 现在可以关机。" \
            || { echo "ERROR: master 上的会话没有起来，请把 ssh fa-master 'tail -n 30 $RREPO/docs/m3_a4/stepc_all.log' 的输出发给我。" >&2; exit 1; }
        echo "        查看进度：bash $0 status    跑完后取回：bash $0 fetch"
        ;;
    status)
        if [ "$ON_MASTER" = 1 ] || [ "$LOCAL" = 1 ]; then
            echo "== 编排：$(alive && echo "运行中（PID $(cat "$PIDF")）" || echo '未运行') =="
            tail -n 15 "$PROGRESS" 2>/dev/null || echo "（还没有进度记录）"
        else
            echo "== master 上的编排：$(remote_alive && echo 运行中 || echo 未运行) =="
            ssh fa-master "tail -n 15 $RREPO/$PROGRESS 2>/dev/null || echo '（还没有进度记录）'"
            if [ -f "$PROGRESS" ]; then echo "== 本地 Mac 上的编排（--local）=="; tail -n 5 "$PROGRESS"; fi
        fi
        echo
        bash "$SCRIPT_DIR/syn-m3-watch.sh" status 2>/dev/null | head -n 30
        ;;
    fetch)
        remote_alive && echo "[stepc] 注意：master 上的编排仍在运行，现在取回的是部分结果。"
        mkdir -p docs
        ssh fa-master "cd $RREPO && tar czf - docs/m3_a4" | tar xzf - -C "$REPO_DIR" \
            && echo "[stepc] 已取回到本地 docs/m3_a4/：" && ls docs/m3_a4 | sed 's/^/        /'
        ;;
    stop)
        if [ "$ON_MASTER" = 1 ] || [ "$LOCAL" = 1 ]; then
            if alive; then pkill -P "$(cat "$PIDF")" 2>/dev/null; kill "$(cat "$PIDF")" 2>/dev/null; fi
            rm -f "$PIDF"
            cleanup_here
        else
            ssh fa-master "cd $RREPO 2>/dev/null && SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m3-a4-stepc.sh stop; tmux kill-session -t $TSESSION 2>/dev/null; true"
            echo "[stepc] master 上的编排已中止。"
        fi
        ;;
    run)
        ;;
    *)
        echo "用法 / usage: $0 setup-master | start [--runs t1,t2,t4,kill,cancel,fix] [--speed N] [--ckpt-ms MS] [--local] | status | fetch | stop" >&2
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
    EXTRA="$EXTRA_BASE --train-threads $THREADS"
    [ "$RUN" = fix ] && EXTRA="${EXTRA/--checkpoint-ms 30000/--checkpoint-ms $CKPT_MS}"
    bash "$SCRIPT_DIR/syn-submit-m2.sh" --extra "$EXTRA" > "$DIR/submit.log" 2>&1 \
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
    if [ "$RUN" = fix ]; then
        # 训练态状态快照（windowEnd 为 -1）的逐台条数，须在删除监测转储之前统计 / count before the dump is deleted
        ssh fa-master "python3 -c '
import json, collections
c = collections.Counter()
for line in open(\"/opt/fa-iforest/m3march/monitoring.jsonl\"):
    try: o = json.loads(line)
    except ValueError: continue
    if o.get(\"windowEnd\") == -1: c[o.get(\"device\")] += 1
print(\"device,count\")
for d in sorted(c): print(\"%s,%d\" % (d, c[d]))'" > "$DIR/status_snapshots.csv"
    fi
    ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir "$DIR" >/dev/null 2>&1
    if [ "$RUN" = fix ]; then
        python3 "$SCRIPT_DIR/m3_a4_equality.py" --run "$DIR" --ref "$OUT/stepc_t2" --ref-label "步骤 C 的 t2" \
            --out "$DIR/equality_t2.md" > /dev/null 2>&1
        python3 "$SCRIPT_DIR/m3_a4_fixcheck.py" --run "$DIR" --speed "$SPEED" --equality "$DIR/equality_t2.md" \
            --out "$DIR/fix_check.md" > /dev/null 2>&1
        prog "修正验证：$(tail -n 1 "$DIR/fix_check.md" 2>/dev/null)（详见 $DIR/fix_check.md）"
    fi
    rm -f "$DIR/scores.jsonl"
    ssh fa-master 'rm -rf /opt/fa-iforest/m3march'
    prog "== 运行 $RUN 完成：$(grep -E '训练完成的设备数' "$DIR/collect_summary.txt" 2>/dev/null) =="
}

prog "编排开始：$RUNS"
for r in ${RUNS//,/ }; do
    run_one "$r"
done
bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$OUT/reset_final.log" 2>&1
rm -f "$PIDF"
if [ "$RUNS" = fix ]; then
    MSG="git add -f docs/m3_a4/stepc_fix && git commit -m \"data(m3): 排空缺陷修正后的步骤 C 验证结果\" && git push origin dev-claude"
    [ "$ON_MASTER" = 1 ] && prog "全部完成。请在 Mac 上执行：bash deploy/scripts/syn-m3-a4-stepc.sh fetch，然后 $MSG" \
        || prog "全部完成。请执行：$MSG"
    exit 0
fi

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
if [ "$ON_MASTER" = 1 ]; then
    prog "全部完成。请在 Mac 上执行：bash deploy/scripts/syn-m3-a4-stepc.sh fetch，然后 git add -f docs/m3_a4 && git commit -m \"data(m3): 补遗四步骤 C 短重放预验证结果\" && git push origin dev-claude"
else
    prog "全部完成。请执行：git add -f docs/m3_a4 && git commit -m \"data(m3): 补遗四步骤 C 短重放预验证结果\" && git push origin dev-claude"
fi
