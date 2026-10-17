#!/usr/bin/env bash
# ============================================================================
# syn-m3-a4-full.sh
# 补遗四全量重放（注入运行的完全重复，验收项 A4-1 至 A4-5）在 master 上无人值守地一键执行：
#   预检（任务管理器 5120 兆）→ 复位 → 以生产配置提交（守卫阈值写入配置）→ master 上
#   启动采样 → 单段重放 2022-03-01 至 05-01（设备 E 按原方案注入，默认 150 倍速）→ 等重放结束并排空 → 七条断言 → 收集、
#   性能与迟到丢弃 → 守卫在线核对 → 取消作业 → 与注入运行、与上一次全量重放分别逐位比较 → 生成汇总报告 → 复位。
# Runs the addendum-4 full replay (exact repeat of the injection run) unattended on master, then compares and reports.
#
# 依据：交接文档第四节第 2 条（10 月 14 日修订版）与 2026-10-16 裁决（开发设计日志 0.84 版）：倍速 150；A4-1 的检查点判据
# 为训练期间每一次检查点的端到端时长不超过检查点间隔 30 秒。2026-10-17 裁决（0.86 版）：排空缺陷修正后重跑，结果写入
# docs/m3_a4/full_fix/（上一次的 docs/m3_a4/full/ 保留，作为第二个对照），A4-2 同时与注入运行和上一次全量重放比较。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录；ssh 别名 fa-master、fa-worker1、fa-worker2 可用。整个编排在 master 的 tmux
#    会话 syn-m3-a4-full 里运行，start 之后本地 Mac 可以关机。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m3-a4-full.sh start                 # 立即返回；约 10.5 小时跑完
#      bash deploy/scripts/syn-m3-a4-full.sh start --speedup 150   # 倍速（默认 150，0.84 版裁决）
#      bash deploy/scripts/syn-m3-a4-full.sh status                # 随时查看进度
#      bash deploy/scripts/syn-m3-a4-full.sh fetch                 # 跑完后把 docs/m3_a4/full_fix 拉回本地
#      bash deploy/scripts/syn-m3-a4-full.sh stop                  # 中止：停编排、停重放、取消本项目作业
# 3. 前置条件 / Preconditions: 已执行过 `syn-m3-a4-stepc.sh setup-master`（master 能登录三台机器）；最新 jar 已构建核对并上传；
#    两台任务管理器已按 5120 兆重建（预检会核对）；集群上没有任何作业在运行；本地有上一次的 docs/m3_a4/full/（作为对照复制到 master）。
# 4. 期望产出 / Expected output: start 打印「已在 master 的 tmux 会话 syn-m3-a4-full 中启动」；status 显示进度；全部完成后
#    fetch 到本地 docs/m3_a4/full_fix/：a4_full_report.md（A4-1 至 A4-5 汇总）、equality.md（A4-2，对照注入运行）、
#    equality_prev.md（A4-2，对照上一次全量重放）、guard_check.md（A4-4）、replay_verify.txt（七条断言）、perf/、late_drop/、
#    ckpt_history.csv（逐个检查点）、m3_tm_log.txt、scores.jsonl 等。
# 5. 失败兜底 / Failure fallback: 某一步失败时，编排取消作业、停止重放、取回已有采样，在 docs/m3_a4/full_fix/progress.txt 写明失败
#    的步骤后停止；先 fetch，再把 progress.txt 与对应日志发给我。退出码：2 参数错误或 setup-master 未完成；3 已有编排在运行。
# ============================================================================
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
REPO_DIR="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a
cd "$REPO_DIR" || exit 2

DIR=docs/m3_a4/full_fix                     # 修正后的重跑（0.86 版裁决）；上一次的 docs/m3_a4/full 作为对照保留
PREV=docs/m3_a4/full
PROGRESS=$DIR/progress.txt
LOG=$DIR/full.log
PIDF=$DIR/full.pid
TSESSION="syn-m3-a4-full"
RH="${REMOTE_HOME:-/opt/fa-iforest}"
RREPO="$RH/a4full/repo"                     # master 上的仓库副本 / repo copy on master
ON_MASTER="${SYN_STEPC_ON_MASTER:-0}"
CMD="${1:-}"; shift || true
SPEED=150
while [[ $# -gt 0 ]]; do
    case "$1" in
        --speedup) SPEED="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

prog() { echo "[$(date '+%F %T')] $*" | tee -a "$PROGRESS"; }
alive() { [ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null; }
remote_alive() { ssh fa-master "tmux has-session -t $TSESSION 2>/dev/null"; }

case "$CMD" in
    start)
        if [ "$ON_MASTER" = 1 ]; then
            alive && { echo "ERROR: 已有编排在运行（PID $(cat "$PIDF")）。" >&2; exit 3; }
            mkdir -p "$DIR"
            nohup bash "$SCRIPT_DIR/$(basename "$0")" run --speedup "$SPEED" >> "$LOG" 2>&1 &
            echo $! > "$PIDF"
            exit 0
        fi
        [ -s "$PREV/scores.jsonl" ] && [ -s "$PREV/m3_tm_log.txt" ] \
            || { echo "ERROR: 本地缺少上一次全量重放的 $PREV/scores.jsonl 或 m3_tm_log.txt（先 git pull）" >&2; exit 2; }
        ssh fa-master "test -f $RH/stepc.ready" \
            || { echo "ERROR: master 还不能登录三台机器，先执行：bash deploy/scripts/syn-m3-a4-stepc.sh setup-master" >&2; exit 2; }
        remote_alive && { echo "ERROR: master 上已有编排在运行。先 status 查看，或 stop 中止。" >&2; exit 3; }
        # 复制到 master：deploy/、守卫阈值、注入规格、逐日轮数表（断言用）与注入运行的对照数据。副本里的 .env 让 REST 地址
        # 走内网，并写入冻结的守卫阈值（守卫收口裁决第三节）。
        # Copy deploy/, the guard thresholds, the injection spec, the daily round counts and the injection-run reference.
        ssh fa-master "mkdir -p $RREPO && rm -rf $RREPO/deploy $RREPO/docs"
        COPYFILE_DISABLE=1 tar czf - deploy docs/stuck_guard/thresholds.env docs/m3_inject_spec.txt docs/m3_marapr_eda_rounds.csv \
            docs/m3_inject/scores.jsonl docs/m3_inject/m3_tm_log.txt docs/m3_inject/ckpt_timeline.csv docs/m3_inject/perf/perf_summary.md \
            $PREV/scores.jsonl $PREV/m3_tm_log.txt \
            | ssh fa-master "tar --warning=no-unknown-keyword -xzf - -C $RREPO && cd $RREPO && \
                echo 'NODE_MASTER_PUBLIC_IP=$NODE_MASTER_IP' >> deploy/.env && grep '^SYN_STUCK_' docs/stuck_guard/thresholds.env >> deploy/.env"
        ssh fa-master "cd $RREPO && tmux new-session -d -s $TSESSION 'SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m3-a4-full.sh start --speedup $SPEED; sleep 5; while [ -f $DIR/full.pid ] && kill -0 \$(cat $DIR/full.pid) 2>/dev/null; do sleep 30; done'"
        sleep 5
        remote_alive && echo "[full] 已在 master 的 tmux 会话 $TSESSION 中启动（倍速 ${SPEED}）。本地 Mac 现在可以关机。" \
            || { echo "ERROR: master 上的会话没有起来，请把 ssh fa-master 'tail -n 30 $RREPO/$LOG' 的输出发给我。" >&2; exit 1; }
        echo "       查看进度：bash $0 status    跑完后取回：bash $0 fetch"
        ;;
    status)
        echo "== master 上的编排：$(remote_alive && echo 运行中 || echo 未运行) =="
        ssh fa-master "tail -n 20 $RREPO/$PROGRESS 2>/dev/null || echo '（还没有进度记录）'"
        echo
        bash "$SCRIPT_DIR/syn-m3-watch.sh" status 2>/dev/null | head -n 30
        ;;
    fetch)
        remote_alive && echo "[full] 注意：master 上的编排仍在运行，现在取回的是部分结果。"
        ssh fa-master "cd $RREPO && tar czf - $DIR" | tar xzf - -C "$REPO_DIR" \
            && echo "[full] 已取回到本地 $DIR/：" && ls "$DIR" | sed 's/^/       /'
        ;;
    stop)
        if [ "$ON_MASTER" = 1 ]; then
            if alive; then pkill -P "$(cat "$PIDF")" 2>/dev/null; kill "$(cat "$PIDF")" 2>/dev/null; fi
            rm -f "$PIDF"
            bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
            if [ -f "$DIR/run.env" ]; then
                source "$DIR/run.env"
                ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1 && echo "[full] 已取消作业 $JID"
            fi
            bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir "$DIR" >/dev/null 2>&1
            prog "已手动中止（stop）。重放已停止，本项目作业已取消，采样会话已结束。"
        else
            ssh fa-master "cd $RREPO 2>/dev/null && SYN_STEPC_ON_MASTER=1 bash deploy/scripts/syn-m3-a4-full.sh stop; tmux kill-session -t $TSESSION 2>/dev/null; true"
            echo "[full] master 上的编排已中止。"
        fi
        ;;
    run)
        ;;
    *)
        echo "用法 / usage: $0 start [--speedup 150] | status | fetch | stop" >&2
        exit 2 ;;
esac
[ "$CMD" = run ] || exit 0

# ============================== 后台编排（在 master 上）/ background orchestration on master ==============================
REST="http://$NODE_MASTER_IP:8081"
JID=""
EXTRA="--m3-enabled true --window-sec 3600 --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 7 --m3-window-length 60 --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0 --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true --checkpoint-ms 30000 --checkpoint-timeout-min 10 --checkpoint-tolerable-failures 3 --idle-wall 60 --train-threads 2"

fail() {
    prog "失败：$*。已停止；请先 fetch，再把 progress.txt 与相关日志发给我。"
    [ -n "$JID" ] && ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
    bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir "$DIR" >/dev/null 2>&1
    rm -f "$PIDF"
    exit 1
}

drained_devices() {
    for pair in "fa-worker1 taskmanager-2" "fa-worker2 taskmanager-3"; do
        set -- $pair
        ssh "$1" "docker logs --since $SINCE $2 2>&1 | grep -E 'buffer drained|; 0 buffered rounds to drain'" 2>/dev/null
    done | sed -n 's/.*\[M3\] Device \([^ ]*\) .*/\1/p' | sort -u | wc -l | tr -d ' '
}

prog "== 全量重放开始：倍速 $SPEED，训练线程数 2，空闲时限 60 秒，检查点超时 10 分钟 =="

# 预检：两台任务管理器的进程总量必须是 TM_HEAP_MB（交接文档第五节，5120 兆）
MEM=$(REST="$REST" python3 -c '
import json, os, subprocess
def get(path):
    out = subprocess.run(["ssh", "fa-master", "curl -s --max-time 10 " + os.environ["REST"] + path],
                         capture_output=True, text=True).stdout
    return json.loads(out) if out.strip() else {}
ids = [t["id"] for t in get("/taskmanagers").get("taskmanagers", [])]
print(" ".join(str(get("/taskmanagers/" + i).get("memoryConfiguration", {}).get("totalProcessMemory", 0) // 1048576) for i in ids))')
prog "预检：任务管理器进程总量（兆）：${MEM:-读不到}；期望两台都是 ${TM_HEAP_MB:-5120}"
if [ "$MEM" = "0 0" ] || [ -z "$MEM" ]; then
    prog "警告：REST 接口没有给出进程总量（字段缺失），跳过这项预检，继续运行"
elif [ "$MEM" != "${TM_HEAP_MB:-5120} ${TM_HEAP_MB:-5120}" ]; then
    fail "任务管理器进程总量不是 ${TM_HEAP_MB:-5120} 兆，请先执行 syn-tm-recreate.sh"
fi

bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$DIR/reset.log" 2>&1 || fail "复位未通过（见 $DIR/reset.log）"
SINCE=$(ssh fa-master date -u +%Y-%m-%dT%H:%M:%SZ)
bash "$SCRIPT_DIR/syn-submit-m2.sh" --extra "$EXTRA" > "$DIR/submit.log" 2>&1 || fail "提交失败（见 $DIR/submit.log）"
JID=$(grep -oE 'JobID [a-f0-9]{32}' "$DIR/submit.log" | awk '{print $2}' | head -1)
[ -n "$JID" ] || fail "提交输出里没有 JobID"
printf 'SINCE=%s\nJID=%s\nSPEED=%s\n' "$SINCE" "$JID" "$SPEED" > "$DIR/run.env"
grep -q 'Stuck guard:     ON' "$DIR/submit.log" || fail "启动横幅里守卫不是 ON，阈值没有写入配置（见 $DIR/submit.log）"
prog "作业已提交：$JID（日志起点 $SINCE）；$(grep -E 'Idle timeout|Ckpt interval' "$DIR/submit.log" | tr -s ' ' | tr '\n' ' ')"

bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir /tmp/m3watch_leftover >/dev/null 2>&1
bash "$SCRIPT_DIR/syn-m3-watch.sh" start --jid "$JID" --since "$SINCE" >> "$DIR/watch.log" 2>&1 || fail "采样没有启动（见 $DIR/watch.log）"
bash "$SCRIPT_DIR/syn-replay.sh" stop >/dev/null 2>&1
bash "$SCRIPT_DIR/syn-replay.sh" --speedup "$SPEED" --start 2022-03-01 --end 2022-05-01 --inject-file docs/m3_inject_spec.txt \
    > "$DIR/replay_start.log" 2>&1 || fail "重放没有启动（见 $DIR/replay_start.log）"
prog "采样与单段重放已启动（2022-03-01 至 05-01，设备 E 按原方案注入）"

for _ in $(seq 1 960); do
    ssh fa-master "tmux has-session -t syn-replay 2>/dev/null" || break
    sleep 60
done
ssh fa-master "tmux has-session -t syn-replay 2>/dev/null" && fail "重放 16 小时仍未结束"
prog "重放结束：$(ssh fa-master "grep -E 'rc=' $RH/syn-replay.log | tail -1")"

for _ in $(seq 1 120); do
    [ "$(drained_devices)" -ge 8 ] && break
    sleep 60
done
N=$(drained_devices)
[ "$N" -ge 8 ] || prog "警告：120 分钟后只有 $N 台设备排空，仍继续核验与收集"
for _ in $(seq 1 15); do
    bash "$SCRIPT_DIR/syn-m2-metrics.sh" > /tmp/a4full_m1.txt 2>&1; sleep 120
    bash "$SCRIPT_DIR/syn-m2-metrics.sh" > "$DIR/m2_metrics_final.txt" 2>&1
    diff -q /tmp/a4full_m1.txt "$DIR/m2_metrics_final.txt" >/dev/null && break
done
prog "已排空（$N 台设备完成排空）"

TOTAL=$(python3 -c "import csv; r=list(csv.reader(open('docs/m3_marapr_eda_rounds.csv'))); print(sum(int(x) for row in r[1:] for x in row[1:]))")
bash "$SCRIPT_DIR/syn-replay-verify.sh" --start-utc 2022-03-01T00:00:00Z --end-utc 2022-05-01T00:00:00Z --expected-total "$TOTAL" \
    --tol-pct 0.2 --max-messages 6000000 --report-name m2_replay_verify_a4full.csv --since "$SINCE" > "$DIR/replay_verify.txt" 2>&1
VRC=$?
cp -f docs/m2_replay_verify_a4full.csv "$DIR/" 2>/dev/null
prog "七条断言：退出码 $VRC（$(grep -c 'PASS' "$DIR/replay_verify.txt") 条 PASS 行；$(tail -n 1 "$DIR/replay_verify.txt")）"

bash "$SCRIPT_DIR/syn-m3-march-collect.sh" --out-dir "$DIR" --since "$SINCE" > "$DIR/collect.log" 2>&1 || fail "收集失败（见 $DIR/collect.log）"
bash "$SCRIPT_DIR/syn-m3-perf-collect.sh" --since "$SINCE" --out-dir "$DIR/perf" > "$DIR/perf_collect.log" 2>&1
bash "$SCRIPT_DIR/syn-m2-late-drop-history.sh" --since "$SINCE" --job "$JID" --out-dir "$DIR/late_drop" > "$DIR/late_drop.log" 2>&1
python3 "$SCRIPT_DIR/m3_a4_guard_check.py" --monitoring "$RH/m3march/monitoring.jsonl" --truth "$DIR/inject-truth.csv" \
    --thresholds docs/stuck_guard/thresholds.env --out "$DIR/guard_check.md" > /dev/null 2>&1
prog "收集完成：$(grep -E '训练完成的设备数' "$DIR/collect_summary.txt")；守卫核对：$(tail -n 1 "$DIR/guard_check.md" 2>/dev/null)"

ssh fa-master "docker exec jobmanager flink cancel $JID" >/dev/null 2>&1
bash "$SCRIPT_DIR/syn-m3-watch.sh" stop --out-dir "$DIR" >/dev/null 2>&1
python3 "$SCRIPT_DIR/m3_a4_equality.py" --run "$DIR" --ref docs/m3_inject --out "$DIR/equality.md" > /dev/null 2>&1
ERC=$?
python3 "$SCRIPT_DIR/m3_a4_equality.py" --run "$DIR" --ref "$PREV" --ref-label "上一次全量重放（修正前）" \
    --out "$DIR/equality_prev.md" > /dev/null 2>&1
PRC=$?
python3 "$SCRIPT_DIR/m3_a4_full_report.py" --run "$DIR" --ref docs/m3_inject --out "$DIR/a4_full_report.md" > /dev/null 2>&1
prog "逐位等值：对照注入运行 $( [ $ERC -eq 0 ] && echo 全部相同 || echo "存在不同（退出码 $ERC，见 equality.md）" )；对照上一次全量重放 $( [ $PRC -eq 0 ] && echo 全部相同 || echo "存在不同（退出码 $PRC，见 equality_prev.md）" )；汇总报告已生成：$DIR/a4_full_report.md"

ssh fa-master "rm -rf $RH/m3march"
bash "$SCRIPT_DIR/syn-reset-env.sh" --yes > "$DIR/reset_final.log" 2>&1
rm -f "$PIDF"
prog "全部完成。请在 Mac 上执行：bash deploy/scripts/syn-m3-a4-full.sh fetch，然后 git add -f $DIR && git commit -m \"data(m3): 补遗四全量重放（排空缺陷修正后）结果\" && git push origin dev-claude"
