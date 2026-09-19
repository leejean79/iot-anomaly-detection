#!/usr/bin/env bash
# ============================================================================
# syn-reset-env.sh
# 阶段实验开始前，把整套测试环境复位到「理想干净状态」，并以一张核对表给出可否开跑的结论。
# Reset the whole test environment to a known-clean state before a new experiment phase, and
# finish with a checklist that says whether it is safe to start.
#
# 【为什么需要它】2026-09 的三月基线连续两轮翻车，事后看两次的共同点都不是算法或参数，而是
# **环境残留**：第一轮是上一次运行的 topic 数据与作业状态未清干净，checkpoint 在 akka.framesize
# 上越限、作业重启、AT_LEAST_ONCE 重发，留下 25,517 条重复轮；第二轮在一套刚重建、Kafka 与
# ZooKeeper 全新空卷的环境里跑，43 次 checkpoint 无一失败，四条断言一次通过。把「复位」做成一条
# 可执行、可核对的流程，比每次凭记忆逐项回想可靠。
# Why: two March baseline runs failed on environment residue, not on algorithm or parameters.
#
# 【本脚本不做什么】不提交作业、不启动重放、不改任何算法参数（半径 R、k、窗口 W/S、标定天数
# 一概不碰）。它只负责把环境恢复到可以开跑的状态，并如实报告哪一项没达标。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash + python3），仓库根目录；ssh 主机别名
#    fa-master / fa-worker1 / fa-worker2 可用（由 deploy/scripts/refresh-ips.sh 维护）。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-reset-env.sh                      # 交互确认后复位
#      bash deploy/scripts/syn-reset-env.sh --yes                # 跳过确认（自动化）
#      bash deploy/scripts/syn-reset-env.sh --min-free-gb 25     # 提高磁盘门槛，默认 20
#      bash deploy/scripts/syn-reset-env.sh --prune-volumes      # 顺带回收孤儿匿名卷（不可撤销）
#      bash deploy/scripts/syn-reset-env.sh --dry-run            # 只检查不改动
#    参数 / Arguments:
#      --yes            跳过交互确认
#      --dry-run        只做检查与报告，不执行任何有副作用的步骤
#      --min-free-gb N  每节点可用空间门槛，默认 20
#      --prune-volumes  执行 docker volume prune -f（**不可撤销**，默认不做，只报告）
#      --keep-topics    不清空 synergia-* topic（少数续跑场景）
# 3. 前置条件 / Preconditions: 三台节点已开机、容器已由 2-up-all.sh 拉起；deploy/.env 就绪；
#    本地 target/ 下有当前要用的 jar（用于与集群侧 jar 比对 SHA-256）。
# 4. 期望产出 / Expected output: 逐阶段进度 + 末尾一张核对表，每项 PASS/FAIL/SKIP；
#    全部 PASS 时退出码 0（可用 && 串联下一步），任何一项 FAIL 退出码 1。
# 5. 常见失败兜底 / Failure fallback:
#      磁盘不达标 → 见 docs/cluster_disk_cleanup_zh.md；
#      容器缺失 → 先 bash deploy/scripts/2-up-all.sh 并保留完整输出；
#      jar 不一致 → bash deploy/scripts/syn-upload-m1.sh --jar-only 后重跑本脚本；
#      topic 清空后末端偏移非 0 → Kafka 删除是异步的，等 1~2 分钟再跑一次本脚本复核。
#
# 缩写自查 / Abbreviations: topic = Kafka 主题；末端偏移 = 分区当前最大偏移量；
#   孤儿卷 = 无任何容器引用的 Docker 匿名卷；REST = Flink 的 HTTP 接口（:8081）。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

ASSUME_YES=0; DRY_RUN=0; MIN_FREE_GB=20; PRUNE_VOLUMES=0; KEEP_TOPICS=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --yes) ASSUME_YES=1; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        --min-free-gb) MIN_FREE_GB="$2"; shift 2 ;;
        --prune-volumes) PRUNE_VOLUMES=1; shift ;;
        --keep-topics) KEEP_TOPICS=1; shift ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

NODES=(fa-master fa-worker1 fa-worker2)
REST="http://$NODE_MASTER_IP:8081"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
RHOME="${REMOTE_HOME:-/opt/fa-iforest}"
JAR_NAME="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"

# 核对表 / checklist
CHECK_NAMES=(); CHECK_STATES=(); CHECK_NOTES=()
record() { CHECK_NAMES+=("$1"); CHECK_STATES+=("$2"); CHECK_NOTES+=("$3"); }
step()   { echo ""; echo "───────── $1"; }
run()    { if [ "$DRY_RUN" -eq 1 ]; then echo "    [dry-run] $*"; else eval "$@"; fi; }

echo "===================================================================="
echo "  syn-reset-env.sh — 阶段实验前的环境复位"
echo "  磁盘门槛 ${MIN_FREE_GB} GB/节点   清空 topic: $([ "$KEEP_TOPICS" -eq 1 ] && echo 否 || echo 是)"
echo "  回收孤儿卷: $([ "$PRUNE_VOLUMES" -eq 1 ] && echo '是（不可撤销）' || echo '否（只报告）')"
echo "  模式: $([ "$DRY_RUN" -eq 1 ] && echo '只检查不改动' || echo '执行复位')"
echo "===================================================================="
if [ "$ASSUME_YES" -eq 0 ] && [ "$DRY_RUN" -eq 0 ]; then
    echo "将要：停重放 → 取消本项目作业 → 重启 Flink 容器 → 清空 synergia-* topic → 清理中间产物。"
    printf "继续？输入 yes 回车："
    read -r ans
    [ "$ans" = "yes" ] || { echo "已取消。"; exit 130; }
fi

# ---------- 1. 容器在位 ----------
step "1/8 容器在位检查 / containers present"
MISSING=""
for h in "${NODES[@]}"; do
    names="$(ssh "$h" "docker ps --format '{{.Names}}'" 2>/dev/null | tr '\n' ' ')"
    echo "  $h: ${names:-<无>}"
    case "$h" in
        fa-master)  for c in zookeeper kafka-1 jobmanager; do
                        echo "$names" | grep -qw "$c" || MISSING="$MISSING $h/$c"; done ;;
        fa-worker1) for c in kafka-2 taskmanager-2; do
                        echo "$names" | grep -qw "$c" || MISSING="$MISSING $h/$c"; done ;;
        fa-worker2) for c in kafka-3 taskmanager-3; do
                        echo "$names" | grep -qw "$c" || MISSING="$MISSING $h/$c"; done ;;
    esac
done
if [ -n "$MISSING" ]; then
    record "容器在位" FAIL "缺失:$MISSING —— 先跑 2-up-all.sh 并保留完整输出"
else
    record "容器在位" PASS "六个核心容器齐全"
fi

# ---------- 1b. JobManager REST 可达性 ----------
# 【为何单列一项】REST 不可达时，第 4 项「无作业在跑」与第 8 项「akka.framesize」都会退化成 SKIP，
# 而一串 SKIP 看上去人畜无害——但 REST 不可达意味着**作业根本提交不上去**，是硬故障而不是"跳过"。
# 这一项把它明确记为 FAIL，并让后面两项在备注里指回这里。
# Why its own check: when REST is unreachable the later checks degrade to SKIP, which reads as
# benign; but an unreachable REST means no job can be submitted at all. Record it as a hard failure.
step "1b/8 JobManager REST 可达性 / JobManager REST reachability"
REST_OK=0
OVERVIEW="$(ssh fa-master "curl -s --max-time 10 '$REST/overview'" 2>/dev/null)"
if printf '%s' "$OVERVIEW" | grep -q 'slots-total'; then
    REST_OK=1
    SLOTS0="$(printf '%s' "$OVERVIEW" | python3 -c '
import json,sys
try:
    d=json.load(sys.stdin); print("%s/%s" % (d.get("slots-available"), d.get("slots-total")))
except Exception: print("?")' 2>/dev/null)"
    echo "  $REST 可达，可用 slot: $SLOTS0"
    record "JM REST 可达" PASS "slot $SLOTS0"
else
    echo "  $REST 无响应。"
    echo "  排查顺序：ssh fa-master \"docker ps --format '{{.Names}}\t{{.Status}}' | grep -i jobmanager\""
    echo "            ssh fa-master \"docker logs jobmanager --tail 60\""
    record "JM REST 可达" FAIL "$REST 无响应 —— 作业无法提交，先查 docker logs jobmanager"
fi

# ---------- 2. 磁盘门槛 ----------
step "2/8 磁盘门槛 / free space >= ${MIN_FREE_GB} GB"
DISK_BAD=""
for h in "${NODES[@]}"; do
    avail_kb="$(ssh "$h" "df -Pk / | awk 'NR==2{print \$4}'" 2>/dev/null)"
    if [ -z "$avail_kb" ]; then echo "  $h: 读取失败"; DISK_BAD="$DISK_BAD $h(读取失败)"; continue; fi
    avail_gb=$(( avail_kb / 1024 / 1024 ))
    echo "  $h: 可用 ${avail_gb} GB"
    [ "$avail_gb" -lt "$MIN_FREE_GB" ] && DISK_BAD="$DISK_BAD $h(${avail_gb}GB)"
done
if [ -n "$DISK_BAD" ]; then
    record "磁盘门槛" FAIL "不达标:$DISK_BAD —— 见 docs/cluster_disk_cleanup_zh.md"
else
    record "磁盘门槛" PASS "三台均 >= ${MIN_FREE_GB} GB"
fi

# ---------- 3. 停重放 ----------
step "3/8 停止重放器并清掉续跑位点 / stop replayer, drop the resume offset"
run "bash '$SCRIPT_DIR/syn-replay.sh' stop >/dev/null 2>&1 || true"
run "ssh fa-master \"rm -f ${SYN_DATASET_DIR:-$RHOME/datasets/synergia/files_csv}/.replayer.offset\" >/dev/null 2>&1 || true"
LEFT="$(ssh fa-master "docker ps --format '{{.Names}}' | grep -i replay || true" 2>/dev/null)"
if [ -n "$LEFT" ]; then record "重放器已停" FAIL "仍有容器: $LEFT"
else record "重放器已停" PASS "无残留容器，续跑位点已清"; fi

# ---------- 4. 取消本项目作业（绝不碰其他作业）----------
step "4/8 取消本项目作业 / cancel this project's jobs only"
JOBS="$(ssh fa-master "curl -s --max-time 20 '$REST/jobs/overview'" 2>/dev/null)"
OURS="$(printf '%s' "$JOBS" | python3 -c '
import json,sys
try:
    js=json.load(sys.stdin).get("jobs",[])
except Exception:
    js=[]
for j in js:
    # 只匹配本项目的作业名；其余一律不动。/ match only this project; never touch anything else.
    if j.get("state")=="RUNNING" and ("M1Job" in j.get("name","") or "M2Job" in j.get("name","")
                                      or "M3" in j.get("name","")):
        print(j["jid"])' 2>/dev/null)"
if [ -z "$OURS" ]; then
    echo "  没有本项目作业在跑。"
else
    for jid in $OURS; do
        echo "  取消 $jid"
        run "ssh fa-master \"docker exec jobmanager flink cancel $jid\" >/dev/null 2>&1 || true"
    done
fi
sleep 3
STILL="$(ssh fa-master "curl -s --max-time 20 '$REST/jobs/overview'" 2>/dev/null | python3 -c '
import json,sys
try: js=json.load(sys.stdin).get("jobs",[])
except Exception: js=[]
print(sum(1 for j in js if j.get("state")=="RUNNING"))' 2>/dev/null || echo "?")"
if [ "$REST_OK" -eq 0 ]; then
    # REST 不可达时 JSON 解析失败同样会得到 0，那是"读不到"而不是"没有作业"，不可据此判 PASS。
    # An unreachable REST also parses to 0; that is "unknown", not "none running".
    record "无作业在跑" SKIP "REST 不可达，无从判断（见 JM REST 可达 一项）"
elif [ "$STILL" = "0" ]; then record "无作业在跑" PASS "RUNNING 作业数 0"
elif [ "$DRY_RUN" -eq 1 ]; then record "无作业在跑" SKIP "dry-run 未取消"
else record "无作业在跑" FAIL "仍有 $STILL 个 RUNNING 作业，请在 Flink UI 上核对后处理"; fi

# ---------- 5. 重启 Flink 容器（清空每 JVM 的指标计数器）----------
step "5/8 重启 Flink 容器 / restart Flink containers"
echo "  目的：Flink 的指标计数器是**每 JVM** 的。上一轮运行留下的计数若不清零，本轮的对账恒等式"
echo "        会读到跨轮累加的数字，且表面看不出异常（断言五抓的正是这一类）。"
run "ssh fa-master   'docker restart jobmanager'    >/dev/null 2>&1"
run "ssh fa-worker1  'docker restart taskmanager-2' >/dev/null 2>&1"
run "ssh fa-worker2  'docker restart taskmanager-3' >/dev/null 2>&1"
if [ "$DRY_RUN" -eq 1 ]; then
    record "Flink 已重启" SKIP "dry-run"
else
    echo "  等待 JobManager REST 就绪……"
    OK=0
    for _ in $(seq 1 30); do
        if ssh fa-master "curl -s --max-time 5 '$REST/overview'" 2>/dev/null | grep -q 'taskmanagers'; then OK=1; break; fi
        sleep 3
    done
    if [ "$OK" -eq 1 ]; then
        SLOTS="$(ssh fa-master "curl -s --max-time 10 '$REST/overview'" 2>/dev/null | python3 -c '
import json,sys
try:
    d=json.load(sys.stdin); print("%s/%s" % (d.get("slots-available"), d.get("slots-total")))
except Exception: print("?")' 2>/dev/null)"
        echo "  REST 就绪，可用 slot: $SLOTS"
        record "Flink 已重启" PASS "REST 就绪，slot $SLOTS"
    else
        record "Flink 已重启" FAIL "90 秒内 REST 未就绪，查 docker logs jobmanager"
    fi
fi

# ---------- 6. 清空 topic 并复核末端偏移 ----------
step "6/8 清空 synergia-* topic 并复核末端偏移 / clean topics and verify end offsets"
if [ "$KEEP_TOPICS" -eq 1 ]; then
    record "topic 已清零" SKIP "--keep-topics"
else
    run "bash '$SCRIPT_DIR/syn-clean-topics.sh' --yes >/dev/null 2>&1"
    if [ "$DRY_RUN" -eq 1 ]; then
        record "topic 已清零" SKIP "dry-run"
    else
        echo "  Kafka 删除是异步的，等待 20 秒后复核……"
        sleep 20
        NONZERO=""
        for t in "${SYN_TOPIC_SOURCE:-synergia-source}" synergia-m1-out synergia-monitoring synergia-scores; do
            sum="$(ssh fa-master "docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell \
                --broker-list $BROKERS --topic $t --time -1" 2>/dev/null \
                | awk -F: '{s+=$3} END{print s+0}')"
            echo "    $t 末端偏移合计: ${sum:-?}"
            [ "${sum:-1}" != "0" ] && NONZERO="$NONZERO $t(${sum})"
        done
        if [ -n "$NONZERO" ]; then
            record "topic 已清零" FAIL "非零:$NONZERO —— 删除是异步的，等 1~2 分钟后重跑本脚本复核"
        else
            record "topic 已清零" PASS "四个 topic 末端偏移均为 0"
        fi
    fi
fi

# ---------- 7. 清理中间产物 / 报告孤儿卷 ----------
step "7/8 清理远端中间产物、盘点孤儿卷 / clear scratch dirs, report orphan volumes"
run "ssh fa-master \"rm -rf $RHOME/m2probe $RHOME/m2baseline $RHOME/m2surge\" >/dev/null 2>&1 || true"
TOTAL_DANG=0
for h in "${NODES[@]}"; do
    # 一次远端调用同时取「个数」与「合计 MB」，并且**必须**在列表为空时短路：
    # 若直接把空列表喂给 du，du 会退化成度量当前目录（登录后的家目录），于是打印出
    # 「0 个、约 18 MB」这种自相矛盾的读数。/ Guard the empty case: `du` with no operand
    # measures the login directory, which prints "0 volumes, 18 MB".
    read -r n mb <<<"$(ssh "$h" 'v=$(docker volume ls -q -f dangling=true); \
        if [ -z "$v" ]; then echo "0 0"; else \
        echo "$(echo "$v" | wc -l | tr -d " ") $(du -sm $(echo "$v" | sed "s#^#/var/lib/docker/volumes/#") 2>/dev/null | awk "{s+=\$1} END{print s+0}")"; fi' 2>/dev/null)"
    n="${n:-0}"; mb="${mb:-0}"
    echo "  $h: 孤儿卷 ${n} 个，约 ${mb} MB"
    TOTAL_DANG=$(( TOTAL_DANG + n ))
    if [ "$PRUNE_VOLUMES" -eq 1 ]; then
        run "ssh '$h' 'docker volume prune -f' >/dev/null 2>&1"
    fi
done
if [ "$PRUNE_VOLUMES" -eq 1 ]; then
    record "孤儿卷" PASS "已回收（本次共 ${TOTAL_DANG} 个）"
elif [ "$TOTAL_DANG" -gt 0 ]; then
    record "孤儿卷" WARN "三台共 ${TOTAL_DANG} 个未回收；需要时加 --prune-volumes（不可撤销）"
else
    record "孤儿卷" PASS "无孤儿卷"
fi

# ---------- 8. 配置与 jar 一致性 ----------
step "8/8 配置与 jar 一致性 / configuration and jar consistency"
TM_IMG="$(ssh fa-worker1 "docker inspect taskmanager-2 --format '{{.Config.Image}}'" 2>/dev/null)"
echo "  TaskManager 镜像: ${TM_IMG:-?}（期望 ${FLINK_IMAGE_TAG:-fa-iforest/flink:1.13.6-java11}）"
if [ "$TM_IMG" = "${FLINK_IMAGE_TAG:-fa-iforest/flink:1.13.6-java11}" ]; then
    record "TM 镜像" PASS "$TM_IMG"
else
    record "TM 镜像" FAIL "实测 ${TM_IMG:-?} —— 跑 syn-sync-flink-image.sh --config-only 后重启"
fi

TM_PHYS="$(ssh fa-worker1 "docker inspect taskmanager-2 | grep -o 'maxphysicalbytes=[^ \"]*' | head -1 | cut -d= -f2" 2>/dev/null)"
echo "  TaskManager maxphysicalbytes: ${TM_PHYS:-?}（期望 ${SYN_JAVACPP_MAXPHYSICALBYTES:-3584m}）"
if [ "$TM_PHYS" = "${SYN_JAVACPP_MAXPHYSICALBYTES:-3584m}" ]; then
    record "JavaCPP 上限" PASS "$TM_PHYS"
else
    record "JavaCPP 上限" FAIL "实测 ${TM_PHYS:-?} 与 .env 不一致"
fi

# 本地预检：SYN_AKKA_FRAMESIZE 的单位写法必须是 Typesafe Config 能解析的。
# 【为何要这一条】akka.framesize 的值被**原样透传给 Akka**，由 Typesafe Config 解析，而不是 Flink 的
# MemorySize。Typesafe Config 接受 b/B/kB/K/k/KiB/MB/M/m/MiB/GB/G/g/GiB，**不接受小写 mb/kb/gb**；
# 而 Flink 自己的解析器接受 64mb，于是写错时在配置层面毫无征兆，直到 JobManager 启动时 Akka 抛出
# "Could not parse size-in-bytes unit 'mb'"、JM 直接起不来、REST 无响应为止（2026-09-19 实际发生过）。
# 这一项在本地就能判定，不必等部署到集群才发现。
# Local preflight: the value goes verbatim to Akka's Typesafe Config parser, which rejects lowercase
# "mb" while Flink's own MemorySize accepts it, so a bad unit only surfaces when the JobManager dies.
# 期望值只在这里算一次，下面的「写法校验」与「与容器实测比对」共用同一个变量。
# 【教训】这两处曾各写各的默认值（一处 67108864b、一处 64mb），于是在 .env 根本没有设这一项时，
# 脚本报出"与 .env 的 64mb 不一致"——而 .env 里连这行都没有，读者会去修一个不存在的问题。
# Single source of truth: the two checks previously carried different fallbacks, so with the key
# unset the script blamed a value that was nowhere in .env.
if [ -n "${SYN_AKKA_FRAMESIZE:-}" ]; then
    FRAME_CFG="$SYN_AKKA_FRAMESIZE"; FRAME_SRC=".env"
else
    FRAME_CFG="67108864b"; FRAME_SRC="compose 默认值"
fi
if printf '%s' "$FRAME_CFG" | grep -qE '^[0-9]+(b|B|byte|bytes|kB|kilobyte|kilobytes|K|k|Ki|KiB|kibibyte|kibibytes|MB|megabyte|megabytes|M|m|Mi|MiB|mebibyte|mebibytes|GB|gigabyte|gigabytes|G|g|Gi|GiB|gibibyte|gibibytes)$'; then
    record "framesize 写法" PASS "$FRAME_CFG 可被 Typesafe Config 解析（来自 ${FRAME_SRC}）"
else
    echo "  [!] SYN_AKKA_FRAMESIZE=$FRAME_CFG 的单位写法 Akka 解析不了。"
    record "framesize 写法" FAIL "$FRAME_CFG 写法不被接受或缺少单位 —— 改用纯字节写法（如 67108864b）；小写 mb/kb/gb 会让 JM 起不来"
fi

FRAME="$(ssh fa-master "curl -s --max-time 20 '$REST/jobmanager/config'" 2>/dev/null | python3 -c '
import json,sys
try:
    for e in json.load(sys.stdin):
        if e.get("key")=="akka.framesize": print(e.get("value")); break
    else: print("<未设置，取默认 10485760b>")
except Exception: print("?")' 2>/dev/null)"
echo "  JobManager akka.framesize: ${FRAME:-?}（期望 ${FRAME_CFG}，来自 ${FRAME_SRC}）"
case "${FRAME:-}" in
    "$FRAME_CFG") record "akka.framesize" PASS "${FRAME}（期望值来自 ${FRAME_SRC}）" ;;
    "?"|"") record "akka.framesize" SKIP "读不到 /jobmanager/config$([ "$REST_OK" -eq 0 ] && echo '（REST 不可达，见 JM REST 可达 一项）')" ;;
    *) record "akka.framesize" WARN "容器实测 ${FRAME}，与期望值 ${FRAME_CFG}（来自 ${FRAME_SRC}）不一致；改动需重建 JM 与两个 TM 才生效" ;;
esac

LOCAL_JAR="$PROJECT_ROOT/target/$JAR_NAME"
if [ -f "$LOCAL_JAR" ]; then
    LSHA="$(shasum -a 256 "$LOCAL_JAR" 2>/dev/null | awk '{print $1}')"
    [ -z "$LSHA" ] && LSHA="$(sha256sum "$LOCAL_JAR" 2>/dev/null | awk '{print $1}')"
    RSHA="$(ssh fa-master "sha256sum $RHOME/jars/$JAR_NAME 2>/dev/null | awk '{print \$1}'" 2>/dev/null)"
    echo "  jar SHA-256 本地: ${LSHA:0:16}…   集群: ${RSHA:0:16}…"
    if [ -n "$LSHA" ] && [ "$LSHA" = "$RSHA" ]; then
        record "jar 一致" PASS "SHA-256 一致"
    else
        record "jar 一致" FAIL "不一致 —— bash deploy/scripts/syn-upload-m1.sh --jar-only 后重跑"
    fi
else
    record "jar 一致" SKIP "本地 target/ 下没有 ${JAR_NAME}（先 mvn -DskipTests package）"
fi

# ---------- 核对表 ----------
echo ""
echo "===================================================================="
echo "  复位核对表 / reset checklist"
echo "===================================================================="
FAILED=0
for i in "${!CHECK_NAMES[@]}"; do
    st="${CHECK_STATES[$i]}"
    printf "  [%-4s] %-14s %s\n" "$st" "${CHECK_NAMES[$i]}" "${CHECK_NOTES[$i]}"
    [ "$st" = "FAIL" ] && FAILED=1
done
echo "===================================================================="
if [ "$DRY_RUN" -eq 1 ]; then
    echo "  dry-run 结束：以上为当前状态，未做任何改动。"
    exit 0
fi
if [ "$FAILED" -eq 0 ]; then
    echo "  ✅ 环境已复位，可以开始本阶段实验。"
    echo "     下一步通常是：syn-create-topics.sh（若 topic 被删后未重建）→ 提交作业 → 启动重放。"
    exit 0
else
    echo "  ⛔ 有项目未达标（见上方 FAIL）。**不要开跑**——在残留环境上跑出的结果不可用，"
    echo "     只会再浪费一轮时间。逐项解决后重跑本脚本。"
    exit 1
fi
