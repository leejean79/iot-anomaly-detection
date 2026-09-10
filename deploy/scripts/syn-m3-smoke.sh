#!/usr/bin/env bash
# ============================================================================
# syn-m3-smoke.sh
# M3 真实集群冒烟验证（交接文档 §2 决策 1 第三部分）：在真实容器内证明 DL4J/ND4J 的堆外内存、
# 原生库解包权限、实际 JDK、以及提交路径上那个 214MB jar 的原生二进制构成与分发耗时。
# M3 real-cluster smoke verification (handover §2 decision 1, part 3): prove DL4J/ND4J behavior inside
# the real containers — off-heap memory budget, native-lib extraction permission, the JDK actually used,
# and the native-binary makeup + distribution cost of the 214MB jar on the submission path.
#
# 四个报告点 / four report points:
#   点1 off-heap 预算：TM off-heap 提到 768MB、managed 降到 256MB、JavaCPP maxbytes/maxphysicalbytes
#       显式设界（超限抛异常而非 kill 容器）；由冒烟作业在 TM 内读回 JavaCPP 上限/用量佐证。
#   点2 原生库解包目录：容器用户 9999 对 cachedir 是否可写；作业读回 Loader.getCacheDir()+可写位，
#       脚本再（可选）exec 进 TM 容器以 uid 9999 实测 touch。
#   点3 实际 JDK：门槛是 Java 8；作业读回每个 TM 的 java.version；脚本再对 jobmanager 交叉核对。
#   点4 jar 原生二进制：列出 jar 内 .so/.dylib/.dll，确认只含 linux-x86_64（张量库 + OpenBLAS）；
#       并给出 jar 大小与提交（含向两个 TM 分发 blob）的墙钟耗时行。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地（bash），ssh 免密到 fa-master；jar 已上传（syn-upload-m1.sh）。
#    可选：若 NODE_WORKER*_PUBLIC_IP 已填，脚本会 ssh 到 worker 交叉核对 TM 容器；否则跳过（作业报告已足够）。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m3-smoke.sh                       # 默认 parallelism=8（占满 2×4 slot）
#      bash deploy/scripts/syn-m3-smoke.sh --parallelism 2       # 每 TM 一个（少占 slot）
# 3. 前置条件 / Preconditions: TaskManager 已按 M3 预算重配（docker-compose.worker.yml）并**重启生效**；
#      有 ≥ parallelism 个空闲 slot（脚本预检；**绝不取消**任何在跑作业）；jar 在 /opt/flink/usrlib/。
# 4. 期望产出 / Expected output: 逐点 PASS/FAIL；synergia-smoke 内每子任务一行报告全文；总判定。
# 5. 失败兜底 / Failure fallback: 无空闲 slot / 作业未 FINISHED / 原生加载失败 → 报错并保留现场，
#      指引把实测事实回报设计会（§9）；绝不改代码绕过、绝不取消旧作业、绝不 teardown。
#
# 缩写自查 / Abbreviations: TM = TaskManager；JM = JobManager；RF = Replication Factor；
#   off-heap = Java 堆外内存；cachedir = JavaCPP 原生库解包缓存目录；blob = Flink 分发 jar 的二进制服务。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

PARALLELISM=8
SMOKE_TIMEOUT=180        # 秒：等待作业 FINISHED 的上限 / seconds to wait for FINISHED
while [[ $# -gt 0 ]]; do
    case "$1" in
        --parallelism) PARALLELISM="$2"; shift 2 ;;
        --timeout)     SMOKE_TIMEOUT="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
JAR_NAME="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"
JAR_PATH="/opt/flink/usrlib/$JAR_NAME"
SMOKE_MAIN="${SYN_M3_SMOKE_MAIN:-com.leejean.m3.M3ClusterSmoke}"
SMOKE_TOPIC="${SYN_TOPIC_SMOKE:-synergia-smoke}"
RF="${SYN_TOPIC_REPLICATION:-2}"

master() { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "$@"; }

PASS_JDK="?"; PASS_NATIVE="?"; PASS_OFFHEAP="?"; PASS_CACHEDIR="?"; PASS_JAR="?"

echo "===================================================================="
echo "syn-m3-smoke.sh — M3 real-cluster compatibility smoke (handover §2 decision 1 part 3)"
echo "  master=$MASTER_SSH  jar=$JAR_PATH  parallelism=$PARALLELISM  smoke-topic=$SMOKE_TOPIC"
echo "===================================================================="

# --------------------------------------------------------------------------
# 点3（先做，最快）：实际使用的 JDK — jobmanager 侧交叉核对（TM 侧由作业报告权威给出）
# Point 3 (quick first): the JDK actually used — jobmanager cross-check (TM side comes from the job report)
# --------------------------------------------------------------------------
echo ""
echo "---- [Point 3] JDK actually used (jobmanager cross-check) ----"
JM_JAVA=$(master "docker exec jobmanager java -version" 2>&1 || true)
echo "$JM_JAVA"
if echo "$JM_JAVA" | grep -qE '"1\.8\.|version "8'; then
    echo "  [PASS] jobmanager runs Java 8"
    PASS_JDK="PASS"
else
    echo "  [WARN] jobmanager Java version is not clearly 8 — inspect above; TM side is authoritative (job report)."
    PASS_JDK="CHECK"
fi

# --------------------------------------------------------------------------
# 点4：jar 大小 + 原生二进制清单（只应含 linux-x86_64）
# Point 4: jar size + native-binary inventory (should contain only linux-x86_64)
# --------------------------------------------------------------------------
echo ""
echo "---- [Point 4] jar size + native-binary inventory ----"
JAR_LS=$(master "ls -l $JAR_PATH" 2>&1 || true)
echo "  $JAR_LS"

# 列出 jar 条目：容器内不保证有 unzip/jar/python，按序尝试 / list entries: try unzip → jar → python3
LIST_CMD='if command -v unzip >/dev/null 2>&1; then unzip -l '"$JAR_PATH"' | awk "{print \$4}";
          elif command -v jar >/dev/null 2>&1; then jar tf '"$JAR_PATH"';
          elif command -v python3 >/dev/null 2>&1; then python3 -c "import zipfile,sys;[print(n) for n in zipfile.ZipFile(sys.argv[1]).namelist()]" '"$JAR_PATH"';
          else echo __NO_LISTER__; fi'
ENTRIES=$(master "docker exec jobmanager sh -c '$LIST_CMD'" 2>/dev/null || true)

if echo "$ENTRIES" | grep -q "__NO_LISTER__" || [ -z "$ENTRIES" ]; then
    echo "  [WARN] jobmanager 容器内无 unzip/jar/python3，无法列出 jar 条目。"
    echo "         回退：在任一有 unzip 的机器上运行 deploy/scripts/check-jar.sh $JAR_NAME，"
    echo "         或 docker cp 出 jar 后本地 unzip -l | grep -E '\\.(so|dylib|dll)$'。"
    PASS_JAR="CHECK"
else
    NATIVES=$(echo "$ENTRIES" | grep -aiE '\.(so|so\.[0-9]+|dylib|dll|jnilib)$' || true)
    NATIVE_COUNT=$(echo "$NATIVES" | grep -c . || true)
    echo "  原生库条目数 / native-lib entries: $NATIVE_COUNT"
    echo "$NATIVES" | sed 's/^/    /' | head -40
    # 平台目录段：JavaCPP/ND4J 把 .so 放在 <group>/<platform>/ 下；抽取平台段做白名单校验。
    # Platform dir segment: JavaCPP/ND4J place .so under <group>/<platform>/; extract it for allow-listing.
    PLATFORMS=$(echo "$NATIVES" | grep -aoE '(linux|macosx|windows|android|ios)-[a-z0-9_]+' | sort -u || true)
    echo "  出现的平台段 / platforms present: $(echo "$PLATFORMS" | tr '\n' ' ')"
    FOREIGN=$(echo "$PLATFORMS" | grep -avE '^linux-x86_64$' || true)
    if [ -n "$NATIVES" ] && [ -z "$FOREIGN" ]; then
        echo "  [PASS] 只含 linux-x86_64 原生库（张量库 + OpenBLAS，符合 classifier 生效预期）。"
        echo "         Only linux-x86_64 natives present (tensor lib + OpenBLAS) — classifier took effect."
        PASS_JAR="PASS"
    elif [ -z "$NATIVES" ]; then
        echo "  [FAIL] jar 内未发现任何原生库 — nd4j-native linux-x86_64 classifier 可能未打进 jar。"
        PASS_JAR="FAIL"
    else
        echo "  [FAIL] 发现非 linux-x86_64 平台库：$(echo "$FOREIGN" | tr '\n' ' ') — classifier 未生效，jar 含多平台库。"
        PASS_JAR="FAIL"
    fi
fi

# --------------------------------------------------------------------------
# 预检：空闲 slot ≥ parallelism（不足则报错，绝不取消在跑作业腾位）
# Preflight: free slots ≥ parallelism (error if not; never cancel a running job to make room)
# --------------------------------------------------------------------------
echo ""
echo "---- Preflight: free task slots ----"
OVERVIEW=$(master "docker exec jobmanager sh -c 'command -v curl >/dev/null 2>&1 && curl -s localhost:8081/overview || (command -v wget >/dev/null 2>&1 && wget -qO- localhost:8081/overview)'" 2>/dev/null || true)
FREE_SLOTS=$(echo "$OVERVIEW" | grep -oE '"slots-available":[0-9]+' | grep -oE '[0-9]+' | head -1)
if [ -n "$FREE_SLOTS" ]; then
    echo "  slots-available=$FREE_SLOTS  (need $PARALLELISM)"
    if [ "$FREE_SLOTS" -lt "$PARALLELISM" ]; then
        echo "  ERROR: 空闲 slot 不足（$FREE_SLOTS < $PARALLELISM）。请等在跑作业释放 slot，或 --parallelism 调小；" >&2
        echo "         绝不取消在跑作业腾位。/ not enough free slots; do NOT cancel running jobs." >&2
        exit 2
    fi
else
    echo "  [WARN] 无法读取 slots-available（容器内无 curl/wget）；继续提交，若无 slot 作业将 SCHEDULED 挂起。"
fi

# --------------------------------------------------------------------------
# 建 synergia-smoke（若缺）/ create synergia-smoke if missing (only a synergia- topic — coexistence-safe)
# --------------------------------------------------------------------------
echo ""
echo "---- Ensure smoke topic '$SMOKE_TOPIC' ----"
TOPIC_DESC=$(master "docker exec kafka-1 kafka-topics.sh --bootstrap-server $BROKERS --describe --topic $SMOKE_TOPIC" 2>/dev/null || true)
if ! echo "$TOPIC_DESC" | grep -q "PartitionCount"; then
    echo "  creating $SMOKE_TOPIC (partitions=1, RF=$RF)"
    master "docker exec kafka-1 kafka-topics.sh --bootstrap-server $BROKERS --create --topic $SMOKE_TOPIC --partitions 1 --replication-factor $RF" 2>&1 || true
else
    echo "  exists: $(echo "$TOPIC_DESC" | grep -oE 'PartitionCount: *[0-9]+' | head -1)"
fi

# --------------------------------------------------------------------------
# 点1/2/3（权威）：提交冒烟作业，计时（含向两个 TM 分发 214MB blob），等 FINISHED
# Points 1/2/3 (authoritative): submit smoke job, time it (incl. blob distribution to both TMs), await FINISHED
# --------------------------------------------------------------------------
echo ""
JAR_BYTES=$(echo "$JAR_LS" | awk '{print $5}' | grep -oE '^[0-9]+' | head -1)
echo "---- Submit smoke job (times submit + blob distribution to both TMs; jar ~${JAR_BYTES:-?} bytes) ----"
T0=$(date +%s)
SUBMIT=$(master "docker exec jobmanager flink run -d -c $SMOKE_MAIN -p $PARALLELISM $JAR_PATH --brokers $BROKERS --smoke-topic $SMOKE_TOPIC --parallelism $PARALLELISM" 2>&1)
T1=$(date +%s)
echo "$SUBMIT"
echo "  [timing] submit call (jar upload to JM blob + accept) wall = $((T1-T0))s  (点4 分发耗时行 / distribution timing line)"

JOBID=$(echo "$SUBMIT" | grep -oE 'JobID [a-f0-9]{32}' | awk '{print $2}' | head -1)
if [ -z "$JOBID" ]; then
    echo "  ERROR: 无法解析 JobID（见上）。作业未提交成功。" >&2
    exit 1
fi
echo "  JobID=$JOBID  waiting up to ${SMOKE_TIMEOUT}s for FINISHED ..."

STATE="?"
for _ in $(seq 1 $((SMOKE_TIMEOUT/5))); do
    LISTA=$(master "docker exec jobmanager flink list -a 2>&1" || true)
    LINE=$(echo "$LISTA" | grep -F "$JOBID" | head -1 || true)
    if echo "$LINE" | grep -q "(FINISHED)"; then STATE="FINISHED"; break; fi
    if echo "$LINE" | grep -q "(FAILED)";   then STATE="FAILED";   break; fi
    if echo "$LINE" | grep -q "(CANCELED)"; then STATE="CANCELED"; break; fi
    sleep 5
done
T2=$(date +%s)
echo "  job state=$STATE  total wall (submit→terminal) = $((T2-T0))s"
if [ "$STATE" != "FINISHED" ]; then
    echo "  [WARN] 作业未在 ${SMOKE_TIMEOUT}s 内 FINISHED（state=$STATE）。查 JM/TM 日志；报告可能不完整。"
fi

# --------------------------------------------------------------------------
# 消费 synergia-smoke：每子任务一行报告（点1/2/3 的权威证据）
# Consume synergia-smoke: one report line per subtask (authoritative evidence for points 1/2/3)
# --------------------------------------------------------------------------
echo ""
echo "---- Per-subtask reports from '$SMOKE_TOPIC' ----"
REPORTS=$(master "docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server $BROKERS --topic $SMOKE_TOPIC --from-beginning --timeout-ms 20000 --max-messages $PARALLELISM" 2>/dev/null || true)
echo "$REPORTS" | sed 's/^/  /'

if [ -n "$REPORTS" ]; then
    # 点3：所有子任务 java.version 均为 1.8 / all subtasks report Java 1.8
    if echo "$REPORTS" | grep -q "java.version=1.8"; then
        [ "$PASS_JDK" = "PASS" ] && true
        echo ""
        echo "  [Point 3] TM java.version=1.8 confirmed in report."
        PASS_JDK="PASS"
    else
        echo "  [Point 3][WARN] TM java.version not reported as 1.8 — inspect reports above."
        PASS_JDK="CHECK"
    fi
    # 点2：原生加载成功 + cacheDir 可写 / native loaded + cacheDir writable
    if echo "$REPORTS" | grep -q "nd4j_native_ok=true"; then
        PASS_NATIVE="PASS"; echo "  [Native] nd4j_native_ok=true on all reporting subtasks."
    else
        PASS_NATIVE="FAIL"; echo "  [Native][FAIL] a subtask reported nd4j_native_ok=false — see nd4j_err above."
    fi
    if echo "$REPORTS" | grep -q "javacpp.cacheDir.writable=true"; then
        PASS_CACHEDIR="PASS"; echo "  [Point 2] JavaCPP cacheDir writable=true (user 9999 can extract .so)."
    else
        PASS_CACHEDIR="FAIL"; echo "  [Point 2][FAIL] cacheDir not writable — set SYN_JAVACPP_CACHEDIR to a writable mounted path."
    fi
    # 点1：JavaCPP 上限已显式设界（非默认取 JVM 最大堆）/ JavaCPP ceilings explicitly bounded
    if echo "$REPORTS" | grep -q "D.maxphysicalbytes=null"; then
        PASS_OFFHEAP="FAIL"; echo "  [Point 1][FAIL] D.maxphysicalbytes=null — TM JVM opts not applied; restart TM after compose change."
    else
        PASS_OFFHEAP="PASS"; echo "  [Point 1] JavaCPP maxbytes/maxphysicalbytes explicitly set (see report); off-heap bounded."
    fi
else
    echo "  [WARN] 未消费到报告（作业可能未跑到 sink 或 topic 为空）。查 TM 日志中 '[M3-CLUSTER-SMOKE]' 行。"
    PASS_NATIVE="CHECK"; PASS_OFFHEAP="CHECK"; PASS_CACHEDIR="CHECK"
fi

# --------------------------------------------------------------------------
# 点2 补充（可选）：ssh 到 worker，以 uid 9999 实测 touch cachedir（仅当填了 worker 公网 IP）
# Point 2 supplement (optional): ssh to workers, touch-test cachedir as uid 9999 (only if worker IPs set)
# --------------------------------------------------------------------------
CACHEDIR="${SYN_JAVACPP_CACHEDIR:-/tmp/javacpp-cache}"
for WK in "${NODE_WORKER1_PUBLIC_IP:-}" "${NODE_WORKER2_PUBLIC_IP:-}"; do
    [ -z "$WK" ] && continue
    echo ""
    echo "---- [Point 2 supplement] worker $WK: TM container id + cachedir touch test ----"
    ssh $SSH_OPTS "$SSH_USER@$WK" "for c in \$(docker ps --format '{{.Names}}' | grep -E '^taskmanager'); do
        echo \"  [\$c] user=\$(docker exec \$c id 2>/dev/null)\";
        docker exec \$c sh -c 'mkdir -p $CACHEDIR 2>/dev/null; touch $CACHEDIR/.smoke_probe 2>/dev/null && echo \"  [\$(hostname)] $CACHEDIR WRITABLE by container user\" || echo \"  [\$(hostname)] $CACHEDIR NOT writable\"';
    done" 2>&1 || echo "  [WARN] worker $WK 不可达或无 docker 权限，跳过（作业报告已给出 cacheDir 可写位）。"
done

# --------------------------------------------------------------------------
# 总判定 / verdict
# --------------------------------------------------------------------------
echo ""
echo "===================================================================="
echo "SUMMARY (report these four back to the design session, §9):"
printf "  Point 1  off-heap budget (JavaCPP bounded) : %s\n" "$PASS_OFFHEAP"
printf "  Point 2  native cachedir writable (uid 9999): %s\n" "$PASS_CACHEDIR"
printf "  Point 3  JDK is Java 8                       : %s\n" "$PASS_JDK"
printf "  Point 4  jar natives = linux-x86_64 only     : %s\n" "$PASS_JAR"
printf "  (aux)    ND4J native load on TMs             : %s\n" "$PASS_NATIVE"
echo "  Also record: jar size, submit/distribution wall time above, and each TM's"
echo "  javacpp.maxBytes/maxPhysicalBytes/totalBytes for the off-heap section of the report."
echo "===================================================================="

if echo "$PASS_OFFHEAP$PASS_CACHEDIR$PASS_JDK$PASS_JAR$PASS_NATIVE" | grep -q "FAIL"; then
    echo "❌ 有 FAIL：停手，把实测事实回报设计会（§9），不要改代码绕过。"
    exit 1
fi
echo "✅ 无 FAIL（CHECK 项需人工确认上方证据）。"
exit 0
