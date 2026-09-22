#!/usr/bin/env bash
# ============================================================================
# syn-m3-grid.sh
# V-M3-3 离线超参数网格：把 synergia-m1-out 与 synergia-scores 转储下来，在 master 的临时容器里
# 运行 com.leejean.m3.M3Grid，对三台代表设备逐一训练隐藏层大小 × 窗口长度的每一种组合，
# 输出早停集误差、实际训练轮数（epoch）与耗时，最后把 CSV 拉回本地。
# Offline hyper-parameter grid for V-M3-3: dump m1-out and scores, run M3Grid in a transient
# container on master, pull the CSV back.
#
# 【为什么要两份转储】在线算子从 AnnotatedRound 上直接读到 M2 给每一轮打的离群标记，据此把含离群轮
# 的整窗从训练集剔除（训练净化）。而 m1-out 里装的是 DeviceRound，**不带这个标记**。因此还要转储
# synergia-scores——它列出所有被判为离群的 (设备, 轮时间戳)，据此可以还原标记，使离线口径与在线一致。
# 不转 scores 也能跑，但训练净化会被跳过，结果偏乐观，CSV 的 sanitized 列会记为 false。
# Why two dumps: the outlier flag the online operator sanitizes on lives in AnnotatedRound, not in
# the m1-out DeviceRound; synergia-scores restores it.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash），仓库根目录；ssh 主机别名 fa-master 可用；
#      master 上有 Java 11 的 flink 镜像与本项目的 jar。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m3-grid.sh
#      bash deploy/scripts/syn-m3-grid.sh --devices E,G,C --hidden-grid 40,60,90 --window-grid 30,60,120
#      bash deploy/scripts/syn-m3-grid.sh --no-scores        # 不转储 scores，跳过训练净化
#      bash deploy/scripts/syn-m3-grid.sh --reuse-dump       # 复用上次转储，只重跑网格
#      bash deploy/scripts/syn-m3-grid.sh --detach           # 后台跑，训练与本地 ssh 连接脱钩
#      bash deploy/scripts/syn-m3-grid.sh --collect          # 取回后台任务的状态与结果
#    参数 / Arguments:
#      --devices <列表>        代表设备，默认 E,G,C（E 在带内、G 被掩码、C 结构中等）
#      --hidden-grid <列表>    隐藏层大小网格，默认 40,60,90
#      --window-grid <列表>    窗口长度网格，默认 30,60,120
#      --train-days <n>        训练段天数，默认 7
#      --early-stop-days <n>   早停段天数，默认 2
#      --max-epochs <n>        单次训练的上限轮数，默认 60（补遗三 §1 定的生产值）
#      --batch-grid <列表>     小批量大小网格，即一次权重更新用到多少个窗口，默认 "1"。
#                              默认 1 即 2026-09-21 参照点的口径；步骤 A 传 "1,16,32,64" 一次跑完。
#      --no-reverse-target     关闭逆序重构目标（默认开启）。仅供消融实验；关闭后与在线算子的
#                              默认值不一致，等值核验会失效，不可用于正式网格。
#      --reference-loss <v>    参照早停集误差。给出后 CSV 的 relDeltaVsRef 列写出相对偏差，
#                              并在解读段按 5% 判据给出选型建议。步骤 A 传 0.160610。
#      --node <名称>           在哪台机器上跑：master（默认）、worker1、worker2。
#                              **不做静默回落**：指定哪台就是哪台，不合格直接报错退出，
#                              否则事后无法确认某一行读数究竟出自哪台机器。
#      --container-mb <n>      容器内存上限（MB）。缺省时 master 取 1500、worker 取 2048。
#                              JVM 的 -Xmx 取其三分之一，JavaCPP 堆外上限取其 45%。
#      --probe-nodes           只探测三台节点是否具备跑网格的条件并打印结论，不跑任何训练。
#      --omp-threads <n>       容器内 OpenMP 线程数，默认 1。经 OMP_NUM_THREADS 环境变量传入，
#                              ND4J 启动日志里的 "Number of threads used for OpenMP BLAS" 是权威确认。
#      --patience <n>          早停耐心，默认 10
#      --max-messages <n>      每个 topic 的转储条数上限，默认 3000000
#      --out-name <文件名>     本地 CSV 文件名，默认 m3_grid.csv
#      --no-scores             不转储 scores（跳过训练净化）
#      --reuse-dump            复用 master 上已有的转储
#      --detach                把训练容器交给 master 的 Docker 守护进程后台托管，启动后立即返回。
#                              适用于耗时数小时以上、本地 Mac 会休眠或需要关机的场合。
#      --collect               查询后台任务状态。仍在运行时打印启动时刻、容器 CPU 与内存占用、
#                              master 的内存余量，以及最近 12 行逐 epoch 进度；已成功结束则拉回
#                              CSV 并清理容器；失败则打印退出码、是否被内核内存杀手终止
#                              （OOMKilled）、起止时刻与完整日志，保留容器供排查，并以其退出码结束。
#
# 【为什么需要后台模式】默认跑法用 ssh 前台附着执行 docker run，本地 Mac 一旦休眠，ssh 断开，
# 脚本拿到的是 ssh 的断线退出码而不是 M3Grid 的退出码，于是判定失败并拒绝拉回 CSV；远端容器是否
# 继续运行也无从保证。--detach 把容器交给 Docker 守护进程，训练从此不依赖本地连接；守护进程会
# 保留容器的退出码与完整日志，--collect 据此判读，不丢失任何依据。
# Why a detached mode: the default run attaches over ssh, so a sleeping laptop breaks the run.
# 3. 前置条件 / Preconditions: synergia-m1-out 与 synergia-scores 已含目标月份的数据
#      （即 M2 联合作业已跑过该段重放），且 topic 尚未被清理。
# 4. 期望产出 / Expected output: stdout 逐组合打印进度与解读；本地 docs/<out-name>；
#      每一行含 device,hiddenSize,windowLength,batchSize,ompThreads,trainWindows,trainExcluded,
#      esWindows,epochs,esLoss,relDeltaVsRef,esLossClean,esLossOutlier,separationRatio,
#      trainSeconds,secPerEpoch,sanitized。
#      其中 separationRatio 是诊断列：早停集中含离群轮的窗口平均误差 ÷ 不含离群轮的窗口平均误差。
#      **只作诊断、不作判据**；接近 1 说明模型在无差别抄写输入，该行其余读数存疑。
#      使用 --detach 时，本次调用只打印容器名与后续命令，CSV 要等 --collect 成功之后才出现在本地。
# 5. 常见失败兜底 / Failure fallback:
#      「转储里没有任何一台目标设备的可用轮」→ 多半是 --max-messages 不足以覆盖标定期
#      （预热轮会被整段跳过，7 天标定 = 483,840 轮），提高它后重跑；
#      「样本不足，跳过」→ 该设备在训练段或早停段没切出窗口，检查转储是否覆盖足够天数；
#      --collect 报「找不到容器」→ 后台任务从未启动，或已被 --collect 成功回收过一次，
#      前者重新执行 --detach，后者结果已在 docs/<out-name>。
#
# 缩写自查 / Abbreviations: epoch = 训练时在整个训练集上完整跑一遍；
#   早停（early stopping）= 早停集误差不再下降时提前结束训练；CSV = 逗号分隔值。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

DEVICES="E,G,C"; HIDDEN_GRID="40,60,90"; WINDOW_GRID="30,60,120"
TRAIN_DAYS=7; ES_DAYS=2; MAX_EPOCHS=60; PATIENCE=10
BATCH_GRID="1"; OMP_THREADS=1; REFERENCE_LOSS=""; NODE="master"; CONTAINER_MB=""; PROBE_ONLY=0
REVERSE_TARGET=1
MAX_MESSAGES=3000000; OUT_NAME="m3_grid.csv"; USE_SCORES=1; REUSE=0; DETACH=0; COLLECT=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --devices) DEVICES="$2"; shift 2 ;;
        --hidden-grid) HIDDEN_GRID="$2"; shift 2 ;;
        --window-grid) WINDOW_GRID="$2"; shift 2 ;;
        --train-days) TRAIN_DAYS="$2"; shift 2 ;;
        --early-stop-days) ES_DAYS="$2"; shift 2 ;;
        --max-epochs) MAX_EPOCHS="$2"; shift 2 ;;
        --batch-grid) BATCH_GRID="$2"; shift 2 ;;
        --omp-threads) OMP_THREADS="$2"; shift 2 ;;
        --reference-loss) REFERENCE_LOSS="$2"; shift 2 ;;
        --no-reverse-target) REVERSE_TARGET=0; shift ;;
        --node) NODE="$2"; shift 2 ;;
        --container-mb) CONTAINER_MB="$2"; shift 2 ;;
        --probe-nodes) PROBE_ONLY=1; shift ;;
        --patience) PATIENCE="$2"; shift 2 ;;
        --max-messages) MAX_MESSAGES="$2"; shift 2 ;;
        --out-name) OUT_NAME="$2"; shift 2 ;;
        --no-scores) USE_SCORES=0; shift ;;
        --reuse-dump) REUSE=1; shift ;;
        --detach) DETACH=1; shift ;;
        --collect) COLLECT=1; shift ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
JAR_NAME="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"
RHOME="${REMOTE_HOME:-/opt/fa-iforest}"
WORK="${RHOME}/m3grid"
RUN_IMAGE="${FLINK_IMAGE_TAG:-fa-iforest/flink:1.13.6-java11}"

CONTAINER_NAME="syn-m3grid"

# 探测一台节点是否具备跑网格的条件，逐项打印。只读，不改动任何东西。
# Probe one node for grid-readiness; read-only.
probe_node() {
    local host="$1"
    echo "  ── ${host} ──"
    if ! ssh -o ConnectTimeout=8 -o BatchMode=yes "$host" true 2>/dev/null; then
        echo "     [FAIL] ssh 不可达"
        return 1
    fi
    echo "     [OK]   ssh 可达"
    local rc=0
    if ssh "$host" "test -f ${RHOME}/jars/${JAR_NAME}" 2>/dev/null; then
        echo "     [OK]   jar 已就位：${RHOME}/jars/${JAR_NAME}"
    else
        echo "     [FAIL] 缺少 jar：${RHOME}/jars/${JAR_NAME}（用 syn-upload-m1.sh 传过去）"
        rc=1
    fi
    if ssh "$host" "docker image inspect ${RUN_IMAGE} >/dev/null 2>&1" 2>/dev/null; then
        echo "     [OK]   镜像已就位：${RUN_IMAGE}"
    else
        echo "     [FAIL] 缺少镜像：${RUN_IMAGE}（用 syn-sync-flink-image.sh 同步）"
        rc=1
    fi
    local avail
    avail="$(ssh "$host" "free -m | awk '/^Mem:/{print \$7}'" 2>/dev/null | tr -d '[:space:]')"
    if [ -n "${avail:-}" ] && [ "$avail" -ge 1600 ]; then
        echo "     [OK]   可用内存 ${avail} MB"
    else
        echo "     [WARN] 可用内存仅 ${avail:-未知} MB，低于 1600 MB 的建议下限"
    fi
    # Flink 作业占着这台机器时不宜再压训练进去：两者会争抢仅有的两个核。
    # A running Flink job and the grid would contend for the same two cores.
    local slots
    slots="$(ssh "$host" "docker ps --format '{{.Names}}' | grep -c taskmanager" 2>/dev/null | tr -d '[:space:]')"
    echo "     [INFO] 本机 taskmanager 容器数 ${slots:-0}"
    return $rc
}

if [ "$PROBE_ONLY" -eq 1 ]; then
    echo "===================================================================="
    echo "节点探测（只读）：检查各节点是否具备跑离线网格的条件"
    echo "===================================================================="
    probe_node fa-master || true
    probe_node fa-worker1 || true
    probe_node fa-worker2 || true
    echo ""
    echo "另需确认集群上没有正在运行的 Flink 作业（网格与作业会争抢 CPU）："
    echo "  curl -s http://${NODE_MASTER_IP}:8081/jobs/overview | python3 -m json.tool | grep -c RUNNING"
    echo ""
    echo "选定之后用 --node <名称> 指定；本脚本**不做静默回落**，"
    echo "指定的节点不合格即报错退出，以免事后分不清某行读数出自哪台机器。"
    exit 0
fi

case "$NODE" in
    master)  RUN_HOST="fa-master";  DEFAULT_MB=1500 ;;
    worker1) RUN_HOST="fa-worker1"; DEFAULT_MB=2048 ;;
    worker2) RUN_HOST="fa-worker2"; DEFAULT_MB=2048 ;;
    *) echo "ERROR: --node 只能是 master、worker1 或 worker2，收到 ${NODE}" >&2; exit 1 ;;
esac
MEM_MB="${CONTAINER_MB:-$DEFAULT_MB}"
# 三者都必须显式设死（补遗三 §5）。容器上限之外，JVM 堆与 JavaCPP 的堆外分配若不设上限，
# 两者叠加越过容器上限时进程会被内核直接杀死，而且不留任何 Java 侧的痕迹。
# All three must be pinned: heap plus JavaCPP off-heap can otherwise exceed the container cap and
# the process is killed with nothing logged on the Java side.
XMX_MB=$(( MEM_MB / 3 ))
JAVACPP_MB=$(( MEM_MB * 45 / 100 ))
LOCAL_CSV="${PROJECT_ROOT}/docs/${OUT_NAME}"

if [ "$DETACH" -eq 1 ] && [ "$COLLECT" -eq 1 ]; then
    echo "ERROR: --detach 与 --collect 不能同时使用；前者启动后台任务，后者取回其结果。" >&2
    exit 1
fi

# 把 master 上的 CSV 拉回本地。成功返回 0，失败删掉半截文件并返回 1。
# Pull the CSV back from master; on failure remove the partial file.
pull_csv() {
    if ssh "$RUN_HOST" "cat ${WORK}/m3_grid.csv" > "$LOCAL_CSV" 2>/dev/null && [ -s "$LOCAL_CSV" ]; then
        echo "[grid] 已拉回本地：${LOCAL_CSV}"
        return 0
    fi
    rm -f "$LOCAL_CSV" 2>/dev/null || true
    echo "[grid] 拉回失败，可手动：ssh ${RUN_HOST} 'cat ${WORK}/m3_grid.csv' > docs/${OUT_NAME}" >&2
    return 1
}

if [ "$COLLECT" -eq 1 ]; then
    # 一次 inspect 取齐判读所需的全部字段，避免多次往返导致读到不一致的快照。
    # One inspect for every field needed, so the snapshot is self-consistent.
    STATE="$(ssh "$RUN_HOST" "docker inspect -f '{{.State.Status}}|{{.State.ExitCode}}|{{.State.OOMKilled}}|{{.State.StartedAt}}|{{.State.FinishedAt}}|{{.State.Error}}' ${CONTAINER_NAME} 2>/dev/null" || true)"
    STATUS="$(echo "$STATE" | cut -d'|' -f1)"
    CODE="$(echo "$STATE" | cut -d'|' -f2)"
    OOM="$(echo "$STATE" | cut -d'|' -f3)"
    STARTED="$(echo "$STATE" | cut -d'|' -f4)"
    FINISHED="$(echo "$STATE" | cut -d'|' -f5)"
    DERR="$(echo "$STATE" | cut -d'|' -f6)"
    if [ -z "${STATUS:-}" ]; then
        echo "ERROR: master 上找不到容器 ${CONTAINER_NAME}。" >&2
        echo "       要么后台任务从未启动（重新执行 --detach），" >&2
        echo "       要么上一次 --collect 已经成功回收过它（结果应已在 docs/ 下）。" >&2
        exit 4
    fi
    if [ "$STATUS" = "running" ]; then
        echo "===================== 后台任务仍在运行 ====================="
        echo "  启动时刻：${STARTED}"
        echo "  当前资源占用（CPU 接近 100% 属正常，单线程 BLAS 占满一个核）："
        ssh "$RUN_HOST" "docker stats --no-stream --format '  CPU {{.CPUPerc}}   内存 {{.MemUsage}} ({{.MemPerc}})   进程数 {{.PIDs}}' ${CONTAINER_NAME}" || true
        echo "  master 节点内存余量："
        ssh "$RUN_HOST" "free -h | sed -n '1,2p' | sed 's/^/    /'" || true
        echo ""
        echo "  最近的训练进度（每个 epoch 一行）："
        ssh "$RUN_HOST" "docker logs --tail 12 ${CONTAINER_NAME} 2>&1 | sed 's/^/    /'" || true
        echo ""
        echo "  稍后重新执行：bash deploy/scripts/syn-m3-grid.sh --collect --out-name ${OUT_NAME}"
        echo "  实时跟随日志：ssh ${RUN_HOST} 'docker logs -f ${CONTAINER_NAME}'"
        exit 0
    fi
    echo "===================== 后台任务已结束 ====================="
    echo "  退出码：${CODE}"
    echo "  是否被内核内存杀手终止（OOMKilled）：${OOM}"
    echo "  启动 ${STARTED}   结束 ${FINISHED}"
    [ -n "${DERR:-}" ] && echo "  Docker 记录的错误：${DERR}"
    echo "  master 节点当前内存余量："
    ssh "$RUN_HOST" "free -h | sed -n '1,2p' | sed 's/^/    /'" || true
    echo ""
    echo "  完整日志如下："
    ssh "$RUN_HOST" "docker logs ${CONTAINER_NAME} 2>&1" || true
    if [ "${CODE:-1}" -ne 0 ]; then
        echo "" >&2
        echo "ERROR: M3Grid 退出码 ${CODE}——网格未完成，**不拉回 CSV**（避免留下不完整的产物）。" >&2
        if [ "${CODE}" = "137" ] || [ "${OOM}" = "true" ]; then
            echo "       退出码 137 或 OOMKilled=true 表示进程被 SIGKILL 终止，通常是内存不足；" >&2
            echo "       请核对上面的内存余量，以及日志里每个 epoch 末尾的「进程驻留」走向。" >&2
        fi
        echo "       日志末尾若**没有**「进程正在退出」那一行，说明它是被直接杀死而非自行退出。" >&2
        echo "       容器 ${CONTAINER_NAME} 已保留供排查；排查完毕后手动清理：" >&2
        echo "       ssh ${RUN_HOST} 'docker rm ${CONTAINER_NAME}'" >&2
        # M3Grid 每完成一个组合就落盘一次，因此中止时 master 上可能留有**部分**结果。
        # 它以 .partial 后缀单独拉回，绝不冒充完整产物。
        # M3Grid saves after each combination, so a partial CSV may exist; pull it under a .partial
        # name so it can never be mistaken for the complete artifact.
        if ssh "$RUN_HOST" "cat ${WORK}/m3_grid.csv" > "${LOCAL_CSV}.partial" 2>/dev/null \
                && [ "$(wc -l < "${LOCAL_CSV}.partial")" -gt 1 ]; then
            echo "       已完成的组合并未丢失：部分结果拉回到 ${LOCAL_CSV}.partial" >&2
            echo "       （共 $(($(wc -l < "${LOCAL_CSV}.partial") - 1)) 行，**不是**完整网格）" >&2
        else
            rm -f "${LOCAL_CSV}.partial" 2>/dev/null || true
        fi
        exit "$CODE"
    fi
    pull_csv || exit 1
    ssh "$RUN_HOST" "docker rm ${CONTAINER_NAME} >/dev/null 2>&1" || true
    echo "提醒：本阶段**不定终值**——网格表交设计会话裁决 (hidden, window)。"
    exit 0
fi

echo "===================================================================="
echo "syn-m3-grid.sh — V-M3-3 离线超参数网格"
echo "  设备 ${DEVICES}   隐藏层 ${HIDDEN_GRID}   窗口长度 ${WINDOW_GRID}"
echo "  训练 ${TRAIN_DAYS} 天 / 早停 ${ES_DAYS} 天   maxEpochs=${MAX_EPOCHS} patience=${PATIENCE}"
echo "  小批量大小 ${BATCH_GRID}   OpenMP 线程 ${OMP_THREADS}"
echo "  运行节点 ${RUN_HOST}   容器上限 ${MEM_MB} MB（-Xmx ${XMX_MB}m，JavaCPP ${JAVACPP_MB}m）"
[ -n "$REFERENCE_LOSS" ] && echo "  参照早停集误差 ${REFERENCE_LOSS}（按 5% 判据给出小批量选型建议）"
echo "  训练净化：$([ "$USE_SCORES" -eq 1 ] && echo '开启（转储 scores 还原离群标记）' || echo '关闭')"
echo "===================================================================="

if [ "$REUSE" -eq 0 ]; then
    echo "[grid] 转储 synergia-m1-out（最多 ${MAX_MESSAGES} 条）…"
    ssh fa-master "mkdir -p ${WORK} && chmod 777 ${WORK} && docker exec kafka-1 kafka-console-consumer.sh \
        --bootstrap-server ${BROKERS} --topic ${SYN_TOPIC_M1_OUT:-synergia-m1-out} \
        --from-beginning --max-messages ${MAX_MESSAGES} --timeout-ms 60000 > ${WORK}/m1out.jsonl 2>/dev/null || true"
    if [ "$USE_SCORES" -eq 1 ]; then
        echo "[grid] 转储 synergia-scores（用于还原离群标记）…"
        ssh fa-master "docker exec kafka-1 kafka-console-consumer.sh \
            --bootstrap-server ${BROKERS} --topic ${SYN_TOPIC_SCORES:-synergia-scores} \
            --from-beginning --max-messages ${MAX_MESSAGES} --timeout-ms 60000 > ${WORK}/scores.jsonl 2>/dev/null || true"
    fi
fi

LINES="$(ssh fa-master "wc -l < ${WORK}/m1out.jsonl 2>/dev/null || echo 0" | tr -d '[:space:]')"
if [ "${LINES:-0}" -eq 0 ]; then
    echo "ERROR: ${WORK}/m1out.jsonl 为空——synergia-m1-out 无数据。" >&2
    echo "       请先跑一次重放让 M2 联合作业把目标月份写进 m1-out，再执行本脚本。" >&2
    exit 2
fi
echo "[grid] m1-out 转储 ${LINES} 行。"

SCORES_ARG=""
if [ "$USE_SCORES" -eq 1 ]; then
    SLINES="$(ssh fa-master "wc -l < ${WORK}/scores.jsonl 2>/dev/null || echo 0" | tr -d '[:space:]')"
    echo "[grid] scores 转储 ${SLINES:-0} 行。"
    if [ "${SLINES:-0}" -gt 0 ]; then
        SCORES_ARG="--scores-jsonl /work/scores.jsonl"
    else
        echo "[grid] scores 转储为空——将**跳过训练净化**，结果偏乐观，CSV 的 sanitized 列记为 false。"
    fi
fi

# 启动前删掉上一次留下的 CSV。否则本次若在写出 CSV 之前就中止，--collect 会把**上一次的旧结果**
# 当成本次产物拉回来，造成一次静默的误判。删掉之后，CSV 存在即意味着本次确实写出了结果。
# Remove any stale CSV first: otherwise a run that dies before writing one would let --collect pull
# the PREVIOUS run's file back and pass it off as this run's result.
# 转储是在 master 上产生的（Kafka 命令行工具在那台机器的容器里）。若训练要在 worker 上跑，
# 先把转储搬过去。优先让 master 直接推给 worker；master 之间若没打通免密，再退回经本机中转。
# The dump is produced on master; copy it to the worker, preferring a direct master→worker push.
if [ "$RUN_HOST" != "fa-master" ]; then
    DUMP_MB="$(ssh fa-master "du -m ${WORK}/m1out.jsonl | cut -f1" 2>/dev/null | tr -d '[:space:]')"
    echo "[grid] 训练将在 ${RUN_HOST} 上进行，先搬运转储（m1out.jsonl 约 ${DUMP_MB:-未知} MB）…"
    ssh "$RUN_HOST" "mkdir -p ${WORK} && chmod 777 ${WORK}"
    for f in m1out.jsonl scores.jsonl; do
        if ! ssh fa-master "test -f ${WORK}/${f}" 2>/dev/null; then
            continue
        fi
        if ssh fa-master "scp -o BatchMode=yes -o StrictHostKeyChecking=no ${WORK}/${f} ${RUN_HOST}:${WORK}/" 2>/dev/null; then
            echo "[grid]   ${f} 已由 master 直接推送到 ${RUN_HOST}"
        else
            echo "[grid]   master 无法直连 ${RUN_HOST}，改为经本机中转（较慢）…"
            ssh fa-master "cat ${WORK}/${f}" | ssh "$RUN_HOST" "cat > ${WORK}/${f}"
            echo "[grid]   ${f} 已中转完成"
        fi
    done
fi

ssh "$RUN_HOST" "rm -f ${WORK}/m3_grid.csv" || true

REF_ARG=""
[ -n "$REFERENCE_LOSS" ] && REF_ARG="--reference-loss ${REFERENCE_LOSS}"
# 逆序重构目标默认开启，须与在线算子的默认值一致；两边不一致会让等值核验失效。
REV_ARG=""
[ "$REVERSE_TARGET" -eq 0 ] && REV_ARG="--reverse-target false"

RUN_MOUNTS="-m ${MEM_MB}m -v ${RHOME}/jars:/jars:ro -v ${WORK}:/work -e OMP_NUM_THREADS=${OMP_THREADS}"
RUN_CMD="java -Xmx${XMX_MB}m -Dorg.bytedeco.javacpp.maxbytes=${JAVACPP_MB}m \
        -Dorg.bytedeco.javacpp.maxphysicalbytes=${MEM_MB}m \
        -cp /jars/${JAR_NAME} com.leejean.m3.M3Grid \
        --rounds-jsonl /work/m1out.jsonl ${SCORES_ARG} \
        --devices ${DEVICES} --hidden-grid ${HIDDEN_GRID} --window-grid ${WINDOW_GRID} \
        --train-days ${TRAIN_DAYS} --early-stop-days ${ES_DAYS} \
        --max-epochs ${MAX_EPOCHS} --patience ${PATIENCE} --batch-grid ${BATCH_GRID} \
        ${REF_ARG} ${REV_ARG} --out /work/m3_grid.csv"

if [ "$DETACH" -eq 1 ]; then
    # 后台模式刻意不加 --rm：容器结束后要保留退出码与日志，供 --collect 判读，回收由 --collect 负责。
    # Deliberately no --rm here: the exit code and logs must survive for --collect to read.
    EXIST="$(ssh "$RUN_HOST" "docker inspect -f '{{.State.Status}}' ${CONTAINER_NAME} 2>/dev/null" || true)"
    if [ -n "${EXIST:-}" ]; then
        echo "ERROR: master 上已存在容器 ${CONTAINER_NAME}（状态 ${EXIST}）。" >&2
        echo "       若上一次任务还在跑，请等它结束；若已结束，先执行 --collect 取回结果。" >&2
        echo "       确认不再需要时可手动删除：ssh ${RUN_HOST} 'docker rm -f ${CONTAINER_NAME}'" >&2
        exit 5
    fi
    ssh "$RUN_HOST" "docker run -d --name ${CONTAINER_NAME} --user root ${RUN_MOUNTS} ${RUN_IMAGE} ${RUN_CMD}" >/dev/null
    RC=$?
    if [ "$RC" -ne 0 ]; then
        echo "ERROR: 后台容器启动失败，退出码 ${RC}。" >&2
        exit "$RC"
    fi
    echo ""
    echo "[grid] 已在 master 上以后台方式启动容器 ${CONTAINER_NAME}。"
    echo "[grid] 训练由 master 的 Docker 守护进程托管，**与本地 ssh 连接无关**："
    echo "       本地 Mac 休眠、断网、关机都不会中断它。"
    echo ""
    echo "  查看进度：ssh ${RUN_HOST} 'docker logs --tail 20 ${CONTAINER_NAME}'"
    echo "  跟随日志：ssh ${RUN_HOST} 'docker logs -f ${CONTAINER_NAME}'"
    echo "  取回结果：bash deploy/scripts/syn-m3-grid.sh --collect --out-name ${OUT_NAME}"
    echo ""
    exit 0
fi

ssh "$RUN_HOST" "docker run --rm --user root ${RUN_MOUNTS} ${RUN_IMAGE} ${RUN_CMD}"
RC=$?
if [ "$RC" -ne 0 ]; then
    echo "" >&2
    echo "ERROR: M3Grid 退出码 ${RC}——网格未完成，**不拉回 CSV**（避免留下不完整的产物）。" >&2
    if [ "$RC" -eq 255 ]; then
        echo "       退出码 255 是 ssh 断线，不是 M3Grid 的退出码：本地休眠或网络中断都会这样。" >&2
        echo "       耗时较长的网格请改用 --detach 启动、--collect 取回。" >&2
    fi
    exit "$RC"
fi

pull_csv || exit 1
echo "提醒：本阶段**不定终值**——网格表交设计会话裁决 (hidden, window)。"
