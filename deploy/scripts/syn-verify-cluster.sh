#!/usr/bin/env bash
# ============================================================================
# syn-verify-cluster.sh
# ENV 阶段验收一键脚本：执行交接文档 §6 全部步骤，逐项打印 通过/失败，并生成
# docs/env_acceptance.md（含命令、原始输出摘录、核对表、DeviceId→分区哈希映射）。
# One-button ENV acceptance: run all handover §6 steps, print PASS/FAIL per item, and
# emit docs/env_acceptance.md (commands, output excerpts, checklist, DeviceId→partition map).
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment:
#      本地 Mac（bash），ssh 免密到 fa-master；.env 含节点 IP/SSH_KEY 与 SYN_* 段；
#      本机可 ssh 到 master 并在其上 curl Flink REST（:8081）。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-verify-cluster.sh
#      （先跑 syn-create-topics.sh 建好 synergia-* topic）
# 3. 前置条件 / Preconditions:
#      集群运行中（旧项目容器共存）；synergia-source / synergia-smoke 已创建；
#      master 上 zookeeper / kafka-1 / jobmanager 容器在跑；flink example jar 在镜像内
#      (/opt/flink/examples/streaming/WordCount.jar)。
# 4. 期望产出 / Expected output:
#      控制台核对表 + docs/env_acceptance.md。
# 5. 失败兜底 / Failure fallback:
#      不用 set -e——单步失败记为 FAIL 并继续，最终以"是否全部通过"决定退出码（0/1）；
#      发现 RUNNING 的旧 job 只记录上报、**绝不 cancel**；只读验证，不改旧资源。
#      No set -e: a failed step is recorded as FAIL and execution continues; a RUNNING old
#      job is only reported, never cancelled; verification is read-only w.r.t. old resources.
#
# 缩写自查 / Abbreviations:
#   ZK = ZooKeeper；JM = JobManager；TM = TaskManager；RF = Replication Factor 副本因子；
#   REST = Representational State Transfer（此处指 Flink 的 HTTP 接口，:8081）；
#   slot = Flink 任务槽（TM 的并行执行单元）。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"

if [[ ! -f "$DEPLOY_DIR/.env" ]]; then
    echo "ERROR: $DEPLOY_DIR/.env not found." >&2
    exit 1
fi
set -a; source "$DEPLOY_DIR/.env"; set +a

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
REST="http://$NODE_MASTER_IP:8081"
SRC_TOPIC="${SYN_TOPIC_SOURCE:-synergia-source}"
SMOKE_TOPIC="${SYN_TOPIC_SMOKE:-synergia-smoke}"
SRC_PARTS="${SYN_SOURCE_PARTITIONS:-8}"
RF="${SYN_TOPIC_REPLICATION:-2}"

OUT="$PROJECT_ROOT/docs/env_acceptance.md"
mkdir -p "$PROJECT_ROOT/docs"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# 远端调用助手 / remote helpers
on_master() { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "$@"; }
kcmd()      { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "docker exec kafka-1 $*"; }
kcmd_in()   { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "docker exec -i kafka-1 $*"; }
zkcmd()     { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "docker exec zookeeper $*"; }
mcurl()     { ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "curl -s --max-time 20 $*"; }

# 结果记录 / result recording
PASS_CNT=0; FAIL_CNT=0
declare -a CHECKLIST=()
record() {   # record <PASS|FAIL> <item>
    local st="$1"; shift
    if [[ "$st" == PASS ]]; then PASS_CNT=$((PASS_CNT+1)); echo "  [PASS] $*";
    else FAIL_CNT=$((FAIL_CNT+1)); echo "  [FAIL] $*"; fi
    CHECKLIST+=("| $st | $* |")
}
section() { echo ""; echo "== $* =="; }

echo "############################################################"
echo "# ENV 验收 / acceptance  —  $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "# master=$MASTER_SSH  REST=$REST"
echo "############################################################"

# ---------------------------------------------------------------------------
section "步骤 0 / Step 0: 连通性预检 / connectivity preflight"
# ---------------------------------------------------------------------------
# 先证明 ssh 能到 master，再往下跑——否则后面每一步都各自超时刷 FAIL，浪费时间且误导。
# Prove ssh reaches master before anything else; otherwise every later step just times out.
if [[ "$NODE_MASTER_IP" == "172.16.0.11" ]]; then
    echo "  警告 / WARNING: NODE_MASTER_IP=172.16.0.11 与 env.example 的占位值相同——"
    echo "  deploy/.env 很可能还没填真实集群 IP。/ .env still carries the template placeholder IP."
fi
if ! ssh $SSH_OPTS -o BatchMode=yes "$SSH_USER@$MASTER_SSH" true 2>/dev/null; then
    echo ""
    echo "FATAL: 无法 ssh 到 master（$SSH_USER@${MASTER_SSH}）。/ cannot ssh to master." >&2
    echo "  排查 / troubleshoot:" >&2
    echo "    1) deploy/.env 是否已从 env.example 复制并填入**真实**节点 IP" >&2
    echo "       （env.example 里的 172.16.0.11/12/13 是占位值；同一集群可直接复用旧仓库" >&2
    echo "         FA-iForest-master/deploy/.env 的 NODE_* / SSH_* 取值）；" >&2
    echo "    2) 公网 IP 是否过期（实例重启会换）—— bash deploy/scripts/refresh-ips.sh；" >&2
    echo "    3) SSH_KEY 路径是否正确、权限是否 600。" >&2
    exit 2
fi
record PASS "ssh 连通 master（$SSH_USER@${MASTER_SSH}）/ ssh to master OK"

# ---------------------------------------------------------------------------
section "步骤 1 / Step 1: Broker 与版本自证 / brokers and versions"
# ---------------------------------------------------------------------------
# Broker 存活判定 / broker-liveness check —— 采用双证据，任一成立即通过：
#   (1) 权威口径 / authoritative: Kafka 自身视角 `kafka-broker-api-versions.sh` 列出的 broker id；
#   (2) ZK 口径 / ZK view: `zookeeper-shell ls /brokers/ids`（保留以对齐交接文档 §6.1）。
# 只有两条口径都拿不到 3 个 broker 时才判 FAIL——避免 ZK 容器命名/输出流差异造成假阴性
# （上一版 `docker exec zookeeper` + `2>/dev/null` 在旧集群上返回空，却与 topic 的 RF=2/ISR
#  跨 3 broker 事实矛盾，属工具假阴性，非集群问题）。
# Two independent evidences; PASS if either shows three brokers. Guards against a ZK-shell
# container-name / output-stream quirk producing a false negative.

# --- 口径 1：Kafka 自身（在 kafka-1 容器内，已证可用）/ evidence 1: Kafka's own view ---
BROKER_API="$(kcmd kafka-broker-api-versions.sh --bootstrap-server "$BROKERS" 2>/dev/null)"
KAFKA_IDS="$(echo "$BROKER_API" | grep -oE 'id: *[0-9]+' | grep -oE '[0-9]+' | sort -un | tr '\n' ' ')"
KAFKA_CNT="$(echo "$KAFKA_IDS" | wc -w | tr -d ' ')"

# --- 口径 2：ZooKeeper（M1 §2.3 指定的修正）/ evidence 2: ZK (fix per M1 handover §2.3) ---
# zookeeper-shell 把结果写到 stderr，故必须 2>&1（原版 2>/dev/null 会吞掉输出返回空）。
# 在 kafka-1 容器内调 zookeeper-shell.sh 连 master:2181（该镜像已带 kafka 脚本，比依赖
# 单独的 zk 容器名更稳）。/ zookeeper-shell writes to stderr, so 2>&1 is required; run it from
# kafka-1 (which ships the kafka CLI) against master:2181.
ZK_RAW="$(kcmd zookeeper-shell.sh "$NODE_MASTER_IP:2181" ls /brokers/ids 2>&1 || true)"
ZK_IDS="$(echo "$ZK_RAW" | grep -oE '\[[0-9, ]+\]' | tail -1)"
{ echo "== kafka-broker-api-versions (ids: $KAFKA_IDS) =="; echo "$BROKER_API";
  echo "== zookeeper-shell.sh $NODE_MASTER_IP:2181 ls /brokers/ids (2>&1) =="; echo "$ZK_RAW"; } > "$TMP/zk_ids.txt"
echo "  Kafka broker ids = {${KAFKA_IDS}}（$KAFKA_CNT 个）; ZK /brokers/ids = ${ZK_IDS:-<empty>}"

zk_ok=1
for b in 1 2 3; do echo "$ZK_IDS" | grep -qE "\b$b\b" || zk_ok=0; done
if [[ "$KAFKA_CNT" -ge 3 ]]; then
    record PASS "3 broker 存活（Kafka 口径 ids={${KAFKA_IDS}}$( [[ "$zk_ok" == 1 ]] && echo '，ZK 口径一致' || echo '；ZK 口径未取到，见摘录' )）/ three brokers alive"
elif [[ "$zk_ok" == 1 ]]; then
    record PASS "3 broker 存活（ZK 口径 ${ZK_IDS}；Kafka 口径仅 ${KAFKA_CNT}，见摘录）/ three brokers alive (ZK)"
else
    record FAIL "未能确认 3 broker 存活（Kafka ids={${KAFKA_IDS}}, ZK=${ZK_IDS:-空}）—— 与 §2 不符，须上报"
fi

FLINK_CFG="$(mcurl "$REST/config" 2>/dev/null)"
echo "$FLINK_CFG" > "$TMP/flink_config.json"
if echo "$FLINK_CFG" | grep -q '"flink-version":"1.13.6"'; then
    record PASS "Flink 版本 1.13.6 自证 / flink-version 1.13.6"
else
    record FAIL "Flink 版本非 1.13.6（$(echo "$FLINK_CFG" | grep -oE '"flink-version":"[^"]*"' | head -1)）"
fi

TM_JSON="$(mcurl "$REST/taskmanagers" 2>/dev/null)"
echo "$TM_JSON" > "$TMP/taskmanagers.json"
TM_CNT="$(echo "$TM_JSON" | grep -oE '"id"' | wc -l | tr -d ' ')"
SLOT_SUM="$(echo "$TM_JSON" | grep -oE '"slotsNumber":[0-9]+' | grep -oE '[0-9]+' | awk '{s+=$1} END{print s+0}')"
echo "  TM 数=$TM_CNT, slots 合计=$SLOT_SUM"
if [[ "$TM_CNT" == "2" && "$SLOT_SUM" == "8" ]]; then
    record PASS "2 TaskManager / 共 8 slots"
else
    record FAIL "TM/slots 非 2/8（实测 TM=$TM_CNT, slots=${SLOT_SUM}）"
fi

JOBS_JSON="$(mcurl "$REST/jobs" 2>/dev/null)"
echo "$JOBS_JSON" > "$TMP/jobs.json"
RUNNING="$(echo "$JOBS_JSON" | grep -oE '"status":"RUNNING"' | wc -l | tr -d ' ')"
echo "  当前 job（RUNNING 计数=${RUNNING}）: $JOBS_JSON"
if [[ "$RUNNING" == "0" ]]; then
    record PASS "无 RUNNING 残留 job / no lingering RUNNING job"
else
    record PASS "检出 $RUNNING 个 RUNNING job（可能为旧项目）——已记录、未 cancel，请人工确认"
fi

# ---------------------------------------------------------------------------
section "步骤 2 / Step 2: Topic 创建核验 / topic creation"
# ---------------------------------------------------------------------------
SRC_DESC="$(kcmd kafka-topics.sh --bootstrap-server "$BROKERS" --describe --topic "$SRC_TOPIC" 2>/dev/null)"
echo "$SRC_DESC" > "$TMP/source_describe.txt"
echo "$SRC_DESC"
cur_p="$(echo "$SRC_DESC" | grep -oE 'PartitionCount: *[0-9]+' | grep -oE '[0-9]+' | head -1)"
cur_rf="$(echo "$SRC_DESC" | grep -oE 'ReplicationFactor: *[0-9]+' | grep -oE '[0-9]+' | head -1)"
# leader 覆盖的 broker 数 / distinct leader brokers
leaders="$(echo "$SRC_DESC" | grep -oE 'Leader: *[0-9]+' | grep -oE '[0-9]+' | sort -u | tr '\n' ' ')"
n_leaders="$(echo "$leaders" | wc -w | tr -d ' ')"
echo "  partitions=$cur_p, RF=$cur_rf, leader brokers={$leaders}"
if [[ "$cur_p" == "$SRC_PARTS" && "$cur_rf" == "$RF" ]]; then
    record PASS "$SRC_TOPIC = $SRC_PARTS 分区 / RF=$RF"
else
    record FAIL "$SRC_TOPIC 参数不符（实测 partitions=$cur_p, RF=${cur_rf}）"
fi
if [[ "$n_leaders" -ge 3 ]]; then
    record PASS "leader 分布覆盖 3 个 broker / leaders spread over 3 brokers"
else
    record FAIL "leader 仅覆盖 $n_leaders 个 broker（{$leaders}）—— 副本分布不均，须查看"
fi

# ---------------------------------------------------------------------------
section "步骤 3 / Step 3: 保序冒烟 / ordering smoke（key=A..H 各 10 条）"
# ---------------------------------------------------------------------------
# 生产：每个 DeviceId 写 10 条带序号消息（key:value = A:A-0 ...）
# Produce 10 sequenced messages per DeviceId (key:value = A:A-0 ...)
PAYLOAD=""
for k in A B C D E F G H; do
    for i in $(seq 0 9); do PAYLOAD+="$k:$k-$i"$'\n'; done
done
printf "%s" "$PAYLOAD" | kcmd_in kafka-console-producer.sh \
    --bootstrap-server "$BROKERS" --topic "$SRC_TOPIC" \
    --property parse.key=true --property key.separator=: >/dev/null 2>&1 \
    && record PASS "生产 80 条（8×10）到 $SRC_TOPIC / produced 80 keyed messages" \
    || record FAIL "生产失败 / producer failed"

# 逐分区消费，构建 DeviceId→分区 映射并核验分区内序号有序
# Consume per partition; build the DeviceId→partition map and check in-partition ordering.
MAP_FILE="$TMP/mapping.txt"; : > "$MAP_FILE"
ORDER_OK=1
for ((p=0; p<SRC_PARTS; p++)); do
    part_out="$(kcmd kafka-console-consumer.sh --bootstrap-server "$BROKERS" \
        --topic "$SRC_TOPIC" --partition "$p" --from-beginning --timeout-ms 8000 \
        --property print.key=true --property key.separator=: 2>/dev/null)"
    # 该分区出现的 key 集合 / keys seen in this partition
    keys="$(echo "$part_out" | awk -F: 'NF>=2{print $1}' | sort -u | tr '\n' ',' | sed 's/,$//')"
    [[ -n "$keys" ]] && echo "partition $p <- {$keys}" >> "$MAP_FILE"
    # 每个 key 在该分区内序号是否递增 / per-key sequence monotonic within the partition
    for k in $(echo "$part_out" | awk -F: 'NF>=2{print $1}' | sort -u); do
        seqs="$(echo "$part_out" | awk -F: -v kk="$k" '$1==kk{sub(/^[A-H]-/,"",$2); print $2}')"
        sorted="$(echo "$seqs" | sort -n)"
        [[ "$seqs" == "$sorted" ]] || { ORDER_OK=0; echo "  [order] key=$k in partition $p NOT monotonic"; }
    done
done
echo "  实测映射 / observed map:"; sed 's/^/    /' "$MAP_FILE"
# 每个 key 是否恒落单一分区 / each key lands in exactly one partition
SINGLE_PART_OK=1
for k in A B C D E F G H; do
    cnt="$(grep -c "\b$k\b" "$MAP_FILE" || true)"
    [[ "$cnt" -le 1 ]] || { SINGLE_PART_OK=0; echo "  [split] key=$k 落在多个分区 / spans multiple partitions"; }
done
[[ "$SINGLE_PART_OK" == 1 ]] && record PASS "同 key 恒落同一分区 / each DeviceId maps to one partition" \
    || record FAIL "存在 key 跨分区 / a DeviceId spans partitions（异常）"
[[ "$ORDER_OK" == 1 ]] && record PASS "分区内按 key 序号有序 / in-partition order preserved" \
    || record FAIL "分区内序号非单调 / out-of-order within a partition"
USED_PARTS="$(wc -l < "$MAP_FILE" | tr -d ' ')"
record PASS "实际占用分区数 = $USED_PARTS / ${SRC_PARTS}（哈希碰撞属正常，映射见报告）"

# ---------------------------------------------------------------------------
section "步骤 4 / Step 4: Flink 调度冒烟 / scheduling smoke (WordCount example)"
# ---------------------------------------------------------------------------
EX_JAR="/opt/flink/examples/streaming/WordCount.jar"
SUBMIT_OUT="$(on_master "docker exec jobmanager flink run $EX_JAR" 2>&1)"
echo "$SUBMIT_OUT" > "$TMP/wordcount.txt"
echo "$SUBMIT_OUT" | tail -5 | sed 's/^/    /'
if echo "$SUBMIT_OUT" | grep -qiE "Job Runtime|Program execution finished|(finished|completed)"; then
    record PASS "WordCount example 提交并正常结束 / example submitted and finished"
else
    record FAIL "WordCount example 未见完成标志（详见 env_acceptance.md 摘录）"
fi

# ---------------------------------------------------------------------------
section "步骤 5 / Step 5: 监控核验 / monitoring"
# ---------------------------------------------------------------------------
PROM_TARGETS="$(mcurl "http://$NODE_MASTER_IP:${PROMETHEUS_PORT:-9090}/api/v1/targets" 2>/dev/null)"
echo "$PROM_TARGETS" > "$TMP/prom_targets.json"
if [[ -n "$PROM_TARGETS" ]]; then
    UP_CNT="$(echo "$PROM_TARGETS" | grep -oE '"health":"up"' | wc -l | tr -d ' ')"
    DOWN_CNT="$(echo "$PROM_TARGETS" | grep -oE '"health":"down"' | wc -l | tr -d ' ')"
    echo "  targets up=$UP_CNT, down=$DOWN_CNT (期望 up≥6: JM×1 + TM×2 + node×3)"
    if [[ "$UP_CNT" -ge 6 && "$DOWN_CNT" == 0 ]]; then
        record PASS "Prometheus targets 全绿（up=${UP_CNT}）/ all targets up"
    else
        record FAIL "Prometheus 存在 down 或 up 不足（up=$UP_CNT, down=${DOWN_CNT}）"
    fi
else
    record FAIL "Prometheus /targets 无响应（监控未起或端口不通）—— 可选项，按需排查"
fi
GRAFANA_CODE="$(mcurl "-o /dev/null -w '%{http_code}' http://$NODE_MASTER_IP:${GRAFANA_PORT:-3000}/login" 2>/dev/null | tr -d "'")"
if [[ "$GRAFANA_CODE" == "200" ]]; then
    record PASS "Grafana 登录页可达 (HTTP 200)"
else
    record FAIL "Grafana 登录页不可达 (HTTP ${GRAFANA_CODE:-none}) —— 可选项"
fi

# ---------------------------------------------------------------------------
section "生成验收报告 / writing acceptance report -> docs/env_acceptance.md"
# ---------------------------------------------------------------------------
{
    echo "# ENV 阶段验收记录 / Environment Stage Acceptance"
    echo ""
    echo "> 由 \`deploy/scripts/syn-verify-cluster.sh\` 自动生成 / auto-generated."
    echo "> 生成时间 / generated: $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "> master=\`$MASTER_SSH\`  REST=\`$REST\`  brokers=\`$BROKERS\`"
    echo ""
    echo "## 核对表 / Checklist"
    echo ""
    echo "| 结果 / result | 验收项 / item |"
    echo "|---|---|"
    for row in "${CHECKLIST[@]}"; do echo "$row"; done
    echo ""
    echo "合计 / total: PASS=$PASS_CNT, FAIL=$FAIL_CNT"
    echo ""
    echo "## DeviceId → 分区哈希映射 / hash mapping（供 M1 重放器参考）"
    echo ""
    echo "> 本阶段冒烟用 console 工具按 Kafka 默认 \`hash(key) % $SRC_PARTS\` 分区；"
    echo "> M1 重放器将改用显式 partitioner（A→0…H→7）实现一设备一分区。"
    echo ""
    echo '```'
    cat "$MAP_FILE" 2>/dev/null || echo "(无数据 / none)"
    echo '```'
    echo ""
    echo "## 原始输出摘录 / Raw output excerpts"
    echo ""
    echo "### Broker 存活（Kafka 口径 + ZK 口径）/ broker liveness (Kafka + ZK views)"
    echo '```'; cat "$TMP/zk_ids.txt" 2>/dev/null; echo '```'
    echo "### Flink /taskmanagers"; echo '```json'; head -c 1200 "$TMP/taskmanagers.json" 2>/dev/null; echo; echo '```'
    echo "### Flink /jobs"; echo '```json'; cat "$TMP/jobs.json" 2>/dev/null; echo; echo '```'
    echo "### $SRC_TOPIC --describe"; echo '```'; cat "$TMP/source_describe.txt" 2>/dev/null; echo '```'
    echo "### WordCount example (tail)"; echo '```'; tail -8 "$TMP/wordcount.txt" 2>/dev/null; echo '```'
    echo "### Prometheus targets (health 计数)"; echo '```'
    echo "up=$(grep -oE '"health":"up"' "$TMP/prom_targets.json" 2>/dev/null | wc -l | tr -d ' '), down=$(grep -oE '"health":"down"' "$TMP/prom_targets.json" 2>/dev/null | wc -l | tr -d ' ')"
    echo '```'
    echo ""
    echo "## 边界声明 / Boundary note"
    echo ""
    echo "- 本次验收为**只读验证**：未 cancel 任何 job、未改动任何非 \`synergia-\` topic 与旧容器。"
    echo "- 步骤 3 向 \`$SRC_TOPIC\` 写入了 80 条测试消息；M1 正式重放前请先 \`bash deploy/scripts/syn-clean-topics.sh\` 清零。"
} > "$OUT"

echo ""
echo "############################################################"
echo "# 验收完成 / DONE:  PASS=$PASS_CNT  FAIL=$FAIL_CNT"
echo "# 报告 / report: $OUT"
echo "# 提醒：步骤 3 已向 $SRC_TOPIC 写入测试消息，M1 重放前请先 syn-clean-topics.sh。"
echo "############################################################"

[[ "$FAIL_CNT" == 0 ]] && exit 0 || exit 1
