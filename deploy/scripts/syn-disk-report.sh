#!/usr/bin/env bash
# ============================================================================
# syn-disk-report.sh
# 只读地盘点三台节点的磁盘占用，把 33 GB 究竟花在哪里逐项量出来：Docker 镜像/容器可写层、
# Kafka 各 topic 的日志目录（区分本项目 synergia-* 与旧项目 FA-iForest）、REMOTE_HOME 下的
# jar 与数据集、容器 json 日志、systemd journal。最后给出「可安全回收」清单与实测大小。
# Read-only disk audit across the three nodes: Docker images/container writable layers, Kafka
# per-topic log dirs (separating this project's synergia-* from the old FA-iForest project),
# REMOTE_HOME jars and datasets, container json logs, systemd journal; then a reclaim candidate
# list with measured sizes.
#
# 为什么需要它：本集群的 Kafka 容器**没有挂载数据卷**，日志目录落在容器可写层里（overlay2），
# 因此 `du /` 看不出是哪个 topic 占的，必须进容器量。而且 synergia-* 的 retention.ms=-1（永不过期），
# 多轮实验不清理必然累积——这正是 deploy/env.example 第 155 行早已写下的告警。
# Why: the Kafka containers mount no data volume, so topic data sits in the container writable
# layer and only shows up from inside; and synergia-* uses retention.ms=-1, so runs accumulate.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash），仓库根目录；ssh 免密到三台节点；deploy/.env 已就绪。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-disk-report.sh                 # 盘点三台节点
#      bash deploy/scripts/syn-disk-report.sh --node master    # 只盘点一台（master|worker1|worker2）
#      bash deploy/scripts/syn-disk-report.sh --top 40         # 每张表多打几行，默认 20
# 3. 前置条件 / Preconditions: 三台节点已开机、ssh 可达；Docker 守护进程在跑。
#      Kafka 容器若未运行，该节点的 per-topic 明细会跳过并标注 SKIP，其余部分照常输出。
# 4. 期望产出 / Expected output: 每台节点一段报告，末尾一张「可安全回收」汇总表（含实测 MB）。
#      本脚本**不删除任何东西**，只读。
# 5. 常见失败兜底 / Failure fallback:
#      ssh 不通 → 先跑 refresh-ips.sh 刷新公网 IP；
#      `docker exec kafka-N` 报 No such container → 该 broker 未启动，先 2-up-all.sh；
#      某些镜像里没有 du/awk → 相应小节打印 N/A，不影响其他小节。
#
# 缩写自查 / Abbreviations: overlay2 = Docker 默认存储驱动的目录；可写层 = 容器运行期写入的独立层；
#   RF = Replication Factor 副本因子；journal = systemd 的系统日志。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

ONLY_NODE=""
TOP=20
while [[ $# -gt 0 ]]; do
    case "$1" in
        --node) ONLY_NODE="$2"; shift 2 ;;
        --top) TOP="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15"
RHOME="${REMOTE_HOME:-/opt/fa-iforest}"

# 远端只读盘点脚本：$1=REMOTE_HOME  $2=本节点 kafka 容器名  $3=TOP
REMOTE=$(cat <<'REMOTE_EOF'
RHOME="$1"; KC="$2"; TOP="$3"
hr() { printf '%s\n' "----------------------------------------------------------------"; }

echo "### 1. 文件系统 / filesystem"
df -h / | sed 's/^/  /'
hr

echo "### 2. Docker 总览 / docker system df"
docker system df 2>/dev/null | sed 's/^/  /' || echo "  N/A（docker 不可用）"
hr

echo "### 3. 各容器可写层大小（Kafka 数据就在这里）/ container writable layers"
docker ps -a --size --format '{{.Size}}\t{{.Names}}\t{{.Status}}' 2>/dev/null \
    | sed 's/^/  /' || echo "  N/A"
hr

echo "### 4. 镜像（含未被任何容器使用的）/ images"
docker images --format '{{.Size}}\t{{.Repository}}:{{.Tag}}\t{{.ID}}' 2>/dev/null \
    | sed 's/^/  /' | head -"$TOP" || echo "  N/A"
echo "  -- 悬空镜像 / dangling --"
docker images -f dangling=true --format '{{.Size}}\t{{.ID}}' 2>/dev/null | sed 's/^/  /' || true
hr

echo "### 5. Kafka 各 topic 数据量（容器内 du，按 topic 汇总）/ kafka per-topic"
if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$KC"; then
    docker exec "$KC" sh -c '
        LOGDIR=$(ls -d /kafka/kafka-logs-* 2>/dev/null | head -1)
        [ -z "$LOGDIR" ] && LOGDIR=/kafka
        du -sm "$LOGDIR"/*/ 2>/dev/null
    ' 2>/dev/null | awk -v top="$TOP" '
        {
            mb=$1; path=$2
            n=split(path,a,"/"); dir=a[n]; if (dir=="") dir=a[n-1]
            sub(/-[0-9]+$/,"",dir)              # 去掉分区号 / strip the partition suffix
            sum[dir]+=mb
        }
        END {
            syn=0; other=0
            for (t in sum) { if (t ~ /^synergia-/) syn+=sum[t]; else other+=sum[t] }
            printf "  %-46s %10s\n", "topic", "MB"
            cmd="sort -k2 -rn"
            for (t in sum) printf "  %-46s %10d\n", t, sum[t] | cmd
            close(cmd)
            printf "\n  本项目 synergia-* 合计: %d MB\n", syn
            printf "  其他（含旧项目 FA-iForest 与 __consumer_offsets）合计: %d MB\n", other
        }' || echo "  N/A（容器内缺少 du/awk）"
else
    echo "  SKIP：容器 $KC 未运行"
fi
hr

echo "### 5b. Docker 数据卷实测 / docker volumes (measured with du)"
# 【为何单列一节】wurstmeister/kafka 等镜像在 Dockerfile 里声明了 VOLUME，因此每次**新建容器**都会
# 生成一个匿名卷来存放 kafka-logs；容器被删除后这个匿名卷不会自动消失，于是历次实验的 broker 数据
# 会以「孤儿卷」的形式一直留在盘上。`docker system df` 的 Local Volumes 一行实测会显著高于真实占用
# （worker-1 曾报 51.73 GB 而整块盘只用了 15 GB），所以这里一律以 du 的实测值为准。
# Why a separate section: images like wurstmeister/kafka declare VOLUME, so every new container gets
# an anonymous volume for its kafka-logs; removing the container leaves that volume behind. The
# Local Volumes row of `docker system df` overcounts, so measure with du instead.
VOLTOT=$(du -sm /var/lib/docker/volumes 2>/dev/null | awk '{print $1}')
echo "  /var/lib/docker/volumes 实测合计: ${VOLTOT:-N/A} MB"
echo "  卷总数: $(docker volume ls -q 2>/dev/null | wc -l | tr -d ' ')   其中无容器引用（dangling）: $(docker volume ls -q -f dangling=true 2>/dev/null | wc -l | tr -d ' ')"
echo "  -- 最大的若干个卷（含是否被容器引用）/ largest volumes --"
DANGLING=$(docker volume ls -q -f dangling=true 2>/dev/null)
du -sm /var/lib/docker/volumes/*/ 2>/dev/null | sort -rn | head -"$TOP" | while read -r mb path; do
    vid=$(basename "$path")
    if printf '%s\n' "$DANGLING" | grep -qx "$vid"; then use="未被引用 dangling"; else use="使用中 in-use"; fi
    printf "  %8d MB  %-12s %s\n" "$mb" "$use" "$(echo "$vid" | cut -c1-40)"
done
DANGMB=$(for v in $DANGLING; do du -sm "/var/lib/docker/volumes/$v" 2>/dev/null | awk '{print $1}'; done | awk '{s+=$1} END {print s+0}')
echo "  无容器引用的卷合计: ${DANGMB:-0} MB   （docker volume prune 可回收，**不可撤销**）"
hr

echo "### 6. ${RHOME} 下的占用 / remote home"
du -sm "$RHOME"/* 2>/dev/null | sort -rn | head -"$TOP" | awk '{printf "  %10d MB  %s\n",$1,$2}' \
    || echo "  N/A"
echo "  -- jars 目录明细 / jars --"
ls -lh "$RHOME"/jars 2>/dev/null | sed 's/^/    /' || echo "    N/A"
hr

echo "### 7. 容器 json 日志 / container json logs"
du -sm /var/lib/docker/containers/*/*-json.log 2>/dev/null | sort -rn | head -"$TOP" \
    | awk '{printf "  %10d MB  %s\n",$1,$2}' || echo "  N/A"
TOTLOG=$(du -sm /var/lib/docker/containers 2>/dev/null | awk '{print $1}')
echo "  容器日志目录合计: ${TOTLOG:-N/A} MB"
hr

echo "### 8. systemd journal 与其他大目录 / journal and other large dirs"
journalctl --disk-usage 2>/dev/null | sed 's/^/  /' || echo "  N/A"
du -sm /var/log /tmp /root 2>/dev/null | sort -rn | awk '{printf "  %10d MB  %s\n",$1,$2}'
hr

echo "### 9. 可安全回收候选（实测，仅统计不删除）/ reclaim candidates (measured, nothing deleted)"
T=$(du -sm "$RHOME"/fa-iforest-flink.tar 2>/dev/null | awk '{print $1}')
echo "  镜像 tar 包 $RHOME/fa-iforest-flink.tar : ${T:-0} MB   （docker load 之后即无用）"
D=$(docker images -f dangling=true -q 2>/dev/null | wc -l | tr -d ' ')
echo "  悬空镜像数量: ${D:-0} 个   （docker image prune 可回收，不影响在用镜像）"
echo "  容器 json 日志合计: ${TOTLOG:-N/A} MB   （truncate 可回收，注意会丢失历史日志）"
echo "  无容器引用的 Docker 卷: ${DANGMB:-0} MB   （历次 Kafka 容器留下的孤儿卷，通常是最大的一块）"
J=$(du -sm "$RHOME"/jars 2>/dev/null | awk '{print $1}')
echo "  $RHOME/jars 合计: ${J:-0} MB   （每个本项目 fat jar 约 214 MB，旧版本可删）"
REMOTE_EOF
)

run_node() {
    local label="$1" host="$2" kc="$3"
    echo ""
    echo "================================================================"
    echo "  节点 / node: $label   ($host)   kafka 容器: $kc"
    echo "================================================================"
    ssh $SSH_OPTS "$SSH_USER@$host" "bash -s -- '$RHOME' '$kc' '$TOP'" <<< "$REMOTE" \
        || echo "  [ERROR] 该节点盘点失败（ssh 不通或 docker 不可用）"
}

[ -z "$ONLY_NODE" -o "$ONLY_NODE" = "master" ]  && run_node "fa-master"  "${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"   "kafka-1"
[ -z "$ONLY_NODE" -o "$ONLY_NODE" = "worker1" ] && run_node "fa-worker1" "${NODE_WORKER1_PUBLIC_IP:-$NODE_WORKER1_IP}" "kafka-2"
[ -z "$ONLY_NODE" -o "$ONLY_NODE" = "worker2" ] && run_node "fa-worker2" "${NODE_WORKER2_PUBLIC_IP:-$NODE_WORKER2_IP}" "kafka-3"

echo ""
echo "================================================================"
echo "  盘点结束。本脚本是只读的，没有删除任何内容。"
echo "  清理步骤与安全边界见 docs/cluster_disk_cleanup_zh.md（2.0 版）。"
echo "  最重要的一条：占用主体是历次容器遗留的**孤儿匿名卷**（第 5b 小节），不是容器可写层。"
echo "  因此不可撤销的动作是 docker volume prune，而不是删除容器；且一律不可加 -a 执行"
echo "  docker system prune（会连 fa-iforest/flink 镜像一起删掉）。"
echo "================================================================"
