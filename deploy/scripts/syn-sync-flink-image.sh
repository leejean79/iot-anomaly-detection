#!/usr/bin/env bash
# ============================================================================
# syn-sync-flink-image.sh
# 共存安全地把新的自定义 Flink 镜像 tar + 两个 compose + .env 分发到三节点并 docker load。
# 复用 1-sync-to-nodes.sh 的 ssh/rsync 模式，但**只发镜像与配置、绝不发任何 jar**——因为 1-sync 会把
# $JOB_JAR_NAME(=FA-iForest-1.0-SNAPSHOT.jar) 覆盖到共享 jars/，触碰旧项目产物、违反共存红线；本项目的
# jar 由 syn-upload-m1.sh 以 SYN_JOB_JAR_NAME 单独投放。
# Coexistence-safe distribution of the new custom Flink image tar + the two compose files + .env to the
# three nodes, then docker load. Reuses 1-sync-to-nodes.sh's ssh/rsync pattern but ships ONLY the image
# and config, never any jar (1-sync would overwrite the old FA-iForest jar in the shared jars/ dir; this
# project's jar is delivered separately by syn-upload-m1.sh under SYN_JOB_JAR_NAME).
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash），ssh 免密到三节点（与 1-sync 同一可达性模型：优先公网 IP，
#    未填则用内网 IP）；已先跑 0-prepare-local.sh 生成 deploy/.build/fa-iforest-flink.tar。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/0-prepare-local.sh          # 先构建 jar + java11 镜像 tar
#      bash deploy/scripts/syn-sync-flink-image.sh     # 再分发镜像+compose+.env 并 docker load
#      bash deploy/scripts/2-up-all.sh                 # 最后仅重建变更了的 flink 容器（jobmanager/taskmanager）
# 3. 前置条件 / Preconditions: deploy/.env 填好节点 IP/SSH_KEY 与 FLINK_IMAGE_TAG；.build/ 下有镜像 tar。
# 4. 期望产出 / Expected output: 三节点 $REMOTE_HOME 下有最新 .env 与 compose，镜像已 docker load；
#    旧 Java 8 镜像 fa-iforest/flink:1.13.6 保留不动。
# 5. 失败兜底 / Failure fallback: 找不到镜像 tar → 立即报错退出（提示先跑 0-prepare-local.sh）；
#    某节点 ssh 不通 → 该节点报错；**绝不发送或覆盖任何 jar，绝不动旧项目容器/topic**。
#
# 缩写自查 / Abbreviations: tar = 打包归档；rsync = 远程同步；TM = TaskManager；JM = JobManager。
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

BUILD_DIR="$DEPLOY_DIR/.build"
IMAGE_TAR="$BUILD_DIR/fa-iforest-flink.tar"
SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

# 镜像 tar 必须已由 0-prepare-local.sh 生成 / the image tar must exist (built by 0-prepare-local.sh)
if [[ ! -f "$IMAGE_TAR" ]]; then
    echo "ERROR: 找不到镜像 tar：$IMAGE_TAR" >&2
    echo "  请先运行：bash deploy/scripts/0-prepare-local.sh / run 0-prepare-local.sh first." >&2
    exit 1
fi

# 与 1-sync 相同的可达性解析：优先公网 IP，未填用内网 IP / same reachability model as 1-sync
resolve_ssh_host() { local internal=$1 public=$2; echo "${public:-$internal}"; }
MASTER_SSH=$(resolve_ssh_host "$NODE_MASTER_IP" "${NODE_MASTER_PUBLIC_IP:-}")
WORKER1_SSH=$(resolve_ssh_host "$NODE_WORKER1_IP" "${NODE_WORKER1_PUBLIC_IP:-}")
WORKER2_SSH=$(resolve_ssh_host "$NODE_WORKER2_IP" "${NODE_WORKER2_PUBLIC_IP:-}")

echo "===================================================================="
echo "syn-sync-flink-image.sh — 分发镜像+compose+.env（不含任何 jar）/ image+config only, no jar"
echo "  image tar: $IMAGE_TAR"
echo "  FLINK_IMAGE_TAG=${FLINK_IMAGE_TAG:-<未设置/unset>}"
echo "===================================================================="

# 把镜像 tar + .env + 两个 compose 传到一个节点，并 docker load / push image+config to one node, then load
sync_one() {
    local host=$1 label=$2
    echo "[$label @ $host]"
    ssh $SSH_OPTS "$SSH_USER@$host" "mkdir -p $REMOTE_HOME/compose"
    rsync -az -e "ssh $SSH_OPTS" "$DEPLOY_DIR/.env"                                "$SSH_USER@$host:$REMOTE_HOME/.env"
    rsync -az -e "ssh $SSH_OPTS" "$DEPLOY_DIR/compose/docker-compose.master.yml"  "$SSH_USER@$host:$REMOTE_HOME/compose/"
    rsync -az -e "ssh $SSH_OPTS" "$DEPLOY_DIR/compose/docker-compose.worker.yml"  "$SSH_USER@$host:$REMOTE_HOME/compose/"
    rsync -azP -e "ssh $SSH_OPTS" "$IMAGE_TAR"                                     "$SSH_USER@$host:$REMOTE_HOME/fa-iforest-flink.tar"
    echo "  [load image @ $host]"
    ssh $SSH_OPTS "$SSH_USER@$host" "docker load -i $REMOTE_HOME/fa-iforest-flink.tar"
}

sync_one "$MASTER_SSH"  "master"
sync_one "$WORKER1_SSH" "worker-1"
sync_one "$WORKER2_SSH" "worker-2"

echo ""
echo "===================================================================="
echo "DONE. 旧 Java 8 镜像未动，可回滚。/ old Java 8 image untouched (rollback-safe)."
echo "下一步 / Next: bash $SCRIPT_DIR/2-up-all.sh  （仅重建变更的 flink 容器：jobmanager / taskmanager）"
echo "  然后 / then: bash $SCRIPT_DIR/syn-upload-m1.sh --jar-only  （投放本项目 M3 jar）"
echo "===================================================================="
