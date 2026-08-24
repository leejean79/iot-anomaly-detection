#!/usr/bin/env bash
# ============================================================================
# syn-upload-m1.sh
# 上传 M1 fat jar 与数据集到集群旧挂载目录（DEV-D8：集群侧目录归旧基础设施）。
# Upload the M1 fat jar and the dataset to the cluster's old mount (DEV-D8: cluster-side dirs
# belong to the old infra), distinguished by filename (iot-anomaly-detection-*).
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash），ssh 免密到 fa-master；已 mvn package 出 jar。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-upload-m1.sh --jar-only            # 只传 jar（改代码后常用）
#      bash deploy/scripts/syn-upload-m1.sh --data-dir <local_csv_dir>   # 传 jar + 数据集（首次）
# 3. 前置条件 / Preconditions: target/ 下有 iot-anomaly-detection-*.jar；.env 填好节点/密钥。
# 4. 期望产出 / Expected output:
#      jar  → <REMOTE_HOME>/jars/$SYN_JOB_JAR_NAME （即 jobmanager 容器挂载的 /opt/flink/usrlib）
#      data → $SYN_DATASET_DIR （默认 /opt/fa-iforest/datasets/synergia/files_csv）
# 5. 失败兜底 / Failure fallback: 找不到 jar 报错退出；rsync 断点续传（-P）；数据集约 2.3GB，
#      仅传到 master（重放器在 master 上就近读）。
#
# 缩写自查 / Abbreviations: jar = Java ARchive；rsync = remote sync 远程同步；scp = secure copy。
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DEPLOY_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

JAR_ONLY=false
LOCAL_DATA_DIR=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --jar-only) JAR_ONLY=true; shift ;;
        --data-dir) LOCAL_DATA_DIR="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
JAR_NAME="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"
DATASET_DIR="${SYN_DATASET_DIR:-/opt/fa-iforest/datasets/synergia/files_csv}"

LOCAL_JAR="$(ls "$PROJECT_ROOT"/target/iot-anomaly-detection-*.jar 2>/dev/null | grep -vE 'original-|sources|javadoc' | head -1 || true)"
if [[ -z "$LOCAL_JAR" ]]; then
    echo "ERROR: no target/iot-anomaly-detection-*.jar; run 'mvn clean package' first." >&2
    exit 1
fi

echo "== upload jar =="
echo "  $LOCAL_JAR  ->  $MASTER_SSH:${REMOTE_HOME}/jars/$JAR_NAME"
ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "mkdir -p ${REMOTE_HOME}/jars"
scp $SSH_OPTS "$LOCAL_JAR" "$SSH_USER@$MASTER_SSH:${REMOTE_HOME}/jars/$JAR_NAME"

if ! $JAR_ONLY; then
    if [[ -z "$LOCAL_DATA_DIR" ]]; then
        echo "ERROR: --data-dir <local_csv_dir> required unless --jar-only." >&2
        exit 1
    fi
    if [[ ! -d "$LOCAL_DATA_DIR" ]]; then
        echo "ERROR: local data dir not found: $LOCAL_DATA_DIR" >&2
        exit 1
    fi
    echo "== upload dataset (~2.3GB, resumable) =="
    echo "  $LOCAL_DATA_DIR/  ->  $MASTER_SSH:$DATASET_DIR/"
    ssh $SSH_OPTS "$SSH_USER@$MASTER_SSH" "mkdir -p $DATASET_DIR"
    rsync -aP -e "ssh $SSH_OPTS" "$LOCAL_DATA_DIR"/ "$SSH_USER@$MASTER_SSH:$DATASET_DIR"/
fi

echo "DONE."
echo "下一步 / Next: syn-submit-m1.sh（先提交作业）→ syn-replay.sh（再重放）。"
