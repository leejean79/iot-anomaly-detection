#!/usr/bin/env bash
# ============================================================================
# syn-replay.sh
# 前台运行 CsvKafkaReplayer，把数据集重放进 synergia-source（交接文档 §3.5）。
# Run CsvKafkaReplayer in the FOREGROUND, replaying the dataset into synergia-source.
#
# 运行方式沿用 5-load-data.sh 的既有模式：在 master 上用 `docker run --rm --network host` 起一个
# 临时 flink 镜像容器（镜像自带 JDK），挂载 jars（只读）与数据集（读写，供 offset 落盘）。
# Uses 5-load-data.sh's proven pattern: a transient `docker run` of the flink image on master
# (the image ships a JDK), mounting jars (read-only) and the dataset (read-write for the offset file).
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash），ssh 到 fa-master；jar+数据集已上传（syn-upload-m1.sh）。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-replay.sh --speedup 600 --start 2022-05-21 --end 2022-05-22   # 单日对账
#      bash deploy/scripts/syn-replay.sh --speedup 3600                                        # 全量压力
#      bash deploy/scripts/syn-replay.sh --dry-run                                             # 只归并+日志
#      bash deploy/scripts/syn-replay.sh --resume --speedup 3600                               # 断点续跑
# 3. 前置条件 / Preconditions: M1Job 已 RUNNING（先提交作业再重放）；synergia-source 已建（8 分区）；
#      数据集在 ${SYN_DATASET_DIR}；fa-iforest/flink:$FLINK_VERSION 镜像在 master。
# 4. 期望产出 / Expected output: 进度与空闲压缩审计日志（stdout）；消息落 synergia-source（显式分区）。
# 5. 失败兜底 / Failure fallback: **前台运行**（后台 producer 收 SIGHUP 会死，继承的坑，§3.5）；
#      --resume 从 offset（.replayer.offset，落数据集目录）续跑；ssh -t 分配 TTY 保证前台/可 Ctrl-C。
#
# 缩写自查 / Abbreviations: k = speedup 加速倍数；TTY = teletypewriter（终端）；offset = 位移/进度。
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

REPLAY_ARGS="$*"   # 透传给重放器 / pass-through to the replayer

SSH_OPTS="-i ${SSH_KEY:-} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
MASTER_SSH="${NODE_MASTER_PUBLIC_IP:-$NODE_MASTER_IP}"
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
JAR_NAME="${SYN_JOB_JAR_NAME:-iot-anomaly-detection-1.0-SNAPSHOT.jar}"
MAIN="${SYN_REPLAYER_MAIN:-com.leejean.source.CsvKafkaReplayer}"
DATASET_DIR="${SYN_DATASET_DIR:-/opt/fa-iforest/datasets/synergia/files_csv}"

echo "===================================="
echo "[replay] $MAIN  (foreground, docker run --rm)"
echo "  data-dir(host)=$DATASET_DIR  topic=${SYN_TOPIC_SOURCE:-synergia-source}"
echo "  extra args: $REPLAY_ARGS"
echo "===================================="

# -t 分配 TTY，保证前台语义（Ctrl-C 可停）；docker run 无 -d 即前台阻塞。
# 挂载：jars 只读、数据集读写；--network host 直连 brokers。
# --user root：数据集目录属主为 root，flink 镜像默认用户(uid 9999)无法写入 offset 文件，
#   故以 root 运行使 .replayer.offset 可落盘（否则 --resume 失效）。offset 另可用 --offset-file 覆写。
# --user root: the dataset dir is root-owned; the flink image's default user (uid 9999) cannot write
#   the offset file, so run as root so .replayer.offset persists (else --resume breaks).
ssh -t $SSH_OPTS "$SSH_USER@$MASTER_SSH" "
    docker run --rm --network host --user root \
        -v ${REMOTE_HOME}/jars:/jars:ro \
        -v $DATASET_DIR:/data \
        fa-iforest/flink:${FLINK_VERSION} \
        java -cp /jars/$JAR_NAME $MAIN \
            --data-dir /data \
            --offset-file /data/.replayer.offset \
            --brokers $BROKERS \
            --topic ${SYN_TOPIC_SOURCE:-synergia-source} \
            --num-partitions ${SYN_SOURCE_PARTITIONS:-8} \
            $REPLAY_ARGS
"
