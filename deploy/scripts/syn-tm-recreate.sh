#!/usr/bin/env bash
# ============================================================================
# syn-tm-recreate.sh
# 把本地的 deploy/.env 与 worker 容器编排文件分发到两台 worker，然后只重建两个 TaskManager 容器，使
# TaskManager 的配置改动（例如 JavaCPP 物理内存上限，2026-10-06 裁决第四节）生效。Kafka 等其他容器不动。
# Ship deploy/.env and the worker compose file to both workers and recreate ONLY the two TaskManager
# containers, so TaskManager configuration changes take effect. Kafka and other containers are untouched.
#
# 为什么不用 1-sync-to-nodes.sh 与 2-up-all.sh：前者用密钥直连公网地址登录（已改为经 ssh 别名登录），后者会
# 对整套服务执行 up，有连带重建 Kafka 的风险；Kafka 的数据在匿名卷里，重建即丢失。本脚本用 --no-deps
# --force-recreate taskmanager，只重建 TaskManager。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录；ssh 别名 fa-worker1、fa-worker2 可用。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-tm-recreate.sh
# 3. 前置条件 / Preconditions: 集群上**没有作业在运行**（重建 TaskManager 会让运行中的作业失败）；
#    本地 deploy/.env 已改好（例如 SYN_JAVACPP_MAXPHYSICALBYTES=3900m）。
# 4. 期望产出 / Expected output: 每台 worker 打印重建前后 Kafka 容器的创建时间（应相同）、TaskManager 的
#    新创建时间，以及 JavaCPP 物理内存上限的两个取值：容器配置里的值与运行中进程实际带的值（读 /proc，镜像里
#    没有 ps）。两者都应等于 .env 中的 SYN_JAVACPP_MAXPHYSICALBYTES，不一致时以退出码 1 结束。
# 5. 失败兜底 / Failure fallback: 发现有作业在运行时退出码 3，不做任何改动；.env 中某台 worker 的内网地址与
#    现有 TaskManager 注册的地址不同时跳过该台并以退出码 1 结束；某台 worker 的 Kafka 创建时间前后不同时
#    打印「Kafka 被重建」并以退出码 1 结束，此时请立即停下上报。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a
RHOME="${REMOTE_HOME:-/opt/fa-iforest}"

RUNNING="$(ssh fa-master "docker exec jobmanager flink list 2>/dev/null" | grep -E '\(RUNNING\)' || true)"
if [ -n "$RUNNING" ]; then
    echo "ERROR: 集群上有作业在运行，重建 TaskManager 会让它失败。请先取消我们自己的作业：" >&2
    echo "$RUNNING" | sed 's/^/  /' >&2
    exit 3
fi

rc=0
for spec in "fa-worker1 2 $NODE_WORKER1_IP taskmanager-2 kafka-2" "fa-worker2 3 $NODE_WORKER2_IP taskmanager-3 kafka-3"; do
    set -- $spec
    host=$1; broker=$2; ip=$3; tm=$4; kafka=$5
    echo "===== ${host} ====="
    # 新容器用 .env 中的内网地址向 JobManager 注册；与现有容器不同时不重建，以免注册到过时的地址上。
    # The new container registers with the .env IP; skip the host if it differs from the running container's.
    cur_ip="$(ssh "$host" "docker inspect $tm" 2>/dev/null | grep -o 'taskmanager.host: [0-9.]*' | head -1 | awk '{print $2}')"
    if [ "$cur_ip" != "$ip" ]; then
        echo "  ⚠ .env 中的地址 ${ip} 与现有 ${tm} 注册的地址 ${cur_ip:-?} 不同，跳过本台。请先核对 deploy/.env 再重试。"
        rc=1
        continue
    fi
    before="$(ssh "$host" "docker inspect -f '{{.Created}}' $kafka" 2>/dev/null)"
    scp -q "$DEPLOY_DIR/.env" "$host:$RHOME/.env"
    scp -q "$DEPLOY_DIR/compose/docker-compose.worker.yml" "$host:$RHOME/compose/docker-compose.worker.yml"
    ssh "$host" "cd $RHOME/compose && BROKER_ID=$broker NODE_SELF_IP=$ip docker compose -f docker-compose.worker.yml \
        --env-file ../.env up -d --no-deps --force-recreate taskmanager" 2>&1 | tail -3
    sleep 5
    after="$(ssh "$host" "docker inspect -f '{{.Created}}' $kafka" 2>/dev/null)"
    echo "Kafka 创建时间：重建前 ${before}；重建后 ${after}"
    if [ "$before" != "$after" ]; then
        echo "  ⚠ Kafka 被重建，请立即停下上报。"
        rc=1
    fi
    echo "TaskManager 创建时间：$(ssh "$host" "docker inspect -f '{{.Created}}' $tm" 2>/dev/null)"
    # 镜像里没有 ps，直接读 /proc 下各进程的命令行；同时列出容器配置里的值作对照。
    # The image has no ps: read each process's command line from /proc; also show the configured value.
    cfg="$(ssh "$host" "docker inspect $tm" 2>/dev/null | grep -o 'maxphysicalbytes=[^ \"]*' | head -1 | cut -d= -f2)"
    # 只看 TaskManager 的 Java 进程（命令行含 TaskManagerRunner）；容器刚重建时 JVM 可能尚未启动，最多等 30 秒。
    # Only the TaskManager JVM (its command line contains TaskManagerRunner); wait up to 30 s for it to start.
    run=""
    for _ in 1 2 3 4 5 6; do
        run="$(ssh "$host" "docker exec -i $tm sh -s" 2>/dev/null <<'EOS'
for f in /proc/[0-9]*/cmdline; do tr '\000' ' ' < "$f"; echo; done 2>/dev/null | grep TaskManagerRunner | grep -o 'maxphysicalbytes=[^ ]*' | head -1 | cut -d= -f2
EOS
)"
        [ -n "$run" ] && break
        sleep 5
    done
    echo "JavaCPP 物理内存上限：容器配置 ${cfg:-?}；运行中进程 ${run:-?}（期望 ${SYN_JAVACPP_MAXPHYSICALBYTES:-3900m}）"
    if [ "$run" != "${SYN_JAVACPP_MAXPHYSICALBYTES:-3900m}" ]; then
        echo "  ⚠ 运行中进程的取值与 .env 不一致。"
        rc=1
    fi
done
exit $rc
