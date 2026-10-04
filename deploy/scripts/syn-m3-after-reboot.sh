#!/usr/bin/env bash
# ============================================================================
# syn-m3-after-reboot.sh
# 运行结束后、收尾之前，集群实例被停止或重启过时，判断这次运行还能收回多少（只读）。
# After the instances were stopped or rebooted between the end of a run and its wrap-up, check how much
# of the run is still recoverable (read-only).
#
# 依次查看 / checks, in order:
#   1. 三台节点的开机时长、容器的创建时间与状态。容器创建时间早于重启时刻，说明是同一个容器重新启动，
#      Kafka 的数据卷与 docker 日志都还在；创建时间晚于重启时刻，说明容器被重建过，数据卷可能已换新。
#   2. Flink 上的作业列表（重启后原作业不会自动恢复，这里应当看不到它在运行）。
#   3. synergia-m1-out、synergia-scores、synergia-monitoring 三个主题的末端偏移合计（消息总数）。
#   4. master 上无人值守监测的输出目录是否还在。
#   5. 从 Prometheus 取点通道迟到丢弃计数 m2_gate_late_drop（作业已不存在，Flink 接口读不到，Prometheus
#      保留着运行期间的采样）。给了 --late-drop-out 时，按 syn-m2-metrics.sh 的格式写一行到该文件，
#      供 m3_dual_channel_week.py --metrics 读取。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac，仓库根目录；ssh 别名 fa-master、fa-worker1、fa-worker2 可用。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-m3-after-reboot.sh --since 2026-10-03T08:00:00Z \
#          --late-drop-out docs/m3_marapr/m2_metrics_final.txt
#    --since 为第五步提交作业前记下的 SINCE；--late-drop-out 可省略（省略时只打印，不写文件）。
# 3. 前置条件 / Preconditions: 三台节点都能 ssh 登录；master 上 Prometheus 容器在运行。
# 4. 期望产出 / Expected output: 终端打印五段结果；有 --late-drop-out 时写出一行
#    「  m2_gate_late_drop   <数值>」。
# 5. 失败兜底 / Failure fallback: 某节点 ssh 失败时该段显示「无法登录」并继续；Prometheus 查不到该计数时
#    打印「Prometheus 没有该计数」，此时不写文件，需要上报。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

SINCE=""; LATE_OUT=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --since) SINCE="$2"; shift 2 ;;
        --late-drop-out) LATE_OUT="$2"; shift 2 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done
[ -z "$SINCE" ] && { echo "ERROR: 需要 --since（第五步记下的 SINCE）" >&2; exit 2; }
BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"

echo "== 一、开机时长与容器（创建时间早于重启时刻 = 同一个容器，数据还在）=="
for n in fa-master fa-worker1 fa-worker2; do
    echo "-- ${n}"
    ssh -o ConnectTimeout=10 "$n" "uptime -s | sed 's/^/开机时刻 /'; \
        docker ps -a --format '{{.Names}}|创建于 {{.CreatedAt}}|{{.Status}}'" 2>/dev/null || echo "   无法登录"
done

echo ""
echo "== 二、Flink 作业列表 =="
ssh fa-master "docker exec jobmanager flink list -a 2>/dev/null | grep -E ' : ' || echo '没有任何作业记录'"

echo ""
echo "== 三、主题的消息总数（末端偏移合计）=="
for t in synergia-m1-out synergia-scores synergia-monitoring; do
    n="$(ssh fa-master "docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list $BROKERS --topic $t --time -1 2>/dev/null" | awk -F: '{s+=$3} END{print s+0}')"
    echo "${t}：${n}"
done

echo ""
echo "== 四、无人值守监测的输出目录 =="
ssh fa-master "ls -la ${REMOTE_HOME:-/opt/fa-iforest}/m3watch 2>/dev/null | head -15 || echo '目录不存在'"

echo ""
echo "== 五、点通道迟到丢弃计数（Prometheus）=="
RANGE_SEC=$(python3 -c "
import datetime as d,sys
t=d.datetime.strptime(sys.argv[1],'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=d.timezone.utc)
print(int((d.datetime.now(d.timezone.utc)-t).total_seconds())+60)" "$SINCE")
Q="max_over_time(flink_taskmanager_job_task_operator_m2_gate_late_drop[${RANGE_SEC}s])"
RES="$(ssh fa-master "curl -s --max-time 30 'http://localhost:9090/api/v1/query' --data-urlencode 'query=${Q}'")"
LATE="$(printf '%s' "$RES" | python3 -c "
import json,sys
try:
    r=json.load(sys.stdin)['data']['result']
except Exception:
    r=[]
if not r:
    print('')
else:
    print('%d %d' % (sum(float(x['value'][1]) for x in r), len(r)))")"
if [ -z "$LATE" ]; then
    echo "Prometheus 没有该计数：无法核对迟到丢弃，请把本段输出告诉代码开发代理。"
else
    set -- $LATE
    echo "迟到丢弃合计：$1（来自 $2 个子任务的序列，取运行期间各自的最大值再求和）"
    if [ -n "$LATE_OUT" ]; then
        mkdir -p "$(dirname "$LATE_OUT")"
        printf '# 来源：Prometheus，%s 起的运行期间（作业已不存在，Flink 接口读不到）\n  m2_gate_late_drop            %s\n' \
            "$SINCE" "$1" > "$LATE_OUT"
        echo "已写出：${LATE_OUT}"
    fi
fi
