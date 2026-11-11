#!/usr/bin/env bash
# ============================================================================
# syn-adapt-send.sh
# 手工命令工具（适应执行第一批交接文档第三节）：向命令主题 synergia-adapt 写入一条零级命令，字段与聚合模块发出的命令相同。
# 生效时刻取给定当地日的日界（Europe/London 零点），决定时刻取其前一天的日界，命令序号默认一（执行后参数版本号为二）。
# Manual command tool: write one level-0 command to synergia-adapt, in the same JSON as the M7 commands.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac（bash，经 ssh fa-master）或集群 master（SYN_ON_MASTER=1）；仓库根目录；需要 python3。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/syn-adapt-send.sh --device G --day 2022-03-28            # 生效于 03-28 当地零点，序号 1
#      bash deploy/scripts/syn-adapt-send.sh --device G --day 2022-03-28 --seq 2    # 第二条命令
#      bash deploy/scripts/syn-adapt-send.sh --device G --effective 1648422000      # 直接给生效时刻（秒，须为 60 的整数倍）
#      bash deploy/scripts/syn-adapt-send.sh --device G --day 2022-03-28 --dry-run  # 只打印命令，不写主题
#    可选 --topic（默认 synergia-adapt）、--level 0|1|2（默认 0；一级与二级由数据接入模块转发给上下文模块，M6b）。
# 3. 前置条件 / Preconditions: 命令主题已建（syn-create-topics.sh，一个分区）；作业以 --adapt-enabled true 运行或即将提交。
#    命令须在数据流的水位线越过生效时刻之前写入，否则记为迟到、不执行；在提交作业之后、重放到达生效日之前写入即可。
# 4. 期望产出 / Expected output: 打印写入的一行 JSON；作业的 TaskManager 日志出现「[M6a] Command stored」，到生效时刻出现
#    「[M6a] Level 0 executed」。
# 5. 失败兜底 / Failure fallback: 生效时刻不是 60 的整数倍时拒绝（点通道按滑动步判换版本，生效时刻须落在滑动步边界）；
#    写主题失败时退出码非零，检查 kafka-1 容器是否在运行。
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
set -a; source "$DEPLOY_DIR/.env"; set +a

DEVICE=""
DAY=""
EFFECTIVE=""
SEQ=1
LEVEL=0
TOPIC="${SYN_TOPIC_ADAPT:-synergia-adapt}"
DRY=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --device) DEVICE="$2"; shift 2 ;;
        --day) DAY="$2"; shift 2 ;;
        --effective) EFFECTIVE="$2"; shift 2 ;;
        --seq) SEQ="$2"; shift 2 ;;
        --level) LEVEL="$2"; shift 2 ;;
        --topic) TOPIC="$2"; shift 2 ;;
        --dry-run) DRY=1; shift ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done
[ -n "$DEVICE" ] || { echo "ERROR: 需要 --device" >&2; exit 2; }
[ -n "$DAY" ] || [ -n "$EFFECTIVE" ] || { echo "ERROR: 需要 --day 或 --effective" >&2; exit 2; }

[[ "$LEVEL" =~ ^[012]$ ]] || { echo "ERROR: --level 只能是 0、1 或 2" >&2; exit 2; }

LINE="$(python3 - "$DEVICE" "$DAY" "$EFFECTIVE" "$SEQ" "$LEVEL" <<'PY'
import json, sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
dev, day, eff, seq, level = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
z = ZoneInfo("Europe/London")
if day:
    d = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=z)
    eff = int(d.timestamp())
    decided = int((d - timedelta(days=1)).timestamp())
    dday = (d - timedelta(days=1)).strftime("%Y-%m-%d")
else:
    eff = int(eff)
    decided = eff - 86400
    dday = datetime.fromtimestamp(decided, z).strftime("%Y-%m-%d")
if eff % 60:
    sys.exit("ERROR: 生效时刻 %d 不是 60 的整数倍" % eff)
print(json.dumps({"device": dev, "day": dday, "decidedAt": decided, "effectiveAt": eff, "seq": seq, "level": level,
                  "scope": "手工", "label": "手工命令", "triggers": "", "forms": "", "co": 0, "coDevices": "",
                  "episodeN": 0, "type": "command"}, ensure_ascii=False, separators=(",", ":")))
PY
)"
echo "$LINE"
[ "$DRY" -eq 1 ] && exit 0

BROKERS="$NODE_MASTER_IP:9092,$NODE_WORKER1_IP:9092,$NODE_WORKER2_IP:9092"
CMD="docker exec -i kafka-1 kafka-console-producer.sh --bootstrap-server $BROKERS --topic $TOPIC \
    --property parse.key=true --property key.separator=\$'\\t'"
if [ "${SYN_ON_MASTER:-0}" = "1" ]; then
    printf '%s\t%s\n' "$DEVICE" "$LINE" | bash -c "$CMD"
else
    printf '%s\t%s\n' "$DEVICE" "$LINE" | ssh fa-master "$CMD"
fi
echo "已写入 $TOPIC（键 $DEVICE）/ written."
