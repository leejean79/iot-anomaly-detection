#!/usr/bin/env bash
# ============================================================================
# check-jar.sh
# 检查一个 shaded jar 是否包含最新代码标记（不反编译，直接扒 .class 常量池里的字段名/方法名/字符串常量）。
# Verify a shaded jar contains the latest code markers by grepping the .class constant pool
# (field names, method names, string literals) — no decompiler needed.
#
# 为什么这样可行：Java .class 文件的常量池以 UTF-8 明文保存字段名、方法名与字符串字面量；maven-shade
# 默认不混淆，故 `unzip -p jar <class> | grep -a <标记>` 能可靠判断某段代码是否被打进这个 jar。
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 任意有 unzip + grep 的机器（Mac 自带；集群 master 亦可）。无需 JDK。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/check-jar.sh                                   # 默认查 target/ 下刚打的 jar
#      bash deploy/scripts/check-jar.sh /opt/fa-iforest/jars/iot-anomaly-detection-1.0-SNAPSHOT.jar
#      # 在集群上查 jobmanager 真正加载的那个：
#      ssh fa-master "docker exec jobmanager bash -lc 'unzip -p /opt/flink/usrlib/<jar> \
#          com/leejean/m1/MonitoringSnapshot.class | grep -a -c m2ColdCleared'"
# 3. 前置条件 / Preconditions: jar 存在且可读。
# 4. 期望产出 / Expected output: 逐标记 PASS/FAIL + 总判定（全 PASS = 该 jar 含最新内容）。
# 5. 失败兜底 / Failure fallback: jar 不存在 → 报错退出；某标记 FAIL → 该 jar 是旧的，需 mvn clean package 重打。
#
# 缩写自查 / Abbreviations: 常量池 = class 文件里存符号名与字面量的区段；shaded jar = 打进依赖的胖 jar。
# ============================================================================
set -uo pipefail

JAR="${1:-}"
if [ -z "$JAR" ]; then
    # 默认优先本地刚打的 jar
    for cand in \
        "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/target/iot-anomaly-detection-1.0-SNAPSHOT.jar" \
        "./target/iot-anomaly-detection-1.0-SNAPSHOT.jar"; do
        [ -f "$cand" ] && { JAR="$cand"; break; }
    done
fi
if [ -z "$JAR" ] || [ ! -f "$JAR" ]; then
    echo "ERROR: 找不到 jar。用法：bash deploy/scripts/check-jar.sh <jar 路径>" >&2
    echo "  （本地默认查 target/iot-anomaly-detection-1.0-SNAPSHOT.jar）" >&2
    exit 2
fi

echo "===================================="
echo "检查 jar / inspecting: $JAR"
echo "  大小：$(ls -lh "$JAR" | awk '{print $5}')  修改时间：$(date -r "$JAR" '+%F %T' 2>/dev/null || stat -c '%y' "$JAR" 2>/dev/null)"
echo "===================================="

# 待检标记：class 路径 | 标记字符串 | 说明（对应提交）
# 每行一个；grep -a 把二进制当文本，-c 计数（>0 即含）。
checks=(
    "com/leejean/m1/MonitoringSnapshot.class|m2ColdCleared|冷启动清空监测字段（提交 ae4f42c）"
    "com/leejean/m2/PmcodFunction.class|setM2ColdCleared|PmcodFunction 写入冷启动标记"
    "com/leejean/m2/M2Job.class|mcod-r-per-device|逐设备 R 参数（提交 6db8d95）"
    "com/leejean/m2/M2Job.class|parseRPerDevice|逐设备 R 解析方法"
    "com/leejean/m2/M2Probe.class|dispersion-out|通道离散度诊断（提交 6db8d95）"
)

fail=0
for row in "${checks[@]}"; do
    cls="${row%%|*}"; rest="${row#*|}"; tok="${rest%%|*}"; desc="${rest#*|}"
    n=$(unzip -p "$JAR" "$cls" 2>/dev/null | grep -a -c "$tok" 2>/dev/null || echo 0)
    n=$(echo "$n" | tr -d '[:space:]')
    if [ "${n:-0}" -gt 0 ]; then
        printf "  [PASS] %-22s in %-40s — %s\n" "$tok" "$(basename "$cls")" "$desc"
    else
        printf "  [FAIL] %-22s in %-40s — %s\n" "$tok" "$(basename "$cls")" "$desc"
        fail=1
    fi
done

echo "===================================="
if [ "$fail" -eq 0 ]; then
    echo "✅ 全部 PASS：该 jar 含最新内容（m2ColdCleared + 逐设备 R + 离散度）。"
    echo "   若集群跑出来仍缺这些，则问题在**部署环节**（上传未覆盖 / jobmanager 加载了别处的旧 jar），"
    echo "   请核对 syn-upload-m1.sh 的目标路径与 syn-submit-m2.sh 里 /opt/flink/usrlib/<jar> 是否同一个。"
else
    echo "❌ 有 FAIL：该 jar 是旧的（未含上述改动）。请 mvn clean package 重新打包后再 syn-upload-m1.sh --jar-only。"
fi
exit $fail
