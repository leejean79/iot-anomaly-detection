#!/usr/bin/env bash
# ============================================================================
# lint-scripts.sh
# 交付前自检：对 deploy/scripts/ 下所有 shell 脚本做两项检查——语法解析，以及「$变量 紧跟中文
# 字符」这一类只在部分 bash 版本上才暴露的写法问题。
# Pre-delivery lint for deploy/scripts/*.sh: syntax parse, plus unbraced variables directly
# followed by a multi-byte character.
#
# 【为什么需要它】本项目的脚本在 Linux 容器里写、在 macOS 上跑。macOS 自带 bash 3.2，对多字节
# 字符的处理与新版 bash 不同：`"$FRAME（期望值…）"` 这种写法，新版 bash 正常，bash 3.2 会把全角
# 括号的首字节并进变量名，配合 `set -u` 直接报 `FRAME?: unbound variable`，脚本当场中断。这类问题
# 在开发机上完全复现不出来，只能靠静态检查拦住。
# Why: these scripts are written on Linux and run on macOS, whose bash 3.2 folds the leading byte of
# a multi-byte character into the variable name; with `set -u` the script aborts. Not reproducible
# on newer bash, so it has to be caught statically.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 任意有 bash + python3 的机器；仓库根目录。无需集群、无需 JDK。
# 2. 调用命令 / Invocation:
#      bash deploy/scripts/lint-scripts.sh
# 3. 前置条件 / Preconditions: 无。
# 4. 期望产出 / Expected output: 逐项 OK，末尾「全部通过」，退出码 0；有问题时逐条打印
#      文件、行号与问题片段，退出码 1。
# 5. 常见失败兜底 / Failure fallback:
#      「$变量 紧跟多字节字符」→ 改写成 ${变量}；
#      语法错误 → 按 bash -n 给出的行号修正。
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAILED=0

echo "───────── 1/2 语法解析 / bash -n"
for f in "$SCRIPT_DIR"/*.sh; do
    if bash -n "$f" 2>/tmp/lint-err.$$; then
        printf "  [OK]   %s\n" "$(basename "$f")"
    else
        printf "  [FAIL] %s\n" "$(basename "$f")"
        sed 's/^/         /' /tmp/lint-err.$$
        FAILED=1
    fi
done
rm -f /tmp/lint-err.$$

echo ""
echo "───────── 2/2 未加花括号的变量紧跟多字节字符 / unbraced \$VAR before a multi-byte char"
if python3 - "$SCRIPT_DIR" <<'PY'
import io, re, sys, glob, os
pat = re.compile(r'\$[A-Za-z_][A-Za-z0-9_]*(?=[^\x00-\x7f])')
hits = 0
for p in sorted(glob.glob(os.path.join(sys.argv[1], '*.sh'))):
    for i, line in enumerate(io.open(p, encoding='utf-8'), 1):
        if line.lstrip().startswith('#'):      # 注释不影响执行 / comments do not execute
            continue
        for m in pat.finditer(line):
            print("  [FAIL] %s:%d  %s  → 改写成 ${%s}"
                  % (os.path.basename(p), i, m.group(0), m.group(0)[1:]))
            print("         %s" % line.strip()[:100])
            hits += 1
print("  [OK]   未发现此类写法" if hits == 0 else "  合计 %d 处" % hits)
sys.exit(1 if hits else 0)
PY
then :; else FAILED=1; fi

echo ""
if [ "$FAILED" -eq 0 ]; then
    echo "✅ 全部通过。"
    exit 0
else
    echo "⛔ 有检查未通过，见上方逐条。"
    exit 1
fi
