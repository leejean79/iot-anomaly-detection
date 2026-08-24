#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# run_smoke_test.sh —— EDA 工具链端到端自测（不需要真实数据集）。
# run_smoke_test.sh -- end-to-end self-test of the EDA toolchain (no real data needed).
#
# ============================== 脚本交付五要素 ==============================
# 1. 执行环境 / environment：bash，Python 3.9+，已装 pandas / numpy / matplotlib
# 2. 调用命令 / invocation：bash eda/tests/run_smoke_test.sh [工作目录 / workdir]
#    （默认工作目录 / default workdir: /tmp/eda_smoke）
# 3. 前置条件 / preconditions：工作目录可写；仓库根目录下执行
# 4. 期望产出 / expected outputs：控制台逐项 PASS/FAIL；工作目录下的合成数据与 EDA 产出
# 5. 失败兜底 / failure fallback：任一检查失败即以非零退出码结束并打印失败项，
#    中间产物保留在工作目录供排查（不自动清理）
#
# 覆盖的验收项 / acceptance criteria covered:
#   - --limit 冒烟运行与全量运行均无崩溃
#   - 畸形行 / 异常文件有计数入报告
#   - 断点续跑实测有效（第二次运行全部命中缓存且结果一致）
#   - 缓存损坏可自愈；真实样例文件可解析
#   - 五层产出齐全、§6 八问齐答
# -----------------------------------------------------------------------------
set -u

WORK="${1:-/tmp/eda_smoke}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATA="$WORK/synth"
OUT="$WORK/out"
FIXTURE_OUT="$WORK/out_fixture"
FAILED=0

pass() { echo "  [PASS] $1"; }
fail() { echo "  [FAIL] $1"; FAILED=$((FAILED + 1)); }
check() { if [ "$1" -eq 0 ]; then pass "$2"; else fail "$2"; fi; }

echo "== 0. 准备 / setup =========================================="
rm -rf "$WORK"; mkdir -p "$WORK"
python3 "$REPO/eda/tests/make_test_data.py" --out-dir "$DATA" --days 70 --files-per-day 2 \
  --rounds 40 > "$WORK/gen.log" 2>&1
check $? "合成数据生成 / synthetic dataset generated ($(ls "$DATA" | wc -l) files)"

echo "== 1. 冒烟运行 --limit 5 / smoke run ========================"
python3 "$REPO/eda/run_eda.py" --data-dir "$DATA" --output-dir "$OUT" --limit 5 --workers 2 \
  > "$WORK/limit.log" 2>&1
check $? "--limit 5 无崩溃 / no crash"

echo "== 2. 全量运行 / full run ==================================="
python3 "$REPO/eda/run_eda.py" --data-dir "$DATA" --output-dir "$OUT" --workers 3 \
  > "$WORK/full.log" 2>&1
check $? "全量扫描无崩溃 / full scan without crash"
grep -q "畸形跳过" "$WORK/full.log" && grep -q "失败" "$WORK/full.log"
check $? "畸形行与异常文件已计数 / malformed rows and bad files counted"

echo "== 2b. 补丁 01：发现完整性 / patch-01 discovery ============="
# 发现文件数须等于 find -type f（补丁 01 核心验收）
find_count=$(find "$DATA" -type f | wc -l | tr -d ' ')
inv_count=$(( $(wc -l < "$OUT/file_inventory.csv") - 1 ))   # 去表头 / minus header
[ "$find_count" -eq "$inv_count" ]
check $? "发现数 = find -type f（$find_count = ${inv_count}）/ discovery == find count, zero silent skips"
grep -q "一致 / matches" "$WORK/full.log"
check $? "归属恒等式 ok+failed+non_data = 发现数 / accounting identity balances"
[ -s "$OUT/unmatched_files.csv" ]
check $? "unmatched_files.csv 已交付 / unmatched_files.csv delivered"
# 非数据文件应出现在 unmatched 清单 / non-data files listed
grep -q "README.txt" "$OUT/unmatched_files.csv" && grep -q "archive_2022.zip" "$OUT/unmatched_files.csv"
check $? "非数据文件进入 unmatched 清单 / non-data files present in unmatched list"
# 非 .csv 扩展名的数据文件应被并入统计（included_in_stats=1）
grep -E "2022_02_03_120000\.data,.*,1,1," "$OUT/unmatched_files.csv" >/dev/null 2>&1 || \
  awk -F, '$2 ~ /2022_02_03_1200/ && $7==1' "$OUT/unmatched_files.csv" | grep -q .
check $? "非 .csv 扩展名数据文件已并入统计 / non-.csv data files included in stats"

echo "== 3. 断点续跑 / resume ====================================="
cp "$OUT/aggregate.json" "$WORK/aggregate_first.json"
python3 "$REPO/eda/run_eda.py" --data-dir "$DATA" --output-dir "$OUT" --workers 3 \
  > "$WORK/resume.log" 2>&1
n_total=$(find "$DATA" -type f | wc -l | tr -d ' ')
grep -q "缓存命中 / cache hits : $n_total / $n_total" "$WORK/resume.log"
check $? "第二次运行全部命中缓存 / every file served from cache"
python3 - "$WORK/aggregate_first.json" "$OUT/aggregate.json" <<'PY'
# 浮点容差比较：进程池乱序归并使 Welford 合并的求和顺序不同，均值/M2 会有
# 1e-15 量级的舍入差异——这是浮点非结合性，不是结果不一致。
# Tolerant comparison: out-of-order pool merging changes the summation order in the
# Welford combination, so means and M2 differ at the 1e-15 level. That is floating-point
# non-associativity, not an inconsistent result.
import json, math, sys

def same(a, b, path=""):
    if isinstance(a, float) or isinstance(b, float):
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            if math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9):
                return True
            print("差异 / differs at %s: %r vs %r" % (path, a, b))
            return False
    if isinstance(a, dict) and isinstance(b, dict):
        if set(a) != set(b):
            print("键集合不同 / key sets differ at %s" % path)
            return False
        return all(same(a[k], b[k], "%s/%s" % (path, k)) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            print("列表长度不同 / list length differs at %s" % path)
            return False
        return all(same(x, y, "%s[%d]" % (path, i)) for i, (x, y) in enumerate(zip(a, b)))
    if a != b:
        print("差异 / differs at %s: %r vs %r" % (path, a, b))
        return False
    return True

a = json.load(open(sys.argv[1])); b = json.load(open(sys.argv[2]))
a.pop("run", None); b.pop("run", None)
sys.exit(0 if same(a, b) else 1)
PY
check $? "续跑结果与首跑一致（浮点容差内）/ resumed aggregate matches the first run"

echo "== 4. 缓存损坏自愈 / cache corruption recovery =============="
echo "garbage" > "$(ls "$OUT"/_cache/*.json.gz | head -1)"
python3 "$REPO/eda/run_eda.py" --data-dir "$DATA" --output-dir "$OUT" --workers 2 \
  > "$WORK/corrupt.log" 2>&1
check $? "损坏缓存不致崩溃 / corrupt cache does not crash the run"

echo "== 5. 报告生成 / report generation =========================="
python3 "$REPO/eda/report_gen.py" --output-dir "$OUT" > "$WORK/report.log" 2>&1
check $? "report_gen.py 无崩溃 / no crash"
grep -q "## 7. 补丁 01" "$OUT/eda_report.md"
check $? "报告含补丁 01 章节 / report contains the patch-01 section"

# 基线差异表：以自身为基线应产出"无变化" / diff table: self-baseline should show no change
python3 "$REPO/eda/report_gen.py" --output-dir "$OUT" --baseline "$WORK/aggregate_first.json" \
  > "$WORK/report_diff.log" 2>&1
check $? "带 --baseline 的报告生成无崩溃 / report with --baseline runs"

for f in file_inventory.csv unmatched_files.csv device_sensor_counts.csv uptime_matrix.csv gaps_top100.csv \
         round_completeness.csv channel_stats.csv channel_stats_monthly.csv accel_nonzero.csv \
         unit_sanity.csv interarrival_quantiles.csv clock_skew_sample.csv \
         ks_adjacent_months.csv seasonal_trend.png eda_report.md \
         monthly_quantile_drift_temperature.png; do
  [ -s "$OUT/$f" ]
  check $? "产出存在且非空 / output present: $f"
done

for q in "### Q1." "### Q2." "### Q3." "### Q4." "### Q5." "### Q6." "### Q7." "### Q8."; do
  grep -q "$q" "$OUT/eda_report.md"
  check $? "报告含 $q / report contains $q"
done

echo "== 6. 真实样例文件 / real sample fixture ===================="
python3 "$REPO/eda/run_eda.py" --data-dir "$REPO/eda/tests/fixtures" \
  --output-dir "$FIXTURE_OUT" --workers 1 > "$WORK/fixture.log" 2>&1 &&
  python3 "$REPO/eda/report_gen.py" --output-dir "$FIXTURE_OUT" >> "$WORK/fixture.log" 2>&1
check $? "真实样例文件端到端通过 / real fixture processed end to end"
grep -q "0 畸形跳过" "$WORK/fixture.log"
check $? "真实样例无畸形行误判 / no false malformed rows on real data"

echo "============================================================"
if [ "$FAILED" -eq 0 ]; then
  echo "全部检查通过 / all checks passed（产出目录 / outputs: ${WORK}）"
  exit 0
fi
echo "$FAILED 项检查失败 / checks failed（日志与产出保留在 / logs kept in: ${WORK}）"
exit 1
