#!/usr/bin/env python3
# ============================================================================
# m6a/compare.py
# 零级重估的跨语言比较（验收项一 V-M6a-1 的「Java 重估函数与 Python 在四个非光照通道逐位相同、光照通道容差 1e-12」）：
# 逐值比较 m6a/reestimate.py 与 com.leejean.m6.M6aOfflineRunner 的 JSON。冻结与重估的中位数、分母、旁路，参照直方图的边界、
# 频数、P10、中位数、P90，回填附件的时间戳与标准化值；光照通道（下标 4）的浮点数容差 1e-12，其余一律逐位相同，整数一律相等。
# Cross-language comparison of the level-0 re-estimation: bitwise except the Light channel (tolerance 1e-12).
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m6a/compare.py docs/m6a/sim/G_py.json docs/m6a/sim/G_java.json
# 3. 前置条件 / Preconditions: 两份 JSON 由同一轮表、同一组参数生成。
# 4. 期望产出 / Expected output: 打印「逐位相同（光照最大差 …）」或逐条列出不同之处。
# 5. 失败兜底 / Failure fallback: 有不同之处时退出码 1；文件缺失时退出码 2。
# ============================================================================
import json
import os
import sys

LIGHT = 4
TOL = 1e-12


class Cmp:
    def __init__(self):
        self.diffs = []
        self.light_max = 0.0
        self.n = 0

    def num(self, a, b, light, where):
        self.n += 1
        if isinstance(a, bool) or isinstance(b, bool) or isinstance(a, int) and isinstance(b, int):
            if a != b:
                self.diffs.append("%s：%r ≠ %r" % (where, a, b))
            return
        a, b = float(a), float(b)
        if light:
            d = abs(a - b)
            self.light_max = max(self.light_max, d)
            if d > TOL:
                self.diffs.append("%s：光照差 %.3e 超过容差（%r，%r）" % (where, d, a, b))
        elif a.hex() != b.hex():
            self.diffs.append("%s：%r ≠ %r" % (where, a, b))

    def seq(self, a, b, light, where):
        if len(a) != len(b):
            self.diffs.append("%s：长度 %d ≠ %d" % (where, len(a), len(b)))
            return
        for i, (x, y) in enumerate(zip(a, b)):
            self.num(x, y, light, "%s[%d]" % (where, i))


def compare(p, j):
    """逐值比较两份零级 JSON，返回 Cmp（diffs 为空即相同）/ compare two level-0 JSONs."""
    c = Cmp()
    for k in ("effectiveAt", "version", "freezeTs", "windowRounds", "guardExcluded"):
        c.num(p[k], j[k], False, k)
    for part in ("freeze", "new"):
        for k in ("median", "scale", "bypass"):
            for ch in range(5):
                c.num(p[part][k][ch], j[part][k][ch], ch == LIGHT, "%s.%s[%d]" % (part, k, ch))
    for ch in range(5):
        hp, hj = p["hist"][ch], j["hist"][ch]
        if (hp is None) != (hj is None):
            c.diffs.append("hist[%d]：一边为空" % ch)
            continue
        if hp is None:
            continue
        light = ch == LIGHT
        c.seq(hp["edges"], hj["edges"], light, "hist[%d].edges" % ch)
        c.seq(hp["cells"], hj["cells"], False, "hist[%d].cells" % ch)
        for k in ("m", "p10", "median", "p90"):
            c.num(hp[k], hj[k], light and k != "m", "hist[%d].%s" % (ch, k))
    c.seq(p["backfill"]["ts"], j["backfill"]["ts"], False, "backfill.ts")
    if len(p["backfill"]["xNorm"]) == len(j["backfill"]["xNorm"]):
        for i, (a, b) in enumerate(zip(p["backfill"]["xNorm"], j["backfill"]["xNorm"])):
            for ch in range(5):
                c.num(a[ch], b[ch], ch == LIGHT, "backfill.xNorm[%d][%d]" % (i, ch))
    return c


def main() -> int:
    if len(sys.argv) != 3 or not all(os.path.exists(p) for p in sys.argv[1:]):
        print("用法：python3 m6a/compare.py <python.json> <java.json>", file=sys.stderr)
        return 2
    p, j = (json.load(open(f, encoding="utf-8")) for f in sys.argv[1:])
    c = compare(p, j)
    if c.diffs:
        print("有 %d 处不同（共比较 %d 个值），前 20 处：" % (len(c.diffs), c.n))
        for d in c.diffs[:20]:
            print("  " + d)
        return 1
    print("逐位相同：共比较 %d 个值；光照通道最大差 %.3e（容差 %.0e）" % (c.n, c.light_max, TOL))
    return 0


if __name__ == "__main__":
    sys.exit(main())
