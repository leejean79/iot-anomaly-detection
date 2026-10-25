#!/usr/bin/env python3
# ============================================================================
# m5a/stats.py
# M5a 的统计函数（标准库实现，供离线判读与等值核对使用）：分布检验的渐近显著性、卡方分布的上尾概率（正则化上不完全伽马函数）。
# 与 scipy 的 `ks_2samp(method="asymp")` 的极限分布、`chi2.sf` 一致（自检：`python3 m5a/stats.py`）。
# Statistics for M5a in the standard library: asymptotic KS p-value and the chi-square upper tail.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 任意有 python3 的机器；只用标准库。
# 2. 调用命令 / Invocation: 作为模块导入（`import stats`）；自检 `python3 m5a/stats.py`。
# 3. 前置条件 / Preconditions: 无。
# 4. 期望产出 / Expected output: 自检打印「自检通过」。
# 5. 失败兜底 / Failure fallback: 自检失败时抛出 AssertionError。
# ============================================================================
import math


def ks_p(d, n, m):
    """渐近显著性 Q_KS(√(nm/(n+m))·D)，Q_KS(λ) = 2 Σ (−1)^{k−1} e^{−2k²λ²} / asymptotic KS p-value."""
    lam = math.sqrt(n * m / (n + m)) * d
    if lam < 1e-3:
        return 1.0
    s = sum((-1) ** (k - 1) * math.exp(-2 * k * k * lam * lam) for k in range(1, 101))
    return max(0.0, min(1.0, 2 * s))


def chi2_sf(x, df):
    """卡方分布上尾概率 = Q(df/2, x/2)，正则化上不完全伽马函数（级数与连分式，Numerical Recipes 的 gammq）。"""
    if x <= 0:
        return 1.0
    a, z = df / 2.0, x / 2.0
    gln = math.lgamma(a)
    if z < a + 1:                                     # 级数求 P，再取 1 − P / series for P
        ap, s, term = a, 1.0 / a, 1.0 / a
        for _ in range(10000):
            ap += 1
            term *= z / ap
            s += term
            if abs(term) < abs(s) * 1e-15:
                break
        return max(0.0, 1.0 - s * math.exp(-z + a * math.log(z) - gln))
    b = z + 1 - a                                     # 连分式求 Q / continued fraction for Q
    c, d = 1.0 / 1e-300, 1.0 / b
    h = d
    for i in range(1, 10000):
        an = -i * (i - a)
        b += 2
        d = an * d + b
        d = 1e-300 if abs(d) < 1e-300 else d
        c = b + an / c
        c = 1e-300 if abs(c) < 1e-300 else c
        d = 1.0 / d
        de = d * c
        h *= de
        if abs(de - 1) < 1e-15:
            break
    return max(0.0, h * math.exp(-z + a * math.log(z) - gln))


if __name__ == "__main__":
    try:
        from scipy.stats import chi2, kstwobign
    except ImportError:
        print("没有 scipy，跳过对照，只做基本检查。")
        assert abs(chi2_sf(2.0, 2) - math.exp(-1)) < 1e-12
    else:
        for x, df in [(0.5, 1), (3.0, 4), (400, 402), (450, 402), (600, 402), (1200, 380), (50, 83), (1e5, 400)]:
            a, b = chi2_sf(x, df), chi2.sf(x, df)
            assert abs(a - b) <= 1e-12 + 1e-9 * b, (x, df, a, b)
        for lam in (0.3, 0.8, 1.2, 1.63, 2.5):
            n = m = 2 * lam * lam                      # √(nm/(n+m)) = lam
            assert abs(ks_p(1.0, n, m) - kstwobign.sf(lam)) < 1e-12
    print("自检通过。")
