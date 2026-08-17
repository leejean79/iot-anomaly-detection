"""
增量统计原语：Welford 累加器、稀疏直方图、计数字典合并。
Incremental statistics primitives: Welford accumulators, sparse histograms,
counter-dict merging.

设计约束 / design constraint:
全部原语必须 (a) JSON 可序列化——逐文件部分聚合要落盘供断点续跑；
(b) 可结合（associative）——合并顺序不影响结果，才能进程池乱序归并。
Every primitive must be (a) JSON-serialisable, because per-file partials are cached
to disk for resume, and (b) associative, so that pool results can be merged in any order.

Welford 说明 / note on Welford:
文件内用 numpy 一次算出 (n, mean, M2)（等价于 Welford 单遍结果，数值稳定），
跨文件用 Chan et al. 并行合并公式累加——全集口径上仍是"单遍、不留原始数据"。
Within a file we compute (n, mean, M2) with numpy (equivalent to the single-pass
Welford result); across files we combine with Chan et al.'s parallel formula. At the
whole-dataset level this remains a single pass that retains no raw data.
"""

import math

import numpy as np

from . import config

# Welford 累加器布局 / accumulator layout: [n, mean, M2, min, max]
EMPTY_WELFORD = [0, 0.0, 0.0, float("inf"), float("-inf")]


# ---------------------------------------------------------------------------
# Welford
# ---------------------------------------------------------------------------

def welford_from_array(values) -> list:
    """
    由一批数值构造累加器（NaN 须由调用方预先剔除）。
    Build an accumulator from a batch of values (caller must strip NaNs first).
    """
    arr = np.asarray(values, dtype=np.float64)
    n = int(arr.size)
    if n == 0:
        return list(EMPTY_WELFORD)
    mean = float(arr.mean())
    m2 = float(((arr - mean) ** 2).sum())
    return [n, mean, m2, float(arr.min()), float(arr.max())]


def welford_merge(a: list, b: list) -> list:
    """
    合并两个累加器（Chan et al. 并行公式）。
    Merge two accumulators (Chan et al. parallel formula).
    """
    if not a or a[0] == 0:
        return list(b) if b else list(EMPTY_WELFORD)
    if not b or b[0] == 0:
        return list(a)
    n_a, mean_a, m2_a, min_a, max_a = a
    n_b, mean_b, m2_b, min_b, max_b = b
    n = n_a + n_b
    delta = mean_b - mean_a
    mean = mean_a + delta * (n_b / n)
    m2 = m2_a + m2_b + delta * delta * (n_a * n_b / n)
    return [n, mean, m2, min(min_a, min_b), max(max_a, max_b)]


def welford_std(acc: list) -> float:
    """
    样本标准差（n < 2 返回 nan）。
    Sample standard deviation (nan when n < 2).
    """
    if not acc or acc[0] < 2:
        return float("nan")
    return math.sqrt(acc[2] / (acc[0] - 1))


def welford_mean(acc: list) -> float:
    return float(acc[1]) if acc and acc[0] else float("nan")


# ---------------------------------------------------------------------------
# 稀疏直方图 / sparse histograms
# ---------------------------------------------------------------------------

def bin_spec(channel: str):
    """
    取通道的分箱方案 (lo, hi, width, nbins)。
    Bin specification (lo, hi, width, nbins) for a channel.
    """
    lo, hi, width = config.BIN_SPECS.get(channel, config.DEFAULT_BIN_SPEC)
    nbins = int(round((hi - lo) / width))
    return lo, hi, width, nbins


def _sorted_items(hist: dict):
    """
    直方图 → 按桶序排列的 (int_bin, count) 列表；容忍 JSON 反序列化后的字符串键。
    Histogram → (int_bin, count) pairs in bin order; tolerates string keys left by JSON.
    """
    return sorted(((int(k), int(v)) for k, v in hist.items()), key=lambda kv: kv[0])


def hist_from_array(values, channel: str) -> dict:
    """
    数值批 → 稀疏直方图 {bin_index: count}。越界值被 clip 到首/末桶——
    越界另有独立计数（unit_sanity），故首末桶不得用于尾部推断。
    Values → sparse histogram {bin_index: count}. Out-of-range values are clipped into
    the first/last bin; they are counted separately (unit_sanity), so the edge bins
    must not be used for tail inference.
    """
    arr = np.asarray(values, dtype=np.float64)
    if arr.size == 0:
        return {}
    lo, _hi, width, nbins = bin_spec(channel)
    idx = np.floor((arr - lo) / width).astype(np.int64)
    np.clip(idx, 0, nbins - 1, out=idx)
    counts = np.bincount(idx, minlength=nbins)
    nz = np.nonzero(counts)[0]
    return {int(i): int(counts[i]) for i in nz}


def hist_merge(a: dict, b: dict) -> dict:
    """
    合并两个稀疏直方图（就地更新 a 的副本）。
    Merge two sparse histograms.
    """
    out = dict(a)
    for k, v in b.items():
        k = int(k)
        out[k] = out.get(k, 0) + int(v)
    return out


def hist_merge_inplace(target: dict, other: dict) -> dict:
    """
    就地合并，避免归并大量文件时反复拷贝。
    In-place merge, avoiding repeated copies when merging many files.
    """
    for k, v in other.items():
        k = int(k)
        target[k] = target.get(k, 0) + int(v)
    return target


def hist_total(hist: dict) -> int:
    return int(sum(hist.values()))


def hist_quantile(hist: dict, channel: str, q: float):
    """
    由稀疏直方图近似分位数：定位分位点所在桶，桶内按均匀分布线性插值。
    精度上限 = 分箱宽度（报告中注明）。
    Approximate a quantile from the sparse histogram: locate the bin holding the
    quantile and interpolate linearly inside it (uniform-within-bin assumption).
    Precision is bounded by the bin width (stated in the report).
    """
    items = _sorted_items(hist)
    total = sum(c for _, c in items)
    if total == 0:
        return float("nan")
    lo, _hi, width, _nbins = bin_spec(channel)
    target = q * total
    cum = 0
    for idx, c in items:
        if cum + c >= target:
            frac = 0.0 if c == 0 else (target - cum) / c
            return lo + (idx + min(max(frac, 0.0), 1.0)) * width
        cum += c
    return lo + (items[-1][0] + 1) * width


def hist_median_iqr(hist: dict, channel: str):
    """
    近似 (median, q1, q3, iqr)。
    Approximate (median, q1, q3, iqr).
    """
    med = hist_quantile(hist, channel, 0.5)
    q1 = hist_quantile(hist, channel, 0.25)
    q3 = hist_quantile(hist, channel, 0.75)
    iqr = q3 - q1 if not (math.isnan(q1) or math.isnan(q3)) else float("nan")
    return med, q1, q3, iqr


def hist_ks_statistic(hist_a: dict, hist_b: dict) -> float:
    """
    由两个同分箱方案的直方图近似 KS 统计量 sup|F_a − F_b|。
    Approximate the KS statistic sup|F_a - F_b| from two histograms sharing a bin grid.
    近似性来源：桶内分布未知（报告中注明为近似值）。
    The approximation comes from the unknown within-bin distribution (flagged in the report).
    """
    a = {int(k): int(v) for k, v in hist_a.items()}
    b = {int(k): int(v) for k, v in hist_b.items()}
    n_a, n_b = sum(a.values()), sum(b.values())
    if n_a == 0 or n_b == 0:
        return float("nan")
    cum_a = cum_b = 0
    best = 0.0
    for k in sorted(set(a) | set(b)):
        cum_a += a.get(k, 0)
        cum_b += b.get(k, 0)
        best = max(best, abs(cum_a / n_a - cum_b / n_b))
    return best


# ---------------------------------------------------------------------------
# 整数计数字典（到达间隔/相位/取值分布等）/ integer counter dicts
# ---------------------------------------------------------------------------

def counter_merge_inplace(target: dict, other: dict) -> dict:
    """
    合并计数字典（键统一转 int，兼容 JSON 反序列化后的字符串键）。
    Merge counter dicts; keys are normalised to int to survive JSON round-trips.
    """
    for k, v in other.items():
        k = int(k)
        target[k] = target.get(k, 0) + int(v)
    return target


def counter_merge_str_inplace(target: dict, other: dict) -> dict:
    """
    字符串键计数字典的合并（如 uptime 的 'device|date' 键）。
    Merge counter dicts with string keys (e.g. the 'device|date' uptime keys).
    """
    for k, v in other.items():
        target[k] = target.get(k, 0) + int(v)
    return target


def counter_quantile(counter: dict, q: float, overflow_bin: int = None):
    """
    整数分箱计数字典的分位数（用于到达间隔）。返回桶左边界（秒）。
    落入溢出桶时返回 float('inf') 的替代标记 overflow_bin，调用方自行解读。
    Quantile of an integer-binned counter (used for inter-arrival). Returns the bin's
    left edge in seconds; if the quantile falls in the overflow bucket the bucket index
    is returned and the caller interprets it as ">= that many seconds".
    """
    items = _sorted_items(counter)
    total = sum(c for _, c in items)
    if total == 0:
        return float("nan")
    target = q * total
    cum = 0
    for k, c in items:
        cum += c
        if cum >= target:
            return float(k)
    return float(items[-1][0])
