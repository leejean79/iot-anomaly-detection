"""
报告层：全集聚合结果 → CSV 表格、图表与 eda_report.md。
Reporting layer: whole-dataset aggregate → CSV tables, figures and eda_report.md.

约定 / conventions:
- **图表文字一律英文**：Mac 默认 matplotlib 无中文字体，中文标签会渲染成方块。
  报告正文保持中英双语，图内文字用英文。
  All figure text is English: the default matplotlib font stack on macOS has no CJK
  glyphs and would render Chinese labels as tofu boxes. The report prose stays bilingual.
- 所有由直方图导出的分位数（中位数/IQR/P10/P90/KS）均为**近似值**，
  精度等于对应通道的分箱宽度，报告中逐处标注。
  Every histogram-derived quantile is approximate with precision equal to the channel's
  bin width; this is flagged wherever such a number appears.
- 本层不读原始数据，只读 aggregate.json 与 file_inventory.csv。
  This layer never touches the raw dataset; it only reads aggregate.json and the inventory.
"""

import csv
import datetime as _dt
import math
import os

import matplotlib

matplotlib.use("Agg")  # 无显示环境作图 / headless rendering
import matplotlib.pyplot as plt  # noqa: E402

from . import config, stats as st, timeutil  # noqa: E402

SEP = "|"
NA = "数据不足以回答 / insufficient data"


# ---------------------------------------------------------------------------
# 小工具 / helpers
# ---------------------------------------------------------------------------

def _split3(key: str):
    parts = key.split(SEP)
    return parts[0], parts[1], parts[2]


def _fmt(x, nd=3):
    if x is None:
        return ""
    if isinstance(x, float):
        if math.isnan(x):
            return "nan"
        if math.isinf(x):
            return "inf"
        return ("%%.%df" % nd) % x
    return str(x)


def _write_csv(path: str, header, rows) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def _q_label(q) -> str:
    """
    到达间隔分位数的显示：落入溢出桶时显示为 ">=N"，避免把桶索引当成秒数读。
    Render an inter-arrival quantile; values in the overflow bucket are shown as ">=N"
    so the bucket index is never misread as a plain second count.
    """
    if q is None or (isinstance(q, float) and math.isnan(q)):
        return "nan"
    if q >= config.INTERARRIVAL_OVERFLOW_BIN:
        return ">=%d" % config.INTERARRIVAL_OVERFLOW_BIN
    return _fmt(q, 1)


def _median_iqr(acc, hist, sensor):
    """
    近似中位数与四分位数；**恒定通道特判**：min == max 时直接给精确值，
    否则桶内均匀插值会把常量通道报成"中位数 = 半个分箱宽度、IQR = 半个分箱宽度"。
    Approximate median and quartiles, with an exact short-circuit for constant channels
    (min == max): otherwise the uniform-within-bin interpolation would report a constant
    channel as having a non-zero IQR of half a bin width.
    """
    if acc and acc[0] and acc[3] == acc[4]:
        return acc[3], acc[3], acc[3], 0.0
    return st.hist_median_iqr(hist, sensor)


def _sorted_devices(agg: dict):
    devs = set(agg.get("rounds_total", {}).keys())
    for key in agg.get("stats", {}):
        devs.add(_split3(key)[1])
    known = [d for d in config.EXPECTED_DEVICES if d in devs]
    extra = sorted(d for d in devs if d not in config.EXPECTED_DEVICES)
    return known + extra


def _sorted_sensors(agg: dict):
    sensors = {_split3(k)[2] for k in agg.get("stats", {})}
    known = [s for s in config.EXPECTED_SENSORS if s in sensors]
    extra = sorted(s for s in sensors if s not in config.EXPECTED_SENSORS)
    return known + extra


def _sorted_months(agg: dict):
    return sorted({_split3(k)[0] for k in agg.get("stats", {})})


def _roll_up(agg: dict):
    """
    把 "month|dev|sensor" 键的统计与直方图卷成多种视角。
    Roll the "month|dev|sensor" keyed stats and histograms into the views we need.
    """
    by_ds, by_ds_hist = {}, {}          # (dev, sensor)
    by_ms_hist = {}                     # (month, sensor)  —— 逐月分布（设备合并）
    by_s_hist = {}                      # sensor
    by_mds = {}                         # (month, dev, sensor)
    for key, acc in agg.get("stats", {}).items():
        month, dev, sensor = _split3(key)
        by_ds[(dev, sensor)] = st.welford_merge(by_ds.get((dev, sensor)), acc)
        by_mds[(month, dev, sensor)] = st.welford_merge(by_mds.get((month, dev, sensor)), acc)
    for key, hist in agg.get("hist", {}).items():
        month, dev, sensor = _split3(key)
        st.hist_merge_inplace(by_ds_hist.setdefault((dev, sensor), {}), hist)
        st.hist_merge_inplace(by_ms_hist.setdefault((month, sensor), {}), hist)
        st.hist_merge_inplace(by_s_hist.setdefault(sensor, {}), hist)
    return by_ds, by_ds_hist, by_ms_hist, by_s_hist, by_mds


def _sum_by(dic: dict, idx_dev: int = 1, idx_sensor: int = 2):
    """
    把 "a|b|c" 键的计数按 (dev, sensor) 汇总。
    Sum "a|b|c"-keyed counters by (device, sensor).
    """
    out = {}
    for key, n in dic.items():
        parts = key.split(SEP)
        out[(parts[idx_dev], parts[idx_sensor])] = out.get((parts[idx_dev], parts[idx_sensor]), 0) + int(n)
    return out


def _date_range(agg: dict):
    if agg.get("time_min") is None:
        return []
    d0 = _dt.datetime.strptime(str(timeutil.local_date_keys([agg["time_min"]])[0]), "%Y-%m-%d").date()
    d1 = _dt.datetime.strptime(str(timeutil.local_date_keys([agg["time_max"]])[0]), "%Y-%m-%d").date()
    days = (d1 - d0).days
    return [(d0 + _dt.timedelta(days=i)).isoformat() for i in range(days + 1)]


# ---------------------------------------------------------------------------
# E1 清单层 / inventory layer
# ---------------------------------------------------------------------------

def emit_e1(agg: dict, out_dir: str) -> dict:
    weekly = agg.get("weekly_files", {})
    rows = [[w, weekly[w]] for w in sorted(weekly)]
    _write_csv(os.path.join(out_dir, "files_per_week.csv"), ["iso_week", "n_files"], rows)

    if rows:
        fig, ax = plt.subplots(figsize=(max(8, len(rows) * 0.28), 4))
        ax.bar(range(len(rows)), [r[1] for r in rows], color="#3b6ea5")
        step = max(1, len(rows) // 20)
        ax.set_xticks(range(0, len(rows), step))
        ax.set_xticklabels([rows[i][0] for i in range(0, len(rows), step)], rotation=90, fontsize=7)
        ax.set_xlabel("ISO week (Europe/London)")
        ax.set_ylabel("files")
        ax.set_title("E1 - files per week")
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, "files_per_week.png"), dpi=130)
        plt.close(fig)

    dates = _date_range(agg)
    return {
        "n_days": len(dates),
        "first_date": dates[0] if dates else None,
        "last_date": dates[-1] if dates else None,
        "weeks": len(rows),
    }


# ---------------------------------------------------------------------------
# E2 完整性层 / completeness layer
# ---------------------------------------------------------------------------

def emit_e2(agg: dict, out_dir: str, by_ds: dict) -> dict:
    devices, sensors = _sorted_devices(agg), _sorted_sensors(agg)

    # --- 设备 × 传感器行数矩阵 / device x sensor row-count matrix -------
    rows = []
    for dev in devices:
        row = [dev] + [int(by_ds.get((dev, s), st.EMPTY_WELFORD)[0]) for s in sensors]
        row.append(sum(row[1:]))
        rows.append(row)
    _write_csv(os.path.join(out_dir, "device_sensor_counts.csv"),
               ["device"] + sensors + ["total"], rows)

    # --- uptime 在场矩阵（设备 × 当地日期 → 行数）/ uptime matrix -------
    dates = _date_range(agg)
    uptime = agg.get("uptime", {})
    up_rows = []
    presence = {d: set() for d in devices}
    for dev in devices:
        row = [dev]
        for date in dates:
            n = int(uptime.get("%s%s%s" % (dev, SEP, date), 0))
            row.append(n)
            if n:
                presence[dev].add(date)
        up_rows.append(row)
    _write_csv(os.path.join(out_dir, "uptime_matrix.csv"), ["device"] + dates, up_rows)

    if dates and devices:
        fig, ax = plt.subplots(figsize=(max(8, len(dates) * 0.05), 0.5 * len(devices) + 2))
        grid = [[1 if d in presence[dev] else 0 for d in dates] for dev in devices]
        ax.imshow(grid, aspect="auto", cmap="Greens", vmin=0, vmax=1, interpolation="nearest")
        ax.set_yticks(range(len(devices)))
        ax.set_yticklabels(devices)
        step = max(1, len(dates) // 15)
        ax.set_xticks(range(0, len(dates), step))
        ax.set_xticklabels([dates[i] for i in range(0, len(dates), step)], rotation=90, fontsize=7)
        ax.set_title("E2 - device uptime (green = data present on that local date)")
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, "uptime_timeline.png"), dpi=130)
        plt.close(fig)

    # --- 采样轮齐全率 / round completeness ------------------------------
    rc_rows = []
    completeness = {}
    for dev in devices:
        sizes = {int(k): int(v) for k, v in agg.get("round_size", {}).get(dev, {}).items()}
        total = sum(sizes.values())
        full = sizes.get(len(config.EXPECTED_SENSORS), 0)
        completeness[dev] = (full / total) if total else float("nan")
        rc_rows.append([
            dev, total, full, _fmt(completeness[dev] * 100 if total else float("nan"), 4),
            ";".join("%d:%d" % (k, sizes[k]) for k in sorted(sizes)),
        ])
    _write_csv(os.path.join(out_dir, "round_completeness.csv"),
               ["device", "rounds_total", "rounds_with_8_sensors", "full_round_pct",
                "sensors_per_round_distribution"], rc_rows)

    # --- 缺口清单与摘要 / gap listing and summary -----------------------
    gap_rows = []
    for g in agg.get("gaps_top", []):
        gap_rows.append([
            g["device"], g["start_epoch"], g["end_epoch"], g["duration_s"],
            timeutil.utc_iso(g["start_epoch"]), timeutil.utc_iso(g["end_epoch"]),
            round(g["duration_s"] / 3600.0, 3), g["kind"], g["file"],
        ])
    _write_csv(os.path.join(out_dir, "gaps_top100.csv"),
               ["device", "start_epoch", "end_epoch", "duration_s", "start_utc", "end_utc",
                "duration_h", "kind", "file"], gap_rows)

    edges = agg.get("gap_hist_edges", [])
    summary = agg.get("gap_summary", {})
    span_total = (agg.get("time_max") or 0) - (agg.get("time_min") or 0)
    gs_rows = []
    for dev in devices:
        s = summary.get(dev, {"count": 0, "total_s": 0, "max_s": 0, "hist": {}})
        share = (s["total_s"] / span_total * 100.0) if span_total else float("nan")
        gs_rows.append([dev, s["count"], s["total_s"], round(s["total_s"] / 3600.0, 2),
                        s["max_s"], round(s["max_s"] / 3600.0, 3), _fmt(share, 3)])
    _write_csv(os.path.join(out_dir, "gap_summary.csv"),
               ["device", "gap_count", "gap_total_s", "gap_total_h", "gap_max_s", "gap_max_h",
                "gap_share_of_span_pct"], gs_rows)

    # 缺口时长直方图 / gap-duration histogram
    labels = _gap_bucket_labels(edges)
    totals = [0] * len(labels)
    for s in summary.values():
        for b, c in s.get("hist", {}).items():
            b = int(b)
            if b < len(totals):
                totals[b] += int(c)
    if sum(totals):
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.bar(range(len(labels)), totals, color="#a5543b")
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
        ax.set_yscale("log")
        ax.set_ylabel("gap segments (log)")
        ax.set_title("E2 - gap duration distribution (all devices, gap > %d s)" % config.GAP_THRESHOLD_S)
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, "gap_duration_hist.png"), dpi=130)
        plt.close(fig)

    return {
        "devices": devices,
        "sensors": sensors,
        "presence_days": {d: len(presence[d]) for d in devices},
        "n_days": len(dates),
        "completeness": completeness,
        "gap_summary": summary,
        "gap_bucket_labels": labels,
        "gap_bucket_totals": totals,
    }


def _gap_bucket_labels(edges):
    labels = []
    prev = config.GAP_THRESHOLD_S
    for e in edges:
        labels.append("%s-%s" % (_dur(prev), _dur(e)))
        prev = e
    labels.append(">=%s" % _dur(prev))
    return labels


def _dur(seconds: int) -> str:
    if seconds < 3600:
        return "%dm" % (seconds // 60) if seconds >= 60 else "%ds" % seconds
    if seconds < 86400:
        return "%dh" % (seconds // 3600)
    return "%dd" % (seconds // 86400)


# ---------------------------------------------------------------------------
# E3 数值层 / numeric layer
# ---------------------------------------------------------------------------

def emit_e3(agg: dict, out_dir: str, by_ds, by_ds_hist, by_mds, by_s_hist) -> dict:
    devices, sensors = _sorted_devices(agg), _sorted_sensors(agg)
    nan_by = _sum_by(agg.get("nan", {}))
    sentinel_by = _sum_by(agg.get("sentinel", {}))
    oor_by = _sum_by(agg.get("oor", {}))

    rows = []
    for dev in devices:
        for sensor in sensors:
            acc = by_ds.get((dev, sensor))
            if not acc or acc[0] == 0:
                continue
            hist = by_ds_hist.get((dev, sensor), {})
            med, q1, q3, iqr = _median_iqr(acc, hist, sensor)
            rows.append([
                dev, sensor, acc[0], _fmt(acc[3], 6), _fmt(acc[4], 6), _fmt(acc[1], 6),
                _fmt(st.welford_std(acc), 6), _fmt(med, 4), _fmt(q1, 4), _fmt(q3, 4), _fmt(iqr, 4),
                nan_by.get((dev, sensor), 0), sentinel_by.get((dev, sensor), 0),
                oor_by.get((dev, sensor), 0), _fmt(st.bin_spec(sensor)[2], 4),
            ])
    _write_csv(os.path.join(out_dir, "channel_stats.csv"),
               ["device", "sensor", "count", "min", "max", "mean", "std",
                "median_approx", "q1_approx", "q3_approx", "iqr_approx",
                "nan_count", "sentinel_count", "out_of_range_count", "hist_bin_width"], rows)

    # --- 逐月 / monthly -------------------------------------------------
    m_rows = []
    monthly_hist_by_mds = {}
    for key, hist in agg.get("hist", {}).items():
        month, dev, sensor = _split3(key)
        st.hist_merge_inplace(monthly_hist_by_mds.setdefault((month, dev, sensor), {}), hist)
    for (month, dev, sensor) in sorted(by_mds):
        acc = by_mds[(month, dev, sensor)]
        if acc[0] == 0:
            continue
        hist = monthly_hist_by_mds.get((month, dev, sensor), {})
        med, q1, q3, iqr = _median_iqr(acc, hist, sensor)
        m_rows.append([month, dev, sensor, acc[0], _fmt(acc[3], 6), _fmt(acc[4], 6),
                       _fmt(acc[1], 6), _fmt(st.welford_std(acc), 6),
                       _fmt(med, 4), _fmt(q1, 4), _fmt(q3, 4), _fmt(iqr, 4)])
    _write_csv(os.path.join(out_dir, "channel_stats_monthly.csv"),
               ["month", "device", "sensor", "count", "min", "max", "mean", "std",
                "median_approx", "q1_approx", "q3_approx", "iqr_approx"], m_rows)

    # --- Accelerometer 非零记录（DEV-Q1 裁决证据）/ non-zero records ------
    acc_rows = []
    for rec in agg.get("accel_records", []):
        acc_rows.append([rec[0], timeutil.utc_iso(rec[0]), timeutil.local_iso(rec[0]),
                         rec[1], _fmt(rec[2], 8), rec[3] if len(rec) > 3 else ""])
    _write_csv(os.path.join(out_dir, "accel_nonzero.csv"),
               ["epoch", "utc", "local", "device", "value", "file"], acc_rows)

    accel_zero = sum(int(v) for v in agg.get("accel_zero", {}).values())
    accel_nonzero = sum(int(v) for v in agg.get("accel_nonzero", {}).values())

    # --- 单位合理性核验 / unit sanity ------------------------------------
    sanity_rows = []
    for sensor in sensors:
        acc_all = None
        for dev in devices:
            acc_all = st.welford_merge(acc_all, by_ds.get((dev, sensor)))
        if not acc_all or acc_all[0] == 0:
            continue
        rng = config.PHYSICAL_RANGES.get(sensor)
        oor = sum(v for (d, s), v in oor_by.items() if s == sensor)
        sanity_rows.append([
            sensor, acc_all[0], _fmt(acc_all[3], 6), _fmt(acc_all[4], 6), _fmt(acc_all[1], 6),
            "" if rng is None else _fmt(rng[0], 2), "" if rng is None else _fmt(rng[1], 2),
            "" if rng is None else oor,
            "" if rng is None else _fmt(oor / acc_all[0] * 100.0, 6),
            "no prior range asserted" if rng is None else "",
        ])
    _write_csv(os.path.join(out_dir, "unit_sanity.csv"),
               ["sensor", "count", "observed_min", "observed_max", "observed_mean",
                "expected_min", "expected_max", "out_of_range_count", "out_of_range_pct", "note"],
               sanity_rows)

    # --- MIC 取值分布（Q6 质量门控作用面）/ MIC value distribution -------
    mic_rows = []
    mic_lo, _hi, mic_w, _n = st.bin_spec("MIC")
    months = _sorted_months(agg)
    mic_month_totals, mic_month_ones = {}, {}
    for key, hist in agg.get("hist", {}).items():
        month, dev, sensor = _split3(key)
        if sensor != "MIC":
            continue
        for b, c in hist.items():
            val = mic_lo + int(b) * mic_w
            mic_rows.append([month, dev, _fmt(val, 2), int(c)])
            mic_month_totals[month] = mic_month_totals.get(month, 0) + int(c)
            if abs(val - 1.0) < 1e-9:
                mic_month_ones[month] = mic_month_ones.get(month, 0) + int(c)
    # 合并同 (month, dev, value) 的多条 / collapse duplicates
    collapsed = {}
    for month, dev, val, c in mic_rows:
        collapsed[(month, dev, val)] = collapsed.get((month, dev, val), 0) + c
    _write_csv(os.path.join(out_dir, "mic_distribution.csv"),
               ["month", "device", "mic_value", "count"],
               [[m, d, v, c] for (m, d, v), c in sorted(collapsed.items())])

    mic_total = sum(mic_month_totals.values())
    mic_ones = sum(mic_month_ones.values())
    mic_values_seen = sorted({float(v) for (_m, _d, v) in collapsed})

    # --- 低方差/常量通道（Q5 RobustScaler 退化防护）/ low-variance channels
    degenerate = []
    for dev in devices:
        for sensor in sensors:
            acc = by_ds.get((dev, sensor))
            if not acc or acc[0] == 0:
                continue
            sd = st.welford_std(acc)
            _med, _q1, _q3, iqr = _median_iqr(acc, by_ds_hist.get((dev, sensor), {}), sensor)
            # 常量判据用**精确的 min/max 与 std**，不用直方图 IQR：单桶内的近似 IQR 恒为
            # 一个分箱宽度而非 0，恒零通道会被漏判。
            # Constancy is decided on the exact min/max and std rather than the histogram
            # IQR: a single-bin histogram yields one bin width, not 0, so an all-zero
            # channel would be missed.
            is_const = acc[3] == acc[4] or (not math.isnan(sd) and sd == 0.0)
            if is_const or (not math.isnan(iqr) and iqr <= 0.0):
                degenerate.append((dev, sensor, sd, iqr, acc[3], acc[4], is_const))
    _write_csv(os.path.join(out_dir, "degenerate_channels.csv"),
               ["device", "sensor", "std", "iqr_approx", "min", "max", "risk"],
               [[d, s, _fmt(sd, 8), _fmt(iqr, 6), _fmt(lo, 6), _fmt(hi, 6),
                 "constant value -> zero IQR -> RobustScaler division by zero" if const
                 else "zero IQR -> RobustScaler division by zero"]
                for d, s, sd, iqr, lo, hi, const in degenerate])

    return {
        "accel_zero": accel_zero,
        "accel_nonzero": accel_nonzero,
        "accel_nonzero_per_device": agg.get("accel_nonzero", {}),
        "accel_records_truncated": agg.get("accel_records_truncated", False),
        "mic_values_seen": mic_values_seen,
        "mic_total": mic_total,
        "mic_ones": mic_ones,
        "mic_month_totals": mic_month_totals,
        "mic_month_ones": mic_month_ones,
        "months": months,
        "degenerate": degenerate,
        "sanity_rows": sanity_rows,
    }


# ---------------------------------------------------------------------------
# E4 节奏层 / rhythm layer
# ---------------------------------------------------------------------------

def emit_e4(agg: dict, out_dir: str) -> dict:
    devices = _sorted_devices(agg)
    ia = agg.get("interarrival", {})

    rows, quant, quant_ng = [], {}, {}
    for dev in devices:
        counter = {int(k): int(v) for k, v in ia.get(dev, {}).items()}
        total = sum(counter.values())
        if not total:
            continue
        qs = [st.counter_quantile(counter, q) for q in config.INTERARRIVAL_QUANTILES]
        quant[dev] = qs
        # 剔除缺口段后的分位数：只保留 ≤ GAP_THRESHOLD_S 的间隔，代表"正常运行期抖动"。
        # M1 的 watermark 应按这一口径取值，含缺口的尾部分位数由设备离线主导，不是乱序延迟。
        # Gap-excluded quantiles keep only intervals <= GAP_THRESHOLD_S, i.e. normal-operation
        # jitter. The M1 watermark should be sized from these: the full tail is dominated by
        # device outages, which are not out-of-order lateness.
        counter_ng = {k: c for k, c in counter.items() if k <= config.GAP_THRESHOLD_S}
        total_ng = sum(counter_ng.values())
        qs_ng = [st.counter_quantile(counter_ng, q) for q in config.INTERARRIVAL_QUANTILES]
        quant_ng[dev] = qs_ng
        overflow = counter.get(config.INTERARRIVAL_OVERFLOW_BIN, 0)
        mean = sum(k * c for k, c in counter.items() if k < config.INTERARRIVAL_OVERFLOW_BIN) / max(
            1, total - overflow)
        rows.append([dev, total, _fmt(mean, 3)] +
                    [_q_label(q) for q in qs] + [total_ng] + [_q_label(q) for q in qs_ng] +
                    [max(k for k in counter if counter[k]), overflow,
                     _fmt(overflow / total * 100.0, 5)])
    _write_csv(os.path.join(out_dir, "interarrival_quantiles.csv"),
               ["device", "n_intervals", "mean_s", "p50_s", "p90_s", "p99_s", "p999_s",
                "n_intervals_excl_gaps", "p50_excl_gaps_s", "p90_excl_gaps_s",
                "p99_excl_gaps_s", "p999_excl_gaps_s",
                "max_bin_s", "overflow_count", "overflow_pct"], rows)

    # 到达间隔分布图 / inter-arrival distribution
    if quant:
        fig, ax = plt.subplots(figsize=(10, 4.5))
        for dev in devices:
            counter = {int(k): int(v) for k, v in ia.get(dev, {}).items()}
            total = sum(counter.values())
            if not total:
                continue
            xs = sorted(k for k in counter if k <= config.INTERARRIVAL_MAX_S)
            ys = [counter[x] / total for x in xs]
            ax.plot(xs, ys, marker=".", linewidth=1, label="device %s" % dev)
        ax.set_yscale("log")
        ax.set_xlim(0, 60)
        ax.set_xlabel("inter-arrival between sampling rounds (s, 1 s bins)")
        ax.set_ylabel("share of intervals (log)")
        ax.set_title("E4 - inter-arrival distribution per device")
        ax.legend(fontsize=8, ncol=2)
        ax.grid(alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, "interarrival_distribution.png"), dpi=130)
        plt.close(fig)

    # 跨设备时钟相位 / cross-device clock phase
    skew = agg.get("skew", {})
    sk_rows = []
    for dev in sorted(skew.get("hist", {})):
        counter = {int(k): int(v) for k, v in skew["hist"][dev].items()}
        total = sum(counter.values())
        if not total:
            continue
        mean = sum(k * c for k, c in counter.items()) / total
        p05 = st.counter_quantile(counter, 0.05)
        p50 = st.counter_quantile(counter, 0.5)
        p95 = st.counter_quantile(counter, 0.95)
        abs_counter = {}
        for k, c in counter.items():
            abs_counter[abs(k)] = abs_counter.get(abs(k), 0) + c
        p95_abs = st.counter_quantile(abs_counter, 0.95)
        p99_abs = st.counter_quantile(abs_counter, 0.99)
        sk_rows.append([dev, total, _fmt(mean, 3), _fmt(p05, 1), _fmt(p50, 1), _fmt(p95, 1),
                        _fmt(p95_abs, 1), _fmt(p99_abs, 1),
                        int(skew.get("outside", {}).get(dev, 0))])
    _write_csv(os.path.join(out_dir, "clock_skew_sample.csv"),
               ["device", "n_rounds_matched", "mean_offset_s", "p05_offset_s", "p50_offset_s",
                "p95_offset_s", "p95_abs_offset_s", "p99_abs_offset_s", "outside_window_rounds"],
               sk_rows)

    return {
        "quantiles": quant,
        "quantiles_excl_gaps": quant_ng,
        "rows": rows,
        "skew_rows": sk_rows,
        "skew_refs": skew.get("refs", {}),
        "skew_files": skew.get("files", 0),
    }


# ---------------------------------------------------------------------------
# E5 漂移预览层 / drift-preview layer
# ---------------------------------------------------------------------------

def emit_e5(agg: dict, out_dir: str, by_ms_hist: dict) -> dict:
    months = _sorted_months(agg)
    drift = {}
    for channel in config.F_DET_CHANNELS:
        series = []
        for month in months:
            hist = by_ms_hist.get((month, channel), {})
            if not hist:
                continue
            series.append((
                month,
                st.hist_quantile(hist, channel, 0.10),
                st.hist_quantile(hist, channel, 0.50),
                st.hist_quantile(hist, channel, 0.90),
                st.hist_total(hist),
            ))
        if not series:
            continue
        drift[channel] = series
        fig, ax = plt.subplots(figsize=(8, 4))
        xs = range(len(series))
        ax.plot(xs, [s[1] for s in series], marker="o", label="P10")
        ax.plot(xs, [s[2] for s in series], marker="o", label="P50")
        ax.plot(xs, [s[3] for s in series], marker="o", label="P90")
        ax.fill_between(xs, [s[1] for s in series], [s[3] for s in series], alpha=0.12)
        ax.set_xticks(list(xs))
        ax.set_xticklabels([s[0] for s in series], rotation=45, ha="right")
        ax.set_ylabel(channel)
        ax.set_title("E5 - monthly quantile drift: %s (bin width %.3g)"
                     % (channel, st.bin_spec(channel)[2]))
        ax.legend()
        ax.grid(alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, "monthly_quantile_drift_%s.png" % channel.lower()), dpi=130)
        plt.close(fig)

    # 相邻月 KS（由逐月直方图经验 CDF 近似）/ adjacent-month KS from histogram CDFs
    ks_rows, ks_by_channel = [], {}
    for channel in config.F_DET_CHANNELS:
        for a, b in zip(months, months[1:]):
            ha, hb = by_ms_hist.get((a, channel), {}), by_ms_hist.get((b, channel), {})
            if not ha or not hb:
                continue
            ks = st.hist_ks_statistic(ha, hb)
            ks_rows.append([channel, a, b, _fmt(ks, 5), st.hist_total(ha), st.hist_total(hb)])
            ks_by_channel.setdefault(channel, []).append((a, b, ks))
    _write_csv(os.path.join(out_dir, "ks_adjacent_months.csv"),
               ["channel", "month_a", "month_b", "ks_statistic_approx", "n_a", "n_b"], ks_rows)

    # 季节趋势（日均值）/ seasonal trend (daily means)
    daily = agg.get("daily", {})
    per_channel = {}
    for key, (n, s) in daily.items():
        date, sensor = key.split(SEP)
        if sensor not in config.SEASONAL_CHANNELS:
            continue
        acc = per_channel.setdefault(sensor, {})
        cur = acc.get(date, [0, 0.0])
        acc[date] = [cur[0] + int(n), cur[1] + float(s)]
    if per_channel:
        fig, axes = plt.subplots(len(config.SEASONAL_CHANNELS), 1, figsize=(11, 8), sharex=True)
        if len(config.SEASONAL_CHANNELS) == 1:
            axes = [axes]
        for ax, channel in zip(axes, config.SEASONAL_CHANNELS):
            series = per_channel.get(channel, {})
            dates = sorted(series)
            if not dates:
                ax.set_visible(False)
                continue
            ys = [series[d][1] / series[d][0] for d in dates]
            xs = [_dt.date.fromisoformat(d) for d in dates]
            ax.plot(xs, ys, linewidth=1.0, color="#3b6ea5")
            ax.set_ylabel(channel)
            ax.grid(alpha=0.3)
        axes[0].set_title("E5 - daily mean (all devices pooled, Europe/London day boundaries)")
        axes[-1].set_xlabel("local date")
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, "seasonal_trend.png"), dpi=130)
        plt.close(fig)

    daily_rows = []
    for channel in config.SEASONAL_CHANNELS:
        for date in sorted(per_channel.get(channel, {})):
            n, s = per_channel[channel][date]
            daily_rows.append([date, channel, n, _fmt(s / n, 5)])
    _write_csv(os.path.join(out_dir, "daily_channel_means.csv"),
               ["local_date", "channel", "count", "daily_mean"], daily_rows)

    return {"drift": drift, "ks_by_channel": ks_by_channel, "months": months}


# ---------------------------------------------------------------------------
# 与交接文档 §3 数据事实的对照 / cross-check against handover §3 data facts
# ---------------------------------------------------------------------------

def check_data_facts(agg: dict, e2: dict, e3: dict, e4: dict, by_ds: dict) -> list:
    """
    逐条核对交接文档 §3 的既有数据事实，返回 (编号, 原始表述, 实测, 结论) 列表。
    Check each handover §3 data fact against what was measured; return
    (id, asserted, observed, verdict) rows.

    禁区提醒（§8）：冲突项一律如实报告并标注为**新数据事实候选**，交回设计会话登记，
    EDA 不自行改写前提。
    Per handover §8, conflicts are reported as new-data-fact candidates for the design
    session to register; the EDA never rewrites the premises itself.
    """
    files = agg.get("files", {})
    out = []

    # DF-1: 每设备每采样轮 8 行共享同一时间戳
    comp = e2.get("completeness", {})
    if comp:
        worst = min(comp.values())
        best = max(comp.values())
        verdict = "一致 / consistent" if worst >= 0.99 else "部分偏离 / partially deviates"
        out.append(("DF-1", "每设备每采样轮 8 行共享同一时间戳",
                    "8 传感器齐备轮占比 %.4f%%–%.4f%%（逐设备）" % (worst * 100, best * 100), verdict))

    # DF-2: 标称 10 s，实测抖动 7–18 s
    # 口径：按**剔除缺口段**的正常运行期间隔核对——含离线缺口的尾部不属于"采样抖动"。
    # Checked on the gap-excluded intervals: an outage tail is not sampling jitter.
    quant_ng = e4.get("quantiles_excl_gaps", {})
    if quant_ng:
        p50s = [q[0] for q in quant_ng.values() if not math.isnan(q[0])]
        p999s = [q[3] for q in quant_ng.values() if not math.isnan(q[3])]
        if p50s and p999s:
            ok = all(abs(p - 10.0) <= 2.0 for p in p50s) and max(p999s) <= 18.0
            out.append(("DF-2", "采样标称 10 s，实测抖动 7–18 s（正常运行期口径）",
                        "P50 ∈ [%.0f, %.0f] s；P99.9 最大 %.0f s（已剔除 > %d s 的缺口段）"
                        % (min(p50s), max(p50s), max(p999s), config.GAP_THRESHOLD_S),
                        "一致 / consistent" if ok else
                        "**冲突：抖动区间须按实测重述** / conflict: the jitter range needs restating"))

    # DF-3: 设备 H 缺席（样例文件观察）
    presence = e2.get("presence_days", {})
    n_days = e2.get("n_days", 0)
    if "H" in presence and n_days:
        share = presence["H"] / n_days * 100.0
        out.append(("DF-3", "样例文件中设备 H 缺席",
                    "H 在 %d/%d 天（%.1f%%）有数据" % (presence["H"], n_days, share),
                    "一致（H 全程缺席）/ consistent" if presence["H"] == 0 else
                    "**冲突：H 并非全程缺席，为新数据事实候选** / conflict: H is not absent throughout"))
    elif n_days:
        out.append(("DF-3", "样例文件中设备 H 缺席", "全集未出现设备 H",
                    "一致 / consistent"))

    # DF-5: Accelerometer 恒 0.0
    nz = e3.get("accel_nonzero", 0)
    total_acc = nz + e3.get("accel_zero", 0)
    if total_acc:
        out.append(("DF-5", "Accelerometer 样例中恒 0.0",
                    "非零 %d / 总计 %d（%.6f%%）" % (nz, total_acc, nz / total_acc * 100.0),
                    "一致（全集恒零）/ consistent" if nz == 0 else
                    "**冲突：存在非零值，DEV-Q1 须按非零形态裁决** / conflict: non-zero values exist"))

    # DF-6: MIC ∈ {1.0, 3.0}
    vals = e3.get("mic_values_seen", [])
    if vals:
        unexpected = [v for v in vals if abs(v - 1.0) > 1e-9 and abs(v - 3.0) > 1e-9]
        out.append(("DF-6", "MIC ∈ {1.0, 3.0}",
                    "实测取值 %s" % ", ".join("%.2g" % v for v in vals),
                    "一致 / consistent" if not unexpected else
                    "**冲突：出现 %s，MIC 取值集须重述** / conflict: extra values observed"
                    % ", ".join("%.2g" % v for v in unexpected)))

    # DF-7: Pressure 单位为 Pa
    acc_p = None
    for (dev, sensor), acc in by_ds.items():
        if sensor == "Pressure":
            acc_p = st.welford_merge(acc_p, acc)
    if acc_p and acc_p[0]:
        in_pa = 90000.0 <= acc_p[1] <= 110000.0
        out.append(("DF-7", "Pressure 实际单位为 Pa（README 的 hPa 有误）",
                    "均值 %.1f，范围 [%.1f, %.1f]" % (acc_p[1], acc_p[3], acc_p[4]),
                    "一致 / consistent" if in_pa else
                    "**冲突：量级不符合 Pa** / conflict: magnitude is not Pa"))

    # DF-8: 两种文件命名模式
    patterns = agg.get("patterns", {})
    classes = agg.get("name_classes", {})
    known = {"dashed_data", "dashed_plain"}
    extra = {k: v for k, v in patterns.items() if k not in known and v}
    out.append(("DF-8", "文件命名两种模式：..._HH-MM-SS_data.csv 与 ..._HH-MM-SS.csv",
                "三类归属 dashed_data=%d/dashed_plain=%d/unmatched=%d；细分 %s"
                % (classes.get("dashed_data", 0), classes.get("dashed_plain", 0),
                   classes.get("unmatched", 0),
                   ", ".join("%s=%d" % (k, v) for k, v in sorted(patterns.items()))),
                "一致 / consistent" if not extra else
                "**冲突：存在 unmatched 类命名（如紧凑时间 HHMMSS、非 .csv 扩展名），"
                "已兼容并入统计，须登记为新事实** / conflict: unmatched naming forms exist"))

    # DF-15（补丁 01 起因）：文件发现是否完整 / discovery completeness
    total = files.get("total", 0)
    accounted = files.get("ok", 0) + files.get("failed", 0) + files.get("non_data", 0)
    out.append(("DF-15", "文件发现须覆盖目录全部常规文件（原实现按 .csv 扩展名过滤，漏发现约 276 个）",
                "发现 %d 个常规文件，归属 ok+failed+non_data=%d，非数据文件 %d 个"
                % (total, accounted, files.get("non_data", 0)),
                "已修复：发现与分类分离，零静默跳过 / fixed: discovery decoupled from naming, "
                "zero silent skips" if accounted == total else
                "**归属不平：ok+failed+non_data ≠ 发现数** / accounting imbalance"))

    # DF-10（覆盖期，补丁 01 §3.2 解冻依据）：是否延伸过 2022-07-25、是否有 08/09 月
    if agg.get("time_max") is not None:
        last_local = str(timeutil.local_date_keys([agg["time_max"]])[0])
        months = sorted({_split3(k)[0] for k in agg.get("stats", {})})
        has_aug = any(m >= "2022-08" for m in months)
        past_0725 = last_local > "2022-07-25"
        out.append(("DF-10", "全集覆盖期（冻结待补扫解冻；决定 A9「六个月」表述）",
                    "覆盖至当地日期 %s；出现月份 %s" % (last_local, ", ".join(months)),
                    ("延伸过 2022-07-25%s / extends past 2022-07-25"
                     % ("、含 2022-08/09 月数据" if has_aug else "，但未见 2022-08 及以后")
                     ) if past_0725 else
                    "未延伸过 2022-07-25 / does not extend past 2022-07-25"))
    return out


# ---------------------------------------------------------------------------
# 补丁 01：关键指标摘要与基线差异 / patch 01 metric digest and baseline diff
# ---------------------------------------------------------------------------

def metrics_digest(agg: dict) -> dict:
    """
    从聚合中提炼补丁 §3.3 关注的关键指标，供两次运行差异对照。
    Extract the key metrics named in patch §3.3 from an aggregate, for run-to-run diffing.

    返回的每一项都是可比的标量/元组/排序列表，便于逐项判等。
    Every returned value is a comparable scalar/tuple/ordered list for item-wise equality.
    """
    _by_ds, _h, by_ms_hist, _s, _mds = _roll_up(agg)

    # 设备在场天数 / device presence days：uptime 每条键为 device|date，故按 device 计数即天数。
    # Each uptime key is device|date, so counting per device yields the number of days present.
    presence = {}
    for key in agg.get("uptime", {}):
        dev = key.split(SEP, 1)[0]
        presence[dev] = presence.get(dev, 0) + 1

    # 缺口摘要 / gap summary
    gs = agg.get("gap_summary", {})
    gap_max = max((v.get("max_s", 0) for v in gs.values()), default=0)
    gap_total = sum(v.get("total_s", 0) for v in gs.values())

    # 到达间隔分位数（剔缺口口径）/ inter-arrival quantiles (gap-excluded)
    ia_p99 = ia_p999 = 0.0
    for dev, counter in agg.get("interarrival", {}).items():
        c = {int(k): int(v) for k, v in counter.items() if int(k) <= config.GAP_THRESHOLD_S}
        if c:
            ia_p99 = max(ia_p99, st.counter_quantile(c, 0.99))
            ia_p999 = max(ia_p999, st.counter_quantile(c, 0.999))

    # 逐月 KS 排序 / monthly KS ranking
    months = sorted({_split3(k)[0] for k in agg.get("stats", {})})
    ks_rank = []
    for channel in config.F_DET_CHANNELS:
        vals = []
        for a, b in zip(months, months[1:]):
            ha, hb = by_ms_hist.get((a, channel), {}), by_ms_hist.get((b, channel), {})
            if ha and hb:
                k = st.hist_ks_statistic(ha, hb)
                if not math.isnan(k):
                    vals.append(k)
        if vals:
            ks_rank.append((channel, round(sum(vals) / len(vals), 4)))
    ks_rank.sort(key=lambda r: -r[1])

    # MIC 取值集合 / MIC value set
    mic_lo, _hi, mic_w, _n = st.bin_spec("MIC")
    mic_vals = set()
    for key, hist in agg.get("hist", {}).items():
        if _split3(key)[2] == "MIC":
            for b in hist:
                mic_vals.add(round(mic_lo + int(b) * mic_w, 3))

    return {
        "presence_days": presence,
        "gap_max_s": gap_max,
        "gap_total_s": gap_total,
        "ia_p99_excl_gaps_s": round(ia_p99, 1),
        "ia_p999_excl_gaps_s": round(ia_p999, 1),
        "ks_ranking": [c for c, _ in ks_rank],
        "accel_nonzero": sum(int(v) for v in agg.get("accel_nonzero", {}).values()),
        "mic_values": sorted(mic_vals),
        "coverage_last_local": (str(timeutil.local_date_keys([agg["time_max"]])[0])
                                if agg.get("time_max") is not None else None),
    }


def diff_digests(baseline: dict, current: dict, baseline_label: str) -> dict:
    """
    对比两份 metrics_digest，只列出有变化的项（补丁 §3.3）。
    Compare two digests and keep only the changed items (patch §3.3).
    """
    labels = {
        "presence_days": "设备在场天数 / device presence days",
        "gap_max_s": "最长缺口 (s) / longest gap",
        "gap_total_s": "缺口总时长 (s) / total gap",
        "ia_p99_excl_gaps_s": "到达间隔 P99 剔缺口 (s) / inter-arrival P99",
        "ia_p999_excl_gaps_s": "到达间隔 P99.9 剔缺口 (s) / inter-arrival P99.9",
        "ks_ranking": "逐月 KS 排序 / monthly KS ranking",
        "accel_nonzero": "Accelerometer 非零计数 / non-zero count",
        "mic_values": "MIC 取值集合 / MIC value set",
        "coverage_last_local": "覆盖末日(当地) / coverage last local date",
    }
    rows = []
    for key, label in labels.items():
        b, c = baseline.get(key), current.get(key)
        if b == c:
            continue
        rows.append((label, _digest_cell(b), _digest_cell(c), _digest_change(b, c)))
    return {"baseline_label": baseline_label, "rows": rows}


def _digest_cell(v) -> str:
    if isinstance(v, dict):
        return "; ".join("%s:%s" % (k, v[k]) for k in sorted(v))
    if isinstance(v, list):
        return "[" + ", ".join(str(x) for x in v) + "]"
    return str(v)


def _digest_change(b, c) -> str:
    if isinstance(b, (int, float)) and isinstance(c, (int, float)):
        return "%+g" % (c - b)
    return "changed / 变化"


# ---------------------------------------------------------------------------
# 补丁 01：文件发现补扫章节 / patch 01 rescan section
# ---------------------------------------------------------------------------

def _emit_patch01(w, agg: dict, e5: dict, files: dict) -> None:
    """
    报告 §7：补丁 01（文件发现缺口修复与补扫）的四问作答。
    Report §7: the four questions of patch 01 (file-discovery gap fix and rescan).
    """
    w("## 7. 补丁 01：文件发现补扫 / File-discovery rescan")
    w()
    w("> 背景 / background：首轮实现以 `.csv` 扩展名过滤文件发现，凡非 `.csv` 扩展名的文件"
      "被静默跳过。补丁 01 改为**无条件递归列举全部常规文件**，命名模式仅用于分类，"
      "是否入统计由**内容 schema 嗅探**决定。")
    w(">")
    w("> **根因更正（如实报告）**：交接文档推断漏发现源于「命名模式 glob 匹配」；经核对首轮"
      "代码，发现过滤实为 **`.csv` 扩展名过滤**（非命名模式），故 276 个漏发现文件应为"
      "**非 `.csv` 扩展名**的文件，而非命名不匹配。已按补丁要求修复，根因表述一并更正。")
    w()

    unmatched = agg.get("unmatched", [])
    classes = agg.get("name_classes", {})

    # --- §3.1 未匹配文件的命名形态清单 / unmatched naming forms ----------
    w("### 7.1 未匹配（unmatched 类）文件的命名形态 / unmatched naming forms")
    w()
    w("`unmatched` 类共 **%d** 个文件（完整逐文件清单见 `unmatched_files.csv`）。"
      % classes.get("unmatched", 0))
    w()
    if unmatched:
        by_pat = {}
        for r in unmatched:
            key = r.get("name_pattern", "?")
            g = by_pat.setdefault(key, {"n": 0, "data": 0, "bytes": 0, "samples": []})
            g["n"] += 1
            g["data"] += int(r.get("is_data_file", 0))
            g["bytes"] += int(r.get("bytes", 0))
            if len(g["samples"]) < 3:
                g["samples"].append(r.get("file_name", ""))
        w("| 细分命名模式 / pattern | 文件数 | 其中数据文件 | 字节 | 样例文件名 |")
        w("|---|---|---|---|---|")
        for key in sorted(by_pat, key=lambda k: -by_pat[k]["n"]):
            g = by_pat[key]
            w("| `%s` | %d | %d | %.3f MB | %s |"
              % (key, g["n"], g["data"], g["bytes"] / 1048576.0,
                 "; ".join("`%s`" % s for s in g["samples"])))
        w()
        n_data = sum(1 for r in unmatched if r.get("is_data_file"))
        n_incl = sum(1 for r in unmatched if r.get("included_in_stats"))
        w("- 其中 **%d 个为数据文件**（内容符合四列 schema），**%d 个已并入五层聚合**；"
          "其余 %d 个为非数据文件（说明文件/压缩包/二进制/空文件等），单列 `unmatched_files.csv` 说明。"
          % (n_data, n_incl, len(unmatched) - n_data))
        w("- **成因**：`unmatched` 类以「紧凑时间命名（`HHMMSS`，无连字符）」与/或「非 `.csv` 扩展名」为主；"
          "首轮 `.csv` 扩展名过滤会漏掉后者——这正是补丁 01 修复的缺口。")
    else:
        w("本次运行未发现 `unmatched` 类文件。 / No unmatched-class files in this run.")
    w()

    # --- §3.2 覆盖期是否变化 / coverage-period change ------------------
    w("### 7.2 补入后全集覆盖期 / coverage period after rescan（DF-10 解冻依据）")
    w()
    if agg.get("time_max") is not None:
        last_local = str(timeutil.local_date_keys([agg["time_max"]])[0])
        first_local = str(timeutil.local_date_keys([agg["time_min"]])[0])
        months = e5.get("months") or sorted({_split3(k)[0] for k in agg.get("stats", {})})
        past = last_local > "2022-07-25"
        has_aug = any(m >= "2022-08" for m in months)
        w("- 覆盖当地日期 / local coverage：**%s → %s**" % (first_local, last_local))
        w("- 出现月份 / months present：%s" % ", ".join(months))
        w("- **是否延伸过 2022-07-25** / extends past 2022-07-25：**%s**" % ("是 / yes" if past else "否 / no"))
        w("- **是否出现 2022-08/09 月数据** / 2022-08 or later present：**%s**"
          % ("是 / yes" if has_aug else "否 / no"))
        w()
        w("> 供设计会话裁决 DF-10 与 A9「六个月」表述：以上为补扫后的覆盖事实。")
    else:
        w("- %s" % NA)
    w()

    # --- §3.3 关键结论差异对照 / diff vs baseline ----------------------
    w("### 7.3 补入后关键结论差异对照 / key-metric diff vs baseline")
    w()
    diff = agg.get("_diff")
    if diff is None:
        w("> 未提供基线聚合（`report_gen.py --baseline <首轮 aggregate.json>`），无法生成逐项差异表。")
        w("> 如需差异对照，请在补扫后以首轮 `aggregate.json` 作基线重跑报告。")
        w(">")
        w("> 当前全集关键指标（供人工对照）/ current key metrics for manual comparison：")
        w()
        w("| 指标 / metric | 值 / value |")
        w("|---|---|")
        w("| 发现文件数 / files discovered | %d |" % files.get("total", 0))
        w("| 并入统计的数据文件 / data files in stats | %d |" % files.get("ok", 0))
        w("| 解析数据行 / parsed rows | %d |" % files.get("rows_parsed", 0))
        if agg.get("accel_nonzero"):
            w("| Accelerometer 非零计数 / non-zero | %d |"
              % sum(int(v) for v in agg.get("accel_nonzero", {}).values()))
    else:
        w("基线 / baseline：`%s`。仅列**有变化**的项。" % diff.get("baseline_label", "?"))
        w()
        rows = diff.get("rows", [])
        if rows:
            w("| 指标 / metric | 基线 / baseline | 补扫后 / after | 变化 / change |")
            w("|---|---|---|---|")
            for metric, base, after, change in rows:
                w("| %s | %s | %s | %s |" % (metric, base, after, change))
        else:
            w("补扫前后关键结论**无变化**。 / No key metric changed after the rescan.")
    w()

    # --- §3.4 与本地清点对账 / reconciliation --------------------------
    w("### 7.4 与用户本地清点对账 / reconciliation with the local count")
    w()
    total = files.get("total", 0)
    accounted = files.get("ok", 0) + files.get("failed", 0) + files.get("non_data", 0)
    w("- **归属恒等式（可验证）** / accounting identity：发现 %d = 成功 %d + 失败数据文件 %d "
      "+ 非数据文件 %d = %d —— %s"
      % (total, files.get("ok", 0), files.get("failed", 0), files.get("non_data", 0), accounted,
         "平衡 / balanced" if accounted == total else "**不平衡，须排查 / IMBALANCED**"))
    w("- 全集数据行 / total data rows：%d；总字节 %.3f GB（数据 %.3f + 非数据 %.3f）"
      % (files.get("rows_parsed", 0), files.get("bytes_total", 0) / 1073741824.0,
         files.get("bytes_data", 0) / 1073741824.0, files.get("bytes_non_data", 0) / 1073741824.0))
    # 与交接文档 §1 本地清点（3747 / 2.46 GB）对账——仅当规模可比时给出。
    if total >= 0.5 * config.PATCH01_LOCAL_FILES:
        resid_files = total - config.PATCH01_LOCAL_FILES
        gb = files.get("bytes_total", 0) / 1073741824.0
        resid_gb = gb - config.PATCH01_LOCAL_BYTES_GB
        w("- **对账参照（交接文档 §1 本地清点：%d 个 / %.2f GB）**："
          "发现 %d（残差 %+d）、总字节 %.2f GB（残差 %+.2f GB）。"
          % (config.PATCH01_LOCAL_FILES, config.PATCH01_LOCAL_BYTES_GB, total, resid_files, gb, resid_gb))
        w("  - 首轮报告发现数 %d → 本轮 %d，**回收 %+d 个文件**。允许残差来源：非数据文件、"
          "符号链接（本工具不跟随）、运行期间目录变动。"
          % (config.PATCH01_PREV_FOUND, total, total - config.PATCH01_PREV_FOUND))
    else:
        w("- 对账参照（本地清点 %d 个 / %.2f GB）：本次为合成/子集运行，规模不可比，参照不适用。"
          % (config.PATCH01_LOCAL_FILES, config.PATCH01_LOCAL_BYTES_GB))
    w()


# ---------------------------------------------------------------------------
# eda_report.md
# ---------------------------------------------------------------------------

def write_report(agg, out_dir, e1, e2, e3, e4, e5, by_ds, by_ds_hist, facts) -> str:
    """
    汇总报告：五层结果 + 图表引用 + M1 设计参数建议八问。
    Summary report: the five analysis layers, figure references and the eight
    M1 design-parameter questions.
    """
    run = agg.get("run", {})
    files = agg.get("files", {})
    devices, sensors = e2["devices"], e2["sensors"]
    L = []

    def w(line=""):
        L.append(line)

    w("# EDA 报告：Erol/SYNERGIA 室内环境数据集")
    w("# EDA Report: Erol/SYNERGIA Indoor Environment Dataset")
    w()
    w("> 生成方式 / generated by: `eda/run_eda.py` + `eda/report_gen.py`（本文件为自动生成，"
      "勿手工编辑；结论性文字由脚本按实测数值填充）")
    w("> Auto-generated; do not hand-edit. Every conclusion below is filled in from measured values.")
    w()
    w("| 项 / item | 值 / value |")
    w("|---|---|")
    w("| 数据目录 / data dir | `%s` |" % run.get("data_dir", "?"))
    w("| 生成时间 (UTC) / generated at | %s |" % run.get("generated_at_utc", "?"))
    w("| 文件发现 / files found | %s |" % run.get("files_found", "?"))
    w("| 本次处理 / files processed | %s%s |" % (run.get("files_processed", "?"),
      "（`--limit %s` 冒烟运行 / smoke run）" % run.get("limit") if run.get("limit") else ""))
    w("| 扫描用时 / elapsed | %s s (%.2f min) |" % (run.get("elapsed_s", "?"),
      float(run.get("elapsed_s", 0)) / 60.0))
    w("| 峰值内存估计 / peak RSS estimate | %s MB（主进程 %s MB + 单子进程最大 %s MB × %s workers）|"
      % (run.get("peak_rss_estimate_mb", "?"), run.get("peak_rss_parent_mb", "?"),
         run.get("peak_rss_max_child_mb", "?"), run.get("workers", "?")))
    w("| Python | %s |" % run.get("python", "?"))
    w()
    if run.get("limit"):
        w("> ⚠️ **本报告基于 `--limit %s` 的子集，不能作为全集结论使用。**"
          " / This report covers a `--limit %s` subset and is not a whole-dataset conclusion."
          % (run.get("limit"), run.get("limit")))
        w()

    # --- 0 阅读须知 ---------------------------------------------------
    w("## 0. 阅读须知 / How to read this report")
    w()
    w("1. **近似值标注**：中位数、IQR、P10/P50/P90、KS 统计量均由固定分辨率直方图导出，"
      "精度等于该通道的分箱宽度（见下表）；min/max/mean/std 为精确值（Welford 单遍 + 并行合并）。")
    w("   Histogram-derived numbers (median, IQR, P10/P50/P90, KS) are approximate with precision "
      "equal to the channel bin width; min/max/mean/std are exact.")
    w()
    w("| 通道 / channel | 分箱范围 / range | 分箱宽度 = 精度 / bin width |")
    w("|---|---|---|")
    for ch in sensors:
        lo, hi, width = config.BIN_SPECS.get(ch, config.DEFAULT_BIN_SPEC)
        w("| %s | [%g, %g] | %g |" % (ch, lo, hi, width))
    w()
    w("2. **时间口径**：`Time` 字段为 UTC epoch 秒；一切**日历分桶**（日期、月份、uptime、"
      "季节趋势）按 Europe/London 当地时间（夏令时 UTC+1）换算，昼夜解读直接可用。"
      " / Time is UTC epoch seconds; all calendar bucketing uses Europe/London local time.")
    w("3. **跨文件边界**：到达间隔与缺口已用逐文件 [first_ts, last_ts] 缝合；"
      "采样轮齐全率为**文件内闭合统计**，跨文件边界被切开的轮按近似处理。"
      " / Round completeness is computed within files; boundary-split rounds are approximated.")
    w("4. **哨兵值处置**：%s 等缺失占位值**已从分布统计与直方图中排除，只计数**"
      "（否则均值/最小值被占位符拖走）。这是分析视图，不是数据清洗——原始数据一字未改。"
      " / Sentinel placeholders are excluded from the statistics and histograms and only "
      "counted; the raw data is untouched."
      % ", ".join(str(v) for v in config.SENTINEL_VALUES))
    w("5. **禁区遵守**：本阶段只做分析，未产出任何清洗版数据、未试跑任何检测/漂移算法"
      "（交接文档 §8）。 / Analysis only: no cleaned dataset, no detector/drift trial runs.")
    w()

    # --- 1 E1 ---------------------------------------------------------
    w("## 1. E1 清单层 / Inventory")
    w()
    classes = agg.get("name_classes", {})
    accounted = files.get("ok", 0) + files.get("failed", 0) + files.get("non_data", 0)
    w("- 发现文件总数 / files discovered：**%d**（无条件递归列举全部常规文件，补丁 01；"
      "应等于 `find <dir> -type f | wc -l`）" % files.get("total", 0))
    w("  - 归属核验 / accounting：处理成功 %d + 失败数据文件 %d + 非数据文件 %d = **%d** —— %s"
      % (files.get("ok", 0), files.get("failed", 0), files.get("non_data", 0), accounted,
         "与发现总数一致，零静默跳过 / matches discovery, zero silent skips"
         if accounted == files.get("total", 0) else "**与发现总数不一致，须排查 / MISMATCH**"))
    w("- 命名三类归属 / three naming classes（补丁 01）：`dashed_data`=%d, `dashed_plain`=%d, "
      "`unmatched`=%d（未匹配清单见 `unmatched_files.csv`）"
      % (classes.get("dashed_data", 0), classes.get("dashed_plain", 0), classes.get("unmatched", 0)))
    w("- 细粒度命名模式 / fine-grained patterns：%s"
      % ", ".join("`%s` = %d" % (k, v) for k, v in sorted(agg.get("patterns", {}).items())))
    w("- 总字节数 / total bytes：**%.3f GB**（数据文件 %.3f GB + 非数据文件 %.3f GB）"
      % (files.get("bytes_total", 0) / 1073741824.0, files.get("bytes_data", 0) / 1073741824.0,
         files.get("bytes_non_data", 0) / 1073741824.0))
    w("- 解析数据行 / parsed data rows：**%d**" % files.get("rows_parsed", 0))
    w("- 带表头文件 / files with header：%d；无表头 / without header：%d"
      % (files.get("with_header", 0), files.get("without_header", 0)))
    if agg.get("time_min") is not None:
        w("- 覆盖时间线 / coverage：**%s → %s**（当地日期，共 %d 天）"
          % (e1.get("first_date"), e1.get("last_date"), e1.get("n_days", 0)))
        w("  - UTC 起止 / UTC bounds：%s → %s"
          % (timeutil.utc_iso(agg["time_min"]), timeutil.utc_iso(agg["time_max"])))
    w("- 按周文件数直方图 / files per week：见 `files_per_week.png`、`files_per_week.csv`"
      "（共 %d 个 ISO 周）" % e1.get("weeks", 0))
    w("- 逐文件台账 / per-file ledger：`file_inventory.csv`（%d 行，兼作断点续跑处理记录）"
      % files.get("total", 0))
    w()

    # --- 2 E2 ---------------------------------------------------------
    w("## 2. E2 完整性层 / Completeness")
    w()
    w("### 2.1 设备 × 传感器行数矩阵 / device x sensor row counts")
    w()
    w("完整表见 `device_sensor_counts.csv`。 / Full table in `device_sensor_counts.csv`.")
    w()
    w("| device | " + " | ".join(sensors) + " | total |")
    w("|" + "---|" * (len(sensors) + 2))
    for dev in devices:
        cells = [str(int(by_ds.get((dev, s), st.EMPTY_WELFORD)[0])) for s in sensors]
        w("| %s | %s | %d |" % (dev, " | ".join(cells), sum(int(c) for c in cells)))
    w()
    w("### 2.2 设备在场（uptime）与 DEV-Q2 / device uptime and DEV-Q2")
    w()
    w("在场矩阵（设备 × 当地日期 → 行数）见 `uptime_matrix.csv`，图见 `uptime_timeline.png`。")
    w()
    w("| device | 有数据天数 / days present | 占比 / share | 采样轮总数 / rounds |")
    w("|---|---|---|---|")
    n_days = e2.get("n_days", 0) or 1
    for dev in devices:
        days = e2["presence_days"].get(dev, 0)
        w("| %s | %d / %d | %.1f%% | %d |"
          % (dev, days, e2.get("n_days", 0), days / n_days * 100.0,
             agg.get("rounds_total", {}).get(dev, 0)))
    w()
    w("### 2.3 采样轮齐全率 / round completeness")
    w()
    w("每轮 8 传感器齐备的比例（文件内闭合统计）见 `round_completeness.csv`：")
    w()
    w("| device | 轮数 / rounds | 8 齐备 / full | 齐全率 / full-rate |")
    w("|---|---|---|---|")
    for dev in devices:
        sizes = {int(k): int(v) for k, v in agg.get("round_size", {}).get(dev, {}).items()}
        total = sum(sizes.values())
        full = sizes.get(len(config.EXPECTED_SENSORS), 0)
        w("| %s | %d | %d | %.4f%% |" % (dev, total, full, (full / total * 100.0) if total else float("nan")))
    w()
    w("### 2.4 缺口分析 / gaps")
    w()
    w("缺口定义：同一设备相邻采样轮间隔 > %d s。最长 %d 条见 `gaps_top100.csv`，"
      "逐设备汇总见 `gap_summary.csv`，时长分布见 `gap_duration_hist.png`。"
      % (config.GAP_THRESHOLD_S, config.GAPS_TOP_N))
    w()
    span_total = (agg.get("time_max") or 0) - (agg.get("time_min") or 0)
    w("| device | 缺口段数 / count | 缺口总时长 / total | 最长缺口 / max | 占全集跨度 / share |")
    w("|---|---|---|---|---|")
    for dev in devices:
        s = e2["gap_summary"].get(dev, {"count": 0, "total_s": 0, "max_s": 0})
        share = (s["total_s"] / span_total * 100.0) if span_total else float("nan")
        w("| %s | %d | %.2f h | %.2f h | %.2f%% |"
          % (dev, s["count"], s["total_s"] / 3600.0, s["max_s"] / 3600.0, share))
    w()
    w("### 2.5 重复时间戳 / duplicate (Device, Time, Sensor)")
    w()
    w("- 重复键的多余行数 / extra rows beyond the first occurrence：**%d**" % agg.get("dup_extra_rows", 0))
    w("- 出现重复的键个数 / distinct duplicated keys：%d" % agg.get("dup_keys", 0))
    w("- 口径 / scope：文件内统计；同一 (Device, Time, Sensor) 出现多值即计入，"
      "M1 的去重策略（保留首值 / 取均值 / 全部保留）属 M1 设计决策，本阶段不预设。")
    w()

    # --- 3 E3 ---------------------------------------------------------
    w("## 3. E3 数值层 / Numeric")
    w()
    w("逐设备 × 逐通道的 count/min/max/mean/std/近似中位数/IQR 见 `channel_stats.csv`，"
      "逐月版本见 `channel_stats_monthly.csv`。")
    w()
    w("### 3.1 全集通道摘要（设备合并）/ whole-dataset channel summary (devices pooled)")
    w()
    w("| channel | count | min | max | mean | std | median≈ | IQR≈ |")
    w("|---|---|---|---|---|---|---|---|")
    for sensor in sensors:
        acc, hist = None, {}
        for dev in devices:
            acc = st.welford_merge(acc, by_ds.get((dev, sensor)))
            st.hist_merge_inplace(hist, by_ds_hist.get((dev, sensor), {}))
        if not acc or acc[0] == 0:
            continue
        med, _q1, _q3, iqr = _median_iqr(acc, hist, sensor)
        w("| %s | %d | %s | %s | %s | %s | %s | %s |"
          % (sensor, acc[0], _fmt(acc[3], 4), _fmt(acc[4], 4), _fmt(acc[1], 4),
             _fmt(st.welford_std(acc), 4), _fmt(med, 3), _fmt(iqr, 3)))
    w()
    w("### 3.2 非法值 / invalid values")
    w()
    w("- 值不可解析（→NaN）/ unparsable values：**%d**" % files.get("rows_bad_value", 0))
    w("- 哨兵值命中 / sentinel hits（%s）：**%d**"
      % (", ".join(str(v) for v in config.SENTINEL_VALUES),
         sum(int(v) for v in agg.get("sentinel", {}).values())))
    w("- 物理量程越界 / out-of-range：**%d**（逐通道见 `unit_sanity.csv`）"
      % sum(int(v) for v in agg.get("oor", {}).values()))
    w()
    w("### 3.3 单位合理性核验 / unit sanity check")
    w()
    w("| channel | observed min | observed max | expected range | out-of-range | share |")
    w("|---|---|---|---|---|---|")
    for row in e3.get("sanity_rows", []):
        rng = "—" if row[5] == "" else "[%s, %s]" % (row[5], row[6])
        w("| %s | %s | %s | %s | %s | %s |"
          % (row[0], row[2], row[3], rng, row[7] if row[7] != "" else "—",
             ("%s%%" % row[8]) if row[8] != "" else "—"))
    w()
    w("### 3.4 Accelerometer：DEV-Q1 裁决证据 / evidence for DEV-Q1")
    w()
    nz, zero = e3.get("accel_nonzero", 0), e3.get("accel_zero", 0)
    tot = nz + zero
    w("- 零值 / zeros：**%d**；非零 / non-zero：**%d**（占 %.6f%%）"
      % (zero, nz, (nz / tot * 100.0) if tot else float("nan")))
    if nz:
        w("- 非零逐设备分布 / non-zero by device：%s"
          % ", ".join("%s=%s" % (k, v) for k, v in sorted(e3.get("accel_nonzero_per_device", {}).items())))
        trunc = "（**已按全集上限截断** / truncated at the global cap）" \
            if e3.get("accel_records_truncated") else ""
        w("- 全部非零记录 / full non-zero listing：`accel_nonzero.csv`%s" % trunc)
    w()
    w("### 3.5 MIC 取值分布 / MIC value distribution")
    w()
    w("- 全集出现的取值 / values observed：**%s**"
      % ", ".join("%.2g" % v for v in e3.get("mic_values_seen", [])))
    mic_total, mic_ones = e3.get("mic_total", 0), e3.get("mic_ones", 0)
    w("- MIC=1 占比 / share of MIC=1：**%.3f%%**（%d / %d）"
      % ((mic_ones / mic_total * 100.0) if mic_total else float("nan"), mic_ones, mic_total))
    w("- 逐月 MIC=1 占比 / monthly share：")
    w()
    w("| month | MIC=1 | total | share |")
    w("|---|---|---|---|")
    for month in e3.get("months", []):
        t = e3["mic_month_totals"].get(month, 0)
        o = e3["mic_month_ones"].get(month, 0)
        if t:
            w("| %s | %d | %d | %.3f%% |" % (month, o, t, o / t * 100.0))
    w()
    w("逐月逐设备明细见 `mic_distribution.csv`。")
    w()
    w("### 3.6 RSSI 电平迁移预览 / RSSI level drift preview")
    w()
    w("逐设备逐月 RSSI 均值 ± std 见 `channel_stats_monthly.csv`（筛 `sensor == RSSI`）。")
    w()

    # --- 4 E4 ---------------------------------------------------------
    w("## 4. E4 节奏层 / Rhythm")
    w()
    w("### 4.1 到达间隔分位数 / inter-arrival quantiles")
    w()
    w("采样轮（unique timestamp）到达间隔，1 s 分箱，含跨文件边界缝合；"
      "完整表见 `interarrival_quantiles.csv`，分布图见 `interarrival_distribution.png`。")
    w()
    w("两套口径 / two views：**全量**含设备离线造成的长间隔；**剔除缺口**只保留 ≤ %d s 的间隔，"
      "代表正常运行期抖动——M1 的 watermark 应按后者取值。"
      " / The full view includes outage-scale intervals; the gap-excluded view keeps only "
      "intervals <= %d s (normal-operation jitter) and is the one to size the watermark from."
      % (config.GAP_THRESHOLD_S, config.GAP_THRESHOLD_S))
    w()
    w("| device | 间隔数 | mean | P50 | P90 | P99 | P99.9 | P99（剔缺口）| P99.9（剔缺口）| "
      "≥%d s 溢出 / overflow |" % config.INTERARRIVAL_OVERFLOW_BIN)
    w("|---|---|---|---|---|---|---|---|---|---|")
    for row in e4.get("rows", []):
        w("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s (%s%%) |"
          % (row[0], row[1], row[2], row[3], row[4], row[5], row[6],
             row[10], row[11], row[13], row[14]))
    w()
    w("### 4.2 跨设备时钟相位 / cross-device clock phase")
    w()
    w("抽样文件数 / sampled files：**%d**（每个文件名日期取首个文件）；"
      "参考设备分布 / reference device usage：%s"
      % (e4.get("skew_files", 0),
         ", ".join("%s=%d" % (k, v) for k, v in sorted(e4.get("skew_refs", {}).items())) or "—"))
    w()
    if e4.get("skew_rows"):
        w("| device | 匹配轮数 / matched | mean offset | P50 | P95 | \\|offset\\| P95 | \\|offset\\| P99 | 窗外 / outside |")
        w("|---|---|---|---|---|---|---|---|")
        for r in e4["skew_rows"]:
            w("| %s | %s | %s | %s | %s | %s | %s | %s |"
              % (r[0], r[1], r[2], r[4], r[5], r[6], r[7], r[8]))
    else:
        w("（无抽样结果 / no sample collected）")
    w()

    # --- 5 E5 ---------------------------------------------------------
    w("## 5. E5 漂移预览层 / Drift preview")
    w()
    w("### 5.1 逐月分位数漂移 / monthly quantile drift")
    w()
    for channel in config.F_DET_CHANNELS:
        series = e5.get("drift", {}).get(channel)
        if not series:
            continue
        w("**%s** — `monthly_quantile_drift_%s.png`" % (channel, channel.lower()))
        w()
        w("| month | P10≈ | P50≈ | P90≈ | n |")
        w("|---|---|---|---|---|")
        for month, p10, p50, p90, n in series:
            w("| %s | %s | %s | %s | %d |" % (month, _fmt(p10, 3), _fmt(p50, 3), _fmt(p90, 3), n))
        w()
    w("### 5.2 相邻月 KS 统计量（近似）/ adjacent-month KS (approximate)")
    w()
    w("由逐月直方图的经验 CDF 近似计算，**为近似值**；完整表见 `ks_adjacent_months.csv`。")
    w()
    if not e5.get("ks_by_channel"):
        w("**%s**：数据仅覆盖 %d 个月，不存在相邻月对。"
          " / Only %d month(s) covered, so there is no adjacent-month pair."
          % (NA, len(e5.get("months", [])), len(e5.get("months", []))))
        w()
    w("| channel | 最强迁移月对 / strongest transition | KS≈ | 均值 KS / mean KS |")
    w("|---|---|---|---|")
    for channel, items in sorted(e5.get("ks_by_channel", {}).items()):
        if not items:
            continue
        top = max(items, key=lambda x: (x[2] if not math.isnan(x[2]) else -1))
        vals = [x[2] for x in items if not math.isnan(x[2])]
        w("| %s | %s → %s | %s | %s |"
          % (channel, top[0], top[1], _fmt(top[2], 4),
             _fmt(sum(vals) / len(vals) if vals else float("nan"), 4)))
    w()
    w("### 5.3 季节趋势 / seasonal trend")
    w()
    w("温/湿/光的日均值时间序列见 `seasonal_trend.png`（数据 `daily_channel_means.csv`）；"
      "日界按 Europe/London 当地时间。")
    w()

    # --- 6 健壮性 ------------------------------------------------------
    w("## 6. 异常文件与健壮性统计 / Anomalous files and robustness")
    w()
    w("| 类别 / category | 计数 / count |")
    w("|---|---|")
    w("| 处理失败的文件 / failed files | %d |" % files.get("failed", 0))
    w("| 畸形行（列数不符，已跳过）/ malformed rows skipped | %d |" % files.get("rows_malformed", 0))
    w("| Time 不可解析的行 / unparsable Time | %d |" % files.get("rows_bad_time", 0))
    w("| DeviceId/Sensor 缺失的行 / missing key fields | %d |" % files.get("rows_bad_key", 0))
    w("| Value 不可解析（NaN）/ unparsable Value | %d |" % files.get("rows_bad_value", 0))
    w("| 文件时间范围重叠次数 / overlapping file ranges | %d |"
      % agg.get("boundary", {}).get("overlaps", 0))
    w("| 跨文件边界折入的间隔数 / stitched boundary deltas | %d |"
      % agg.get("boundary", {}).get("deltas_folded", 0))
    w()
    if agg.get("errors"):
        w("失败原因分布 / failure reasons：")
        w()
        w("| 原因 / reason | 文件数 / files |")
        w("|---|---|")
        for k, v in sorted(agg["errors"].items(), key=lambda kv: -kv[1]):
            w("| `%s` | %d |" % (k, v))
        w()
    w("逐文件明细（含 error 列）见 `file_inventory.csv`。")
    w()

    # --- 7 补丁 01：文件发现补扫 ----------------------------------------
    _emit_patch01(w, agg, e5, files)

    # --- 8 数据事实对照 -------------------------------------------------
    w("## 8. 与交接文档 §3「数据事实」的对照 / Cross-check against handover §3")
    w()
    w("> 按交接文档 §8：与既有数据事实冲突的实测结果**如实报告并标注为新数据事实候选**，"
      "交回设计会话登记，EDA 不自行改写前提。")
    w()
    w("| 编号 | 原表述 / asserted | 实测 / observed | 结论 / verdict |")
    w("|---|---|---|---|")
    for fid, asserted, observed, verdict in facts:
        w("| %s | %s | %s | %s |" % (fid, asserted, observed, verdict))
    w()
    conflicts = [f for f in facts if "冲突" in f[3]]
    if conflicts:
        w("**新数据事实候选 / new-data-fact candidates**：%s —— 请设计会话登记。"
          % ", ".join(f[0] for f in conflicts))
    else:
        w("本次运行未发现与 §3 冲突的实测结果。 / No conflicts with §3 were observed in this run.")
    w()

    # --- 8 M1 设计参数建议 ---------------------------------------------
    w("## 9. M1 设计参数建议 / M1 design-parameter recommendations")
    w()
    w("> 交接文档 §6 的八问逐条作答；空缺项标注「%s」。" % NA)
    w()
    for i, (title, body) in enumerate(_answer_eight(agg, e1, e2, e3, e4, e5, by_ds, by_ds_hist), 1):
        w("### Q%d. %s" % (i, title))
        w()
        for line in body:
            w(line)
        w()

    # --- 10 产出索引 ----------------------------------------------------
    w("## 10. 产出文件索引 / Output index")
    w()
    w("| 层 / layer | 文件 / file |")
    w("|---|---|")
    w("| E1 | `file_inventory.csv`, `unmatched_files.csv`, `files_per_week.csv`, `files_per_week.png` |")
    w("| E2 | `device_sensor_counts.csv`, `uptime_matrix.csv`, `uptime_timeline.png`, "
      "`round_completeness.csv`, `gaps_top100.csv`, `gap_summary.csv`, `gap_duration_hist.png` |")
    w("| E3 | `channel_stats.csv`, `channel_stats_monthly.csv`, `accel_nonzero.csv`, "
      "`unit_sanity.csv`, `mic_distribution.csv`, `degenerate_channels.csv` |")
    w("| E4 | `interarrival_quantiles.csv`, `interarrival_distribution.png`, `clock_skew_sample.csv` |")
    w("| E5 | `monthly_quantile_drift_<channel>.png`, `ks_adjacent_months.csv`, "
      "`seasonal_trend.png`, `daily_channel_means.csv` |")
    w("| 原始聚合 / raw aggregate | `aggregate.json`（report_gen.py 的输入，可重复出图）|")
    w()

    text = "\n".join(L) + "\n"
    path = os.path.join(out_dir, "eda_report.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)
    return path


# ---------------------------------------------------------------------------
# §6 八问作答 / the eight questions
# ---------------------------------------------------------------------------

def _answer_eight(agg, e1, e2, e3, e4, e5, by_ds, by_ds_hist):
    devices = e2["devices"]
    answers = []

    # Q1 watermark ------------------------------------------------------
    quant = e4.get("quantiles", {})
    quant_ng = e4.get("quantiles_excl_gaps", {})
    body = []
    if quant_ng:
        p99 = max(q[2] for q in quant_ng.values() if not math.isnan(q[2]))
        p999 = max(q[3] for q in quant_ng.values() if not math.isnan(q[3]))
        rec = int(math.ceil(max(p999, p99) * 2 / 5.0) * 5)
        body.append("- **正常运行期口径（剔除 > %d s 的缺口段）**：逐设备 P99 最大值 **%.0f s**、"
                    "P99.9 最大值 **%.0f s**（`interarrival_quantiles.csv` 的 `*_excl_gaps` 列）。"
                    % (config.GAP_THRESHOLD_S, p99, p999))
        if quant:
            p99_all = max(q[2] for q in quant.values() if not math.isnan(q[2]))
            p999_all = max(q[3] for q in quant.values() if not math.isnan(q[3]))
            body.append("- 全量口径（含离线缺口）作对照：P99 最大 %s s、P99.9 最大 %s s——"
                        "该尾部由设备离线主导，**不应**用于 watermark 定值。"
                        % (_q_label(p99_all), _q_label(p999_all)))
        body.append("- **建议 M1 watermark 允许延迟 = %d s**（≈ 2 × P99.9 上取整到 5 s 网格），"
                    "在「迟到数据丢弃率」与「窗口触发延迟」之间取保守侧。" % rec)
        body.append("  若 M1 采用 `BoundedOutOfOrderness`，起步取 %d s；"
                    "设备离线属缺口而非乱序，由缓存回填协议（见 Q7）承接，不靠 watermark 兜。" % rec)
        body.append("- 口径说明：到达间隔按**采样轮**（unique timestamp）计，非按行计；"
                    "跨文件边界已缝合。")
    else:
        body.append("- %s（到达间隔直方图为空）" % NA)
    answers.append(("M1 watermark 延迟建议值 / watermark lateness", body))

    # Q2 pivot tolerance -------------------------------------------------
    body = []
    comp = e2.get("completeness", {})
    if comp:
        worst = min(comp.values())
        body.append("- **同设备同轮 8 行严格同秒**：按 (Device, Time) 分组统计，"
                    "8 传感器齐备轮占比最低 %.4f%%（`round_completeness.csv`）——"
                    "同轮 8 行共享同一 `Time` 值成立，pivot 可按精确时间戳等值分组，"
                    "**设备内不需要时间容差**。" % (worst * 100))
    else:
        body.append("- 设备内同轮时间戳一致性：%s" % NA)
    if e4.get("skew_rows"):
        p95s = [float(r[6]) for r in e4["skew_rows"] if r[6] not in ("", "nan")]
        p99s = [float(r[7]) for r in e4["skew_rows"] if r[7] not in ("", "nan")]
        if p95s and p99s:
            tol = int(math.ceil(max(p99s)))
            body.append("- **跨设备对齐容差**：抽样文件的相位分布显示 |偏移| P95 最大 %.0f s、"
                        "P99 最大 %.0f s（`clock_skew_sample.csv`）。" % (max(p95s), max(p99s)))
            body.append("  → 跨设备同轮组装建议容差 **±%d s**；若采用论文式小时聚合，"
                        "该容差远小于聚合粒度，可忽略。" % max(tol, 1))
    else:
        body.append("- 跨设备对齐容差：%s（未采集到相位样本）" % NA)
    answers.append(("采样轮组装（pivot）的时间容差 / pivot time tolerance", body))

    # Q3 device scale -----------------------------------------------------
    body = []
    n_days = e2.get("n_days", 0)
    if n_days:
        pres = e2["presence_days"]
        body.append("| device | 有数据天数 | 占比 |")
        body.append("|---|---|---|")
        for dev in devices:
            body.append("| %s | %d / %d | %.1f%% |" % (dev, pres.get(dev, 0), n_days,
                                                       pres.get(dev, 0) / n_days * 100.0))
        body.append("")
        h_days = pres.get("H", 0)
        if "H" not in pres or h_days == 0:
            body.append("- **结论：H 全程缺席，开发期按 7 台计**（DEV-Q2 判定）。")
        elif h_days / n_days < 0.5:
            body.append("- **结论：H 非长期在场（在场 %.1f%% 的天数），开发期按 7 台计，"
                        "H 作为「间歇设备」用于测试设备上下线路径**（DEV-Q2 判定）。"
                        % (h_days / n_days * 100.0))
        else:
            body.append("- **结论：H 在场 %.1f%% 的天数，并非长期缺席 → 开发期按 8 台计**；"
                        "样例文件观察到的 H 缺席属局部现象，须登记为对 DF-3 的更正（DEV-Q2 判定）。"
                        % (h_days / n_days * 100.0))
        low = [d for d in devices if pres.get(d, 0) / n_days < 0.5]
        if low:
            body.append("- 其他在场率 < 50%% 的设备 / other devices below 50%% presence：%s" % ", ".join(low))
    else:
        body.append("- %s" % NA)
    answers.append(("设备规模结论（DEV-Q2）/ device count", body))

    # Q4 accelerometer -----------------------------------------------------
    body = []
    nz, zero = e3.get("accel_nonzero", 0), e3.get("accel_zero", 0)
    tot = nz + zero
    if not tot:
        body.append("- %s（无 Accelerometer 数据）" % NA)
    elif nz == 0:
        body.append("- 全集 %d 个 Accelerometer 读数**全部为 0.0**。" % tot)
        body.append("- **结论：恒零 → 归入侧信道集（side-channel set），不进检测特征集 F_det，"
                    "在管线中仅作监测**（DEV-D4 得到确认证据）。")
        body.append("- 同时提示 M1：RobustScaler 对该通道 IQR = 0，若误入标准化路径将除零，"
                    "退化防护必须覆盖（见 Q5）。")
    else:
        body.append("- 全集 %d 个读数中 **%d 个非零**（%.6f%%），逐设备：%s。"
                    % (tot, nz, nz / tot * 100.0,
                       ", ".join("%s=%s" % (k, v) for k, v in sorted(e3.get("accel_nonzero_per_device", {}).items()))))
        trunc = "（**已按全集上限截断**）" if e3.get("accel_records_truncated") else ""
        body.append("- 非零记录全量清单：`accel_nonzero.csv`%s。" % trunc)
        body.append("- **结论：非恒零 → 与 DF-5「恒 0.0」冲突，属新数据事实候选**；"
                    "DEV-Q1 的裁决须基于非零事件形态（发生频次、是否与其他通道同时异常）；"
                    "在形态未定性前，建议仍按侧信道处置并在管线中监测，不纳入 F_det。")
    answers.append(("Accelerometer 处置结论（DEV-Q1 / DEV-D4）/ Accelerometer disposition", body))

    # Q5 RobustScaler degeneracy ------------------------------------------
    body = []
    degen = e3.get("degenerate", [])
    if degen:
        body.append("- 检出 %d 个 (设备, 通道) 组合为常量或零 IQR（`degenerate_channels.csv`）：" % len(degen))
        for dev, sensor, sd, iqr, lo, hi, const in degen[:20]:
            body.append("  - %s / %s：std=%.6g, IQR≈%.6g, 取值范围 [%.6g, %.6g]%s"
                        % (dev, sensor, sd, iqr, lo, hi, "，**常量通道**" if const else ""))
        if len(degen) > 20:
            body.append("  - …… 其余 %d 项见 CSV" % (len(degen) - 20))
        body.append("- **结论：RobustScaler 退化防护的作用面 = 上述组合**；"
                    "M1 实现须对 IQR ≤ ε 的通道走旁路（原值透传或置零），"
                    "并把退化事件计数暴露给 M5a 作为数据层监测信号。")
    else:
        body.append("- 未检出常量或零 IQR 通道；但 IQR 近似精度为分箱宽度，"
                    "M1 仍应保留 IQR ≤ ε 的旁路分支（防御性）。")
    answers.append(("RobustScaler 退化防护的作用面 / RobustScaler degeneracy surface", body))

    # Q6 MIC gating --------------------------------------------------------
    body = []
    mic_total, mic_ones = e3.get("mic_total", 0), e3.get("mic_ones", 0)
    if mic_total:
        body.append("- 全集 MIC=1 占比 **%.3f%%**（%d / %d）；取值集合实测为 **%s**。"
                    % (mic_ones / mic_total * 100.0, mic_ones, mic_total,
                       ", ".join("%.2g" % v for v in e3.get("mic_values_seen", []))))
        body.append("- 逐月趋势 / monthly trend：")
        body.append("")
        body.append("| month | MIC=1 share |")
        body.append("|---|---|")
        for month in e3.get("months", []):
            t = e3["mic_month_totals"].get(month, 0)
            if t:
                body.append("| %s | %.3f%% |" % (month, e3["mic_month_ones"].get(month, 0) / t * 100.0))
        body.append("")
        body.append("- **结论：若 M1 以 MIC 作质量门控（quality gate），其作用面为上述占比对应的行**；"
                    "占比越高、门控丢弃的数据越多，须与 S6 材料评估联动确认门控语义"
                    "（MIC 的物理含义未在数据集文档中明确，建议在门控启用前先取得语义确认）。")
    else:
        body.append("- %s（无 MIC 数据）" % NA)
    answers.append(("质量门控作用面：MIC 占比与趋势 / MIC-based quality gating", body))

    # Q7 gaps ---------------------------------------------------------------
    body = []
    gs = e2.get("gap_summary", {})
    span_total = (agg.get("time_max") or 0) - (agg.get("time_min") or 0)
    if gs and span_total:
        worst_dev = max(gs, key=lambda d: gs[d]["max_s"])
        total_gap = sum(v["total_s"] for v in gs.values())
        n_dev = max(1, len(gs))
        body.append("- 最长缺口 / longest gap：**%.2f h**（设备 %s）；全部 top-%d 见 `gaps_top100.csv`。"
                    % (gs[worst_dev]["max_s"] / 3600.0, worst_dev, config.GAPS_TOP_N))
        body.append("- 缺口总时长占比（逐设备平均）/ mean gap share per device：**%.2f%%**"
                    % (total_gap / (span_total * n_dev) * 100.0))
        body.append("- 缺口段数合计 / total gap segments：%d" % sum(v["count"] for v in gs.values()))
        body.append("- **对 M1 的含义**：缓存回填协议须能跨越 %.1f h 量级的空洞——"
                    "回填窗口若短于最长缺口，状态重建（B §12.4）在设备恢复后会拿不到足量历史；"
                    "建议回填缓存按「最长缺口 + 一个训练窗」取上界。"
                    % (gs[worst_dev]["max_s"] / 3600.0))
        body.append("- **对 S6 材料评估的含义**：真实缺口形态可直接充当 M6 再训练数据完整性的压力材料，"
                    "无需合成注入。")
    else:
        body.append("- %s" % NA)
    answers.append(("缺口形态摘要 / gap profile", body))

    # Q8 drift ranking -------------------------------------------------------
    body = []
    ks = e5.get("ks_by_channel", {})
    if ks:
        ranked = []
        for channel, items in ks.items():
            vals = [x[2] for x in items if not math.isnan(x[2])]
            if vals:
                ranked.append((channel, sum(vals) / len(vals), max(vals),
                               max(items, key=lambda x: (x[2] if not math.isnan(x[2]) else -1))))
        ranked.sort(key=lambda r: -r[1])
        body.append("| 排名 | channel | 均值 KS≈ | 最大 KS≈ | 最强月对 |")
        body.append("|---|---|---|---|---|")
        for i, (channel, mean_ks, max_ks, top) in enumerate(ranked, 1):
            body.append("| %d | %s | %.4f | %.4f | %s → %s |"
                        % (i, channel, mean_ks, max_ks, top[0], top[1]))
        body.append("")
        if ranked:
            body.append("- **结论：迁移最强通道 = %s**（均值 KS≈%.4f）；"
                        "最强单次月际迁移出现在 %s 的 %s → %s（KS≈%.4f）。"
                        % (ranked[0][0], ranked[0][1], ranked[0][0], ranked[0][3][0],
                           ranked[0][3][1], ranked[0][2]))
            body.append("- **对 M5a/M6 实验选段的建议**：以上述最强月对作为漂移正样本段，"
                        "以 KS 最小的月对作为阴性对照段；KS 由直方图 CDF 近似，"
                        "作为**选段依据**足够，不可直接当作检验统计量的 p 值来源。")
    else:
        body.append("- %s" % NA)
    answers.append(("各月分布迁移强度排序 / monthly distribution-shift ranking", body))

    return answers
