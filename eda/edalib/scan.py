"""
单文件扫描器：一个 CSV → 一份可合并、可落盘的部分聚合（partial aggregate）。
Single-file scanner: one CSV → one mergeable, cacheable partial aggregate.

设计要点 / design notes:
1. **健壮优先**：任何异常（空文件、截断、列数不符、数值不可解析、编码错误）一律
   计数并继续，绝不抛出到进程池之外——交接文档 §3.5 的硬要求。
   Robustness first: every anomaly (empty/truncated file, wrong column count,
   unparsable value, encoding error) is counted and execution continues. Nothing
   escapes to the pool (handover §3.5).
2. **键的月份前缀**：数值层的键统一为 "YYYY-MM|Device|Sensor"。一份结构同时支撑
   E3 全集统计与 E5 逐月分布，避免缓存体积翻倍。
   Numeric-layer keys are "YYYY-MM|Device|Sensor" so a single structure serves both
   the E3 whole-dataset statistics and the E5 monthly distributions, which keeps the
   on-disk cache from doubling in size.
3. **文件内闭合**：到达间隔、缺口、采样轮齐全率均在文件内计算；跨文件边界由
   merge 阶段用逐文件 (first_ts, last_ts) 补齐（近似处理在报告中注明）。
   Inter-arrival, gaps and round completeness are computed within the file; the merge
   stage stitches file boundaries using per-file (first_ts, last_ts). The residual
   approximation is stated in the report.
"""

import io
import os
import warnings

import numpy as np
import pandas as pd

from . import config, timeutil

SCHEMA = 1
SEP = "|"


# ---------------------------------------------------------------------------
# 空/失败载荷 / empty and failure payloads
# ---------------------------------------------------------------------------

def _base_payload(path: str, data_dir: str) -> dict:
    st = os.stat(path)
    from .inventory import parse_filename

    name = os.path.basename(path)
    info = parse_filename(name)
    return {
        "schema": SCHEMA,
        "path": os.path.relpath(path, data_dir),
        "name": name,
        "bytes": int(st.st_size),
        "mtime": float(st.st_mtime),
        "name_pattern": info["pattern"],
        "name_ts_utc": info["name_ts_utc"],
        "ok": True,
        "error": None,
        "has_header": False,
        "lines_raw": 0,
        "rows_parsed": 0,
        "rows_malformed": 0,
        "rows_bad_time": 0,
        "rows_bad_key": 0,
        "rows_bad_value": 0,
        "time_min": None,
        "time_max": None,
        "n_rounds": 0,
        "stats": {},
        "hist": {},
        "nan": {},
        "sentinel": {},
        "oor": {},
        "uptime": {},
        "daily": {},
        "rounds": {},
        "round_size": {},
        "dup_extra_rows": 0,
        "dup_keys": 0,
        "accel_zero": {},
        "accel_nonzero": {},
        "accel_records": [],
        "accel_truncated": False,
        "skew": None,
    }


# ---------------------------------------------------------------------------
# 主扫描函数 / main scan entry
# ---------------------------------------------------------------------------

def scan_file(path: str, data_dir: str, sample_skew: bool = False,
              max_file_mb: float = 256.0) -> dict:
    """
    扫描单个 CSV，返回部分聚合字典（JSON 可序列化）。
    Scan one CSV and return a JSON-serialisable partial aggregate.

    失败兜底 / failure fallback: 返回的 payload 中 ok=False 且 error 记录原因，
    调用方计入报告，不中断整体运行。
    On failure the payload carries ok=False plus an error string; the caller records it
    in the report and the run continues.
    """
    try:
        payload = _base_payload(path, data_dir)
    except OSError as exc:  # stat 失败（权限/竞态删除）/ stat failed
        return {
            "schema": SCHEMA, "path": os.path.basename(path), "name": os.path.basename(path),
            "bytes": 0, "mtime": 0.0, "name_pattern": "unparsed", "name_ts_utc": None,
            "ok": False, "error": "stat failed: %s" % exc, "has_header": False,
            "lines_raw": 0, "rows_parsed": 0, "rows_malformed": 0, "rows_bad_time": 0,
            "rows_bad_key": 0, "rows_bad_value": 0, "time_min": None, "time_max": None,
            "n_rounds": 0, "stats": {}, "hist": {}, "nan": {}, "sentinel": {}, "oor": {},
            "uptime": {}, "daily": {}, "rounds": {}, "round_size": {},
            "dup_extra_rows": 0, "dup_keys": 0, "accel_zero": {}, "accel_nonzero": {},
            "accel_records": [], "accel_truncated": False, "skew": None,
        }

    if payload["bytes"] == 0:
        payload["ok"] = False
        payload["error"] = "empty file"
        return payload

    if payload["bytes"] > max_file_mb * 1024 * 1024:
        # 超大文件超出"逐文件载入"的内存前提，跳过并计数（保护 <2GB 内存约束）。
        # Oversized files break the load-one-file-at-a-time memory premise: skip and count.
        payload["ok"] = False
        payload["error"] = "file larger than --max-file-mb (%.1f MB)" % (payload["bytes"] / 1048576.0)
        return payload

    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except OSError as exc:
        payload["ok"] = False
        payload["error"] = "read failed: %s" % exc
        return payload

    stripped = raw.rstrip()
    if not stripped:
        payload["ok"] = False
        payload["error"] = "blank file"
        return payload

    # 原始行数（尾部空行已剔除；文件内部空行的处理为近似，见报告注记）
    # Raw line count (trailing blanks stripped; interior blank lines are approximate)
    payload["lines_raw"] = stripped.count(b"\n") + 1

    first_line = stripped.split(b"\n", 1)[0].strip().lower()
    payload["has_header"] = first_line.startswith(b"time,deviceid")

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_csv(
                io.BytesIO(raw),
                header=0 if payload["has_header"] else None,
                names=config.CSV_COLUMNS,
                on_bad_lines="skip",
                engine="c",
                encoding="utf-8",
                encoding_errors="replace",
            )
    except Exception as exc:  # noqa: BLE001 —— 解析层兜底，任何异常都不得逃逸
        payload["ok"] = False
        payload["error"] = "parse failed: %s: %s" % (type(exc).__name__, exc)
        return payload

    payload["rows_parsed"] = int(len(df))
    data_lines = payload["lines_raw"] - (1 if payload["has_header"] else 0)
    payload["rows_malformed"] = max(0, data_lines - payload["rows_parsed"])

    if payload["rows_parsed"] == 0:
        payload["ok"] = False
        payload["error"] = "no parsable data rows"
        return payload

    try:
        _accumulate(df, payload, sample_skew)
    except Exception as exc:  # noqa: BLE001 —— 聚合层兜底 / aggregation-layer fallback
        payload["ok"] = False
        payload["error"] = "aggregate failed: %s: %s" % (type(exc).__name__, exc)
    return payload


# ---------------------------------------------------------------------------
# 聚合实现 / aggregation
# ---------------------------------------------------------------------------

def _accumulate(df: pd.DataFrame, payload: dict, sample_skew: bool) -> None:
    from . import stats as st

    # --- 类型规整 / type coercion ---------------------------------------
    time_col = pd.to_numeric(df["Time"], errors="coerce")
    value_col = pd.to_numeric(df["Value"], errors="coerce")
    dev_col = df["DeviceId"].astype("object")
    sen_col = df["Sensor"].astype("object")

    good_time = time_col.notna().to_numpy()
    good_key = (dev_col.notna() & sen_col.notna()).to_numpy()
    keep = good_time & good_key
    payload["rows_bad_time"] = int((~good_time).sum())
    payload["rows_bad_key"] = int((good_time & ~good_key).sum())
    if not keep.any():
        payload["ok"] = False
        payload["error"] = "no rows with usable Time/DeviceId/Sensor"
        return

    times = time_col.to_numpy(dtype="float64")[keep].astype(np.int64)
    values = value_col.to_numpy(dtype="float64")[keep]
    devices = np.asarray([str(x).strip() for x in dev_col.to_numpy()[keep]], dtype=object)
    sensors = np.asarray([str(x).strip() for x in sen_col.to_numpy()[keep]], dtype=object)

    payload["rows_bad_value"] = int(np.isnan(values).sum())
    payload["time_min"] = int(times.min())
    payload["time_max"] = int(times.max())

    months = timeutil.local_month_keys(times)
    dates = timeutil.local_date_keys(times)

    work = pd.DataFrame(
        {
            "dev": devices,
            "sensor": sensors,
            "time": times,
            "value": values,
            "month": months,
            "date": dates,
        }
    )

    # --- E1/E2 采样轮与在场 / rounds and presence ------------------------
    payload["n_rounds"] = int(np.unique(times).size)

    for dev, g in work.groupby("dev", sort=False):
        ts_u = np.unique(g["time"].to_numpy())
        entry = {
            "n": int(ts_u.size),
            "first": int(ts_u[0]),
            "last": int(ts_u[-1]),
            "ia": {},
            "gaps": [],
        }
        if ts_u.size > 1:
            deltas = np.diff(ts_u)
            binned = np.clip(deltas, 0, config.INTERARRIVAL_OVERFLOW_BIN)
            counts = np.bincount(binned, minlength=config.INTERARRIVAL_OVERFLOW_BIN + 1)
            entry["ia"] = {int(i): int(c) for i, c in enumerate(counts) if c}
            gap_idx = np.nonzero(deltas > config.GAP_THRESHOLD_S)[0]
            entry["gaps"] = [
                [int(ts_u[i]), int(ts_u[i + 1]), int(deltas[i])] for i in gap_idx
            ]
        payload["rounds"][str(dev)] = entry

    # uptime 在场矩阵：设备 × 当地日期 → 行数
    # uptime matrix: device x local date -> row count
    for (dev, date), n in work.groupby(["dev", "date"], sort=False).size().items():
        payload["uptime"]["%s%s%s" % (dev, SEP, date)] = int(n)

    # 采样轮齐全率：每轮的传感器行数分布（文件内闭合）
    # Round completeness: distribution of rows per round (within-file)
    rs = work.groupby(["dev", "time"], sort=False).size().reset_index(name="k")
    for dev, g in rs.groupby("dev", sort=False):
        counts = np.bincount(g["k"].to_numpy().astype(np.int64))
        payload["round_size"][str(dev)] = {int(i): int(c) for i, c in enumerate(counts) if c}

    # 重复 (Device, Time, Sensor) / duplicate key triples
    dup_extra = work.duplicated(subset=["dev", "time", "sensor"], keep="first")
    dup_any = work.duplicated(subset=["dev", "time", "sensor"], keep=False)
    payload["dup_extra_rows"] = int(dup_extra.sum())
    payload["dup_keys"] = int(dup_any.sum() - dup_extra.sum())

    # --- E3 数值层 / numeric layer ---------------------------------------
    valid = work[work["value"].notna()]
    nan_rows = work[work["value"].isna()]

    for (month, dev, sensor), n in nan_rows.groupby(["month", "dev", "sensor"], sort=False).size().items():
        payload["nan"]["%s%s%s%s%s" % (month, SEP, dev, SEP, sensor)] = int(n)

    sentinels = np.asarray(config.SENTINEL_VALUES, dtype=np.float64)

    for (month, dev, sensor), g in valid.groupby(["month", "dev", "sensor"], sort=False):
        key = "%s%s%s%s%s" % (month, SEP, dev, SEP, sensor)
        vals = g["value"].to_numpy(dtype=np.float64)

        # 哨兵值（-999 等缺失占位）从分布统计与直方图中排除，只计数——否则均值/最小值
        # 会被占位符拖走，中位数也会被 clip 进首桶。这是**分析视图**，不是数据清洗：
        # 原始数据一字未改（交接文档 §8）。
        # Sentinel values (missing-data placeholders such as -999) are excluded from the
        # distribution statistics and histograms and only counted: otherwise they drag the
        # mean/min and get clipped into the first histogram bin. This is an analysis view,
        # not data cleaning -- the raw data is left untouched (handover §8).
        sentinel_mask = np.isin(vals, sentinels)
        n_sentinel = int(sentinel_mask.sum())
        if n_sentinel:
            payload["sentinel"][key] = n_sentinel
            vals = vals[~sentinel_mask]
        if vals.size == 0:
            continue

        payload["stats"][key] = st.welford_from_array(vals)
        payload["hist"][key] = st.hist_from_array(vals, str(sensor))

        rng = config.PHYSICAL_RANGES.get(str(sensor))
        if rng is not None:
            n_oor = int(((vals < rng[0]) | (vals > rng[1])).sum())
            if n_oor:
                payload["oor"][key] = n_oor

    # 以下两项同样排除哨兵值：日均值与 Accelerometer 非零判定都会被 -999 污染。
    # The next two blocks also drop sentinels: daily means and the Accelerometer non-zero
    # test would both be corrupted by -999 placeholders.
    clean = valid[~np.isin(valid["value"].to_numpy(dtype=np.float64), sentinels)]

    # 逐日通道均值材料（季节趋势图）/ daily per-channel material for the seasonal trend
    for (date, sensor), g in clean.groupby(["date", "sensor"], sort=False):
        vals = g["value"].to_numpy(dtype=np.float64)
        payload["daily"]["%s%s%s" % (date, SEP, sensor)] = [int(vals.size), float(vals.sum())]

    # --- Accelerometer：DEV-Q1 裁决证据 / evidence for DEV-Q1 -------------
    accel = clean[clean["sensor"] == "Accelerometer"]
    if len(accel):
        acc_vals = accel["value"].to_numpy(dtype=np.float64)
        acc_dev = accel["dev"].to_numpy()
        acc_time = accel["time"].to_numpy()
        nonzero_mask = acc_vals != 0.0
        for dev in np.unique(acc_dev):
            dmask = acc_dev == dev
            nz = int((nonzero_mask & dmask).sum())
            payload["accel_zero"][str(dev)] = int(dmask.sum() - nz)
            if nz:
                payload["accel_nonzero"][str(dev)] = nz
        idx = np.nonzero(nonzero_mask)[0]
        if idx.size > config.ACCEL_NONZERO_CAP_PER_FILE:
            payload["accel_truncated"] = True
            idx = idx[: config.ACCEL_NONZERO_CAP_PER_FILE]
        payload["accel_records"] = [
            [int(acc_time[i]), str(acc_dev[i]), float(acc_vals[i])] for i in idx
        ]

    # --- E4 跨设备时钟相位（抽样文件才算）/ clock phase on sampled files ---
    if sample_skew:
        payload["skew"] = _clock_skew(work)


def _clock_skew(work: pd.DataFrame) -> dict:
    """
    同一采样轮内各设备时间戳相对参考设备的偏移分布。
    Offsets of each device's round timestamps relative to a reference device.

    参考设备 = 轮数最多者（并列取字典序最小），逐设备时间戳取最近的参考时间戳作差；
    |偏移| > SKEW_MAX_S 的记为窗外（不入直方图，另计数）。
    The reference device is the one with the most rounds (ties broken alphabetically).
    Each timestamp is differenced against the nearest reference timestamp; offsets beyond
    SKEW_MAX_S are counted separately instead of entering the histogram.
    """
    per_dev = {}
    for dev, g in work.groupby("dev", sort=False):
        per_dev[str(dev)] = np.unique(g["time"].to_numpy())
    if len(per_dev) < 2:
        return None
    ref = sorted(per_dev.keys(), key=lambda d: (-per_dev[d].size, d))[0]
    ref_ts = per_dev[ref]
    out = {"ref": ref, "ref_rounds": int(ref_ts.size), "hist": {}, "outside": {}}
    for dev, ts in per_dev.items():
        if dev == ref:
            continue
        pos = np.searchsorted(ref_ts, ts)
        pos = np.clip(pos, 1, ref_ts.size - 1) if ref_ts.size > 1 else np.zeros_like(pos)
        left = ref_ts[np.maximum(pos - 1, 0)]
        right = ref_ts[np.minimum(pos, ref_ts.size - 1)]
        nearest = np.where(np.abs(ts - left) <= np.abs(ts - right), left, right)
        delta = (ts - nearest).astype(np.int64)
        inside = np.abs(delta) <= config.SKEW_MAX_S
        out["outside"][dev] = int((~inside).sum())
        d_in = delta[inside]
        if d_in.size:
            shifted = d_in + config.SKEW_MAX_S
            counts = np.bincount(shifted, minlength=2 * config.SKEW_MAX_S + 1)
            out["hist"][dev] = {
                int(i - config.SKEW_MAX_S): int(c) for i, c in enumerate(counts) if c
            }
    return out
