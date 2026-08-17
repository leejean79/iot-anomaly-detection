"""
时间工具：UTC epoch 秒 → Europe/London 当地日历分桶。
Time utilities: UTC epoch seconds → Europe/London local calendar bucketing.

为什么需要 / why this exists:
数据集 Time 字段为 UTC epoch 秒，而站点在布里斯托（Europe/London，夏令时 UTC+1）。
交接文档 §4 E5 要求"日界与昼夜分析按当地时间换算"，因此一切**日历分桶**
（日期、月份、uptime 在场矩阵）使用当地时间；原始时间戳本身不做任何改写。
The Time column is UTC epoch seconds while the site is in Bristol (Europe/London,
UTC+1 during BST). Handover §4 E5 requires day boundaries to follow local time, so
every *calendar* bucket (date, month, uptime matrix) uses local time. Raw timestamps
are never rewritten.

实现取舍 / implementation trade-off:
不依赖 zoneinfo/pytz（macOS 上 tzdata 可用性不稳，且逐行时区换算代价高）。
BST 规则自 1996 年起稳定：3 月最后一个周日 01:00 UTC 起 +1h，10 月最后一个周日
01:00 UTC 止。此处按该规则预生成年度切换点，用 numpy searchsorted 向量化换算。
We avoid zoneinfo/pytz (tzdata availability varies on macOS and per-row conversion is
slow). The BST rule has been stable since 1996: +1h from the last Sunday of March at
01:00 UTC until the last Sunday of October at 01:00 UTC. Yearly switch points are
pre-computed and applied with a vectorised searchsorted.
"""

import calendar
import datetime as _dt

import numpy as np

_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)

# 预生成年份范围（覆盖数据集并留足余量）/ pre-computed year range (dataset + margin)
_YEAR_LO, _YEAR_HI = 1996, 2050


def _last_sunday_utc(year: int, month: int, hour: int) -> int:
    """
    某年某月最后一个周日 hour:00 UTC 的 epoch 秒。
    Epoch seconds of the last Sunday of (year, month) at hour:00 UTC.
    """
    last_day = calendar.monthrange(year, month)[1]
    d = _dt.date(year, month, last_day)
    # weekday(): Monday=0 ... Sunday=6
    d -= _dt.timedelta(days=(d.weekday() + 1) % 7)
    dt = _dt.datetime(d.year, d.month, d.day, hour, tzinfo=_dt.timezone.utc)
    return int((dt - _EPOCH).total_seconds())


def _build_switch_table():
    """
    构造 BST 切换点数组：升序的 epoch 秒切换点，及其后生效的偏移（秒）。
    Build the BST switch table: ascending epoch switch points and the offset in
    force after each one.
    """
    points, offsets = [], []
    for year in range(_YEAR_LO, _YEAR_HI + 1):
        points.append(_last_sunday_utc(year, 3, 1))   # BST 开始 / BST starts
        offsets.append(3600)
        points.append(_last_sunday_utc(year, 10, 1))  # BST 结束 / BST ends
        offsets.append(0)
    return np.asarray(points, dtype=np.int64), np.asarray(offsets, dtype=np.int64)


_SWITCH_POINTS, _SWITCH_OFFSETS = _build_switch_table()


def local_offset_seconds(ts):
    """
    向量化返回每个 UTC epoch 秒对应的 Europe/London UTC 偏移（0 或 3600）。
    Vectorised Europe/London UTC offset (0 or 3600) for each UTC epoch second.
    """
    ts = np.asarray(ts, dtype=np.int64)
    idx = np.searchsorted(_SWITCH_POINTS, ts, side="right") - 1
    # 表起点之前（1996 年 3 月前）一律按 UTC 处理，数据集不涉及。
    # Before the table start (pre-1996-03) we fall back to UTC; the dataset never hits this.
    out = np.where(idx >= 0, _SWITCH_OFFSETS[np.clip(idx, 0, None)], 0)
    return out.astype(np.int64)


def to_local_epoch(ts):
    """
    UTC epoch 秒 → "当地墙钟" epoch 秒（仅用于日历分桶，不是真实 UTC 时刻）。
    UTC epoch seconds → local wall-clock epoch seconds (calendar bucketing only;
    the result is NOT a real UTC instant).
    """
    ts = np.asarray(ts, dtype=np.int64)
    return ts + local_offset_seconds(ts)


def local_date_keys(ts):
    """
    返回当地日期字符串数组 'YYYY-MM-DD'。
    Return an array of local date strings 'YYYY-MM-DD'.
    """
    local = to_local_epoch(ts)
    return np.asarray(local.astype("datetime64[s]").astype("datetime64[D]"), dtype="U10")


def local_month_keys(ts):
    """
    返回当地月份字符串数组 'YYYY-MM'。
    Return an array of local month strings 'YYYY-MM'.
    """
    local = to_local_epoch(ts)
    return np.asarray(local.astype("datetime64[s]").astype("datetime64[M]"), dtype="U7")


def local_hour_of_day(ts):
    """
    返回当地小时（0–23），供昼夜相关解读使用。
    Return the local hour of day (0-23) for day/night interpretation.
    """
    local = to_local_epoch(ts)
    return ((local % 86400) // 3600).astype(np.int64)


def utc_iso(ts) -> str:
    """
    单个 epoch 秒 → UTC ISO 字符串（报告与台账用）。
    Single epoch second → UTC ISO string (used in reports and the inventory).
    """
    if ts is None:
        return ""
    return _dt.datetime.fromtimestamp(int(ts), tz=_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def local_iso(ts) -> str:
    """
    单个 epoch 秒 → Europe/London 当地时间 ISO 字符串。
    Single epoch second → Europe/London local ISO string.
    """
    if ts is None:
        return ""
    off = int(local_offset_seconds([int(ts)])[0])
    stamp = _dt.datetime.fromtimestamp(int(ts) + off, tz=_dt.timezone.utc)
    return stamp.strftime("%Y-%m-%d %H:%M:%S") + (" BST" if off else " GMT")


def iso_week_key(ts) -> str:
    """
    单个 epoch 秒 → ISO 周键 'YYYY-Www'（当地时间），供 E1 按周文件数直方图使用。
    Single epoch second → ISO week key 'YYYY-Www' in local time (E1 weekly histogram).
    """
    off = int(local_offset_seconds([int(ts)])[0])
    d = _dt.datetime.fromtimestamp(int(ts) + off, tz=_dt.timezone.utc).date()
    y, w, _ = d.isocalendar()
    return "%04d-W%02d" % (y, w)
