"""
文件清单与文件名解析（E1 清单层的基础）。
File listing and filename parsing (foundation of the E1 inventory layer).

文件名只作元数据参考：行内 Time 字段是唯一权威时间（交接文档 §3 DF-8）。
Filenames are metadata only; the in-row Time column is the sole authoritative
time source (handover §3 DF-8).

已知命名模式 / known naming patterns:
  A: YYYY_MM_DD_HH-MM-SS_data.csv
  B: YYYY_MM_DD_HH-MM-SS.csv
  C: YYYY_MM_DD_HHMMSS_data.csv   （样例文件实测存在，属新事实候选）
  D: YYYY_MM_DD_HHMMSS.csv        （C 的无后缀变体，一并兼容）
解析器对分隔符与 _data 后缀均宽容；无法解析的文件名照常处理数据，
只在报告中计入 "unparsed_name" 一类，绝不因文件名跳过文件。
The parser is permissive about separators and the _data suffix. Files whose name
cannot be parsed are still processed; they are merely counted under "unparsed_name".
Data is never skipped because of a filename.
"""

import datetime as _dt
import os
import re

# 宽容匹配：日期 + 时间（时分秒之间的分隔符可为 '-'、'_' 或无）+ 可选 _data 后缀
# Permissive: date + time (H/M/S separated by '-', '_' or nothing) + optional _data suffix
_NAME_RE = re.compile(
    r"^(?P<y>\d{4})_(?P<mo>\d{2})_(?P<d>\d{2})_"
    r"(?P<h>\d{2})(?P<sep1>[-_]?)(?P<mi>\d{2})(?P<sep2>[-_]?)(?P<s>\d{2})"
    r"(?P<suffix>_data)?\.csv$",
    re.IGNORECASE,
)


def parse_filename(name: str) -> dict:
    """
    解析文件名 → {pattern, name_ts_utc, parsed}。
    Parse a filename → {pattern, name_ts_utc, parsed}.

    pattern 取值 / pattern values:
      'dashed_data' | 'dashed_plain' | 'compact_data' | 'compact_plain' | 'unparsed'
    name_ts_utc: 按 UTC 解释的文件名时间戳（仅供排序与抽样，非权威时间）。
    name_ts_utc is the filename timestamp read as UTC (used for ordering and sampling only).
    """
    m = _NAME_RE.match(name)
    if not m:
        return {"pattern": "unparsed", "name_ts_utc": None, "parsed": False}
    dashed = bool(m.group("sep1")) or bool(m.group("sep2"))
    has_suffix = bool(m.group("suffix"))
    pattern = ("dashed" if dashed else "compact") + ("_data" if has_suffix else "_plain")
    try:
        stamp = _dt.datetime(
            int(m.group("y")), int(m.group("mo")), int(m.group("d")),
            int(m.group("h")), int(m.group("mi")), int(m.group("s")),
            tzinfo=_dt.timezone.utc,
        )
        ts = int(stamp.timestamp())
    except ValueError:
        # 日期字段本身非法（如 13 月）——仍算解析失败，但文件照常处理。
        # Invalid date fields (e.g. month 13): treated as unparsed, file still processed.
        return {"pattern": "unparsed", "name_ts_utc": None, "parsed": False}
    return {"pattern": pattern, "name_ts_utc": ts, "parsed": True}


def list_csv_files(data_dir: str, recursive: bool = True) -> list:
    """
    列出数据目录下的全部 .csv 文件（默认递归），按相对路径排序返回绝对路径。
    List every .csv file under the data directory (recursive by default), returned as
    absolute paths sorted by relative path.

    排序用相对路径而非文件名时间：文件名不可信是既定前提，排序只需稳定可复现。
    Sorting uses the relative path rather than the filename timestamp: filenames are
    untrusted by design, and the ordering only needs to be stable and reproducible.
    """
    data_dir = os.path.abspath(os.path.expanduser(data_dir))
    found = []
    if recursive:
        for root, _dirs, files in os.walk(data_dir):
            for fn in files:
                if fn.lower().endswith(".csv"):
                    found.append(os.path.join(root, fn))
    else:
        for fn in os.listdir(data_dir):
            full = os.path.join(data_dir, fn)
            if fn.lower().endswith(".csv") and os.path.isfile(full):
                found.append(full)
    found.sort(key=lambda p: os.path.relpath(p, data_dir))
    return found


def select_skew_sample(paths: list, data_dir: str) -> set:
    """
    选取跨设备时钟相位的抽样文件：每个文件名日期取第一个文件（E4 要求"每日一个文件"）。
    文件名不可解析者按其相对路径的前缀日期无法判断，退化为不抽样。
    Pick the files used for the cross-device clock-phase sample: the first file of each
    filename date (E4 asks for roughly one file per day). Files with unparsable names are
    not sampled.
    """
    chosen = {}
    for p in paths:
        info = parse_filename(os.path.basename(p))
        if not info["parsed"]:
            continue
        day = _dt.datetime.fromtimestamp(info["name_ts_utc"], tz=_dt.timezone.utc).strftime("%Y-%m-%d")
        if day not in chosen:
            chosen[day] = p
    return set(chosen.values())
