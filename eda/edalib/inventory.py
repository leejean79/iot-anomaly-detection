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


# 三类命名归属（补丁 01：发现与分类分离）/ three naming classes (patch 01)
# dashed_data : ..._HH-MM-SS_data.csv
# dashed_plain: ..._HH-MM-SS.csv
# unmatched   : 其余一切（紧凑时间名、非 .csv 扩展名、无扩展名、说明文件、压缩包……）
#               everything else (compact-time names, non-.csv extensions, readme, zip, ...)
DASHED_CLASSES = ("dashed_data", "dashed_plain")


def name_class(name: str) -> str:
    """
    文件名 → 三类之一（dashed_data / dashed_plain / unmatched）。
    Filename → one of the three classes.

    命名模式**只用于分类**，绝不用于决定是否处理文件——处理与否由内容 schema 判定
    （见 scan.sniff_schema）。这是补丁 01 的核心修正：发现与分类彻底分离。
    The naming pattern is used ONLY for classification, never to decide whether a file is
    processed; that decision is made from the content schema (scan.sniff_schema). This is
    the core of patch 01: discovery and classification are fully decoupled.
    """
    p = parse_filename(name)["pattern"]
    return p if p in DASHED_CLASSES else "unmatched"


def list_all_files(data_dir: str, recursive: bool = True) -> list:
    """
    **无条件**列举目录下全部常规文件（补丁 01）——不按扩展名、不按命名模式过滤。
    List EVERY regular file under the directory (patch 01) with NO extension or
    naming-pattern filter, so that discovery count == `find <dir> -type f | wc -l`.

    背景 / background：原 `list_csv_files` 以 `.csv` 扩展名过滤，导致非 .csv 扩展名的
    数据文件被静默跳过（用户实测 3747 个文件中 276 个未被发现）。补丁 01 要求发现阶段
    对文件一视同仁，凡文件必有归属，是否入统计交由内容判定。
    The old `list_csv_files` filtered on the `.csv` extension, silently dropping data files
    with other extensions (276 of the user's 3747 files went missing). Patch 01 requires
    discovery to be filter-free; every file gets an accounting, and content decides inclusion.

    排序用相对路径而非文件名时间：文件名不可信是既定前提，排序只需稳定可复现。
    Sorted by relative path (filenames are untrusted; ordering only needs to be reproducible).

    符号链接不跟随（followlinks=False，os.walk 默认），避免环与目录外逃逸。
    Symlinks are not followed (os.walk default), avoiding cycles and escapes outside the tree.
    """
    data_dir = os.path.abspath(os.path.expanduser(data_dir))
    found = []
    if recursive:
        for root, _dirs, files in os.walk(data_dir):
            for fn in files:
                full = os.path.join(root, fn)
                # 只收常规文件；跳过 FIFO/socket/设备节点等特殊文件（同样会被计数于发现日志）。
                # Regular files only; skip FIFOs/sockets/device nodes (still logged as skipped).
                if os.path.isfile(full) and not os.path.islink(full):
                    found.append(full)
    else:
        for fn in os.listdir(data_dir):
            full = os.path.join(data_dir, fn)
            if os.path.isfile(full) and not os.path.islink(full):
                found.append(full)
    found.sort(key=lambda p: os.path.relpath(p, data_dir))
    return found


# 向后兼容别名（旧调用点/测试可能引用）/ backward-compat alias
def list_csv_files(data_dir: str, recursive: bool = True) -> list:
    """
    已弃用 / deprecated：保留别名指向 list_all_files，避免旧调用点断裂。
    补丁 01 起，发现不再按扩展名过滤——此函数名保留仅为兼容，行为与 list_all_files 相同。
    Kept as an alias to list_all_files so older call sites keep working; since patch 01 the
    behaviour is identical (no extension filter).
    """
    return list_all_files(data_dir, recursive)


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
