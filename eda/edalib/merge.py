"""
归并器：逐文件部分聚合 → 全集聚合。
Aggregator: per-file partial aggregates → whole-dataset aggregate.

三件事 / three jobs:
1. 可结合地累加所有计数、Welford 累加器与稀疏直方图；
   associatively accumulate every counter, Welford accumulator and sparse histogram;
2. 缝合跨文件边界——逐设备把各文件的 [first_ts, last_ts] 段按时间排序，
   段间空隙补进到达间隔直方图与缺口清单（文件内部分已由 scan 算好）；
   stitch file boundaries: per device, sort each file's [first_ts, last_ts] segment by
   time and fold the inter-segment deltas into the inter-arrival histogram and gap list;
3. 生成逐文件台账（file_inventory.csv 的行），该表同时是断点续跑的处理记录。
   emit the per-file inventory rows, which double as the processing ledger.

内存 / memory: 一次只持有一个 payload，其余为固定规模的聚合体；
逐设备文件段列表规模 = 文件数 × 设备数（万级，MB 量级）。
Only one payload is held at a time; the rest is bounded aggregate state. The per-device
segment list is (files x devices) entries — tens of thousands, a few MB.
"""

import heapq

from . import config, stats as st, timeutil

SEP = "|"

# 缺口时长直方图的分箱边界（秒）/ gap-duration histogram edges in seconds
GAP_HIST_EDGES = [60, 120, 300, 600, 1800, 3600, 7200, 21600, 86400, 604800]


class Aggregator:
    """
    全集聚合器 / whole-dataset aggregator.
    """

    def __init__(self):
        self.files = {
            "total": 0, "ok": 0, "failed": 0, "non_data": 0,
            "bytes_total": 0, "bytes_data": 0, "bytes_non_data": 0,
            "lines_raw": 0, "rows_parsed": 0,
            "rows_malformed": 0, "rows_bad_time": 0, "rows_bad_key": 0, "rows_bad_value": 0,
            "with_header": 0, "without_header": 0, "accel_truncated": 0,
            "skew_sampled": 0,
        }
        self.patterns = {}          # 细粒度命名模式 / fine-grained name pattern histogram
        self.name_classes = {}      # 三类归属计数（补丁 01）/ three-class histogram
        self.unmatched = []         # name_class=="unmatched" 的逐文件记录 / unmatched records
        self.errors = {}
        self.weekly_files = {}
        self.time_min = None
        self.time_max = None

        self.stats = {}
        self.hist = {}
        self.nan = {}
        self.sentinel = {}
        self.oor = {}
        self.uptime = {}
        self.daily = {}

        self.interarrival = {}      # dev -> {bin: count}
        self.round_size = {}        # dev -> {sensors_per_round: count}
        self.rounds_total = {}      # dev -> rounds (文件内计数之和 / sum of per-file counts)
        self.segments = {}          # dev -> [(first_ts, last_ts, file_rel_path)]
        self.dup_extra_rows = 0
        self.dup_keys = 0

        self.accel_zero = {}
        self.accel_nonzero = {}
        self.accel_records = []
        self.accel_records_truncated = False

        self.skew_hist = {}         # dev -> {delta: count}
        self.skew_outside = {}      # dev -> count
        self.skew_refs = {}         # ref dev -> file count

        self._gap_heap = []         # 最长缺口的小顶堆 / min-heap of longest gaps
        self.gap_summary = {}       # dev -> {count,total_s,max_s,hist}

        self.inventory_rows = []

    # ------------------------------------------------------------------
    # 逐文件累加 / per-file accumulation
    # ------------------------------------------------------------------
    def add(self, payload: dict) -> None:
        self.files["total"] += 1
        nbytes = int(payload.get("bytes", 0))
        self.files["bytes_total"] += nbytes
        pattern = payload.get("name_pattern", "unparsed")
        self.patterns[pattern] = self.patterns.get(pattern, 0) + 1
        cls = payload.get("name_class", "unmatched")
        self.name_classes[cls] = self.name_classes.get(cls, 0) + 1

        # 补丁 01：name_class=="unmatched" 的文件逐一登记，绝不静默消失。
        # Patch 01: every unmatched-class file is recorded so none disappears silently.
        if cls == "unmatched":
            self._add_unmatched_row(payload)

        is_data = bool(payload.get("is_data_file", False))
        if not payload.get("ok", False):
            err = payload.get("error") or "unknown error"
            key = err.split(":")[0].strip()
            self.errors[key] = self.errors.get(key, 0) + 1
            if is_data:
                # 数据文件但未能使用（超大 / 无有效行 / 读失败）/ a data file we could not use
                self.files["failed"] += 1
                self.files["bytes_data"] += nbytes
            else:
                # 非数据文件（说明文件、压缩包、二进制、空文件……）/ non-data file
                self.files["non_data"] += 1
                self.files["bytes_non_data"] += nbytes
            self._add_inventory_row(payload)
            return

        self.files["ok"] += 1
        self.files["bytes_data"] += nbytes
        self.files["lines_raw"] += int(payload.get("lines_raw", 0))
        for k in ("rows_parsed", "rows_malformed", "rows_bad_time", "rows_bad_key", "rows_bad_value"):
            self.files[k] += int(payload.get(k, 0))
        self.files["with_header" if payload.get("has_header") else "without_header"] += 1
        if payload.get("accel_truncated"):
            self.files["accel_truncated"] += 1

        t_min, t_max = payload.get("time_min"), payload.get("time_max")
        if t_min is not None:
            self.time_min = t_min if self.time_min is None else min(self.time_min, t_min)
            self.time_max = t_max if self.time_max is None else max(self.time_max, t_max)
            week = timeutil.iso_week_key(t_min)
            self.weekly_files[week] = self.weekly_files.get(week, 0) + 1

        # 数值层 / numeric layer
        for key, acc in payload.get("stats", {}).items():
            self.stats[key] = st.welford_merge(self.stats.get(key), acc)
        for key, h in payload.get("hist", {}).items():
            st.hist_merge_inplace(self.hist.setdefault(key, {}), h)
        for name in ("nan", "sentinel", "oor"):
            target = getattr(self, name)
            for key, n in payload.get(name, {}).items():
                target[key] = target.get(key, 0) + int(n)

        st.counter_merge_str_inplace(self.uptime, payload.get("uptime", {}))
        for key, (n, s) in payload.get("daily", {}).items():
            cur = self.daily.get(key)
            if cur is None:
                self.daily[key] = [int(n), float(s)]
            else:
                cur[0] += int(n)
                cur[1] += float(s)

        # 节奏层 / rhythm layer
        rel = payload.get("path", payload.get("name", "?"))
        for dev, entry in payload.get("rounds", {}).items():
            st.counter_merge_inplace(self.interarrival.setdefault(dev, {}), entry.get("ia", {}))
            self.rounds_total[dev] = self.rounds_total.get(dev, 0) + int(entry.get("n", 0))
            self.segments.setdefault(dev, []).append(
                (int(entry["first"]), int(entry["last"]), rel)
            )
            for g_start, g_end, g_dur in entry.get("gaps", []):
                self._record_gap(dev, int(g_start), int(g_end), int(g_dur), "intra-file", rel)

        for dev, sizes in payload.get("round_size", {}).items():
            st.counter_merge_inplace(self.round_size.setdefault(dev, {}), sizes)

        self.dup_extra_rows += int(payload.get("dup_extra_rows", 0))
        self.dup_keys += int(payload.get("dup_keys", 0))

        # Accelerometer（DEV-Q1 证据）/ evidence for DEV-Q1
        st.counter_merge_str_inplace(self.accel_zero, payload.get("accel_zero", {}))
        st.counter_merge_str_inplace(self.accel_nonzero, payload.get("accel_nonzero", {}))
        for rec in payload.get("accel_records", []):
            if len(self.accel_records) < config.ACCEL_NONZERO_CAP_TOTAL:
                self.accel_records.append([int(rec[0]), str(rec[1]), float(rec[2]), rel])
            else:
                self.accel_records_truncated = True
                break

        # 时钟相位抽样 / clock-phase sample
        skew = payload.get("skew")
        if skew:
            self.files["skew_sampled"] += 1
            ref = skew.get("ref")
            if ref:
                self.skew_refs[ref] = self.skew_refs.get(ref, 0) + 1
            for dev, h in skew.get("hist", {}).items():
                st.counter_merge_inplace(self.skew_hist.setdefault(dev, {}), h)
            for dev, n in skew.get("outside", {}).items():
                self.skew_outside[dev] = self.skew_outside.get(dev, 0) + int(n)

        self._add_inventory_row(payload)

    # ------------------------------------------------------------------
    def _add_unmatched_row(self, payload: dict) -> None:
        """
        登记一个 name_class=="unmatched" 的文件（补丁 01 交付物 unmatched_files.csv）。
        Record one unmatched-class file (patch 01 deliverable unmatched_files.csv).
        """
        self.unmatched.append(
            {
                "rel_path": payload.get("path", ""),
                "file_name": payload.get("name", ""),
                "name_pattern": payload.get("name_pattern", ""),
                "bytes": int(payload.get("bytes", 0)),
                "schema_parsable": int(bool(payload.get("schema_parsable"))),
                "is_data_file": int(bool(payload.get("is_data_file"))),
                "included_in_stats": int(bool(payload.get("ok")) and bool(payload.get("is_data_file"))),
                "rows_parsed": int(payload.get("rows_parsed", 0)),
                "reason": payload.get("error") or "",
                "first_line_summary": payload.get("first_line_summary", ""),
            }
        )

    # ------------------------------------------------------------------
    def _add_inventory_row(self, payload: dict) -> None:
        t_min, t_max = payload.get("time_min"), payload.get("time_max")
        span = (int(t_max) - int(t_min)) if (t_min is not None and t_max is not None) else ""
        self.inventory_rows.append(
            {
                "rel_path": payload.get("path", ""),
                "file_name": payload.get("name", ""),
                "name_pattern": payload.get("name_pattern", ""),
                "name_class": payload.get("name_class", ""),
                "bytes": payload.get("bytes", 0),
                "ok": int(bool(payload.get("ok"))),
                "is_data_file": int(bool(payload.get("is_data_file"))),
                "schema_parsable": int(bool(payload.get("schema_parsable"))),
                "error": payload.get("error") or "",
                "has_header": int(bool(payload.get("has_header"))),
                "lines_raw": payload.get("lines_raw", 0),
                "rows_parsed": payload.get("rows_parsed", 0),
                "rows_malformed": payload.get("rows_malformed", 0),
                "rows_bad_time": payload.get("rows_bad_time", 0),
                "rows_bad_key": payload.get("rows_bad_key", 0),
                "rows_bad_value": payload.get("rows_bad_value", 0),
                "time_min_epoch": t_min if t_min is not None else "",
                "time_max_epoch": t_max if t_max is not None else "",
                "time_min_utc": timeutil.utc_iso(t_min) if t_min is not None else "",
                "time_max_utc": timeutil.utc_iso(t_max) if t_max is not None else "",
                "span_s": span,
                "n_rounds": payload.get("n_rounds", 0),
                "n_devices": len(payload.get("rounds", {})),
                "skew_sampled": int(payload.get("skew") is not None),
            }
        )

    # ------------------------------------------------------------------
    def _record_gap(self, dev, start, end, dur, kind, rel) -> None:
        """
        记录一个缺口段：全量进统计摘要，最长的 GAPS_TOP_N 条进清单。
        Record one gap segment: all of them feed the summary, the longest GAPS_TOP_N
        are kept in the listing.
        """
        s = self.gap_summary.setdefault(dev, {"count": 0, "total_s": 0, "max_s": 0, "hist": {}})
        s["count"] += 1
        s["total_s"] += dur
        s["max_s"] = max(s["max_s"], dur)
        bucket = len(GAP_HIST_EDGES)
        for i, edge in enumerate(GAP_HIST_EDGES):
            if dur < edge:
                bucket = i
                break
        s["hist"][bucket] = s["hist"].get(bucket, 0) + 1

        # 排序键完全由内容决定（不含插入序号），确保并行乱序归并的结果可复现。
        # The heap key is content-only (no insertion counter) so that out-of-order parallel
        # merging still yields a reproducible top-N.
        item = (dur, dev, start, end, kind, rel)
        if len(self._gap_heap) < config.GAPS_TOP_N:
            heapq.heappush(self._gap_heap, item)
        elif dur > self._gap_heap[0][0]:
            heapq.heapreplace(self._gap_heap, item)

    # ------------------------------------------------------------------
    # 收尾：缝合跨文件边界 / finalise: stitch file boundaries
    # ------------------------------------------------------------------
    def finalize(self) -> dict:
        boundary = {"deltas_folded": 0, "overlaps": 0, "devices": {}}
        for dev, segs in self.segments.items():
            segs.sort(key=lambda x: (x[0], x[1]))
            dev_overlaps = 0
            prev_last = None
            for first, last, _rel in segs:
                if prev_last is not None:
                    delta = first - prev_last
                    if delta < 0:
                        # 文件时间范围重叠：不折入间隔直方图，另计数供报告说明。
                        # Overlapping file time ranges: counted, not folded into the histogram.
                        dev_overlaps += 1
                    else:
                        binned = min(delta, config.INTERARRIVAL_OVERFLOW_BIN)
                        ia = self.interarrival.setdefault(dev, {})
                        ia[binned] = ia.get(binned, 0) + 1
                        boundary["deltas_folded"] += 1
                        if delta > config.GAP_THRESHOLD_S:
                            self._record_gap(dev, prev_last, first, delta, "inter-file", "")
                prev_last = last if prev_last is None else max(prev_last, last)
            boundary["overlaps"] += dev_overlaps
            boundary["devices"][dev] = {
                "n_segments": len(segs),
                "overlaps": dev_overlaps,
                "first_ts": segs[0][0] if segs else None,
                "last_ts": max(s[1] for s in segs) if segs else None,
            }

        gaps_top = sorted(self._gap_heap, key=lambda x: (-x[0], x[1], x[2]))
        return {
            "schema": 1,
            "files": self.files,
            "patterns": self.patterns,
            "name_classes": self.name_classes,
            # 按相对路径排序，使进程池乱序归并下产物确定（与 gaps/accel 同处理）。
            # Sorted by relative path so the output is deterministic under out-of-order
            # pool merging (same treatment as gaps/accel records).
            "unmatched": sorted(self.unmatched, key=lambda r: r["rel_path"]),
            "errors": self.errors,
            "weekly_files": self.weekly_files,
            "time_min": self.time_min,
            "time_max": self.time_max,
            "stats": self.stats,
            "hist": {k: {str(b): c for b, c in v.items()} for k, v in self.hist.items()},
            "nan": self.nan,
            "sentinel": self.sentinel,
            "oor": self.oor,
            "uptime": self.uptime,
            "daily": self.daily,
            "interarrival": {d: {str(b): c for b, c in v.items()} for d, v in self.interarrival.items()},
            "round_size": {d: {str(b): c for b, c in v.items()} for d, v in self.round_size.items()},
            "rounds_total": self.rounds_total,
            "dup_extra_rows": self.dup_extra_rows,
            "dup_keys": self.dup_keys,
            "accel_zero": self.accel_zero,
            "accel_nonzero": self.accel_nonzero,
            # 记录按时间排序：进程池乱序返回时结果仍确定（截断发生时保留的子集
            # 仍依赖到达顺序，已在报告中标注截断）。
            # Sorted by time so the output is deterministic under out-of-order pool results
            # (which subset survives truncation still depends on arrival order; truncation
            # is flagged in the report).
            "accel_records": sorted(self.accel_records, key=lambda r: (r[0], r[1], r[3])),
            "accel_records_truncated": self.accel_records_truncated,
            "skew": {
                "hist": {d: {str(b): c for b, c in v.items()} for d, v in self.skew_hist.items()},
                "outside": self.skew_outside,
                "refs": self.skew_refs,
                "files": self.files["skew_sampled"],
            },
            "gaps_top": [
                {"device": g[1], "start_epoch": g[2], "end_epoch": g[3], "duration_s": g[0],
                 "kind": g[4], "file": g[5]}
                for g in gaps_top
            ],
            "gap_summary": {
                d: {"count": v["count"], "total_s": v["total_s"], "max_s": v["max_s"],
                    "hist": {str(k): c for k, c in v["hist"].items()}}
                for d, v in self.gap_summary.items()
            },
            "gap_hist_edges": GAP_HIST_EDGES,
            "boundary": boundary,
        }
