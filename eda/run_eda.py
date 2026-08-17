#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_eda.py —— Erol/SYNERGIA 数据集单遍流式扫描与聚合（EDA 阶段入口 1/2）。
run_eda.py -- single-pass streaming scan and aggregation of the Erol/SYNERGIA dataset
(EDA stage entry point 1 of 2).

================================ 脚本交付五要素 ================================
================== Five-element script delivery (per CLAUDE.md) ================

【1. 执行环境 / execution environment】
  - macOS / Linux，Python 3.9+
  - 依赖：pandas ≥ 1.3、numpy ≥ 1.20（本脚本不需要 matplotlib；作图在 report_gen.py）
  - 无需 root，无网络访问；只读数据目录，只写 --output-dir

【2. 调用命令 / invocation】
  # 冒烟测试（先跑这个）/ smoke test first
  python3 eda/run_eda.py --data-dir /path/to/files_csv --limit 50

  # 全量扫描 / full scan
  python3 eda/run_eda.py --data-dir /path/to/files_csv --workers 6

  # 中断后续跑（默认开启，重复执行同一命令即可）/ resume (on by default: just re-run)
  python3 eda/run_eda.py --data-dir /path/to/files_csv --workers 6

  # 忽略缓存重算 / ignore cache and recompute
  python3 eda/run_eda.py --data-dir /path/to/files_csv --no-resume

【3. 前置条件 / preconditions】
  - --data-dir 指向解压后的 CSV 目录（默认递归子目录）；目录只读即可
  - 磁盘可写空间：逐文件缓存约为数据集体积的 2–4%（22 GB 数据 ≈ 0.5–1 GB 缓存）
  - 本脚本**不修改、不清洗、不落盘任何数据集内容**（交接文档 §8 禁区）

【4. 期望产出 / expected outputs】
  <output-dir>/file_inventory.csv   逐文件台账（E1；兼作处理记录）
  <output-dir>/aggregate.json       全集聚合结果（report_gen.py 的输入）
  <output-dir>/_cache/*.json.gz     逐文件部分聚合（断点续跑用，可安全删除）
  stdout                            进度、运行时长、峰值内存、异常文件计数

【5. 失败兜底 / failure fallback】
  - 单文件任何异常（空文件、截断、列数不符、编码错误、数值不可解析）→ 计数并跳过，
    进入 file_inventory.csv 的 error 列与报告的"异常文件"小节，绝不中断整体运行
  - Ctrl-C 中断 → 已完成文件的缓存保留，不写出半成品 aggregate.json；
    重跑同一命令自动跳过已处理文件
  - 缓存损坏（断电写坏的 .json.gz）→ 该文件自动重扫并覆盖缓存
  - 单文件超过 --max-file-mb（默认 256）→ 跳过并计数，保护 < 2 GB 内存约束
  - 内存/时长实测值打印在结束摘要中，并写入 aggregate.json 的 run 字段
==============================================================================
"""

import argparse
import gzip
import json
import multiprocessing as mp
import os
import resource
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from edalib import config, inventory, merge, scan  # noqa: E402


# ---------------------------------------------------------------------------
# 缓存 / cache
# ---------------------------------------------------------------------------

def cache_path_for(cache_dir: str, rel_path: str) -> str:
    """
    相对路径 → 缓存文件路径（子目录分隔符编码进文件名，缓存目录保持扁平）。
    Relative path → cache file path (separators encoded into the name, flat cache dir).
    """
    safe = rel_path.replace(os.sep, "__").replace("/", "__")
    return os.path.join(cache_dir, safe + ".json.gz")


def load_cached(cache_file: str, size: int, mtime: float, need_skew: bool):
    """
    读取并校验缓存；不可用时返回 None（调用方重扫）。
    Load and validate a cache entry; return None when unusable so the caller rescans.
    """
    try:
        with gzip.open(cache_file, "rt", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception:  # noqa: BLE001 —— 缓存损坏一律重扫 / any corruption → rescan
        return None
    if payload.get("schema") != scan.SCHEMA:
        return None
    if int(payload.get("bytes", -1)) != int(size):
        return None
    if abs(float(payload.get("mtime", -1.0)) - float(mtime)) > 1e-6:
        return None
    if need_skew and payload.get("skew") is None and payload.get("ok", False):
        return None  # 该文件本轮被选为相位抽样点，缓存里没有 / newly selected for skew sampling
    return payload


def write_cache(cache_file: str, payload: dict) -> None:
    """
    原子写缓存（先写临时文件再 rename），避免中断留下半截 JSON。
    Atomic cache write (temp file + rename) so an interrupt cannot leave a half JSON.
    """
    tmp = cache_file + ".tmp"
    try:
        with gzip.open(tmp, "wt", encoding="utf-8") as fh:
            json.dump(payload, fh, separators=(",", ":"))
        os.replace(tmp, cache_file)
    except Exception:  # noqa: BLE001 —— 缓存写失败不影响本轮结果 / cache failure is non-fatal
        try:
            os.unlink(tmp)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# 进程池 worker / pool worker
# ---------------------------------------------------------------------------

_WORKER_CTX = {}


def _init_worker(ctx: dict) -> None:
    _WORKER_CTX.update(ctx)


def _process_one(task):
    """
    worker 入口：命中缓存则直接返回，否则扫描并写缓存。
    Worker entry: return the cached payload on a hit, otherwise scan and write the cache.
    返回 (payload, from_cache)。
    """
    path, sample_skew = task
    ctx = _WORKER_CTX
    rel = os.path.relpath(path, ctx["data_dir"])
    cache_file = cache_path_for(ctx["cache_dir"], rel)

    if ctx["resume"]:
        try:
            stt = os.stat(path)
            cached = load_cached(cache_file, stt.st_size, stt.st_mtime, sample_skew)
        except OSError:
            cached = None
        if cached is not None:
            return cached, True

    payload = scan.scan_file(path, ctx["data_dir"], sample_skew=sample_skew,
                             max_file_mb=ctx["max_file_mb"])
    write_cache(cache_file, payload)
    return payload, False


# ---------------------------------------------------------------------------
# 台账写出 / inventory writing
# ---------------------------------------------------------------------------

INVENTORY_FIELDS = [
    "rel_path", "file_name", "name_pattern", "bytes", "ok", "error", "has_header",
    "lines_raw", "rows_parsed", "rows_malformed", "rows_bad_time", "rows_bad_key",
    "rows_bad_value", "time_min_epoch", "time_max_epoch", "time_min_utc",
    "time_max_utc", "span_s", "n_rounds", "n_devices", "skew_sampled",
]


def write_inventory(rows, out_path: str) -> None:
    import csv

    rows = sorted(rows, key=lambda r: (r["time_min_epoch"] == "", r["time_min_epoch"], r["rel_path"]))
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=INVENTORY_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# ---------------------------------------------------------------------------
# 内存与时间 / memory and timing
# ---------------------------------------------------------------------------

def peak_rss_mb() -> float:
    """
    近似峰值常驻内存（主进程 + 子进程）。macOS 的 ru_maxrss 单位是字节，Linux 是 KB。
    Approximate peak RSS (parent + children). ru_maxrss is bytes on macOS, KB on Linux.
    """
    unit = 1.0 / (1024 * 1024) if sys.platform == "darwin" else 1.0 / 1024
    self_peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * unit
    child_peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * unit
    # 子进程峰值取的是"单个子进程的最大值"，并行时按 workers 数放大为上界估计。
    # ru_maxrss for children is the max over single children; scaled by worker count
    # this gives an upper-bound estimate of the concurrent footprint.
    return self_peak, child_peak


# ---------------------------------------------------------------------------
# 主流程 / main
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Erol/SYNERGIA 数据集 EDA 扫描与聚合 / dataset scan and aggregation",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--data-dir", default=config.DEFAULT_DATA_DIR,
                   help="CSV 数据目录 / directory holding the CSV files")
    p.add_argument("--output-dir", default=config.DEFAULT_OUTPUT_DIR,
                   help="产出目录 / output directory")
    p.add_argument("--cache-dir", default=None,
                   help="逐文件缓存目录，默认 <output-dir>/_cache / per-file cache directory")
    p.add_argument("--limit", type=int, default=0,
                   help="只处理前 N 个文件（0 = 全量），冒烟测试用 / process first N files only")
    p.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 3) - 2),
                   help="并行进程数 / worker processes")
    p.add_argument("--no-resume", dest="resume", action="store_false",
                   help="忽略缓存，全部重算 / ignore cache and recompute everything")
    p.add_argument("--no-recursive", dest="recursive", action="store_false",
                   help="不递归子目录 / do not descend into subdirectories")
    p.add_argument("--no-skew-sample", dest="skew_sample", action="store_false",
                   help="跳过跨设备时钟相位抽样 / skip the cross-device clock-phase sample")
    p.add_argument("--max-file-mb", type=float, default=256.0,
                   help="单文件大小上限，超过则跳过并计数 / per-file size cap in MB")
    p.add_argument("--progress-every", type=int, default=200,
                   help="每处理多少文件打印一次进度 / progress print interval")
    p.set_defaults(resume=True, recursive=True, skew_sample=True)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    data_dir = os.path.abspath(os.path.expanduser(args.data_dir))
    out_dir = os.path.abspath(os.path.expanduser(args.output_dir))
    cache_dir = os.path.abspath(os.path.expanduser(args.cache_dir)) if args.cache_dir \
        else os.path.join(out_dir, config.CACHE_SUBDIR)

    if not os.path.isdir(data_dir):
        print("[错误 / error] 数据目录不存在 / data directory not found: %s" % data_dir, file=sys.stderr)
        return 2
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(cache_dir, exist_ok=True)

    print("[1/4] 扫描文件清单 / listing files: %s" % data_dir)
    paths = inventory.list_csv_files(data_dir, recursive=args.recursive)
    total_found = len(paths)
    if args.limit and args.limit > 0:
        paths = paths[: args.limit]
    if not paths:
        print("[错误 / error] 目录下没有 .csv 文件 / no .csv files found", file=sys.stderr)
        return 2
    print("      发现 %d 个 CSV，本次处理 %d 个 / found %d, processing %d"
          % (total_found, len(paths), total_found, len(paths)))

    skew_files = inventory.select_skew_sample(paths, data_dir) if args.skew_sample else set()
    print("      时钟相位抽样文件 / clock-phase sample files: %d" % len(skew_files))

    tasks = [(p, p in skew_files) for p in paths]
    ctx = {
        "data_dir": data_dir,
        "cache_dir": cache_dir,
        "resume": args.resume,
        "max_file_mb": args.max_file_mb,
    }

    workers = max(1, min(args.workers, len(tasks)))
    print("[2/4] 逐文件扫描 / scanning (workers=%d, resume=%s)" % (workers, args.resume))

    agg = merge.Aggregator()
    t0 = time.time()
    n_done = n_cached = 0
    interrupted = False
    pool = None
    try:
        if workers == 1:
            _init_worker(ctx)
            results = (_process_one(t) for t in tasks)
            for payload, from_cache in results:
                agg.add(payload)
                n_done += 1
                n_cached += int(from_cache)
                _maybe_progress(n_done, n_cached, len(tasks), t0, args.progress_every)
        else:
            # spawn 在 macOS 上是默认启动方式；显式指定以保证跨平台一致。
            # spawn is the macOS default; set explicitly for cross-platform consistency.
            mp_ctx = mp.get_context("spawn")
            chunk = max(1, min(16, len(tasks) // (workers * 8) or 1))
            pool = mp_ctx.Pool(workers, initializer=_init_worker, initargs=(ctx,))
            for payload, from_cache in pool.imap_unordered(_process_one, tasks, chunksize=chunk):
                agg.add(payload)
                n_done += 1
                n_cached += int(from_cache)
                _maybe_progress(n_done, n_cached, len(tasks), t0, args.progress_every)
            pool.close()
            pool.join()
            pool = None
    except KeyboardInterrupt:
        interrupted = True
        if pool is not None:
            pool.terminate()
            pool.join()
        print("\n[中断 / interrupted] 已处理 %d/%d 个文件；缓存已保留。"
              "\n  重跑同一命令将跳过已处理文件（断点续跑）。"
              "\n  Processed %d/%d files; the cache is intact. Re-run the same command to resume."
              % (n_done, len(tasks), n_done, len(tasks)), file=sys.stderr)

    elapsed = time.time() - t0
    self_peak, child_peak = peak_rss_mb()

    if interrupted:
        print("[退出 / exit] 未写出 aggregate.json（结果不完整）/ aggregate.json not written "
              "(incomplete run)", file=sys.stderr)
        return 130

    print("[3/4] 归并与缝合跨文件边界 / merging and stitching file boundaries")
    result = agg.finalize()
    result["run"] = {
        "data_dir": data_dir,
        "files_found": total_found,
        "files_processed": len(tasks),
        "files_from_cache": n_cached,
        "limit": args.limit,
        "workers": workers,
        "resume": bool(args.resume),
        "skew_sample_files": len(skew_files),
        "elapsed_s": round(elapsed, 1),
        "peak_rss_parent_mb": round(self_peak, 1),
        "peak_rss_max_child_mb": round(child_peak, 1),
        "peak_rss_estimate_mb": round(self_peak + child_peak * workers, 1),
        "generated_at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "python": sys.version.split()[0],
        "bin_specs": {k: list(v) for k, v in config.BIN_SPECS.items()},
        "gap_threshold_s": config.GAP_THRESHOLD_S,
    }

    print("[4/4] 写出产出 / writing outputs -> %s" % out_dir)
    inv_path = os.path.join(out_dir, config.INVENTORY_FILENAME)
    write_inventory(agg.inventory_rows, inv_path)
    agg_path = os.path.join(out_dir, config.AGGREGATE_FILENAME)
    with open(agg_path, "w", encoding="utf-8") as fh:
        json.dump(result, fh, separators=(",", ":"))

    files = result["files"]
    print("\n===== 扫描摘要 / scan summary =====")
    print("  文件 / files          : %d 处理成功, %d 失败（详见 file_inventory.csv 的 error 列）"
          % (files["ok"], files["failed"]))
    print("  数据行 / data rows    : %d 解析成功, %d 畸形跳过, %d 值不可解析"
          % (files["rows_parsed"], files["rows_malformed"], files["rows_bad_value"]))
    print("  缓存命中 / cache hits : %d / %d" % (n_cached, len(tasks)))
    print("  用时 / elapsed        : %.1f s (%.2f min)" % (elapsed, elapsed / 60.0))
    print("  峰值内存 / peak RSS   : 主进程 %.0f MB, 单子进程最大 %.0f MB, 估计总计 %.0f MB"
          % (self_peak, child_peak, self_peak + child_peak * workers))
    print("  产出 / outputs        : %s, %s" % (inv_path, agg_path))
    print("\n下一步 / next: python3 eda/report_gen.py --output-dir %s" % out_dir)
    return 0


def _maybe_progress(n_done: int, n_cached: int, n_total: int, t0: float, every: int) -> None:
    if every <= 0 or (n_done % every and n_done != n_total):
        return
    elapsed = time.time() - t0
    rate = n_done / elapsed if elapsed > 0 else 0.0
    eta = (n_total - n_done) / rate if rate > 0 else 0.0
    print("      %6d/%d  缓存命中 %d  %.1f 文件/秒  已用 %.1f min  预计剩余 %.1f min"
          % (n_done, n_total, n_cached, rate, elapsed / 60.0, eta / 60.0), flush=True)


if __name__ == "__main__":
    sys.exit(main())
