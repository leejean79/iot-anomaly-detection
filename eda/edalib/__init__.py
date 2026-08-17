"""
edalib —— Erol/SYNERGIA 数据集 EDA 的内部模块包（非项目主体，离线分析工具）。
edalib -- internal package for the Erol/SYNERGIA dataset EDA (offline analysis tool,
not part of the Java streaming pipeline).

模块划分 / module layout:
  config.py     常量与分箱方案 / constants and histogram bin specs
  timeutil.py   UTC epoch → Europe/London 本地日历分桶 / local calendar bucketing
  stats.py      Welford 增量统计与稀疏直方图 / incremental stats and sparse histograms
  inventory.py  文件清单与文件名解析 / file listing and filename parsing
  scan.py       单文件扫描器（进程池 worker）/ single-file scanner (pool worker)
  merge.py      逐文件部分聚合 → 全集聚合 / per-file partials → global aggregate
  report.py     聚合结果 → CSV/图表/报告 / aggregate → CSV, charts, report
"""

__all__ = ["config", "timeutil", "stats", "inventory", "scan", "merge", "report"]

SCHEMA_VERSION = 1
