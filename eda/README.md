# EDA 阶段：Erol/SYNERGIA 室内环境数据集探索性分析
# EDA Stage: Exploratory Analysis of the Erol/SYNERGIA Indoor Environment Dataset

> **定位**：本目录是**离线 Python 分析工具**，不属于 Flink/Java 流式管线主体（`com.leejean`），
> 也不参与 Maven 构建。它一次性回答 M1 的设计参数问题，产出留档供后续模块查阅。
> **Scope**: an offline Python analysis tool. It is not part of the Flink/Java streaming
> pipeline and not wired into the Maven build. It answers M1's design-parameter questions
> once and leaves its outputs on record for later modules.
>
> 任务来源 / task source：交接文档《EDA 阶段 —— Erol/SYNERGIA 数据集探索性分析》（依据
> `dev_design_log` v0.2，决策 DEV-D5）。

---

## 1. 快速开始 / Quick start

```bash
# 依赖 / dependencies（Python 3.9+）
pip3 install -r eda/requirements.txt

# ① 冒烟测试：先跑 50 个文件确认环境无误 / smoke test on 50 files first
python3 eda/run_eda.py --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
                       --output-dir eda_output --limit 50

# ② 全量扫描（可中断，重跑自动续跑）/ full scan (interruptible, resumes automatically)
python3 eda/run_eda.py --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
                       --output-dir eda_output --workers 6

# ③ 出表、出图、出报告 / render tables, figures and the report
python3 eda/report_gen.py --output-dir eda_output

# 报告 / report: eda_output/eda_report.md
```

中断了怎么办：**直接重跑同一条命令**。逐文件结果缓存在 `eda_output/_cache/`，
已处理文件按 (大小, mtime) 校验后跳过。
Interrupted? Re-run the same command: per-file results are cached and validated by
(size, mtime), so finished files are skipped.

---

## 2. 两个入口脚本 / The two entry points

| 脚本 | 职责 | 输入 | 输出 |
|---|---|---|---|
| `run_eda.py` | 单遍流式扫描 + 增量聚合（重活，可并行、可续跑） | CSV 数据目录 | `file_inventory.csv`, `aggregate.json`, `_cache/` |
| `report_gen.py` | 由聚合结果出表、出图、写报告（轻活，可反复重跑） | `aggregate.json` | 各层 CSV/PNG + `eda_report.md` |

两者拆开的理由：扫描 22 GB 是分钟级到小时级的一次性成本，而报告口径（分位数呈现方式、
图表样式、结论措辞）需要反复调整。改报告不必重扫数据。
They are split because scanning 22 GB is a one-off cost of minutes to hours, while the
reporting choices need iterating. Re-rendering never re-reads the dataset.

**每个脚本的「五要素」（执行环境 / 调用命令 / 前置条件 / 期望产出 / 失败兜底）写在各自
文件头的模块 docstring 中**，`python3 eda/run_eda.py --help` 也可查看参数。
Each script's five delivery elements are in its module docstring.

### 常用参数 / common options (`run_eda.py`)

| 参数 | 默认 | 说明 |
|---|---|---|
| `--data-dir` | 交接文档给定路径 | CSV 目录，递归扫描 |
| `--output-dir` | `eda_output` | 产出目录 |
| `--limit N` | 0（全量） | 只处理前 N 个文件（冒烟测试） |
| `--workers N` | `cpu_count()-2` | 并行进程数 |
| `--no-resume` | 关 | 忽略缓存全部重算 |
| `--no-skew-sample` | 关 | 跳过跨设备时钟相位抽样 |
| `--max-file-mb` | 256 | 单文件大小上限，超过则跳过并计数 |

---

## 3. 分析内容与产出对照 / Analysis layers and outputs

| 层 | 内容 | 产出 |
|---|---|---|
| **E1 清单** | 发现文件总数/命名三类归属/字节数、逐文件台账、覆盖时间线、按周文件数、**未匹配清单**（补丁 01） | `file_inventory.csv`, `unmatched_files.csv`, `files_per_week.csv/.png` |
| **E2 完整性** | 设备×传感器行数矩阵、uptime 在场矩阵（**DEV-Q2**）、缺口分析、采样轮齐全率、重复时间戳 | `device_sensor_counts.csv`, `uptime_matrix.csv`, `uptime_timeline.png`, `round_completeness.csv`, `gaps_top100.csv`, `gap_summary.csv`, `gap_duration_hist.png` |
| **E3 数值** | 逐设备×逐通道统计（Welford + 直方图分位数）、非法值、**DEV-Q1 Accelerometer 证据**、MIC 分布、RSSI 电平、单位合理性 | `channel_stats.csv`, `channel_stats_monthly.csv`, `accel_nonzero.csv`, `unit_sanity.csv`, `mic_distribution.csv`, `degenerate_channels.csv` |
| **E4 节奏** | 到达间隔分布与分位数（**M1 watermark 依据**）、跨设备时钟相位 | `interarrival_quantiles.csv`, `interarrival_distribution.png`, `clock_skew_sample.csv` |
| **E5 漂移预览** | 逐月分位数漂移、相邻月 KS（近似）、季节趋势 | `monthly_quantile_drift_<channel>.png`, `ks_adjacent_months.csv`, `seasonal_trend.png`, `daily_channel_means.csv` |
| **汇总** | 五层结果 + 图表引用 + **M1 设计参数建议（交接文档 §6 八问）** + 与 §3 数据事实的对照 | `eda_report.md` |

---

## 3b. 补丁 01：文件发现缺口修复 / Patch 01: file-discovery gap fix

首轮实现以 `.csv` 扩展名过滤文件发现，导致非 `.csv` 扩展名的数据文件被静默跳过
（用户实测 3747 个文件中 276 个未被发现）。补丁 01 的修正：

1. **发现与分类分离**：`inventory.list_all_files` **无条件递归列举全部常规文件**
   （不按扩展名/命名过滤，不跟随符号链接），发现数 == `find <dir> -type f | wc -l`；
   命名模式只用于**分类**为三类 `dashed_data` / `dashed_plain` / `unmatched`。
2. **内容判定入统计**：`scan.sniff_schema` 只读文件前缀（默认 64 KB）判定是否为四列
   `Time,DeviceId,Sensor,Value` 数据文件——凡内容符合 schema 者（无论扩展名、无论命名）
   照常进入五层聚合；非数据文件（说明文件、压缩包、二进制、空文件）计数但不聚合。
3. **零静默跳过**：每个被发现的文件在 `file_inventory.csv` 有且仅有一行；
   `unmatched_files.csv` 单列所有 `unmatched` 类文件（文件名、字节、首行摘要、是否可解析、
   是否入统计）；报告 §1 打印归属恒等式 `发现 = 成功 + 失败数据文件 + 非数据文件`。
4. **报告 §7**：补扫结论——未匹配命名形态清单、覆盖期是否延伸过 2022-07-25 / 是否含
   2022-08/09（DF-10 解冻依据）、与本地清点（3747/2.46 GB）对账。
   可选 `--baseline <首轮 aggregate.json>` 生成关键指标差异表（§7.3）。

缓存 schema 已升级（SCHEMA=2），旧缓存自动失效并触发一次全量重扫（实测约 42 s）。

```bash
# 补扫（沿用断点续跑，只补算新发现的文件）/ rescan (resume picks up newly discovered files)
python3 eda/run_eda.py --data-dir <CSV_DIR> --output-dir eda_output --workers 6
# 出报告，并与首轮聚合做差异对照 / render report with a baseline diff
python3 eda/report_gen.py --output-dir eda_output --baseline <首轮 aggregate.json>
```

## 4. 实现要点 / Implementation notes

1. **内存**：逐文件流式处理 + 增量聚合器，**禁止全量载入**。实测峰值：主进程 ≈70 MB，
   单 worker ≈85 MB → 6 workers 的上界估计 ≈0.6 GB，远低于 2 GB 约束。
2. **断点续跑**：每个文件的部分聚合以 gzip JSON 落盘（`_cache/`），按 (大小, mtime, schema
   版本) 校验命中；缓存损坏自动重扫。缓存体积实测约为数据集的 1–4%（22 GB ≈ 0.3–0.9 GB），
   分析结束后可整目录删除。
3. **可结合的聚合**：Welford（Chan 并行合并）、稀疏直方图、计数字典——合并顺序不影响结果，
   因此进程池可乱序归并。浮点求和顺序会带来 1e-15 量级的舍入差异，属浮点非结合性，
   不影响任何结论。
4. **跨文件边界**：到达间隔与缺口用逐文件 `[first_ts, last_ts]` 缝合；采样轮齐全率为
   文件内闭合统计（边界被切开的轮按近似处理，报告中注明）。
5. **近似值**：中位数/IQR/P10/P50/P90/KS 由固定分辨率直方图导出，精度 = 通道分箱宽度
   （`edalib/config.py: BIN_SPECS`，报告开头列出）；min/max/mean/std 为精确值。
6. **时区**：`Time` 为 UTC epoch 秒；一切**日历分桶**（日/月/uptime/季节趋势）按
   Europe/London 当地时间换算（BST 规则自算，不依赖 tzdata）。
7. **哨兵值**：`-999` 等缺失占位值从分布统计与直方图中排除、单独计数——这是分析视图，
   **不是数据清洗**，原始数据一字未改。
8. **图内文字用英文**：macOS 默认 matplotlib 字体无中文字形，中文标签会渲染成方块；
   报告正文保持中英双语。

### 性能实测 / measured performance

在 4 核容器上以真实样例文件（0.68 MB / 21,688 行）复制 24 份测试：**≈27 文件/秒**
（4 workers）。按 22,000 个同规模文件外推约 **14 分钟**，符合 ≤60 分钟的硬约束。
用户本机的实际时长与内存峰值会打印在运行摘要中，并写入 `aggregate.json` 的 `run` 字段、
报告开头的表格。

---

## 5. 自测 / Self-test

真实数据集在用户本地，开发环境没有。因此附带一个按 §3 数据事实合成的等价数据集生成器，
并**刻意注入全部边界情形**（空文件、截断文件、缺表头、畸形行、哨兵值、越界值、重复行、
设备中途下线、Accelerometer 非零事件、长缺口、两种文件名模式、子目录）。

```bash
bash eda/tests/run_smoke_test.sh            # 端到端自测，逐项 PASS/FAIL
python3 eda/tests/make_test_data.py --out-dir /tmp/synth --days 70   # 只生成数据
```

自测覆盖验收标准：`--limit` 与全量运行无崩溃、畸形行/异常文件有计数、断点续跑实测有效
（第二次运行全部命中缓存且结果一致）、缓存损坏自愈、五层产出齐全、§6 八问齐答。

`eda/tests/fixtures/2022_02_11_134552_data.csv` 是**真实样例文件的前 1600 行**，
用于对真实 schema 做端到端验证（自测第 6 步）。

---

## 6. 边界与禁区（交接文档 §8）/ Boundaries

- 只做分析，**不产出任何清洗版数据集**——清洗策略是 M1 的设计决策，不在 EDA 越权预做
  （参考项目对 REFIT 清洗版填充语义制造伪事件的教训）；
- **不做任何异常检测/漂移检测算法试跑**——那属于后续模块的验证挂钩；
- 与交接文档 §3「数据事实」冲突的实测结果，一律**如实报告并标注为新数据事实候选**
  （报告 §7 自动生成对照表），交回设计会话登记，EDA 不自行改写前提。

---

## 7. 目录结构 / Layout

```
eda/
  run_eda.py              入口 1：扫描 + 聚合 / entry 1: scan and aggregate
  report_gen.py           入口 2：出表出图出报告 / entry 2: tables, figures, report
  requirements.txt        依赖 / dependencies
  README.md               本文件 / this file
  edalib/
    config.py             常量、分箱方案、合理性区间（数据形态假设集中地）
    timeutil.py           UTC → Europe/London 日历分桶（BST 规则自算）
    stats.py              Welford、稀疏直方图、计数字典（可结合、JSON 可序列化）
    inventory.py          文件清单与文件名解析（两类模式 + 紧凑变体）
    scan.py               单文件扫描器（进程池 worker，异常一律计数不抛出）
    merge.py              逐文件部分聚合 → 全集聚合 + 跨文件边界缝合
    report.py             CSV / 图表 / eda_report.md 生成
  tests/
    make_test_data.py     合成数据生成器（注入全部边界情形）
    run_smoke_test.sh     端到端自测
    fixtures/             真实样例文件前 1600 行
```
