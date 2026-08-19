"""
常量、分箱方案与合理性区间。
Constants, histogram bin specifications and physical sanity ranges.

本文件是 EDA 的"事实假设集中地"：所有与数据集形态相关的先验（设备集、通道集、
物理量程、分箱分辨率）集中在此，便于后续会话按实测结果订正。
This file centralises every dataset-shape assumption (device set, channel set,
physical ranges, bin resolution) so later sessions can revise them in one place.

注意 / NOTE: 依据交接文档 §3 DF-7，Pressure 的实际单位为 Pa（≈101,665），
README 声明的 hPa 有误；本文件按 Pa 处理。
Per handover §3 DF-7 the Pressure channel is in Pa (not hPa as the dataset README claims).
"""

# ---------------------------------------------------------------------------
# 数据 schema / dataset schema
# ---------------------------------------------------------------------------

CSV_COLUMNS = ["Time", "DeviceId", "Sensor", "Value"]

# 先验设备集；实测出现的其他 DeviceId 一律照收并在报告中标注为新事实候选。
# Prior device set; any other DeviceId observed is still accepted and flagged
# in the report as a new-data-fact candidate.
EXPECTED_DEVICES = ["A", "B", "C", "D", "E", "F", "G", "H"]

# 先验传感器集（每设备每采样轮 8 行，DF-1）。
# Prior sensor set (8 rows per device per sampling round, DF-1).
EXPECTED_SENSORS = [
    "Temperature",
    "Humidity",
    "Pressure",
    "Gas",
    "Accelerometer",
    "Light",
    "MIC",
    "RSSI",
]

# M3/M5 检测特征集（F_det 五通道），漂移预览层只对这五个通道做逐月分布。
# Detection feature set (F_det, five channels); the drift-preview layer only
# builds monthly distributions for these.
F_DET_CHANNELS = ["Temperature", "Humidity", "Pressure", "Gas", "Light"]

# 侧信道集：仅监测，不进入检测特征集（DEV-D4 待确认，见 E3 Accelerometer 证据）。
# Side-channel set: monitored only, not fed to the detectors (pending DEV-D4).
SIDE_CHANNELS = ["Accelerometer", "MIC", "RSSI"]

# 季节趋势图选用的通道 / channels plotted in the seasonal trend figure
SEASONAL_CHANNELS = ["Temperature", "Humidity", "Light"]

# ---------------------------------------------------------------------------
# 节奏层参数 / rhythm-layer parameters
# ---------------------------------------------------------------------------

# 采样标称 10 s（DF-2）。相邻轮间隔超过此秒数记为缺口段。
# Nominal sampling period is 10 s (DF-2); a round-to-round gap above this is a "gap segment".
GAP_THRESHOLD_S = 60

# 到达间隔直方图：1 s 分箱 0..120，最后一个桶（索引 121）为溢出桶。
# Inter-arrival histogram: 1 s bins over 0..120 s; index 121 is the overflow bucket.
INTERARRIVAL_MAX_S = 120
INTERARRIVAL_OVERFLOW_BIN = INTERARRIVAL_MAX_S + 1

# 跨设备时钟相位：相对参考设备的偏移直方图，1 s 分箱，[-SKEW_MAX, +SKEW_MAX]。
# Cross-device clock phase: offset histogram vs the reference device, 1 s bins.
SKEW_MAX_S = 30

# 到达间隔分位数（E4 → M1 watermark 参数依据）
# Inter-arrival quantiles (E4 → basis for the M1 watermark parameter)
INTERARRIVAL_QUANTILES = [0.5, 0.9, 0.99, 0.999]

# ---------------------------------------------------------------------------
# 数值层：分箱方案 / numeric layer: bin specifications
# ---------------------------------------------------------------------------
# 每个通道 (lo, hi, width)：值经 clip 后落入 [0, nbins-1]；低于 lo 落 0 号桶、
# 高于 hi 落末桶，越界另有独立计数（见 PHYSICAL_RANGES），故直方图末端不用于分位数外推。
# Per channel (lo, hi, width): values are clipped into [0, nbins-1]. Out-of-range
# values are counted separately (see PHYSICAL_RANGES), so the edge bins are not
# used for tail extrapolation.
# 近似中位数/IQR 的精度 = 分箱宽度（报告中注明）。
# Median/IQR precision equals the bin width (stated in the report).
# 分箱宽度同时决定缓存体积（稀疏直方图的非零桶数）：过细会让逐文件缓存膨胀，
# 此处取"够用即止"的分辨率，报告中按此声明精度。
# The bin width also drives cache size (number of non-zero sparse bins), so the widths
# below are the coarsest that still serve the analysis; the report states them as the
# precision of every histogram-derived quantile.
BIN_SPECS = {
    "Temperature":   (-20.0, 60.0, 0.1),       # °C
    "Humidity":      (0.0, 100.0, 0.1),        # %RH
    "Pressure":      (90000.0, 110000.0, 5.0), # Pa
    "Gas":           (0.0, 20000.0, 10.0),     # 气体传感器电阻/指数 / gas resistance-index
    "Light":         (0.0, 70000.0, 25.0),     # lux
    "MIC":           (0.0, 10.0, 1.0),         # 离散档位 / discrete levels
    "RSSI":          (-120.0, 120.0, 1.0),     # dBm 或其绝对值 / dBm or its magnitude
    "Accelerometer": (0.0, 10.0, 0.01),
}

# 未知通道的兜底分箱 / fallback bins for unknown channels
DEFAULT_BIN_SPEC = (-1e6, 1e6, 1.0)

# ---------------------------------------------------------------------------
# 单位合理性区间（E3 越界计数）/ physical sanity ranges (E3 out-of-range counting)
# ---------------------------------------------------------------------------
# None 表示该通道无先验合理区间，不做越界判定。
# None means no prior range is asserted for that channel.
PHYSICAL_RANGES = {
    "Temperature": (0.0, 45.0),
    "Humidity": (0.0, 100.0),
    "Pressure": (95000.0, 105000.0),   # Pa，按 DF-7 / per DF-7
    "Gas": None,
    "Light": None,
    "MIC": None,
    "RSSI": None,
    "Accelerometer": None,
}

# 明显哨兵值（缺失占位）/ obvious sentinel (missing-data placeholder) values
SENTINEL_VALUES = [-999.0, -9999.0, -99.0, 999999.0]

# ---------------------------------------------------------------------------
# 输出与缓存 / outputs and cache
# ---------------------------------------------------------------------------

DEFAULT_DATA_DIR = "/Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv"
DEFAULT_OUTPUT_DIR = "eda_output"

# 补丁 01：内容 schema 嗅探参数 / patch 01: content-schema sniffing
# 只读文件前缀判定"是否为四列数据文件"，避免为一个大压缩包/二进制文件读入全量。
# Only a prefix is read to decide "is this a 4-column data file", so a large archive or
# binary file is never fully loaded just to reject it.
SNIFF_BYTES = 65536                 # 嗅探读取的前缀字节数 / prefix bytes read for sniffing
SNIFF_SAMPLE_LINES = 20             # 抽样判定的数据行数 / sample data lines to test
SNIFF_MATCH_RATIO = 0.6            # 达标比例（抽样行中符合四列 schema 的占比）/ pass ratio
FIRST_LINE_SUMMARY_MAX = 160       # unmatched_files.csv 首行摘要截断长度 / first-line clip
UNMATCHED_FILENAME = "unmatched_files.csv"

# 补丁 01 §3.4 对账参照：用户本地清点数（交接文档 §1）。仅用于报告中的最终对账行；
# 与合成/子集运行无关时报告会自动标注"参照不适用"。
# Patch 01 §3.4 reconciliation reference: the user's local file/byte count (handover §1).
# Used only for the final reconciliation line; flagged "not applicable" on synthetic/subset runs.
PATCH01_LOCAL_FILES = 3747
PATCH01_LOCAL_BYTES_GB = 2.46
PATCH01_PREV_FOUND = 3471          # 首轮报告的发现数（漏发现的对照基线）/ prior run's count
CACHE_SUBDIR = "_cache"          # 逐文件部分聚合缓存（断点续跑）/ per-file partials (resume)
AGGREGATE_FILENAME = "aggregate.json"   # run_eda.py 的全集聚合产物 / global aggregate
INVENTORY_FILENAME = "file_inventory.csv"

# 单文件最多保留多少条 Accelerometer 非零记录（防止畸形文件撑爆缓存）
# Cap on Accelerometer non-zero records kept per file (guards against pathological files)
ACCEL_NONZERO_CAP_PER_FILE = 20000
# 全集最多写入 accel_nonzero.csv 的记录数 / cap on rows written to accel_nonzero.csv
ACCEL_NONZERO_CAP_TOTAL = 500000
# 缺口清单保留条数（其余进统计摘要）/ number of gap segments kept in the top list
GAPS_TOP_N = 100
