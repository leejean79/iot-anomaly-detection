# 点通道迟到丢弃计数的历史（Prometheus）

## 一、近 15 天每个作业的丢弃合计

| 作业编号 | 作业名 | 首次采样 | 末次采样 | 首次出现丢弃 | 丢弃合计 |
| --- | --- | --- | --- | --- | --- |
| ab526027 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-09-30 04:21 UTC | 2026-09-30 04:51 UTC | 2026-09-30 04:36 UTC | 5852 |
| 6905b033 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-01 02:41 UTC | 2026-10-01 03:16 UTC | 无 | 0 |
| b6687789 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-01 09:16 UTC | 2026-10-01 12:01 UTC | 2026-10-01 10:01 UTC | 6854 |
| d9c21609 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-03 07:26 UTC | 2026-10-03 08:41 UTC | 2026-10-03 08:11 UTC | 615474 |
| ed120553 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-05 13:36 UTC | 2026-10-05 14:26 UTC | 无 | 0 |
| fa9ee1a1 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-06 01:46 UTC | 2026-10-06 02:46 UTC | 无 | 0 |

## 二、本次运行逐子任务的丢弃

| 子任务 | 设备（代理键） | 设备（原始编号） | 丢弃合计 | 首次出现丢弃 | 最后一次增加 |
| --- | --- | --- | --- | --- | --- |
| 0 | A | - | 0 | 无 | - |
| 1 | B | B、C、E | 0 | 无 | - |
| 2 | C | H | 0 | 无 | - |
| 3 | D | - | 0 | 无 | - |
| 4 | E | - | 0 | 无 | - |
| 5 | F | F | 0 | 无 | - |
| 6 | G | A | 0 | 无 | - |
| 7 | H | D、G | 0 | 无 | - |

