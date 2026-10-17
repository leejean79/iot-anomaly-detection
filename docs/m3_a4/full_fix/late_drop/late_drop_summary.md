# 点通道迟到丢弃计数的历史（Prometheus）

## 一、近 15 天每个作业的丢弃合计

| 作业编号 | 作业名 | 首次采样 | 末次采样 | 首次出现丢弃 | 丢弃合计 |
| --- | --- | --- | --- | --- | --- |
| d9c21609 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-03 07:25 UTC | 2026-10-03 08:45 UTC | 2026-10-03 08:10 UTC | 615474 |
| ed120553 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-05 13:35 UTC | 2026-10-05 14:30 UTC | 无 | 0 |
| fa9ee1a1 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-06 01:45 UTC | 2026-10-06 03:05 UTC | 无 | 0 |
| bce435a5 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-09 01:10 UTC | 2026-10-09 03:20 UTC | 无 | 0 |
| 56a91831 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-14 07:30 UTC | 2026-10-14 11:00 UTC | 无 | 0 |
| 8dc691ef | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-14 11:15 UTC | 2026-10-14 11:20 UTC | 无 | 0 |
| c93913fd | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-15 01:40 UTC | 2026-10-15 02:30 UTC | 无 | 0 |
| a2094bee | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-15 02:35 UTC | 2026-10-15 03:25 UTC | 无 | 0 |
| dfdb2362 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-15 03:30 UTC | 2026-10-15 04:20 UTC | 无 | 0 |
| df5559ef | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-15 04:25 UTC | 2026-10-15 05:15 UTC | 无 | 0 |
| 30c262e0 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-15 05:20 UTC | 2026-10-15 05:25 UTC | 无 | 0 |
| 9218af22 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-16 01:40 UTC | 2026-10-16 11:20 UTC | 无 | 0 |
| 417c5a18 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-17 02:05 UTC | 2026-10-17 02:20 UTC | 无 | 0 |
| 8da8ac12 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-17 02:55 UTC | 2026-10-17 12:35 UTC | 无 | 0 |

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

