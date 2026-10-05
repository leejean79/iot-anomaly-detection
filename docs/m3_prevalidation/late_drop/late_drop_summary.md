# 点通道迟到丢弃计数的历史（Prometheus）

## 一、近 15 天每个作业的丢弃合计

| 作业编号 | 作业名 | 首次采样 | 末次采样 | 首次出现丢弃 | 丢弃合计 |
| --- | --- | --- | --- | --- | --- |
| ab526027 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-09-30 04:22 UTC | 2026-09-30 04:52 UTC | 2026-09-30 04:37 UTC | 5852 |
| 6905b033 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-01 02:42 UTC | 2026-10-01 03:17 UTC | 无 | 0 |
| b6687789 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-01 09:17 UTC | 2026-10-01 12:02 UTC | 2026-10-01 09:57 UTC | 6854 |
| d9c21609 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-03 07:22 UTC | 2026-10-03 08:42 UTC | 2026-10-03 08:12 UTC | 615474 |
| ed120553 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-05 13:32 UTC | 2026-10-05 14:07 UTC | 无 | 0 |

## 二、本次运行逐子任务的丢弃

| 子任务 | 对应设备 | 丢弃合计 | 首次出现丢弃 | 最后一次增加 |
| --- | --- | --- | --- | --- |
| 0 | - | 0 | 无 | - |
| 1 | B、C、E | 0 | 无 | - |
| 2 | H | 0 | 无 | - |
| 3 | - | 0 | 无 | - |
| 4 | - | 0 | 无 | - |
| 5 | F | 0 | 无 | - |
| 6 | A | 0 | 无 | - |
| 7 | D、G | 0 | 无 | - |

