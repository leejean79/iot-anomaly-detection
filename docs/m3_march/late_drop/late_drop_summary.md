# 点通道迟到丢弃计数的历史（Prometheus）

## 一、近 15 天每个作业的丢弃合计

| 作业编号 | 作业名 | 首次采样 | 末次采样 | 首次出现丢弃 | 丢弃合计 |
| --- | --- | --- | --- | --- | --- |
| 012b6a35 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-09-20 01:51 UTC | 2026-09-20 03:01 UTC | 无 | 0 |
| ab526027 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-09-30 04:21 UTC | 2026-09-30 04:51 UTC | 2026-09-30 04:36 UTC | 5852 |
| 6905b033 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-01 02:41 UTC | 2026-10-01 03:16 UTC | 无 | 0 |
| b6687789 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-01 09:16 UTC | 2026-10-01 12:01 UTC | 2026-10-01 10:01 UTC | 6854 |
| d9c21609 | M2Job___M1_ingestion_normalization___pMCOD___LSTM_AE_contextual_anomaly_detection | 2026-10-03 07:26 UTC | 2026-10-03 08:41 UTC | 2026-10-03 08:11 UTC | 615474 |

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
| 7 | D、G | 6854 | 2026-10-01 09:58 UTC | 2026-10-01 09:58 UTC |

