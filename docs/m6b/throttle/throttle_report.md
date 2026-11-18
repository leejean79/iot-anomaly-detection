# 重放器限速开关对照（m6b/full_check.py --proof 生成）

| 核对项 | 结果 | 详情 |
| --- | --- | --- |
| 运行 a：零重启 | 通过 | 作业转入重启 0 次 |
| 运行 b：零重启 | 通过 | 作业转入重启 0 次 |
| 限速开与关两次运行逐位相同 | 通过 | scores.jsonl 56735/56735 行相同；drift.jsonl 396/396 行相同；alerts.jsonl 3674/3674 行相同；adapt.jsonl 28/28 行相同；m5a_records.jsonl 1030/1030 行相同；shadow.jsonl 0/0 行相同；m3_markers.jsonl 0/0 行相同；m1out.sha 相同；monitoring_norm.sha 相同 |

| 运行 | 限速 | 重放器日志中的限速记录 |
| --- | --- | --- |
| a | 打开（43,200 秒） | Max skew: 43200 s (M7-Decision vs RobustScaler watermark, http://localhost:8081, every 5000 ms) Skew pauses: 4 (total 1630 s wall, keepalives 52, max skew seen 44678 s, poll errors 0) |
| b | 关闭 | Max skew: OFF |
