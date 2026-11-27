# 第三批短重放第二组核对（m6c/short_check.py 生成）

| 核对项 | 结果 | 详情 |
| --- | --- | --- |
| 运行 a：零重启 | 通过 | 作业转入重启 0 次 |
| 运行 a：启动横幅的第三批开关 | 通过 | Batch 3:         anchor=trigger min=7 d; defer=7 d; oscillation=false; rebuild plan=true; switch validate=false |
| 运行 b：零重启 | 通过 | 作业转入重启 0 次 |
| 运行 b：启动横幅的第三批开关 | 通过 | Batch 3:         anchor=trigger min=7 d; defer=7 d; oscillation=false; rebuild plan=true; switch validate=false |
| 运行 a：数据发完时没有设备保持水位线 | 通过 | 无 |
| 运行 b：数据发完时没有设备保持水位线 | 通过 | 无 |
| 第二组：开启丁档与二级预案后两次运行逐位相同 | 通过 | scores.jsonl 41890/41890 行相同；drift.jsonl 365/365 行相同；alerts.jsonl 3260/3260 行相同；adapt.jsonl 18/18 行相同；m5a_records.jsonl 1030/1030 行相同；shadow.jsonl 0/0 行相同；m3_markers.jsonl 3/3 行相同；m1out.sha 相同；monitoring_norm.sha 相同 |
