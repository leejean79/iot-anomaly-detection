# 第三批短重放第一组核对（m6c/short_check.py 生成）

| 核对项 | 结果 | 详情 |
| --- | --- | --- |
| 运行 a：零重启 | 通过 | 作业转入重启 0 次 |
| 运行 a：标签 jar（横幅里没有第三批一行） | 通过 | 横幅里没有「Batch 3:」一行 |
| 运行 b：零重启 | 通过 | 作业转入重启 0 次 |
| 运行 b：启动横幅的第三批开关 | 通过 | Batch 3:         anchor=off min=2 d; defer=0 d; oscillation=false; rebuild plan=false; switch validate=false |
| 运行 a：数据发完时没有设备保持水位线 | 通过 | 无 |
| 运行 b：数据发完时没有设备保持水位线 | 通过 | 无 |
| 第一组：标签 jar 与新 jar（第三批全部关闭）逐位相同 | 失败 | scores.jsonl 24427/24427 行相同；drift.jsonl 417/417 行相同；alerts.jsonl 2553/2553 行相同；adapt.jsonl 31/31 行相同；m5a_records.jsonl 1030/1030 行相同；shadow.jsonl 0/0 行相同；m3_markers.jsonl 290/290 行相同；m1out.sha 不同；monitoring_norm.sha 相同 |
