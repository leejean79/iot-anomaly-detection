# V-M6b-3 短重放核对（m6b/verify_short.py 生成）

一级生效 2022-03-29 当地零点（1648508400），N = 3 天，窗口 60 轮。

| 核对项 | 结果 | 详情 |
| --- | --- | --- |
| 两次运行逐位相同 | 失败 | scores.jsonl 9754/9754 行相同；drift.jsonl 306/306 行相同；m1out_G.jsonl 85296/85296 行相同；monitoring_G.jsonl 28415/28415 行，原样不同 108 行（不同的字段：m3BufferRounds 98 行）；去掉m3BufferRounds与状态快照时间戳后仍不同；m1-out 摘要相同 |
| 运行 a：一级命令转发一次 | 通过 | 转发 1 条，零级执行 0 条 |
| 运行 a：一级训练集范围 | 通过 | Device G level 1 (seq 1, effective 1648508400): training set [1648508400, 1648767600) (3 days after the effective time, cold start) |
| 运行 a：切换前后的模型代数与版本号 | 通过 | 评分 1046 条；第一条新模型评分窗口末 1648768380（生效后 3.01 天）；不一致 0 条；版本号 [1]；切换日志：Device G level-1 switch: model generation 1, training version 1 -> 1; 0 rounds of the open window re-standardized |
| 运行 a：每个在线窗口恰好一条评分 | 通过 | 评分 1046 条，窗口末重复 0 个；从第 13861 个接纳轮起每 60 轮一条，与 1046 条评分中的前 1046 条逐条对上 |
| 运行 a：性能层在模型代数改变时清空一次 | 通过 | 清空记录 1 条 |
| 运行 a：没有一级失败 | 通过 | 失败标记 0 条，放弃日志 0 条 |
| 运行 b：一级命令转发一次 | 通过 | 转发 1 条，零级执行 0 条 |
| 运行 b：一级训练集范围 | 通过 | Device G level 1 (seq 1, effective 1648508400): training set [1648508400, 1648767600) (3 days after the effective time, cold start) |
| 运行 b：切换前后的模型代数与版本号 | 通过 | 评分 1046 条；第一条新模型评分窗口末 1648768380（生效后 3.01 天）；不一致 0 条；版本号 [1]；切换日志：Device G level-1 switch: model generation 1, training version 1 -> 1; 0 rounds of the open window re-standardized |
| 运行 b：每个在线窗口恰好一条评分 | 通过 | 评分 1046 条，窗口末重复 0 个；从第 13861 个接纳轮起每 60 轮一条，与 1046 条评分中的前 1046 条逐条对上 |
| 运行 b：性能层在模型代数改变时清空一次 | 通过 | 清空记录 1 条 |
| 运行 b：没有一级失败 | 通过 | 失败标记 0 条，放弃日志 0 条 |
