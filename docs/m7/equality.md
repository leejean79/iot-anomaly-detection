# 聚合模块 Java 离线运行器与 Python 参考实现的逐条相同核对（m7/equality_configs.py 生成）

输入：`docs/m7/inputs`。

| 配置 | 结果 | 三份日志 |
| --- | --- | --- |
| 默认配置 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 不带同一段偏离只发一次 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：138 / 138 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 只用数据层 | 逐条相同 | alerts.csv：8302 / 8302 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：46 / 46 条，逐条相同 |
| 只用性能层 | 逐条相同 | alerts.csv：563 / 563 条，逐条相同；commands.csv：0 / 0 条，逐条相同；review.csv：46 / 46 条，逐条相同 |
| 只用检测通道 | 逐条相同 | alerts.csv：1072 / 1072 条，逐条相同；commands.csv：0 / 0 条，逐条相同；review.csv：46 / 46 条，逐条相同 |
| 共模比例二分之一 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 共模比例全体 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| K_site 二 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| K_site 五 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：49 / 49 条，逐条相同 |
| p_ctx 六分之一 | 逐条相同 | alerts.csv：8409 / 8409 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| p_ctx 六分之二 | 逐条相同 | alerts.csv：8404 / 8404 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 决策节拍一小时、等待六小时、提前量一小时 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：85 / 85 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 决策节拍一小时、等待六小时、提前量一天 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：80 / 80 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 决策节拍一小时、等待十二小时、提前量一小时 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：79 / 79 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 决策节拍一小时、等待十二小时、提前量一天 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：78 / 78 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 决策节拍一小时、等待一个日界、提前量一小时 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：78 / 78 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 决策节拍一小时、等待一个日界、提前量一天 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：77 / 77 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 决策节拍一天、等待六小时、提前量一小时 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 决策节拍一天、等待六小时、提前量一天 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 决策节拍一天、等待十二小时、提前量一小时 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 决策节拍一天、等待十二小时、提前量一天 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 决策节拍一天、等待一个日界、提前量一小时 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：48 / 48 条，逐条相同 |
| 决策节拍一天、等待一个日界、提前量一天 | 逐条相同 | alerts.csv：8391 / 8391 条，逐条相同；commands.csv：76 / 76 条，逐条相同；review.csv：48 / 48 条，逐条相同 |

共 23 种配置，0 种不同或失败。
