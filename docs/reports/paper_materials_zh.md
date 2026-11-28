# 论文实验材料清单

> 起草：代码开发 agent（2026 年 11 月 28 日）。依据：第三批交接文档第五节（V-M6c-5）、1.58 版收口记录第六节。读者：用户（理论与叙述执笔）、开发设计会话（口径核对）。
> 每一项给出四件事：**来源报告**（`docs/reports/` 下，结论与判读在那里）、**表或图的文件**（入库的数据表）、**生成脚本**（从入库的表或转储重新生成）、**数字的口径**（这个数怎么来的，避免叙述时混用）。图一律由脚本从入库的表重新生成；大转储不入库的，报告的「复现方式」一节写明再生成命令。
> 章节按交接文档第五节建议的顺序；编号只供引用。配置名称：「第二批配置」指零级生效前七天、湿度与气压不重估；「第三批配置」指丁档（锚定触发通道、最短七天、不足则推迟生效）。

---

## 一、数据集与数据事实

| 编号 | 内容 | 来源报告 | 表或图 | 生成脚本 | 口径 |
| --- | --- | --- | --- | --- | --- |
| 1.1 | 覆盖与逐日轮数、一周拷贝段 | `data_copy_scan_report.md`、`data_june_shifted_copy_for_decision.md` | `docs/dataset_daily_rounds.csv`、`docs/dataset_coverage.csv`、`docs/copy_segments.csv`、`docs/figures/dataset_copy_timeline.png` | `eda/count_rounds.py`、`eda/find_copy_candidates.py`、`eda/copy_timeline.py` | 一轮为同一时间戳的读数；拷贝段按逐点差判定，按非数据处理。 |
| 1.2 | G 的标定周人工照明 | `g_light_profile.md`、`g_light_facts_for_decision.md` | `docs/stuck_guard/floor_values.csv`、报告内剖面图 | `eda/g_light_profile.py` | 光照尺度相对正常期之比；时间按布里斯托当地时间。 |
| 1.3 | 全场事件与气体抬高、四月五个时段 | `fleet_drift_episodes.md` | `docs/fleet_episodes/hourly.csv`、`docs/m5b/streams_hourly.csv` | `eda/fleet_episode_profile.py` | 中位数相对标定宽度的倍数；时段边界见聚合模块报告表格的 `APRIL` 定义（`m7/report_tables.py`）。 |
| 1.4 | 传感器卡死与守卫阈值 | `m1_stuck_guard_report.md` | `docs/stuck_guard/stuck_check.csv`、`top10_runs.csv` | `eda/repeat_run_lengths.py` | 连续相同读数的游程长度；阈值为自然最长游程的两倍以上。 |
| 1.5 | 气压相对冻结周的八台同日偏离（天气通道） | `m6b_chain_keep_report.md` 第四节、`m6b_q12_supp_report.md` 第三节 | `docs/m6b/chain/D7K/data_days.csv` | `com.leejean.m6.ChainDriver`、`m6b/chain_keep_report.py` | 设备日的数据层日末偏离；约一半可评价日偏离。 |
| 1.6 | 数据事实清单、理论回传、决策三张附录 | 开发设计日志第四、六、三节 | — | — | 由开发设计会话整理（1.58 版收口记录第六节）。 |

## 二、系统与实验纪律

| 编号 | 内容 | 来源报告 | 表或图 | 生成脚本 | 口径 |
| --- | --- | --- | --- | --- | --- |
| 2.1 | 逐位等值的方法 | 方法手册 `docs/design/method_bitwise_equivalence_zh.md` | — | — | 同一主机逐位；跨主机只比离散决策量，连续量通常百分之一以内、个别训练实例可达百分之五（第五条，1.55 版修订）。 |
| 2.2 | 在线与离线、Java 与 Python 的等值证据 | `m5b_verify_report.md`、`m5a_reference_check.md`、`m7_online_report.md`、`m6b_chain_keep_report.md`、`m6c_chain_anchor_zh.md` | `docs/m7/equality.md`、`docs/m6b/chain/check_D7K.md`、`docs/m6c/chain/check_anchor.md`、`check_ding.md` | `m7/compare_logs.py`、`m6b/chain_check.py` | 逐条相同；光照（对数变换）容差 1e-12，实测最大差 8.9e-16。 |
| 2.3 | 重放完整性八条断言 | 各全量报告第二节 | `docs/m6b/full/check.txt`、`docs/m6c/full/check.txt` | `deploy/scripts/syn-replay-verify.sh` | 八条断言与零重启；全量重放另核对数据发完无设备保持水位线。 |
| 2.4 | 开环与闭环、链式离线实验 | `m6b_chain_report.md` 第一节 | `docs/m6b/chain/report_tables.md` | `com.leejean.m6.ChainDriver`、`m6b/chain_report.py` | 链式为离线闭环：决策函数加八台模拟器加数据层重算，逐日推进，约半分钟一档。 |
| 2.5 | 短重放等值（第三批两组） | `m6c_full_replay_report_zh.md` 第一节第 4 条 | `docs/m6c/short1/short_report.md`、`docs/m6c/short2/short_report.md` | `deploy/scripts/syn-m6c-short.sh`、`m6c/short_check.py` | 第一组：标签 jar 与新 jar（第三批全部关闭）九类输出逐位相同；第二组：开启选定项两次运行逐位相同。 |
| 2.6 | 异步训练的确定性 | `m3_addendum4_report.md` | `docs/m3_a4/` 下的等值核对 | `deploy/scripts/m3_a4_equality.py` | 训练墙钟只改变延迟，不改变事件时间上的输出。 |

## 三、检测器基线

| 编号 | 内容 | 来源报告 | 表或图 | 生成脚本 | 口径 |
| --- | --- | --- | --- | --- | --- |
| 3.1 | 注入召回（十四段：尖峰、阶跃、爬坡、卡死） | `m3_inject_run_report.md` | `docs/m3_inject/recall/recall.csv`、`recall.md`、`figs/` | `deploy/scripts/m3_injection_recall.py` | 每段按点通道与上下文通道分别计是否检出与时延；注入真值 `docs/m3_inject/inject-truth.csv`。 |
| 3.2 | 平稳日误报率（上下文通道参考误报率） | `m3_marapr_run_report.md`、V-M3-4（平稳日误报率）裁决 | `docs/m3_inject/v34/v34_daily.csv`、`v34_report.md`、`w_derivation.csv`、`v34_daily.png` | `deploy/scripts/m3_v34_report.py` | 实测 2.23%，未达先验期望，按「不迎合」登记（1.0 版指南第七章、日志 0.76 版）。 |
| 3.3 | 上下文通道选型网格 | `m3_grid_final_report.md` | `docs/m3_grid.csv`、`docs/m3_grid_per_epoch.csv` | 报告复现方式一节 | 隐单元、窗长、批大小的网格；早停损失与平稳段超阈率。 |
| 3.4 | 性能层确认表与时延 | `m5b_stage_report.md`、`m5b_offline_grid.md` | `docs/m5b/grid/segments.csv`（`cand_delay_*`、`confirm_delay_*` 列）、`confirms.csv` | `m5b/run_grid.py`、`m5b/optwin_offline.py` | 候选到确认的时延；持续性核对六小时（三十六元素）。 |
| 3.5 | 数据层统计方案与两层对照 | `m5a_offline_report.md`、`m5a_stage_report.md` | `docs/m5a/schemes.csv`、`segments.csv`、`two_layer.csv`、`lm_transitions.csv` | `m5a/run_offline.py`、`m5a/stats.py` | KS 与 G 检验经五通道校正，效应量门槛按通道推导；状态转移语义。 |
| 3.6 | 点通道基线（逐设备离群率） | `m2_march_baseline_findings.md`、`m2_java11_march_baseline.md` | `docs/reports/m2_java11_march_per_device.md`、`m2_java11_march-m3_per_device.csv`、`m2_java11_march_rate_cmp.svg` | 报告复现方式一节 | 十分钟离群率；逐设备半径的网格定值。 |

## 四、聚合决策

| 编号 | 内容 | 来源报告 | 表或图 | 生成脚本 | 口径 |
| --- | --- | --- | --- | --- | --- |
| 4.1 | 片段矩阵（开环） | `m7_offline_report.md` | `docs/m7/report/tables.md`、`docs/m7/report/default/` | `m7/decide.py`、`m7/report_tables.py` | 每段写定的预期与实测并排；平稳日、日照直射日、四月时段的定义在 `m7/report_tables.py`。 |
| 4.2 | 三层融合消融 | `m7_offline_report.md` | `docs/m7/report/only_data/`、`only_perf/`、`only_detect/` | `m7/report_tables.py` | 只用一层与全部三层的命令数、告警按归因。 |
| 4.3 | 共模比例与现场条件天数 | `m7_offline_report.md` | `docs/m7/report/common_1_2/`、`common_1_1/`、`ksite_2/`、`ksite_5/`、`pctx_1/`、`pctx_2/` | `m7/report_tables.py` | 共模门槛 max(2, ⌈N·比例⌉)。 |
| 4.4 | 时间参数扫描（十二种组合） | `m7_time_params_for_decision.md` | `docs/m7/scan_time/summary.csv`、`latency.csv`、`scan.md` | `m7/scan_time_params.py` | 决策节拍、等待时长、生效提前量三个参数的组合；时延按第一条信号到命令生效。 |
| 4.5 | 在线时延与在线等于离线 | `m7_online_report.md` | `docs/m7/report/latency.md`、`docs/m7/verify/` | `m7/latency_table.py` | 在线三份日志与离线运行器逐字节相同。 |

## 五、适应闭环

| 编号 | 内容 | 来源报告 | 表或图 | 生成脚本 | 口径 |
| --- | --- | --- | --- | --- | --- |
| 5.1 | 零级的含义与上下文失配（第一批） | `m6a_offline_sim_report.md`、`m6a_online_report.md` | `docs/m6a/sim/tables.md` | `m6a/sim_report.py`、`m6a/reestimate.py` | 零级重估后上下文超阈率升到约八成（上下文通道跟随零级时）。 |
| 5.2 | 重估窗口三档与湿度气压开关（第二批） | `m6b_chain_report.md`、`m6b_chain_keep_report.md` | `docs/m6b/chain/report_tables.md`、`report_tables_D7K.md`、`keep_check.md` | `ChainDriver`、`m6b/chain_report.py`、`m6b/chain_keep_report.py` | 命令总数；生效后七天非触发通道连带偏离日数；丙型实例（同一台同一通道相邻两条零级偏移符号相反）。 |
| 5.3 | 零级窗口锚定四档并排（第三批） | `m6c_chain_anchor_zh.md`、`m6c_chain_ding_zh.md` | `docs/m6c/chain/anchor_check.md`、`docs/m6c/chain/{off,trigger,all,ding}/` | `ChainDriver --anchor … --anchor-defer-days …`、`m6c/chain_anchor_report.py` | 甲 60 条、连带 364、七天内丙型 11；乙 64、406、21；丙 70、450、31；丁 23、165、5。典型延迟按「生效时刻减最早进入之日的当地零点」：甲 72 小时、丁 168 小时（1.57 版第四项）。 |
| 5.4 | 振荡规则两档 | `m6c_chain_anchor_zh.md` 第四节、`m6c_chain_ding_zh.md` 第四节 | `docs/m6c/chain/{off_osc,ding_osc}/`、`anchor_check.md` | `ChainDriver --osc-suppress true` | 连锁丙型实例；甲上命令 60 降 54，丁上不减（推迟两条）。默认不开。 |
| 5.5 | 一级重训的窗口位置、长度、启动方式与五个种子（开放问题第十二条） | `m6b_q12_report.md`、`m6b_q12_supp_report.md` | `docs/m6b/q12/report_tables.md`、`docs/m6b/q12_supp_report_tables.md` | `m6b/q12_report.py`、`m6b/q12_supp_report.py` | 耗时、平稳段超阈率、注入召回三项；训练窗口取生效之后三天回到初始模型量级。 |
| 5.6 | 全量闭环：命令与时延（两种配置） | `m6b_full_replay_v2_report.md`、`m6c_full_replay_report_zh.md` | `docs/m6b/full/full_report_tables.md`、`docs/m6c/full/full_report_tables.md` | `m6b/full_check.py` | 第二批配置命令 57（零级 40、一级 15、二级 2），三月事件零级 48 至 96 小时；第三批配置命令 35（19、11、3），120 至 191 小时。时延起点为第一条信号（与 5.3 的口径不同）。 |
| 5.7 | 影子评分表（选项一「上下文通道不跟随零级」的验证） | `m6b_full_replay_report.md`、`m6b_full_replay_v2_report.md` 第五节 | `docs/m6b/full/full_report_tables.md` 影子评分表 | `m6b/full_check.py` | 零级生效后七天正式超阈率（按训练版本）与影子超阈率（按当前版本）；只是验证参照，第三批起默认关闭（1.52 版）。 |
| 5.8 | 一级切换比值复算（二十三行） | `m6c_switch_recompute_zh.md` | `docs/m6c/recompute/recompute_table.md`、`rerun_a_pass{1,2}.csv`、`replay1_pass{1,2}.csv` | `src/test/java/com/leejean/m3/M6cSwitchRecompute.java`、`m6c/recompute_table.py` | 比值为新旧模型各自标定段误差中位数之比；切换后七天入口轮按新旧模型分别评分的超阈率。 |
| 5.9 | 二级三例复核与跨级交叉触发 | `m6c_switch_recompute_zh.md` 第五节、`m6c_chain_anchor_zh.md` 第五节 | `docs/m6c/level2_cases.txt`、`docs/m6c/chain/cross_level.md` | `m6c/level2_cases.py`、`m6c/cross_level.py` | 交叉触发九次，六次为升级格本意，三次为一级切换后三天内的零级。 |
| 5.10 | 迟到命令与限速（加速评测的事件时间偏差） | `m6b_full_attempt1_incident.md`、`m6b_throttle_proof1_notes_zh.md`、`m6b_full_replay_v2_report.md` 第一节 | `docs/m6b/throttle/throttle_report.md` | `deploy/scripts/syn-m6b-full.sh --proof`、`m6b/full_check.py --proof` | 第一次全量六条迟到命令；限速阈值 43,200 秒，暂停期间保活。 |

## 六、对理论的修正

日志第六节第二十六至三十二条，每条的证据材料：

| 条 | 内容 | 证据 |
| --- | --- | --- |
| 第二十六条 | 零级重估窗口不能一刀切，天气通道不随零级重估 | 5.2；1.5 |
| 第二十七条 | 上下文通道不跟随零级 | 5.1；5.7 |
| 第二十八条 | 决策矩阵「零级先行、不回落则二级」失去前半段 | 5.1；4.1 |
| 第二十九条及补充 | 窗口锚定在变化点之后，不足最短长度则推迟生效 | 5.3（锚定窗口在现行触发规则下几乎都是三天，丁档推迟到七天） |
| 第三十条 | 影子评分驱动的版本切换（给理论线的选项） | 5.7 |
| 第三十一条 | 同一通道来回反向的零级相当于「隔离加人工审计」一行的零级对应物 | 5.4 |
| 第三十二条 | 「重训后验证」不能用新旧标定段误差之比 | 5.8；数据事实第四十五条候选 |

## 七、使用说明

- 引用数字时同时写明配置（第二批或第三批）与口径（例如时延的起点是第一条信号还是进入之日零点）。
- 跨主机重算的连续量只作近似对照；逐位相同的结论只对同一主机成立。
- 本清单之外的阶段性报告（`*_for_decision.md`、`*_incident.md`）记录了决策过程，可作为附录或补充材料，不建议作为正文的数据来源。
