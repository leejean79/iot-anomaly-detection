# M3 上下文通道（LSTM 自编码器）验收报告

版本：2026-10-12。作者：代码开发代理。依据：交接文档 `docs/handover/handover_m3_lstm_ae_en.md` 第五节（两批验收）。
按用户要求用中文书写，附命令与输出摘录。各项的详细过程见文中所列的报告文件。

## 一、总表

| 编号 | 内容 | 结果 | 判定依据 |
| --- | --- | --- | --- |
| V-M3-1 | 兼容性冒烟测试（三部分，含 Java 11 下复核） | 通过 | `docs/java11_migration_report.md` 第 4.4 节 |
| V-M3-2 | 单元与集成测试 | 通过（M3 相关 33 个用例全部通过） | 第三节 |
| V-M3-3 | 离线超参数网格 | 通过，选定隐单元 60、窗长 60 | `docs/reports/m3_grid_final_report.md`；开发设计日志 0.60 网格结论定稿 |
| V-M3-4 | 正常数据上的误报率 | 已按裁决口径给出，**判定待设计会话**：逐台平稳日机队合并 2.18%，高于期望 0.1% 至 1% | 第五节 |
| V-M3-5 | 注入召回表 | 通过（2026-10-09 裁决第二节第 3 条「验收要求满足」） | 第六节 |
| V-M3-6 | 性能报告 | 通过（2026-10-09 裁决第一节采纳） | 第七节 |

## 二、第一批：门槛与离线

### 2.1 V-M3-1（兼容性冒烟测试）

- 内容：在真实的 Flink 容器中验证四点——JavaCPP 堆外预算有界、原生库解包目录可写、运行时为 Java 11、jar 中的原生库
  只有 linux-x86_64；另验证 ND4J 原生库在八个子任务上都能加载。
- 命令（本地 Mac，仓库根目录）：

```bash
bash deploy/scripts/syn-m3-smoke.sh --parallelism 8
```

- 输出摘录（Java 11 迁移后复核，`docs/java11_migration_report.md` 第 4.4 节）：

```
Point 1  off-heap budget (JavaCPP bounded)  : PASS
Point 2  native cachedir writable (uid 9999): PASS
Point 3  JDK is Java 11                     : PASS
Point 4  jar natives = linux-x86_64 only    : PASS
(aux)    ND4J native load on TMs            : PASS
```

- 过程中修正的两项迁移缺陷：胖 jar 缺 OpenBLAS 与 JavaCPP 的 linux-x86_64 原生库（已在 `pom.xml` 显式声明）；
  JavaCPP 物理内存上限曾按堆外预算设为 768 MB（它比对的是整个进程常驻内存，改按进程总量设定；现行值见第七节）。

### 2.2 V-M3-2（单元与集成测试）

- 命令（本地或任意装有 JDK 11 的机器，仓库根目录）：

```bash
mvn test -Dtest='DL4JCompatSmokeTest,M3CoreTest,M3EvalBatchTest,M3GridTest,M3StateMachineTest,M3TrainingTest,PmcodM3FlagEquivalenceTest'
```

- 输出摘录（2026-10-12，全量 120 个用例同时通过）：

```
Tests run: 3,  Failures: 0, Errors: 0  com.leejean.m3.DL4JCompatSmokeTest
Tests run: 10, Failures: 0, Errors: 0  com.leejean.m3.M3CoreTest
Tests run: 2,  Failures: 0, Errors: 0  com.leejean.m3.M3EvalBatchTest
Tests run: 7,  Failures: 0, Errors: 0  com.leejean.m3.M3GridTest
Tests run: 6,  Failures: 0, Errors: 0  com.leejean.m3.M3StateMachineTest
Tests run: 3,  Failures: 0, Errors: 0  com.leejean.m3.M3TrainingTest
Tests run: 2,  Failures: 0, Errors: 0  com.leejean.m2.PmcodM3FlagEquivalenceTest
```

- 交接文档要求的覆盖项与对应用例：

| 要求 | 用例 |
| --- | --- |
| 损失掩码 | `M3CoreTest.lossMaskingZerosWeightForCensoredChannels`、`lossMaskingWithChannelWeights`、`buildMaskRespectsNullAndCensored` |
| 状态机每一个转移 | `M3StateMachineTest.everyPhaseTransitionHappensInOrder` |
| 早停 | `M3StateMachineTest.earlyStoppingEndsTrainingBeforeMaxEpochs`、`M3TrainingTest.earlyStopBookkeepingIsConsistentWithTheStoppingRule` |
| 阈值校准 | `M3CoreTest.scorerCalibrationAndScoring`、`scorerHandlesEdgeCases` |
| 检查点恢复后模型权重存活 | `M3StateMachineTest.modelWeightsSurviveCheckpointRestore`、`restoringWithMismatchedHiddenSizeFailsLoudly` |
| 设备隔离 | `M3StateMachineTest.devicesAdvanceIndependently` |
| 其他（在 Flink 中训练与推理、取消响应、批量评估等值、点通道输出不受 M3 开关影响、代理键） | `DL4JCompatSmokeTest.lstmInFlinkKeyedProcessFunction`、`M3TrainingTest.interruptStopsTrainingWithinOneEpoch`、`M3EvalBatchTest`、`PmcodM3FlagEquivalenceTest` |

### 2.3 V-M3-3（离线超参数网格）

- 内容：三台代表设备（E 在带内、G 光照被屏蔽、C 结构中等）上，隐单元 {40, 60, 90} × 窗长 {30, 60, 120} 共九种组合，
  比较早停集误差，按规则选型。
- 命令（本地 Mac，经 master 运行；原网格先以上限 100 轮运行，被截断的三格再以上限 300 轮补跑，下面是一次跑完的等效写法）：

```bash
bash deploy/scripts/syn-m3-grid.sh --devices E,G,C --hidden-grid 40,60,90 --window-grid 30,60,120 --batch-grid 64 --lr-grid 0.001 --grad-clip 39072.0 --max-epochs 300 --patience 20 --omp-threads 1 --node master --detach
```

- 结果摘录（窗长 60 一列，末十轮早停集误差均值，括号内为相对该设备最优值的倍数）：

| 设备 | 隐单元 40 | 隐单元 60 | 隐单元 90 |
| --- | --- | --- | --- |
| C | 0.204015（1.337） | 0.152560（1.000） | 0.163448（1.071） |
| E | 0.030409（1.242） | 0.024488（1.000） | 0.025458（1.040） |
| G | 0.037302（1.000） | 0.045624（1.223） | 0.042256（1.133） |

- 选型：最差倍数最小者为隐单元 90（1.133），隐单元 60（1.223）在其 1.10 倍以内视为并列，并列取参数更少者，**选定
  隐单元 60、窗长 60**，与作业默认值相同。图见 `docs/figures/m3_v3_grid.png`。选型过程中把训练轮数上限由 100 提到
  300（设备 C 在第 111 轮才早停）。

## 三、第二批：在线验收所用的运行

| 运行 | 作业 | 时段 | 用途 |
| --- | --- | --- | --- |
| 三月至四月运行（不注入） | `d9c21609` | 2022-03-01 至 04-30 | V-M3-6 首次测量、E 与 C 等值核验；D、G 自 03-25 起因迟到丢弃作废（`docs/reports/m3_marapr_late_drop_incident.md`） |
| 注入运行 | `bce435a5700bea8e045b3c1a7054afb3` | 2022-03-01 至 04-30，两段重放，注入在第二段 | V-M3-4、V-M3-5、V-M3-6 的正式数据（`docs/reports/m3_inject_run_report.md`） |

数据分段：03-01 至 03-08 标准化预热，03-08 至 03-15 训练集，03-15 至 03-17 早停集，03-17 至 03-24 阈值校准，
03-24 至 04-30 在线。注入运行的有效性：七条重放完整性断言与两条预验证条件全部通过（`docs/m3_inject/replay_verify.txt`）：

```
[断言一 轮数对账] PASS —— 实测 4004965 vs 期望 4005039（容差 0.2%）
[断言五 零重启]   PASS —— 作业 bce435a5700bea8e045b3c1a7054afb3 重启次数 0。
[断言六 迟到丢弃] PASS —— 2026-10-09T01:10:57Z 以来点通道迟到丢弃合计 0。
[断言七 覆盖末尾] PASS —— 八台设备两个通道的最晚输出都在最后一轮之前 3600 秒以内。
✅ 七条断言全部通过
```

确定性核对：A、B、C、F、H 五台的上下文评分与三月至四月运行逐窗相同（主分到小数点后九位），E 的 241 个不同窗口
全部落在注入区间及其后一个窗长之内。

## 四、第二批的主分分布与阈值

告警阈值为主分 ≥ 2.22（约 3 个标准差），阈值校准期 7 天（2026-10-02 裁决第四节第 1 条）。各台自身平稳日上的主分
分布见 `docs/m3_inject/v34/v34_score_hist.png`（虚线为阈值）。

## 五、V-M3-4（正常数据上的误报率）

- 口径：2026-10-09 裁决第二节第 5 条——逐台设备按自身五个通道的偏移（相对参照期 03-08 至 03-17，以参照期 P10–P90
  宽度为单位）判定平稳日，绝对值都不超过 0.5 的日子为该台的平稳日；误报率在各台自己的平稳日上计算；注入设备 E 在
  注入区间及其后 3,600 秒内的评分排除；阈值不调。
- 命令（本地 Mac，仓库根目录）：

```bash
python3 deploy/scripts/m3_v34_report.py --scores docs/m3_inject/m3_scores.jsonl --profile docs/m3_marapr/daily_channel_profile_ref0308.csv --start 2022-03-24 --end 2022-05-01 --exclude-truth docs/m3_inject/inject-truth.csv --out-dir docs/m3_inject/v34
```

- 结果（`docs/m3_inject/v34/v34_report.md`）：

| 设备 | A | B | C | D | E | F | G | H | 机队合并 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 平稳日天数 | 6 | 8 | 10 | 6 | 6 | 5 | 5 | 5 | - |
| 误报率 | 0.00% | 3.77% | 1.67% | 5.81% | 0.00% | 0.14% | 0.15% | 5.48% | 2.18% |

- 判读：A、E、F、G 在 0.15% 以内；B、D、H 的告警集中在少数几天（B 与 H 在 03-30、04-17，D 在 04-12），这些日子各台五个
  通道的偏移都在 ±0.5 以内，但气体通道的宽度倍数为 1.7 至 2.2——中位数没动、散布变大，平稳日判据看不见。敏感性分析
  （不改变判据）：气体宽度倍数小于 1.5 的平稳日上机队误报率 0.55%（30 / 5,441），不小于 1.5 的为 8.83%（118 / 1,337）。
  C 的告警出现在气体宽度正常的日子，原因未查明。
- 判定：机队合并 2.18% 高于交接文档期望的 0.1% 至 1%；是否通过、是否在平稳日判据中加入宽度条件，由设计会话决定。

## 六、V-M3-5（注入召回表）

- 口径：2026-10-09 裁决第二节第 2 条，设计会话 10-10 答复定稿。点通道只计时间戳在注入区间内的轮；上下文通道的
  窗口与区间重叠（窗口起点早于注入结束、窗口终点晚于注入开始）即计入，延迟为首个报警窗口的终点减注入开始；结束时
  的跳回单列、不计入检出；卡死期间点通道的检出记为「附带，跨通道不一致」。
- 注入方案：设备 E，温度通道尖峰、阶跃、爬坡各三档（1、2、4 倍阈值校准期 P10–P90 宽度，温度 1 倍 = 4.56），卡死三次；
  气体通道补做 2 倍阶跃与 2 倍爬坡（`docs/m3_inject_plan.csv`）。
- 命令（本地 Mac，仓库根目录）：

```bash
python3 deploy/scripts/m3_injection_recall.py --truth docs/m3_inject/inject-truth.csv --scores docs/m3_inject/scores.jsonl --plan docs/m3_inject_plan.csv --out-dir docs/m3_inject/recall
```

- 结果（`docs/m3_inject/recall/recall.md`；设备 E 在注入日、注入区间以外的背景超阈比例两个通道都是 0）：

| 类型 | 档位 | 点通道检出 / 延迟 | 上下文检出 / 延迟 | 上下文超阈窗 / 窗 |
| --- | --- | --- | --- | --- |
| 尖峰 | 1 倍 / 1 轮 | 是 / 52 秒 | 否 | 0/1 |
| 尖峰 | 2 倍 / 2 轮 | 是 / 35 秒 | 否 | 0/1 |
| 尖峰 | 4 倍 / 3 轮 | 是 / 9 秒 | 是 / 429 秒 | 1/1 |
| 阶跃 | 1 倍 | 是 / 38 秒 | 是 / 77.6 分钟 | 2/13 |
| 阶跃 | 2 倍 | 是 / 38 秒 | 是 / 458 秒 | 13/13 |
| 阶跃 | 4 倍 | 是 / 38 秒 | 是 / 518 秒 | 13/13 |
| 爬坡 | 1 倍 | 否 | 否 | 0/37 |
| 爬坡 | 2 倍 | 否 | 是 / 149.0 分钟 | 22/36 |
| 爬坡 | 4 倍 | 否 | 是 / 40.0 分钟 | 33/36 |
| 卡死 | 第 1 次 | 否 | 否 | 0/13 |
| 卡死 | 第 2 次 | 否 | 否 | 0/13 |
| 卡死 | 第 3 次 | 检出（附带，跨通道不一致）/ 13.0 分钟 | 否 | 0/13 |
| 气体阶跃 | 2 倍 | 是 / 60 秒 | 是 / 540 秒 | 13/13 |
| 气体爬坡 | 2 倍 | 否 | 是 / 39.0 分钟 | 33/36 |

- 每类注入的响应图：`docs/m3_inject/recall/figs/inject_NN_<类型>.png`（上为点通道每 10 分钟离群轮数，下为上下文
  通道主分，灰底为注入区间，虚线为阈值 2.22）。
- 检出边缘（裁决第二节第 3 条）：上下文通道对阶跃两倍以上可靠、一倍处于边缘；爬坡四倍 40 分钟、两倍 149 分钟检出、
  一倍未检出。结构性解释：一倍宽度的水平仍在模型学过的范围内，只是出现在不该出现的时刻，而模型没有时间输入。
- 卡死对两个检测通道按构造不可见，由数据接入的重复值游程守卫承担（开发决策第十条，`docs/reports/m1_stuck_guard_report.md`）。
- 三条预言：卡死对上下文通道可见——不成立；爬坡上下文通道早于点通道——成立（点通道在爬坡期间不报）；尖峰点通道
  即时可见、一轮尖峰上下文通道可能不可见——成立。

## 七、V-M3-6（性能报告）

- 命令（本地 Mac，仓库根目录，注入运行结束后）：

```bash
bash deploy/scripts/syn-m3-perf-collect.sh --since 2026-10-09T01:10:57Z --out-dir docs/m3_inject/perf
python3 deploy/scripts/m3_coldstart_report.py --dir docs/m3_inject --timeout-min 120
```

- 逐台训练时长（在线冷启动，注入运行，`docs/m3_inject/coldstart_report.md`；离线网格的训练时长见第 2.3 节所列报告，
  master 单线程上 C 922 秒、E 611 秒、G 452 秒）：

| 设备 | A | B | C | D | E | F | G | H |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 训练轮数 | 79 | 46 | 63 | 126 | 118 | 44 | 82 | 43 |
| 训练秒数 | 1,085.6 | 380.4 | 835.5 | 1,576.0 | 1,494.4 | 620.9 | 924.8 | 360.0 |

  八台各占一个子任务，最多 6 台同时训练；训练期间挂起最长的检查点端到端 1,627 秒，训练后完成，失败 0 次。

- 运行期测量（`docs/m3_inject/perf/perf_summary.md`）：

| 项目 | 测量值 |
| --- | --- |
| 逐窗推理时延 | 中位数 171 至 184 毫秒，P99 447 至 539 毫秒（重放积压下八个子任务同时推理时的读数，不代表实时到达时的时延；三月至四月运行只有五个子任务推理，中位数 38 至 91 毫秒） |
| 点通道转发引入的延迟 | 中位数 30 至 31 秒，最大 60 秒（由 60 秒滑动步决定） |
| TaskManager 进程内存峰值 | 3,502 MB、3,568 MB |
| JavaCPP 堆外分配峰值 | 23 MB、25 MB |

- 内存配置：本次运行 JavaCPP 物理内存上限 3,900 MB、进程总量 4,096 MB。按 2026-10-09 裁决第二节第 6 条，下一次重建
  TaskManager 时进程总量提到 5,120 MB，JavaCPP 物理内存上限等于进程总量（5,120 MB），分配上限 512 MB 不变；重建前用
  `syn-tm-recreate.sh --check-only` 核实工作节点至少剩 1 GB。

## 八、遗留事项

1. V-M3-4（正常数据上的误报率）的判定与平稳日判据是否加入宽度条件，待设计会话决定。
2. H 在 04-20、04-26 两天独自偏高，登记待查（2026-10-09 裁决第二节第 4 条）。
3. 冷启动只打标记、不重估标准化与降权告警的缺口，归适应执行与聚合阶段。
4. 训练移出任务线程（补遗四）：解决同步训练期间水位线停摆、取消响应、并发与放置问题，完成后上下文通道收口。
5. 重复值游程守卫的阈值待确认气体与光照的处理后写入配置，在线核对随下一次重放进行。
