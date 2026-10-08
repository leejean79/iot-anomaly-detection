# 代码阅读指引：物联网多通道异常检测（M1 / M2 / M3）

版本：2026-10-08。作者：代码开发代理。每完成一个模块或一项较大改动，本文随之更新，改动记在文末「更新记录」。
用途：在逐个阅读源码之前，先建立整体图景，知道每个文件做什么、关键代码在哪几行、为什么这样设计、读的时候该
留意什么。本文只指路，不复述代码；行号以本文写作时为准，代码改动后可能前后偏移几行。

---

## 一、一页纸概览

**系统做什么。** 八台设备（A 至 H）大约每 10 秒各上报一组读数，每组含五个检测通道（温度、湿度、气压、气体、
光照）以及若干质量通道。系统在 Apache Flink 上实时地把这些读数整理成「采样轮」，做标准化，然后用两种互补的
方法找异常：

| 模块 | 名称 | 看什么 | 方法 | 输出 |
| --- | --- | --- | --- | --- |
| M1 | 接入与标准化 | 数据本身是否完整、可用 | 解析、成轮、稳健标准化（先标定后冻结） | 标准化的轮、逐设备监测快照 |
| M2 | 点异常通道 | 单个时刻的读数组合在近一小时内是否孤立 | pMCOD（基于距离的流式离群检测） | 离群点名单、三路监测信号 |
| M3 | 上下文通道 | 一段时间（10 分钟）的形状是否反常 | 编码器—解码器 LSTM 自编码器的重构误差 | 每窗一条评分 |

**技术栈。** Java 11、Maven、Flink 1.13.6、Kafka 2.6.3、DL4J 1.0.0-M2.1（M3 的神经网络）。三台云主机：master 上是
Kafka、ZooKeeper、Flink JobManager 与 Prometheus；两台 worker 上各一个 Flink TaskManager 与一个 Kafka 节点。

**代码规模。** 主代码约 8,800 行、38 个类，分 4 个包；测试约 3,700 行、24 个测试类。另有约 40 个部署脚本与
十余个 Python 分析脚本。

---

## 二、数据流全景

```
原始 CSV（本地 Mac / master 上的数据集）
  │  CsvKafkaReplayer：全局按时间归并，按设备显式分区，按倍速放出，可注入
  ▼
Kafka: synergia-source（8 分区，一台设备一个分区）
  │
  ▼  M2Job（一个 Flink 作业，内含 M1 + M2 + M3）
  ├─ Kafka Source（事件时间 = 记录时间，55 秒有界乱序，空闲时限）
  ├─ RawLineParser ──keyBy──► RoundAssembler ──► AlignEventTime
  ├─ ──keyBy──► RobustScaler ──keyBy──► RawCache                    ← M1 标准化流
  │       ├──keyBy──► MonitoringAggregator ──► synergia-monitoring（M1 快照）
  │       └──► synergia-m1-out（可选，供离线工具与核验）
  ├─ M2Gate ──keyBy──► 滑动窗口(3600 s / 60 s, 允许迟到 0) ──► PmcodFunction   ← M2
  │       ├──► synergia-scores（离群点名单，channel = m2_point）
  │       ├─(侧)► synergia-monitoring（M2 三路信号）
  │       ├─(侧，迟到数据)► LateDropCounter（只计数）
  │       └─(侧)► AnnotatedRound ──keyBy──► M3Function                    ← M3
  │                   ├──► synergia-scores（channel = m3_context）
  │                   └─(侧)► synergia-monitoring（M3 重构误差）
  ▼
离线：syn-replay-verify.sh / M2Probe / M3Grid / Python 分析脚本读取上述主题的转储
```

总装配在 `src/main/java/com/leejean/m2/M2Job.java:60` 的 `main` 里，按「M1 段（268 行起）→ 事件时间重对齐（274 行起）
→ M2 段（326 行起）→ M3 段（359 行起）」的顺序读，就是上图从上到下的顺序。

---

## 三、贯穿全项目的设计原则（读任何一个文件之前先知道）

1. **事件时间就是数据里的时间。** 每条 Kafka 记录的时间戳是读数自身的时间（`CsvKafkaReplayer` 写入，
   `M2Job.java:259-265` 读出）。所以把三个月数据以 3600 倍速重放，作业看到的时间流与现场实时运行完全一样，结果
   与重放速度无关。这是所有「重放即验收」的前提。
2. **一个作业、算子链内传递，不经 Kafka 中转。** M3 需要 M2 对每一轮的离群判定来净化训练数据，并且三个模块必须
   共用同一条事件时间线（同一个水位线）。拆成三个作业就得经过 Kafka，事件时间的对齐会变复杂。代价是：任一模块阻塞会拖住整个作业
   （见第九节的三起事故）。
3. **先标定、后冻结，不在线自适应。** 标准化参数在前 7 天标定后冻结（`RobustScalerFunction`），M3 模型训练一次
   后冻结。漂移检测与重新标定是将来 M6 模块的职责，现在的代码只留了入口（`RobustScalerFunction.recalibrate`）。
   这样做使每个模块的行为可预期、可复现，也使「漂移」成为可被观测的对象而不是被悄悄吸收掉。
4. **忠实迁移加朴素对照，正确性可证明。** M2 的 MCOD 算法从 pMCOD 的 Scala 源码逐行迁移（`McodCore`），并配一个
   按定义暴力计算的对照器（`NaiveOutlierOracle`），测试要求两者逐窗口的离群点集合完全相等。
5. **在线与离线用同一份实现。** M3 的训练核心 `M3Training` 是纯函数，在线算子 `M3Function` 和离线网格 `M3Grid`
   调用同一份；离线探针 `M2Probe` 复用 `McodCore`。因此「在线结果与离线参照逐位相同」可以作为验收判据
   （E、C 两台已在三月至四月运行中逐位相同）。
6. **一切可计数、可核验。** 每一道过滤都有计数器（Flink 指标），每台设备每分钟有监测快照，每次运行结束有七条
   断言（`syn-replay-verify.sh`）。设计上不允许「静默丢弃」。
7. **已知限制写在代码与文档里。** 例如同步训练的代价、空闲时限 3600 秒在生产环境的副作用，都在相应代码旁注明，
   并说明何时改回。

---

## 四、包与文件地图

| 包 | 文件（行数） | 角色 |
| --- | --- | --- |
| `source` | `CsvKafkaReplayer`（718） | 把数据集按事件时间重放进 Kafka；节奏控制、空闲压缩、断点续跑、注入挂钩 |
|  | `DevicePartition`（56） | 设备到分区的显式映射 A→0 … H→7 |
|  | `Injector`（194） | 四种注入（尖峰、阶跃、爬坡、卡死），写地面真值 |
| `m1` | `Reading`（81）、`DeviceRound`（201）、`MonitoringSnapshot`（156） | 三种数据载体 |
|  | `Channels`（73）、`ChannelTransform`（52） | 通道定义与分类、光照通道的 log1p 预变换 |
|  | `RawLineParser`（124） | 一行 CSV → 读数，四类守卫 |
|  | `RoundAssembler`（207） | 同设备同时间戳的读数装成一轮，事件时间 +30 秒关闭 |
|  | `RobustScalerFunction`（325） | 每设备每通道中位数 / 四分位距，预热后冻结 |
|  | `RawCacheFunction`（121） | 原始值环形缓存，长缺席后标记冷启动 |
|  | `MonitoringAggregator`（102） | 每设备每 60 秒一份监测快照 |
|  | `DeviceKeys`（165） | 设备代理键：一台设备一个子任务（2026-10-05 新增） |
|  | `CheckpointCeiling`（61） | 检查点状态上限的解释与打印 |
|  | `M1Job`（267） | 独立的 M1 作业（已验收，现在主要用 `M2Job`） |
|  | `RoundWindow`（79） | 计数滑动窗口的纯库类，**未接入任何作业** |
| `m2` | `M2Job`（565） | **总装配**：M1 + M2 + M3 联合作业的入口 |
|  | `M2Gate`（95） | 进入 M2 前的三道闸 |
|  | `PmcodFunction`（208） | 窗口算子：把 `McodCore` 接到滑动窗口上，产出三类输出 |
|  | `McodCore`（288）、`McodState`（41）、`McodPoint`（136）、`MicroCluster`（25）、`McodDistance`（37） | MCOD 算法核与其数据结构（不依赖 Flink） |
|  | `NaiveOutlierOracle`（49） | 按定义暴力计算的对照器（只在测试中用） |
|  | `DevicePoint`（55）、`ScoreEvent`（78） | 载体 |
|  | `M2Probe`（588） | 离线 (R, k) 网格探针 |
|  | `ReplayVerify`（306） | 重放完整性核验的断言一至四 |
| `m3` | `M3Function`（680） | 上下文通道的 Flink 算子：每设备一台状态机 |
|  | `M3Training`（286） | 训练核心（纯函数），在线与离线共用 |
|  | `LstmAutoEncoder`（473） | 网络结构、训练一个 epoch、推理、序列化 |
|  | `WeightedMseLoss`（128）、`M3Scorer`（237） | 带掩码的加权误差、阈值标定与评分 |
|  | `AnnotatedRound`（61）、`M3ScoreRecord`（77） | 载体 |
|  | `M3Grid`（956） | 离线超参数网格与等值核验的参照 |
|  | `GradientNormRecorder`（192） | 梯度范数的只读记录（定梯度裁剪阈值用） |
|  | `M3ClusterSmoke`（271） | 集群冒烟作业（验证 DL4J 原生库在容器内可用） |

---

## 五、按数据流逐站阅读

每一站按「做什么 → 关键位置 → 为什么这样设计 → 读的时候留意 → 对应测试」组织。

### 5.1 数据源：`source` 包

**`CsvKafkaReplayer`**（`source/CsvKafkaReplayer.java`）

- 做什么：读取全部原始 CSV，按事件时间做全局归并，按设备写入固定分区，按倍速节奏放出。
- 关键位置：`main` 在 65 行；文件发现与嗅探在 234、298 行；按时段过滤在 135 行（注意 `row.ts > endSec`，结束时刻
  **包含在内**，两段重放时第一段要用 23:59:59 的秒数作结束）；节奏器 `Pacer` 在 469 行起，空闲压缩在 508 行附近；
  发送在 613 行（记录时间戳 = 数据时间）。
- 为什么：事件时间来自数据而不是处理时间，倍速只影响多快放完，不影响作业看到的时间流。空闲压缩把数据里几天的停机
  压成几秒处理时间，但**不改事件时间戳**，所以停机在作业眼里仍是停机。
- 留意：`--resume` 才会使用断点文件；`--inject-file` 经 `syn-replay.sh` 传入，避免分号被 shell 截断。

**`DevicePartition`**：显式映射 A→0 … H→7。默认哈希会让 A 与 F 撞到同一分区、留一个空分区，所以改为显式。
注意它决定的是 **Kafka 分区**，与 Flink 内部按键分组落到哪个子任务是两回事（后者见 `DeviceKeys`）。

**`Injector`**：四种注入直接改写原始值，因此注入会自然地经过 M1 的标准化，进入 M2、M3 的视角，与真实异常的路径
相同。规格格式见类注释；测试 `InjectorTest`（14 个用例）。

### 5.2 M1：接入与标准化（`m1` 包）

**`RawLineParser`**：不按键分组（畸形行可能取不到设备号）。四类守卫：畸形行、未知传感器、光照 65536（右删失）、
信号强度 0（缺失哨兵）。设计要点是**不静默丢弃**：可识别的读数打上标记照常下发，由下游逐设备计数进入监测快照。

**`Channels`**：五个检测通道的固定顺序（温度、湿度、气压、气体、光照），以及质量通道与未知通道的分类。全项目
所有 `double[5]` 都按这个顺序。

**`RoundAssembler`**（`m1/RoundAssembler.java`）

- 做什么：同一设备、时间戳**精确相等**的读数装成一轮；同轮重复传感器保留首值；事件时间定时器在 ts+30 秒关闭
  这一轮（38 行常量），缺通道的轮带缺失掩码照常输出。
- 关键位置：`processElement` 59 行、`onTimer` 133 行（142 行设设备号，经 `DeviceKeys.deviceOf` 还原）。
- 为什么：数据里一轮的各传感器读数时间戳完全相同，所以用精确相等成轮；30 秒是等待同轮迟到读数的宽限。
- 留意：轮只在 `onTimer` 里发出，Flink 会给它盖上**定时器的时间戳**（轮时间 + 30 秒）。这曾导致一半的轮进不了
  MCOD 状态，修复见下一条。

**事件时间重对齐 `AlignEventTime`**（`m2/M2Job.java:274-297`）

- 做什么：把每一轮的事件时间改回轮自身的时间戳。
- 为什么：上一条的 30 秒偏移使窗口分配所用的事件时间戳（定时器时间）与 MCOD 准入判据所用的轮时间戳（`arrival`）相差 30 秒，在 60 秒滑动步下，
  有一半的轮被计入窗口却从不进入 MCOD 状态（2026-09-18 发现）。代码旁的长注释完整记录了原因，值得细读。
- 留意：这里用 `forMonotonousTimestamps`，同时保留空闲判定 `withIdleness`，时限由 `--idle-wall` 决定（见 5.5）。
- 测试：`M1M2TimestampAlignmentTest`、`PmcodTimestampOffsetTest`。

**`RobustScalerFunction` 与 `ChannelTransform`**

- 做什么：每设备每通道，前 `warmupRounds` 轮（默认 7 天 × 8,640 = 60,480 轮）只收集样本，然后冻结中位数与四分位距，
  此后 xNorm = (x − 中位数) / 四分位距。预热期输出 `warmup=true`，下游一律不处理。
- 关键位置：`processElement` 133 行、`freeze` 197 行、`decideScale` 263/275 行（四分位距过小时旁路缩放）。
- 为什么：中位数与四分位距对离群值稳健；冻结使标准化不吸收漂移。光照通道先做 log1p（`ChannelTransform`），因为
  它乘性重尾、又有 65536 的删失顶格值，不变换会把距离与损失拽飞（M2 收尾诊断的结论）。
- 留意：被删失的光照值不进标定统计；「相对退化防护」默认关闭（`--relative-guard`），代码保留。
- 测试：`ChannelTransformTest`、`RelativeDegeneracyTest`、`M1PipelineTest`。

**`RawCacheFunction`**：每设备缓存最近 1,000 轮原始值；设备缺席超过「1,000 × 10 秒」后返场即标记冷启动。注释里写明
这是实现取舍。冷启动标记一路传到 M2，触发清空该设备的 MCOD 状态。

**`MonitoringAggregator` 与 `MonitoringSnapshot`**：每设备每 60 秒一份快照（轮数、不完整轮、畸形、未知传感器、
重复、删失、哨兵、预热、旁路）。同一个 `MonitoringSnapshot` 类也承载 M2 的三路信号（`m2*` 字段，`windowEnd>0`）
与 M3 的重构误差（`m3*` 字段）。区分方法：M1 快照 `windowEnd=0`；M2 快照 `m2WindowPoints>0`；M3 快照 `m3ReconError`
有值。断言七就是按这个规则只取 M2 快照。

**`M1Job`**：已验收的独立 M1 作业。现在跑验收都用 `M2Job`（它内含完整 M1 链），两者不能同时运行。

**`RoundWindow`**：计数滑动窗口的纯库类，按交接文档的要求**故意没有接入**任何作业，读的时候可以跳过。

### 5.3 M2：点异常通道（`m2` 包）

**`M2Gate`**：三道闸。预热轮不进；有缺失通道的轮不进（缺维向量上距离没有定义）；冷启动轮照常进但带标志；光照
删失的轮照常进（65536 经 log1p 后是确定的数，距离有定义）。每条规则都有计数器。类注释末尾记了一处已知简化。

**窗口配置**（`M2Job.java:336-339`）：事件时间滑动窗口，窗长 3,600 秒、滑动 60 秒，**允许迟到为 0**，迟到数据走侧
输出只计数（`LateDropCounter`，452 行，指标名 `m2_gate_late_drop`）。允许迟到为 0 是为了让结果确定、与离线探针
可比；代价是任何让数据「迟到」的机制都会直接丢数据（三月至四月运行的事故即此）。

**`PmcodFunction`**（`m2/PmcodFunction.java`，`process` 在 96 行）——M2 最值得精读的一个文件：

1. 100 行：窗口键可能是代理键，先还原设备号。这一行漏掉过，导致输出带 `F#3`、逐设备半径失效（10-05 预验证发现）。
2. 105-121 行：物化窗口内的点，同时挑出**本滑动步新到**的点（`arrival >= windowEnd − slide`）。
3. 123-131 行：读出该设备的 MCOD 状态；若本步有冷启动信号，先清空。
4. 134 行：逐设备半径 `rEff`（D 为 0.75、G 为 1.5，其余为全局 1.0）。
5. 135-137 行：调用算法核，再把状态写回（显式 `update`，对任何状态后端都正确）。
6. 141-143 行：主输出，**窗口内当前全部离群点**每个滑动步都重发一次，所以评分主题里同一轮会出现最多 60 次。
7. 146-164 行：三路监测信号——离群率、微簇占比、邻居数 P10/P50。微簇占比对「尺度漂移」敏感。
8. 168-181 行：给 M3 的侧输出——**只对本滑动步新到的轮**各发一条 `AnnotatedRound`，带「在到达的那一步是否离群」
   的判定。因此每一轮恰好送达 M3 一次，离群标记的规范定义（2026-10-02 裁决）就来自这里。

**`McodCore`**（`m2/McodCore.java`，`processSlide` 在 61 行）

- 做什么：MCOD 的一个滑动步——插入新点、统计离群、删除滑出点、拆掉缩水的微簇并重插成员。
- 为什么：从 `Pmcod.scala` 逐行迁移，行内注释标了对应的 Scala 行号，可以对照原文读。唯一的有意偏离在 186 行的
  `deletePoint`：按 id 在状态里定位点，而不是信任窗口元素上的可变字段（后者只在堆状态后端下成立）。
- 留意：两级剪枝（R/2 与 3R/2）、`safe_inlier` 短路、微簇内的点永不离群（所以离群只在 PD 中统计）。
- 配套：`McodState`（把微簇计数器并入受检查点保护的状态，修复风险 R8：恢复后编号相撞）、`McodPoint`（裁剪后的
  数据点，`id` 取轮的时间戳秒）、`MicroCluster`、`McodDistance`。
- 测试：`McodEquivalenceTest`（与朴素对照器逐窗口相等）、`McodLifecycleTest`、`McodStateRecoveryTest`。

**`NaiveOutlierOracle`**：O(n²) 按定义数邻居，只在测试里用。它是 M2 正确性的最终依据。

### 5.4 M3：上下文通道（`m3` 包）

**`M3Function`**（`m3/M3Function.java`）——每台设备一台状态机，状态值存在 99 行的 `statePhase`
（0 收集、1 训练、2 标定、3 在线）。

- `processElement`（255 行）：更新转发延迟指标，累计轮数，按相位分派。
- **收集**（`handleCollecting`，284 行）：
  - 设备 G 的光照输入置零（`zeroDeviceGLight`，675 行；G 的光照传感器有已知问题）。
  - 每满 60 轮成一个**不重叠**的窗口（10 分钟）。
  - 按已见轮数把窗口分到三段：训练（7 天）、早停（2 天）、阈值标定（7 天），每天按 8,640 轮折算（54 行
    `DEFAULT_ROUNDS_PER_DAY`；预验证时用 864 把冷启动压缩到几分钟）。
  - **训练净化**：窗口内只要有一轮被 M2 判为离群，整窗不进训练集，并逐条记日志（等值核验要用）。早停集与阈值
    标定集不做净化。
  - 三段攒齐即触发训练。
- **训练与标定**（`trainAndCalibrate`，355 行）：日志里打印子任务编号；调用 `M3Training.train`；被取消时中断异常向上
  抛出；记下 `REPORT` 类告警（触顶 epoch 上限、平台接近耐心）；模型参数序列化进 Flink 状态。
- **标定**（`calibrate`，454 行）：在阈值标定集上推理，交给 `M3Scorer` 估计中位数、四分位距与协方差。
- **在线**（`handleOnline`，499 行）：每满一窗推理一次，前向传播两侧计时（推理时延指标），计算主分与马氏距离分，
  发出 `M3ScoreRecord`。
- 为什么是同步训练：实现最简单，且训练只发生一次。代价是训练期间该子任务的线程被占住，反压拖住整个作业——这是
  第九节三起事故的共同根源。按 2026-10-05 裁决，这是最后一轮缓解，再出事就做补遗四（训练移出任务线程）。

**`M3Training`**（`m3/M3Training.java`，`train` 在 175 行）

- 纯函数：不读写 Flink 状态，不引用 Flink 类型。在线与离线必须调用同一份，否则等值核验无从成立。
- 回调 `EpochListener`（145 行定义，201 行调用）：它不是线程，而是一个回调接口。`train` 在每个 epoch 结束时，在**同一个
  线程**里同步调用一次 `onEpoch`，调用者借此得到每轮的进度：在线算子（`M3Function.java:395-405`）写日志，离线网格
  （`M3Grid.java:321-335`）打印进度与内存。这样做让 `M3Training` 本身不依赖任何日志或输出方式，保持纯函数。整个训练
  （包括这个回调）都跑在 Flink 的任务线程上，这正是训练会阻塞作业的原因。
- 训练循环：每个 epoch 训练一遍、在早停集上评估；早停集误差改善不足 1e-6 计为无改善，连续 20 个 epoch 无改善即停；
  每个 epoch 末检查线程中断（作业取消时能及时退出）；同时记录最后一次改善的 epoch 与最长平台。
- 测试：`M3TrainingTest`（含中断用例）、`M3EvalBatchTest`。

**`LstmAutoEncoder`**（`m3/LstmAutoEncoder.java`，网络在 `buildModel` 139 行）

- 结构：编码器 LSTM → 取末态 LSTM（整段窗口压成一个 c 维摘要向量）→ 重复向量（复制 L 次）→ 解码器 LSTM → 逐步
  输出层（恒等激活、带掩码的均方误差）。
- 为什么：旧结构每一步输出都能看到同一步的输入，网络可以学成恒等映射，重构任务是退化的（2026-09-22 裁决改正）。
  新结构里解码器只凭摘要向量重构，压缩比是真实的（300 个数压到 60 个）。
- 其他要点：重构目标默认取逆序（`reverseTarget`）；固定随机种子 42；Adam 学习率 0.001；按层二范数梯度裁剪阈值
  39,072——类注释写明这个阈值在实测数据上**从不触发**，是保险而不是约束。窗口长度是结构参数（重复次数），改窗长
  就改了网络。
- 序列化：`serializeModel`/`deserializeModel`（415/438 行），模型参数以字节数组存进检查点状态。

**`WeightedMseLoss`**：缺失与删失的条目权重为零；通道权重可配置；同时保留逐通道误差供马氏距离使用。

**`M3Scorer`**（`calibrate` 51 行，`score` 144 行）：主分 z =（误差 − 中位数）/ 四分位距，阈值 2.22（正态下约等于
3 个标准差）；马氏距离分只报告、不报警。

**`GradientNormRecorder`**：类注释解释了为什么挂在 `onGradientCalculation` 上——挂错钩子会量到 Adam 的更新量而不是
原始梯度。只读，不影响训练结果。

### 5.5 横切机制

**设备代理键 `DeviceKeys`**（`m1/DeviceKeys.java`，2026-10-05）

- 问题：Flink 按「键的哈希 → 键组 → 子任务」分配，八台设备的原始编号在并行度 8 时只落到 5 个子任务上（B、C、E 同在
  子任务 1；D、G 同在 7），同一子任务上的设备只能串行训练。
- 做法：代理键形如 `设备号#序号`，序号由 `generate` 搜索得到，使第 i 台恰好落到子任务 i。整条按键链路的六处分组
  （`M2Job.java` 中的 `DeviceKeys.selector`）统一使用；凡把键当设备号用的地方经 `deviceOf` 还原（`RoundAssembler`、
  `MonitoringAggregator`、`PmcodFunction`）。
- 配置：`SYN_DEVICE_SURROGATE_KEYS`，提交脚本未设时用生成表；表与并行度不匹配时作业拒绝启动
  （`M2Job.java:224-236`）。
- 测试：`DeviceKeysTest`、`DeviceKeysRuntimeTest`（在本地 Flink 运行时核对实际落位）、
  `PmcodM3FlagEquivalenceTest.surrogateKeysKeepDeviceIdsAndPerDeviceRadius`。

**空闲时限**（`M2Job.java:72-78`）：默认 3,600 秒。数据源与 `AlignEventTime` 两处使用。时限太短时，被同步训练阻塞的
路径会被标为空闲，水位线越过它，释放后的积压在允许迟到为 0 的窗口处被全部丢弃（三月至四月运行丢了 615,474 轮）。
注释写明生产环境的副作用（真正离线的设备会拖住全机队窗口一小时）与改回 10 秒的条件。

**检查点**（`M2Job.java:237-245`，`CheckpointCeiling`）：集群没有共享文件系统，用 `JobManagerCheckpointStorage`，状态随
确认消息送回 JobManager。所以真正的上限是 `--checkpoint-max-state-mb` 与集群 `akka.framesize` 中较小的一个；
JobManager 堆内存也要装得下。代码里的默认值（间隔 10 秒、超时 10 分钟、容忍 0 次）与验收运行实际传入的值（30 秒、
120 分钟、3 次）不同，以运行手册为准。

**指标**：Flink 自定义计数器（`m1_*`、`m2_*`、`m3_*`）经 Prometheus 采集。2026-10-02 新增：`m3_inference_latency_ms`
与 `m3_forward_delay_sec` 两个直方图、两个 JavaCPP 内存仪表（`M3Function.java:240-251`）。

---

## 六、离线工具（同一套算法，离线复用）

| 工具 | 输入 | 做什么 | 为什么存在 |
| --- | --- | --- | --- |
| `M2Probe` | `synergia-m1-out` 转储 | 对 (R, k) 网格复用 `McodCore` 逐设备算离群率 | 选半径与邻居阈值；作为点通道 ±1% 比较的参照 |
| `M3Grid` | `m1-out` 与 `scores` 转储 | 对超参数网格复用 `M3Training` 训练，输出早停误差、剔除清单 | 选型；在线训练的等值核验参照 |
| `ReplayVerify` | `m1-out` 转储 | 断言一至四（轮数、重复、边界、冻结日） | 每次运行的完整性门槛；断言五至七在 `syn-replay-verify.sh` 里 |
| `M3ClusterSmoke` | 无 | 在每个子任务内加载 DL4J 原生库、读堆外内存 | 证明原生库与堆外预算在真实容器内可用 |

`M3Grid` 的类注释解释了离群标记为什么要从 `scores` 转储还原，以及「到达滑动步」的规范定义，读等值核验之前应先读它。

---

## 七、测试地图：每组测试证明什么

| 测试 | 证明什么 |
| --- | --- |
| `McodEquivalenceTest` | MCOD 增量算法与朴素对照器逐窗口离群集合完全相等 |
| `McodLifecycleTest`、`McodStateRecoveryTest` | 微簇的生命周期正确；从检查点恢复后计数器延续、编号不撞 |
| `PmcodM3FlagEquivalenceTest` | 打开 M3 侧输出不改变 M2 的任何产出；按代理键分组与按原设备号分组输出逐条相同 |
| `PmcodTimestampOffsetTest`、`M1M2TimestampAlignmentTest` | 钉住 30 秒事件时间戳偏移的修复 |
| `M1PipelineTest`、`ChannelTransformTest`、`RelativeDegeneracyTest` | M1 管线与标准化的行为 |
| `ReplayVerifyTest` | 一份干净数据四条断言全过；逐一注入故障时各自报错 |
| `M3StateMachineTest` | 状态机跃迁、设备隔离、检查点恢复后模型仍可用 |
| `M3CoreTest`、`M3EvalBatchTest`、`M3TrainingTest` | 损失掩码、评分标定、序列化、批推理一致、早停记录、可中断 |
| `M3GridTest` | 离线网格的口径与在线一致（含离群标记只取到达滑动步） |
| `DL4JCompatSmokeTest` | DL4J 能在 Flink MiniCluster 内训练与推理 |
| `DeviceKeysTest`、`DeviceKeysRuntimeTest` | 八台设备落八个子任务，计算与运行时一致 |
| `InjectorTest`、`ReplayerLogicTest`、`DevicePartitionTest` | 注入、归并顺序、空闲压缩记账、分区映射 |

运行方式：`JAVA_HOME=<JDK 11> mvn test`，或 `mvn test -Dtest=类名#方法名` 跑单个。

---

## 八、部署与运维层速览

- `deploy/compose/`：master 与 worker 的容器编排。TaskManager 设 `OMP_NUM_THREADS=1`（训练单线程，结果可复现）；
  两侧 `akka.framesize` 为 256 MB；JobManager 挂载检查点调试日志配置。
- `deploy/env.example`：全部可配置项与注释，包括逐设备半径 `SYN_M2_R_PER_DEVICE` 与代理键表。
- `deploy/scripts/`，按用途分组：
  - 生命周期：`syn-upload-m1.sh`、`syn-submit-m2.sh`、`syn-replay.sh`、`syn-reset-env.sh`、`check-jar.sh`、`refresh-ips.sh`。
  - 运行中监测：`syn-m3-watch.sh`（在 master 上无人值守）、`syn-m2-metrics.sh`、`syn-m3-diag.sh`。
  - 运行后核验与收集：`syn-replay-verify.sh`（七条断言）、`syn-m3-march-collect.sh`、`syn-m3-perf-collect.sh`、
    `syn-m2-late-drop-history.sh`、`syn-m3-after-reboot.sh`。
  - 离线参照：`syn-m2-probe.sh`、`syn-m2-baseline.sh`、`syn-m3-grid.sh`。
  - 报告生成（Python）：`m3_coldstart_report.py`、`m3_v34_report.py`、`m3_dual_channel_week.py`。
- `eda/`：探索性数据分析与只读原始 CSV 的工具（`count_rounds.py`、`daily_channel_profile.py`、`make_injection_specs.py`、
  `m3_window_gap_check.py`），只能在存有原始数据的本地 Mac 上运行。

每个脚本的文件头都有「执行环境、调用命令、前置条件、期望产出、失败兜底」五要素，可以当作脚本的使用说明读。

---

## 九、关键参数一览

| 参数 | 值 | 定义处 | 来由 |
| --- | --- | --- | --- |
| 标准化预热 | 7 天（60,480 轮） | `M2Job.java:85-90` | 覆盖完整的周内周期 |
| 窗长 / 滑动 | 3,600 秒 / 60 秒 | `M2Job.java:115-116` | 交接文档 |
| MCOD 半径 / 邻居阈值 | R=1.0（D 0.75、G 1.5）/ k=10 | `M2Job.java:117-122`、`.env` | M2 校准与收尾裁决 |
| 允许迟到 | 0 | `M2Job.java:338` | 结果确定、可与离线比较 |
| 空闲时限 | 3,600 秒 | `M2Job.java:78` | 2026-10-05 裁决（补遗四后改回 10 秒） |
| M3 分段 | 训练 7 天 / 早停 2 天 / 阈值标定 7 天 | `M2Job.java:126-132` | 2026-10-02 裁决把标定从 2 天改为 7 天 |
| M3 窗长 | 60 轮（10 分钟），不重叠 | `M2Job.java:133` | 离线网格选定 |
| 隐单元 / 批大小 / 学习率 | 60 / 64 / 0.001 | `M2Job.java:146-165` | 网格与诊断裁决 |
| 梯度裁剪 | 39,072（实测从不触发） | `M2Job.java:171` | 批 64 时逐层 P99.9 的三倍 |
| 早停 | 最多 300 epoch，耐心 20 | `M2Job.java:139-143` | 实测最长平台 15 |
| 报警阈值 | 主分 ≥ 2.22 | `M2Job.java:134` | 约 3 个标准差 |

---

## 十、已知限制与历史教训（读代码时会看到它们留下的痕迹）

1. **同步训练**（`M3Function`）：已引发三起事故——第一次三月运行的 JobManager 内存耗尽与检查点超帧；三月重跑的
   D、G 缺 10.5 小时；三月至四月运行的 D、G 丢 615,474 轮。现有缓解：120 分钟检查点超时、3,600 秒空闲时限、一台
   设备一个子任务、两段重放、可中断训练。下一次再出事即做补遗四。
2. **30 秒事件时间戳偏移**（`AlignEventTime` 的由来）：修复前一半的轮从未进入 MCOD 状态。
3. **风险 R8**（`McodState`）：原文的微簇计数器不在状态里，恢复后编号相撞。
4. **光照通道重尾**（`ChannelTransform`）：log1p 之前，C、D 的点通道过度活跃几乎全部来自光照。
5. **检查点上限是 `akka.framesize`**（`CheckpointCeiling`）：多次排查方向错在 `--checkpoint-max-state-mb`。
6. **窗口键当设备号**（`PmcodFunction.java:100`）：引入代理键时漏改，预验证的断言七抓到。

---

## 十一、已更正的过时注释（2026-10-06）

下列注释曾与当前代码不一致，已于 2026-10-06 更正：

| 位置 | 原注释 | 更正为 |
| --- | --- | --- |
| `M3Function` 类注释 | 阈值标定集 2 天；早停集选最佳隐单元数 | 7 天（2026-10-02 裁决起）；隐单元数固定为 60，早停集只决定训练到第几个 epoch |
| `LstmAutoEncoder` 学习率字段注释 | 默认仍是 0.01 | 诊断期间默认 0.01，诊断后改为 0.001 |
| `CsvKafkaReplayer`、`Injector`、`M3ClusterSmoke` | JDK 8、门槛 Java 8、java8 镜像 | JDK 11、门槛 Java 11、镜像 `fa-iforest/flink:1.13.6-java11` |
| `M1Job` 算子链注释 | 预热 8,640 轮 | 预热 `--calib-days` 天，默认 7 天 60,480 轮 |
| `M2Job` 类注释的流程图 | 缺 `AlignEventTime`、迟到计数与 M3 段 | 补全，并注明所有按设备分组都用代理键 |
| `pom.xml` 文件头 | JDK 8；src 为空包架子 | JDK 11；已含全部代码 |

`CLAUDE.md` 中的语言版本已于同日经用户同意改为 Java 11。

## 十二、建议的阅读路线

**路线一：两小时建立全局。**
`M2Job.main`（60-398 行，只看装配）→ `PmcodFunction.process`（96-185 行）→ `M3Function` 的四个相位方法
（255、284、355、499 行）→ `M3Training.train`（175-253 行）→ `syn-replay-verify.sh` 的七条断言说明（文件头）。

**路线二：M2 点异常通道深入。**
`M2Gate` → `PmcodFunction` → `McodCore.processSlide` 与 `insertPoint`/`deletePoint`（对照 `Pmcod.scala`）→
`McodState`、`McodPoint` → `NaiveOutlierOracle` 与 `McodEquivalenceTest` → `M2Probe.sweep`（488 行）。

**路线三：M3 上下文通道深入。**
`AnnotatedRound` → `M3Function` 全文 → `M3Training` → `LstmAutoEncoder`（类注释 + `buildModel` + `trainEpoch`）→
`WeightedMseLoss` → `M3Scorer` → `M3Grid` 类注释与 `readOutlierKeys`（583 行）→ `M3StateMachineTest`、`M3GridTest`。

**路线四：时间与可靠性。**
`CsvKafkaReplayer` 的节奏器 → `RoundAssembler.onTimer` → `M2Job.java:274-297` 的长注释 → 空闲时限与 `DeviceKeys` →
`CheckpointCeiling` → `docs/reports/m3_marapr_late_drop_incident.md`（一次完整的事故推理）。

读的过程中有任何一处与本文对不上，或者想知道某个设计当时的裁决原文，告诉代码开发代理即可。

---

## 更新记录

| 日期 | 改动 |
| --- | --- |
| 2026-10-06 | 初版，对应提交 `ce05467` 之后的代码 |
| 2026-10-06 | 过时注释已更正（第十一节）；时间用语统一为 Flink 术语「事件时间」「处理时间」 |
| 2026-10-08 | 在 5.4 节 `M3Training` 处补充 `EpochListener` 的说明（回调，不是线程）；新增注入专题文档 `docs/injection_guide_zh.md`，涵盖注入策略、代码阅读要点与使用方法 |
