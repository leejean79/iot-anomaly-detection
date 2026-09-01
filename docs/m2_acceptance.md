# M2 阶段验收 — pMCOD 点异常检测（Scala→Java 迁移）

> 交接文档 §8 分两批验收。第一批（功能，离线/单日）现已可完全交付；第二批（依据与压力）需在活集群上
> 由操作者执行，本文件给出命令与填写表。所有结论用通俗完整中文，附命令与输出摘录。

## 总体状态

| 验收 | 内容 | 状态 |
|---|---|---|
| **V-M2-1** | 第六节全部等价性与生命周期测试通过（`mvn test` 全绿） | ✅ **PASS**（26 测试全绿，见下） |
| **V-M2-2** | 集群联合作业运行 + 计数器对账 + scores/monitoring 产出 | ✅ **PASS**（对账逐字闭合、scores>0、监测含 M2 信号；详见下） |
| **V-M2-3** | (R,k) 校准探针表交付（含通俗解读段） | ⏳ 待活集群执行（工具已备，离线已自测） |
| **V-M2-4** | 一个月段 k=3600 压测（吞吐/反压/checkpoint/MCOD 状态规模）+ DF-12 段观察 | ⏳ 待活集群执行 |

**迁移忠实性**：`Pmcod.scala` / `Data.scala` / `Utils.scala` 三文件逐行对照迁移到
`com.leejean.m2.{McodCore, McodPoint, McodDistance, MicroCluster}`，注释标注了对应原文行号段与
**两处经批准的差异**（id→long；mc_counter 并入受 checkpoint 保护的 `McodState`，修复 R8）以及一处
**稳健性改动**（`deletePoint` 按 id 在状态中定位，不依赖窗口缓冲区的对象同一性，对任意状态后端正确）。

---

## V-M2-1 — 功能正确性（离线，已 PASS）

```bash
mvn test    # 26 测试全绿（含 M1 的 17 + M2 的 9）
```

M2 测试（`src/test/java/com/leejean/m2/`）：

| 测试 | 覆盖（交接文档 §6） |
|---|---|
| `McodEquivalenceTest.randomStreamEquivalenceAcrossGrid` | §6.1 随机流等价：R∈{0.5,1,2}×k∈{5,10,20}×种子{1,2,3}，逐滑动步断言 MCOD 与朴素对照器**离群 id 集合完全相等**，并断言窗口成员集合一致 |
| `McodEquivalenceTest.realisticRandomWalkEquivalence` | §6.2 真实数据代理：五维平滑随机游走 + 周期尖峰（贴近标准化 xNorm），同样逐步对拍。整段真数据等价在 V-M2-2 核验 |
| `McodEquivalenceTest.denseThenSparseCreatesAndDissolvesMicroClusters` | 稠密段建微簇、稀疏段解体，全程对拍 |
| `McodEquivalenceTest.deviceIsolation` | §6.4 设备隔离：两设备独立核各自对拍通过（状态互不串扰） |
| `McodLifecycleTest`（4 项） | §6.3 微簇创建、缩水删除与成员重插、safe_inlier 短路、R/2 与 3R/2 两级剪枝分支各自触发 |
| `McodStateRecoveryTest` | §6.5 checkpoint 恢复：`McodState` 序列化往返后 **mcCounter 延续不归零、新簇编号不撞**（R8 直验） |

> **等价性是精确性的铁证**：MCOD 是精确算法，与 O(n²) 暴力对照器在每个窗口的离群点集合必须逐一相等。
> 跨参数网格、跨数据形态、跨种子均相等，即证移植忠实且无剪枝错误。

---

## 0. 前置条件（活集群，V-M2-2/-3/-4 之前）

```bash
# (0) 确认 deploy/.env 的 SYN_RETENTION_MS=-1（否则输出被事件时间戳 + 时间保留秒删；见 cluster_runbook.md）
#     并确认 SYN_EXTRA_TOPICS 含 synergia-scores:4（M2 启用该 topic）。
# (1) 重建 topic（scores/monitoring/source 就位；source=8 分区、scores=4 分区、retention=-1）
bash deploy/scripts/syn-clean-topics.sh --yes
# (2) 打包并上传 jar（含 M2Job/M2Probe）
mvn clean package
bash deploy/scripts/syn-upload-m1.sh --jar-only
```

> **关键运行规则**：`M1Job` 与 `M2Job` **不可同时运行**（都消费 `synergia-source`）。M2 联合作业
> （`M2Job`）已在内部复用 M1 全部算子并额外产出 M1 监测快照，所以做 M2 验收时**只提交 `M2Job`**。

---

## V-M2-2 — 集群单日联合作业 + 计数器对账

```bash
# 先提交 M2 联合作业（先作业后重放；若有 M1Job 在跑，先 cancel 自己的那个）
bash deploy/scripts/syn-submit-m2.sh
# 再重放单日（k=600）
bash deploy/scripts/syn-replay.sh --speedup 600 --start 2022-05-21 --end 2022-05-22
```

> **预热与单日的重要关系（务必先读）**：RobustScaler 预热 = **8640 轮/设备 ≈ 正好 1 天**。**只重放单日
> 时整天都在预热期**，`M2Gate` 会把所有 warmup 轮丢弃 → M2 admit≈0、`synergia-scores` 为空、监测里
> `m2WindowPoints=0`——**这是预期，不是故障**。要看到 M2 真正产出离群名单，二选一：
> (A) 重放 **≥1.x 天**（如 `--start 2022-05-21 --end 2022-05-23`），预热跑满冻结后 M2 才 admit；
> (B) **功能验证**用临时小预热：`bash deploy/scripts/syn-submit-m2.sh --extra '--warmup-rounds 600'`
> （600 轮≈100 分钟即冻结，单日就能大量 admit、产出 scores）。计数器对账在两种情形下都成立。

**对账式核验（计数器闭合，§8 第一批）**：M2 处理的轮数应满足
`admitted = M1 产出轮数 − warmup 旁路 − 缺失掩码旁路`。**一键拉取计数器并对账**（本地跑，直连 Flink REST）：

```bash
bash deploy/scripts/syn-m2-metrics.sh          # 自动找 RUNNING 的 M2Job，打印计数器 + 对账行
```

或从 Flink UI 各算子 Metrics 手动读累计计数器：

| 计数器（Flink 指标） | 含义 |
|---|---|
| `m2_gate_admitted` | 进入 M2 的轮数 |
| `m2_gate_warmup_bypass` | warmup 旁路数 |
| `m2_gate_missing_bypass` | 缺失掩码旁路数 |
| `m2_gate_coldstart_clear` | 冷启动清空次数 |
| `m2_gate_censored_entered` | 删失轮进入数（照常进入） |
| `m2_gate_late_drop` | 迟到丢弃数 |
| `m2_outliers_total` / `m2_points_total` | 累计离群点数 / 窗口内点观测数 |

| item | value |
|---|---|
| 选定段 | 2022-05-21 起（默认预热 8640/设备）|
| M1 产出轮数（`m1_assembler_rounds_total`）| 114,581（取样时刻）|
| warmup 旁路 / 缺失旁路 | 60,480 / 123 |
| **对账**：admitted == rounds − warmup − missing ? | **admitted 53,978 == 114,581 − 60,480 − 123 = 53,978 → 闭合 OK**；交叉核对 `m1_scaler_warmup_rounds(60,480) == m2_gate_warmup_bypass(60,480)` 一致 |
| `synergia-scores` 收到名单消息数 | > 0（如取样时 1,539；随重放继续增长）|
| monitoring 快照含 M2 三路信号与计数? | 是——预热冻结后产出。实测样例（2022-05-23 某窗，多设备）：`m2WindowPoints=176~180, m2McOccupancy=1.0, m2OutlierRate=0.0, m2NeighborCountP10/P50=0.0`，四字段自洽（占用率 1.0→PD 空→邻居百分位 0、无离群）。用 `--timeout-ms 20000` + `grep -v '"windowEnd":0'` 过滤（M1 快照 windowEnd 恒 0）|
| **verdict** | **PASS**（计数器逐字闭合、scores 产出、监测含 M2 信号）|

> **口径提醒**：计数器为**实时累计值**，具体数字随取样时刻变化；关键是**闭合关系**成立。用
> `bash deploy/scripts/syn-m2-metrics.sh` 一键拉取并自动对账。功能验证曾用 `--warmup-rounds 600`
> 得 scores≈110 万（小预热标准化粗糙、离群暴增，属假象）；默认 8640 预热校准良好，scores 回落到合理量级。
> 最终离群率是否合理由 V-M2-3 探针校准 (R,k)。

```bash
# scores 落盘计数
ssh fa-master "docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list <brokers> --topic synergia-scores" | awk -F: '{s+=$3} END{print \"scores:\", s}'
# 抽看 M2 快照（用 --timeout-ms 而非 --max-messages：后者若超过实际消息数会一直挂起、tail 永不输出）
ssh fa-master "docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server <brokers> \
  --topic synergia-monitoring --from-beginning --timeout-ms 20000" \
  | grep -v '"windowEnd":0' | grep m2OutlierRate | tail -5   # windowEnd≠0 即 M2 快照（M1 快照恒 0）
```

> `<brokers>` = `172.16.0.162:9092,172.16.0.163:9092,172.16.0.164:9092`。

---

## V-M2-3 — (R,k) 校准探针表

探针为**离线**网格扫描，复用 `McodCore`（不复制算法）。前提：先用 `M1Job` 把目标月份
（建议 **2022-03**，EDA 逐月漂移相对平稳）重放进 `synergia-m1-out`，再跑探针。

```bash
# 先让 m1-out 含 2022-03（提交 M1Job → 重放三月）
bash deploy/scripts/syn-submit-m1.sh
bash deploy/scripts/syn-replay.sh --speedup 3600 --start 2022-03-01 --end 2022-04-01
# 跑探针（网格 R∈{0.5..3.0}×k∈{5,10,20}），产出 CSV + 通俗解读
bash deploy/scripts/syn-m2-probe.sh --max-messages 2000000
```

产出 `docs/m2_probe.csv`（列：`device,R,k,slides,meanWindowPoints,meanOutlierRate,fracZeroSlides`）
与 stdout 解读段（哪些组合离群率过高、哪些恒为零）。**本阶段不定 (R,k) 终值——表格交回设计会话裁决。**

| item | value |
|---|---|
| 探针段 | _paste_（建议 2022-03）|
| CSV 交付 | _paste 路径_ |
| 解读：过高组合 / 恒零组合 | _paste_ |
| **verdict** | _交付即 PASS（不定终值）_ |

---

## V-M2-4 — 一个月段压测

```bash
bash deploy/scripts/syn-submit-m2.sh
bash deploy/scripts/syn-replay.sh --speedup 3600 --start 2022-03-01 --end 2022-04-01
bash deploy/scripts/syn-replay.sh status
```

| item | value |
|---|---|
| throughput (records/s) | _paste_ |
| max backpressure | _paste_ |
| checkpoint durations (p50/p99) | _paste_ |
| **MCOD 状态规模随窗口** | _paste_（Flink UI Checkpoints → Pmcod 算子 State Size；重点：滑动窗口缓冲 + McodState 随点数增长）|
| **DF-12 停机段跨越行为** | _paste 一段：冷启动清空计数、恢复浪涌时的离群率/状态是否失控_ |
| **verdict** | _PASS / FAIL_ |

> **状态规模提示**：pMCOD 用事件时间滑动窗口（W=3600s/S=60s，60 个重叠窗格），窗口缓冲 + 每设备
> `McodState`（PD/MC 哈希表）会随窗口内点数增长。这是所移植设计的固有特性；若状态过大，可在设计会话
> 讨论调 W/S 或换增量窗口实现（属后续，不在本阶段）。

---

## 交回设计会话的实现取舍（§9）

- **冷启动 = 清空重来**：冷启动轮触发 `McodState` 清空后照常处理（完整状态重建回填属 M6）。若某轮
  同时冷启动且缺失掩码非空，按缺失丢弃、该次显式清空信号丢失，但陈旧点会随滑动窗口自然淘汰（最多 W
  内自愈），不影响正确性——已在 `M2Gate` 注明。
- **三路信号归属**：`m2_neighbor_count_p10/p50` 按 **PD 点**的"R 内邻居数"（count_after + 窗口内
  有效 nn_before）取百分位（MC 内点按构造恒 ≥k 邻居、不追踪该计数）；`m2_mc_occupancy`、
  `m2_outlier_rate` 分母为窗口内总点数（PD + 所有微簇成员）。若设计意图不同请指正。
- **监测并入方式**：M2 按滑动步产出独立的 `MonitoringSnapshot`（M2 字段有值、M1 字段为 0）并入
  `synergia-monitoring`，与 M1 的 60s 快照共存于同一 topic（§5.2「字段追加、不另建 topic」）。
- 以上及任何与交接文档冲突的实测，按 §9 如实交回设计会话，不自行改写方案。
