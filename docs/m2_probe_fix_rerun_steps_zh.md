# 探针口径修正后的测试步骤，与阶段实验前的环境复位

- **版本**：1.0（2026-09-19）
- **依据**：设计会话对「探针排空尾巴」的裁决（同意修正 `M2Probe.sweep`），以及交接文档
  `handover_m2_addendum_checkpoint_peak_en.md` 中仍然成立的三条裁决。
- **前一份发现报告**：`docs/reports/m2_march_baseline_findings.md`

---

## 1. 这次改了什么

**其一，`M2Probe` 的两处滑窗循环上界。** 由 `windowEnd - windowMs <= maxArrival`（等价于
`windowStart <= maxArrival`）改为 `windowEnd <= maxArrival`，与运行中的 Flink 作业口径一致。
裁决只点名了 `sweep`，但 `writeOutlierHod`（设备 G 逐小时诊断）用的是**同一个**上界、同样会多走
一个窗长的排空段，而它正要重跑 v2，所以我一并改了并在此显式说明——**如设计会话认为不该动它，
请回退这一处**。

**其二，`ProbeDrainTailTest` 增加回归守卫。** `M2Probe.sweep` 与 `RateResult` 由 `private` 放宽为
包级可见，测试直接调用真实实现，断言其滑窗数与作业口径一致；若上界被改回去，这条断言会指出
「等于 33678 说明排空尾巴的上界被改回去了」。

**其三，落地交接文档中仍然成立的三条。** `akka.framesize` 抬到 64 MB（JM 与 TM 两侧，经
`SYN_AKKA_FRAMESIZE` 参数化）；两个作业启动时打印
`min(--checkpoint-max-state-mb, akka.framesize)` 三个数与生效者；`syn-replay-verify.sh` 增加
**断言五：整轮运行期间作业重启次数为 0**。

**其四，新增 `deploy/scripts/syn-reset-env.sh`**，见第 4 节。

**没有改的**：半径 R、k、窗口 W/S、`--calib-days`、标定统计量、任何 M3 代码路径。

---

## 2. 修正后的测试步骤

### 2.1 本地：单元测试与打包

```bash
# 执行环境：本地 Mac，仓库根目录；前置条件：JAVA_HOME 指向 JDK 11
git pull origin dev-claude
mvn -o test                       # 期望 82/82 通过（新增 ProbeDrainTailTest 三条）
mvn -o test -Dtest=ProbeDrainTailTest    # 只想看守卫这一条时
mvn -DskipTests package
bash deploy/scripts/check-jar.sh  # 期望全部标记 PASS
```

**期望产出**：`Tests run: 82, Failures: 0, Errors: 0`；`check-jar.sh` 全 PASS。
**失败兜底**：若 `realSweepUsesTheJobBound` 失败且提示「说明排空尾巴的上界被改回去了」，说明
`M2Probe` 的循环上界被还原，先修代码再继续。

### 2.2 集群：环境复位（新流程，见第 4 节）

```bash
bash deploy/scripts/syn-reset-env.sh --dry-run    # 先看现状，不改动
bash deploy/scripts/syn-reset-env.sh              # 确认后执行复位
```

**期望产出**：末尾核对表全 PASS，退出码 0。任何一项 FAIL 都不要开跑。

### 2.3 集群：使 `akka.framesize` 生效

`akka.framesize` 是容器启动参数，改了 `.env` 与 compose 之后**必须重建容器**才生效——注意是
`up -d`（compose 检测到配置变化会自行重建），不是 `restart`。

```bash
bash deploy/scripts/1-sync-to-nodes.sh   # 把新的 .env 与 compose 同步到三台节点
bash deploy/scripts/2-up-all.sh          # compose 检测到 FLINK_PROPERTIES 变化，重建 Flink 容器
ssh fa-master "curl -s localhost:8081/jobmanager/config" | python3 -m json.tool | grep -A1 framesize
```

**期望产出**：`akka.framesize` 显示 `64mb`。
**重要提醒**：重建 `jobmanager` 与 `taskmanager` 容器是安全的（它们不存数据）；但 `2-up-all.sh`
同时作用于 `kafka-*` 与 `zookeeper`，若输出里出现 `Recreating kafka-1`，**立即中止**——那会让
broker 拿到全新空卷、topic 数据归零。正常情况下这两个容器的配置没变，compose 不会重建它们。

### 2.4 重跑探针，重建参考值（顺序已更正）

**更正**：本节 1.0 版把探针写成可以在环境复位之后单独执行，这是错的。`syn-m2-probe.sh` 消费的是
**`synergia-m1-out`**（见该脚本第 17 行的前置条件与第 86 行消费的 topic），而 `syn-reset-env.sh`
会连同 `m1-out` 一起清空。因此探针**不能**在复位之后单独跑，它必须跑在一次完整重放**之后**、
`syn-clean-topics.sh` 之前——也就是折进 2.5 节第 2 轮（三月基线）里。

同时要更正 1.0 版给出的判读依据。它要求「新旧探针 CSV 对比，slides 应恰好减少 60」，但现有的旧表
`docs/m2_probe_7d_clean.csv` 是 **Java 8 运行时 + 旧口径**算出来的，而新表会是 **Java 11 运行时 +
新口径**——一次比较里变了两个变量，得出的差值无法归因。更麻烦的是，那份 Java 8 参考**无法用新口径
重算**：本项目的 jar 自 Addendum 2 起是 Java 11 字节码（class file major 55），在 JDK 8 上会直接
`UnsupportedClassVersionError`。

解决办法是把一次双变量比较拆成两次单变量比较。`M2Probe` 新增了 `--legacy-drain-tail` 开关，可以在
**同一份数据、同一个运行时**下复现旧口径，于是：

```bash
# 执行环境：本地 Mac，仓库根目录
# 前置条件：三月重放已完成，M2Job 已排空（syn-m2-metrics.sh 连续两次读数一致），topic 尚未清理
# 甲、新口径（今后的判据基准）
bash deploy/scripts/syn-m2-probe.sh --r-grid 1.0 --k-grid 10 --out-name m2_probe_corrected.csv
# 乙、旧口径（仅用于与 Java 8 参考表做同口径对照）
bash deploy/scripts/syn-m2-probe.sh --r-grid 1.0 --k-grid 10 --legacy-drain-tail \
     --out-name m2_probe_legacy.csv
```

两次比较各自只变一个变量：

| 比较 | 两侧 | 唯一变量 | 期望结果 |
| --- | --- | --- | --- |
| 比较一：运行时等值 | `m2_probe_legacy.csv`（J11+旧口径） 对 `m2_probe_7d_clean.csv`（J8+旧口径） | 运行时 | 逐设备 `meanOutlierRate` 落在 ±1% 内 |
| 比较二：口径差异 | `m2_probe_corrected.csv` 对 `m2_probe_legacy.csv` | 口径 | 逐设备 `slides` 恰好少 **60**，比率之和恰好少约 **2.00** |

**判读依据**就是上表右列这三个数：**±1%**、**恰好 60**、**约 2.00**。比较一通过，说明 Java 8 到
Java 11 的迁移在探针这条路径上是等值的；比较二的两个数与三月基线实测的差值一致，说明口径修正的
效果正如所析。设备 H 在比较二里两项都应为 0（它本就没有排空尾巴差异）。

**失败兜底**：比较二里 `slides` 的减少量若不是 60，说明窗口几何与假设不符，**立即停下**，把两份
CSV 一并贴出；比较一若超出 ±1%，那是一个真正的迁移等值问题，也要停下上报，**不要**调任何参数。

**留档**：比较一通过之后，`m2_probe_corrected.csv` 取代 `m2_probe_7d_clean.csv` 成为
`m2_device_baseline.py` 的参考基准（该脚本的 `--java8-probe` 参数指向它）。旧表保留不删，它是
Java 8 时期的历史锚点。

### 2.5 四轮重跑（顺序不可颠倒）

按交接文档的顺序执行，每一轮都以 `syn-reset-env.sh` 全 PASS 开头：

1. **M1 单日回归**——确认修正没有影响 M1 段。
2. **三月基线**：提交作业 → `syn-ckpt-watch.sh`（独立终端常驻）→ `--speedup 3600` 重放 →
   `syn-m2-metrics.sh` 连续两次读数一致 → `syn-replay-verify.sh --tol-pct 0.2`（**现在是五条
   断言**）→ **2.4 节的两次探针（新口径与旧口径）** → `syn-m2-baseline.sh --tag march`。
   注意 2.4 节的探针必须在这一步之内完成，因为它读 `synergia-m1-out`，一旦清理 topic 就没有输入了。
3. **六月 DF-12 突变** `syn-m2-surge.sh --tag v2`。
4. **设备 G 逐小时分布 v2**——注意它用的是已修正的 `writeOutlierHod`。

---

## 3. 关于交接文档里的方案 E：不应实施

交接文档 §1.1 把方案 E（把 `RobustScaler` 蓄水池从装箱的 `List<ListState<Double>>` 改为原始
`double[]`）定为主修复，§2 要求配套两个测试，§4 把它排在工作顺序第一位。**这条裁决建立在一个已被
实测推翻的前提上，不应实施。** 三条证据：

**第一，装箱不是序列化开销。** 我在本地用作业真实的状态序列化器实测：装箱形态 2,419,220 字节
（2.31 MB），改成每轮一条 `double[5]` 后是 2,661,124 字节（2.54 MB），**反而大 10%**。原因是
`DoubleSerializer` 无论装箱与否都写 8 字节，而数组形态额外付出每条数组的长度前缀。装箱的代价在
**堆内存**，不在 checkpoint 载荷。

**第二，「每条目约 91 字节」这个数是循环推导出来的。** 它是用 26.1 MB 反除以条目数得到的，再用它
去论证 26.1 MB 的来源。这正是我在这个项目里连续犯了三次的错误——从单个异常数字反推机制而不先
独立测量候选项。

**第三，三月干净重跑的实测直接否掉了前提。** `RobustScaler` 的峰值出现在标定冻结前的
checkpoint 14，八个子任务**合计** 15.76 MB，按并行度 8 平均约 1.97 MB/子任务；冻结后塌回
0.045 MB。方案 E 要解决的「蓄水池单子任务 26 MB」这一情形并不存在。

交接文档的其余三条裁决都成立，且已全部落地：方案 A（抬 framesize，纵深防护）、方案 B 被否
（不做无 checkpoint 的基线运行）、`--checkpoint-max-state-mb` 保留并说明有效上限是
`min(该参数, akka.framesize)` 且作业启动时打印。§3 的第五条断言也已实现。

**需要设计会话回一句**：确认放弃方案 E，或者说明它要解决的是我尚未识别到的另一个问题。在此之前
我不会改蓄水池的存储形态。另外 §2 要求的「载荷测试」我建议改成对**当前**装箱形态做一次断言
（实测 2.31 MB，远低于 10 MB 且现在更远低于 64 MB），作为回归守卫保留——这个测试有价值，只是
不该以更换存储形态为前提。

---

## 4. 阶段实验前的环境复位：`syn-reset-env.sh`

你的判断是对的：`--checkpoint-tolerable-failures 100` 解决不了根本问题，两轮三月基线的差别更像是
**环境状态**的差别。第一轮跑在一套积压了历次实验残留的环境上，第二轮跑在一套刚重建、Kafka 与
ZooKeeper 全新空卷、磁盘充裕的环境上，43 次 checkpoint 无一失败。

需要说明清楚的是：**我没有证据证明这个因果关系。** 两轮之间同时变了三件事（事件时间对齐修复、
checkpoint 间隔 10 s → 30 s、环境重建），一次观测分离不了三者的贡献。把「环境复位」做成固定流程
的理由不是「已证明它是根因」，而是**它把一整类干扰从今后的实验里剔除出去**——代价是每轮几分钟，
收益是下次再出问题时，候选原因里少了最难排查的一类。

```bash
# 1. 执行环境：本地 Mac（bash + python3），仓库根目录；ssh 别名 fa-master/fa-worker1/fa-worker2 可用
# 2. 调用命令：
bash deploy/scripts/syn-reset-env.sh --dry-run          # 只检查不改动，先看现状
bash deploy/scripts/syn-reset-env.sh                    # 交互确认后复位
bash deploy/scripts/syn-reset-env.sh --yes              # 跳过确认
bash deploy/scripts/syn-reset-env.sh --min-free-gb 25   # 提高磁盘门槛，默认 20
bash deploy/scripts/syn-reset-env.sh --prune-volumes    # 顺带回收孤儿卷（**不可撤销**，默认只报告）
bash deploy/scripts/syn-reset-env.sh --keep-topics      # 少数续跑场景，不清 topic
# 3. 前置条件：三台节点已开机、容器已由 2-up-all.sh 拉起；本地 target/ 下有本轮要用的 jar
# 4. 期望产出：八个阶段的进度 + 末尾核对表（每项 PASS/FAIL/WARN/SKIP）；全 PASS 退出码 0
# 5. 常见失败兜底：磁盘不达标见 docs/cluster_disk_cleanup_zh.md；容器缺失先跑 2-up-all.sh；
#    jar 不一致先 syn-upload-m1.sh --jar-only；topic 末端偏移非 0 是删除异步，等 1~2 分钟重跑复核
```

脚本做的八件事，以及每件事对应的教训：

| 阶段 | 动作 | 对应的教训 |
| --- | --- | --- |
| 1 | 六个核心容器在位检查 | 开机后曾只起了 prometheus 与 grafana，三个核心容器根本不存在 |
| 2 | 每节点可用空间 ≥ 门槛 | 曾在 master 只剩 4.6 GB 时准备开跑 |
| 3 | 停重放器、删 `.replayer.offset` | 残留的续跑位点会让下一轮从中间接上 |
| 4 | 只取消**本项目**作业 | 按作业名匹配 M1Job/M2Job/M3，其余一律不动 |
| 5 | 重启 Flink 容器 | 指标计数器是每 JVM 的，不重启就会读到跨轮累加值 |
| 6 | 清空 `synergia-*` 并复核末端偏移为 0 | Kafka 删除是异步的，脚本等待后复核而不是假定 |
| 7 | 清远端中间产物、盘点孤儿卷 | 孤儿卷是磁盘被吃光的主因；默认只报告，回收要显式加参数 |
| 8 | 镜像标签、JavaCPP 上限、`akka.framesize`、jar 的 SHA-256 一致性 | 曾在旧 jar 上跑出 PASS；曾因配置只改本地没同步到节点而白跑一轮 |

**它不做什么**：不提交作业、不启动重放、不改任何算法参数。复位完成后仍由你按 2.5 节的顺序开跑。

**建议的使用方式**：把 `bash deploy/scripts/syn-reset-env.sh --yes && <本轮第一条命令>` 串起来，
让核对表的退出码成为硬门槛——任何一项 FAIL，后面的命令根本不会执行。
