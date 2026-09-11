# M3 集群实验操作手册 / M3 Cluster Experiment Runbook

本手册给出 M3（上下文异常检测）在真实集群上每次实验的标准流程：实验前准备、实验中按序执行、实验后收尾。
适用对象是"每次跑实验前照着做一遍"的操作者。所有命令均在**本地 Mac** 的仓库根目录执行，脚本内部通过 ssh
连到集群 master（`fa-master`）再 `docker exec` 进相应容器。

> 模型说明 / Model note：M3 已按设计会决定回退为 Smile/JSAT 的重建模型（纯 JVM，Java 8）。因此本手册
> **不含**堆外内存重配（`taskmanager.memory.task.off-heap.size` 等）与 DL4J 集群冒烟步骤——那些仅在
> DL4J/ND4J 方案下才需要。若将来切回 DL4J，需另行恢复相应步骤。

---

## 共存红线（每次实验都必须遵守）/ Coexistence hard rules

1. 只操作 `synergia-` 前缀的 topic；绝不改动或删除旧 FA-iForest 的 topic。
2. 绝不 `flink cancel` 任何在跑的旧 FA-iForest 作业。
3. 绝不执行 `9-teardown.sh --purge`（会清空共享基础设施）。
4. `M1Job`、`M2Job`（含 M3）都消费 `synergia-source`，**三者不可同时运行**；提交前先确认没有另一个在跑。

---

## 阶段零：一次性准备（仅首次，或对应文件改动后重做）/ One-time setup

| 步骤 | 命令 | 何时需要重做 |
|---|---|---|
| 0.1 填写 `.env` | 复制 `deploy/env.example` 为 `deploy/.env` 并填节点 IP、SSH 密钥 | 节点/密钥变化时 |
| 0.2 起集群容器 | `bash deploy/scripts/1-sync-to-nodes.sh` 然后 `bash deploy/scripts/2-up-all.sh` | 首次；或改了 `docker-compose.*.yml` / `.env` 中集群参数时 |
| 0.3 建 topic | `bash deploy/scripts/syn-create-topics.sh` | 首次；或需新增 topic 时（幂等，可重复跑） |
| 0.4 传数据集 | `bash deploy/scripts/syn-upload-m1.sh --data-dir <本地 CSV 目录>` | 首次；数据集约 2.3GB，`rsync -P` 断点续传 |

> 注意 / Note：`2-up-all.sh` 会重建 master 与两台 worker 的容器。若旧 FA-iForest 作业正在跑，重建
> TaskManager 会导致其任务重启（从 checkpoint 恢复）。仅在可容忍旧作业短暂重启的窗口执行 0.2。

---

## 阶段一：实验前检查（每次实验都做）/ Pre-flight checklist

按顺序执行，任一步不过则停下排查，不要带病开跑。

1. **构建并自检 jar / Build & verify the jar**（本地）
   ```bash
   mvn clean package                       # 生成 target/iot-anomaly-detection-1.0-SNAPSHOT.jar
   bash deploy/scripts/check-jar.sh        # 校验 jar 含最新代码标记（全 PASS 才继续）
   ```
   - 期望产出：`BUILD SUCCESS`；`check-jar.sh` 全部 `[PASS]`。
   - 失败兜底：编译失败先修代码；`check-jar` 有 `[FAIL]` 说明 jar 是旧的，重新 `mvn clean package`。

2. **上传 jar / Upload the jar**（本地）
   ```bash
   bash deploy/scripts/syn-upload-m1.sh --jar-only
   ```
   - 期望产出：jar 落到 `/opt/fa-iforest/jars/iot-anomaly-detection-1.0-SNAPSHOT.jar`（即 jobmanager 挂载的 `/opt/flink/usrlib`）。

3. **确认 topic 就绪 / Confirm topics**（本地）
   ```bash
   bash deploy/scripts/syn-create-topics.sh    # 幂等；已存在则跳过
   ```
   - 期望产出：`synergia-source`(8 分区)、`synergia-scores`(4)、`synergia-monitoring`(1)、`synergia-m1-out`(1) 均存在。

4. **确认无冲突作业 / Confirm no conflicting job**（本地）
   ```bash
   ssh <master> "docker exec jobmanager flink list"
   ```
   - 期望：没有另一个 `M1Job`/`M2Job` 在 `RUNNING`。若有，先让其自然结束或与负责人确认，**不要 cancel 旧作业**。

5. **（仅标定/探针类实验）重放完整性核验 / Replay-integrity gate**（本地）
   ```bash
   bash deploy/scripts/syn-replay-verify.sh --expected-total <EDA 参照值>
   ```
   - 这是标定/探针运行前的固定门槛；四条断言全过才继续。冒烟或功能性小实验可跳过本步。

---

## 阶段二：实验执行（按实验类型选一条）/ Run the experiment

### 类型 A：联合作业 + 重放（M3 在线评分的主路径）

1. **提交联合作业（M1 摄取/标准化 + pMCOD + M3 重建评分）**
   ```bash
   bash deploy/scripts/syn-submit-m2.sh
   # 可选按需覆盖：--extra '--m3-window-length 60 --m3-z-threshold 2.22'
   ```
   - 期望产出：打印 `JobID` 并轮询至 `RUNNING`。M3 默认启用（`--m3-enabled true`）。

2. **启动重放（tmux 后台常驻）**
   ```bash
   bash deploy/scripts/syn-replay.sh --speedup 600 --start 2022-05-21 --end 2022-05-22
   bash deploy/scripts/syn-replay.sh status     # 查看进度
   bash deploy/scripts/syn-replay.sh attach     # 附着观看（Ctrl+B D 脱离）
   ```
   - 期望产出：`synergia-source` 持续进消息，作业开始向 `synergia-scores`/`synergia-monitoring` 写。

### 类型 B：注入实验（人工异常召回评估，交接 §4）

> 已知限制 / Known gap：注入引擎 `Injector` 已实现，但 `syn-replay.sh` 目前**尚未透传** `--inject` 参数。
> 在跑注入实验前需要先给 `syn-replay.sh` 增加 `--inject`/`--inject-log` 的透传（待办）。补上后：
> ```bash
> bash deploy/scripts/syn-replay.sh --speedup 600 --start <d> --end <d> \
>      --inject "E:Temperature:<startTs>:300:spike:10" --inject-log inject-truth.csv
> ```

---

## 阶段三：实验中监控 / Monitoring during the run

- 作业状态：`ssh <master> "docker exec jobmanager flink list"`；或浏览器开 Flink UI（JM 8081）。
- 指标：Prometheus（9090）/ Grafana（3000）看 M2/M3 自定义指标（如 `m3_online_windows`）。
- 结果流抽样：
  ```bash
  ssh <master> "docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server <brokers> \
      --topic synergia-scores --from-beginning --max-messages 20 --timeout-ms 15000"
  ```
  - M3 的记录 `channel` 字段为 `m3_context`，可据此从 `synergia-scores` 里筛出上下文评分。

---

## 阶段四：实验后收尾 / Post-experiment cleanup

1. **停重放**（重放是常驻的，必须显式停）
   ```bash
   bash deploy/scripts/syn-replay.sh stop
   ```
2. **导出/落盘结果**：把本次 `synergia-scores`/`synergia-monitoring` 需要留存的数据消费导出到本地分析目录。
3. **停本次作业**：M3 联合作业是常驻流作业，实验结束后按需停止**本次自己提交的**作业（用步骤记录的 JobID）：
   ```bash
   ssh <master> "docker exec jobmanager flink cancel <本次JobID>"
   ```
   - 只 cancel 自己这次提交的 JobID；**绝不** cancel 旧 FA-iForest 作业。
4. **清理本实验 topic 数据**（下次实验隔离，仅当需要重来时）：
   ```bash
   bash deploy/scripts/syn-clean-topics.sh --yes     # 只删+重建 synergia- 前缀 topic
   ```
   - 绝不 `9-teardown.sh --purge`。
5. **记录**：把 JobID、重放区间、参数、观察到的现象与结果路径记入实验日志，便于复现与回报。

---

## 常见失败兜底 / Failure fallbacks

| 现象 | 排查方向 |
|---|---|
| `syn-submit-m2.sh` 60s 内未 `RUNNING` | 看 `docker exec jobmanager flink list` 与 JM 日志；常见是 slot 不足（有旧作业占用）或 topic 分区数不符 |
| `synergia-scores` 无 M3 记录 | 确认 M3 已过状态机的 COLLECTING→TRAINING→ONLINE（需累积足够天数的数据）；查 TM 日志中 `[M3]` 行 |
| 重放"看似成功但 0 条落地" | topic 被自动建成 1 分区；用 `syn-clean-topics.sh --yes` 重建为 8 分区后重放 |
| 重放中断 | `syn-replay.sh status` 看状态；重放器支持 `--resume` 断点续跑 |
