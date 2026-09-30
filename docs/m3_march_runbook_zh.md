# 第二批在线验收：三月重放操作手册（2026-09-30）

依据：2026-09-28 裁决书《拷贝段的用途、第二批在线验收的数据段与相关订正》第三节。一次三月重放同时承载：
上下文通道的在线验收（V-M3-4 误报率、V-M3-5 注入召回、V-M3-6 性能与等值核验）、点异常通道探针等值
核验（挂账项）、逐子任务检查点峰值测量。另做三次离线训练作等值核验的参照。

| 时段（2022 年 3 月，UTC） | 用途 |
| --- | --- |
| 03-01 至 03-08 | M1 预热与标定（7 天） |
| 03-08 至 03-15 | M3 训练集（7 天） |
| 03-15 至 03-17 | M3 早停集（2 天） |
| 03-17 至 03-19 | M3 阈值校准（2 天） |
| 03-19 至 03-26 | 在线；**留出正常周**，供 V-M3-4 |
| 03-26 至 03-31 | 在线；**注入实验窗口**，设备 E，供 V-M3-5 |

M3 的分段按「轮数」而不是日历：各设备的训练在攒满 (7 + 2 + 2) × 8,640 = 95,040 个可用轮后触发，
缺轮较多的设备（例如 B）会比日历晚一些进入在线。

---

## 零、开跑前须确认的四件事

以下四项在开跑前必须确认；其中第一项与第三项需要设计会话的答复，第二项需要用户同意，第四项关系到
验收的判读方式。

### 0.1 冷启动训练期间的 checkpoint 容忍（需设计会话确认）

2026-09-21 起报告过、至今没有裁决的问题：M3 训练在任务线程内同步执行，每台设备约 10 至 15 分钟
（离线网格实测；在集群上多台同时训练会更慢）。其间该子任务无法响应 checkpoint；checkpoint 超时是
Flink 默认的 10 分钟，作业默认容忍失败 0 次，因此**按默认参数，作业会在训练约 10 分钟后重启，
恢复后再次触发训练，永远训练不完**（`docs/reports/m3_online_coldstart_blocking_for_decision.md`）。

本手册的提交命令带上 `--checkpoint-tolerable-failures 100`，即当时报告中的「选项二」。它不改代码，
只让超时的 checkpoint 不导致重启；代价是训练期间作业无法完成 checkpoint，其间若发生任何故障，
会丢掉训练开始以来的全部状态。DF-12 浪涌分析那一轮也用过这个参数。**若设计会话不同意，本次重放无法
完成在线验收。**

### 0.2 TaskManager 的 OpenMP 线程数（需用户同意，会重建两个 TaskManager 容器）

TaskManager 容器没有设置 `OMP_NUM_THREADS`，ND4J 会按全部核心开线程。离线实测表明这在小矩阵上反而
大幅变慢（本地一次小批量 64 的前向传播，默认线程 3,804 毫秒，单线程 157 毫秒；集群两线程比单线程慢
2.42 倍）。而本作业每台 TaskManager 最多有 4 个子任务同时训练。

建议在两台 worker 的 `taskmanager` 服务环境变量里加 `OMP_NUM_THREADS=1`，然后**只重建 taskmanager
服务**。它不触碰 Kafka 容器，Kafka 数据不受影响；此时处在阶段之间，也符合「阶段进行中不重建容器」
的约定。具体做法：

```bash
# 在 deploy/compose/docker-compose.worker.yml 的 taskmanager 服务 environment 下加一行：
#     - OMP_NUM_THREADS=1
# 然后同步到两台 worker 并只重建 taskmanager（BROKER_ID 与 NODE_SELF_IP 按 compose 文件头的说明）：
bash deploy/scripts/1-sync-to-nodes.sh
ssh fa-worker1 "cd /opt/fa-iforest/compose && BROKER_ID=2 NODE_SELF_IP=<worker1 内网 IP> \
    docker compose -f docker-compose.worker.yml --env-file ../.env up -d --no-deps taskmanager"
ssh fa-worker2 "cd /opt/fa-iforest/compose && BROKER_ID=3 NODE_SELF_IP=<worker2 内网 IP> \
    docker compose -f docker-compose.worker.yml --env-file ../.env up -d --no-deps taskmanager"
ssh fa-worker1 "docker exec taskmanager-2 printenv OMP_NUM_THREADS"   # 应输出 1
ssh fa-worker2 "docker exec taskmanager-3 printenv OMP_NUM_THREADS"   # 应输出 1
```

若不改，训练仍能完成，但 V-M3-6 量出的训练耗时会偏大，训练阻塞任务线程的时间也会更长。

### 0.3 注入方案（需设计会话确认）

交接文档只规定「设备 E，四种类型 × 三档强度」，没有规定通道、强度单位、持续时间与排期。本手册的默认
方案（由 `eda/make_injection_specs.py` 生成）：

| 项目 | 默认值 | 理由 |
| --- | --- | --- |
| 通道 | Temperature | 连续、M1 不做变换、E 为「在带内」的代表设备 |
| 强度单位 | 该通道在 M1 标定期（03-01 至 03-08）原始值 IQR 的 1、3、6 倍 | 重放器把幅度直接加到原始值上，用 IQR 为单位才能跨设备、跨通道解读 |
| spike | 持续 30 秒（3 轮） | 交接文档「一轮或几轮」 |
| step | 持续 2 小时 | 覆盖 12 个 10 分钟窗口 |
| ramp | 持续 6 小时，结束时达到满幅 | 缓慢变化 |
| stuck | 没有「幅度」，三档改为冻结 30 分钟、2 小时、6 小时 | 冻结值取注入开始时的读数 |
| 排期 | 每条占一个 10 小时时段，从时段开头 2 小时处开始 | 12 条恰好铺满 03-26 至 03-31，前后留出恢复间隔 |

### 0.4 注入与点异常通道等值核验的冲突（请设计会话知情）

裁决书把点异常通道的探针等值核验（±1%）放进同一次重放。但注入发生在设备 E 上，也会改变 E 在
03-26 至 03-31 的点异常离群率，而等值参照（`docs/m2_probe_7d_clean.csv`）来自未注入的数据。因此
**本次运行中 E 的 ±1% 比较会被注入污染**。可选的处置：E 不参与本次比较；或把 E 的比较限定在 03-26
之前（`syn-m2-baseline.sh` 目前不支持按时段截取，需要改脚本）。其余七台设备不受影响。

---

## 一、准备（本地 Mac，仓库根目录）

```bash
git pull --rebase origin dev-claude
mvn clean package -DskipTests
bash deploy/scripts/syn-upload-m1.sh --jar-only
bash deploy/scripts/check-jar.sh
```

期望：`check-jar.sh` 全部 PASS。

## 二、生成注入规格（本地 Mac）

```bash
python3 eda/make_injection_specs.py \
    --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
    --out docs/m3_march_injection_plan.csv
git add docs/m3_march_injection_plan.csv docs/m3_march_inject_spec.txt
git commit -m "docs(m3): 三月注入计划" && git push origin dev-claude
```

期望：终端打印 E 的 Temperature 在标定期的中位数与 IQR、12 条注入的计划表；生成
`docs/m3_march_injection_plan.csv` 与 `docs/m3_march_inject_spec.txt`。注入的开始时刻都落在 03-26
至 03-31 之内。

## 三、复位与开跑门槛（本地 Mac）

```bash
bash deploy/scripts/syn-reset-env.sh
```

期望：核对表全部 PASS，退出码 0。这一步会清空 Kafka 里现有的六月段数据（六月段已登记为文件，且不再
使用）。任何一项 FAIL 都先处理，不要往下走。

## 四、先提交作业（本地 Mac）

参与等值核验的参数一律显式传入，不依赖默认值。`--extra` 可以分行书写：`syn-submit-m2.sh` 自
2026-09-30 起会把其中的换行压成空格（此前换行会让远端命令提前结束，之后的参数全部丢失、作业带着默认值
悄悄运行）。

```bash
bash deploy/scripts/syn-submit-m2.sh --extra '--m3-enabled true --window-sec 3600
    --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 2 --m3-window-length 60
    --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0
    --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true
    --checkpoint-tolerable-failures 100' 2>&1 | tee /tmp/submit_m3.log
grep -E 'W=|Window W/S|M3 |Ckpt tolerable' /tmp/submit_m3.log
```

期望：`W=3600s`、`M3 enabled: true`、`M3 train/es/th: 7d/2d/2d`、`M3 window: 60 rounds`、
`M3 max epochs: 300 (patience=20)`、`M3 hidden size: 60`、`M3 batch size: 64`、
`Ckpt tolerable failures: 100`。然后确认恰好一个本项目作业在运行：

```bash
ssh fa-master "docker exec jobmanager flink list" | grep -E 'M1Job|M2Job'
```

## 五、启动检查点监测（本地 Mac，另开一个终端，一直开着）

```bash
bash deploy/scripts/syn-ckpt-watch.sh --interval 20 --framesize-mb 64 \
    --out docs/reports/ckpt_sizes_march_m3.csv
```

期望：每 20 秒记录一次逐算子与逐子任务的 checkpoint 大小；训练期间 checkpoint 超时属预期。
`--framesize-mb 64` 与 `.env` 的 `SYN_AKKA_FRAMESIZE=67108864b` 一致。

## 六、启动重放（本地 Mac）

```bash
bash deploy/scripts/syn-replay.sh --speedup 3600 --start 2022-03-01 --end 2022-04-01 \
    --inject-file docs/m3_march_inject_spec.txt
bash deploy/scripts/syn-replay.sh logs      # 观察进度，Ctrl+C 只停跟踪
```

**注入规格必须用 `--inject-file` 传**：直接用 `--inject` 时，规格里的分号会被启动脚本截断，只剩第一条
注入生效；脚本现在会拒绝这种写法。地面真值写到 master 的 `/opt/fa-iforest/replay-state/inject-truth.csv`。

期望：重放器启动时打印 `Injection: 12 spec(s)` 与 12 行注入规格；结束时打印 `Finished.` 汇总块、
`Injection applied: … modifications (12 spec(s))`（大于 0）与 `rc=0`。

## 七、观察 M3 冷启动（本地 Mac，重放开始约 8 分钟后）

```bash
ssh fa-worker1 "docker logs -f taskmanager-2 2>&1 | grep --line-buffered -E '\[M3\]|OpenMP BLAS'"
ssh fa-worker2 "docker logs -f taskmanager-3 2>&1 | grep --line-buffered -E '\[M3\]|OpenMP BLAS'"
```

期望：每台设备依次出现 `entering TRAINING`、逐轮 `training epoch`、`trained: … epochs (last improvement
at epoch …, longest plateau …), early-stop loss=…`、`calibrated`、`entering ONLINE`。
ND4J 初始化时打印的 `OpenMP BLAS` 线程数应为 1（若做了 0.2）。出现 `REPORT` 行的设备须上报设计会话。

训练期间重放器会继续把数据写进 Kafka，作业因反压暂停消费；训练结束后作业会全速追赶积压。追赶期间
检查点大小的峰值正是第五步要测的量。

## 八、完整性门槛（本地 Mac，重放结束后）

```bash
bash deploy/scripts/syn-replay-verify.sh --tol-pct 0.2
```

期望：四条断言全部 PASS，断言五（零重启）PASS，退出码 0。注入只改数值、不增删轮，不影响轮数对账。

## 九、排空与收集（本地 Mac，在任何清理之前）

```bash
bash deploy/scripts/syn-m2-metrics.sh | tee /tmp/m3m_t1.txt
sleep 120
bash deploy/scripts/syn-m2-metrics.sh | tee /tmp/m3m_t2.txt
diff /tmp/m3m_t1.txt /tmp/m3m_t2.txt && echo "已排空"

bash deploy/scripts/syn-m3-march-collect.sh
bash deploy/scripts/syn-m2-baseline.sh --tag march-m3      # 点异常通道 ±1% 等值核验（E 见 0.4）
```

期望：`docs/m3_march/` 下有 `scores.jsonl`、`m3_scores.jsonl`、`inject-truth.csv`、`m3_tm_log.txt`、
`collect_summary.txt`；摘要中「M3 训练完成的设备数」为 8（H 若可用轮不足会少一台，须核对）。

## 十、离线参照：三次训练（本地 Mac，收集之后、清理之前）

```bash
ssh fa-master 'docker rm -f syn-m3grid 2>/dev/null'
bash deploy/scripts/syn-m3-grid.sh \
    --devices E,G,C --hidden-grid 60 --window-grid 60 \
    --batch-grid 64 --lr-grid 0.001 --grad-clip 39072.0 \
    --max-epochs 300 --patience 20 --omp-threads 1 \
    --node master --detach \
    --out-name m3_march_reference.csv --per-epoch-name m3_march_reference_per_epoch.csv
# 约 45 分钟后：
bash deploy/scripts/syn-m3-grid.sh --collect --node master \
    --out-name m3_march_reference.csv --per-epoch-name m3_march_reference_per_epoch.csv
```

不加 `--reuse-dump`，脚本会从刚跑完的三月 m1-out 重新转储。期望：三行结果；每台设备的 `esLoss` 与
第七步日志中同一设备的 `early-stop loss` 相差不超过 5%（裁决书第三节）。

## 十一、收尾（结果已取回并提交入库之后）

```bash
bash deploy/scripts/syn-reset-env.sh
```

它会清空本次写进 Kafka 的数据与远端工作目录 `m3grid`、`m2probe` 等；`m3march/` 下的 monitoring 转储
若不再需要，另行删除：`ssh fa-master 'rm -rf /opt/fa-iforest/m3march'`。

---

## 预计耗时

| 步骤 | 墙钟 |
| --- | --- |
| 重放本身（3600 倍） | 约 20 至 60 分钟 |
| M3 冷启动训练 | 每台 10 至 30 分钟，8 个子任务并行；同一子任务上的设备依次训练 |
| 追赶积压 | 取决于训练时长，预计半小时以内 |
| 收集与离线参照 | 约 1 小时 |

之后的分析（V-M3-4 误报率与分布图、V-M3-5 召回表与每类注入的得分曲线图、V-M3-6 性能报告、等值核验表）
在拿到第九、十步的产物后编写脚本并出报告。
