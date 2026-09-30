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

**前置要求（2026-09-30 裁决第二节）**：先完成冷启动短重放验证（补遗三步骤 C，
`docs/m3_coldstart_probe_runbook_zh.md`），再提交三月重放。步骤 C 同时是 M3 的第一次在线打分，即实验手册
`docs/m3_experiment_runbook.md` 阶段二的「类型 A：联合作业 + 重放」。

M3 的分段按「轮数」而不是日历：各设备的训练在攒满 (7 + 2 + 2) × 8,640 = 95,040 个可用轮后触发，
缺轮较多的设备（例如 B）会比日历晚一些进入在线。

---

## 零、开跑前须确认的事项

### 0.1 冷启动训练期间的检查点配置（2026-09-30 已裁决）

M3 训练在任务线程内同步执行，其间该子任务无法响应检查点。裁决维持同步训练，靶向放宽超时，不再用
「容忍一百次失败」：**检查点间隔 30 秒、超时 60 分钟、容忍连续失败 3 次**。超时的六十分钟是估算值，
应由步骤 C 的结果替换（取最慢子任务外推时长的两倍）；第四步的提交命令里用 `<超时分钟数>` 表示这个值，
设计会话确认前按 60 填写。

需要知道的两点（详见 `docs/reports/m3_coldstart_probe_notes_for_decision.md`）：

- 设备按键哈希分到子任务，B、C、E 三台在同一个子任务上依次训练，D、G 在另一个子任务上依次训练。
  决定检查点挂起多久的是这两个子任务的训练时长之和。
- 容忍 3 次、超时 60 分钟时，被训练阻塞的作业要连续 4 次超时，也就是约 4 小时，才会重启。
  训练超过 60 分钟只会留下一次可见的超时失败，作业不会重启。

冷启动期间作业事实上不可检查点，其间任何故障都会让训练从头再来。在重放数据上，这只是重跑的代价。

### 0.2 TaskManager 单线程（2026-09-30 用户已确认）

在步骤 C 的第一步完成：worker 的 compose 文件给 taskmanager 加了 `OMP_NUM_THREADS=1`，并且只重建了
taskmanager 容器。第三步的复位核对表中「TM 单线程」一项核对它仍然生效。

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
git add -f docs/m3_march_injection_plan.csv docs/m3_march_inject_spec.txt   # csv 与 txt 被 .gitignore 忽略，须加 -f
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

先记下提交前的时刻，第九步收集 TaskManager 日志时要用。容器重启不会清空 `docker logs`，不加时刻
过滤会混入步骤 C 的记录：

```bash
SINCE=$(ssh fa-master date -u +%Y-%m-%dT%H:%M:%SZ); echo "SINCE=$SINCE"
```

参与等值核验的参数一律显式传入，不依赖默认值。`--extra` 可以分行书写：`syn-submit-m2.sh` 自
2026-09-30 起会把其中的换行压成空格（此前换行会让远端命令提前结束，之后的参数全部丢失、作业带着默认值
悄悄运行）。

```bash
bash deploy/scripts/syn-submit-m2.sh --extra '--m3-enabled true --window-sec 3600
    --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 2 --m3-window-length 60
    --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0
    --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true
    --checkpoint-ms 30000 --checkpoint-timeout-min <超时分钟数> --checkpoint-tolerable-failures 3' \
    2>&1 | tee /tmp/submit_m3.log
grep -E 'W=|Window W/S|M3 |Ckpt' /tmp/submit_m3.log
JID=$(grep -oE 'JobID [a-f0-9]{32}' /tmp/submit_m3.log | awk '{print $2}' | head -1); echo "JID=$JID"
```

期望：`W=3600s`、`M3 enabled: true`、`M3 train/es/th: 7d/2d/2d`、`M3 window: 60 rounds`、
`M3 max epochs: 300 (patience=20)`、`M3 hidden size: 60`、`M3 batch size: 64`、
`Ckpt interval/timeout: 30000 ms / <超时分钟数> min`、`Ckpt tolerable failures: 3`、
`M3 rounds/day:   8640`（**不得**出现 `COLD-START PROBE MODE`）。然后确认恰好一个本项目作业在运行：

```bash
ssh fa-master "docker exec jobmanager flink list" | grep -E 'M1Job|M2Job'
```

## 五、启动检查点监测（本地 Mac，另开两个终端，一直开着）

```bash
bash deploy/scripts/syn-ckpt-watch.sh --jid <第四步的 JID> --interval 20 --framesize-mb 64 \
    --out docs/reports/ckpt_sizes_march_m3.csv
bash deploy/scripts/syn-m3-coldstart-watch.sh --jid <第四步的 JID> --out docs/m3_march/ckpt_timeline.csv
```

期望：第一条每 20 秒记录一次逐算子与逐子任务的检查点大小，`--framesize-mb 64` 与 `.env` 的
`SYN_AKKA_FRAMESIZE=67108864b` 一致。第二条每 15 秒记录一行检查点时间线：训练期间「进行中=1」且挂起秒数
持续增长；训练全部结束后「完成」计数加一；「重启」始终为 0。第二条回答裁决第三节的「训练期间被挂起的
检查点在训练结束后是否正常完成」。

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
ssh fa-worker1 "docker logs -f --since $SINCE taskmanager-2 2>&1 | grep --line-buffered -E '\[M3\]|OpenMP BLAS'"
ssh fa-worker2 "docker logs -f --since $SINCE taskmanager-3 2>&1 | grep --line-buffered -E '\[M3\]|OpenMP BLAS'"
```

期望：每台设备依次出现 `entering TRAINING`（带子任务编号；B、C、E 在子任务 1 上依次训练，D、G 在子任务 7
上依次训练）、逐轮 `training epoch`、`trained: … epochs (last improvement
at epoch …, longest plateau …), early-stop loss=…`、`calibrated`、`entering ONLINE`。
ND4J 初始化时打印的 `OpenMP BLAS` 线程数应为 1。出现 `REPORT` 行的设备须上报设计会话。

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

bash deploy/scripts/syn-m3-march-collect.sh --since $SINCE
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

然后生成冷启动报告，并在第五步的两个终端里按 Ctrl+C 停止监测：

```bash
python3 deploy/scripts/m3_coldstart_report.py --dir docs/m3_march \
    --offline-csv docs/m3_march_reference.csv --timeout-min <超时分钟数>
```

期望：`docs/m3_march/coldstart_report.md` 给出 2026-09-30 裁决第三节要求记录的三项。第一节是逐台设备的
冷启动墙钟时长与实际轮数；第三节是训练期间挂起的检查点是否在训练结束后完成，以及重启次数；第四节是
E、G、C 在线每轮秒数与离线参照之比（即减速比），以及早停集误差的相对偏差。离线参照 CSV 的实际路径
以第十步 `--collect` 打印的为准。

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
| M3 冷启动训练 | 最慢的是子任务 1（B、C、E 依次训练），按六月离线值估算约 40 分钟以上；以步骤 C 的外推为准 |
| 追赶积压 | 取决于训练时长，预计半小时以内 |
| 收集与离线参照 | 约 1 小时 |

之后的分析（V-M3-4 误报率与分布图、V-M3-5 召回表与每类注入的得分曲线图、V-M3-6 性能报告、等值核验表）
在拿到第九、十步的产物后编写脚本并出报告。
