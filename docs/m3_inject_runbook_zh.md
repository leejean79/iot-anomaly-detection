# 注入运行（类型 B）操作手册

版本：2026-10-08。依据 2026-10-06 裁决书《三月至四月运行报告（六台口径）的处置》与同日的《注入实验参数（书面版）》。
本手册取代 `docs/m3_march_runbook_zh.md` 与 `docs/m3_marapr_runbook_zh.md` 中关于注入运行的所有旧说法。

## 一、这次运行要做的事

| 项目 | 做法 | 依据 |
| --- | --- | --- |
| 平稳日参照期 | 改为 03-08 至 03-17（训练集加早停集），判据不变：五个检测通道的机队中位偏移绝对值都不超过 0.5 个参照期宽度 | 裁决第三节 |
| 注入日 | 按新参照期重算后的四月平稳日 | 裁决第三节 |
| 注入方案 | 设备 E；主通道温度：尖峰、阶跃、爬坡各三档（1、2、4 倍），卡死三次；排得下时在气体通道补做阶跃 2 倍、爬坡 2 倍各一次 | 参数书 |
| 幅度单位 | 阈值校准期 03-17 至 03-24 该通道 P10 至 P90 宽度 | 参数书 |
| 相邻间隔 | 默认 7,200 秒，下限 3,660 秒 | 参数书 |
| 共模门槛 | 在场设备数的四分之三向上取整（八台为六，六台为五） | 裁决第四节 |
| JavaCPP 物理内存上限 | 由 3,584 MB 提到 3,900 MB，需要重建两台 TaskManager | 裁决第四节 |
| 重放 | 两段：第一段 03-01 至 03-26 末，不注入；第二段 03-27 至 04-30 末，带注入 | 10-05 裁决第三节 |
| D、G 覆盖 | 八台全量评估（代理键已让八台各占一个子任务） | 裁决第五节 |
| 通过条件 | 七条断言，加上「两条预验证条件」 | 裁决第五节 |
| B、H 分析 | 在三月至四月运行的已有数据上做，不重放（第二步） | 裁决第二节 |

**「两条预验证条件」的理解。** 短重放预验证的通过条件共四项：八台落在八个子任务、迟到丢弃为零、训练后检查点
正常完成、重启为零。其中迟到丢弃为零就是断言六，重启为零就是断言五，七条断言已经覆盖。剩下两项不在断言之内，
本手册把它们当作「两条预验证条件」：

1. 八台设备落在八个不同的子任务上（A 到 H 依次对应子任务 0 到 7）；
2. 训练期间挂起的检查点在训练结束后完成，失败次数为零。

若设计会话的本意不是这两条，请告诉我，我再改手册。

**执行环境的约定**：除特别注明外，所有命令都在本地 Mac 的仓库根目录执行，经 `fa-master`、`fa-worker1`、
`fa-worker2` 三个 ssh 别名操作集群。读原始 CSV 的命令（第一、二、四步）只能在本地 Mac 上执行。

**前置条件**：短重放预验证已通过并已复位（10-06 已完成）；集群上没有我们自己的作业在运行。FA-iForest 的作业
不受本手册任何一步影响，也不要去取消它。

---

## 二、开跑前（本地 Mac，仓库根目录）

### 第一步：按新参照期重算剖面与平稳日

```bash
git pull --rebase origin dev-claude
python3 eda/daily_channel_profile.py \
    --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
    --start 2022-03-01 --end 2022-05-01 --ref-start 2022-03-08 --ref-end 2022-03-17 \
    --scores docs/m3_marapr/m3_scores.jsonl --out docs/m3_marapr/daily_channel_profile_ref0308.csv
python3 deploy/scripts/m3_v34_report.py --scores docs/m3_marapr/m3_scores.jsonl \
    --profile docs/m3_marapr/daily_channel_profile_ref0308.csv --start 2022-03-01 --end 2022-05-01 \
    --exclude-devices D,G --out-dir docs/m3_marapr/v34_ref0308
DAYS=$(python3 -c "import csv; print(','.join('2022-'+r['day'] for r in csv.DictReader(open('docs/m3_marapr/v34_ref0308/v34_daily.csv')) if r['stable']=='True' and r['day'].startswith('04')))"); echo "DAYS=$DAYS"
```

- 参照期的结束日是不含在内的，所以 `--ref-end 2022-03-17` 表示 03-08 至 03-16 共九天，正好是训练集加早停集。
- 评估区间从 03-01 开始，是为了按裁决「重算三月至四月的平稳日清单」。三月上旬没有评分，这些天的告警率一栏
  为空，只参与平稳日判定。
- 期望：终端打印「平稳日共 N 天：……」；`docs/m3_marapr/v34_ref0308/` 下有 `v34_report.md`、`v34_daily.csv` 与两幅图；
  最后一行打印 `DAYS=2022-04-..,2022-04-..,...`，即重算后的四月平稳日。
- 若 `DAYS=` 后面为空，说明四月没有平稳日，注入无处可排，请停下来把 `v34_report.md` 推送给我。

### 第二步：B、H 的并行分析（不重放，裁决第二节）

```bash
python3 deploy/scripts/m3_bh_analysis.py --scores docs/m3_marapr/m3_scores.jsonl \
    --tm-log docs/m3_marapr/m3_tm_log.txt --profile docs/m3_marapr/daily_channel_profile_ref0308.csv \
    --out docs/reports/m3_bh_analysis.md
```

- 期望：终端与 `docs/reports/m3_bh_analysis.md` 中有三节：逐台的校准误差、阈值与平稳日误差；B 的逐日、逐小时与
  主导通道分布；H 在 04-04 恢复前后的超阈比例、跨缺口的窗口与主导通道。
- 退出码 2 表示输入文件不存在；退出码 3 表示训练日志里找不到 B 或 H 的标定记录，请检查 `m3_tm_log.txt` 是否完整。

### 第三步：生成注入计划

```bash
python3 eda/make_injection_specs.py \
    --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
    --days "$DAYS" --out docs/m3_inject_plan.csv
cat docs/m3_inject_spec.txt | tr ';' '\n'
```

- 期望：终端先打印温度与气体两个通道在 03-17 至 03-24 的中位数与 P10–P90 宽度，再打印计划表：温度通道 12 条
  （尖峰 3、阶跃 3、爬坡 3、卡死 3），排得下时另有气体通道 2 条（`extra` 列为 1）。`docs/m3_inject_spec.txt` 每段
  形如 `E:Temperature:<开始秒>:<持续秒>:<类型>:<幅度>`。
- 若终端提示「平稳日已排满，气体通道的补做……不再安排」，属参数书允许的情形（「若排程有余」），照常继续，报告中注明。
- 退出码 3 表示平稳日排不下温度通道的 12 条，退出码 4 表示 `DAYS` 中混入了非四月的日子；两种情况都请停下来，
  把终端输出告诉我。

### 第四步：入库

```bash
git add -f docs/m3_marapr/daily_channel_profile_ref0308.csv docs/m3_marapr/v34_ref0308 \
    docs/reports/m3_bh_analysis.md docs/m3_inject_plan.csv docs/m3_inject_spec.txt
git commit -m "data(m3): 新参照期的平稳日、B 与 H 的并行分析、注入计划" && git push origin dev-claude
```

推送后，B 与 H 的分析就可以先交给设计会话，不必等注入运行结束。下面的步骤可以不等我回复直接继续。

### 第五步：改 JavaCPP 上限，重建两台 TaskManager，然后复位

先用编辑器打开本地的 `deploy/.env`，把这一行

```
SYN_JAVACPP_MAXPHYSICALBYTES=3584m
```

改为

```
SYN_JAVACPP_MAXPHYSICALBYTES=3900m
```

然后执行：

```bash
grep '^SYN_JAVACPP_MAXPHYSICALBYTES' deploy/.env
bash deploy/scripts/syn-tm-recreate.sh
bash deploy/scripts/syn-reset-env.sh
```

- `syn-tm-recreate.sh` 把 `deploy/.env` 与 worker 编排文件复制到两台 worker，只重建 `taskmanager-2`、`taskmanager-3`
  两个容器，Kafka 不动。
- 期望：每台 worker 打印「Kafka 创建时间：重建前 X；重建后 X」两值相同，TaskManager 创建时间为刚才，
  「JavaCPP 物理内存上限：maxphysicalbytes=3900m」。复位核对表全部 PASS（其中「JavaCPP 上限」一项为 3900m），
  退出码 0。
- 退出码 3：集群上有作业在运行。只取消我们自己的 M2Job，FA-iForest 的作业不要动，然后重试。
- 打印「.env 中的地址……与现有……注册的地址不同，跳过本台」：说明 `deploy/.env` 里的 worker 内网地址与正在运行的
  TaskManager 不一致，请把这一行输出告诉我，先不要继续。
- 打印「Kafka 被重建」：请立即停下上报。

### 第六步：打包、检查、上传

```bash
mvn clean package -DskipTests
bash deploy/scripts/check-jar.sh
bash deploy/scripts/syn-upload-m1.sh --jar-only
```

期望 `check-jar.sh` 全部 PASS。

### 第七步：确认期望轮数的逐日表

```bash
ls docs/m3_marapr_eda_rounds.csv || python3 eda/count_rounds.py \
    --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
    --start 2022-03-01 --end 2022-05-01 --out docs/m3_marapr_eda_rounds.csv
```

注入只改数值、不改轮数，所以沿用三月至四月运行的逐日表。文件已在本地时第一条命令直接列出它；不在时自动重新
统计，期望打印总轮数约 400 万。

---

## 三、运行

### 第八步：记下时刻，然后提交作业

```bash
mkdir -p docs/m3_inject
SINCE=$(ssh fa-master date -u +%Y-%m-%dT%H:%M:%SZ); echo "SINCE=$SINCE"
bash deploy/scripts/syn-submit-m2.sh --extra '--m3-enabled true --window-sec 3600 --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 7 --m3-window-length 60 --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0 --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true --checkpoint-ms 30000 --checkpoint-timeout-min 120 --checkpoint-tolerable-failures 3' 2>&1 | tee /tmp/submit_inject.log
grep -E 'Idle timeout|Device subtasks|M3 train|M3 rounds|Ckpt|RUNNING' /tmp/submit_inject.log
JID=$(grep -oE 'JobID [a-f0-9]{32}' /tmp/submit_inject.log | awk '{print $2}' | head -1); echo "JID=$JID"
```

期望：

- `Idle timeout:    3600 s`；
- `Device subtasks: {A=0, B=1, C=2, D=3, E=4, F=5, G=6, H=7} (surrogate keys, maxParallelism=128)`；
- `M3 train/es/th:  7d/2d/7d`，`M3 rounds/day:   8640`，不得带 `COLD-START PROBE MODE`；
- `Ckpt interval/timeout: 30000 ms / 120 min`，最后一行 `M2Job is RUNNING`。

请把 `SINCE` 与 `JID` 抄下来，后面的步骤都要用。若 `Device subtasks` 一行显示 `raw device ids`，先停下来。

### 第九步：启动监测与第一段重放（不注入）

```bash
bash deploy/scripts/syn-m3-watch.sh start --jid $JID --since $SINCE
bash deploy/scripts/syn-replay.sh --speedup 3600 --start 2022-03-01 --end 1648339199
```

`1648339199` 是 2022-03-26 23:59:59 UTC；重放器的结束时刻包含在内。第一段约 11 分钟放完；约 9 分钟后首批设备
进入训练，冷启动约 45 分钟。

### 第十步：等八台都进入在线

```bash
bash deploy/scripts/syn-m3-watch.sh status
bash deploy/scripts/syn-replay.sh status
```

约一小时后看一次，直到监测显示「已进入在线 8 台」、重放器显示 `rc=0`。在此之前不要启动第二段，作业在两段之间
保持运行。若关过终端，先重新设置变量：`JID=<第八步抄下的值>; SINCE=<第八步抄下的值>`。

若作业失败，先诊断并推送，然后告诉我：

```bash
bash deploy/scripts/syn-m3-diag.sh --since $SINCE --out-dir docs/m3_inject/diag
```

### 第十一步：第二段重放（带注入）

```bash
bash deploy/scripts/syn-replay.sh --speedup 3600 --start 2022-03-27 --end 2022-05-01 --inject-file docs/m3_inject_spec.txt
```

- 注入只放在第二段：重放器每次启动都会删掉上一次的真值文件，所以注入规格必须在最后一次启动时带上。
- 约 15 分钟放完，等重放器显示 `rc=0`。

---

## 四、运行结束后（全部在复位之前完成）

### 第十二步：排空，然后做七条断言（作业仍在运行）

```bash
bash deploy/scripts/syn-m2-metrics.sh > /tmp/inj_t1.txt; sleep 120
bash deploy/scripts/syn-m2-metrics.sh > docs/m3_inject/m2_metrics_final.txt
diff /tmp/inj_t1.txt docs/m3_inject/m2_metrics_final.txt && echo "已排空"
TOTAL=$(python3 -c "import csv; r=list(csv.reader(open('docs/m3_marapr_eda_rounds.csv'))); print(sum(int(x) for row in r[1:] for x in row[1:]))"); echo "TOTAL=$TOTAL"
bash deploy/scripts/syn-replay-verify.sh --start-utc 2022-03-01T00:00:00Z --end-utc 2022-05-01T00:00:00Z --expected-total "$TOTAL" --tol-pct 0.2 --max-messages 6000000 --report-name m2_replay_verify_inject.csv --since "$SINCE" 2>&1 | tee docs/m3_inject/replay_verify.txt
```

- 若「已排空」没有打印，等两分钟后重复前三行，再做核验。
- 期望七条断言全部 PASS，最后一行为「七条断言全部通过」。断言六是迟到丢弃为零；断言七逐台列出两个通道最晚
  输出与最后一轮的差，这次 D、G 也必须在列（裁决第五节「补齐 D、G 覆盖」）。
- `--report-name m2_replay_verify_inject.csv` 不能省：默认文件里存着三月重跑的逐台冻结时刻，第十六步要拿它比对。
- 任何一条不是 PASS 时，先执行第十步里的诊断命令并推送，暂不取消作业、不复位。

### 第十三步：收集，并核对两条预验证条件

```bash
bash deploy/scripts/syn-m3-watch.sh stop --out-dir docs/m3_inject
bash deploy/scripts/syn-m3-march-collect.sh --out-dir docs/m3_inject --since $SINCE
bash deploy/scripts/syn-m3-perf-collect.sh --since $SINCE --out-dir docs/m3_inject/perf
bash deploy/scripts/syn-m2-late-drop-history.sh --since "$SINCE" --job $JID --out-dir docs/m3_inject/late_drop
python3 deploy/scripts/m3_coldstart_report.py --dir docs/m3_inject --timeout-min 120
grep -oE 'Device [A-H] \(subtask [0-9]+\) entering TRAINING' docs/m3_inject/m3_tm_log.txt | sort -u
```

期望：

- `docs/m3_inject/collect_summary.txt` 中「训练完成的设备数」为 8，「注入真值行数（含表头）」等于注入条数加一
  （12 或 14 条注入，即 13 或 15 行）。若提示 master 上没有真值文件，说明第二段没有带 `--inject-file`，请告诉我。
- `docs/m3_inject/perf/perf_summary.md` 有推理时延、转发延迟、TaskManager 内存三张表。内存一表的峰值应低于
  3,900 MB；若接近，请告诉我。
- **预验证条件一**：最后一条命令打印八行，A 到 H 依次对应子任务 0 到 7。
- **预验证条件二**：`docs/m3_inject/coldstart_report.md` 的检查点一节里，训练期间挂起的检查点在训练结束后完成，
  失败次数为 0。
- `docs/m3_inject/late_drop/late_drop_summary.md` 八个子任务的丢弃都是 0（与断言六相互印证）。

### 第十四步：点异常通道 ±1% 比较（三月至四月运行尚欠的一项）

```bash
bash deploy/scripts/syn-m2-probe.sh --r-grid 0.75,1.0,1.5 --k-grid 10 --max-messages 6000000 --out-name m2_probe_inject.csv
bash deploy/scripts/syn-m2-baseline.sh --tag inject-m3 --probe-ref docs/m2_probe_inject.csv --max-messages 3000000
```

- 参照表在本次重放的 `synergia-m1-out` 上现算，注入后的数值两边一致，所以比较不受注入影响。
- 期望第二条输出逐设备表，八台的相对差都在 ±1% 以内；同时把监测转储拉到本地 `docs/m2_monitoring_inject-m3.jsonl`，
  第十六步要用。
- 探针在 master 上多占约 2 GB 磁盘，第十八步清理。

### 第十五步：V-M3-5 召回表与双通道对注入的响应

```bash
python3 deploy/scripts/m3_injection_recall.py --truth docs/m3_inject/inject-truth.csv \
    --scores docs/m3_inject/scores.jsonl --plan docs/m3_inject_plan.csv --out-dir docs/m3_inject/recall
```

- 期望：终端打印召回表（逐类型、逐幅度，点通道与上下文通道各自的检出与首次检出延迟）、背景超阈比例，以及三条
  预言的核对数据；`docs/m3_inject/recall/` 下有 `recall.md`、`recall.csv` 与 `figs/inject_NN_<类型>.png`，每条注入一幅图。
- 退出码 2：真值或评分文件不存在，请回到第十三步检查收集结果。退出码 3：评分里没有设备 E 的记录，请告诉我。

### 第十六步：三月二十二日至二十七日事件的双通道对比（三月至四月运行尚欠的一项）

第一段重放不注入，本次运行的点通道在三月与无注入运行相同，可以作为对比的点通道一侧；上下文通道与剖面仍取三月
重跑的已有文件。

```bash
python3 deploy/scripts/m3_dual_channel_week.py \
    --monitoring docs/m2_monitoring_inject-m3.jsonl \
    --scores docs/m3_march/m3_scores.jsonl \
    --profile docs/m3_march/daily_channel_profile.csv \
    --hourly-gas docs/m3_march/hourly_gas_profile.csv \
    --freeze-ref docs/m2_replay_verify.csv \
    --freeze-new docs/m2_replay_verify_inject.csv \
    --verify-log docs/m3_inject/replay_verify.txt \
    --metrics docs/m3_inject/m2_metrics_final.txt \
    --out-dir docs/reports/m3_march_dual_channel_week
```

期望：

1. 「点通道等价性的两个条件」都为「成立」。若显示「条件不全成立」，请把输出告诉我。
2. 逐日总表、预言一至五的核对表、上下文通道的设备内检验。
3. 输出目录下有 `dual_channel_check.md`、`dual_channel_hourly.csv`、`dual_fleet.png`、`dual_D.png`、`dual_E.png`。

### 第十七步：八台口径的 V-M3-4

```bash
python3 deploy/scripts/m3_v34_report.py --scores docs/m3_inject/m3_scores.jsonl \
    --profile docs/m3_marapr/daily_channel_profile_ref0308.csv --start 2022-03-24 --end 2022-05-01 \
    --exclude-truth docs/m3_inject/inject-truth.csv --out-dir docs/m3_inject/v34
```

- 平稳日沿用第一步按新参照期判定的结果；`--exclude-truth` 去掉设备 E 在每次注入区间及其后一个点通道窗长内的
  评分，使注入不计入误报率。共模门槛按每小时在场设备数自动取（八台为六）。
- 期望 `docs/m3_inject/v34/` 下有 `v34_report.md`、`v34_daily.csv`、两幅图；报告开头注明排除了注入区间。

### 第十八步：入库与收尾

```bash
git add -f docs/m3_inject docs/m2_replay_verify_inject.csv docs/m2_probe_inject.csv \
    docs/reports/m2_java11_inject-m3_* docs/reports/m3_march_dual_channel_week docs/m3_marapr_eda_rounds.csv
git commit -m "data(m3): 注入运行（类型 B）结果" && git push origin dev-claude
ssh fa-master "docker exec jobmanager flink cancel $JID"
bash deploy/scripts/syn-reset-env.sh
ssh fa-master 'rm -rf /opt/fa-iforest/m3march /opt/fa-iforest/m2probe'
gzip -f docs/m2_monitoring_inject-m3.jsonl
```

- 先确认推送成功，再取消作业与复位：复位之后本次运行的 Kafka 数据就没有了。
- `flink cancel` 只取消第八步提交的作业。
- 监测转储压缩后留在本地（不入库），保留到设计会话接受双通道报告为止，届时再执行
  `rm -f docs/m2_monitoring_inject-m3.jsonl.gz`。
- 推送后告诉我。我据此写注入运行报告：V-M3-5 召回表、双通道对注入的响应对比、B 与 H 的并行分析，另附 ±1% 比较、
  三月事件的双通道对比、八台口径的 V-M3-4 与 V-M3-6。

---

## 五、预授权的退路

若作业在训练后的第一个检查点上失败（确认消息超帧，或 JobManager 内存耗尽），按 2026-10-01 裁决第四节，复位后
只把第八步的 `--checkpoint-ms 30000` 改为 `--checkpoint-ms 86400000`，其余照做。报告中注明，失去的只是训练后的
检查点峰值读数。

## 六、预计耗时

| 步骤 | 实际耗时 |
| --- | --- |
| 第一至四步（本地分析与计划） | 约 20 分钟 |
| 第五至七步（重建、复位、打包） | 约 15 分钟 |
| 第一段重放与冷启动 | 约 1 小时 |
| 第二段重放与排空 | 约 30 分钟 |
| 第十二至十七步（核验、收集、探针、分析） | 约 1 小时 |
