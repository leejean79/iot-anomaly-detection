# 第二批在线验收：三月至四月重放操作手册（类型 A，不注入）

版本：2026-10-03，合并了 2026-10-02 裁决书《三月重跑（不注入）报告的五项待决事项》、同日的分析任务书
《三月二十二日至二十七日事件的双通道响应对比》，以及设计会话 2026-10-03 对该任务书初步结果的回复。
本手册取代 `docs/m3_march_runbook_zh.md` 中第一次运行（类型 A）的部分；注入运行（类型 B）在本次运行之后另给步骤。

## 一、这次运行要回答的问题

| 项目 | 三月重跑的做法 | 本次的做法 | 依据 |
| --- | --- | --- | --- |
| 重放区间 | 2022-03-01 至 03-31 | **2022-03-01 至 04-30** | 裁决第四节第 2 条 |
| 阈值校准集 | 2 天 | **7 天**（作业默认值已改为 7） | 裁决第四节第 1 条 |
| 离线参照的离群口径 | 60 次判定中任意一次 | **到达滑动步的那一次**，与在线一致 | 裁决第二节第 1 条 |
| 等值核验 | 早停集误差相差不超过 5% | **先核训练集逐一相同，再要求误差逐位相同** | 裁决第二节第 2 条 |
| 点异常通道 ±1% 比较 | 用了含偏差的旧参照表 | **在本次重放上现算修正口径的参照表** | 裁决第三节 |
| V-M3-4 | 留出周误报率 | **逐日告警率与通道剖面并排、平稳日误报率、共模比例** | 裁决第四节第 3、4 条 |
| V-M3-6 | 只有训练时长 | **另测逐窗推理时延、转发延迟、任务管理器内存** | 裁决第五节 |
| 三月事件的双通道对比 | 无 | **点通道取本次运行，上下文通道与剖面取三月重跑** | 分析任务书 |
| 点通道等价性 | 无 | **核对两个条件：断言全过且迟到丢弃为零；逐台冻结时刻与三月重跑相同** | 10-03 回复第一节 |
| 预言五 | 两个通道都做跨设备排序 | **只对点通道排序；上下文通道改为设备内检验** | 10-03 回复第二节 |
| B 的缝合窗口 | 无 | **把告警窗口与缺轮位置对齐**（第三步，不需要重放） | 10-03 回复第二节 |
| 预言三 | 只有逐日宽度 | **03-26 至 03-28 的气体宽度改按小时计算** | 10-03 回复第二节 |

数据分段（按各设备的可用轮数，不按日历）：

| 时段 | 用途 |
| --- | --- |
| 03-01 至 03-08 | M1 预热与标定（7 天） |
| 03-08 至 03-15 | M3 训练集（7 天） |
| 03-15 至 03-17 | M3 早停集（2 天） |
| 03-17 至 03-24 | M3 阈值校准（7 天） |
| 03-24 至 04-30 | 在线；V-M3-4 在这一段上评估。H 在 04-01 至 04-04 没有数据，按已知停机处理 |

训练在各设备攒满 (7 + 2 + 7) × 8,640 = 138,240 个可用轮后触发。检查点配置不变：间隔 30 秒、超时 120 分钟、
容忍 3 次，保持开启。

**执行环境的约定**：除特别注明「本地 Mac」的命令外，所有命令都在本地 Mac 的仓库根目录执行，经 `fa-master`
别名操作集群。读原始 CSV 的命令（第二、三、十三步）只能在本地 Mac 上执行，因为原始数据只在那里。

---

## 二、开跑前（本地 Mac，仓库根目录）

### 第一步：拉取、打包、上传

```bash
git pull --rebase origin dev-claude
mvn clean package -DskipTests
bash deploy/scripts/check-jar.sh
bash deploy/scripts/syn-upload-m1.sh --jar-only
```

期望 `check-jar.sh` 全部 PASS，其中包括 2026-10-02 新增的三项：离线网格写剔除清单（`excluded-csv`）、在线算子
记录剔除窗口（`excluded training window ending at round`）、推理时延指标（`m3_inference_latency_ms`）。
若有 FAIL，说明打出的 jar 是旧的，重新执行 `mvn clean package -DskipTests`。

### 第二步：用 EDA 数出本区间的期望轮数（第八步断言一要用）

```bash
python3 eda/count_rounds.py \
    --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
    --start 2022-03-01 --end 2022-05-01 --out docs/m3_marapr_eda_rounds.csv
```

期望终端打印总轮数，约 400 万。请把这个数记下来。

### 第三步：两项不依赖本次运行的本地分析

这两项只读原始 CSV 和三月重跑已入库的文件，可以在开跑前做，也可以在等待重放时做，但必须在第十一步之前完成。

**3.1 03-26 至 03-28 的逐小时气体宽度（预言三）**

```bash
python3 eda/daily_channel_profile.py \
    --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
    --start 2022-03-15 --end 2022-04-01 --ref-start 2022-03-17 --ref-end 2022-03-19 \
    --out docs/m3_march/daily_channel_profile.csv \
    --hourly-start 2022-03-26 --hourly-end 2022-03-29 --hourly-out docs/m3_march/hourly_gas_profile.csv
git diff --stat docs/m3_march/daily_channel_profile.csv
```

- 参照期必须是 03-17 至 03-19，与三月重跑那份剖面相同。逐日部分用的是与已入库文件完全相同的参数，所以
  `git diff --stat` 应当没有输出；若有差异，说明剖面不可复现，请先告诉我，不要继续。
- 期望 `docs/m3_march/hourly_gas_profile.csv` 有 8 台 × 72 小时左右的行。列 `width_x` 是小时宽度倍数，
  分母是参照期内逐小时宽度的中位数（一小时内的散布天然小于一整天，所以不用日宽度作分母）；列 `ref_p90_x`
  是判定「回落」的上限。

**3.2 B 的告警窗口与缺轮位置对齐（缝合窗口假说）**

```bash
python3 eda/m3_window_gap_check.py \
    --scores docs/m3_march/m3_scores.jsonl \
    --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
    --out-dir docs/reports/m3_window_gap_check
```

- 对象是三月重跑的上下文通道记录（B 的那组数字就出自这次运行）。默认八台都算，其他七台作对照。
- 期望终端打印三张表：粗分辨率（窗口内缺失不少于 60 秒）、细分辨率（窗口内相邻两轮间隔超过 15 秒）、按最大
  间隔分档的告警率；`docs/reports/m3_window_gap_check/` 下有 `gap_check.md` 与 `gap_windows.csv`。
- 若退出码为 3，说明 `--data-dir` 路径不对。

**3.3 入库**

```bash
git add -f docs/m3_march/hourly_gas_profile.csv docs/reports/m3_window_gap_check
git commit -m "data(m3): 逐小时气体宽度与 B 的缺轮对齐" && git push origin dev-claude
```

### 第四步：复位，作为开跑门槛

```bash
bash deploy/scripts/syn-reset-env.sh
```

期望核对表全部 PASS，退出码 0。

---

## 三、运行

### 第五步：记下时刻，然后提交作业

```bash
mkdir -p docs/m3_marapr
SINCE=$(ssh fa-master date -u +%Y-%m-%dT%H:%M:%SZ); echo "SINCE=$SINCE"
bash deploy/scripts/syn-submit-m2.sh --extra '--m3-enabled true --window-sec 3600
    --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 7 --m3-window-length 60
    --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0
    --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true
    --checkpoint-ms 30000 --checkpoint-timeout-min 120 --checkpoint-tolerable-failures 3' \
    2>&1 | tee /tmp/submit_m3.log
grep -E 'W=|M3 |Ckpt' /tmp/submit_m3.log
JID=$(grep -oE 'JobID [a-f0-9]{32}' /tmp/submit_m3.log | awk '{print $2}' | head -1); echo "JID=$JID"
```

期望：

- 出现 `M3 train/es/th:  7d/2d/7d`、`Ckpt interval/timeout: 30000 ms / 120 min`、`Ckpt tolerable failures: 3`；
- 出现 `M3 rounds/day:   8640`，不得带 `COLD-START PROBE MODE`；
- 最后一行是 `M2Job is RUNNING`。

请把 `JID` 与 `SINCE` 的值抄下来，后面的步骤都要用。若提交后取消重交，重交前要重新执行 `SINCE=` 那一行。

### 第六步：在 master 上启动监测，然后启动重放

```bash
bash deploy/scripts/syn-m3-watch.sh start --jid $JID --since $SINCE
bash deploy/scripts/syn-replay.sh --speedup 3600 --start 2022-03-01 --end 2022-05-01
```

两者都在 master 上无人值守地运行，本地终端可以关掉。重放约 25 分钟；约 9 分钟后首批设备进入训练，冷启动约
45 分钟，之后作业追赶积压。

### 第七步：查看进度（约 1.5 小时后看一次即可）

```bash
bash deploy/scripts/syn-m3-watch.sh status
```

等它显示「已进入在线 8 台」、重放器已退出（`rc=0`），再做第八步。若关过终端，先重新设置变量：
`JID=<第五步抄下的值>; SINCE=<第五步抄下的值>`。

若作业失败，先诊断，把材料提交后告诉我：

```bash
bash deploy/scripts/syn-m3-diag.sh --since $SINCE --out-dir docs/m3_marapr/diag
```

---

## 四、运行结束后（全部在复位之前完成）

### 第八步：重放完整性核验（在取消作业之前）

```bash
TOTAL=$(python3 -c "import csv; r=list(csv.reader(open('docs/m3_marapr_eda_rounds.csv'))); print(sum(int(x) for row in r[1:] for x in row[1:]))"); echo "TOTAL=$TOTAL"
bash deploy/scripts/syn-replay-verify.sh --start-utc 2022-03-01T00:00:00Z --end-utc 2022-05-01T00:00:00Z \
    --expected-total "$TOTAL" --tol-pct 0.2 --max-messages 6000000 \
    --report-name m2_replay_verify_marapr.csv 2>&1 | tee docs/m3_marapr/replay_verify.txt
```

- 第一行从第二步的逐日表求出总轮数，打印的 `TOTAL` 应与第二步终端显示的总轮数相同。
- 期望五条断言全部 PASS，最后一行为「五条断言全部通过」。
- `--report-name m2_replay_verify_marapr.csv` 是必需的：默认文件名 `docs/m2_replay_verify.csv` 里存着三月重跑
  的逐台冻结时刻，第十一步要拿它作比对，不能被覆盖。
- `--max-messages 6000000` 也是必需的：默认 300 万条覆盖不了两个月约 400 万轮。
- 终端输出存入 `docs/m3_marapr/replay_verify.txt`，第十一步据此读五条断言的结果。

### 第九步：排空，然后收集结果与性能指标

```bash
bash deploy/scripts/syn-m2-metrics.sh > /tmp/m3m_t1.txt; sleep 120
bash deploy/scripts/syn-m2-metrics.sh > docs/m3_marapr/m2_metrics_final.txt
diff /tmp/m3m_t1.txt docs/m3_marapr/m2_metrics_final.txt && echo "已排空"
grep m2_gate_late_drop docs/m3_marapr/m2_metrics_final.txt
bash deploy/scripts/syn-m3-watch.sh stop --out-dir docs/m3_marapr
bash deploy/scripts/syn-m3-march-collect.sh --out-dir docs/m3_marapr --since $SINCE
bash deploy/scripts/syn-m3-perf-collect.sh --since $SINCE --out-dir docs/m3_marapr/perf
```

期望：

- `diff` 没有输出并打印「已排空」；若有差异，等两分钟后重复前三行。
- `m2_gate_late_drop` 为 0。这是点通道等价性条件一的一部分，若不为 0 请告诉我。
- `docs/m3_marapr/collect_summary.txt` 中「训练完成的设备数」为 8。
- `docs/m3_marapr/perf/perf_summary.md` 给出三张表：推理时延、转发延迟、任务管理器内存。若某项显示
  「Prometheus 没有该指标的数据」，请告诉我。

### 第十步：点异常通道 ±1% 比较（裁决第三节）

先在本次重放的 `synergia-m1-out` 上现算修正口径的参照表，再比较：

```bash
bash deploy/scripts/syn-m2-probe.sh --r-grid 0.75,1.0,1.5 --k-grid 10 --max-messages 6000000 \
    --out-name m2_probe_corrected.csv
bash deploy/scripts/syn-m2-baseline.sh --tag marapr-m3 --probe-ref docs/m2_probe_corrected.csv --max-messages 3000000
```

- 半径网格取 0.75、1.0、1.5，覆盖各设备实际使用的半径（D 为 0.75，G 为 1.5，其余为 1.0）。
- 期望第二条输出逐设备表，八台的相对差都在 ±1% 以内。
- 第二条同时把本次的监测转储拉到本地 `docs/m2_monitoring_marapr-m3.jsonl`，第十一步要用。三月的监测主题约
  62 万条，两个月约 125 万条，所以上限提到 300 万条。
- 探针要转储约 400 万轮，master 上会多占约 2 GB 磁盘，第十四步复位时清掉。

### 第十一步：三月二十二日至二十七日事件的双通道对比

点通道取第十步拉回的本次监测转储；上下文通道和气体剖面取**三月重跑**的已有文件。原因是本次运行的上下文
通道要到 03-24 才进入在线，覆盖不了 03-19 至 03-23，而且本次的阈值校准期 03-17 至 03-24 包含了事件的前半段。

```bash
python3 deploy/scripts/m3_dual_channel_week.py \
    --monitoring docs/m2_monitoring_marapr-m3.jsonl \
    --scores docs/m3_march/m3_scores.jsonl \
    --profile docs/m3_march/daily_channel_profile.csv \
    --hourly-gas docs/m3_march/hourly_gas_profile.csv \
    --freeze-ref docs/m2_replay_verify.csv \
    --freeze-new docs/m2_replay_verify_marapr.csv \
    --verify-log docs/m3_marapr/replay_verify.txt \
    --metrics docs/m3_marapr/m2_metrics_final.txt \
    --out-dir docs/reports/m3_march_dual_channel_week
```

期望终端依次打印：

1. 「点通道等价性的两个条件」：条件一（五条断言与迟到丢弃）和条件二（逐台冻结时刻）都应为「成立」，结论为
   「两个条件都成立」。若显示「条件不全成立」，点通道数字不能当作三月重跑的结果，请把输出告诉我。
2. 逐日总表，以及预言一至五的核对表。预言三用小时级的宽度回落时刻对点通道的回落时刻；预言五只对点通道排序。
3. 「上下文通道的设备内检验」：每台设备的告警率与本台气体偏移、宽度的秩相关。

输出目录下有 `dual_channel_check.md`、`dual_channel_hourly.csv`、`dual_fleet.png`、`dual_D.png`、`dual_E.png`。
若某一节显示「缺少……」，说明对应的输入文件没有生成，请回到生成它的那一步检查。

### 第十二步：离线参照（约 45 分钟）

```bash
ssh fa-master 'docker rm -f syn-m3grid 2>/dev/null'
bash deploy/scripts/syn-m3-grid.sh --devices E,G,C --hidden-grid 60 --window-grid 60 \
    --batch-grid 64 --lr-grid 0.001 --grad-clip 39072.0 --max-epochs 300 --patience 20 --omp-threads 1 \
    --node master --detach --out-name m3_marapr_reference.csv --per-epoch-name m3_marapr_reference_per_epoch.csv
# 约 45 分钟后：
bash deploy/scripts/syn-m3-grid.sh --collect --node master \
    --out-name m3_marapr_reference.csv --per-epoch-name m3_marapr_reference_per_epoch.csv
python3 deploy/scripts/m3_coldstart_report.py --dir docs/m3_marapr --timeout-min 120 \
    --offline-csv docs/m3_marapr_reference.csv --offline-excluded docs/m3_marapr_reference_excluded.csv
```

期望：

- `--collect` 拉回三个文件：参照结果、逐轮记录、剔除窗口清单 `m3_marapr_reference_excluded.csv`。
- 冷启动报告第五节为等值核验：训练集相同时，早停集误差应逐位相同（结论列为「通过」）；训练集不同时，
  列出只在一侧被剔除的窗口。

离线参照在 master 上运行的 45 分钟里，可以同时做第十三步。

### 第十三步：逐日通道剖面与 V-M3-4（本地 Mac）

```bash
python3 eda/daily_channel_profile.py \
    --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
    --start 2022-03-01 --end 2022-05-01 --ref-start 2022-03-17 --ref-end 2022-03-24 \
    --scores docs/m3_marapr/m3_scores.jsonl --out docs/m3_marapr/daily_channel_profile.csv
python3 deploy/scripts/m3_v34_report.py --scores docs/m3_marapr/m3_scores.jsonl \
    --profile docs/m3_marapr/daily_channel_profile.csv --start 2022-03-24 --end 2022-05-01 \
    --out-dir docs/m3_marapr/v34
```

- 这里的参照期是本次的阈值校准期 03-17 至 03-24，与第三步（参照期 03-17 至 03-19）不同，输出也写在
  `docs/m3_marapr/` 下，两份剖面不要混用。
- 期望 `docs/m3_marapr/v34/` 下有 `v34_report.md`、`v34_daily.csv`、`v34_daily.png`、`v34_score_hist.png`。
- 报告里的平稳日清单是注入运行选日子的依据：注入要放在四月的平稳日里。

### 第十四步：入库与收尾

```bash
git add -f docs/m3_marapr_eda_rounds.csv docs/m3_marapr/m3_scores.jsonl docs/m3_marapr/m3_tm_log.txt \
    docs/m3_marapr/ckpt_timeline.csv docs/m3_marapr/ckpt_sizes.csv docs/m3_marapr/timeline.log docs/m3_marapr/sizes.log \
    docs/m3_marapr/coldstart_report.md docs/m3_marapr/collect_summary.txt docs/m3_marapr/perf \
    docs/m3_marapr/replay_verify.txt docs/m3_marapr/m2_metrics_final.txt \
    docs/m3_marapr/daily_channel_profile.csv docs/m3_marapr/v34 \
    docs/m3_marapr_reference.csv docs/m3_marapr_reference_per_epoch.csv docs/m3_marapr_reference_excluded.csv \
    docs/m2_probe_corrected.csv docs/reports/m2_java11_marapr-m3_* docs/m2_replay_verify_marapr.csv \
    docs/reports/m3_march_dual_channel_week
git add -f docs/m3_marapr/ckpt_inspect_*.json 2>/dev/null
git commit -m "data(m3): 三月至四月运行（类型 A）结果" && git push origin dev-claude
bash deploy/scripts/syn-m3-diag.sh --since $SINCE --out-dir docs/m3_marapr/diag
git add -f docs/m3_marapr/diag && git commit -m "data(m3): 三月至四月运行的诊断材料" && git push origin dev-claude
ssh fa-master "docker exec jobmanager flink cancel $JID"
bash deploy/scripts/syn-reset-env.sh
ssh fa-master 'rm -rf /opt/fa-iforest/m3march'
gzip -f docs/m2_monitoring_marapr-m3.jsonl
rm -f docs/m3_marapr/scores.jsonl
```

- 第二条 `git add` 在没有进行中检查点明细时会报「did not match」，忽略即可。
- `flink cancel` 只取消第五步提交的作业。
- 监测转储这次不删除，而是压缩后留在本地（不入库，约几十 MB）。上一次删除后，双通道对比就只能等下一次运行；
  这次保留到设计会话接受双通道报告为止，届时再执行 `rm -f docs/m2_monitoring_marapr-m3.jsonl.gz`。

---

## 五、预授权的退路

若作业又在训练后的第一个检查点上失败（确认消息超帧，或 JobManager 内存耗尽），按 2026-10-01 裁决第四节，
复位后只把第五步的 `--checkpoint-ms 30000` 改为 `--checkpoint-ms 86400000`，其余照做。报告中注明，失去的只是
训练后的检查点峰值读数。

## 六、预计耗时

| 步骤 | 墙钟 |
| --- | --- |
| 第三步的两项本地分析（本地 Mac） | 约 15 分钟 |
| 重放 | 约 25 分钟 |
| 冷启动 | 子任务 1 约 45 分钟 |
| 追赶积压与排空 | 约 30 分钟 |
| 核验、收集、探针、双通道对比 | 约 45 分钟 |
| 离线参照（同时做逐日剖面与 V-M3-4） | 约 45 分钟 |

## 七、结果推送之后由代码开发代理完成的工作

1. 本次运行的报告：冷启动、等值核验、点异常通道 ±1%、V-M3-4、V-M3-6。
2. 双通道报告 `docs/reports/m3_march_dual_channel_week.md`：三张图、五条预言逐条核对并附数字、上下文通道的设备内
   检验、点通道等价性条件的核对结果，以及结论「这一周在两个通道上的签名是否符合尺度漂移的理论预期」。
3. B 的缝合窗口核对结论：若告警集中在跨缺口的窗口上，作为数据接入或窗口构造层面的新事实报给设计会话。
4. 按 V-M3-4 报告的四月平稳日清单排出注入计划，连同注入方案中待确认的事项一起送交设计会话。
