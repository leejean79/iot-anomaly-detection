# 第二批在线验收：三月至四月重放操作手册（类型 A，不注入；2026-10-02）

依据：2026-10-02 裁决书《三月重跑（不注入）报告的五项待决事项》。本手册取代 `docs/m3_march_runbook_zh.md`
中第一次运行（类型 A）的部分；注入运行（类型 B）在本次运行之后另给步骤。

## 一、这次运行与上一次的不同

| 项目 | 三月重跑 | 本次 |
| --- | --- | --- |
| 重放区间 | 2022-03-01 至 03-31 | **2022-03-01 至 04-30**（裁决第四节第 2 条，最长干净连续段） |
| 阈值校准集 | 2 天 | **7 天**（第四节第 1 条；作业默认值已改为 7） |
| 离线参照的离群口径 | 60 次判定中任意一次 | **到达滑动步的那一次**（第二节第 1 条，与在线一致） |
| 等值核验 | 早停集误差相差不超过 5% | **先核训练集逐一相同，再要求误差逐位相同**（第二节第 2 条） |
| 点异常通道 ±1% 比较 | 用了含偏差的旧参照表 | **在本次重放上现算修正口径的参照表**（第三节） |
| V-M3-4 | 留出周误报率 | **逐日告警率与通道剖面并排；平稳日误报率；共模比例**（第四节第 3、4 条） |
| V-M3-6 | 只有训练时长 | **另测逐窗推理时延、转发延迟、任务管理器内存**（第五节） |

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

---

## 二、开跑前（本地 Mac，仓库根目录）

### 第一步：拉取、打包、上传

```bash
git pull --rebase origin dev-claude
mvn clean package -DskipTests
bash deploy/scripts/check-jar.sh
bash deploy/scripts/syn-upload-m1.sh --jar-only
```

期望 `check-jar.sh` 全部 PASS，其中包括本次新增的三项：

- 离线网格写剔除清单（`excluded-csv`）；
- 在线算子记录剔除窗口（`excluded training window ending at round`）；
- 推理时延指标（`m3_inference_latency_ms`）。

### 第二步：用 EDA 数出本区间的期望轮数（断言一要用）

原始数据只在本地 Mac 上：

```bash
python3 eda/count_rounds.py \
    --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
    --start 2022-03-01 --end 2022-05-01 --out docs/m3_marapr_eda_rounds.csv
```

期望：终端打印总轮数，约 400 万。把这个数记下来，第七步要用。

### 第三步：复位，作为开跑门槛

```bash
bash deploy/scripts/syn-reset-env.sh
```

期望核对表全部 PASS，退出码 0。

---

## 三、运行

### 第四步：记下时刻，然后提交作业

```bash
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

请把 `JID` 与 `SINCE` 的值抄下来。提交后若取消重交，重交前要重新执行第一行。

### 第五步：在 master 上启动监测，然后启动重放

```bash
bash deploy/scripts/syn-m3-watch.sh start --jid $JID --since $SINCE
bash deploy/scripts/syn-replay.sh --speedup 3600 --start 2022-03-01 --end 2022-05-01
```

两者都在 master 上无人值守地运行，本地终端可以关掉。重放约 25 分钟；约 9 分钟后首批设备进入训练，冷启动约
45 分钟，之后作业追赶积压。

### 第六步：查看进度（随时，约 1.5 小时后看一次即可）

```bash
bash deploy/scripts/syn-m3-watch.sh status
```

等它显示「已进入在线 8 台」、重放器已退出（`rc=0`），再做第七步。

若作业失败，先诊断，把材料提交后告诉我：

```bash
bash deploy/scripts/syn-m3-diag.sh --since $SINCE --out-dir docs/m3_marapr/diag
```

---

## 四、运行结束后（全部在复位之前完成）

### 第七步：重放完整性核验（在取消作业之前）

```bash
bash deploy/scripts/syn-replay-verify.sh --start-utc 2022-03-01T00:00:00Z --end-utc 2022-05-01T00:00:00Z \
    --expected-total <第二步的总轮数> --tol-pct 0.2 --max-messages 6000000
```

期望五条断言全部 PASS，退出码 0。`--max-messages 6000000` 是必需的：默认 300 万条覆盖不了两个月约 400 万轮。

### 第八步：排空，然后收集结果与性能指标

```bash
bash deploy/scripts/syn-m2-metrics.sh > /tmp/m3m_t1.txt; sleep 120; bash deploy/scripts/syn-m2-metrics.sh > /tmp/m3m_t2.txt
diff /tmp/m3m_t1.txt /tmp/m3m_t2.txt && echo "已排空"
bash deploy/scripts/syn-m3-watch.sh stop --out-dir docs/m3_marapr
bash deploy/scripts/syn-m3-march-collect.sh --out-dir docs/m3_marapr --since $SINCE
bash deploy/scripts/syn-m3-perf-collect.sh --since $SINCE --out-dir docs/m3_marapr/perf
```

期望：

- `docs/m3_marapr/collect_summary.txt` 中「训练完成的设备数」为 8；
- `docs/m3_marapr/perf/perf_summary.md` 给出三张表：推理时延、转发延迟、任务管理器内存。若某项显示
  「Prometheus 没有该指标的数据」，请告诉我。

### 第九步：点异常通道 ±1% 比较（裁决第三节）

先在本次重放的 `synergia-m1-out` 上现算修正口径的参照表，再比较：

```bash
bash deploy/scripts/syn-m2-probe.sh --r-grid 0.75,1.0,1.5 --k-grid 10 --max-messages 6000000 \
    --out-name m2_probe_corrected.csv
bash deploy/scripts/syn-m2-baseline.sh --tag marapr-m3 --probe-ref docs/m2_probe_corrected.csv
```

- 半径网格取 0.75、1.0、1.5，覆盖各设备实际使用的半径（D 为 0.75，G 为 1.5，其余为 1.0）。
- 期望第二条输出逐设备表，八台的相对差都在 ±1% 以内。
- 探针要转储约 400 万轮，master 上会多占约 2 GB 磁盘，第十二步复位时清掉。

### 第十步：离线参照（约 45 分钟）

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

- `--collect` 拉回三个文件：参照结果、逐轮记录、剔除窗口清单 `m3_marapr_reference_excluded.csv`；
- 冷启动报告第五节为等值核验：训练集相同时，早停集误差应逐位相同（结论列为「通过」）；训练集不同时，
  列出只在一侧被剔除的窗口。

### 第十一步：逐日通道剖面与 V-M3-4（本地 Mac）

```bash
python3 eda/daily_channel_profile.py \
    --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv \
    --start 2022-03-01 --end 2022-05-01 --ref-start 2022-03-17 --ref-end 2022-03-24 \
    --scores docs/m3_marapr/m3_scores.jsonl --out docs/m3_marapr/daily_channel_profile.csv
python3 deploy/scripts/m3_v34_report.py --scores docs/m3_marapr/m3_scores.jsonl \
    --profile docs/m3_marapr/daily_channel_profile.csv --start 2022-03-24 --end 2022-05-01 \
    --out-dir docs/m3_marapr/v34
```

- 剖面的参照期必须是阈值校准期 03-17 至 03-24，「标定期宽度」即该期间的 P10 至 P90 宽度。
- 期望 `docs/m3_marapr/v34/` 下有 `v34_report.md`、`v34_daily.csv`、`v34_daily.png`、`v34_score_hist.png`。
- 报告里的平稳日清单也是注入运行选日子的依据：注入要放在四月的平稳日里。

### 第十二步：入库与收尾

```bash
git add -f docs/m3_marapr_eda_rounds.csv docs/m3_marapr/m3_scores.jsonl docs/m3_marapr/m3_tm_log.txt \
    docs/m3_marapr/ckpt_timeline.csv docs/m3_marapr/ckpt_sizes.csv docs/m3_marapr/timeline.log docs/m3_marapr/sizes.log \
    docs/m3_marapr/coldstart_report.md docs/m3_marapr/collect_summary.txt docs/m3_marapr/perf \
    docs/m3_marapr/daily_channel_profile.csv docs/m3_marapr/v34 \
    docs/m3_marapr_reference.csv docs/m3_marapr_reference_per_epoch.csv docs/m3_marapr_reference_excluded.csv \
    docs/m2_probe_corrected.csv docs/reports/m2_java11_marapr-m3_* docs/m2_replay_verify.csv
git add -f docs/m3_marapr/ckpt_inspect_*.json 2>/dev/null
git commit -m "data(m3): 三月至四月运行（类型 A）结果" && git push origin dev-claude
bash deploy/scripts/syn-m3-diag.sh --since $SINCE --out-dir docs/m3_marapr/diag
git add -f docs/m3_marapr/diag && git commit -m "data(m3): 三月至四月运行的诊断材料" && git push origin dev-claude
ssh fa-master "docker exec jobmanager flink cancel $JID"
bash deploy/scripts/syn-reset-env.sh
ssh fa-master 'rm -rf /opt/fa-iforest/m3march'
rm -f docs/m3_marapr/scores.jsonl docs/m2_monitoring_marapr-m3.jsonl
```

- 第二条 `git add` 在没有进行中检查点明细时会报「did not match」，忽略即可。
- `flink cancel` 只取消第四步提交的作业。

---

## 五、预授权的退路

若作业又在训练后的第一个检查点上失败（确认消息超帧，或 JobManager 内存耗尽），按 2026-10-01 裁决第四节，
复位后只把第四步的 `--checkpoint-ms 30000` 改为 `--checkpoint-ms 86400000`，其余照做。报告中注明，失去的只是
训练后的检查点峰值读数。

## 六、预计耗时

| 步骤 | 墙钟 |
| --- | --- |
| 重放 | 约 25 分钟 |
| 冷启动 | 子任务 1 约 45 分钟 |
| 追赶积压与排空 | 约 30 分钟 |
| 核验、收集、探针、离线参照 | 约 1.5 小时 |
| EDA 与 V-M3-4（本地 Mac） | 约 15 分钟 |
