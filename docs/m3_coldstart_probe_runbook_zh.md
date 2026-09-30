# M3 在线冷启动短重放验证（补遗三步骤 C）操作手册（2026-09-30）

## 这次运行是什么

这是 M3 第一次在集群上真正打分，也就是 `docs/m3_experiment_runbook.md` 阶段二的「类型 A：联合作业 +
重放」。同时它是 2026-09-30 裁决要求在三月重放之前完成的补遗三步骤 C。到目前为止，M3 的全部证据都来自
离线网格与单元测试，在线冷启动这条路径一次都没有在集群上执行过。

做法是把「每天折合多少轮」从 8,640 缩小到 864（十分之一），并把 M1 的标定期从 7 天缩到 1 天，使八台设备
在一段约 7 分钟的重放里就进入训练。本次运行要回答以下四个问题：

1. 八台设备是否都依次经过「收集、训练、标定、在线」四个相位，并在在线后写出上下文通道评分。
2. 每台设备在算子内实际训练了多久、训练了多少轮。
3. 训练期间挂起的检查点，是否在训练结束后正常完成，而不是超时失败。
4. 作业的重启次数是否为零。

本次运行的评分**不用于任何验收**：分段不是真实的天数，M1 的标定也只有一天。

## 运行参数

| 项目 | 取值 | 说明 |
| --- | --- | --- |
| 重放区间 | 2022-03-01 至 2022-03-04（3 天） | 各设备在这 3 天里都有 24,000 个以上的轮 |
| 重放倍速 | 600 | 3 天数据约 7.2 分钟重放完 |
| M1 标定轮数 | `--warmup-rounds 8640`（1 天） | 默认 7 天，本次缩短 |
| 每天折合轮数 | `--m3-rounds-per-day 864` | 训练在 (7 + 2 + 2) × 864 = 9,504 个在线轮后触发 |
| M3 训练参数 | 与三月重放完全相同 | 隐藏层 60、窗口 60、小批量 64、学习率 0.001、裁剪 39072.0、上限 300 轮、耐心 20、目标逆序 |
| 检查点 | 间隔 30 秒，超时 60 分钟，容忍连续失败 3 次 | 2026-09-30 裁决第一节 |
| ND4J 线程数 | TaskManager 环境变量 `OMP_NUM_THREADS=1` | 2026-09-30 用户确认，第一步落实 |

按每天 864 轮切分时，每台设备约有 100 个训练窗、28 个早停窗、28 个标定窗，每轮训练做 2 次参数更新。
按时间推算，重放开始后约 5 分钟（1 天标定、1.1 天收集、再加点异常通道 1 小时窗口的延迟），各设备陆续进入训练。

### 八台设备落在哪个子任务上

M3 算子按设备分键，并行度为 8。按 Flink 1.13.6 的键组分配（最大并行度 128）计算，八台设备的分布是：

| 子任务 | 设备 |
| --- | --- |
| 1 | B、C、E |
| 2 | H |
| 5 | F |
| 6 | A |
| 7 | D、G |
| 0、3、4 | 无 |

同一子任务上的设备共用一个任务线程，只能**依次**训练。因此子任务 1 的任务线程会被 B、C、E 三次训练
连续占住，这是整个冷启动里最慢的一段；由于反压，整个作业在这段时间里都会停止推进。本次运行的日志
在「entering TRAINING」一行里带上了子任务编号，可以直接核对上表。

---

## 第一步：TaskManager 改为单线程（只重建两个 taskmanager 容器）

**执行环境**：本地 Mac，仓库根目录。
**前置条件**：三台机器都能用 `fa-master`、`fa-worker1`、`fa-worker2` 登录；没有本项目的作业在运行。

先确认没有正在运行的作业。如果列出了旧项目 FA-iForest 的作业，请先停下来告诉我，因为重建 taskmanager
会让它们从检查点重启：

```bash
ssh fa-master "docker exec jobmanager flink list"
```

然后把新的 worker compose 文件传到两台 worker，并只重建 taskmanager 服务：

```bash
git pull --rebase origin dev-claude
set -a; source deploy/.env; set +a
scp deploy/compose/docker-compose.worker.yml fa-worker1:/opt/fa-iforest/compose/
scp deploy/compose/docker-compose.worker.yml fa-worker2:/opt/fa-iforest/compose/
ssh fa-worker1 "cd /opt/fa-iforest/compose && BROKER_ID=2 NODE_SELF_IP=$NODE_WORKER1_IP \
    docker compose -f docker-compose.worker.yml --env-file ../.env up -d --no-deps taskmanager"
ssh fa-worker2 "cd /opt/fa-iforest/compose && BROKER_ID=3 NODE_SELF_IP=$NODE_WORKER2_IP \
    docker compose -f docker-compose.worker.yml --env-file ../.env up -d --no-deps taskmanager"
```

`--no-deps taskmanager` 保证只动 taskmanager，不动同一个 compose 文件里的 Kafka。按不加数据卷的约定，
Kafka 容器一旦被重建，数据就没有了，所以紧接着必须核对：

```bash
ssh fa-worker1 "docker ps --format '{{.Names}}  创建于 {{.CreatedAt}}  {{.Status}}' | grep -E 'kafka|taskmanager'"
ssh fa-worker2 "docker ps --format '{{.Names}}  创建于 {{.CreatedAt}}  {{.Status}}' | grep -E 'kafka|taskmanager'"
ssh fa-worker1 "docker exec taskmanager-2 printenv OMP_NUM_THREADS"
ssh fa-worker2 "docker exec taskmanager-3 printenv OMP_NUM_THREADS"
ssh fa-master "curl -s http://$NODE_MASTER_IP:8081/overview"
```

**期望产出**：

- `kafka-2` 与 `kafka-3` 的创建时间仍是原来的日期，没有变成刚才。
- `taskmanager-2` 与 `taskmanager-3` 的创建时间是刚才，状态为 `Up`。
- 两条 `printenv` 都输出 `1`。
- 最后一条输出里有 `"taskmanagers":2` 和 `"slots-total":8`。

**常见失败**：

- 如果 `docker compose` 报找不到变量或容器名冲突，请把完整输出发给我，不要改用 `2-up-all.sh`。
- 如果 Kafka 的创建时间变了，说明 Kafka 被重建了。这时先停下来告诉我；三月之前 Kafka 里没有需要保留
  的数据，但要确认三台代理都已正常加入集群。
- 如果 `taskmanagers` 少于 2，等 30 秒再查一次；仍然不够，就查看 `docker logs --tail 50 taskmanager-2`。

## 第二步：构建并上传 jar

**执行环境**：本地 Mac，仓库根目录，JDK 11。

```bash
mvn clean package -DskipTests
bash deploy/scripts/check-jar.sh
bash deploy/scripts/syn-upload-m1.sh --jar-only
```

**期望产出**：`BUILD SUCCESS`；`check-jar.sh` 全部 PASS，其中包括新增的三项：`checkpoint-timeout-min`、
`m3-rounds-per-day` 与「冷启动日志带子任务编号」。

**常见失败**：`check-jar.sh` 有 FAIL，说明 jar 是旧的，重新执行 `mvn clean package -DskipTests`。

## 第三步：复位与开跑门槛

**执行环境**：本地 Mac，仓库根目录。

```bash
bash deploy/scripts/syn-reset-env.sh
```

**期望产出**：核对表全部 PASS，退出码 0。核对表新增了「TM 单线程」一项，它核对两台 TaskManager 的
`OMP_NUM_THREADS` 都是 1。这一步会清空 Kafka 里现有的六月段数据；六月段已经登记为文件，不再使用。

**常见失败**：任何一项 FAIL 都先处理，不要往下走。「TM 单线程」FAIL 说明第一步没有生效，
回到第一步重新核对。

## 第四步：提交作业

**执行环境**：本地 Mac，仓库根目录。
**前置条件**：第三步全部 PASS。

先记下提交前的时刻，第九步收集日志时要用：

```bash
SINCE=$(ssh fa-master date -u +%Y-%m-%dT%H:%M:%SZ); echo "SINCE=$SINCE"
```

然后提交：

```bash
bash deploy/scripts/syn-submit-m2.sh --extra '--m3-enabled true
    --warmup-rounds 8640 --m3-rounds-per-day 864
    --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 2 --m3-window-length 60
    --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0
    --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true
    --checkpoint-ms 30000 --checkpoint-timeout-min 60 --checkpoint-tolerable-failures 3' \
    2>&1 | tee /tmp/submit_probe.log
grep -E 'W=|Warmup rounds|Ckpt|M3 ' /tmp/submit_probe.log
JID=$(grep -oE 'JobID [a-f0-9]{32}' /tmp/submit_probe.log | awk '{print $2}' | head -1); echo "JID=$JID"
```

**期望产出**：

- `W=3600s`
- `Warmup rounds:   8640 (explicit --warmup-rounds)`
- `Ckpt interval/timeout: 30000 ms / 60 min`
- `Ckpt tolerable failures: 3  (job restarts on consecutive failure no. 4)`
- `M3 train/es/th:  7d/2d/2d`、`M3 window: 60 rounds`、`M3 max epochs: 300 (patience=20)`、
  `M3 hidden size: 60`、`M3 batch size: 64`
- `M3 rounds/day:   864  [COLD-START PROBE MODE: …]`
- 最后一行 `M2Job is RUNNING (JobID …)`，`JID` 变量打印出同一个编号。

**常见失败**：60 秒内没有进入 RUNNING 时，用 `ssh fa-master "docker exec jobmanager flink list"` 查看，
常见原因是槽位不足。`SINCE` 与 `JID` 两个变量只在当前终端里有效，下面几步要在同一个终端执行，
或者把它们的值抄下来。

## 第五步：启动检查点时间线（另开一个终端，一直开着）

**执行环境**：本地 Mac，仓库根目录。
**前置条件**：第四步的作业处于 RUNNING。

```bash
bash deploy/scripts/syn-m3-coldstart-watch.sh --jid <第四步的 JID> --out docs/m3_coldstart/ckpt_timeline.csv
```

**期望产出**：每 15 秒一行。训练开始之前，「完成」计数每 30 秒加一，「进行中」多为 0。训练期间，
「进行中=1」，「挂起=编号 已 N s」里的 N 持续增长，「完成」计数不变。训练全部结束后，「完成」计数加一，
「最近完成」那次检查点的端到端秒数接近最慢子任务的训练时长。「重启」始终为 0。

**常见失败**：退出码 2 表示找不到作业，请检查 `--jid` 是否正确。

## 第六步：启动重放

**执行环境**：本地 Mac，仓库根目录。

```bash
bash deploy/scripts/syn-replay.sh --speedup 600 --start 2022-03-01 --end 2022-03-04
bash deploy/scripts/syn-replay.sh logs      # 观察进度，Ctrl+C 只停止跟踪，不停止重放
```

**期望产出**：约 7 分钟后重放器打印 `Finished.` 汇总块与 `rc=0`。本次不带注入参数。

## 第七步：观察冷启动（各开一个终端）

**执行环境**：本地 Mac。重放开始后约 5 分钟会出现训练记录。

```bash
ssh fa-worker1 "docker logs -f --since $SINCE taskmanager-2 2>&1 | grep --line-buffered -E '\[M3\]|OpenMP BLAS'"
ssh fa-worker2 "docker logs -f --since $SINCE taskmanager-3 2>&1 | grep --line-buffered -E '\[M3\]|OpenMP BLAS'"
```

**期望产出**：每台设备依次出现以下各行：

1. `Device X (subtask N) entering TRAINING: … train windows … early-stop windows`
2. 逐轮的 `training epoch`
3. `trained: … epochs (last improvement at epoch …, longest plateau …), early-stop loss=…, …s`
4. `calibrated`
5. `entering ONLINE`

子任务编号应与上面的分布表一致，B、C、E 三台依次训练，而不是同时训练。出现 `REPORT` 行的设备要记下来。
在每天 864 轮的条件下，每轮只有 2 次参数更新，耐心 20 轮只对应 40 次更新，所以本次出现平台接近耐心的
上报并不说明全尺寸训练有问题，只作记录。

## 第八步：确认作业已排空

**执行环境**：本地 Mac，仓库根目录。
**前置条件**：重放已打印 `rc=0`，并且日志里八台设备都出现了 `entering ONLINE`。

```bash
bash deploy/scripts/syn-m2-metrics.sh | tee /tmp/probe_t1.txt
sleep 120
bash deploy/scripts/syn-m2-metrics.sh | tee /tmp/probe_t2.txt
diff /tmp/probe_t1.txt /tmp/probe_t2.txt && echo "已排空"
```

**期望产出**：打印「已排空」。如果两次读数不同，说明作业还在追赶积压，再等两分钟后重复。

## 第九步：收集、出报告

**执行环境**：本地 Mac，仓库根目录。**必须在任何清理之前执行。**

```bash
bash deploy/scripts/syn-m3-march-collect.sh --out-dir docs/m3_coldstart --since $SINCE
python3 deploy/scripts/m3_coldstart_report.py --dir docs/m3_coldstart --rounds-per-day 864 \
    --offline-csv docs/m3_grid_rerun.csv,docs/m3_grid.csv
```

然后在第五步的终端里按 Ctrl+C 停止检查点时间线。

**期望产出**：

- `docs/m3_coldstart/collect_summary.txt` 中「M3 训练完成的设备数」为 8。「其中上下文通道（m3_context）」
  大于 0，这就是 M3 的第一批在线评分。
- 「注入真值缺失」的提示属于正常现象，因为本次没有注入。
- `docs/m3_coldstart/coldstart_report.md` 包含四节：逐设备、逐子任务、检查点与重启、外推到全尺寸。

**常见失败**：报告脚本退出码 2 表示日志里没有训练记录，请检查 `SINCE` 是否早于提交时刻。

## 第十步：提交结果并清理

**执行环境**：本地 Mac，仓库根目录。

```bash
git add -f docs/m3_coldstart/coldstart_report.md docs/m3_coldstart/ckpt_timeline.csv \
        docs/m3_coldstart/m3_tm_log.txt docs/m3_coldstart/m3_scores.jsonl docs/m3_coldstart/collect_summary.txt
git commit -m "data(m3): 冷启动短重放验证结果" && git push origin dev-claude
ssh fa-master "docker exec jobmanager flink cancel $JID"
bash deploy/scripts/syn-reset-env.sh
ssh fa-master 'rm -rf /opt/fa-iforest/m3march'
rm -f docs/m3_coldstart/scores.jsonl
```

`.gitignore` 忽略了 csv、txt 与 jsonl 文件，所以这里要用 `git add -f`。`flink cancel` 只取消本次自己提交的作业，这里用的是第四步记下的 `JID`。`scores.jsonl` 含点异常通道的
全部离群记录，体积较大，已经抽出的 `m3_scores.jsonl` 足以说明上下文通道的输出，所以不入库，并在本地删除。
master 上 `m3march/` 下的转储在结果入库后也不再需要。

---

## 判读标准

| 问题 | 通过的条件 |
| --- | --- |
| 相位跃迁 | 八台设备都出现 `entering TRAINING`、`trained`、`calibrated`、`entering ONLINE` |
| 在线评分 | `m3_scores.jsonl` 中八台设备都有记录 |
| 检查点 | 训练期间有检查点挂起，训练结束后完成；失败次数为 0 |
| 重启 | 重启次数始终为 0 |
| 训练时长 | 报告第一、二节给出逐设备与逐子任务的实测值，第四节给出全尺寸外推 |

报告第四节的外推值要交给设计会话，用来替换三月重放里 60 分钟的检查点超时。外推的做法和它的局限，
见 `docs/reports/m3_coldstart_probe_notes_for_decision.md`。
