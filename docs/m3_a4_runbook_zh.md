# 补遗四（把 M3 训练移出任务线程）运行手册：步骤 C 短重放预验证

版本：2026-10-13。依据交接文档 `docs/handover/handover_m3_addendum4_async_training_zh.md`（开发设计日志 0.82 版）。

## 这次要做什么

补遗四把上下文异常检测模块（M3）的冷启动训练从 Flink 任务线程移到专用的训练线程上。代码已经完成，
单元测试全部通过。这份手册覆盖交接文档第四节第 1 条的步骤 C：在集群上用短重放做五件事，每件给出数字。

1. **任务线程可用性**：训练期间，检查点端到端时长不超过训练前正常值的两倍；全程零检查点失败。
2. **水位线**：训练期间，各子任务的输入水位线持续推进；记录输出水位线的保持时长与缓冲排空时长。
3. **取消**：在某台设备训练中途取消作业，端到端时长在三十秒以内。
4. **重启**：在某台设备训练中途杀掉承载它的任务管理器，作业从检查点恢复后重新提交训练，最终结果与未被打断
   的一次运行逐位相同，缓冲内容不丢。
5. **并发**：训练线程数取一、二、四各跑一次，据此定 `--train-threads` 的默认值。

这些运行的评分**不用于任何验收**：「每天折合轮数」缩小为 864，分段不是真实的天数，M1 的标定也只有一天。

全量重放（第四节第 2 条，验收项 A4-1 至 A4-5）的手册在步骤 C 回传、设计会话对回复中的三点上报作出决定之后
另行给出，因为重放倍速与训练线程数都取决于步骤 C 的数字。

## 共同参数

| 项目 | 取值 | 说明 |
| --- | --- | --- |
| 重放区间 | 2022-03-01 至 2022-03-05（4 天） | 标定 1 天，收集 16 × 864 轮约 1.6 天，之后约 1.4 天在线 |
| 重放倍速 | 120（取消那次可用 600） | 见下方「为什么是 120 倍」 |
| M1 标定轮数 | `--warmup-rounds 8640` | 1 天 |
| 每天折合轮数 | `--m3-rounds-per-day 864` | 训练在 (7 + 2 + 7) × 864 = 13,824 个在线轮后触发 |
| M3 训练参数 | 与注入运行相同 | 隐藏层 60、窗口 60、小批量 64、学习率 0.001、裁剪 39072.0、上限 300 轮、耐心 20、目标逆序、标定 7 天 |
| 检查点 | 间隔 30 秒，超时 10 分钟，容忍连续失败 3 次 | 超时按补遗四第五节恢复为生产值 10 分钟 |
| 空闲时限 | `--idle-wall 60` | 补遗四第五节的生产值，未按倍速换算（见回复第三点） |
| 训练线程数 | 每次运行不同，见第二节 | `--train-threads` |

**为什么是 120 倍。** 训练期间到达的轮进入有界缓冲，上限是「标定天数加两天」，在每天 864 轮时为
9 × 864 = 7,776 轮，按十秒一轮折合约 21.6 小时的事件时间。重放把事件时间压缩了「倍速」那么多倍，所以训练
（含排队等待）期间到达的事件时间等于墙钟训练时长乘以倍速。补遗三的短重放里单台训练 30 至 95 秒，线程数为一
时一台任务管理器上的四台设备依次训练，最长约 300 秒；乘以 120 倍约 10 小时，不到上限的一半。倍速再高，缓冲
就会溢出（溢出即事故，见交接文档第二节第 3 条）。每次运行约 48 分钟。

---

## 第一节：一次性准备

### 第一步：确认集群上没有其他作业

**执行环境**：本地 Mac，仓库根目录。

```bash
git pull --rebase origin dev-claude
ssh fa-master "docker exec jobmanager flink list"
```

**期望产出**：没有任何正在运行的作业。**如果列出了旧项目 FA-iForest 的作业，请先停下来告诉我**：后面的重建
与强杀任务管理器都会让同一任务管理器上的作业重启。

### 第二步：任务管理器进程总量提到 5120 兆（补遗四第五节，执行 10 月 9 日裁决第二节第 6 条）

**执行环境**：本地 Mac，仓库根目录。**前置条件**：第一步没有作业在运行；本地 `deploy/.env` 中
`TM_HEAP_MB=5120`、`SYN_JAVACPP_MAXPHYSICALBYTES=5120m`（与 `deploy/env.example` 一致）。

```bash
grep -E '^(TM_HEAP_MB|SYN_JAVACPP_MAXPHYSICALBYTES)=' deploy/.env
bash deploy/scripts/syn-tm-recreate.sh --check-only
bash deploy/scripts/syn-tm-recreate.sh
```

**期望产出**：第一条打印 `TM_HEAP_MB=5120` 与 `SYN_JAVACPP_MAXPHYSICALBYTES=5120m`；`--check-only` 打印两台
worker 的预计剩余内存，都不低于 1024 兆；重建后两台的 Kafka 创建时间不变，JavaCPP 物理上限的两个取值都是
`5120m`。如果两台任务管理器此前已经按 5120 兆重建过，第三条可以跳过。

**常见失败**：退出码 4 表示内存余量不足，不做任何改动，请把输出发给我；退出码 3 表示有作业在运行；打印
「Kafka 被重建」时立即停下告诉我。

### 第三步：构建、核对并上传 jar

**执行环境**：本地 Mac，仓库根目录，JDK 11。

```bash
mvn clean package -DskipTests
bash deploy/scripts/check-jar.sh
bash deploy/scripts/syn-upload-m1.sh --jar-only
```

**期望产出**：`BUILD SUCCESS`；`check-jar.sh` 全部 PASS，其中包括新增的四项：训练执行器（`m3-train-`）、
输出水位线保持（`m3_watermark_hold_sec`）、训练缓冲（`m3_train_buffer_overflow`）与参数 `train-threads`。

**常见失败**：任何一项 FAIL 都说明 jar 是旧的，重新执行 `mvn clean package -DskipTests`。

---

## 第二节：五次运行

| 标签 | 线程数 | 倍速 | 扰动 | 回答的问题 |
| --- | --- | --- | --- | --- |
| `t1` | 1 | 120 | 无 | 第 1、2、5 件事 |
| `t2` | 2 | 120 | 无 | 第 1、2、5 件事，也是 `kill` 的参照 |
| `t4` | 4 | 120 | 无 | 第 1、2、5 件事 |
| `kill` | 2 | 120 | 训练中强杀任务管理器 | 第 4 件事 |
| `cancel` | 2 | 600 | 训练中取消作业 | 第 3 件事 |

`kill` 必须与 `t2` 使用完全相同的参数，否则逐位比较没有意义。每次运行都按下面的第四步至第十步执行，只替换
标签、线程数与倍速。建议按表中顺序执行；每次运行约一小时（`cancel` 约 25 分钟）。

### 第四步：复位

**执行环境**：本地 Mac，仓库根目录。

```bash
bash deploy/scripts/syn-reset-env.sh
```

**期望产出**：核对表全部 PASS，退出码 0（其中「TM 单线程」一项核对两台任务管理器的 `OMP_NUM_THREADS` 都是 1）。

**常见失败**：任何一项 FAIL 都先处理，不要往下走。

### 第五步：提交作业

**执行环境**：本地 Mac，仓库根目录。**前置条件**：第四步全部 PASS。先设好本次运行的标签与线程数：

```bash
RUN=t2; THREADS=2          # 按第二节的表替换：t1/1、t2/2、t4/4、kill/2、cancel/2
mkdir -p docs/m3_a4/stepc_$RUN
SINCE=$(ssh fa-master date -u +%Y-%m-%dT%H:%M:%SZ); echo "SINCE=$SINCE"
bash deploy/scripts/syn-submit-m2.sh --extra "--m3-enabled true
    --warmup-rounds 8640 --m3-rounds-per-day 864
    --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 7 --m3-window-length 60
    --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0
    --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true
    --checkpoint-ms 30000 --checkpoint-timeout-min 10 --checkpoint-tolerable-failures 3
    --idle-wall 60 --train-threads $THREADS" 2>&1 | tee docs/m3_a4/stepc_$RUN/submit.log
grep -E 'Idle timeout|Ckpt|M3 rounds|M3 async|Device subtasks' docs/m3_a4/stepc_$RUN/submit.log
JID=$(grep -oE 'JobID [a-f0-9]{32}' docs/m3_a4/stepc_$RUN/submit.log | awk '{print $2}' | head -1); echo "JID=$JID"
```

**期望产出**：

- `Idle timeout:    60 s`
- `Ckpt interval/timeout: 30000 ms / 10 min`
- `M3 rounds/day:   864  [COLD-START PROBE MODE: …]`
- `M3 async train:  threads=<THREADS> per TaskManager, drain batch=200 rounds, poll interval=30 s (addendum 4)`
- `Device subtasks:` 八台设备落在八个不同的子任务上
- 最后打印 `M2Job is RUNNING (JobID …)`，`JID` 打印出同一个编号。

**常见失败**：60 秒内没有进入 RUNNING 时，用 `ssh fa-master "docker exec jobmanager flink list"` 查看。
`RUN`、`THREADS`、`SINCE`、`JID` 只在当前终端有效，下面几步在同一个终端执行，另开的终端要把值抄过去。

### 第六步：启动两个采样（各开一个终端，一直开着）

**执行环境**：本地 Mac，仓库根目录。**前置条件**：第五步的作业处于 RUNNING。

```bash
# 终端二：检查点时间线（每 15 秒一行）
bash deploy/scripts/syn-m3-coldstart-watch.sh --jid <JID> --out docs/m3_a4/stepc_<RUN>/ckpt_timeline.csv
# 终端三：水位线与缓冲（每 10 秒一行）
python3 deploy/scripts/m3_a4_watch.py --jid <JID> --out docs/m3_a4/stepc_<RUN>/wm_buffer.csv
```

**期望产出**：终端二与以前相同；训练期间「进行中」的检查点应当在几秒内完成，不再出现长时间挂起。终端三每次
采样打印八个子任务的「缓冲 N 保持 Xs 入 <输入水位线> 出 <输出水位线>」；训练开始前缓冲都是 0，输入与输出
水位线相同；训练期间缓冲增长、输入水位线继续推进，输出水位线停在缓冲头；排空后两者重新一致。

**常见失败**：终端三提示「指标尚未出现」时，等作业处理第一批数据后会自动出现；退出码 2 表示 jar 不是补遗四
之后的版本或 `JID` 不对。

### 第七步：启动重放

**执行环境**：本地 Mac，仓库根目录。

```bash
bash deploy/scripts/syn-replay.sh --speedup 120 --start 2022-03-01 --end 2022-03-05   # cancel 那次用 --speedup 600
bash deploy/scripts/syn-replay.sh status
```

**期望产出**：重放在 master 的 tmux 会话里运行，约 48 分钟后日志打印 `Finished.` 与 `rc=0`
（查看：`ssh fa-master "grep -E 'Finished|rc=' /opt/fa-iforest/syn-replay.log | tail -3"`）。

### 第八步：观察训练（可选，另开终端）

```bash
ssh fa-worker1 "docker logs -f --since $SINCE taskmanager-2 2>&1 | grep --line-buffered -E '\[M3\]'"
ssh fa-worker2 "docker logs -f --since $SINCE taskmanager-3 2>&1 | grep --line-buffered -E '\[M3\]'"
```

**期望产出**：重放开始约 26 分钟后（120 倍时），每台设备依次出现：`entering TRAINING`、`training task N
submitted to the executor`、`training started on m3-train-K after Xs in the executor queue`、逐轮的
`training epoch`、`trained:`、`calibrated:`、`result collected on the task thread … N buffered rounds to drain`、
`entering ONLINE`、`buffer drained in Xs`，以及 `Output watermark hold released after Xs (subtask N)`。线程数为
一时，同一任务管理器上的设备在 `training started` 前会排队；`t4` 时四台同时开始。出现 `REPORT` 的行要记下来。

### 第九步：扰动（只在 `kill` 与 `cancel` 两次运行中执行）

**执行环境**：本地 Mac，仓库根目录。**前置条件**：第七步的重放已启动。脚本会自己等到第一台设备开始训练，再
等 15 秒后动手，所以可以在启动重放后立即执行。

```bash
# 只在 RUN=kill 时执行：
bash deploy/scripts/syn-m3-a4-disrupt.sh kill --jid $JID --since $SINCE --out docs/m3_a4/stepc_kill/kill.txt
# 只在 RUN=cancel 时执行：
bash deploy/scripts/syn-m3-a4-disrupt.sh cancel --jid $JID --since $SINCE --out docs/m3_a4/stepc_cancel/cancel.txt
```

**期望产出**：

- `kill`：打印被选中的设备与任务管理器、强杀时刻、容器重新启动时刻，以及作业重新 RUNNING 的时刻与重启计数 1。
  之后日志里应出现 `is in TRAINING without a model and nothing in flight (restored from a checkpoint): resubmitting`，
  该设备重新训练并最终进入在线。作业恢复后继续第十步，与其他运行相同。
- `cancel`：打印「取消端到端 N 秒（要求不超过 30 秒）」与该任务管理器日志里的 `training interrupted` 和
  `Training executor shut down` 两行。`cancel` 这次运行到此结束：停掉两个采样（Ctrl+C），执行
  `bash deploy/scripts/syn-replay.sh stop`，然后直接跳到下一次运行的第四步。

**常见失败**：退出码 3 表示集群上还有其他作业，脚本没有做任何扰动，请告诉我；退出码 5 表示取消 120 秒后仍未
完成，或强杀后 300 秒作业仍未恢复，请把 `ssh fa-master "docker logs --tail 200 jobmanager"` 的输出发给我。

### 第十步：确认排空、收集结果、取消作业

**执行环境**：本地 Mac，仓库根目录。**前置条件**：重放已打印 `rc=0`；日志里八台设备都出现了 `buffer drained`
（或收回时缓冲为 0）。

```bash
bash deploy/scripts/syn-m2-metrics.sh > /tmp/a4_t1.txt; sleep 120
bash deploy/scripts/syn-m2-metrics.sh > /tmp/a4_t2.txt
diff /tmp/a4_t1.txt /tmp/a4_t2.txt && echo "已排空"
bash deploy/scripts/syn-m3-march-collect.sh --out-dir docs/m3_a4/stepc_$RUN --since $SINCE
grep -c 'buffer drained\|result collected' docs/m3_a4/stepc_$RUN/m3_tm_log.txt
ssh fa-master "docker exec jobmanager flink cancel $JID"
rm -f docs/m3_a4/stepc_$RUN/scores.jsonl
ssh fa-master 'rm -rf /opt/fa-iforest/m3march'
```

收集完成后，在终端二、终端三按 Ctrl+C 停止两个采样（作业取消后它们也会自行退出）。

**期望产出**：打印「已排空」；`docs/m3_a4/stepc_<RUN>/collect_summary.txt` 中「M3 训练完成的设备数」为 8
（`kill` 运行中被重新训练的设备会多一行 `trained:`，计数可能为 9 或更多）；倒数第五条命令打印的计数至少为 8。

**常见失败**：两次读数不同说明作业仍在追赶，再等两分钟重复前三条；收集到的训练设备少于 8 时先不要取消作业，
把 `collect_summary.txt` 发给我。

---

## 第三节：汇总与回传

五次运行都完成后执行：

**执行环境**：本地 Mac，仓库根目录。

```bash
python3 deploy/scripts/m3_a4_stepc_report.py \
    --runs t1=docs/m3_a4/stepc_t1,t2=docs/m3_a4/stepc_t2,t4=docs/m3_a4/stepc_t4,kill=docs/m3_a4/stepc_kill \
    --compare kill:t2 --cancel-log docs/m3_a4/stepc_cancel/cancel.txt --out docs/m3_a4/stepc_report.md
git add -f docs/m3_a4
git commit -m "data(m3): 补遗四步骤 C 短重放预验证结果" && git push origin dev-claude
bash deploy/scripts/syn-reset-env.sh
```

**期望产出**：`docs/m3_a4/stepc_report.md` 有五节，分别对应五件事：

- 第一节：每次运行训练前检查点端到端时长的中位数、训练期间的最大值、「是否不超过两倍」、失败与重启次数。
- 第二节：每个子任务在训练期间输入水位线的最长停滞（墙钟秒，应远小于 60）、缓冲峰值、输出水位线保持时长，
  以及缓冲溢出计数（应为 0）。
- 第三节：逐设备的训练线程、排队等待、训练秒数、提交到收回的时长、收回时的缓冲轮数与排空时长。
- 第四节：取消的端到端秒数与中断记录。
- 第五节：`kill` 与 `t2` 逐设备比较轮数、早停集误差、标定中位数、IQR、阈值与逐窗评分，期望全部「相同」。

推送之后告诉我，我据此写补遗四报告的步骤 C 一节、定 `--train-threads` 的默认值，并给出全量重放手册。

**常见失败**：汇总脚本退出码 2 表示某个目录不存在；某次运行在表里显示「无训练记录」，说明收集时的 `SINCE`
晚于提交时刻或日志被截断，请告诉我。
