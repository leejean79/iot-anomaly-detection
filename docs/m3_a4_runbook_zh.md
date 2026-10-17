# 补遗四（把 M3 训练移出任务线程）运行手册：步骤 C 短重放预验证

版本：2026-10-14。依据交接文档 `docs/handover/handover_m3_addendum4_async_training_zh.md`（开发设计日志 0.82 版）
与 2026-10-14 裁决书《补遗四实现中的三点》（开发设计日志 0.83 版）。

## 开跑前提

三点上报已全部裁决（0.83 版），步骤 C 可以开跑：校准放在训练线程上（认可，代码不改）；步骤 C 用 120 倍速；
空闲时限用 60 秒墙钟，不换算。

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

全量重放（第四节第 2 条，验收项 A4-1 至 A4-5）的手册在步骤 C 回传之后另行给出：按 0.83 版裁决采用单段、
约 200 倍速，具体倍速由步骤 C 定下的线程数按下方「重放倍速的硬约束」推导。

## 共同参数

| 项目 | 取值 | 说明 |
| --- | --- | --- |
| 重放区间 | 2022-03-01 至 2022-03-05（4 天） | 标定 1 天，收集 16 × 864 轮约 1.6 天，之后约 1.4 天在线 |
| 重放倍速 | 120（取消那次可用 600） | 见下方「为什么是 120 倍」 |
| M1 标定轮数 | `--warmup-rounds 8640` | 1 天 |
| 每天折合轮数 | `--m3-rounds-per-day 864` | 训练在 (7 + 2 + 7) × 864 = 13,824 个在线轮后触发 |
| M3 训练参数 | 与注入运行相同 | 隐藏层 60、窗口 60、小批量 64、学习率 0.001、裁剪 39072.0、上限 300 轮、耐心 20、目标逆序、标定 7 天 |
| 检查点 | 间隔 30 秒，超时 10 分钟，容忍连续失败 3 次 | 超时按补遗四第五节恢复为生产值 10 分钟 |
| 空闲时限 | `--idle-wall 60` | 墙钟常量，生产与重放相同，不按倍速换算（0.83 版裁决第三节，见下方） |
| 训练线程数 | 每次运行不同，见第二节 | `--train-threads` |

### 重放倍速的硬约束（异步训练时代，0.83 版裁决第二节）

训练期间到达的轮进入有界缓冲，上限是「标定天数加两天」的轮数，按十秒一轮折合九天事件时间。在生产（实时到达）
中，这个上限是内存约束（每台约八兆）；在重放中，重放把事件时间压缩了「倍速」那么多倍，训练（含排队等待）期间
到达的事件时间等于墙钟时长乘以倍速，所以上限与训练时长一起决定倍速的上限：

> **倍速 ≤ 缓冲上限对应的事件时间（秒）÷ 最长「排队等待 + 训练」时长（墙钟秒）÷ 2**

缓冲溢出即事故（交接文档第二节第 3 条）。全量重放的训练时长用注入运行的实测值、线程数取步骤 C 的定值后外推；
补遗四报告给出逐台缓冲峰值占上限的比例。

步骤 C 的取值：每天 864 轮时上限为 9 × 864 = 7,776 轮，折合 77,760 秒（约 21.6 小时）事件时间。补遗三的短重放
里单台训练 30 至 95 秒，线程数为一时一台任务管理器上的四台设备依次训练，最长约 300 秒，于是倍速上限为
77,760 ÷ 300 ÷ 2 ≈ 130，取 120。每次运行约 48 分钟。

### 空闲时限是墙钟常量（0.83 版裁决第三节）

空闲时限判断的是「这个分区有没有在墙钟意义上停止送数据」，它的下限由 Flink 一侧的墙钟停顿决定（垃圾回收、
检查点对齐、拉取间隙），与数据的事件时间无关。因此它是墙钟常量 60 秒，生产与重放相同，**不按倍速换算**；若按
倍速换算到十几毫秒或零点几秒，任何一次垃圾回收都会把分区误判为空闲，其余分区把水位线推过去，积压在点通道窗口
处被当作迟到丢弃（10 月 5 日那起事故的机理）。随倍速变化的只是 60 秒墙钟对应的事件时间跨度（120 倍时为 2 小时，
200 倍时为 3.3 小时），即一台设备离线时最多拖住全机队水位线 60 秒墙钟，在重放中可以接受。

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

### 一键执行（推荐，在 master 上无人值守运行）

第一节的三步完成后，用一条命令把五次运行交给 master 上的 tmux 会话依次执行，最后生成第三节的汇总报告。提交后本地
Mac 可以关机；全部跑完后再用一条命令把结果拉回本地。脚本按下面第四步至第十步的同样流程逐次执行，扰动在启动重放后
立即进入等待，不会错过。

**一次性准备（只做一次）**：让 master 能用 `fa-master`、`fa-worker1`、`fa-worker2` 三个别名登录三台机器（强杀任务
管理器与收集日志要登录 worker）。脚本在 master 上生成一把只供本项目编排使用的新密钥，把公钥加入三台机器的
`~/.ssh/authorized_keys`，并在 master 的 `~/.ssh/config` 写入走内网地址的三个别名；不复制 Mac 上的任何私钥。

```bash
bash deploy/scripts/syn-m3-a4-stepc.sh setup-master
```

期望产出：依次打印「已授权到 fa-master / fa-worker1 / fa-worker2」，连通性测试列出三台机器的主机名与容器名，最后
打印「全部通过，可以执行 start」。退出码 4 表示测试未通过（多半是 master 到 worker 的内网 22 端口未放行），请把输出
发给我。

**执行环境**：本地 Mac，仓库根目录。**前置条件**：第一节已完成；集群上没有任何作业在运行；setup-master 已通过。

```bash
bash deploy/scripts/syn-m3-a4-stepc.sh start     # 立即返回；之后 Mac 可以关机，约四个半小时跑完
bash deploy/scripts/syn-m3-a4-stepc.sh status    # 任何时候开机查看进度
bash deploy/scripts/syn-m3-a4-stepc.sh fetch     # 跑完后把 docs/m3_a4 拉回本地
bash deploy/scripts/syn-m3-a4-stepc.sh stop      # 需要中止时执行
```

**期望产出**：`start` 打印「已在 master 的 tmux 会话 syn-m3-a4-stepc 中启动……本地 Mac 现在可以关机」。`status`
打印 master 上 `docs/m3_a4/progress.txt` 的最后几行；每次运行依次记下「开始、作业已提交、采样与重放已启动、（扰动
完成）、重放结束、已排空、完成」。全部完成后最后一行是「全部完成」，并提示先 `fetch` 再提交。`fetch` 之后本地
`docs/m3_a4/` 下有五个 `stepc_<标签>/` 目录与 `stepc_report.md`。

**常见失败**：某次运行失败时，编排取消该次作业、停止重放、取回已有采样，在进度里写明失败的运行与步骤并停止；
先 `fetch`，把 `progress.txt` 与对应目录里的日志发给我，排查后用 `start --runs <剩下的标签>` 续跑。退出码 2 表示
还没有执行 setup-master；退出码 3 表示已有编排在运行。

下面第四步至第十步是同一流程的手动版本，一键执行时不需要再做。

### 手动执行

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
printf 'RUN=%s\nTHREADS=%s\nSINCE=%s\nJID=%s\n' "$RUN" "$THREADS" "$SINCE" "$JID" > docs/m3_a4/current_run.env
```

**期望产出**：

- `Idle timeout:    60 s`
- `Ckpt interval/timeout: 30000 ms / 10 min`
- `M3 rounds/day:   864  [COLD-START PROBE MODE: …]`
- `M3 async train:  threads=<THREADS> per TaskManager, drain batch=200 rounds, poll interval=30 s (addendum 4)`
- `Device subtasks:` 八台设备落在八个不同的子任务上
- 最后打印 `M2Job is RUNNING (JobID …)`，`JID` 打印出同一个编号。

**常见失败**：60 秒内没有进入 RUNNING 时，用 `ssh fa-master "docker exec jobmanager flink list"` 查看。
最后一条命令把 `RUN`、`THREADS`、`SINCE`、`JID` 存进 `docs/m3_a4/current_run.env`。此后可以关掉终端；回来时在
任何新终端里先执行 `source docs/m3_a4/current_run.env`，下面各步的命令就能直接使用这四个变量。

### 第六步：在 master 上启动采样（无人值守，本地终端可以关闭）

**执行环境**：本地 Mac，仓库根目录。**前置条件**：第五步的作业处于 RUNNING；master 上没有名为 `syn-m3-watch`
的 tmux 会话（上一次运行已在第十步 `stop`）。

```bash
source docs/m3_a4/current_run.env
bash deploy/scripts/syn-m3-watch.sh start --jid $JID --since $SINCE
bash deploy/scripts/syn-m3-watch.sh status      # 随时查看，不需要一直开着
```

三个采样在 master 的 tmux 会话里运行，与本地 Mac 是否开着终端、是否睡眠都无关：检查点时间线（每 15 秒）、检查点
大小（每 20 秒）、水位线与训练缓冲（每 10 秒，补遗四新增）。作业取消或结束后它们会自行退出，第十步用 `stop`
取回结果。

**期望产出**：`start` 打印「监测已在 master 的 tmux 会话 syn-m3-watch 中启动」。`status` 打印三段最新记录、已进入
训练与在线的设备数、重放器状态。其中「水位线与训练缓冲」一行列出八个子任务的「缓冲 N 保持 Xs 入 <输入水位线>
出 <输出水位线>」：训练开始前缓冲都是 0，输入与输出水位线相同；训练期间缓冲增长、输入水位线继续推进、输出水位线
停在缓冲头；排空后两者重新一致。训练期间「进行中」的检查点应当在几秒内完成，不再长时间挂起。

**常见失败**：退出码 3 表示上一次的会话还在，先执行 `bash deploy/scripts/syn-m3-watch.sh stop --out-dir /tmp/old_watch`；
退出码 2 表示 master 缺少 python3、tmux 或 curl，请把输出发给我。「水位线与训练缓冲」一行暂时为空属正常，作业
处理第一批数据后才有指标。

### 第七步：启动重放

**执行环境**：本地 Mac，仓库根目录。

```bash
bash deploy/scripts/syn-replay.sh --speedup 120 --start 2022-03-01 --end 2022-03-05   # cancel 那次用 --speedup 600
bash deploy/scripts/syn-replay.sh status
```

**期望产出**：重放在 master 的 tmux 会话里运行，约 48 分钟后日志打印 `Finished.` 与 `rc=0`
（查看：`ssh fa-master "grep -E 'Finished|rc=' /opt/fa-iforest/syn-replay.log | tail -3"`）。

**`kill` 与 `cancel` 两次运行**：执行完上面两条命令后，**不要离开，紧接着在同一个终端执行第九步的后台命令**。
扰动脚本自己会等到训练开始再动手；如果等训练开始后再去启动它，训练很可能已经结束（短重放的训练只有 30 至
100 秒），这次运行就失去了扰动。

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

### 第九步：扰动（只在 `kill` 与 `cancel` 两次运行中执行，紧接第七步，不要等）

**执行环境**：本地 Mac，仓库根目录。**前置条件**：第七步的重放已启动。脚本会自己等到第一台设备开始训练（120 倍
时约 26 分钟），再等 15 秒后动手，所以在启动重放后立即用下面的后台方式启动即可，本地终端随后可以关闭；但这段
时间里 Mac 必须接着电源、不睡眠、不断网（`caffeinate -i` 阻止空闲睡眠，合上屏幕仍会睡眠），因为扰动要从 Mac
登录到 worker 执行。

```bash
source docs/m3_a4/current_run.env
# 只在 RUN=kill 时执行：
caffeinate -i nohup bash deploy/scripts/syn-m3-a4-disrupt.sh kill --jid $JID --since $SINCE \
    --out docs/m3_a4/stepc_kill/kill.txt > /tmp/a4_disrupt.log 2>&1 &
# 只在 RUN=cancel 时执行：
caffeinate -i nohup bash deploy/scripts/syn-m3-a4-disrupt.sh cancel --jid $JID --since $SINCE \
    --out docs/m3_a4/stepc_cancel/cancel.txt > /tmp/a4_disrupt.log 2>&1 &
# 回来后查看结果：
cat docs/m3_a4/stepc_$RUN/$RUN.txt
```

**期望产出**：

- `kill`：打印被选中的设备与任务管理器、强杀时刻、容器重新启动时刻，以及作业重新 RUNNING 的时刻与重启计数 1。
  之后日志里应出现 `is in TRAINING without a model and nothing in flight (restored from a checkpoint): resubmitting`，
  该设备重新训练并最终进入在线。作业恢复后继续第十步，与其他运行相同。
- `cancel`：打印「取消端到端 N 秒（要求不超过 30 秒）」与该任务管理器日志里的 `training interrupted` 和
  `Training executor shut down` 两行。`cancel` 这次运行到此结束：执行
  `bash deploy/scripts/syn-m3-watch.sh stop --out-dir docs/m3_a4/stepc_cancel` 与
  `bash deploy/scripts/syn-replay.sh stop`，然后直接跳到下一次运行的第四步。

**常见失败**：退出码 3 表示集群上还有其他作业，脚本没有做任何扰动，请告诉我；退出码 5 表示取消 120 秒后仍未
完成，或强杀后 300 秒作业仍未恢复，请把 `ssh fa-master "docker logs --tail 200 jobmanager"` 的输出发给我。

**如果第九步错过了**（重放已经结束，或训练已经全部完成才想起来）：这次运行没有扰动，不能回答它要回答的问题，
须从第四步重做这一次。重做之前按下面处理当前作业：

- 若错过的是 `kill`：这次运行与 `t2` 参数完全相同、只是没有扰动，可以作为第二次 `t2` 收下，用来核对「同样输入的
  两次运行逐位相同」。做法是 `source docs/m3_a4/current_run.env` 之后执行 `RUN=t2b`，再照常执行第十步，
  结果落在 `docs/m3_a4/stepc_t2b/`。
- 若错过的是 `cancel`：这次运行是 600 倍速，缓冲可能溢出，结果不收；直接执行
  `ssh fa-master "docker exec jobmanager flink cancel $JID"`、`bash deploy/scripts/syn-m3-watch.sh stop --out-dir /tmp/cancel_missed`
  与 `bash deploy/scripts/syn-replay.sh stop`。

### 第十步：确认排空、收集结果、取消作业

**执行环境**：本地 Mac，仓库根目录。**前置条件**：重放已打印 `rc=0`；日志里八台设备都出现了 `buffer drained`
（或收回时缓冲为 0）。

```bash
source docs/m3_a4/current_run.env
bash deploy/scripts/syn-m2-metrics.sh > /tmp/a4_t1.txt; sleep 120
bash deploy/scripts/syn-m2-metrics.sh > /tmp/a4_t2.txt
diff /tmp/a4_t1.txt /tmp/a4_t2.txt && echo "已排空"
bash deploy/scripts/syn-m3-march-collect.sh --out-dir docs/m3_a4/stepc_$RUN --since $SINCE
grep -c 'buffer drained\|result collected' docs/m3_a4/stepc_$RUN/m3_tm_log.txt
ssh fa-master "docker exec jobmanager flink cancel $JID"
bash deploy/scripts/syn-m3-watch.sh stop --out-dir docs/m3_a4/stepc_$RUN
rm -f docs/m3_a4/stepc_$RUN/scores.jsonl
ssh fa-master 'rm -rf /opt/fa-iforest/m3march'
```

`syn-m3-watch.sh stop` 把 `ckpt_timeline.csv`、`ckpt_sizes.csv`、`wm_buffer.csv` 与三份日志取回到本次运行的目录，
第三节的汇总脚本直接读取它们。

**期望产出**：打印「已排空」；`docs/m3_a4/stepc_<RUN>/` 下有 `ckpt_timeline.csv` 与 `wm_buffer.csv`；`docs/m3_a4/stepc_<RUN>/collect_summary.txt` 中「M3 训练完成的设备数」为 8
（`kill` 运行中被重新训练的设备会多一行 `trained:`，计数可能为 9 或更多）；`grep -c` 那一条打印的计数至少为 8。

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
- 第二节：每个子任务在训练期间输入水位线的最长停滞（墙钟秒，应远小于 60）、缓冲峰值及其占上限 7,776 轮的比例、输出水位线保持时长，
  以及缓冲溢出计数（应为 0）。
- 第三节：逐设备的训练线程、排队等待、训练秒数、提交到收回的时长、收回时的缓冲轮数与排空时长。
- 第四节：取消的端到端秒数与中断记录。
- 第五节：`kill` 与 `t2` 逐设备比较轮数、早停集误差、标定中位数、IQR、阈值与逐窗评分，期望全部「相同」。

推送之后告诉我，我据此写补遗四报告的步骤 C 一节、定 `--train-threads` 的默认值，并给出全量重放手册。

**常见失败**：汇总脚本退出码 2 表示某个目录不存在；某次运行在表里显示「无训练记录」，说明收集时的 `SINCE`
晚于提交时刻或日志被截断，请告诉我。

---

## 第四节：全量重放（A4-1 至 A4-5，2026-10-16 裁决后）

依据交接文档第四节第 2 条（10 月 14 日、15 日修订）与 0.84 版裁决：注入运行的完全重复，单段重放 2022-03-01 至 05-01，
设备 E 按 `docs/m3_inject_spec.txt` 注入，生产配置（空闲时限 60 秒墙钟、检查点超时 10 分钟、训练线程数 2），守卫阈值写入配置，
**倍速 150**（按「重放倍速的硬约束」公式取保守值：777,600 ÷ 约 2,580 ÷ 2 ≈ 150）。A4-1 的检查点判据为训练期间每一次检查点的
端到端时长不超过检查点间隔 30 秒。

整个流程由 `deploy/scripts/syn-m3-a4-full.sh` 在 master 的 tmux 会话里无人值守地执行：预检（任务管理器进程总量 5120 兆；摘录步骤 C
强杀时段的作业管理器日志）→ 复位 → 提交（核对启动横幅里守卫为 ON）→ 启动采样 → 单段重放 → 等重放结束并排空 → 七条断言 →
收集、性能与迟到丢弃 → 守卫在线核对（A4-4）→ 取消作业 → 与注入运行逐位比较（A4-2）→ 汇总报告（A4-1、A4-3、A4-5）→ 复位。

**执行环境**：本地 Mac，仓库根目录。

**前置条件**：

1. `git pull --rebase origin dev-claude`。
2. 已执行过 `bash deploy/scripts/syn-m3-a4-stepc.sh setup-master`（步骤 C 时已做，不必重做）。
3. jar 是最新的：`mvn clean package -DskipTests && bash deploy/scripts/check-jar.sh && bash deploy/scripts/syn-upload-m1.sh --jar-only`
   （步骤 C 之后 Java 代码没有改动，若当时已上传可跳过）。
4. 两台任务管理器已按 5120 兆重建（编排预检会核对，不满足时停止并提示执行 `syn-tm-recreate.sh`）。
5. 集群上没有任何作业在运行。

**调用命令**：

```bash
bash deploy/scripts/syn-m3-a4-full.sh start     # 立即返回；之后 Mac 可以关机，约 10.5 小时跑完
bash deploy/scripts/syn-m3-a4-full.sh status    # 任何时候开机查看进度
bash deploy/scripts/syn-m3-a4-full.sh fetch     # 跑完后把 docs/m3_a4/full 拉回本地
bash deploy/scripts/syn-m3-a4-full.sh stop      # 需要中止时执行
```

**期望产出**：`status` 依次显示「预检」「作业已提交」「采样与单段重放已启动」「重放结束」「已排空」「七条断言」「收集完成」「逐位等值」
各行，最后一行为「全部完成」。`fetch` 后本地 `docs/m3_a4/full/` 下有 `a4_full_report.md`（A4-1 至 A4-5 汇总）、`equality.md`、
`guard_check.md`、`replay_verify.txt`、`perf/`、`late_drop/` 与收集的日志和评分；`docs/m3_a4/stepc_kill/jm_log_0455_0458.txt` 是步骤 C
强杀时段的作业管理器日志摘录。然后执行 `git add -f docs/m3_a4 && git commit -m "data(m3): 补遗四全量重放结果" && git push origin dev-claude`。

**常见失败**：任何一步失败时，编排取消作业、停止重放、取回已有采样，在 `progress.txt` 写明失败的步骤后停止；先 `fetch`，再把
`docs/m3_a4/full/progress.txt` 与对应日志发给我。退出码 2 表示 setup-master 未完成，3 表示已有编排在运行。逐位等值出现不同时
编排照常跑完，结论写在 `equality.md` 里，按交接文档 A4-2 属于缺陷，须上报。

## 第五节：排空缺陷修正后的验证与全量重跑（2026-10-17 裁决，开发设计日志 0.86 版）

0.86 版裁决认定上一次全量重放有效、数字入档，但上下文异常检测模块的收口要以修正后的代码为依据。顺序是：修正代码 → 构建上传 jar
→ 步骤 C 验证（`fix` 运行）→ 四项通过后全量重跑。上一次的结果保留在 `docs/m3_a4/full/`，重跑写入 `docs/m3_a4/full_fix/`。

**当前阻塞**：`fix` 运行的倍速等待设计会话裁定。裁决原文是 600 倍，但按步骤 C 的实测，600 倍下设备 C 的缓冲会溢出，见
`docs/reports/m3_addendum4_fix_verify_speed_for_decision.md`。第一步不受阻塞，可以先做。

### 第一步：构建、核对并上传新 jar（不受阻塞，现在即可执行）

**执行环境**：本地 Mac，仓库根目录，JDK 11。

```bash
git pull --rebase origin dev-claude
mvn clean package
bash deploy/scripts/check-jar.sh
bash deploy/scripts/syn-upload-m1.sh --jar-only
```

**前置条件**：集群上没有本项目的作业在运行（上传会替换 jobmanager 里的 jar）。

**期望产出**：`mvn clean package` 运行全部 125 个单元测试并显示 `BUILD SUCCESS`；其中新增的 `oneTimerChainAndElementsDoNotDrain`
就是这次修正的测试。`check-jar.sh` 全部 PASS，上传脚本显示 jar 已复制到 jobmanager。

**常见失败**：单元测试失败时不要上传，把 `target/surefire-reports/` 下失败用例的 `.txt` 发给我。这次修正没有新增可供 `check-jar.sh`
识别的标记，所以 jar 是否为新版只能靠 `mvn clean package` 保证；如果误用了旧 jar，`fix` 运行的第一、四项会判为未通过。

### 第二步：步骤 C 验证（等倍速裁定后执行）

**执行环境**：本地 Mac，仓库根目录。编排在 master 的 tmux 会话 `syn-m3-a4-stepc` 里运行，提交后 Mac 可以关机。

**调用命令**（倍速与检查点间隔按裁定填写；下面是建议的甲方案）：

```bash
bash deploy/scripts/syn-m3-a4-stepc.sh start --runs fix --speed 400 --ckpt-ms 5000   # 立即返回，约 45 分钟跑完
bash deploy/scripts/syn-m3-a4-stepc.sh status                                       # 随时查看进度
bash deploy/scripts/syn-m3-a4-stepc.sh fetch                                        # 跑完后取回
```

- `--speed`：必填，倍速。
- `--ckpt-ms`：可选，检查点间隔（毫秒），默认 30000。

**前置条件**：第一步已完成；`setup-master` 以前已做过，不必重做；集群上没有任何作业在运行；本地有 `docs/m3_a4/stepc_t2/`（作为
对照复制到 master）。

**期望产出**：`status` 的最后一行依次出现「修正验证：……」与「全部完成」。`fetch` 后本地 `docs/m3_a4/stepc_fix/` 下有：

- `fix_check.md`：四项核对与总判定，即排空期间每次检查点不超过 1 秒、排空中的输入水位线落后不超过 10 秒、与 t2 逐位相同、
  训练态状态快照每台每 30 秒一条；
- `equality_t2.md`：与 t2 的逐位比较；
- `status_snapshots.csv`：训练态状态快照的逐台条数；
- `ckpt_history.csv`：逐个检查点的触发与完成时刻。

然后执行 `git add -f docs/m3_a4/stepc_fix && git commit -m "data(m3): 排空缺陷修正后的步骤 C 验证结果" && git push origin dev-claude`。

**常见失败**：

- 退出码 2：没有给 `--speed`，或 `setup-master` 未完成。
- 退出码 3：已有编排在运行。
- 某一步失败时，编排停止并在 `docs/m3_a4/progress.txt` 写明失败的步骤；先 `fetch`，再把 `progress.txt` 发给我。
- `fix_check.md` 有「未通过」或「未覆盖」时，不要开始第三步，把结果推送给我。

### 第三步：全量重跑（第二步四项全部通过后执行）

**执行环境与调用命令**：与第四节相同：`bash deploy/scripts/syn-m3-a4-full.sh start`（倍速 150，约 10.5 小时无人值守），跑完后 `fetch`。

**前置条件**：第二步四项全部通过；本地有上一次的 `docs/m3_a4/full/scores.jsonl` 与 `m3_tm_log.txt`（编排会复制到 master 作为第二个
对照，缺少时 `start` 报错退出）。jar 用第一步上传的那个，不需要重新构建。

**期望产出**：结果在 `docs/m3_a4/full_fix/`，包括：

- `a4_full_report.md`：A4-1 至 A4-5 汇总；
- `equality.md`：对照 10 月 9 日注入运行；
- `equality_prev.md`：对照上一次全量重放（作业 `9218af22…`）；
- `ckpt_history.csv`：逐个检查点。

进度最后一行「逐位等值」应为两个「全部相同」。然后执行 `git add -f docs/m3_a4/full_fix && git commit -m "data(m3): 补遗四全量重放（排空缺陷修正后）结果" && git push origin dev-claude`。

**常见失败**：与第四节相同。

### 倍速公式的校准与注意事项（0.86 版裁决第二节第 4 条与第三节第 7 条）

- 公式「倍速 ≤ 缓冲上限的事件时间 ÷ 最长（排队等待 + 训练）÷ 2」的结构已由全量重放证实：缓冲峰值约等于「提交到收回的秒数 ×
  倍速 ÷ 每轮 10 秒」。例如 G 为 1,871 × 150 ÷ 10 ≈ 28,100 轮，实测 27,965 轮。
- 按全量重放实测的最长「提交到收回」1,871 秒（训练线程数 2），全量数据的倍速上限约为 777,600 ÷ 1,894 ÷ 2 ≈ 205。原先用注入运行
  的训练时长估计的 2,580 秒偏长 27%，因为注入运行是六台同时训练。全量重跑的目的是验证修正，倍速维持 150。
- 设备在两台任务管理器之间的分配会影响排队时长。上一次全量重放中，taskmanager-3 分到了训练最慢的 D、E、F、G 四台，F 和 G 各排队
  约 1,100 至 1,200 秒。这是代理键放置的副作用，不处理；估计倍速时应按最不利的分配计算排队时长。
- 步骤 C（每天折合 864 轮，上限 7,776 轮）按 t2 的最长提交到收回 136 秒计，倍速上限（二倍余量）约 286；不留余量时约 572。
