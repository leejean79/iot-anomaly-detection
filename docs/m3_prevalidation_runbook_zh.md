# 短重放预验证操作手册（2026-10-05 裁决第三节第 4 条）

依据：2026-10-05 裁决书《点通道迟到丢弃事故的处置》第三、四节。三项修正一起上：

1. 空闲时限 3,600 秒（作业默认值已改；启动横幅打印 `Idle timeout:    3600 s`）；
2. 两段重放，作业在两段之间保持运行；
3. 设备代理键，八台设备各占一个子任务（启动横幅打印 `Device subtasks: {A=0, ..., H=7}`）。

本预验证用缩小的「每天折合轮数」（864）把冷启动压到几分钟，核对四件事：八台落八个子任务、训练期间迟到
丢弃为零、训练后检查点正常完成、重启为零；另外核对断言一至七。全部通过后才提交长运行（注入运行）。
旧的冷启动探针（09-30）在 10 秒空闲时限下丢了 5,852 轮，所以这次「迟到丢弃为零」是有意义的对照。

**执行环境**：所有命令都在本地 Mac 的仓库根目录执行，经 `fa-master` 别名操作集群；第二步读原始 CSV，只能在
本地 Mac 上执行。
**前置条件**：`docs/m3_marapr_wrapup_runbook_zh.md` 已做完，集群已复位。

| 项目 | 取值 | 说明 |
| --- | --- | --- |
| 重放 | 第一段 03-01 至 03-04 末，第二段 03-05 至 03-06 末，600 倍速 | 重放器的结束时刻是包含在内的，所以用 23:59:59 的秒数作结束 |
| M1 标定 | `--warmup-rounds 8640`（一天） | 与旧探针相同 |
| 每天折合轮数 | `--m3-rounds-per-day 864` | 训练在 (7 + 2 + 7) × 864 = 13,824 个在线轮后触发，约在 03-03 下午；B、H 缺轮较多，约晚半天 |
| 检查点 | 间隔 30 秒，超时 120 分钟，容忍 3 次 | 与长运行相同 |

## 第一步：打包、检查、上传

```bash
git pull --rebase origin dev-claude
mvn clean package -DskipTests
bash deploy/scripts/check-jar.sh
bash deploy/scripts/syn-upload-m1.sh --jar-only
```

期望 `check-jar.sh` 全部 PASS，其中包括本次新增的两项：`device-surrogate-keys` 与 `Idle timeout:`。

## 第二步：数出期望轮数（本地 Mac）

```bash
python3 eda/count_rounds.py --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv --start 2022-03-01 --end 2022-03-07 --out docs/m3_prevalidation_eda_rounds.csv
```

期望终端打印总轮数，约 40 万。

## 第三步：提交作业

```bash
mkdir -p docs/m3_prevalidation
SINCE=$(ssh fa-master date -u +%Y-%m-%dT%H:%M:%SZ); echo "SINCE=$SINCE"
bash deploy/scripts/syn-submit-m2.sh --extra '--m3-enabled true --window-sec 3600 --warmup-rounds 8640 --m3-rounds-per-day 864 --m3-train-days 7 --m3-earlystop-days 2 --m3-thresh-days 7 --m3-window-length 60 --m3-hidden-size 60 --m3-batch-size 64 --m3-learning-rate 0.001 --m3-grad-clip 39072.0 --m3-max-epochs 300 --m3-earlystop-patience 20 --m3-reverse-target true --checkpoint-ms 30000 --checkpoint-timeout-min 120 --checkpoint-tolerable-failures 3' 2>&1 | tee /tmp/submit_pre.log
grep -E 'Idle timeout|Device subtasks|M3 train|M3 rounds|Ckpt|RUNNING' /tmp/submit_pre.log
JID=$(grep -oE 'JobID [a-f0-9]{32}' /tmp/submit_pre.log | awk '{print $2}' | head -1); echo "JID=$JID"
```

期望：

- `Idle timeout:    3600 s`
- `Device subtasks: {A=0, B=1, C=2, D=3, E=4, F=5, G=6, H=7} (surrogate keys, maxParallelism=128)`
- `M3 train/es/th:  7d/2d/7d`，`M3 rounds/day:   864  [COLD-START PROBE MODE: …]`
- `Ckpt interval/timeout: 30000 ms / 120 min`，最后一行 `M2Job is RUNNING`。

请把 `SINCE` 与 `JID` 抄下来。若 `Device subtasks` 一行显示 `raw device ids`，说明代理键没有传进去，请先停下来。

## 第四步：启动监测与第一段重放

```bash
bash deploy/scripts/syn-m3-watch.sh start --jid $JID --since $SINCE
bash deploy/scripts/syn-replay.sh --speedup 600 --start 2022-03-01 --end 1646438399
```

`1646438399` 是 2022-03-04 23:59:59 UTC。第一段约 10 分钟放完。

## 第五步：等八台都进入在线

```bash
bash deploy/scripts/syn-m3-watch.sh status
bash deploy/scripts/syn-replay.sh status
```

每隔几分钟看一次，直到监测显示「已进入在线 8 台」、重放器显示 `rc=0`。在此之前不要启动第二段。
作业在两段之间保持运行，不要取消。

## 第六步：第二段重放

```bash
bash deploy/scripts/syn-replay.sh --speedup 600 --start 2022-03-05 --end 1646611199
```

`1646611199` 是 2022-03-06 23:59:59 UTC。约 5 分钟放完，等重放器显示 `rc=0`。

## 第七步：排空，然后做七条断言（作业仍在运行）

```bash
bash deploy/scripts/syn-m2-metrics.sh > /tmp/pre_t1.txt; sleep 120; bash deploy/scripts/syn-m2-metrics.sh > /tmp/pre_t2.txt
diff /tmp/pre_t1.txt /tmp/pre_t2.txt && echo "已排空"
TOTAL=$(python3 -c "import csv; r=list(csv.reader(open('docs/m3_prevalidation_eda_rounds.csv'))); print(sum(int(x) for row in r[1:] for x in row[1:]))"); echo "TOTAL=$TOTAL"
bash deploy/scripts/syn-replay-verify.sh --start-utc 2022-03-01T00:00:00Z --end-utc 2022-03-07T00:00:00Z --calib-days 1 --expected-total "$TOTAL" --tol-pct 0.2 --max-messages 1000000 --report-name m2_replay_verify_prevalidation.csv --since "$SINCE" 2>&1 | tee docs/m3_prevalidation/replay_verify.txt
```

- `--calib-days 1` 与 `--warmup-rounds 8640` 对应，断言四据此检查冻结时刻落在第二天。
- 期望七条断言全部 PASS，最后一行为「七条断言全部通过」。断言六是「迟到丢弃为零」，断言七逐台列出两个通道的
  最晚输出与最后一轮的差。
- 若「已排空」没有打印，等两分钟后重复前两行，再做核验。

## 第八步：收集、出报告、核对子任务

```bash
bash deploy/scripts/syn-m3-watch.sh stop --out-dir docs/m3_prevalidation
bash deploy/scripts/syn-m3-march-collect.sh --out-dir docs/m3_prevalidation --since $SINCE
bash deploy/scripts/syn-m2-late-drop-history.sh --since "$SINCE" --out-dir docs/m3_prevalidation/late_drop
python3 deploy/scripts/m3_coldstart_report.py --dir docs/m3_prevalidation --rounds-per-day 864 --timeout-min 120
grep -oE 'Device [A-H] \(subtask [0-9]+\) entering TRAINING' docs/m3_prevalidation/m3_tm_log.txt | sort -u
```

期望（这四项就是裁决第三节第 4 条的通过条件）：

1. 最后一条命令打印八行，A 到 H 依次对应子任务 0 到 7，没有两台共用一个子任务。
2. `docs/m3_prevalidation/late_drop/late_drop_summary.md` 第二张表八个子任务的丢弃都是 0。
3. `docs/m3_prevalidation/coldstart_report.md` 的检查点一节里，训练期间挂起的检查点在训练结束后完成，失败次数为 0。
4. `docs/m3_prevalidation/timeline.log` 中每一行都是「重启=0」（可用 `grep -vc '重启=0' docs/m3_prevalidation/timeline.log`
   核对，只有少数几行监测提示不含这几个字）。

## 第九步：入库与收尾

```bash
git add -f docs/m3_prevalidation docs/m3_prevalidation_eda_rounds.csv docs/m2_replay_verify_prevalidation.csv
git commit -m "data(m3): 短重放预验证结果" && git push origin dev-claude
ssh fa-master "docker exec jobmanager flink cancel $JID"
bash deploy/scripts/syn-reset-env.sh
ssh fa-master 'rm -rf /opt/fa-iforest/m3march /opt/fa-iforest/m2probe'
```

- 推送后告诉代码开发代理。代理核对通过后，给出注入运行（类型 B）的操作手册。
- 任何一项不通过时，先执行 `bash deploy/scripts/syn-m3-diag.sh --since $SINCE --out-dir docs/m3_prevalidation/diag`
  并推送，暂不取消作业、不复位。

## 预计耗时

| 步骤 | 墙钟 |
| --- | --- |
| 打包、上传、数轮数 | 约 10 分钟 |
| 第一段重放与冷启动 | 约 20 分钟 |
| 第二段重放与排空 | 约 10 分钟 |
| 核验、收集、报告 | 约 15 分钟 |
