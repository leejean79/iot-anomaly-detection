# 三月至四月运行的收尾操作手册（按 2026-10-05 裁决第二节）

依据：2026-10-05 裁决书《点通道迟到丢弃事故的处置》第二节。本手册取代 `docs/m3_marapr_runbook_zh.md` 第十至
十四步。D、G 自 03-25 起的输出作废；A、B、C、E、F、H 六台的结果有效，按六台口径分析。

**执行环境**：除注明「本地 Mac」外，所有命令都在本地 Mac 的仓库根目录执行，经 `fa-master` 别名操作集群。
**前置条件**：本次运行的 Kafka 数据还在（10-04 已核对），自 10-03 以来没有执行过 `syn-reset-env.sh`。
**顺序要求**：第一至三步都要读集群上的数据，必须在第五步复位之前做完。本收尾用的是集群上现有的 jar，
新 jar（代理键等改动）留到预验证时再上传。

## 第一步：设置变量并拉取

```bash
git pull --rebase origin dev-claude
SINCE=2026-10-03T07:21:59Z
```

`SINCE` 是本次运行提交前记下的时刻（取自 `docs/m3_marapr/m2_metrics_final.txt` 第一行）。

## 第二步：在 Kafka 数据上重做重放完整性核验（裁决第二节第 4 条）

```bash
TOTAL=$(python3 -c "import csv; r=list(csv.reader(open('docs/m3_marapr_eda_rounds.csv'))); print(sum(int(x) for row in r[1:] for x in row[1:]))"); echo "TOTAL=$TOTAL"
bash deploy/scripts/syn-replay-verify.sh --start-utc 2022-03-01T00:00:00Z --end-utc 2022-05-01T00:00:00Z --expected-total "$TOTAL" --tol-pct 0.2 --max-messages 6000000 --report-name m2_replay_verify_marapr.csv --since "$SINCE" 2>&1 | tee docs/m3_marapr/replay_verify.txt
```

- 第一行若报 `No such file or directory`，说明本地没有第二步的逐日表，先在本地 Mac 执行：
  `python3 eda/count_rounds.py --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv --start 2022-03-01 --end 2022-05-01 --out docs/m3_marapr_eda_rounds.csv`
- 期望结果：断言一至四 PASS；断言五 SKIP（作业已不存在，报告中以监测时间线的 310 次「重启=0」代替）；
  **断言六、七 FAIL**——断言六报出 615,474，断言七列出 D、G 两台在 03-25 以后没有输出。这两条失败是本次事故
  的如实记录，同时也验证了新断言能抓住这一类问题。脚本因此以退出码 1 结束，属预期。
- 若断言一至四中有任何一条不是 PASS，请把输出贴给代码开发代理，先不要继续。

## 第三步：E 与 C 的离线参照与等值核验（约 30 分钟）

```bash
ssh fa-master 'docker rm -f syn-m3grid 2>/dev/null'
bash deploy/scripts/syn-m3-grid.sh --devices E,C --hidden-grid 60 --window-grid 60 --batch-grid 64 --lr-grid 0.001 --grad-clip 39072.0 --max-epochs 300 --patience 20 --omp-threads 1 --node master --detach --out-name m3_marapr_reference.csv --per-epoch-name m3_marapr_reference_per_epoch.csv
```

约 30 分钟后：

```bash
bash deploy/scripts/syn-m3-grid.sh --collect --node master --out-name m3_marapr_reference.csv --per-epoch-name m3_marapr_reference_per_epoch.csv
python3 deploy/scripts/m3_coldstart_report.py --dir docs/m3_marapr --timeout-min 120 --offline-csv docs/m3_marapr_reference.csv --offline-excluded docs/m3_marapr_reference_excluded.csv
```

- 期望：`--collect` 拉回三个文件（参照结果、逐轮记录、剔除窗口清单）；`docs/m3_marapr/coldstart_report.md` 第五节
  中 E、C 两行，训练集相同时结论为「通过」（早停集误差逐位相同）；训练集不同时列出只在一侧被剔除的窗口。
- 离线参照在 master 上运行的这段时间里，可以同时做第四步。

## 第四步：逐日通道剖面与六台口径的 V-M3-4（本地 Mac）

```bash
python3 eda/daily_channel_profile.py --data-dir /Users/lijing/Downloads/fwlmb11wni392kodtyljkw4n2/files_csv --start 2022-03-01 --end 2022-05-01 --ref-start 2022-03-17 --ref-end 2022-03-24 --scores docs/m3_marapr/m3_scores.jsonl --out docs/m3_marapr/daily_channel_profile.csv
python3 deploy/scripts/m3_v34_report.py --scores docs/m3_marapr/m3_scores.jsonl --profile docs/m3_marapr/daily_channel_profile.csv --start 2022-03-24 --end 2022-05-01 --exclude-devices D,G --out-dir docs/m3_marapr/v34
```

- 剖面的参照期是本次的阈值校准期 03-17 至 03-24；平稳日按全部八台的原始数据判定，评分只用六台。
- 期望 `docs/m3_marapr/v34/` 下有 `v34_report.md`（开头注明排除了 D、G）、`v34_daily.csv`、`v34_daily.png`、
  `v34_score_hist.png`。报告里的四月平稳日清单是注入运行排程的依据。

## 第五步：入库，然后复位

```bash
git add -f docs/m3_marapr/replay_verify.txt docs/m2_replay_verify_marapr.csv docs/m3_marapr/coldstart_report.md docs/m3_marapr/daily_channel_profile.csv docs/m3_marapr/v34 docs/m3_marapr_reference.csv docs/m3_marapr_reference_per_epoch.csv docs/m3_marapr_reference_excluded.csv
git commit -m "data(m3): 三月至四月运行收尾（核验重做、E 与 C 等值核验、六台口径 V-M3-4）" && git push origin dev-claude
bash deploy/scripts/syn-reset-env.sh
ssh fa-master 'rm -rf /opt/fa-iforest/m3march /opt/fa-iforest/m2probe'
```

- 期望：推送成功；复位核对表全部 PASS、退出码 0；master 上两处临时转储被删除（约 2 GB）。
- 复位之后本次运行的 Kafka 数据就没有了，所以务必确认推送成功再复位。
- 推送后告诉代码开发代理，代理据此写出本次运行（六台口径）的报告，其中记入 10-04 监测系统实例重启一事。

完成后接着做 `docs/m3_prevalidation_runbook_zh.md` 的短重放预验证。
