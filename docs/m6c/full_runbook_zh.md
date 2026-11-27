# 第三批全量重放运行手册

> 依据：交接文档第六节第 4 步；1.57 版裁决第五项第 3 条。脚本：`deploy/scripts/syn-m6c-full.sh`；核对：`m6b/full_check.py`（第二批同一口径）。

## 配置

2022-03-01 至 05-01，一百五十倍速，有限重启三次，检查点三十秒；命令回路、上下文通道、性能层、数据层、聚合模块与升级格打开，影子评分关闭；
零级为丁档（`--adapt-anchor trigger --adapt-anchor-min-days 7 --m7-anchor-defer-days 7`），二级预案打开（`--m7-rebuild-plan true`），
切换验证与振荡规则关闭；设备 E 的注入段保留。配置相同的两次运行 a、b。

核对：断言八条与零重启；等值第一段（第一条一级或二级命令的决定时刻之前与链式离线 `docs/m6c/chain/ding_up_s` 逐条相同，链式在 E 于 03-24
日界的一级处停止，即 03-25 00:00）；数据发完时没有设备保持水位线；等值第二段（两次在线运行九类输出逐位相同）；时延表与性能表。

## 用户需要在场的时刻

1. **开始时（约 5 分钟）**：本地 Mac 仓库根目录
   ```bash
   git pull
   bash deploy/scripts/syn-m6c-full.sh start     # 看到「本地 Mac 现在可以关机」即可离开
   ```
   第二组短重放时上传的新 jar 已含全部修正，不需要再 prepare（若之后代码有改动，先执行 `bash deploy/scripts/syn-m6c-short.sh prepare`）。
2. **约 21 至 23 小时后**：`bash deploy/scripts/syn-m6c-full.sh fetch`，把 `docs/m6c/full/full_report_tables.md` 与 `docs/m6c/full/check.txt` 发给代码开发 agent。

中途查看进度：`bash deploy/scripts/syn-m6c-full.sh status`。

## 前置条件与常见失败

- 与第二批全量重放相同：master 已 setup-master；JobManager 进程内存不少于 3,072 MB；复位之后三台磁盘可用不少于 20 GB；不能与短重放同时运行。
- 进度里出现「失败」：`fetch` 后把 `progress.txt` 与对应运行目录的 `submit.log`、`jm_failure.log` 发给代码开发 agent。
- 运行 a 在「监测快照的规范化摘要」或其后失败：`bash deploy/scripts/syn-m6c-full.sh start --resume a` 只续做收集的后半段。
- 作业重启过的运行须重跑：`start --runs a` 或 `start --runs b`。
