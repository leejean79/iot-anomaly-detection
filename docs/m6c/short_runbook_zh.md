# 第三批短重放运行手册

> 依据：交接文档第六节第 3 步；1.54 版裁决第八处（两组的时段与配置）；1.57 版裁决第五项（第二组开启的项）。
> 脚本：`deploy/scripts/syn-m6c-short.sh`；核对：`m6c/short_check.py`。

## 两组的内容

| 组 | 运行 a | 运行 b | 核对的结论 |
| --- | --- | --- | --- |
| 第一组 | `m6b-closeout` 标签构建的 jar | 新 jar，第三批全部开关关闭 | 第三批全部开关关闭时，新代码与标签 jar 在同一配置下逐位相同 |
| 第二组 | 新 jar，开启丁档与二级预案 | 与运行 a 相同 | 开启选定项后两次运行逐位相同 |

两组的共同配置：2022-03-01 至 04-03，一百五十倍速，有限重启三次，检查点三十秒；命令回路、上下文通道、性能层、数据层、聚合模块与升级格打开，影子评分关闭；设备 E 的注入段保留。第二组的第三批参数为 `--adapt-anchor trigger --adapt-anchor-min-days 7 --m7-anchor-defer-days 7 --m7-rebuild-plan true`，切换验证与振荡规则关闭。核对口径与第二批全量重放的等值第二段相同（九类输出逐位相同，排除按处理时间产生的诊断量；数据发完时仍有设备保持水位线时，聚合模块的输出从保持日起不参与比较）。

## 用户需要在场的时刻

1. **开始时（约 15 分钟）**：在本地 Mac 的仓库根目录依次执行
   ```bash
   git pull
   bash deploy/scripts/syn-m6c-short.sh prepare          # 打新 jar 与标签 jar、核对标记、上传，约 5 至 10 分钟
   bash deploy/scripts/syn-m6c-short.sh start --group 1  # 提交第一组；看到「本地 Mac 现在可以关机」即可离开
   ```
2. **约 11 至 12 小时后**：取回结果并把核对文件发给代码开发 agent
   ```bash
   bash deploy/scripts/syn-m6c-short.sh fetch --group 1
   ```
   取回后看 `docs/m6c/short1/short_report.md`；若进度里写「未核对」（master 的 python3 没有时区数据），在本地执行
   `python3 m6c/short_check.py --dir docs/m6c/short1 --group 1`。
3. 第二组在第一组核对通过后再提交：`bash deploy/scripts/syn-m6c-short.sh start --group 2`，取回用 `fetch --group 2`。两组不能同时运行。

中途查看进度（任何终端、任何时候）：`bash deploy/scripts/syn-m6c-short.sh status --group 1`。

## 前置条件

- master 已做过 `bash deploy/scripts/syn-m3-a4-stepc.sh setup-master`；Mac 上有 JDK 11 与 Maven；本地仓库能取到标签 `m6b-closeout`。
- JobManager 进程内存不少于 3,072 MB；三台节点复位之后磁盘可用不少于 20 GB；运行期间不做其他集群操作。

## 常见失败

- `prepare` 报「新 jar 里没有第三批的标记」：本地代码不是最新，先 `git pull` 再重来。报「标签 jar 里竟有第三批的标记」：标签不对，把输出发给代码开发 agent。
- `start` 报 master 上另一组或全量重放在运行：先 `status` 确认，必要时 `stop`。
- 进度里出现「失败」：执行 `fetch`，把 `progress.txt` 与 `a/` 或 `b/` 下的 `submit.log`、`jm_failure.log` 发给代码开发 agent。重启过的运行须重跑（`start --group 1 --runs b` 之类）。
