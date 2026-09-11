# M3 集群实验操作手册 / M3 Cluster Experiment Runbook

本手册给出 M3（上下文异常检测）在真实集群上每次实验的标准流程：实验前准备、实验中按序执行、实验后收尾。
适用对象是"每次跑实验前照着做一遍"的操作者。所有命令均在**本地 Mac** 的仓库根目录执行，脚本内部通过 ssh
连到集群 master（`fa-master`）再 `docker exec` 进相应容器。

> 模型说明 / Model note：M3 使用 DL4J 的 LSTM 自编码器，依赖钉在 **1.0.0-beta7**（其字节码为
> Java 7 / 主版本 51，可在 JDK 8 编译、在 Java 8 的 Flink 镜像加载运行；M2.1 为 Java 11 字节码，与
> Java 8 集群不兼容，故不采用）。由于 beta7 仍通过 JavaCPP 使用原生 ND4J，**堆外内存重配与集群冒烟
> 步骤是必需的**（见阶段零 0.3 与阶段一第 6 步）。

---

## 共存红线（每次实验都必须遵守）/ Coexistence hard rules

1. 只操作 `synergia-` 前缀的 topic；绝不改动或删除旧 FA-iForest 的 topic。
2. 绝不 `flink cancel` 任何在跑的旧 FA-iForest 作业。
3. 绝不执行 `9-teardown.sh --purge`（会清空共享基础设施）。
4. `M1Job`、`M2Job`（含 M3）都消费 `synergia-source`，**三者不可同时运行**；提交前先确认没有另一个在跑。

---

## 阶段零：一次性准备 / One-time setup

> **前提认知（很重要）/ Key premise**：本项目的 Flink/Kafka 集群是**已有、共享、正在运行**的
> FA-iForest 基础设施（M1/M2 就跑在其上）。因此**不要**用 `0-prepare-local.sh` / `1-sync-to-nodes.sh`
> / `2-up-all.sh` 去"重新部署"它——那套是从零部署整套集群的机器，会重载镜像、重建 master 与 worker
> 容器，打断正在运行的旧作业。M3 只需要下面这几步；唯一会动到集群容器的是 0.3 的堆外重配，且只单独
> 重建 taskmanager。

### 0.1 配置 `.env`（切勿覆盖已有的可用配置）/ Configure `.env` (do NOT clobber a working one)

`deploy/.env` 是被 git 忽略的本地文件，保存真实节点 IP、`SSH_USER`、`SSH_KEY`。
- **若 `deploy/.env` 已存在**（你之前跑 M1/M2 用过）：**保持原样，不要 `cp env.example .env` 覆盖它**，
  否则会把 `SSH_USER`/`SSH_KEY`/节点 IP 重置成模板占位值，导致 `Permission denied (publickey)`。
- **仅当 `deploy/.env` 不存在时**才 `cp deploy/env.example deploy/.env`，然后填入真实值：
  - `NODE_MASTER_PUBLIC_IP`＝从本地能 ssh 到的 master 公网 IP；`NODE_*_IP`＝同 VPC 内网 IP；
  - `SSH_USER`＝集群实际登录用户；`SSH_KEY`＝该用户被集群接受的私钥路径（权限须 `chmod 600`）。
- 自检：`ssh -i <SSH_KEY> <SSH_USER>@<master公网IP> "echo ok"` 能出 `ok` 才算配好。

### 0.2 建 topic / Create topics
```bash
bash deploy/scripts/syn-create-topics.sh          # 幂等；已存在则跳过
```
- 期望：`synergia-source`(8) / `-scores`(4) / `-monitoring`(1) / `-m1-out`(1) 均存在。

### 0.3 堆外内存重配（仅首次上线 M3，或改了 SYN_TM_*/SYN_JAVACPP_* 时）/ Off-heap reconfig

ND4J 经 JavaCPP 在 Java 堆外分配张量，而 Flink 默认 `taskmanager.memory.task.off-heap.size=0`，未计量的
原生分配会在训练期把容器顶出内存上限而被杀。`docker-compose.worker.yml` 已把 off-heap 提到 768MB、
managed 降到 256MB，并通过 `env.java.opts.taskmanager` 显式设定 JavaCPP 的 `maxbytes`/`maxphysicalbytes`/
`cachedir`（详见该文件注释与 `.env` 的 `SYN_TM_*`/`SYN_JAVACPP_*`）。

这是**唯一**需要动集群容器的一步，且**只重建两台 worker 的 taskmanager，不碰 master**。做法（在本地）：
```bash
# 1) 把改过的 worker compose 与 .env 传到两台 worker（不动 master、不重载镜像）
for wk in <worker1 主机> <worker2 主机>; do
  scp -i <SSH_KEY> deploy/compose/docker-compose.worker.yml "<SSH_USER>@$wk:$REMOTE_HOME/compose/"
  scp -i <SSH_KEY> deploy/.env                              "<SSH_USER>@$wk:$REMOTE_HOME/.env"
done
# 2) 在每台 worker 上仅重建 taskmanager 容器（BROKER_ID/NODE_SELF_IP 按该节点填）
ssh -i <SSH_KEY> <SSH_USER>@<worker1> \
  "cd $REMOTE_HOME/compose && BROKER_ID=2 NODE_SELF_IP=<worker1内网IP> \
   docker compose -f docker-compose.worker.yml --env-file ../.env up -d --force-recreate --no-deps taskmanager"
# worker2 同理，BROKER_ID=3、NODE_SELF_IP=<worker2内网IP>
```
> 注意 / Note：重建 taskmanager 会重启该 TM 上正在跑的旧 FA-iForest 任务（从 checkpoint 恢复）。请在
> 可容忍旧作业短暂重启的窗口执行。`--no-deps` 保证不连带重建同一 compose 里的 kafka 等其它服务。

### 0.4 传数据集（仅当集群侧还没有数据集时）/ Upload dataset (only if absent)
```bash
bash deploy/scripts/syn-upload-m1.sh --data-dir <本地 CSV 目录>   # 约 2.3GB，rsync -P 断点续传
```

> 仅当集群**尚未部署**（全新环境）时，才需要 `0-prepare-local.sh`（先构建 Flink 镜像 tar）→
> `1-sync-to-nodes.sh` → `2-up-all.sh` 这条全量部署链，并需对三台节点都有 SSH 权限。本项目属于共享
> 已运行集群，正常不会走这条。

---

## 阶段一：实验前检查（每次实验都做）/ Pre-flight checklist

按顺序执行，任一步不过则停下排查，不要带病开跑。

1. **构建并自检 jar / Build & verify the jar**（本地）
   ```bash
   mvn clean package                       # 生成 target/iot-anomaly-detection-1.0-SNAPSHOT.jar
   bash deploy/scripts/check-jar.sh        # 校验 jar 含最新代码标记（全 PASS 才继续）
   ```
   - 期望产出：`BUILD SUCCESS`；`check-jar.sh` 全部 `[PASS]`。
   - 失败兜底：编译失败先修代码；`check-jar` 有 `[FAIL]` 说明 jar 是旧的，重新 `mvn clean package`。

2. **上传 jar / Upload the jar**（本地）
   ```bash
   bash deploy/scripts/syn-upload-m1.sh --jar-only
   ```
   - 期望产出：jar 落到 `/opt/fa-iforest/jars/iot-anomaly-detection-1.0-SNAPSHOT.jar`（即 jobmanager 挂载的 `/opt/flink/usrlib`）。

3. **确认 topic 就绪 / Confirm topics**（本地）
   ```bash
   bash deploy/scripts/syn-create-topics.sh    # 幂等；已存在则跳过
   ```
   - 期望产出：`synergia-source`(8 分区)、`synergia-scores`(4)、`synergia-monitoring`(1)、`synergia-m1-out`(1) 均存在。

4. **确认无冲突作业 / Confirm no conflicting job**（本地）
   ```bash
   ssh <master> "docker exec jobmanager flink list"
   ```
   - 期望：没有另一个 `M1Job`/`M2Job` 在 `RUNNING`。若有，先让其自然结束或与负责人确认，**不要 cancel 旧作业**。

5. **（仅标定/探针类实验）重放完整性核验 / Replay-integrity gate**（本地）
   ```bash
   bash deploy/scripts/syn-replay-verify.sh --expected-total <EDA 参照值>
   ```
   - 这是标定/探针运行前的固定门槛；四条断言全过才继续。冒烟或功能性小实验可跳过本步。

6. **（首次上线 M3 前跑一次）DL4J 集群冒烟 / DL4J cluster smoke（handover §2 决策 1 第三部分）**
   ```bash
   bash deploy/scripts/syn-m3-smoke.sh --parallelism 8
   ```
   - 在真实容器内验证四点：JavaCPP 堆外上限已生效、原生库解包目录对容器用户 9999 可写、TaskManager 的
     JDK 为 Java 8、jar 内 ND4J 张量原生库仅 linux-x86_64。四点全 PASS 才可放心让 M3 长期在线。
   - 前置：0.3 的堆外重配已生效（TM 已重建）；有 ≥ parallelism 个空闲 slot。该冒烟作业有界，会自行 FINISHED。

---

## 阶段二：实验执行（按实验类型选一条）/ Run the experiment

### 类型 A：联合作业 + 重放（M3 在线评分的主路径）

1. **提交联合作业（M1 摄取/标准化 + pMCOD + M3 重建评分）**
   ```bash
   bash deploy/scripts/syn-submit-m2.sh
   # 可选按需覆盖：--extra '--m3-window-length 60 --m3-z-threshold 2.22'
   ```
   - 期望产出：打印 `JobID` 并轮询至 `RUNNING`。M3 默认启用（`--m3-enabled true`）。

2. **启动重放（tmux 后台常驻）**
   ```bash
   bash deploy/scripts/syn-replay.sh --speedup 600 --start 2022-05-21 --end 2022-05-22
   bash deploy/scripts/syn-replay.sh status     # 查看进度
   bash deploy/scripts/syn-replay.sh attach     # 附着观看（Ctrl+B D 脱离）
   ```
   - 期望产出：`synergia-source` 持续进消息，作业开始向 `synergia-scores`/`synergia-monitoring` 写。

### 类型 B：注入实验（人工异常召回评估，交接 §4）

> 已知限制 / Known gap：注入引擎 `Injector` 已实现，但 `syn-replay.sh` 目前**尚未透传** `--inject` 参数。
> 在跑注入实验前需要先给 `syn-replay.sh` 增加 `--inject`/`--inject-log` 的透传（待办）。补上后：
> ```bash
> bash deploy/scripts/syn-replay.sh --speedup 600 --start <d> --end <d> \
>      --inject "E:Temperature:<startTs>:300:spike:10" --inject-log inject-truth.csv
> ```

---

## 阶段三：实验中监控 / Monitoring during the run

- 作业状态：`ssh <master> "docker exec jobmanager flink list"`；或浏览器开 Flink UI（JM 8081）。
- 指标：Prometheus（9090）/ Grafana（3000）看 M2/M3 自定义指标（如 `m3_online_windows`）。
- 结果流抽样：
  ```bash
  ssh <master> "docker exec kafka-1 kafka-console-consumer.sh --bootstrap-server <brokers> \
      --topic synergia-scores --from-beginning --max-messages 20 --timeout-ms 15000"
  ```
  - M3 的记录 `channel` 字段为 `m3_context`，可据此从 `synergia-scores` 里筛出上下文评分。

---

## 阶段四：实验后收尾 / Post-experiment cleanup

1. **停重放**（重放是常驻的，必须显式停）
   ```bash
   bash deploy/scripts/syn-replay.sh stop
   ```
2. **导出/落盘结果**：把本次 `synergia-scores`/`synergia-monitoring` 需要留存的数据消费导出到本地分析目录。
3. **停本次作业**：M3 联合作业是常驻流作业，实验结束后按需停止**本次自己提交的**作业（用步骤记录的 JobID）：
   ```bash
   ssh <master> "docker exec jobmanager flink cancel <本次JobID>"
   ```
   - 只 cancel 自己这次提交的 JobID；**绝不** cancel 旧 FA-iForest 作业。
4. **清理本实验 topic 数据**（下次实验隔离，仅当需要重来时）：
   ```bash
   bash deploy/scripts/syn-clean-topics.sh --yes     # 只删+重建 synergia- 前缀 topic
   ```
   - 绝不 `9-teardown.sh --purge`。
5. **记录**：把 JobID、重放区间、参数、观察到的现象与结果路径记入实验日志，便于复现与回报。

---

## 常见失败兜底 / Failure fallbacks

| 现象 | 排查方向 |
|---|---|
| `syn-submit-m2.sh` 60s 内未 `RUNNING` | 看 `docker exec jobmanager flink list` 与 JM 日志；常见是 slot 不足（有旧作业占用）或 topic 分区数不符 |
| `synergia-scores` 无 M3 记录 | 确认 M3 已过状态机的 COLLECTING→TRAINING→ONLINE（需累积足够天数的数据）；查 TM 日志中 `[M3]` 行 |
| 重放"看似成功但 0 条落地" | topic 被自动建成 1 分区；用 `syn-clean-topics.sh --yes` 重建为 8 分区后重放 |
| 重放中断 | `syn-replay.sh status` 看状态；重放器支持 `--resume` 断点续跑 |
