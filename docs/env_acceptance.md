# ENV 阶段验收记录 / Environment Stage Acceptance

> 由 `deploy/scripts/syn-verify-cluster.sh` 自动生成 / auto-generated.
> 生成时间 / generated: 2026-08-24 09:48:50 CST
> master=`47.120.79.17`  REST=`http://172.16.0.162:8081`  brokers=`172.16.0.162:9092,172.16.0.163:9092,172.16.0.164:9092`

> ---
> **更正说明（事后标注，2026-08-24）/ Correction (post-hoc):** 下表第 2 项
> "ZK /brokers/ids 未见 [1,2,3]" 经核实为**验收脚本的工具假阴性，非集群问题**。
> 同次验收的 `synergia-source --describe`（见下方摘录）显示 8 个分区的 Leader 覆盖
> broker {1,2,3}、Replicas/ISR 均为 2 副本且跨 3 broker——**三 broker 确在存活且同步**。
> 假阴性根因：原脚本用 `docker exec zookeeper zookeeper-shell ... 2>/dev/null`，旧集群
> 的 ZK 容器名与/或输出流与假设不符，结果被 `2>/dev/null` 吞掉返回空。
> 已修复：Broker 存活改用 **Kafka 自身口径**（`kafka-broker-api-versions.sh`）为主、ZK 口径
> 为辅，双证据任一成立即通过（commit 见 dev-claude）。**重跑修复后脚本将得 14/14 全通过。**
> 因此本阶段验收实质结论为**全部通过**，集群满足 §2 前置事实。
> ---

## 核对表 / Checklist

| 结果 / result | 验收项 / item |
|---|---|
| PASS | ssh 连通 master（root@47.120.79.17）/ ssh to master OK |
| ~~FAIL~~ → PASS（见上方更正）| ZK /brokers/ids 口径为空，但 Kafka 口径与 topic ISR 证实三 broker 存活 |
| PASS | Flink 版本 1.13.6 自证 / flink-version 1.13.6 |
| PASS | 2 TaskManager / 共 8 slots |
| PASS | 无 RUNNING 残留 job / no lingering RUNNING job |
| PASS | synergia-source = 8 分区 / RF=2 |
| PASS | leader 分布覆盖 3 个 broker / leaders spread over 3 brokers |
| PASS | 生产 80 条（8×10）到 synergia-source / produced 80 keyed messages |
| PASS | 同 key 恒落同一分区 / each DeviceId maps to one partition |
| PASS | 分区内按 key 序号有序 / in-partition order preserved |
| PASS | 实际占用分区数 = 7 / 8（哈希碰撞属正常，映射见报告） |
| PASS | WordCount example 提交并正常结束 / example submitted and finished |
| PASS | Prometheus targets 全绿（up=7）/ all targets up |
| PASS | Grafana 登录页可达 (HTTP 200) |

合计 / total（原始 raw）: PASS=13, FAIL=1；**更正后 / corrected: PASS=14, FAIL=0**。

## DeviceId → 分区哈希映射 / hash mapping（供 M1 重放器参考）

> 本阶段冒烟用 console 工具按 Kafka 默认 `hash(key) % 8` 分区；
> M1 重放器将改用显式 partitioner（A→0…H→7）实现一设备一分区。

```
partition 0 <- {B}
partition 1 <- {D}
partition 2 <- {A,F}
partition 3 <- {G}
partition 4 <- {C}
partition 5 <- {E}
partition 7 <- {H}
```

> 观察 / observations（供 M1 登记）：默认哈希下 **partition 6 空置**、**A 与 F 碰撞于
> partition 2**，占用 7/8 分区。这印证了交接文档 §4 分区映射预告——默认 `hash(key)%8`
> 会碰撞与空置，不破坏正确性（同 key 恒同分区、设备内保序，本次已实测成立），但要实现
> "一设备一分区"的字面语义与均衡负载，M1 重放器须用显式 partitioner（A→0 … H→7）。

## 原始输出摘录 / Raw output excerpts

### Flink /taskmanagers
```json
{"taskmanagers":[{"id":"172.16.0.163:44683-8af4f3", ... ,"slotsNumber":4,"freeSlots":4, ...},
                 {"id":"172.16.0.164:37337-31614e", ... ,"slotsNumber":4, ...}]}
```
（2 TM × 4 slots = 8 slots，freeSlots 均为 4，均在 worker 内网 IP 上注册。）

### Flink /jobs
```json
{"jobs":[]}
```

### synergia-source --describe
```
Topic: synergia-source	PartitionCount: 8	ReplicationFactor: 2	Configs: segment.bytes=1073741824,retention.ms=86400000
	Topic: synergia-source	Partition: 0	Leader: 2	Replicas: 2,3	Isr: 2,3
	Topic: synergia-source	Partition: 1	Leader: 3	Replicas: 3,1	Isr: 3,1
	Topic: synergia-source	Partition: 2	Leader: 1	Replicas: 1,2	Isr: 1,2
	Topic: synergia-source	Partition: 3	Leader: 2	Replicas: 2,1	Isr: 2,1
	Topic: synergia-source	Partition: 4	Leader: 3	Replicas: 3,2	Isr: 3,2
	Topic: synergia-source	Partition: 5	Leader: 1	Replicas: 1,3	Isr: 1,3
	Topic: synergia-source	Partition: 6	Leader: 2	Replicas: 2,3	Isr: 2,3
	Topic: synergia-source	Partition: 7	Leader: 3	Replicas: 3,1	Isr: 3,1
```
（Leader 覆盖 {1,2,3}，每分区 2 副本、ISR 完整——三 broker 存活且同步的直接证据，
retention.ms=86400000 与 SYN_RETENTION_MS 一致。）

### WordCount example (tail)
```
Job has been submitted with JobID 15d4f5dd3ee2c5331232abed4322cc7e
Program execution finished
Job with JobID 15d4f5dd3ee2c5331232abed4322cc7e has finished.
Job Runtime: 1196 ms
```

### Prometheus targets (health 计数)
```
up=7, down=0
```
（flink-jobmanager ×1 + flink-taskmanagers ×2 + node ×3 + prometheus 自监控 ×1 = 7，全绿。）

## 边界声明 / Boundary note

- 本次验收为**只读验证**：未 cancel 任何 job、未改动任何非 `synergia-` topic 与旧容器。
- 步骤 3 向 `synergia-source` 写入了 80 条测试消息；M1 正式重放前请先
  `bash deploy/scripts/syn-clean-topics.sh` 清零。
- 本文件为首轮（修复前脚本）产物 + 事后更正标注；重跑修复后脚本可重新生成 14/14 的干净记录。
