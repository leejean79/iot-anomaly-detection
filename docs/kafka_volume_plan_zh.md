# Kafka 与 ZooKeeper 数据卷改造方案（执行时机：第二批在线验收开始之前的清理点）

依据：2026-09-27 裁决书《网格定稿、训练上限、六月段核验与 Kafka 数据卷》第四节。
用户已同意在上述清理点执行，并要求：**数据写入本地磁盘后，实验不再需要时及时清理**。

本文件只是方案。compose 文件在清理点之前**不改动**：一旦改动并提交，任何一次 `docker compose up`
都会立即重建容器，而此时六月段数据还没有登记成文件。

## 一、现状（2026-09-27 核实）

1. **三个代理都没有命名数据卷。** master 的 `kafka-1` 与 worker 模板里的 `kafka` 服务都没有
   `volumes` 一节。
2. **更正一处我此前的说法。** 我在六月段核验报告里写过「数据存在容器的可写层里」，这不准确。
   wurstmeister/kafka 镜像在 Dockerfile 里声明了 `VOLUME`，所以每个容器新建时会自动得到一个
   **匿名卷**，日志目录是 `/kafka/kafka-logs-<容器主机名>`（`syn-disk-report.sh` 第 83 行就是这样
   查找它的）。后果与原先所说的相同：重建容器会换成一个新的空匿名卷，代理从零开始；区别是旧数据
   并未被删除，而是留在一个不再挂载的匿名卷里，继续占用磁盘。
3. **ZooKeeper 同样没有命名数据卷，这一点裁决书没有提到，但必须一并处理。** 主题的元数据（主题
   列表、分区分配、主题配置，包括 `retention.ms=-1`）存在 ZooKeeper 里。如果只给代理加卷而
   ZooKeeper 被重建，新的 ZooKeeper 会生成新的集群编号，而代理数据目录里记着旧编号，代理会拒绝
   启动。
4. **日志目录名随主机名变化。** 默认目录名里带容器主机名，而主机名在每次新建容器时都会变。即使
   加了命名卷，重建后代理也会在卷里另开一个新目录，旧数据依然用不上。因此必须把
   `KAFKA_LOG_DIRS` 固定下来。

## 二、执行前提

1. 六月段转储已登记成文件：起止时刻、行数、校验和、存放路径写入 `docs/datasets_zh.md`，
   文件放在 `/opt/fa-iforest/datasets/` 下（见裁决书第三节）。
2. 没有正在运行的本项目作业，也没有别人正在使用这个集群。
3. 用户在执行当时再确认一次。重建会清空全部主题，包括旧项目遗留的主题（旧项目已退役）。

## 三、执行步骤（在清理点执行）

**第 1 步：在运行中的容器里核实两个路径（只读）**

```bash
ssh fa-master "docker exec kafka-1 sh -c 'ls -d /kafka/kafka-logs-*; env | grep KAFKA_LOG_DIRS'"
ssh fa-master "docker exec zookeeper sh -c 'grep -E \"^dataDir|^dataLogDir\" /opt/zookeeper-*/conf/zoo.cfg'"
```

第二条命令打印的 `dataDir` 就是 ZooKeeper 的数据卷挂载点。下面的改动以实际打印的路径为准。

**第 2 步：修改 compose 文件**

master（`deploy/compose/docker-compose.master.yml`）：

```yaml
  zookeeper:
    volumes:
      - zk-data:<第 1 步打印的 dataDir>
  kafka-1:
    environment:
      KAFKA_LOG_DIRS: /kafka/kafka-logs
    volumes:
      - kafka-data:/kafka
volumes:
  zk-data:
  kafka-data:
```

worker（`deploy/compose/docker-compose.worker.yml`），同样的两处加在 `kafka` 服务上，
并在文件末尾声明 `kafka-data` 卷。

**第 3 步：重建并恢复主题**

按 `2-up-all.sh` 的顺序重建三台的 ZooKeeper 与代理，再执行 `syn-create-topics.sh` 按 `.env`
原参数重建本项目主题。最后核对每个主题的 `retention.ms` 仍为 -1。

**第 4 步：验证数据确实落在命名卷里**

写入少量消息后，只重建 `kafka-1`（`docker compose up -d --force-recreate kafka-1`）。之后主题
偏移量应当保持不变。这一步证明重建容器不再丢数据。

**第 5 步：回收旧匿名卷**

旧容器留下的匿名卷此时已经不再挂载。先用 `syn-disk-report.sh` 量出它们的大小，再逐个确认后
删除。

## 四、数据清理规则（用户要求：实验不再需要时及时清理）

加了数据卷以后，主题数据会一直留在磁盘上，除非主动删除（`retention.ms=-1`，不会自动过期）。
因此每次实验结束都要做下面三件事：

1. **主题**：实验结束、结果已经取回并入库后，用 `syn-clean-topics.sh` 清空本项目的 `synergia-*`
   主题。这个脚本只能删除带 `synergia-` 前缀的主题。
2. **转储工作副本**：`/opt/fa-iforest/m3grid/`、`/opt/fa-iforest/m2probe/` 下的 `*.jsonl` 是临时
   工作副本，实验结束即删除。需要长期复用的数据只保留 `/opt/fa-iforest/datasets/` 下登记过的
   那一份。
3. **核对**：清理后执行 `syn-disk-report.sh`，确认磁盘空间确实已经释放。

登记过的数据集不随单次实验清理。只有在 `docs/datasets_zh.md` 里标注「退役」之后才能删除。
