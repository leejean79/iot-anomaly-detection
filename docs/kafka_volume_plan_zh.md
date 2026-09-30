# Kafka 数据的存放与清理：不加数据卷，按阶段清理（2026-09-30 用户决定）

依据：2026-09-27 裁决书第四节提出为三个 Kafka 代理加命名数据卷；2026-09-28 裁决书第六节将此事交由
用户决定。**2026-09-30 用户决定：不加数据卷。** 阶段进行中不做任何删除或重建容器的操作；阶段实验
完成后调用 `deploy/scripts/syn-reset-env.sh` 清理数据。

本文件原为加卷的改造方案，现改为记录该决定及其操作规则。compose 文件不改动。

## 一、这个决定为什么可行

Kafka 与 ZooKeeper 的数据存放在各自容器的匿名卷里（wurstmeister 镜像在 Dockerfile 中声明了
`VOLUME`）。这决定了：

- **服务器关机、重启、容器重启**：容器还是原来那个，数据都在。自 2026-09-19 创建以来的情况印证了
  这一点。
- **容器被删除或重建**（例如 `docker compose down` 再 `up`，或者 `.env` 改动后执行 `2-up-all.sh`、
  compose 认为配置变了而重建）：新容器会换一个空的新匿名卷，全部主题从零开始；旧卷不会自动删除，
  继续占磁盘。

所以只要阶段进行中不删除、不重建容器，数据就不会丢；阶段之间的清理由脚本统一完成。

## 二、操作规则

1. **阶段进行中不删除、不重建 Kafka 与 ZooKeeper 容器。** 不执行 `docker compose down`、
   `docker rm`，也不执行会重建容器的 `2-up-all.sh`。
2. **服务器每天正常关机、开机不受限制。** 开机后容器会按 `restart: unless-stopped` 自动拉起原容器。
   但要**等三台代理都起来之后**再读写 Kafka：2026-09-27 那次转储读到 0 条，就是因为两台 worker 上的
   代理还没起来。
3. **公网 IP 变了用 `refresh-ips.sh` 更新 `.env` 与 ssh 配置之后，不要随即执行 `2-up-all.sh`。**
   Kafka 对外通告的是内网 IP（`KAFKA_ADVERTISED_LISTENERS` 的 INTERNAL 一项），集群内部读写不依赖
   公网 IP；而 compose 看到 `.env` 中公网 IP 变了，会重建 `kafka-1`（它的 EXTERNAL 通告地址引用了
   公网 IP）。
4. **阶段实验完成、结果已取回并入库后**，执行 `syn-reset-env.sh` 清理：
   ```bash
   bash deploy/scripts/syn-reset-env.sh              # 交互确认后清理
   bash deploy/scripts/syn-reset-env.sh --dry-run    # 只看当前状态，不改动
   ```
   它会停止重放器并清掉续跑位点、只取消本项目的作业、重启 Flink 容器、清空并重建 `synergia-*`
   主题（`retention.ms` 按 `.env` 原参数重建，仍为 −1）、删除远端工作目录 `m2probe`、`m2baseline`、
   `m2surge`、`m3grid`，最后给出核对表。
5. **下一阶段开始前再执行一次 `syn-reset-env.sh`**，把它的核对表当作开跑门槛（全部 PASS 时退出码为 0）。
6. **万一容器被意外重建**：本阶段写进 Kafka 的数据已经没有了，需要从重放开始重做本阶段；旧的匿名卷
   成为孤儿卷，`syn-reset-env.sh` 会报告它们的个数和大小，确认不再需要后加 `--prune-volumes` 回收
   （不可撤销）。

## 三、清理不到、也不应清理的东西

- **登记过的数据集**：`/opt/fa-iforest/datasets/` 下，见 `docs/datasets_zh.md`。`syn-reset-env.sh` 从不
  触碰这个目录；重放用的原始数据也在这里。数据集只有在登记表中标注「退役」后才能删除。
- **旧项目的主题**：`syn-clean-topics.sh` 只接受 `synergia-` 前缀，在设计上删不到其他主题。

## 四、2026-09-27 核查中得到、仍然有效的两点事实

1. **ZooKeeper 同样没有命名数据卷。** 若将来改为加卷，ZooKeeper 必须一起加，否则 ZooKeeper 重建后
   新的集群编号与代理数据目录里记录的旧编号不一致，代理会拒绝启动。
2. **代理的日志目录名带容器主机名**（`/kafka/kafka-logs-<主机名>`），每次新建容器都会变。若将来加卷，
   须同时固定 `KAFKA_LOG_DIRS`。
