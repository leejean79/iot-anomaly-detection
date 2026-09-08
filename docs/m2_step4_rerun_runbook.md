# M2 step4 干净重跑实验步骤（补充指令五，2026-09-08）

> 背景：补充指令四那轮重放有两处硬性异常（三月初被重发使标定窗口实际翻倍到约 3.5 天；整月覆盖不全，
> 探针只扫到约半月），故 step4 终值被拒绝锁定。本文件是"干净重跑 + 完整性核验门槛 + 按固化规则锁定"的
> 完整执行步骤。**每一步的命令、前置、期望产出、失败兜底都写全，可照单执行。**

前置约定：以下命令在本地 Mac 的仓库根目录执行；`<MASTER>`= `NODE_MASTER_PUBLIC_IP`；所有 topic 操作
只动 `synergia-` 前缀，绝不 cancel 旧 FA-iForest 作业、绝不 `9-teardown.sh --purge`。

---

## 阶段零：确认 jar 是最新构建（含 ReplayVerify）

代码本轮新增了重放核验工具 `com.leejean.m2.ReplayVerify`，必须先打包上传，否则阶段二的核验门槛无法运行。

```bash
mvn -o clean package -DskipTests
bash deploy/scripts/check-jar.sh target/iot-anomaly-detection-1.0-SNAPSHOT.jar   # 期望全 PASS，含"冻结落第八天"
bash deploy/scripts/syn-upload-m1.sh --jar-only
```
- 期望产出：check-jar 全 PASS（`calib-days`、`relative-guard`、`calib-repr-out`、`冻结落第八天` 等标记齐全）。
- 失败兜底：任一 FAIL 说明本地 jar 不是最新，重新 `mvn clean package` 后再传。

---

## 阶段一：干净重放 2022 年 3 月（严格一次，不重发、不截短）

补充指令四那轮的病根是断点续跑 offset 残留导致三月初被重发、以及重放提前停止。故先彻底清干净再放。

```bash
# 1) 停掉可能残留的重放会话，并取消我们自己的 synergia M1/M2 作业（只取消我们的，绝不动旧作业）
bash deploy/scripts/syn-replay.sh stop
ssh <MASTER> "docker exec jobmanager flink list"        # 认出名字含 M1Job/M2Job 的我们的作业
ssh <MASTER> "docker exec jobmanager flink cancel <我们的JobID>"

# 2) 删除重放器断点续跑 offset（关键：残留 offset 会导致重发/错位）
ssh <MASTER> "rm -f ${SYN_REPLAY_STATE_DIR:-/opt/fa-iforest/replay-state}/*"

# 3) 清空本项目 synergia-* topic（硬编码前缀白名单，删不到旧项目 topic）
bash deploy/scripts/syn-clean-topics.sh --yes

# 4) 先提交七天标定的 M1 作业（默认 --calib-days 7、防护关闭；先提交再重放）
bash deploy/scripts/syn-submit-m1.sh
#    核对启动横幅三行：Calib days: 7 (rounds/day=8640) / Warmup rounds: 60480 (from --calib-days) / Relative guard: OFF

# 5) 严格重放三月一整段（恰好 2022-03-01 00:00 → 2022-04-01 00:00，一次）
bash deploy/scripts/syn-replay.sh --speedup 3600 --start 2022-03-01 --end 2022-04-01
bash deploy/scripts/syn-replay.sh status                # 等它完整放完（不要中途重启）
```
- 期望产出：重放一次性跑完整月；M1 把整月消费进 `synergia-m1-out`（含前 7 天 warmup 标记）。
- 失败兜底：若 status 显示中途停止，**不要**直接续跑（会重发/错位）——回到本阶段第 1~3 步彻底清理后重放。

---

## 阶段二：重放完整性核验门槛（补充指令五 step1，四断言全过才放行）

这是从本轮起**每次标定/探针前的固定前置门槛**，已做成脚本，不再人工看日志。

```bash
bash deploy/scripts/syn-replay-verify.sh --expected-total <EDA三月逐日轮数合计>
echo "门槛退出码=$?"     # 0=全过放行；1=有断言失败拦住；3=断言一缺 EDA 参照
```
四条断言：① 轮数对账（消费总轮数 vs EDA 三月逐日合计，容差默认 2%）；② 零重复（同设备同时间戳零重复，
重发在此暴露）；③ 边界对齐（最早=03-01 00:00、最晚=03-31 23:59:50）；④ 冻结落第八天（八台标准化冻结
时刻落在第 8 天区间——压缩/重发会提前到第 4 天，本条专抓上轮的病）。

- **关于 `--expected-total`**：这是 EDA 阶段记录的三月逐日轮数**合计**（权威值）。仓库里没有这份 EDA 记录，
  请填入真实合计；也可写进 `.env` 的 `SYN_EDA_MARCH_ROUNDS_TOTAL`。缺省不传会以退出码 3 提示补参
  （名义上限 8×31×8640=2,006,400 仅占位，真实数据有缺口，勿直接用作参照）。
- 期望产出：stdout 四条 PASS + 逐台冻结日；`docs/m2_replay_verify.csv` 逐设备汇总（总轮数/重复/最早最晚/冻结时刻）。
- 失败兜底：**任一断言失败即停**——回阶段一彻底重放；核验不过不得进入阶段三。用 `&&` 串联可自动拦截：
  `bash deploy/scripts/syn-replay-verify.sh --expected-total <N> && <阶段三命令>`。

---

## 阶段三：在干净数据上重做标定代表性与半径重选（补充指令四 step3/step4）

核验通过后才执行。探针一次同时产出扫描表与两份代表性表（一天、七天）。

```bash
bash deploy/scripts/syn-replay-verify.sh --expected-total <N> && \
bash deploy/scripts/syn-m2-probe.sh --max-messages 3000000 \
     --r-grid 0.75,1.0,1.25,1.5,1.75 --k-grid 10 \
     --out-name m2_probe_7d_clean.csv \
     --calib-repr-name m2_calib_repr_7d_clean.csv --calib-repr-days 1,7

# 机选八台半径（脚本按原规则；--prev 传上一轮=一天标定机选值出对比列）
python3 deploy/scripts/m2_pick_r.py \
     --csv docs/m2_probe_7d_clean.csv \
     --out docs/m2_rk_calibration_7d_clean.md \
     --prev "A=0.75,B=0.75,C=1.0,D=0.75,E=0.75,F=0.75,G=1.75,H=0.75"
```
- 期望产出：`docs/m2_probe_7d_clean.csv`（扫描表，slides 应恢复到整月量级约 41k、而非上轮的约 21k）、
  `docs/m2_calib_repr_7d_clean.csv`（两份代表性）、`docs/m2_rk_calibration_7d_clean.md`（机选表）。
- 自检：扫描表的 slides 与代表性表"前七日轮数"应回到名义量级（约 41k slides；前七日约 60k 轮/设备），
  否则说明重放仍不干净，回阶段一。

---

## 阶段四：按固化锁定规则写入配置（补充指令五 step3，本轮起直接执行、不再回传询问）

设计会话已把带外处置固化为规则，据干净数据的机选结果直接套用：

- **带内设备**（离群率落 [0.1%,0.5%]）：取机选值（最接近 0.3% 的 R）。
- **带外偏安静**（全网格 <0.1%）：一律取 **1.0**（离开网格边界一步；安静设备曲线平坦，影响可忽略）。
- **带外偏活跃**（全网格 >0.5%）：取网格最大值 **1.75** 并标记"过度活跃、待人工复核"。

据此把八台终值与 `--calib-days=7` 一并写入配置：

```bash
# 示例（占位，实际值由阶段三干净机选套用上面规则后确定）：
#   SYN_M2_R_PER_DEVICE="A=1.0,B=1.0,C=1.0,D=1.0,E=1.0,F=1.0,G=<机选或1.75>,H=1.0"
#   SYN_M1_CALIB_DAYS=7
# 写入 deploy/.env 后回传设计会话做最终确认，M2 收口。
```
- 边界：代码 agent 据规则套用并写入 `deploy/.env` 的 `SYN_M2_R_PER_DEVICE` 与 `SYN_M1_CALIB_DAYS=7`，
  连同终表与两份代表性表回传设计会话最终确认；不改动算法逻辑，不做浪涌重跑（六月浪涌加"基于一天标定"注记即可）。

---

## 一页速查（命令顺序）

```bash
# 0 打包上传
mvn -o clean package -DskipTests && bash deploy/scripts/check-jar.sh target/*.jar && bash deploy/scripts/syn-upload-m1.sh --jar-only
# 1 清理 + 干净重放
bash deploy/scripts/syn-replay.sh stop
ssh <MASTER> "docker exec jobmanager flink cancel <我们的JobID>"
ssh <MASTER> "rm -f ${SYN_REPLAY_STATE_DIR}/*"
bash deploy/scripts/syn-clean-topics.sh --yes
bash deploy/scripts/syn-submit-m1.sh
bash deploy/scripts/syn-replay.sh --speedup 3600 --start 2022-03-01 --end 2022-04-01
# 2 门槛（不过则停）
bash deploy/scripts/syn-replay-verify.sh --expected-total <N>
# 3 代表性 + 探针 + 机选（门槛通过后）
bash deploy/scripts/syn-m2-probe.sh --max-messages 3000000 --r-grid 0.75,1.0,1.25,1.5,1.75 --k-grid 10 \
     --out-name m2_probe_7d_clean.csv --calib-repr-name m2_calib_repr_7d_clean.csv --calib-repr-days 1,7
python3 deploy/scripts/m2_pick_r.py --csv docs/m2_probe_7d_clean.csv --out docs/m2_rk_calibration_7d_clean.md \
     --prev "A=0.75,B=0.75,C=1.0,D=0.75,E=0.75,F=0.75,G=1.75,H=0.75"
# 4 按固化规则写入 SYN_M2_R_PER_DEVICE + SYN_M1_CALIB_DAYS=7，回传确认
```
