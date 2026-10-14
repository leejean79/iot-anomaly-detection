# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview



- **语言**: Java 11（2026-09 起；构建须用 JDK 11，Maven enforcer 限定 [11,12)）
- **构建工具**: Maven
- **核心框架**: Apache Flink 1.13.6 (Scala 2.12)
- **数据源/Sink**: Kafka 2.6.3
- **ML 库**: Smile 2.6.0, JSAT 0.0.9
- **序列化**: Jackson 2.13.5, Flink Avro
- **测试**: JUnit 5 (Jupiter 5.9.3)
- **打包**: maven-shade-plugin（生成 fat jar 用于 Flink 集群部署）

## Build Commands

```bash
# 编译
mvn compile

# 运行测试
mvn test

# 运行单个测试类
mvn test -Dtest=ClassName

# 运行单个测试方法
mvn test -Dtest=ClassName#methodName

# 打包（生成 shaded fat jar）
mvn package

# 清理并打包
mvn clean package
```

## Code Conventions

- 关键代码添加中文和英文双语注释
- Java 开发风格，源码编码 UTF-8
- 包名根路径: `com.leejean`
- 标准 Maven 目录结构: `src/main/java/`, `src/test/java/`, `src/main/resources/`

## Architecture Notes

## Workflow

- 在完成一系列代码后务必进行检查和测试
- 优先运行单个测试验证改动
- 需要生成基于 Maven 工程的 .gitignore 文件


## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

## 5. 沟通风格(Response Style)

**一律使用完整、规范的书面表达。** 给出的回答必须是面向他人阅读的成文段落或清单,不再使用简略短语、行话式缩写、或省略主语谓语的口语化短句。每一条结论、每一处改动说明,都需要清晰、明确、可独立理解地呈现给用户,使其无需上下文猜测即可读懂。具体要求:

- 不使用"开干""动手""跑通"等口语化短语作为回答开头;改用完整的动作描述,例如"接下来开始实施这一节的修改"。
- 不使用未经定义的项目内部缩写或行话缩写;首次出现的术语应给出完整名称,必要时附简短解释。
- 表格、代码示例、命令行示例之外的正文部分,必须以完整句子组织,不使用裸名词短语充当一句话。
- 列举要点时,每个条目须为完整的陈述句,而不是名词+省略号的形式。
- 用完整、正常的中文句子写作，不再用那种省略虚词、堆叠短语、用斜杠缩写的电报式压缩中文
- 提到验收项编号（如 V-M3-5）时，同时写出它对应的内容，例如「V-M3-5（注入召回表）」；编号对照表见 `docs/design/experiment_design_guide_zh.md` 第 0.3 节
- 提到补遗、裁决、开发决策、数据事实等编号或代称（如「补遗四」「开发决策第十条」「步骤 C」）时，同时附一句摘要说明它是什么，例如「补遗四（把 M3 训练移出任务线程）」；常用项见同一文件第 0.5 节
- 回复中同时有「需要上报设计会话的事项」与「需要用户执行的步骤」时，必须明确写出先后顺序：哪些要先上报并等裁决、哪些可以并行进行、哪一步被哪一项裁决阻塞

## 6. 脚本与命令的交付要求(Script Delivery)

**任何在回答中提供的脚本、命令行片段、或可执行的代码块,都必须同时给出明确的使用方式。** 仅给出脚本本身而不说明执行环境与调用方式,会让用户无法直接采用,也无法判断脚本是否符合自身环境。每次提供脚本时,至少必须显式包含以下信息:

1. **执行环境**:脚本运行所在的机器或容器(本地 mac、集群 master 节点、Flink JobManager 容器等),以及需要的工作目录。
2. **调用命令**:用户应当如何启动该脚本,包括完整的命令行示例、必需的参数与可选参数的含义。
3. **前置条件**:脚本依赖的环境变量、文件、服务、或前置步骤(例如"必须先执行 `bash deploy/scripts/3-create-topics.sh` 创建 topic")。
4. **期望产出**:脚本正常执行后,用户应当观察到的现象或产物(例如"在 `analysis-output/` 下生成 `drift-summary.csv`")。
5. **常见失败兜底**:如果存在已知的失败模式或退出码,应明确给出排查方向。

不允许在回答中只丢出一段脚本而不附使用说明,即便该脚本看起来"显而易见"。

**长时间运行的监控、采样与等待类脚本必须无人值守。** 用户提交作业或启动重放之后，不应需要一直开着本地终端守候。具体要求：

- 凡是要持续运行几分钟以上的监控、采样、轮询脚本，一律放到集群 master 的 tmux 会话里运行（参照 `deploy/scripts/syn-m3-watch.sh` 的 `start`、`status`、`stop` 三个子命令），与本地 Mac 是否开着终端、是否睡眠无关；作业取消或结束后脚本自行退出，结果文件留在 master 上，由 `stop` 一次取回到本地。
- 新的监控项优先并入 `syn-m3-watch.sh` 的同一个 tmux 会话（增加一个窗口），而不是另起一个需要单独守候的终端；脚本须支持 `SYN_ON_MASTER=1`，在 master 上直接访问 REST 接口，在本地则经 `ssh fa-master`。
- 运行期间需要跨主机的变量（如作业编号、提交时刻）存进文件（例如 `docs/m3_a4/current_run.env`），用户在任何新终端里 `source` 即可继续，不依赖原来的终端。
- 必须从本地 Mac 发起、又要等待较久的动作（例如等训练开始后再扰动），改为 `caffeinate -i nohup … &` 后台启动，并在说明里写明 Mac 须接电源、不睡眠、不断网，以及回来后查看结果的命令。
- 运行手册里要写清用户只需要在场的时刻（开头启动、结尾收集），以及中途查看进度的单条命令。

## 7. 不迎合约定(No Fitting to Expectations)

本约定出自 2026-10-13 裁决第三节，由用户于同日确认生效。

**凡是检测结果与期望不符，且不符之处能归因到数据的真实现场条件，一律登记为数据事实并进入论文证据案例清单（开发设计日志第十节）；不得为迎合期望修改算法、判据、门槛或框架。** 具体要求：

- 只有当不符之处归因到实现缺陷（等值核验失败、重放断言失败、代码错误）时，才修改代码。
- 发现结果与期望不符时，先查明原因并如实报告给设计会话，由设计会话裁决；不得自行调整参数、判据或口径去贴近期望值。

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
