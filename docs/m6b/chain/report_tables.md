# 链式离线实验报告表格（m6b/chain_report.py 生成）

比较期 03-08 至 04-30 当地日；命令日为决定所在的当地日，决定时刻为该日结束的日界，生效时刻为再下一个日界。

## 判读口径一：开环对照与聚合模块收口基线

| 核对项 | 收口基线条数 | 开环对照 |
| --- | --- | --- |
| alerts.csv | 8391 | 逐字节相同 |
| commands.csv | 76 | 逐字节相同 |
| review.csv | 48 | 逐字节相同 |
| 小时离群轮数（收口输入 hours.csv） | 11533 | 不同 0 个设备小时 |
| 数据层日结论（data_days.csv：可判、偏离、转移） | 2140 | 不同 0 条 |
| 离群率爆发（bursts.csv） | 250 | 不同 0 条 |

## 判读口径二：三档命令清单与片段矩阵

### 逐日命令（触发；· 为无命令）

| 命令日 | 开环对照 | 甲：重估一天 | 乙：重估七天 | 丙：只重估位置 |
| --- | --- | --- | --- | --- |
| 03-12 | G Light | G Light | G Light | G Light |
| 03-13 | H Light | H Light | H Light | H Light |
| 03-14 | B Gas；C Gas；E Gas；G Gas | B Gas；C Gas；E Gas | B Gas；C Gas；E Gas | B Gas；C Gas；E Gas |
| 03-15 | D Gas；H Gas | D Gas | D Gas；G Gas | D Gas；G Gas |
| 03-16 | F Temperature | F Temperature | F Temperature；H Gas | F Temperature；H Gas |
| 03-19 | E Temperature | B Gas；C Gas | · | · |
| 03-20 | D Temperature | D Temperature；F Temperature；H Light | · | · |
| 03-21 | A Gas；E Temperature|Gas；G Gas | A Gas | A Gas | A Gas |
| 03-22 | C Temperature；D Temperature|Gas | G Temperature | F Temperature；G Temperature | F Temperature；G Temperature |
| 03-23 | A Temperature；B Temperature|Gas；F Temperature|Light；G Temperature|Gas | E Temperature；F Temperature|Gas | C Temperature；D Temperature；E Temperature | C Temperature；D Temperature；E Temperature |
| 03-24 | H Temperature|Gas | B Gas；C Temperature；D Gas；H Temperature | A Temperature；B Temperature|Gas；H Light | A Temperature；B Temperature|Gas；H Light |
| 03-25 | A Temperature|Gas；C Temperature|Gas；F Temperature|Gas | A Gas | · | · |
| 03-26 | · | G Light | G Temperature|Light | G Temperature|Light |
| 03-27 | H Temperature|Gas|Light | H Light | A Temperature；B Temperature；H Light | A Temperature；B Temperature；H Light |
| 03-28 | F Temperature|Gas | A Gas；B Gas；F Temperature | · | · |
| 03-29 | · | C Gas；D Gas；E Gas；G Gas | G Gas | G Gas |
| 03-30 | · | · | A Gas；B Temperature|Gas；D Temperature | A Gas；B Temperature|Gas；D Temperature |
| 03-31 | · | A Temperature；B Temperature；F Temperature | E Gas | E Gas |
| 04-01 | B Gas | · | G Temperature | G Temperature |
| 04-02 | D Gas | D Gas；G Temperature | A Temperature；B Temperature；C Temperature | A Temperature；B Temperature；C Temperature |
| 04-03 | A Temperature|Gas|Light；B Temperature|Gas；E Gas；F Gas；G Temperature|Gas|Light | A Temperature；B Light；C Temperature；E Gas；F Temperature|Light | D Temperature；E Temperature；F Temperature | D Temperature；E Temperature；F Temperature |
| 04-04 | C Gas | · | A Gas；G Gas | A Gas；G Gas |
| 04-05 | F Temperature | G Temperature | · | · |
| 04-06 | G Light | A Temperature|Gas；B Gas；C Gas；D Temperature|Gas；E Gas；F Temperature|Gas；H Temperature | H Temperature | H Temperature |
| 04-07 | · | · | B Gas | B Gas |
| 04-09 | B Temperature；D Gas；F Gas；H Temperature | H Gas | H Temperature|Gas | H Temperature|Gas |
| 04-11 | A Gas；B Gas；E Gas；G Gas；H Gas | A Gas；B Gas；C Gas；D Gas；E Gas；G Gas | · | · |
| 04-12 | C Gas；F Temperature | F Temperature；H Temperature|Light | H Gas | H Gas |
| 04-13 | A Temperature；B Temperature|Light；D Temperature；E Temperature|Gas | · | A Temperature；B Temperature|Light；D Temperature；G Temperature | A Temperature；B Temperature|Light；D Temperature；G Temperature |
| 04-14 | C Temperature；G Temperature|Gas | B Gas；E Gas | · | · |
| 04-15 | A Temperature|Gas；B Gas；D Temperature|Gas；F Temperature|Gas | D Temperature；F Gas；H Gas | E Temperature | E Temperature |
| 04-16 | G Temperature|Gas|Light；H Gas|Light | · | A Temperature；D Temperature | A Temperature；D Temperature |
| 04-17 | · | C Temperature | B Light；C Temperature；F Temperature；G Temperature | B Light；C Temperature；F Temperature；G Temperature |
| 04-18 | A Temperature|Light；H Temperature|Gas|Light | A Temperature|Light | H Light | H Light |
| 04-21 | · | F Temperature | · | · |
| 04-22 | F Gas | A Gas | A Gas；F Temperature|Gas | A Gas；F Temperature|Gas |
| 04-23 | A Gas；G Gas | C Temperature；D Temperature；H Light | H Light | H Light |
| 04-24 | F Temperature | F Temperature|Gas；G Light | G Light | G Light |
| 04-25 | D Temperature|Gas | D Gas | · | · |
| 04-26 | A Temperature；B Gas；E Temperature|Gas；G Temperature|Gas；H Gas | A Temperature；H Gas | · | · |
| 04-27 | · | B Gas；F Temperature | B Gas；F Gas；H Gas | B Gas；F Gas；H Gas |
| 04-28 | B Temperature|Gas | D Temperature；E Temperature；G Gas | D Temperature | D Temperature |
| 04-30 | D Temperature|Gas；E Gas；F Gas；G Gas | H Temperature | · | · |

### 片段矩阵

| 片段 | 开环对照 | 甲：重估一天 | 乙：重估七天 | 丙：只重估位置 |
| --- | --- | --- | --- | --- |
| 气体抬高（命令日 03-14 至 03-21，触发含气体） | —；命令：B 03-14 Gas；C 03-14 Gas；E 03-14 Gas；G 03-14 Gas；D 03-15 Gas；H 03-15 Gas；A 03-21 Gas；E 03-21 Temperature|Gas；G 03-21 Gas | —；命令：B 03-14 Gas；C 03-14 Gas；E 03-14 Gas；D 03-15 Gas；B 03-19 Gas；C 03-19 Gas；A 03-21 Gas | —；命令：B 03-14 Gas；C 03-14 Gas；E 03-14 Gas；D 03-15 Gas；G 03-15 Gas；H 03-16 Gas；A 03-21 Gas | —；命令：B 03-14 Gas；C 03-14 Gas；E 03-14 Gas；D 03-15 Gas；G 03-15 Gas；H 03-16 Gas；A 03-21 Gas |
| 三月事件 03-22 至 03-27 | 质量 42；切换 34；漂移 1048；瞬态 20；命令：C 03-22 Temperature；D 03-22 Temperature|Gas；A 03-23 Temperature；B 03-23 Temperature|Gas；F 03-23 Temperature|Light；G 03-23 Temperature|Gas；H 03-24 Temperature|Gas；A 03-25 Temperature|Gas；C 03-25 Temperature|Gas；F 03-25 Temperature|Gas；H 03-27 Temperature|Gas|Light | 质量 42；切换 21；漂移 234；现场 38；瞬态 399；环境 5；异常 46；命令：G 03-22 Temperature；E 03-23 Temperature；F 03-23 Temperature|Gas；B 03-24 Gas；C 03-24 Temperature；D 03-24 Gas；H 03-24 Temperature；A 03-25 Gas；G 03-26 Light；H 03-27 Light | 质量 42；切换 20；漂移 233；现场 37；瞬态 458；环境 5；异常 60；命令：F 03-22 Temperature；G 03-22 Temperature；C 03-23 Temperature；D 03-23 Temperature；E 03-23 Temperature；A 03-24 Temperature；B 03-24 Temperature|Gas；H 03-24 Light；G 03-26 Temperature|Light；A 03-27 Temperature；B 03-27 Temperature；H 03-27 Light | 质量 42；切换 39；漂移 228；现场 41；瞬态 440；环境 5；异常 60；命令：F 03-22 Temperature；G 03-22 Temperature；C 03-23 Temperature；D 03-23 Temperature；E 03-23 Temperature；A 03-24 Temperature；B 03-24 Temperature|Gas；H 03-24 Light；G 03-26 Temperature|Light；A 03-27 Temperature；B 03-27 Temperature；H 03-27 Light |
| 四月时段一 04-02 13 时至 04-03 08 时 | 切换 4；漂移 38；现场 5；瞬态 53；环境 5；异常 7；命令：D 04-02 Gas；A 04-03 Temperature|Gas|Light；B 04-03 Temperature|Gas；E 04-03 Gas；F 04-03 Gas；G 04-03 Temperature|Gas|Light | 切换 2；漂移 18；现场 4；瞬态 61；异常 12；命令：D 04-02 Gas；G 04-02 Temperature；A 04-03 Temperature；B 04-03 Light；C 04-03 Temperature；E 04-03 Gas；F 04-03 Temperature|Light | 切换 4；漂移 37；现场 9；瞬态 49；环境 7；命令：A 04-02 Temperature；B 04-02 Temperature；C 04-02 Temperature；D 04-03 Temperature；E 04-03 Temperature；F 04-03 Temperature | 切换 5；漂移 36；现场 10；瞬态 49；环境 6；命令：A 04-02 Temperature；B 04-02 Temperature；C 04-02 Temperature；D 04-03 Temperature；E 04-03 Temperature；F 04-03 Temperature |
| 四月时段二 04-04 05 时至 04-04 22 时 | 切换 4；漂移 106；现场 1；瞬态 15；命令：C 04-04 Gas | 切换 2；漂移 88；现场 5；瞬态 3；环境 7；异常 7；命令：无 | 漂移 54；现场 4；瞬态 31；环境 8；异常 9；命令：A 04-04 Gas；G 04-04 Gas | 切换 4；漂移 52；现场 5；瞬态 28；环境 8；异常 9；命令：A 04-04 Gas；G 04-04 Gas |
| 四月时段三 04-10 00 时至 04-10 07 时 | 切换 1；漂移 32；现场 5；瞬态 23；命令：无 | 切换 2；漂移 8；现场 14；瞬态 33；环境 3；异常 1；命令：无 | 漂移 8；现场 14；瞬态 7；环境 18；异常 4；命令：无 | 漂移 8；现场 15；瞬态 6；环境 18；异常 4；命令：无 |
| 四月时段四 04-14 00 时至 04-16 02 时 | 质量 3；漂移 381；现场 1；瞬态 23；命令：C 04-14 Temperature；G 04-14 Temperature|Gas；A 04-15 Temperature|Gas；B 04-15 Gas；D 04-15 Temperature|Gas；F 04-15 Temperature|Gas；G 04-16 Temperature|Gas|Light；H 04-16 Gas|Light | 质量 3；切换 2；漂移 56；现场 7；瞬态 263；命令：B 04-14 Gas；E 04-14 Gas；D 04-15 Temperature；F 04-15 Gas；H 04-15 Gas | 质量 3；切换 4；漂移 95；现场 9；瞬态 156；异常 2；命令：E 04-15 Temperature；A 04-16 Temperature；D 04-16 Temperature | 质量 3；切换 1；漂移 99；现场 9；瞬态 155；异常 2；命令：E 04-15 Temperature；A 04-16 Temperature；D 04-16 Temperature |
| 四月时段五 04-22 07 时至 04-22 13 时 | 漂移 56；命令：F 04-22 Gas | 切换 2；漂移 7；瞬态 27；异常 2；命令：A 04-22 Gas | 瞬态 13；异常 8；命令：A 04-22 Gas；F 04-22 Temperature|Gas | 瞬态 13；异常 8；命令：A 04-22 Gas；F 04-22 Temperature|Gas |
| C 正午（日照直射日 10 至 15 时） | 质量 1；切换 3；漂移 15；瞬态 4；异常 1；命令：无 | 质量 1；切换 1；漂移 4；现场 3；瞬态 5；命令：C 04-17 Temperature | 质量 1；现场 2；瞬态 11；命令：C 04-17 Temperature | 质量 1；现场 2；瞬态 11；命令：C 04-17 Temperature |
| G 光照 | 质量 13；切换 47；漂移 692；现场 7；瞬态 324；异常 9；命令：G 03-12 Light；G 04-03 Temperature|Gas|Light；G 04-06 Light；G 04-16 Temperature|Gas|Light | 质量 13；切换 19；漂移 208；现场 3；瞬态 543；环境 5；异常 10；命令：G 03-12 Light；G 03-26 Light；G 04-24 Light | 质量 13；切换 10；漂移 230；现场 3；瞬态 328；环境 13；异常 10；命令：G 03-12 Light；G 03-26 Temperature|Light；G 04-24 Light | 质量 13；切换 42；漂移 219；现场 21；瞬态 298；环境 12；异常 20；命令：G 03-12 Light；G 03-26 Temperature|Light；G 04-24 Light |
| 平稳日（50 个设备日） | 质量 65；切换 19；漂移 1009；瞬态 61；异常 1；命令：F 04-05 Temperature；C 04-14 Temperature；A 04-23 Gas；F 04-24 Temperature | 质量 65；切换 35；漂移 209；现场 10；瞬态 403；环境 2；异常 13；命令：B 03-28 Gas；C 03-29 Gas；D 03-29 Gas；E 03-29 Gas；G 04-05 Temperature；C 04-17 Temperature；F 04-21 Temperature；C 04-23 Temperature；D 04-23 Temperature；F 04-24 Temperature|Gas | 质量 65；切换 16；漂移 226；现场 12；瞬态 290；异常 16；命令：A 03-30 Gas；B 03-30 Temperature|Gas；B 04-17 Light；C 04-17 Temperature；G 04-17 Temperature | 质量 65；切换 23；漂移 223；现场 12；瞬态 286；异常 16；命令：A 03-30 Gas；B 03-30 Temperature|Gas；B 04-17 Light；C 04-17 Temperature；G 04-17 Temperature |
| 十四段注入（E，所覆盖的设备小时） | 漂移 38；质量 6 | 无告警 16；异常 12；现场 10；质量 6 | 无告警 16；异常 12；现场 10；质量 6 | 无告警 16；异常 12；现场 10；质量 6 |

### 全程

| 档 | 命令 | 每台 | 同一台命令的最短间隔 | 丙型实例 | 平稳日命令 | 告警 | 告警按归因 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 开环对照 | 76 | A=10 B=10 C=6 D=9 E=8 F=12 G=12 H=9 | 2 天 | — | 4 | 8391 | 质量 287；切换 181；漂移 6068；现场 61；瞬态 1735；环境 5；异常 54 |
| 甲：重估一天 | 80 | A=10 B=10 C=9 D=11 E=8 F=12 G=9 H=11 | 2 天 | 43 | 10 | 6034 | 质量 287；切换 149；漂移 1800；现场 167；瞬态 3431；环境 19；异常 181 |
| 乙：重估七天 | 60 | A=9 B=9 C=4 D=7 E=5 F=6 G=10 H=10 | 2 天 | 29 | 5 | 5110 | 质量 287；切换 105；漂移 1355；现场 156；瞬态 2951；环境 39；异常 217 |
| 丙：只重估位置 | 60 | A=9 B=9 C=4 D=7 E=5 F=6 G=10 H=10 | 2 天 | 29 | 5 | 5130 | 质量 287；切换 170；漂移 1337；现场 179；瞬态 2892；环境 38；异常 227 |

（开环对照的命令只发出不执行，丙型实例不适用。丙型实例的判别见判读口径五。）

## 判读口径三：每条命令生效后七天内触发通道与非触发通道的数据层偏离日数

七天为生效当地日起的七个当地日；偏离日为日末偏离的日记录（换版本后为新版本的记录）。非触发通道含只记录的湿度与气压。同一台在七天内又执行下一条命令时照常计数。

| 档 | 执行的命令 | 触发通道偏离日 | 每条命令 | 非触发通道偏离日 | 每条命令 | 其中湿度与气压 |
| --- | --- | --- | --- | --- | --- | --- |
| 甲：重估一天 | 79 | 218 | 2.76 | 857 | 10.85 | 589 |
| 乙：重估七天 | 60 | 119 | 1.98 | 413 | 6.88 | 284 |
| 丙：只重估位置 | 60 | 119 | 1.98 | 413 | 6.88 | 284 |

气体抬高的命令（命令日 03-14 至 03-16、触发含气体）生效后七天内湿度与气压的偏离日数（片段矩阵第一行的连带偏离）：

| 档 | 设备 湿度偏离日/气压偏离日 |
| --- | --- |
| 甲：重估一天 | B 6/2；C 5/2；E 4/4；D 3/4 |
| 乙：重估七天 | B 1/4；C 1/4；E 1/4；D 2/5；G 3/5；H 2/6 |
| 丙：只重估位置 | B 1/4；C 1/4；E 1/4；D 2/5；G 3/5；H 2/6 |

平稳日上的命令与该台上一条已执行命令的重估窗口（乙、丙两档的重估窗口为生效前 7 × 86,400 秒，甲档为生效前 86,400 秒，所以跨过夏令时开始日的窗口起点为当地 23 时）：

| 档 | 设备 | 命令日 | 触发 | 上一条已执行命令 |
| --- | --- | --- | --- | --- |
| 甲：重估一天 | B | 03-28 | Gas | 03-24 命令（Gas），生效 03-26，重估窗口 03-25 00:00 至 03-25 24:00 |
| 甲：重估一天 | C | 03-29 | Gas | 03-24 命令（Temperature），生效 03-26，重估窗口 03-25 00:00 至 03-25 24:00 |
| 甲：重估一天 | D | 03-29 | Gas | 03-24 命令（Gas），生效 03-26，重估窗口 03-25 00:00 至 03-25 24:00 |
| 甲：重估一天 | E | 03-29 | Gas | 03-23 命令（Temperature），生效 03-25，重估窗口 03-24 00:00 至 03-24 24:00 |
| 甲：重估一天 | G | 04-05 | Temperature | 04-02 命令（Temperature），生效 04-04，重估窗口 04-03 00:00 至 04-03 24:00 |
| 甲：重估一天 | C | 04-17 | Temperature | 04-11 命令（Gas），生效 04-13，重估窗口 04-12 00:00 至 04-12 24:00 |
| 甲：重估一天 | F | 04-21 | Temperature | 04-15 命令（Gas），生效 04-17，重估窗口 04-16 00:00 至 04-16 24:00 |
| 甲：重估一天 | C | 04-23 | Temperature | 04-17 命令（Temperature），生效 04-19，重估窗口 04-18 00:00 至 04-18 24:00 |
| 甲：重估一天 | D | 04-23 | Temperature | 04-15 命令（Temperature），生效 04-17，重估窗口 04-16 00:00 至 04-16 24:00 |
| 甲：重估一天 | F | 04-24 | Temperature|Gas | 04-21 命令（Temperature），生效 04-23，重估窗口 04-22 00:00 至 04-22 24:00 |
| 乙：重估七天 | A | 03-30 | Gas | 03-27 命令（Temperature），生效 03-29，重估窗口 03-21 23:00 至 03-28 24:00 |
| 乙：重估七天 | B | 03-30 | Temperature|Gas | 03-27 命令（Temperature），生效 03-29，重估窗口 03-21 23:00 至 03-28 24:00 |
| 乙：重估七天 | B | 04-17 | Light | 04-13 命令（Temperature|Light），生效 04-15，重估窗口 04-08 00:00 至 04-14 24:00 |
| 乙：重估七天 | C | 04-17 | Temperature | 04-02 命令（Temperature），生效 04-04，重估窗口 03-28 00:00 至 04-03 24:00 |
| 乙：重估七天 | G | 04-17 | Temperature | 04-13 命令（Temperature），生效 04-15，重估窗口 04-08 00:00 至 04-14 24:00 |
| 丙：只重估位置 | A | 03-30 | Gas | 03-27 命令（Temperature），生效 03-29，重估窗口 03-21 23:00 至 03-28 24:00 |
| 丙：只重估位置 | B | 03-30 | Temperature|Gas | 03-27 命令（Temperature），生效 03-29，重估窗口 03-21 23:00 至 03-28 24:00 |
| 丙：只重估位置 | B | 04-17 | Light | 04-13 命令（Temperature|Light），生效 04-15，重估窗口 04-08 00:00 至 04-14 24:00 |
| 丙：只重估位置 | C | 04-17 | Temperature | 04-02 命令（Temperature），生效 04-04，重估窗口 03-28 00:00 至 04-03 24:00 |
| 丙：只重估位置 | G | 04-17 | Temperature | 04-13 命令（Temperature），生效 04-15，重估窗口 04-08 00:00 至 04-14 24:00 |

<details><summary>甲：重估一天：逐条命令</summary>

| 设备 | 命令日 | 生效日 | 触发 | 触发通道偏离日 | 非触发通道偏离日 |
| --- | --- | --- | --- | --- | --- |
| G | 03-12 | 03-14 | Light | 0 | Humidity 2；Pressure 7；Gas 3 |
| H | 03-13 | 03-15 | Light | 2 | Temperature 3；Humidity 4；Pressure 2 |
| B | 03-14 | 03-16 | Gas | 4 | Temperature 1；Humidity 6；Pressure 2 |
| C | 03-14 | 03-16 | Gas | 6 | Humidity 5；Pressure 2 |
| E | 03-14 | 03-16 | Gas | 0 | Temperature 1；Humidity 4；Pressure 4 |
| D | 03-15 | 03-17 | Gas | 2 | Temperature 3；Humidity 3；Pressure 4 |
| F | 03-16 | 03-18 | Temperature | 6 | Humidity 1；Pressure 3；Gas 3 |
| B | 03-19 | 03-21 | Gas | 4 | Temperature 3；Humidity 2；Pressure 2 |
| C | 03-19 | 03-21 | Gas | 3 | Temperature 3；Humidity 2；Pressure 1 |
| D | 03-20 | 03-22 | Temperature | 0 | Humidity 2；Pressure 4；Gas 4 |
| F | 03-20 | 03-22 | Temperature | 6 | Humidity 2；Pressure 4；Gas 3 |
| H | 03-20 | 03-22 | Light | 2 | Temperature 4；Humidity 2；Pressure 3；Gas 4 |
| A | 03-21 | 03-23 | Gas | 6 | Temperature 1；Humidity 7；Pressure 6 |
| G | 03-22 | 03-24 | Temperature | 1 | Humidity 2；Pressure 4；Gas 4；Light 1 |
| E | 03-23 | 03-25 | Temperature | 2 | Humidity 5；Pressure 4；Gas 3 |
| F | 03-23 | 03-25 | Temperature|Gas | 8 | Humidity 5；Pressure 4；Light 1 |
| B | 03-24 | 03-26 | Gas | 4 | Temperature 4；Humidity 6；Pressure 4 |
| C | 03-24 | 03-26 | Temperature | 1 | Humidity 6；Pressure 4；Gas 2 |
| D | 03-24 | 03-26 | Gas | 4 | Temperature 2；Humidity 6；Pressure 4 |
| H | 03-24 | 03-26 | Temperature | 3 | Humidity 5；Pressure 4；Gas 2；Light 2 |
| A | 03-25 | 03-27 | Gas | 3 | Temperature 4；Humidity 6；Pressure 5 |
| G | 03-26 | 03-28 | Light | 0 | Temperature 4；Humidity 5；Pressure 6；Gas 5 |
| H | 03-27 | 03-29 | Light | 0 | Temperature 3；Humidity 3；Pressure 3 |
| A | 03-28 | 03-30 | Gas | 3 | Temperature 6；Humidity 4；Pressure 5 |
| B | 03-28 | 03-30 | Gas | 3 | Temperature 3；Humidity 4；Pressure 5；Light 2 |
| F | 03-28 | 03-30 | Temperature | 6 | Humidity 4；Pressure 5；Gas 3；Light 3 |
| C | 03-29 | 03-31 | Gas | 3 | Temperature 2；Humidity 5；Pressure 6 |
| D | 03-29 | 03-31 | Gas | 5 | Temperature 3；Humidity 7；Pressure 6 |
| E | 03-29 | 03-31 | Gas | 5 | Temperature 1；Humidity 4；Pressure 6 |
| G | 03-29 | 03-31 | Gas | 4 | Temperature 6；Humidity 7；Pressure 6 |
| A | 03-31 | 04-02 | Temperature | 5 | Humidity 1；Pressure 5；Gas 6 |
| B | 03-31 | 04-02 | Temperature | 1 | Humidity 1；Pressure 5；Gas 5；Light 2 |
| F | 03-31 | 04-02 | Temperature | 5 | Humidity 1；Pressure 5；Gas 6；Light 2 |
| D | 04-02 | 04-04 | Gas | 5 | Temperature 2；Humidity 5；Pressure 6 |
| G | 04-02 | 04-04 | Temperature | 3 | Humidity 7；Pressure 5；Gas 4 |
| A | 04-03 | 04-05 | Temperature | 4 | Humidity 2；Pressure 6；Gas 6；Light 1 |
| B | 04-03 | 04-05 | Light | 0 | Temperature 2；Humidity 2；Pressure 6；Gas 6 |
| C | 04-03 | 04-05 | Temperature | 0 | Humidity 1；Pressure 6；Gas 5 |
| E | 04-03 | 04-05 | Gas | 5 | Humidity 1；Pressure 6 |
| F | 04-03 | 04-05 | Temperature|Light | 4 | Humidity 2；Pressure 6；Gas 4 |
| G | 04-05 | 04-07 | Temperature | 0 | Humidity 5；Pressure 4；Gas 5 |
| A | 04-06 | 04-08 | Temperature|Gas | 6 | Humidity 2；Pressure 6；Light 1 |
| B | 04-06 | 04-08 | Gas | 6 | Temperature 4；Humidity 2；Pressure 6；Light 1 |
| C | 04-06 | 04-08 | Gas | 4 | Humidity 1；Pressure 6 |
| D | 04-06 | 04-08 | Temperature|Gas | 4 | Humidity 1；Pressure 6 |
| E | 04-06 | 04-08 | Gas | 5 | Temperature 1；Humidity 1；Pressure 6 |
| F | 04-06 | 04-08 | Temperature|Gas | 5 | Humidity 3；Pressure 6 |
| H | 04-06 | 04-08 | Temperature | 4 | Humidity 3；Pressure 5；Gas 5；Light 3 |
| H | 04-09 | 04-11 | Gas | 4 | Temperature 5；Humidity 3；Pressure 6；Light 5 |
| A | 04-11 | 04-13 | Gas | 1 | Temperature 3；Pressure 7；Light 4 |
| B | 04-11 | 04-13 | Gas | 2 | Temperature 3；Humidity 3；Pressure 6；Light 1 |
| C | 04-11 | 04-13 | Gas | 2 | Temperature 2；Pressure 6 |
| D | 04-11 | 04-13 | Gas | 0 | Temperature 5；Pressure 7 |
| E | 04-11 | 04-13 | Gas | 2 | Temperature 3；Humidity 3；Pressure 6 |
| G | 04-11 | 04-13 | Gas | 2 | Temperature 2；Pressure 7 |
| F | 04-12 | 04-14 | Temperature | 3 | Humidity 4；Pressure 7；Gas 2 |
| H | 04-12 | 04-14 | Temperature|Light | 4 | Humidity 1；Pressure 7；Gas 3 |
| B | 04-14 | 04-16 | Gas | 0 | Temperature 3；Humidity 6；Pressure 6 |
| E | 04-14 | 04-16 | Gas | 0 | Temperature 2；Humidity 6；Pressure 6 |
| D | 04-15 | 04-17 | Temperature | 5 | Humidity 2；Pressure 7；Gas 1 |
| F | 04-15 | 04-17 | Gas | 2 | Temperature 6；Humidity 5；Pressure 7 |
| H | 04-15 | 04-17 | Gas | 1 | Temperature 1；Humidity 4；Pressure 7；Light 2 |
| C | 04-17 | 04-19 | Temperature | 3 | Pressure 5；Gas 3 |
| A | 04-18 | 04-20 | Temperature|Light | 4 | Humidity 3；Pressure 6；Gas 4 |
| F | 04-21 | 04-23 | Temperature | 6 | Humidity 2；Pressure 5；Gas 4 |
| A | 04-22 | 04-24 | Gas | 3 | Temperature 4；Humidity 3；Pressure 6；Light 2 |
| C | 04-23 | 04-25 | Temperature | 0 | Humidity 1；Pressure 6；Gas 3 |
| D | 04-23 | 04-25 | Temperature | 3 | Pressure 5；Gas 2 |
| H | 04-23 | 04-25 | Light | 3 | Temperature 3；Pressure 5；Gas 3 |
| F | 04-24 | 04-26 | Temperature|Gas | 4 | Humidity 2；Pressure 3 |
| G | 04-24 | 04-26 | Light | 0 | Temperature 1；Humidity 1；Pressure 4；Gas 2 |
| D | 04-25 | 04-27 | Gas | 0 | Temperature 3；Pressure 3 |
| A | 04-26 | 04-28 | Temperature | 1 | Pressure 2；Light 1 |
| H | 04-26 | 04-28 | Gas | 0 | Temperature 2；Pressure 2；Light 1 |
| B | 04-27 | 04-29 | Gas | 0 | Temperature 1；Humidity 1；Light 1 |
| F | 04-27 | 04-29 | Temperature | 0 | Humidity 2 |
| D | 04-28 | 04-30 | Temperature | 0 | 0 |
| E | 04-28 | 04-30 | Temperature | 1 | Light 1 |
| G | 04-28 | 04-30 | Gas | 0 | Temperature 1 |

</details>

<details><summary>乙：重估七天：逐条命令</summary>

| 设备 | 命令日 | 生效日 | 触发 | 触发通道偏离日 | 非触发通道偏离日 |
| --- | --- | --- | --- | --- | --- |
| G | 03-12 | 03-14 | Light | 0 | Humidity 1；Pressure 5；Gas 2 |
| H | 03-13 | 03-15 | Light | 0 | Humidity 1；Pressure 6；Gas 3 |
| B | 03-14 | 03-16 | Gas | 0 | Humidity 1；Pressure 4 |
| C | 03-14 | 03-16 | Gas | 2 | Temperature 1；Humidity 1；Pressure 4 |
| E | 03-14 | 03-16 | Gas | 1 | Temperature 1；Humidity 1；Pressure 4 |
| D | 03-15 | 03-17 | Gas | 2 | Temperature 2；Humidity 2；Pressure 5 |
| G | 03-15 | 03-17 | Gas | 1 | Temperature 3；Humidity 3；Pressure 5 |
| F | 03-16 | 03-18 | Temperature | 4 | Humidity 3；Pressure 5；Gas 2 |
| H | 03-16 | 03-18 | Gas | 1 | Temperature 1；Humidity 2；Pressure 6；Light 3 |
| A | 03-21 | 03-23 | Gas | 4 | Temperature 5；Humidity 3；Pressure 1 |
| F | 03-22 | 03-24 | Temperature | 2 | Humidity 1；Pressure 2；Gas 2 |
| G | 03-22 | 03-24 | Temperature | 3 | Humidity 2；Pressure 2；Gas 5；Light 1 |
| C | 03-23 | 03-25 | Temperature | 0 | Humidity 1；Pressure 3；Gas 1 |
| D | 03-23 | 03-25 | Temperature | 3 | Humidity 1；Pressure 3；Gas 4 |
| E | 03-23 | 03-25 | Temperature | 2 | Humidity 2；Pressure 3；Gas 3 |
| A | 03-24 | 03-26 | Temperature | 5 | Humidity 4；Pressure 3；Gas 4 |
| B | 03-24 | 03-26 | Temperature|Gas | 8 | Humidity 4；Pressure 3 |
| H | 03-24 | 03-26 | Light | 2 | Temperature 3；Humidity 5；Pressure 3 |
| G | 03-26 | 03-28 | Temperature|Light | 4 | Humidity 4；Pressure 2；Gas 4 |
| A | 03-27 | 03-29 | Temperature | 5 | Humidity 4；Pressure 3；Gas 4 |
| B | 03-27 | 03-29 | Temperature | 6 | Humidity 4；Pressure 3；Gas 3 |
| H | 03-27 | 03-29 | Light | 0 | Temperature 3；Humidity 3；Pressure 3 |
| G | 03-29 | 03-31 | Gas | 3 | Temperature 4；Humidity 4；Pressure 1 |
| A | 03-30 | 04-01 | Gas | 2 | Temperature 3；Humidity 5；Pressure 2 |
| B | 03-30 | 04-01 | Temperature|Gas | 6 | Humidity 5；Pressure 2 |
| D | 03-30 | 04-01 | Temperature | 2 | Humidity 1；Pressure 2；Gas 2 |
| E | 03-31 | 04-02 | Gas | 1 | Temperature 2；Pressure 2 |
| G | 04-01 | 04-03 | Temperature | 1 | Humidity 2；Pressure 2；Gas 3 |
| A | 04-02 | 04-04 | Temperature | 0 | Humidity 2；Pressure 2；Gas 2 |
| B | 04-02 | 04-04 | Temperature | 2 | Humidity 2；Pressure 2；Gas 3 |
| C | 04-02 | 04-04 | Temperature | 0 | Humidity 1；Pressure 2；Gas 1 |
| D | 04-03 | 04-05 | Temperature | 0 | Pressure 2；Gas 1 |
| E | 04-03 | 04-05 | Temperature | 0 | Pressure 2；Gas 1 |
| F | 04-03 | 04-05 | Temperature | 1 | Humidity 2；Pressure 2 |
| A | 04-04 | 04-06 | Gas | 1 | Temperature 1；Humidity 1；Pressure 2 |
| G | 04-04 | 04-06 | Gas | 1 | Temperature 1；Humidity 1；Pressure 2 |
| H | 04-06 | 04-08 | Temperature | 6 | Humidity 5；Pressure 3；Gas 5；Light 2 |
| B | 04-07 | 04-09 | Gas | 0 | Temperature 5；Humidity 2；Pressure 1；Light 3 |
| H | 04-09 | 04-11 | Temperature|Gas | 5 | Humidity 4；Pressure 3；Light 3 |
| H | 04-12 | 04-14 | Gas | 0 | Temperature 1；Humidity 3；Pressure 3；Light 3 |
| A | 04-13 | 04-15 | Temperature | 6 | Humidity 1；Pressure 2；Gas 1 |
| B | 04-13 | 04-15 | Temperature|Light | 6 | Humidity 2；Pressure 2 |
| D | 04-13 | 04-15 | Temperature | 3 | Humidity 1；Pressure 2 |
| G | 04-13 | 04-15 | Temperature | 3 | Humidity 1；Pressure 2 |
| E | 04-15 | 04-17 | Temperature | 1 | 0 |
| A | 04-16 | 04-18 | Temperature | 4 | Humidity 1；Gas 3 |
| D | 04-16 | 04-18 | Temperature | 0 | Humidity 1 |
| B | 04-17 | 04-19 | Light | 0 | Humidity 5；Pressure 1 |
| C | 04-17 | 04-19 | Temperature | 0 | Humidity 2；Pressure 1；Gas 1 |
| F | 04-17 | 04-19 | Temperature | 1 | Humidity 1；Pressure 1；Gas 3 |
| G | 04-17 | 04-19 | Temperature | 0 | Humidity 5；Pressure 1；Light 2 |
| H | 04-18 | 04-20 | Light | 3 | Temperature 2；Humidity 4；Pressure 5；Gas 3 |
| A | 04-22 | 04-24 | Gas | 1 | Temperature 1；Humidity 6；Pressure 5 |
| F | 04-22 | 04-24 | Temperature|Gas | 4 | Humidity 3；Pressure 4 |
| H | 04-23 | 04-25 | Light | 1 | Temperature 1；Humidity 2；Pressure 4；Gas 2 |
| G | 04-24 | 04-26 | Light | 0 | Temperature 1；Humidity 3；Pressure 5 |
| B | 04-27 | 04-29 | Gas | 0 | Temperature 1；Pressure 1 |
| F | 04-27 | 04-29 | Gas | 0 | Pressure 1 |
| H | 04-27 | 04-29 | Gas | 0 | Temperature 1；Pressure 1；Light 1 |
| D | 04-28 | 04-30 | Temperature | 0 | 0 |

</details>

<details><summary>丙：只重估位置：逐条命令</summary>

| 设备 | 命令日 | 生效日 | 触发 | 触发通道偏离日 | 非触发通道偏离日 |
| --- | --- | --- | --- | --- | --- |
| G | 03-12 | 03-14 | Light | 0 | Humidity 1；Pressure 5；Gas 2 |
| H | 03-13 | 03-15 | Light | 0 | Humidity 1；Pressure 6；Gas 3 |
| B | 03-14 | 03-16 | Gas | 0 | Humidity 1；Pressure 4 |
| C | 03-14 | 03-16 | Gas | 2 | Temperature 1；Humidity 1；Pressure 4 |
| E | 03-14 | 03-16 | Gas | 1 | Temperature 1；Humidity 1；Pressure 4 |
| D | 03-15 | 03-17 | Gas | 2 | Temperature 2；Humidity 2；Pressure 5 |
| G | 03-15 | 03-17 | Gas | 1 | Temperature 3；Humidity 3；Pressure 5 |
| F | 03-16 | 03-18 | Temperature | 4 | Humidity 3；Pressure 5；Gas 2 |
| H | 03-16 | 03-18 | Gas | 1 | Temperature 1；Humidity 2；Pressure 6；Light 3 |
| A | 03-21 | 03-23 | Gas | 4 | Temperature 5；Humidity 3；Pressure 1 |
| F | 03-22 | 03-24 | Temperature | 2 | Humidity 1；Pressure 2；Gas 2 |
| G | 03-22 | 03-24 | Temperature | 3 | Humidity 2；Pressure 2；Gas 5；Light 1 |
| C | 03-23 | 03-25 | Temperature | 0 | Humidity 1；Pressure 3；Gas 1 |
| D | 03-23 | 03-25 | Temperature | 3 | Humidity 1；Pressure 3；Gas 4 |
| E | 03-23 | 03-25 | Temperature | 2 | Humidity 2；Pressure 3；Gas 3 |
| A | 03-24 | 03-26 | Temperature | 5 | Humidity 4；Pressure 3；Gas 4 |
| B | 03-24 | 03-26 | Temperature|Gas | 8 | Humidity 4；Pressure 3 |
| H | 03-24 | 03-26 | Light | 2 | Temperature 3；Humidity 5；Pressure 3 |
| G | 03-26 | 03-28 | Temperature|Light | 4 | Humidity 4；Pressure 2；Gas 4 |
| A | 03-27 | 03-29 | Temperature | 5 | Humidity 4；Pressure 3；Gas 4 |
| B | 03-27 | 03-29 | Temperature | 6 | Humidity 4；Pressure 3；Gas 3 |
| H | 03-27 | 03-29 | Light | 0 | Temperature 3；Humidity 3；Pressure 3 |
| G | 03-29 | 03-31 | Gas | 3 | Temperature 4；Humidity 4；Pressure 1 |
| A | 03-30 | 04-01 | Gas | 2 | Temperature 3；Humidity 5；Pressure 2 |
| B | 03-30 | 04-01 | Temperature|Gas | 6 | Humidity 5；Pressure 2 |
| D | 03-30 | 04-01 | Temperature | 2 | Humidity 1；Pressure 2；Gas 2 |
| E | 03-31 | 04-02 | Gas | 1 | Temperature 2；Pressure 2 |
| G | 04-01 | 04-03 | Temperature | 1 | Humidity 2；Pressure 2；Gas 3 |
| A | 04-02 | 04-04 | Temperature | 0 | Humidity 2；Pressure 2；Gas 2 |
| B | 04-02 | 04-04 | Temperature | 2 | Humidity 2；Pressure 2；Gas 3 |
| C | 04-02 | 04-04 | Temperature | 0 | Humidity 1；Pressure 2；Gas 1 |
| D | 04-03 | 04-05 | Temperature | 0 | Pressure 2；Gas 1 |
| E | 04-03 | 04-05 | Temperature | 0 | Pressure 2；Gas 1 |
| F | 04-03 | 04-05 | Temperature | 1 | Humidity 2；Pressure 2 |
| A | 04-04 | 04-06 | Gas | 1 | Temperature 1；Humidity 1；Pressure 2 |
| G | 04-04 | 04-06 | Gas | 1 | Temperature 1；Humidity 1；Pressure 2 |
| H | 04-06 | 04-08 | Temperature | 6 | Humidity 5；Pressure 3；Gas 5；Light 2 |
| B | 04-07 | 04-09 | Gas | 0 | Temperature 5；Humidity 2；Pressure 1；Light 3 |
| H | 04-09 | 04-11 | Temperature|Gas | 5 | Humidity 4；Pressure 3；Light 3 |
| H | 04-12 | 04-14 | Gas | 0 | Temperature 1；Humidity 3；Pressure 3；Light 3 |
| A | 04-13 | 04-15 | Temperature | 6 | Humidity 1；Pressure 2；Gas 1 |
| B | 04-13 | 04-15 | Temperature|Light | 6 | Humidity 2；Pressure 2 |
| D | 04-13 | 04-15 | Temperature | 3 | Humidity 1；Pressure 2 |
| G | 04-13 | 04-15 | Temperature | 3 | Humidity 1；Pressure 2 |
| E | 04-15 | 04-17 | Temperature | 1 | 0 |
| A | 04-16 | 04-18 | Temperature | 4 | Humidity 1；Gas 3 |
| D | 04-16 | 04-18 | Temperature | 0 | Humidity 1 |
| B | 04-17 | 04-19 | Light | 0 | Humidity 5；Pressure 1 |
| C | 04-17 | 04-19 | Temperature | 0 | Humidity 2；Pressure 1；Gas 1 |
| F | 04-17 | 04-19 | Temperature | 1 | Humidity 1；Pressure 1；Gas 3 |
| G | 04-17 | 04-19 | Temperature | 0 | Humidity 5；Pressure 1；Light 2 |
| H | 04-18 | 04-20 | Light | 3 | Temperature 2；Humidity 4；Pressure 5；Gas 3 |
| A | 04-22 | 04-24 | Gas | 1 | Temperature 1；Humidity 6；Pressure 5 |
| F | 04-22 | 04-24 | Temperature|Gas | 4 | Humidity 3；Pressure 4 |
| H | 04-23 | 04-25 | Light | 1 | Temperature 1；Humidity 2；Pressure 4；Gas 2 |
| G | 04-24 | 04-26 | Light | 0 | Temperature 1；Humidity 3；Pressure 5 |
| B | 04-27 | 04-29 | Gas | 0 | Temperature 1；Pressure 1 |
| F | 04-27 | 04-29 | Gas | 0 | Pressure 1 |
| H | 04-27 | 04-29 | Gas | 0 | Temperature 1；Pressure 1；Light 1 |
| D | 04-28 | 04-30 | Temperature | 0 | 0 |

</details>

## 判读口径四：每条命令生效前后点通道离群率与状态重建瞬态

离群率为到达滑动步的离群轮数除以进入点通道的轮数。生效前取生效前二十四小时；生效后取生效后第一个小时（状态重建瞬态）与生效后二十四小时；开环同窗为同一台在开环对照中同一时间窗的离群率（命令不执行时的对照）。表中为逐条命令的中位数与均值。

| 档 | 生效前 24 小时 | 生效后第 1 小时 | 生效后 24 小时 | 开环同窗（生效后 24 小时） |
| --- | --- | --- | --- | --- |
| 甲：重估一天 | 0.00% / 0.33% | 0.00% / 0.00% | 0.00% / 0.31% | 0.08% / 0.21% |
| 乙：重估七天 | 0.00% / 0.06% | 0.00% / 0.03% | 0.00% / 0.05% | 0.08% / 0.28% |
| 丙：只重估位置 | 0.07% / 0.21% | 0.00% / 0.09% | 0.08% / 0.28% | 0.08% / 0.28% |

（每格为「中位数 / 均值」。p_point = 0.061343306938749904，即小时离群率超过它时点通道有标志。）

## 判读口径五：丙型实例（命令后回落再命令）

判别：同一台、同一触发通道的相邻两条命令，前一条与后一条在各自命令日的偏移（当日中位数相对参照）符号相反，即生效后数据层在新参照上看到反向偏离并以第二条命令映回。「反向进入偏离日」为两条命令之间该通道在新参照上进入偏离的第一个当地日。

**甲：重估一天**：43 例；其中第二条在第一条生效后七天内决定的 27 例。

| 设备 | 通道 | 第一条（命令日、偏移） | 生效日 | 反向进入偏离日 | 第二条（命令日、偏移） | 生效到第二条决定（天） |
| --- | --- | --- | --- | --- | --- | --- |
| A | Gas | 03-25，+0.583 | 03-27 | 03-27 | 03-28，-0.323 | 2 |
| A | Gas | 04-06，-0.908 | 04-08 | 04-08 | 04-11，+1.59 | 4 |
| A | Temperature | 04-03，-0.347 | 04-05 | 04-05 | 04-06，+0.133 | 2 |
| A | Temperature | 04-06，+0.133 | 04-08 | 04-10 | 04-18，-0.0647 | 11 |
| A | Temperature | 04-18，-0.0647 | 04-20 | 04-21 | 04-26，+0.735 | 7 |
| B | Gas | 03-14，+1.09 | 03-16 | 03-16 | 03-19，-0.385 | 4 |
| B | Gas | 03-19，-0.385 | 03-21 | 03-23 | 03-24，+0.898 | 4 |
| B | Gas | 03-24，+0.898 | 03-26 | 03-27 | 03-28，-0.335 | 3 |
| B | Gas | 04-06，-0.852 | 04-08 | 04-08 | 04-11，+1.1 | 4 |
| B | Gas | 04-11，+1.1 | 04-13 | 04-13 | 04-14，-0.0963 | 2 |
| B | Gas | 04-14，-0.0963 | 04-16 | 04-23 | 04-27，+0.284 | 12 |
| C | Gas | 03-14，+1.09 | 03-16 | 03-16 | 03-19，-0.929 | 4 |
| C | Gas | 04-06，-0.817 | 04-08 | 04-10 | 04-11，+1.93 | 4 |
| C | Temperature | 03-24，+0.446 | 03-26 | 03-29 | 04-03，-0.439 | 9 |
| C | Temperature | 04-03，-0.439 | 04-05 | 04-16 | 04-17，+0.369 | 13 |
| C | Temperature | 04-17，+0.369 | 04-19 | 04-22 | 04-23，-0.338 | 5 |
| D | Gas | 03-24，+2.42 | 03-26 | 03-28 | 03-29，-0.365 | 4 |
| D | Gas | 03-29，-0.365 | 03-31 | 04-01 | 04-02，+1.12 | 3 |
| D | Gas | 04-02，+1.12 | 04-04 | 04-05 | 04-06，-0.559 | 3 |
| D | Gas | 04-06，-0.559 | 04-08 | 04-08 | 04-11，+1.22 | 4 |
| D | Temperature | 04-15，+0.223 | 04-17 | 04-18 | 04-23，-0.352 | 7 |
| E | Gas | 03-14，+1.32 | 03-16 | 03-23 | 03-29，-0.437 | 14 |
| E | Gas | 03-29，-0.437 | 03-31 | 04-02 | 04-03，+1.71 | 4 |
| E | Gas | 04-03，+1.71 | 04-05 | 04-05 | 04-06，-1.07 | 2 |
| E | Gas | 04-06，-1.07 | 04-08 | 04-10 | 04-11，+0.901 | 4 |
| E | Gas | 04-11，+0.901 | 04-13 | 04-13 | 04-14，-0.0601 | 2 |
| E | Temperature | 03-23，+0.589 | 03-25 | 03-29 | 04-28，-0.208 | 35 |
| F | Gas | 04-06，-0.696 | 04-08 | 04-08 | 04-15，+1.17 | 8 |
| F | Gas | 04-15，+1.17 | 04-17 | 04-22 | 04-24，-0.476 | 8 |
| F | Temperature | 03-23，+0.387 | 03-25 | 03-25 | 03-28，-0.297 | 4 |
| F | Temperature | 04-03，-0.415 | 04-05 | 04-05 | 04-06，+0.078 | 2 |
| F | Temperature | 04-12，+0.66 | 04-14 | 04-18 | 04-21，-0.221 | 8 |
| F | Temperature | 04-21，-0.221 | 04-23 | 04-23 | 04-24，+0.147 | 2 |
| F | Temperature | 04-24，+0.147 | 04-26 | 04-26 | 04-27，-0.00621 | 2 |
| G | Gas | 03-29，-0.717 | 03-31 | 04-02 | 04-11，+0.739 | 12 |
| G | Gas | 04-11，+0.739 | 04-13 | 04-13 | 04-28，-0.538 | 16 |
| G | Light | 03-12，-0.708 | 03-14 | 03-26 | 03-26，+0.319 | 13 |
| G | Temperature | 03-22，+0.795 | 03-24 | 03-30 | 04-02，-0.763 | 10 |
| G | Temperature | 04-02，-0.763 | 04-04 | 04-04 | 04-05，+0.624 | 2 |
| H | Light | 03-27，-0.406 | 03-29 | 04-11 | 04-12，+1.04 | 15 |
| H | Temperature | 03-24，+0.382 | 03-26 | 03-29 | 04-06，-0.673 | 12 |
| H | Temperature | 04-06，-0.673 | 04-08 | 04-08 | 04-12，+0.466 | 5 |
| H | Temperature | 04-12，+0.466 | 04-14 | 04-15 | 04-30，-0.487 | 17 |

**乙：重估七天**：29 例；其中第二条在第一条生效后七天内决定的 11 例。

| 设备 | 通道 | 第一条（命令日、偏移） | 生效日 | 反向进入偏离日 | 第二条（命令日、偏移） | 生效到第二条决定（天） |
| --- | --- | --- | --- | --- | --- | --- |
| A | Gas | 03-21，+1.18 | 03-23 | 03-24 | 03-30，-0.341 | 8 |
| A | Gas | 03-30，-0.341 | 04-01 | 04-04 | 04-04，+1.26 | 4 |
| A | Temperature | 03-27，+0.137 | 03-29 | 03-30 | 04-02，-0.957 | 5 |
| A | Temperature | 04-02，-0.957 | 04-04 | 04-12 | 04-13，+0.296 | 10 |
| B | Gas | 03-24，+1.14 | 03-26 | 03-29 | 03-30，-0.504 | 5 |
| B | Gas | 04-07，-0.356 | 04-09 | 04-26 | 04-27，+0.441 | 19 |
| B | Light | 04-13，+0.773 | 04-15 | 04-16 | 04-17，-0.658 | 3 |
| B | Temperature | 03-27，+0.203 | 03-29 | 03-29 | 03-30，-0.586 | 2 |
| B | Temperature | 04-02，-0.703 | 04-04 | 04-08 | 04-13，+0.537 | 10 |
| C | Temperature | 03-23，+0.728 | 03-25 | 04-01 | 04-02，-0.49 | 9 |
| C | Temperature | 04-02，-0.49 | 04-04 | 04-16 | 04-17，+0.226 | 14 |
| D | Temperature | 03-23，+0.523 | 03-25 | 03-29 | 03-30，-0.328 | 6 |
| D | Temperature | 04-03，-0.314 | 04-05 | 04-12 | 04-13，+0.203 | 9 |
| D | Temperature | 04-16，+0.279 | 04-18 | 04-27 | 04-28，-0.292 | 11 |
| E | Gas | 03-14，+1.32 | 03-16 | 03-21 | 03-31，-0.35 | 16 |
| E | Temperature | 03-23，+0.672 | 03-25 | 03-29 | 04-03，-0.369 | 10 |
| E | Temperature | 04-03，-0.369 | 04-05 | 04-14 | 04-15，+0.275 | 11 |
| F | Gas | 04-22，+2.13 | 04-24 | 04-24 | 04-27，-0.265 | 4 |
| F | Temperature | 03-22，+0.782 | 03-24 | 03-24 | 04-03，-0.579 | 11 |
| F | Temperature | 04-03，-0.579 | 04-05 | 04-07 | 04-17，+0.324 | 13 |
| F | Temperature | 04-17，+0.324 | 04-19 | 04-22 | 04-22，-0.628 | 4 |
| G | Gas | 03-15，+0.569 | 03-17 | 03-21 | 03-29，-0.296 | 13 |
| G | Gas | 03-29，-0.296 | 03-31 | 04-03 | 04-04，+0.882 | 5 |
| G | Light | 03-12，-0.708 | 03-14 | 03-26 | 03-26，+0.231 | 13 |
| G | Temperature | 03-26，+0.299 | 03-28 | 03-31 | 04-01，-0.857 | 5 |
| G | Temperature | 04-01，-0.857 | 04-03 | 04-03 | 04-13，+0.28 | 11 |
| H | Light | 03-13，-0.847 | 03-15 | 03-22 | 03-24，+0.408 | 10 |
| H | Light | 03-24，+0.408 | 03-26 | 03-26 | 03-27，-0.587 | 2 |
| H | Light | 03-27，-0.587 | 03-29 | 04-12 | 04-18，+0.0264 | 21 |

**丙：只重估位置**：29 例；其中第二条在第一条生效后七天内决定的 11 例。

| 设备 | 通道 | 第一条（命令日、偏移） | 生效日 | 反向进入偏离日 | 第二条（命令日、偏移） | 生效到第二条决定（天） |
| --- | --- | --- | --- | --- | --- | --- |
| A | Gas | 03-21，+1.18 | 03-23 | 03-24 | 03-30，-0.341 | 8 |
| A | Gas | 03-30，-0.341 | 04-01 | 04-04 | 04-04，+1.26 | 4 |
| A | Temperature | 03-27，+0.137 | 03-29 | 03-30 | 04-02，-0.957 | 5 |
| A | Temperature | 04-02，-0.957 | 04-04 | 04-12 | 04-13，+0.296 | 10 |
| B | Gas | 03-24，+1.14 | 03-26 | 03-29 | 03-30，-0.504 | 5 |
| B | Gas | 04-07，-0.356 | 04-09 | 04-26 | 04-27，+0.441 | 19 |
| B | Light | 04-13，+0.773 | 04-15 | 04-16 | 04-17，-0.658 | 3 |
| B | Temperature | 03-27，+0.203 | 03-29 | 03-29 | 03-30，-0.586 | 2 |
| B | Temperature | 04-02，-0.703 | 04-04 | 04-08 | 04-13，+0.537 | 10 |
| C | Temperature | 03-23，+0.728 | 03-25 | 04-01 | 04-02，-0.49 | 9 |
| C | Temperature | 04-02，-0.49 | 04-04 | 04-16 | 04-17，+0.226 | 14 |
| D | Temperature | 03-23，+0.523 | 03-25 | 03-29 | 03-30，-0.328 | 6 |
| D | Temperature | 04-03，-0.314 | 04-05 | 04-12 | 04-13，+0.203 | 9 |
| D | Temperature | 04-16，+0.279 | 04-18 | 04-27 | 04-28，-0.292 | 11 |
| E | Gas | 03-14，+1.32 | 03-16 | 03-21 | 03-31，-0.35 | 16 |
| E | Temperature | 03-23，+0.672 | 03-25 | 03-29 | 04-03，-0.369 | 10 |
| E | Temperature | 04-03，-0.369 | 04-05 | 04-14 | 04-15，+0.275 | 11 |
| F | Gas | 04-22，+2.13 | 04-24 | 04-24 | 04-27，-0.265 | 4 |
| F | Temperature | 03-22，+0.782 | 03-24 | 03-24 | 04-03，-0.579 | 11 |
| F | Temperature | 04-03，-0.579 | 04-05 | 04-07 | 04-17，+0.324 | 13 |
| F | Temperature | 04-17，+0.324 | 04-19 | 04-22 | 04-22，-0.628 | 4 |
| G | Gas | 03-15，+0.569 | 03-17 | 03-21 | 03-29，-0.296 | 13 |
| G | Gas | 03-29，-0.296 | 03-31 | 04-03 | 04-04，+0.882 | 5 |
| G | Light | 03-12，-0.708 | 03-14 | 03-26 | 03-26，+0.231 | 13 |
| G | Temperature | 03-26，+0.299 | 03-28 | 03-31 | 04-01，-0.857 | 5 |
| G | Temperature | 04-01，-0.857 | 04-03 | 04-03 | 04-13，+0.28 | 11 |
| H | Light | 03-13，-0.847 | 03-15 | 03-22 | 03-24，+0.408 | 10 |
| H | Light | 03-24，+0.408 | 03-26 | 03-26 | 03-27，-0.587 | 2 |
| H | Light | 03-27，-0.587 | 03-29 | 04-12 | 04-18，+0.0264 | 21 |

## 判读口径六：重估窗口选定判据的两项计数

判据（交接文档第四节，运行前写定）：以命令总数最少且非触发通道连带偏离日数最少为优先，两者冲突时以连带偏离日数为准；若丙与乙在两项上都相当，取乙。本表只列两项计数，选定由设计会话宣布。

| 档 | 命令总数 | 执行的命令 | 非触发通道连带偏离日数 | 其中湿度与气压 |
| --- | --- | --- | --- | --- |
| 甲：重估一天 | 80 | 79 | 857 | 589 |
| 乙：重估七天 | 60 | 60 | 413 | 284 |
| 丙：只重估位置 | 60 | 60 | 413 | 284 |
