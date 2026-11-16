#!/usr/bin/env python3
# ============================================================================
# m6b/synth_cells.py
# 升级格与抑制规则的合成输入（1.44 版裁决；1.49 版裁决「顺序」第一条）：六台设备、2022-03-01 至 05-01 的正常小时观测，在指定的
# 当地日与时刻注入数据层偏离、性能层确认与回落、上下文评分与模型状态事件，使决策矩阵的每个格与每种抑制都至少命中一次：
#   A 零级（只有数据层持续），随后 K_up 天内同一通道新持续段为一级；
#   B 只有性能层持续为一级，完成后 K_up 天内性能层新确认段为二级；二级之后下线、十天后上线（二级冷却从上线起算），下线期间的
#     新确认段被抑制（模型不在线）；
#   C 两层同日同报为一级；之后的性能层新段在「重训进行中」时被抑制（同时有数据层通道的那天改发零级），一级失败事件解除后照常发一级；
#   D 上下文模型 04-05 才上线：此前只有性能层的结论只记录，含数据层通道的结论改发零级；
#   E 一级之后既无完成也无失败事件：K_fail 天后视为失败，记 REPORT，之后照常发一级；
#   F 正常设备。
# 输出与 m7/prepare_inputs.py 同结构的八张表（多一张 model_events.csv），供 Java M7OfflineRunner 与 m7/decide.py 同读。
# Synthetic inputs that hit every upgrade cell and every suppression, read by both the Java offline runner and m7/decide.py.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机或本地 Mac，仓库根目录，python3.9+（只用标准库）。
# 2. 调用命令 / Invocation: python3 m6b/synth_cells.py [--out docs/m6b/synth_cells/inputs]
# 3. 前置条件 / Preconditions: 无。
# 4. 期望产出 / Expected output: out 下 hours.csv、data_days.csv、perf_events.csv、perf_elements.csv、bursts.csv、refs.csv、
#    freeze.csv、model_events.csv。
# 5. 失败兜底 / Failure fallback: 只写文件，没有已知的失败模式。
# ============================================================================
import argparse
import csv
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/London")
DEVS = list("ABCDEF")
CHANNELS = ("Temperature", "Humidity", "Pressure", "Gas", "Light")


def at(day, hour, minute=0):
    return int(datetime.strptime(day, "%Y-%m-%d").replace(hour=hour, minute=minute, tzinfo=TZ).timestamp())


def days(first, last):
    d = datetime.strptime(first, "%Y-%m-%d")
    while d.strftime("%Y-%m-%d") <= last:
        yield d.strftime("%Y-%m-%d")
        d += timedelta(days=1)


# 数据层偏离：设备 → 通道 → 当地日 / data-layer deviations
DEVIATE = {
    "A": {"Gas": ["2022-03-10", "2022-03-11", "2022-03-15", "2022-03-16"]},
    "C": {"Temperature": ["2022-03-24"], "Gas": ["2022-03-29", "2022-03-30"]},
    "D": {"Gas": ["2022-03-30", "2022-03-31"]},
}
# 性能层确认（门限 1.0）：设备 → [(确认时刻, 回落时刻或 None)] / perf confirmations and fallbacks
PERF = {
    "B": [(("2022-03-09", 20), ("2022-03-11", 6)), (("2022-03-15", 12), ("2022-03-17", 6)), (("2022-03-20", 12), None)],
    "C": [(("2022-03-24", 10), ("2022-03-25", 6)), (("2022-03-28", 12), None)],
    "D": [(("2022-03-29", 10), None)],
    "E": [(("2022-04-06", 10), ("2022-04-08", 6)), (("2022-04-20", 12), None)],
}
# 上下文评分的首条（每日 12:00 一个高于门限的元素）/ first contextual score; one element above threshold daily at noon
FIRST_CTX = {d: "2022-03-08" for d in DEVS}
FIRST_CTX["D"] = "2022-04-05"
# 模型状态事件（--model-events 时使用）/ model-state events
EVENTS = [(d, at(FIRST_CTX[d], 12), "online") for d in DEVS] + [
    ("B", at("2022-03-15", 0), "switch"),      # B 03-10 日界的一级生效 03-12，生效后三天完成 / level 1 done
    ("B", at("2022-03-18", 0), "offline"),     # B 03-16 日界的二级生效 / level 2 effective
    ("B", at("2022-03-28", 12), "online"),     # 新模型上线：二级冷却从这里起算 / the level-2 cooldown starts here
    ("C", at("2022-04-01", 0), "failed"),      # C 的一级失败：解除重训进行中 / releases the pending state
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="docs/m6b/synth_cells/inputs")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    def write(name, header, rows):
        with open(os.path.join(a.out, name), "w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(header)
            w.writerows(rows)

    hours = []
    for day in days("2022-03-01", "2022-04-30"):
        s, e = at(day, 0), int((datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).replace(tzinfo=TZ).timestamp())
        for hs in range(s, e, 3600):
            lh = datetime.fromtimestamp(hs, TZ).strftime("%Y-%m-%d %H")
            for d in DEVS:
                hours.append([d, hs, lh, 360, 360, 0, 0, 0, 0, "", 360, 3600.0, 360, 3600.0, 360, 3600.0])
    write("hours.csv", ["device", "hour_start", "local_hour", "rounds", "admitted", "guard_rounds", "outlier_rounds",
                        "ctx_windows", "ctx_above", "ctx_max", "n_Temperature", "sum_Temperature", "n_Gas", "sum_Gas",
                        "n_Light", "sum_Light"], hours)
    dd = []
    for d in DEVS:
        for day in days("2022-03-01", "2022-04-30"):
            for ch in CHANNELS:
                dd.append([d, day, ch, 1, int(day in DEVIATE.get(d, {}).get(ch, [])), "", "", ""])
    write("data_days.csv", ["device", "day", "channel", "evaluable", "deviated", "transition", "shift", "width_x"], dd)
    ev, el = [], []
    for d in DEVS:
        for day in days(FIRST_CTX[d], "2022-04-30"):
            el.append((d, at(day, 12), 2.0))
        for (cday, ch), fall in PERF.get(d, []):
            ev.append([d, at(cday, ch), "CONFIRM", 1.0, 2.0, 1.0])
            if fall:
                for k in range(6):
                    el.append((d, at(fall[0], fall[1], 10 * k), 0.5))
    el.sort(key=lambda x: (x[0], x[1]))
    write("perf_events.csv", ["device", "ts", "level", "threshold", "magnitude", "hist_sd"], sorted(ev, key=lambda x: (x[0], x[1])))
    write("perf_elements.csv", ["device", "ts", "main_score"], [list(x) for x in el])
    write("bursts.csv", ["device", "ts", "emitted_at", "size", "peak", "duration"], [])
    write("refs.csv", ["device", "channel", "p10", "p90"], [[d, ch, 9.0, 11.0] for d in DEVS for ch in ("Temperature", "Gas", "Light")])
    write("freeze.csv", ["device", "freeze_ts"], [[d, at("2022-03-01", 0, 1)] for d in DEVS])
    write("model_events.csv", ["device", "ts", "event"], sorted(EVENTS, key=lambda x: (x[0], x[1])))
    print("已写出 %s：%d 个小时观测、%d 条性能层事件、%d 个上下文元素、%d 条模型状态事件" % (a.out, len(hours), len(ev), len(el), len(EVENTS)))


if __name__ == "__main__":
    main()
