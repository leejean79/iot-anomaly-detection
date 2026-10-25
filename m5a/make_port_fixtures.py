#!/usr/bin/env python3
# ============================================================================
# m5a/make_port_fixtures.py
# 为 Java 测试 `M5aPortTest` 生成对照数据：一段合成的逐轮序列（含缺失、超过 60 秒的间隔、冷启动间隔、底值与非底值游程），
# 以及 `eda/m5a_extract.py` 中两个移植函数（Injector、RepeatRunGuard）在它上面的输出。Java 测试用原类重算并逐值比较
# （操作口径第七节）。/ Fixtures for the Java test comparing the Python ports of Injector and RepeatRunGuard.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 开发机，仓库根目录；python3 与 numpy、pandas。
# 2. 调用命令 / Invocation: python3 m5a/make_port_fixtures.py
# 3. 前置条件 / Preconditions: eda/m5a_extract.py 存在。
# 4. 期望产出 / Expected output: src/test/resources/m5a/port_rounds.csv（ts、五个通道的原始值、冷启动标记、注入后的五个值、守卫标记）
#    与 port_spec.txt（注入规格、阈值与底值）。
# 5. 失败兜底 / Failure fallback: 导入失败时检查 eda/m5a_extract.py 的路径。
# ============================================================================
import importlib.util
import os

import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("ex", os.path.join(ROOT, "eda", "m5a_extract.py"))
ex = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ex)

rng = np.random.default_rng(11)
ts = []
t = 1649000000
for i in range(3000):
    t += 10 if rng.random() > 0.02 else (int(rng.integers(61, 300)) if rng.random() > 0.1 else 10001 + int(rng.integers(0, 50)))
    ts.append(t)
ts = np.array(ts, dtype="int64")
wide = np.column_stack([np.round(21 + rng.normal(0, 0.3, len(ts)), 1), np.round(40 + rng.normal(0, 1, len(ts)), 2),
                        np.round(101000 + rng.normal(0, 30, len(ts))), np.round(20000 + rng.normal(0, 500, len(ts))),
                        np.where(rng.random(len(ts)) < 0.5, 1.0, np.round(300 + rng.normal(0, 20, len(ts))))]).astype(float)
wide[200:260, 0] = 22.5                    # 非底值游程 / a non-floor run
wide[400:900, 4] = 1.0                     # 底值游程 / a floor run
wide[1200:1300, 3] = 500.0                 # 气体饱和值（底值）/ gas floor value
wide[rng.random(wide.shape) < 0.01] = np.nan
specs = [dict(device="E", ch=0, start=int(ts[1500]), end=int(ts[1500]) + 600, type="spike", mag=9.12),
         dict(device="E", ch=0, start=int(ts[1600]), end=int(ts[1600]) + 1800, type="step", mag=4.56),
         dict(device="E", ch=3, start=int(ts[1700]), end=int(ts[1700]) + 3600, type="ramp", mag=10994.8905),
         dict(device="E", ch=1, start=int(ts[2000]), end=int(ts[2000]) + 2400, type="stuck", mag=0.0)]
thr = [30, 8, 40, 400, 200]
floors = [set(), set(), set(), {500.0}, {1.0, 2.0}]
inj = ex.apply_injections(ts, wide, specs)
stuck = ex.guard_flags(ts, inj, thr, floors)
cold = np.concatenate([[0], (np.diff(ts) > ex.COLD_GAP_SEC).astype(int)])
out = os.path.join(ROOT, "src", "test", "resources", "m5a")
os.makedirs(out, exist_ok=True)
with open(os.path.join(out, "port_rounds.csv"), "w") as fh:
    fh.write("ts,x0,x1,x2,x3,x4,cold,i0,i1,i2,i3,i4,stuck\n")
    for i in range(len(ts)):
        f = lambda v: "nan" if v != v else repr(float(v))
        fh.write(",".join([str(ts[i])] + [f(v) for v in wide[i]] + [str(cold[i])] + [f(v) for v in inj[i]] + [str(int(stuck[i]))]) + "\n")
with open(os.path.join(out, "port_spec.txt"), "w") as fh:
    fh.write(";".join("E:%s:%d:%d:%s:%r" % (ex.CHANNELS[s["ch"]], s["start"], s["end"] - s["start"], s["type"], s["mag"]) for s in specs) + "\n")
    fh.write("E=" + "/".join(map(str, thr)) + "\n")
    fh.write("E.Gas=500,E.Light=1/2\n")
print("rounds %d, stuck %d, cold %d" % (len(ts), stuck.sum(), cold.sum()))
