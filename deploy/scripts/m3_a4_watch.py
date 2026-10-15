#!/usr/bin/env python3
"""
m3_a4_watch.py —— 补遗四（把 M3 训练移出任务线程）的运行期采样：逐子任务记录上下文异常检测算子的输入水位线、
输出水位线、训练缓冲占用、输出水位线被保持的时长与缓冲溢出计数，写成 CSV，供步骤 C 第 2 件事（水位线）与
全量重放 A4-3（缓冲与水位线指标的时间曲线）使用。
Samples, per subtask of the M3 operator, the input and output watermarks, training-buffer size, watermark-hold
duration and buffer-overflow count, for addendum 4 step C item 2 and full-replay item A4-3.

---------------------------- 脚本交付五要素 ----------------------------
1. 执行环境 / Environment: 通常不直接调用，而由 `syn-m3-watch.sh start` 放到 master 的 tmux 会话里运行
   （环境变量 SYN_ON_MASTER=1，直接 curl），本地终端可以关闭。也可在本地 Mac 仓库根目录直接运行（经 ssh fa-master）。
   两种方式都需要 python3 与 deploy/.env 中的 NODE_MASTER_IP。
2. 调用命令 / Invocation:
     bash deploy/scripts/syn-m3-watch.sh start --jid <JobID> --since <UTC 时刻>   # 推荐：随另两个监测一起在 master 上运行
     python3 deploy/scripts/m3_a4_watch.py --jid <JobID> --out docs/m3_a4/stepc_t2/wm_buffer.csv   # 本地前台运行
     可选：--interval 10（采样间隔秒，默认 10）；--duration 0（采样总时长秒，0 表示直到作业结束）。
3. 前置条件 / Preconditions: 作业已提交且为 RUNNING，作业里有名为 M3-LSTM-AE 的算子（补遗四之后的 jar）。
4. 期望产出 / Expected output: 每次采样在终端打印一行（各子任务的缓冲轮数、保持秒数、输入与输出水位线的
   事件时间），并向 --out 追加八行（每个子任务一行）。作业取消或结束后自行退出。
5. 失败兜底 / Failure fallback: 退出码 2 表示参数缺失或找不到作业；连续 20 次读不到 REST 接口（约 3 分钟）
   时退出码 3，请检查 jobmanager 容器。指标名一时取不到（作业刚启动）时只打印提示、继续等待。
"""
import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import time

OP = "M3-LSTM-AE"
WANT = re.compile(r"^(\d+)\.(?:" + re.escape(OP) + r"\.)?(currentInputWatermark|currentOutputWatermark|"
                  r"m3_train_buffer_size|m3_watermark_hold_sec|m3_train_buffer_overflow)$")
COLS = ["currentInputWatermark", "currentOutputWatermark", "m3_train_buffer_size",
        "m3_watermark_hold_sec", "m3_train_buffer_overflow"]


def env_value(name):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = re.match(r"^\s*%s=\"?([^\"\n]*)\"?" % name, line)
            if m:
                return m.group(1)
    return ""


def rest(base, path):
    # 在 master 上运行（由 syn-m3-watch.sh 放进 tmux）时直接 curl，在本地 Mac 上经 ssh。
    # On master (launched by syn-m3-watch.sh in tmux) curl directly; on the Mac go through ssh.
    cmd = "curl -s --max-time 20 '%s%s'" % (base, path)
    argv = ["bash", "-c", cmd] if os.environ.get("SYN_ON_MASTER") == "1" else ["ssh", "fa-master", cmd]
    try:
        out = subprocess.run(argv, capture_output=True, text=True, timeout=60).stdout
        return json.loads(out) if out.strip() else None
    except (subprocess.TimeoutExpired, ValueError):
        return None


def fmt_wm(v):
    try:
        ms = int(float(v))
    except (TypeError, ValueError):
        return ""
    if ms <= 0 or ms >= 2 ** 62:
        return ""
    return datetime.datetime.utcfromtimestamp(ms / 1000).strftime("%m-%d %H:%M:%S")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jid", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval", type=int, default=10)
    ap.add_argument("--duration", type=int, default=0)
    a = ap.parse_args()
    ip = env_value("NODE_MASTER_IP")
    if not ip:
        print("ERROR: deploy/.env 中没有 NODE_MASTER_IP", file=sys.stderr)
        return 2
    base = "http://%s:8081" % ip
    job = rest(base, "/jobs/%s" % a.jid)
    if not job or "vertices" not in job:
        print("ERROR: 找不到作业 %s" % a.jid, file=sys.stderr)
        return 2
    vid = next((v["id"] for v in job["vertices"] if OP in v.get("name", "")), None)
    if not vid:
        print("ERROR: 作业里没有名为 %s 的算子（jar 是否为补遗四之后的版本？）" % OP, file=sys.stderr)
        return 2
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    new = not os.path.exists(a.out)
    f = open(a.out, "a", encoding="utf-8")
    if new:
        f.write("wall_clock,subtask,input_wm_ms,output_wm_ms,buffer_rounds,hold_sec,overflow\n")
    print("[a4-watch] 作业 %s，算子 %s，每 %ds 采样 → %s（Ctrl+C 结束）" % (a.jid, vid, a.interval, a.out))
    ids, misses, start, polls = [], 0, time.time(), 0
    expected = len(COLS) * next(v.get("parallelism", 8) for v in job["vertices"] if v["id"] == vid)
    while True:
        if a.duration and time.time() - start >= a.duration:
            return 0
        # 指标名单在作业刚启动时可能只出现一部分子任务（REST 指标缓存是逐个任务管理器异步更新的），所以名单不全时
        # 每次都重新获取，齐全后每 30 次采样刷新一次。/ refetch the id list until all subtasks appear, then every 30 polls.
        polls += 1
        if len(ids) < expected or polls % 30 == 0:
            avail = rest(base, "/jobs/%s/vertices/%s/metrics" % (a.jid, vid)) or []
            ids = [m["id"] for m in avail if WANT.match(m.get("id", ""))] or ids
            if not ids:
                print("[a4-watch] 指标尚未出现，%ds 后重试" % a.interval)
        vals = {}
        for i in range(0, len(ids), 40):
            got = rest(base, "/jobs/%s/vertices/%s/metrics?get=%s" % (a.jid, vid, ",".join(ids[i:i + 40])))
            for m in got or []:
                k = WANT.match(m["id"])
                if k:
                    sub, name = int(k.group(1)), k.group(2)
                    # 任务级与算子级的同名水位线以算子级为准 / operator-level wins over task-level
                    if name not in vals.setdefault(sub, {}) or ("." + OP + ".") in m["id"]:
                        vals[sub][name] = m.get("value", "")
        state = (rest(base, "/jobs/%s" % a.jid) or {}).get("state", "UNREACHABLE")
        misses = misses + 1 if state == "UNREACHABLE" else 0
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        parts = []
        for sub in sorted(vals):
            v = vals[sub]
            f.write(",".join([now, str(sub)] + [str(v.get(c, "")) for c in COLS]) + "\n")
            parts.append("%d:缓冲%s 保持%ss 入%s 出%s" % (
                sub, v.get("m3_train_buffer_size", "-"), str(v.get("m3_watermark_hold_sec", "-"))[:5],
                fmt_wm(v.get("currentInputWatermark")), fmt_wm(v.get("currentOutputWatermark"))))
        f.flush()
        print("[%s] %s  %s" % (now[11:], state, " | ".join(parts)))
        if state in ("CANCELED", "FINISHED", "FAILED"):
            return 0
        if misses >= 20:
            print("ERROR: 连续 20 次读不到作业状态", file=sys.stderr)
            return 3
        time.sleep(a.interval)


if __name__ == "__main__":
    sys.exit(main())
