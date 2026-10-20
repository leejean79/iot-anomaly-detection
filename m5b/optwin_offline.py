#!/usr/bin/env python3
# ============================================================================
# m5b/optwin_offline.py
# 最优窗口漂移检测器与持续性核对的 Python 参考实现（M5b 交接文档第三节「离线工具」；论文 Tosi 第四章算法 2；裁决补充 0.93 版）。
# 与 Java 的 com.leejean.m5.OptWinDetector / DriftGate 逐步相同（同一张常量表、同一浮点运算次序），两边事件因此逐位一致。
# Python reference of the OPTWIN detector and the persistence gate; step-for-step identical to the Java classes.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 本地 Mac 或开发机，仓库根目录；python3，只用标准库。
# 2. 调用命令 / Invocation:
#      python3 m5b/optwin_offline.py --streams docs/m5b/streams_10min.csv --time-col window_end --value-col main_score \
#          --table docs/m5b/tables/thesis_0.99_0.5_2016.csv --k 36 --out /tmp/events_py.csv
#    小时粒度：--streams docs/m5b/streams_hourly.csv --time-col hour_start --value-col median_main_score，表用窗上限 336 的那张。
# 3. 前置条件 / Preconditions: 常量表由 Java 生成（OptWinOfflineRunner 的 --table-out，或 OptWinTables 的命令行）；
#    元素流由 m5b/prepare_streams.py 生成，按设备、时间升序。
# 4. 期望产出 / Expected output: --out 处的事件清单 CSV（与 Java 的 DriftEvent.CSV_HEADER 列相同），终端打印事件数。
# 5. 失败兜底 / Failure fallback: 退出码 2 表示输入缺失。
# 也可作为模块导入：Detector、Gate、load_table、run_stream。/ Also importable as a module.
# ============================================================================
import argparse
import csv
import math
import os
import sys

W_MIN = 30
BETA = 1e-5
MEDIAN_SPAN = 6
HEADER = ("device,ts,level,test,window_len,hist_len,new_len,hist_mean,hist_sd,new_mean,new_sd,hist_median,"
          "magnitude,during_check,candidate_ts,reference,threshold,last_median,checked")


class Table:
    def __init__(self, path):
        with open(path, encoding="utf-8") as fh:
            meta = dict(kv.split("=") for kv in fh.readline().lstrip("# ").split())
            self.confidence, self.robustness = float(meta["confidence"]), float(meta["robustness"])
            self.wmax = int(meta["wmax"])
            self.cut = [0] * (self.wmax + 1)
            self.f_crit = [0.0] * (self.wmax + 1)
            self.t_crit = [0.0] * (self.wmax + 1)
            for r in csv.DictReader(fh):
                w = int(r["w"])
                self.cut[w], self.f_crit[w], self.t_crit[w] = int(r["cut"]), float(r["f_crit"]), float(r["t_crit"])


def load_table(path):
    return Table(path)


def _variance(s, q, k):
    if k < 2:
        return 0.0
    v = (q - s * s / k) / (k - 1)
    return v if v > 0 else 0.0


def _median(vals):
    s = sorted(vals)
    n = len(s)
    return s[n // 2] if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) / 2.0


class Detector:
    """与 OptWinDetector 逐步相同 / mirrors OptWinDetector."""

    def __init__(self, wmax):
        self.wmax = wmax
        self.buf = [0.0] * (wmax + 1)
        self.f_without_rise = 0
        self.clear()

    def clear(self):
        self.head = self.n = self.h = 0
        self.sum_h = self.sq_h = self.sum_n = self.sq_n = 0.0

    def _get(self, i):
        return self.buf[(self.head + i) % len(self.buf)]

    def update(self, x, t):
        cap = len(self.buf)
        self.buf[(self.head + self.n) % cap] = x
        self.n += 1
        self.sum_n += x
        self.sq_n += x * x
        if self.n > self.wmax:
            y = self.buf[self.head]
            self.head = (self.head + 1) % cap
            self.n -= 1
            if self.h > 0:
                self.h -= 1
                self.sum_h -= y
                self.sq_h -= y * y
            else:
                self.sum_n -= y
                self.sq_n -= y * y
        if self.n < W_MIN:
            return None
        c = t.cut[self.n]
        while self.h < c:
            e = self._get(self.h)
            self.sum_n -= e
            self.sq_n -= e * e
            self.sum_h += e
            self.sq_h += e * e
            self.h += 1
        while self.h > c:
            self.h -= 1
            e = self._get(self.h)
            self.sum_h -= e
            self.sq_h -= e * e
            self.sum_n += e
            self.sq_n += e * e
        h, m = self.h, self.n - self.h
        mean_h, mean_n = self.sum_h / h, self.sum_n / m
        var_h, var_n = _variance(self.sum_h, self.sq_h, h), _variance(self.sum_n, self.sq_n, m)
        sd_h, sd_n = math.sqrt(var_h), math.sqrt(var_n)
        a, b = sd_n + BETA, sd_h + BETA
        f_value = (a * a) / (b * b)
        denom = math.sqrt(var_h / h + var_n / m)
        t_value = (mean_n - mean_h) / denom if denom > 0 else 0.0
        fired = None
        if f_value > t.f_crit[self.n]:
            if mean_n > mean_h:
                fired = "F"
            else:
                self.f_without_rise += 1
        if fired is None and denom > 0 and t_value > t.t_crit[self.n]:
            fired = "T"
        if fired is None:
            return None
        alarm = dict(test=fired, window_len=self.n, hist_len=h, new_len=m, hist_mean=mean_h, hist_sd=sd_h,
                     new_mean=mean_n, new_sd=sd_n, hist_median=_median([self._get(i) for i in range(h)]),
                     magnitude=(mean_n - mean_h) / (sd_h + BETA))
        self.clear()
        return alarm


class Gate:
    """与 DriftGate 逐步相同 / mirrors DriftGate."""

    def __init__(self, k, robustness):
        self.k, self.robustness = k, robustness
        self.last = []
        self.pending = False
        self.seen = 0
        self.reference = self.threshold = 0.0
        self.candidate_ts = 0
        self.origin = None

    def on_element(self, device, ts, x, alarm):
        out = []
        self.last.append(x)
        if len(self.last) > MEDIAN_SPAN:
            self.last.pop(0)
        if self.pending:
            self.seen += 1
            med = _median(self.last)
            if not (med > self.threshold):
                out.append(self._gate_event(device, ts, "REVOKE", med))
                self.pending = False
            elif self.seen == self.k:
                out.append(self._gate_event(device, ts, "CONFIRM", med))
                self.pending = False
        if alarm is not None:
            out.append(_event(device, ts, "CANDIDATE", alarm, during_check=self.pending, candidate_ts=ts))
            if not self.pending:
                self.pending, self.seen = True, 0
                self.reference = alarm["hist_median"]
                self.threshold = self.reference + self.robustness * alarm["hist_sd"]
                self.candidate_ts, self.origin = ts, alarm
        return out

    def _gate_event(self, device, ts, level, med):
        return _event(device, ts, level, self.origin, candidate_ts=self.candidate_ts, reference=self.reference,
                      threshold=self.threshold, last_median=med, checked=self.seen)


def _event(device, ts, level, a, during_check=False, candidate_ts=0, reference=0.0, threshold=0.0,
           last_median=0.0, checked=0):
    e = dict(device=device, ts=ts, level=level, during_check=int(during_check), candidate_ts=candidate_ts,
             reference=reference, threshold=threshold, last_median=last_median, checked=checked)
    e.update({k: a[k] for k in ("test", "window_len", "hist_len", "new_len", "hist_mean", "hist_sd", "new_mean",
                                "new_sd", "hist_median", "magnitude")})
    return e


def run_stream(rows, table, k):
    """rows：按设备、时间升序的 (device, ts, value)；返回事件列表 / events for sorted (device, ts, value) rows."""
    det, gate, events = {}, {}, []
    for dev, ts, x in rows:
        d = det.setdefault(dev, Detector(table.wmax))
        g = gate.setdefault(dev, Gate(k, table.robustness))
        events.extend(g.on_element(dev, ts, x, d.update(x, table)))
    return events


def read_stream(path, time_col, value_col):
    with open(path, encoding="utf-8") as fh:
        return [(r["device"], int(r[time_col]), float(r[value_col])) for r in csv.DictReader(fh)]


def write_events(events, path):
    cols = HEADER.split(",")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(HEADER + "\n")
        for e in events:
            fh.write(",".join(str(e[c]) if not isinstance(e[c], float) else repr(e[c]) for c in cols) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--streams", required=True)
    ap.add_argument("--time-col", required=True)
    ap.add_argument("--value-col", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--k", type=int, required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    for f in (a.streams, a.table):
        if not os.path.exists(f):
            print("ERROR: 缺少 %s" % f, file=sys.stderr)
            return 2
    events = run_stream(read_stream(a.streams, a.time_col, a.value_col), load_table(a.table), a.k)
    write_events(events, a.out)
    print("事件 %d 条 → %s" % (len(events), a.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
