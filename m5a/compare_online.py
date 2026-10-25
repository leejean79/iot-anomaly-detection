#!/usr/bin/env python3
# ============================================================================
# m5a/compare_online.py
# 验收项 V-M5a-3（短重放等值）的比较：在线数据层输出（由 M5aOfflineRunner 的转换模式转成 CSV）与 Java 离线运行器的输出逐行比较。
# 两边都由同一个 Java 格式化函数写出，所以按文本逐行比较。离线以最后一条轮的时刻为最终事件时间，在线的最终水位线停在别处，
# 所以只比较截止时刻（--cut，世界协调时秒）之前的部分：事件按其时刻，日记录按当地日结束的时刻；截掉的条数两边分别报告。
# V-M5a-3 comparison of the online data-layer output with the Java offline runner, up to a cut-off time.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: 任意有 python3 的机器（集群 master 上由 syn-m5a-verify.sh 调用）；只用标准库。
# 2. 调用命令 / Invocation:
#      python3 m5a/compare_online.py --online-events on_ev.csv --offline-events off_ev.csv \
#          --online-records on_rec.csv --offline-records off_rec.csv --cut 1648684800
# 3. 前置条件 / Preconditions: 四份 CSV 都由 M5aOfflineRunner 写出（DriftEvent.DATA_CSV_HEADER 与 RECORD_HEADER 表头）。
# 4. 期望产出 / Expected output: 打印两边的条数、截掉的条数、按级别的事件数与第一处不同；截止之前完全相同且非空时退出码 0。
# 5. 失败兜底 / Failure fallback: 退出码 1 表示不同或为空；2 表示文件缺失。
# ============================================================================
import argparse
import collections
import csv
import os
import sys
from datetime import date, datetime, timedelta, timezone


def bst_bounds(year):
    """英国夏令时的世界协调时起止：三月与十月最后一个星期日 01:00 / UK summer time bounds in UTC."""
    def last_sunday(month):
        d = date(year, month + 1, 1) - timedelta(days=1)
        return d - timedelta(days=(d.weekday() + 1) % 7)
    a, b = last_sunday(3), last_sunday(10)
    return (datetime(a.year, a.month, a.day, 1, tzinfo=timezone.utc).timestamp(),
            datetime(b.year, b.month, b.day, 1, tzinfo=timezone.utc).timestamp())


def day_end(day):
    """当地日（布里斯托）结束的世界协调时秒数 / UTC epoch second at which the local day ends."""
    d = datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)
    t = d.replace(tzinfo=timezone.utc).timestamp()
    s, e = bst_bounds(d.year)
    return int(t - 3600) if s <= t - 3600 < e else int(t)


def read(path):
    with open(path, encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    return rows[0], rows[1:]


def split(rows, key, cut):
    keep = [r for r in rows if key(r) <= cut]
    return keep, len(rows) - len(keep)


def compare(name, a, b):
    for i, (x, y) in enumerate(zip(a, b)):
        if x != y:
            print("%s：第 %d 行不同\n  在线：%s\n  离线：%s" % (name, i, ",".join(x), ",".join(y)))
            return False
    if len(a) != len(b):
        print("%s：前 %d 行相同，但条数不同（在线 %d，离线 %d）" % (name, min(len(a), len(b)), len(a), len(b)))
        return False
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    for k in ("online-events", "offline-events", "online-records", "offline-records"):
        ap.add_argument("--" + k, required=True)
    ap.add_argument("--cut", type=int, required=True)
    a = ap.parse_args()
    paths = [a.online_events, a.offline_events, a.online_records, a.offline_records]
    if not all(os.path.exists(p) for p in paths):
        print("文件缺失：%s" % [p for p in paths if not os.path.exists(p)], file=sys.stderr)
        return 2
    _, on_ev = read(a.online_events)
    _, off_ev = read(a.offline_events)
    _, on_rec = read(a.online_records)
    _, off_rec = read(a.offline_records)
    ev_key = lambda r: int(r[1])
    rec_key = lambda r: day_end(r[1])
    on_ev_k, on_ev_cut = split(on_ev, ev_key, a.cut)
    off_ev_k, off_ev_cut = split(off_ev, ev_key, a.cut)
    on_rec_k, on_rec_cut = split(on_rec, rec_key, a.cut)
    off_rec_k, off_rec_cut = split(off_rec, rec_key, a.cut)
    print("截止时刻：%d（%s UTC）" % (a.cut, datetime.fromtimestamp(a.cut, timezone.utc).strftime("%Y-%m-%d %H:%M")))
    print("事件：在线 %d 条（截掉 %d），离线 %d 条（截掉 %d）" % (len(on_ev_k), on_ev_cut, len(off_ev_k), off_ev_cut))
    print("日记录：在线 %d 条（截掉 %d），离线 %d 条（截掉 %d）" % (len(on_rec_k), on_rec_cut, len(off_rec_k), off_rec_cut))
    print("事件按级别（截止之前，离线）：%s" % dict(collections.Counter(r[2] for r in off_ev_k)))
    print("可判的日记录（截止之前，离线）：%d 条" % sum(r[3] == "1" for r in off_rec_k))
    ok_ev = compare("事件", on_ev_k, off_ev_k)
    ok_rec = compare("日记录", on_rec_k, off_rec_k)
    nonempty = len(off_rec_k) > 0 and len(off_ev_k) > 0
    if not nonempty:
        print("截止之前的离线事件或日记录为空，等值核对没有意义。")
    if ok_ev and ok_rec and nonempty:
        print("逐行相同。")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
