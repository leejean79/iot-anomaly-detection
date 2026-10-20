#!/usr/bin/env python3
# ============================================================================
# m5b/drift_json_to_csv.py
# 把在线算子写到 synergia-drift 的事件（每行一条 JSON，字段为 DriftEvent 的 public 字段）转成离线事件清单的 CSV 格式，供
# compare_events.py 与 Java 离线运行器的输出逐条比较（验收项 V-M5b-3）。主题只有一个分区，同一设备的事件保持发出顺序；
# 输出按设备稳定排序，与离线运行器按设备、时间升序的输出对齐。
# Convert online drift events (JSON lines) to the offline CSV format; stable-sorted by device.
#
# ---------------------------- 脚本交付五要素 -------------------------------
# 1. 执行环境 / Environment: master 或本地，仓库根目录；python3，只用标准库。
# 2. 调用命令 / Invocation: python3 m5b/drift_json_to_csv.py <drift.jsonl> <out.csv>
# 3. 前置条件 / Preconditions: drift.jsonl 由 kafka-console-consumer 从 synergia-drift 转储，每行一条 JSON。
# 4. 期望产出 / Expected output: out.csv（DriftEvent.CSV_HEADER 列），终端打印事件数与各级别条数。
# 5. 失败兜底 / Failure fallback: 退出码 2 表示输入缺失；无法解析的行跳过并计数。
# ============================================================================
import collections
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import optwin_offline as ow   # noqa: E402

FIELDS = {"device": "device", "ts": "ts", "level": "level", "test": "test", "window_len": "windowLen",
          "hist_len": "histLen", "new_len": "newLen", "hist_mean": "histMean", "hist_sd": "histSd",
          "new_mean": "newMean", "new_sd": "newSd", "hist_median": "histMedian", "magnitude": "magnitude",
          "during_check": "duringCheck", "candidate_ts": "candidateTs", "reference": "reference",
          "threshold": "threshold", "last_median": "lastMedian", "checked": "checkedElements"}


def main() -> int:
    if len(sys.argv) != 3 or not os.path.exists(sys.argv[1]):
        print("用法：drift_json_to_csv.py <drift.jsonl> <out.csv>（输入须存在）", file=sys.stderr)
        return 2
    rows, bad = [], 0
    for line in open(sys.argv[1], encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except ValueError:
            bad += 1
    rows.sort(key=lambda r: r["device"])                     # 稳定排序，保留设备内的发出顺序 / stable
    events = []
    for r in rows:
        e = {k: r[v] for k, v in FIELDS.items()}
        e["during_check"] = int(bool(e["during_check"]))
        events.append(e)
    ow.write_events(events, sys.argv[2])
    print("事件 %d 条（%s）；无法解析 %d 行" % (len(events), dict(collections.Counter(e["level"] for e in events)), bad))
    return 0


if __name__ == "__main__":
    sys.exit(main())
