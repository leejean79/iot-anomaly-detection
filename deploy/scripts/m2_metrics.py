#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
m2_metrics.py —— 通过 Flink REST API 自动拉取 M2/M1 的自定义计数器，并做 V-M2-2 的计数器对账。
Fetch M2/M1 custom counters via the Flink REST API and run the V-M2-2 counter reconciliation.

本地运行、直连 Flink UI 的 REST（与浏览器访问 http://<master>:8081 同一入口），无需登进集群。
Runs locally against the same REST endpoint the Flink UI uses; no cluster login needed.

================================ 脚本交付五要素 / Five delivery elements ================================
1. 执行环境 / Environment: 本地（有 python3 标准库即可）；能访问 Flink UI 的 http://<master>:8081。
2. 调用命令 / Invocation:
     python3 deploy/scripts/m2_metrics.py --flink-url http://<master_public_ip>:8081
     # 或由 syn-m2-metrics.sh 读 .env 的 NODE_MASTER_PUBLIC_IP 自动拼 URL
     python3 deploy/scripts/m2_metrics.py --flink-url http://1.2.3.4:8081 --job <jobid>
3. 前置条件 / Preconditions: M2Job 正在运行（RUNNING）；REST 端口可达。
4. 期望产出 / Expected output: 各计数器累计值（跨 subtask 求和）+ 对账行
     admitted ?= rounds_total − warmup_bypass − missing_bypass（应闭合）
     交叉核对 m1_scaler_warmup_rounds == m2_gate_warmup_bypass（两处都数 warmup）。
5. 失败兜底 / Failure fallback: 找不到 RUNNING 的 M2Job → 提示先提交；REST 不可达 → 提示改用 Flink UI
     手动读（Metrics 标签按名字加）。指标名按后缀匹配，兼容不同 scope 前缀。
"""

import argparse
import json
import sys
import urllib.request

# 我们关心的计数器（按后缀匹配，兼容 <scope>.<name> 前缀）
GATE = ["m2_gate_admitted", "m2_gate_warmup_bypass", "m2_gate_missing_bypass",
        "m2_gate_censored_entered", "m2_gate_coldstart_clear", "m2_gate_late_drop"]
M2 = ["m2_outliers_total", "m2_points_total", "m2_mc_points_total", "m2_windows_total",
      "m2_state_cold_clears"]
M1 = ["m1_assembler_rounds_total", "m1_assembler_incomplete_rounds", "m1_assembler_dup_keys",
      "m1_scaler_warmup_rounds", "m1_parser_censored_light", "m1_parser_rssi_sentinel",
      "m1_parser_unknown_sensor"]
WANTED = GATE + M2 + M1


def get(url):
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read().decode("utf-8"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--flink-url", required=True, help="Flink REST base, e.g. http://1.2.3.4:8081")
    ap.add_argument("--job", default=None, help="Job id（默认自动找 RUNNING 的 M2Job）")
    ap.add_argument("--debug", action="store_true", help="打印各 vertex 可用的 m1_/m2_ 指标 ID 原始格式")
    args = ap.parse_args()
    base = args.flink_url.rstrip("/")

    # 1) 找 job
    jid = args.job
    if not jid:
        overview = get(base + "/jobs/overview")
        running = [j for j in overview.get("jobs", []) if j.get("state") == "RUNNING"]
        m2 = [j for j in running if "M2Job" in j.get("name", "")]
        pick = m2 or running
        if not pick:
            print("ERROR: 没有 RUNNING 的作业。先提交 M2Job：bash deploy/scripts/syn-submit-m2.sh", file=sys.stderr)
            sys.exit(2)
        jid = pick[0]["jid"]
        print("[job] %s  (%s)" % (jid, pick[0].get("name", "")))

    # 2) 遍历所有 vertex，按后缀匹配我们关心的计数器，跨 subtask 求和
    detail = get(base + "/jobs/" + jid)
    totals = {name: None for name in WANTED}
    for v in detail.get("vertices", []):
        vid = v["id"]
        try:
            avail = get("%s/jobs/%s/vertices/%s/metrics" % (base, jid, vid))
        except Exception:
            continue
        ids = [m["id"] for m in avail]
        if args.debug:
            hits = [mid for mid in ids if "m1_" in mid or "m2_" in mid]
            print("[debug] vertex '%s' (%s): 指标 %d 个，含 m1_/m2_ 的 %d 个"
                  % (v.get("name", "")[:40], vid, len(ids), len(hits)))
            for mid in hits:
                print("        " + mid)
        # 真实 ID 格式为 <subtask号>.<算子名>.<指标名>（如 0.M2Gate.m2_gate_admitted）。
        # 每个 id 是**单个 subtask** 的值，取末段（最后一个点后）与计数器名精确相等即匹配；
        # 逐 id 取值、按计数器名跨 subtask 求和。
        wanted_set = set(WANTED)
        id_to_name = {}
        for mid in ids:
            seg = mid.rsplit(".", 1)[-1]     # 末段 = 指标名（计数器名不含点）
            if seg in wanted_set:
                id_to_name[mid] = seg
        if not id_to_name:
            continue
        # 分批查询各 id 的值（vertex 级 metrics 端点，逐 id 返回 value）/ fetch per-id values
        matched_ids = list(id_to_name.keys())
        for i in range(0, len(matched_ids), 40):
            batch = matched_ids[i:i + 40]
            try:
                vals = get("%s/jobs/%s/vertices/%s/metrics?get=%s"
                           % (base, jid, vid, ",".join(batch)))
            except Exception:
                continue
            for m in vals:
                name = id_to_name.get(m.get("id"))
                if name is None:
                    continue
                raw = m.get("value")
                if raw is None:
                    continue
                totals[name] = (totals[name] or 0) + int(float(raw))

    # 3) 打印
    def show(title, names):
        print("== %s ==" % title)
        for n in names:
            v = totals.get(n)
            print("  %-28s %s" % (n, "n/a" if v is None else v))

    print("=" * 44)
    show("三道闸 / gates", GATE)
    show("M2", M2)
    show("M1（对账参照）", M1)
    print("=" * 44)

    # 4) 对账
    def g(n):
        return totals.get(n)

    adm, rt, wb, mb = g("m2_gate_admitted"), g("m1_assembler_rounds_total"), \
        g("m2_gate_warmup_bypass"), g("m2_gate_missing_bypass")
    if None not in (adm, rt, wb, mb):
        expect = rt - wb - mb
        ok = "闭合 OK" if adm == expect else ("差 %d ⚠" % (adm - expect))
        print("对账 / reconcile: admitted(%d) ?= rounds(%d) − warmup(%d) − missing(%d) = %d  → %s"
              % (adm, rt, wb, mb, expect, ok))
    else:
        print("对账: 计数器不全（可能作业未消费到数据或指标名不匹配）——可在 Flink UI 手动核对。")

    sw, wb2 = g("m1_scaler_warmup_rounds"), g("m2_gate_warmup_bypass")
    if None not in (sw, wb2):
        print("交叉核对 warmup: scaler(%d) vs gate(%d) → %s"
              % (sw, wb2, "一致" if sw == wb2 else "差 %d（可能因算子链/延迟，稳态应相等）" % (sw - wb2)))

    if all(totals.get(n) in (None, 0) for n in ("m2_gate_admitted",)):
        print("\n提示：m2_gate_admitted=0/空 → 很可能整段仍在预热期（8640 轮/设备≈1 天）。"
              "重放≥1.x 天，或临时 syn-submit-m2.sh --extra '--warmup-rounds 600' 做功能验证。")


if __name__ == "__main__":
    main()
