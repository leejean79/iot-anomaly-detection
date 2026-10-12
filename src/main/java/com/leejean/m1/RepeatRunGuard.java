package com.leejean.m1;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * 重复值游程守卫（开发决策第十条，2026-10-09 裁决第二节第 1 条）：逐设备逐检测通道统计「连续完全相同读数」的
 * 游程长度（以轮计），游程达到该设备该通道的阈值即在轮上打标，并由监测快照计数。
 * Repeated-value run-length guard (development decision 10): per device and detection channel, count the run of
 * consecutive identical raw readings (in rounds); once the run reaches the threshold, flag the round and let the
 * monitoring snapshot count it.
 *
 * <p>为什么需要它 / why：注入运行证明，冻结在合理值上的读数对点通道与上下文通道按构造不可见（冻结值本身是正常
 * 值），传感器卡死只能在数据接入层按「读数不再变化」识别。阈值取探索性分析中该设备该通道自然最长游程的两倍以上
 * （`eda/repeat_run_lengths.py` 给出建议值）。
 * The injection run showed that readings frozen at a plausible value are invisible to both detection channels by
 * construction, so a stuck sensor can only be recognised at ingestion by "the reading stopped changing".
 *
 * <p>计数规则 / counting rules：
 * <ul>
 *   <li>比较的是原始值 x（解析后的 double，逐位相等），不是标准化后的值。 Compares raw values, bit-exact.</li>
 *   <li>本轮该通道缺失时跳过，既不延长也不打断游程。 A missing channel neither extends nor breaks the run.</li>
 *   <li>设备冷启动（缺席超过缓存深度后返场）时清零全部游程。 A cold start resets all runs.</li>
 *   <li>阈值为 0 表示该通道不设守卫。 A threshold of 0 disables the guard for that channel.</li>
 * </ul>
 * 守卫只打标与计数，不改变下游的检测行为；标记供数据层漂移监测与聚合模块使用。
 * The guard only flags and counts; it does not change downstream detection.
 */
public final class RepeatRunGuard {

    private RepeatRunGuard() { }

    /**
     * 解析阈值表 / parse the threshold table。格式：{@code A=120/90/0/300/0,B=...}，每台设备五个整数，按
     * Temperature/Humidity/Pressure/Gas/Light 的固定顺序，用「/」分隔（不用分号，免得被启动脚本截断）。
     * 空串返回空表（守卫关闭）。Format: five integers per device in the fixed channel order, separated by "/".
     */
    public static Map<String, int[]> parse(String spec) {
        if (spec == null || spec.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, int[]> map = new HashMap<>();
        for (String item : spec.split(",")) {
            String s = item.trim();
            if (s.isEmpty()) {
                continue;
            }
            int eq = s.indexOf('=');
            if (eq <= 0 || eq == s.length() - 1) {
                throw new IllegalArgumentException("--stuck-run-thresholds 项格式应为 设备=t1/t2/t3/t4/t5: " + s);
            }
            String[] parts = s.substring(eq + 1).split("/");
            if (parts.length != Channels.N_DET) {
                throw new IllegalArgumentException("--stuck-run-thresholds 每台设备须给 " + Channels.N_DET
                        + " 个阈值（Temperature/Humidity/Pressure/Gas/Light）: " + s);
            }
            int[] thr = new int[Channels.N_DET];
            for (int i = 0; i < Channels.N_DET; i++) {
                thr[i] = Integer.parseInt(parts[i].trim());
                if (thr[i] < 0) {
                    throw new IllegalArgumentException("--stuck-run-thresholds 阈值不得为负: " + s);
                }
            }
            if (map.put(s.substring(0, eq).trim(), thr) != null) {
                throw new IllegalArgumentException("--stuck-run-thresholds 设备重复: " + s);
            }
        }
        return map;
    }

    /** 启动横幅用的描述 / description for the startup banner. */
    public static String describe(Map<String, int[]> thresholds) {
        if (thresholds.isEmpty()) {
            return "OFF (no --stuck-run-thresholds)";
        }
        StringBuilder sb = new StringBuilder("ON  rounds T/H/P/G/L:");
        for (Map.Entry<String, int[]> e : new TreeMap<>(thresholds).entrySet()) {
            int[] t = e.getValue();
            sb.append(' ').append(e.getKey()).append('=')
              .append(t[0]).append('/').append(t[1]).append('/').append(t[2]).append('/').append(t[3]).append('/').append(t[4]);
        }
        return sb.toString();
    }

    /**
     * 用本轮更新游程并在轮上写出卡死掩码 / update the runs with this round and write its stuck mask。
     *
     * @return 本轮游程刚好达到阈值的通道数（即新出现的卡死游程数）/ channels whose run reached the threshold now
     */
    public static int update(RunState s, DeviceRound round, int[] thr) {
        if (round.isColdStart()) {
            s.reset();
        }
        double[] x = round.getX();
        boolean[] missing = round.getMissingMask();
        boolean[] stuck = new boolean[Channels.N_DET];
        int crossed = 0;
        for (int c = 0; c < Channels.N_DET; c++) {
            if (missing != null && missing[c]) {
                continue;
            }
            if (s.seen[c] && Double.compare(x[c], s.last[c]) == 0) {
                s.run[c]++;
            } else {
                s.last[c] = x[c];
                s.run[c] = 1;
                s.seen[c] = true;
            }
            if (thr[c] > 0 && s.run[c] >= thr[c]) {
                stuck[c] = true;
                if (s.run[c] == thr[c]) {
                    crossed++;
                }
            }
        }
        round.setStuckMask(stuck);
        return crossed;
    }

    /**
     * 逐设备游程状态（Flink POJO：public 字段 + 无参构造）/ per-device run state (Flink POJO).
     */
    public static class RunState {
        public double[] last = new double[Channels.N_DET];
        public int[] run = new int[Channels.N_DET];
        public boolean[] seen = new boolean[Channels.N_DET];

        public RunState() { }

        void reset() {
            for (int c = 0; c < Channels.N_DET; c++) {
                run[c] = 0;
                seen[c] = false;
            }
        }
    }
}
