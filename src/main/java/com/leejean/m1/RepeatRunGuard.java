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
 *   <li>相邻两条读数相隔超过 60 秒即断开游程（2026-10-13 裁决第四节）。 A gap over 60 s breaks the run.</li>
 *   <li>设备冷启动（缺席超过缓存深度后返场）时清零全部游程。 A cold start resets all runs.</li>
 *   <li>阈值为 0 表示该通道不设守卫。 A threshold of 0 disables the guard for that channel.</li>
 * </ul>
 * 守卫只打标与计数，不改变下游的检测行为；标记供数据层漂移监测与聚合模块使用。
 * The guard only flags and counts; it does not change downstream detection.
 *
 * <p>底值游程（2026-10-12 裁决第二节）/ floor-value runs：每个通道可配置一个「底值集合」（光照的夜间底值、气体的
 * 饱和值等，由探索性分析得出、设计会话冻结）。读数在底值集合里的游程是底值游程：达到阈值时只写 floorMask 并计数，
 * 不写 stuckMask，因为单台设备无法区分「夜间」与「传感器死在底值」；是否「底值卡死」由聚合模块跨设备交叉核对。
 * 读数不在底值集合里的游程是普通游程，达到阈值即写 stuckMask（疑似卡死）。两类标记都附带游程的起始时刻与长度，
 * 格式一致，便于聚合模块消费。
 * A run on a value in the channel's floor set is a floor-value run: on reaching the threshold it sets floorMask and
 * is counted, never stuckMask; the fleet-level cross-check belongs to the aggregation module.
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

    /**
     * 解析底值集合（逐设备逐通道，2026-10-13 裁决第四节）/ parse the per-device floor-value sets。格式：
     * {@code A.Light=0/1/2,B.Light=5}，键为「设备.检测通道名」，取值用「/」分隔；空串返回空表（没有底值）。
     * 返回 设备 → 按检测通道固定顺序排列的数组。Key "device.channel", values separated by "/".
     */
    public static Map<String, double[][]> parseFloors(String spec) {
        Map<String, double[][]> map = new HashMap<>();
        if (spec == null || spec.trim().isEmpty()) {
            return map;
        }
        for (String item : spec.split(",")) {
            String s = item.trim();
            if (s.isEmpty()) {
                continue;
            }
            int eq = s.indexOf('=');
            int dot = eq > 0 ? s.lastIndexOf('.', eq) : -1;
            int ch = dot > 0 ? Channels.detectionIndex(s.substring(dot + 1, eq).trim()) : -1;
            if (ch < 0 || eq == s.length() - 1) {
                throw new IllegalArgumentException("--stuck-floor-values 项格式应为 设备.检测通道名=v1/v2/...: " + s);
            }
            String[] parts = s.substring(eq + 1).split("/");
            double[] v = new double[parts.length];
            for (int i = 0; i < parts.length; i++) {
                v[i] = Double.parseDouble(parts[i].trim());
            }
            map.computeIfAbsent(s.substring(0, dot).trim(), k -> emptyFloors())[ch] = v;
        }
        return map;
    }

    /** 没有底值的一台设备 / a device without floor values. */
    public static double[][] emptyFloors() {
        return new double[Channels.N_DET][0];
    }

    /** 底值集合的横幅描述 / banner description of the floor sets. */
    public static String describeFloors(Map<String, double[][]> floors) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, double[][]> e : new TreeMap<>(floors).entrySet()) {
            for (int c = 0; c < Channels.N_DET; c++) {
                if (e.getValue()[c].length > 0) {
                    sb.append(sb.length() == 0 ? "" : " ").append(e.getKey()).append('.').append(Channels.DETECTION[c])
                      .append('=').append(java.util.Arrays.toString(e.getValue()[c]));
                }
            }
        }
        return sb.length() == 0 ? "none" : sb.toString();
    }

    private static boolean isFloor(double[] floor, double v) {
        for (double f : floor) {
            if (Double.compare(f, v) == 0) {
                return true;
            }
        }
        return false;
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

    /** 不设底值集合时的更新（全部按普通游程处理）/ update without floor sets. */
    public static int update(RunState s, DeviceRound round, int[] thr) {
        return update(s, round, thr, emptyFloors())[0];
    }

    /** 相邻两条读数相隔超过此秒数即断开游程（探索性分析的缺口定义，2026-10-13 裁决第四节）/ gap that breaks a run. */
    static final long GAP_SEC = 60L;

    /**
     * 用本轮更新游程，在轮上写出卡死掩码、底值掩码与游程信息 / update the runs and write the masks and run info。
     * 达到阈值的通道：读数在底值集合里写 floorMask，否则写 stuckMask；两类都在 guardRunLength / guardRunStartTs
     * 中给出该游程的长度与起始时刻（其余通道为 0）。
     *
     * @return {本轮新出现的普通卡死游程数, 本轮新出现的底值游程数} / {new ordinary stuck runs, new floor runs}
     */
    public static int[] update(RunState s, DeviceRound round, int[] thr, double[][] floors) {
        if (round.isColdStart()) {
            s.reset();
        }
        double[] x = round.getX();
        boolean[] missing = round.getMissingMask();
        boolean[] stuck = new boolean[Channels.N_DET];
        boolean[] floor = new boolean[Channels.N_DET];
        int[] runLen = new int[Channels.N_DET];
        long[] runStart = new long[Channels.N_DET];
        int[] crossed = new int[2];
        for (int c = 0; c < Channels.N_DET; c++) {
            if (missing != null && missing[c]) {
                continue;
            }
            if (s.seen[c] && Double.compare(x[c], s.last[c]) == 0 && round.getTs() - s.lastTs[c] <= GAP_SEC) {
                s.run[c]++;
            } else {
                s.last[c] = x[c];
                s.run[c] = 1;
                s.start[c] = round.getTs();
                s.seen[c] = true;
            }
            s.lastTs[c] = round.getTs();
            if (thr[c] > 0 && s.run[c] >= thr[c]) {
                boolean onFloor = isFloor(floors[c], s.last[c]);
                if (onFloor) {
                    floor[c] = true;
                } else {
                    stuck[c] = true;
                }
                runLen[c] = s.run[c];
                runStart[c] = s.start[c];
                if (s.run[c] == thr[c]) {
                    crossed[onFloor ? 1 : 0]++;
                }
            }
        }
        round.setStuckMask(stuck);
        round.setFloorMask(floor);
        round.setGuardRunLength(runLen);
        round.setGuardRunStartTs(runStart);
        return crossed;
    }

    /**
     * 逐设备游程状态（Flink POJO：public 字段 + 无参构造）/ per-device run state (Flink POJO).
     */
    public static class RunState {
        public double[] last = new double[Channels.N_DET];
        public int[] run = new int[Channels.N_DET];
        public long[] start = new long[Channels.N_DET];     // 当前游程的起始轮时间戳（秒）/ run start ts (s)
        public long[] lastTs = new long[Channels.N_DET];    // 该通道上一条读数的轮时间戳（秒）/ previous reading ts
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
