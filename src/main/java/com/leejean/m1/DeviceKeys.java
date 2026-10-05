package com.leejean.m1;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * 设备代理键：让每台设备落到一个指定的子任务上（2026-10-05 裁决第三节第 3 条，开放问题第十条）。
 * Device surrogate keys: place each device on a chosen subtask (ruling of 2026-10-05, section 3.3).
 *
 * <p>Flink 按「键的哈希 → 键组 → 子任务」分配键，八台设备的原始编号在并行度八时只落到五个子任务上，
 * 同一子任务上的设备只能串行训练，阻塞时间成倍拉长。代理键是形如 {@code 设备号#序号} 的字符串，
 * 序号由 {@link #generate} 搜索得到，使该字符串恰好落到指定子任务。整条按键链路（数据接入、点通道、
 * 上下文通道）统一用代理键分组；输出记录仍带原设备号——用代理键当设备号的算子须经 {@link #deviceOf}
 * 还原（「#」之前的部分）。
 * Flink assigns keys by hash -> key group -> subtask; the eight raw ids land on only five subtasks at
 * parallelism 8. A surrogate key is {@code device#n}, with n searched so the string lands on the chosen
 * subtask. Every device-keyed step uses it; operators that read the key as the device id call
 * {@link #deviceOf}, which drops the "#n" suffix.
 *
 * <p>配置：作业参数 {@code --device-surrogate-keys "A=A#3,B=B#17,..."}（来自 .env 的
 * {@code SYN_DEVICE_SURROGATE_KEYS}）。未配置的设备用原始编号作键，行为与引入代理键之前完全相同。
 * Configuration: {@code --device-surrogate-keys "A=A#3,..."} (from SYN_DEVICE_SURROGATE_KEYS); devices
 * without an entry keep their raw id as the key, exactly as before.
 */
public final class DeviceKeys {

    private DeviceKeys() { }

    /** 代理键中设备号与序号的分隔符 / separator between device id and number. */
    public static final char SEP = '#';

    /** 解析 "A=A#3,B=B#17" / parse the table; empty or null gives an empty map. */
    public static Map<String, String> parse(String spec) {
        Map<String, String> out = new LinkedHashMap<>();
        if (spec == null || spec.trim().isEmpty()) return out;
        for (String part : spec.split(",")) {
            String p = part.trim();
            if (p.isEmpty()) continue;
            int eq = p.indexOf('=');
            if (eq <= 0 || eq == p.length() - 1) {
                throw new IllegalArgumentException("代理键表格式应为 设备=代理键，逗号分隔；无法解析：" + p);
            }
            String device = p.substring(0, eq).trim();
            String key = p.substring(eq + 1).trim();
            if (!deviceOf(key).equals(device)) {
                throw new IllegalArgumentException("代理键必须以「设备号#」开头：" + p);
            }
            out.put(device, key);
        }
        return out;
    }

    /** 设备号 → 分组用的键 / device id -> grouping key. */
    public static String keyOf(Map<String, String> table, String device) {
        String k = table.get(device);
        return k != null ? k : device;
    }

    /** 分组用的键 → 设备号（去掉「#序号」）/ grouping key -> device id. */
    public static String deviceOf(String key) {
        if (key == null) return null;
        int i = key.indexOf(SEP);
        return i < 0 ? key : key.substring(0, i);
    }

    /** 键 → 子任务编号，与 Flink 运行时的分配一致 / key -> subtask index, as the Flink runtime assigns it. */
    public static int subtaskOf(String key, int maxParallelism, int parallelism) {
        return KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, parallelism);
    }

    /** 未显式设置时 Flink 采用的最大并行度 / the max parallelism Flink uses when none is set. */
    public static int effectiveMaxParallelism(int configured, int parallelism) {
        return configured > 0 ? configured : KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
    }

    /** 每台设备当前落到的子任务（按设备号排序）/ the subtask each device lands on, sorted by device id. */
    public static Map<String, Integer> placement(Map<String, String> table, Iterable<String> devices,
                                                 int maxParallelism, int parallelism) {
        Map<String, Integer> out = new TreeMap<>();
        for (String d : devices) out.put(d, subtaskOf(keyOf(table, d), maxParallelism, parallelism));
        return out;
    }

    /**
     * 为按顺序排列的设备生成代理键：第 i 台落到子任务 i（i 取并行度的余数）。
     * Generate surrogate keys: the i-th device lands on subtask i (mod parallelism).
     */
    public static Map<String, String> generate(Iterable<String> devices, int maxParallelism, int parallelism) {
        Map<String, String> out = new LinkedHashMap<>();
        int i = 0;
        for (String d : devices) {
            int target = i++ % parallelism;
            for (int n = 0; ; n++) {
                String k = d + SEP + n;
                if (subtaskOf(k, maxParallelism, parallelism) == target) {
                    out.put(d, k);
                    break;
                }
            }
        }
        return out;
    }

    /** 按代理键表分组的键选择器 / key selector that groups by the surrogate table. */
    public static <T> KeySelector<T, String> selector(Map<String, String> table,
                                                      SerializableFunction<T, String> deviceGetter) {
        return new Selector<>(new LinkedHashMap<>(table), deviceGetter);
    }

    /** 显式声明键类型为字符串，免得 Flink 从泛型 lambda 推断键类型失败 / declares the key type explicitly. */
    private static final class Selector<T> implements KeySelector<T, String>, ResultTypeQueryable<String> {
        private final LinkedHashMap<String, String> table;
        private final SerializableFunction<T, String> deviceGetter;

        Selector(LinkedHashMap<String, String> table, SerializableFunction<T, String> deviceGetter) {
            this.table = table;
            this.deviceGetter = deviceGetter;
        }

        @Override
        public String getKey(T value) {
            return keyOf(table, deviceGetter.apply(value));
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    /** 可序列化的取设备号函数 / serializable device getter. */
    public interface SerializableFunction<T, R> extends Function<T, R>, java.io.Serializable { }

    /** 表格转回配置串 / table back to the configuration string. */
    public static String format(Map<String, String> table) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : table.entrySet()) {
            if (sb.length() > 0) sb.append(',');
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.toString();
    }

    /**
     * 生成代理键表：java -cp <jar> com.leejean.m1.DeviceKeys A,B,C,D,E,F,G,H 8 [最大并行度]
     * Generate the table: java -cp <jar> com.leejean.m1.DeviceKeys A,B,C,D,E,F,G,H 8 [maxParallelism]
     */
    public static void main(String[] args) {
        String[] devices = (args.length > 0 ? args[0] : "A,B,C,D,E,F,G,H").split(",");
        int p = args.length > 1 ? Integer.parseInt(args[1]) : 8;
        int maxP = effectiveMaxParallelism(args.length > 2 ? Integer.parseInt(args[2]) : -1, p);
        Map<String, String> table = generate(java.util.Arrays.asList(devices), maxP, p);
        System.out.println("SYN_DEVICE_SURROGATE_KEYS=" + format(table));
        System.out.println("# parallelism=" + p + " maxParallelism=" + maxP + " placement="
                + placement(table, java.util.Arrays.asList(devices), maxP, p));
    }
}
