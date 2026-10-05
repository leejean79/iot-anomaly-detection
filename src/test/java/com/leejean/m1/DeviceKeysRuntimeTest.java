package com.leejean.m1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 在本地 Flink 运行时里核对代理键的实际落位：并行度八时八台设备各在一个子任务上，且与
 * {@link DeviceKeys#placement} 的计算一致；用代理键当设备号的算子经 {@link DeviceKeys#deviceOf} 还原出原设备号。
 * Checks the actual placement in a local Flink runtime at parallelism 8.
 */
class DeviceKeysRuntimeTest {

    private static final Map<String, String> SEEN = new ConcurrentHashMap<>();

    @Test
    void eachDeviceRunsOnItsOwnSubtaskInTheRuntime() throws Exception {
        SEEN.clear();
        Map<String, String> table = DeviceKeys.parse(DeviceKeysTest.CONFIGURED);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(8);
        env.fromElements("A", "B", "C", "D", "E", "F", "G", "H")
                .keyBy(DeviceKeys.selector(table, (String d) -> d))
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String d, Context ctx, Collector<String> out) {
                        out.collect(DeviceKeys.deviceOf(ctx.getCurrentKey()) + "@"
                                + getRuntimeContext().getIndexOfThisSubtask());
                    }
                })
                .addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String v, Context c) {
                        String[] p = v.split("@");
                        SEEN.put(p[0], p[1]);
                    }
                });
        env.execute("device-keys-runtime-test");

        assertEquals(8, SEEN.size(), SEEN.toString());
        assertEquals(8, new HashSet<>(SEEN.values()).size(), "八台设备应各在一个子任务：" + SEEN);
        Map<String, Integer> expected = DeviceKeys.placement(table, SEEN.keySet(),
                DeviceKeys.effectiveMaxParallelism(-1, 8), 8);
        Map<String, Integer> actual = new HashMap<>();
        SEEN.forEach((d, s) -> actual.put(d, Integer.parseInt(s)));
        assertEquals(expected, actual);
    }
}
