package com.leejean.m1;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M1 管线集成测试，沿用 FA-iForest 的 MiniCluster 模式：env.fromCollection 输入 + 静态收集 sink。
 * M1 pipeline integration tests, following FA-iForest's MiniCluster pattern (env.fromCollection
 * input + a static collecting sink). Bounded sources emit a final MAX watermark at completion, so
 * all event-time round-close timers fire.
 *
 * <p>覆盖 / covers（交接文档 §6）：四类解析守卫、保留首值去重、缺失掩码关轮、预热冻结边界、
 * IQR 旁路、设备间键隔离。
 */
class M1PipelineTest {

    /** 静态收集 sink / static collecting sink. */
    static final class CollectSink implements SinkFunction<DeviceRound> {
        private static final long serialVersionUID = 1L;
        static final List<DeviceRound> VALUES = Collections.synchronizedList(new ArrayList<>());
        @Override
        public void invoke(DeviceRound value, Context context) {
            VALUES.add(value);
        }
    }

    @BeforeEach
    void setUp() {
        CollectSink.VALUES.clear();
    }

    /** 从原始行构建并运行 parser→assembler→scaler→cache 管线，返回收集到的轮。 */
    private List<DeviceRound> run(List<String> lines, int warmup, double eps, int cacheDepth) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        // 事件时间 = 行内秒 × 1000（与 Kafka 记录时间同口径）/ event time = in-row second × 1000
        WatermarkStrategy<String> wm = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((line, ts) -> eventTsMs(line));

        DataStream<String> src = env.fromCollection(lines).assignTimestampsAndWatermarks(wm);

        SingleOutputStreamOperator<DeviceRound> out = src
                .process(new RawLineParser()).name("parser")
                .keyBy((KeySelector<Reading, String>) Reading::getDevice)
                .process(new RoundAssembler()).name("assembler")
                .keyBy((KeySelector<DeviceRound, String>) DeviceRound::getDevice)
                .process(new RobustScalerFunction(warmup, eps)).name("scaler")
                .keyBy((KeySelector<DeviceRound, String>) DeviceRound::getDevice)
                .process(new RawCacheFunction(cacheDepth, 10)).name("cache");
        out.addSink(new CollectSink());
        env.execute("m1-pipeline-test");

        List<DeviceRound> rounds = new ArrayList<>(CollectSink.VALUES);
        rounds.sort((a, b) -> a.getDevice().equals(b.getDevice())
                ? Long.compare(a.getTs(), b.getTs())
                : a.getDevice().compareTo(b.getDevice()));
        return rounds;
    }

    private static long eventTsMs(String line) {
        try {
            return Long.parseLong(line.split(",", -1)[0].trim()) * 1000L;
        } catch (RuntimeException e) {
            return 0L;
        }
    }

    /** 一行 / one CSV line. */
    private static String row(long ts, String dev, String sensor, String value) {
        return ts + "," + dev + "," + sensor + "," + value;
    }

    /** 一个设备一个采样轮的 8 传感器齐备行（可覆写各值）。 */
    private static List<String> fullRound(long ts, String dev,
                                          double temp, double hum, double pres, double gas, double light,
                                          double mic, double rssi, double accel) {
        List<String> l = new ArrayList<>();
        l.add(row(ts, dev, "Temperature", String.valueOf(temp)));
        l.add(row(ts, dev, "Humidity", String.valueOf(hum)));
        l.add(row(ts, dev, "Pressure", String.valueOf(pres)));
        l.add(row(ts, dev, "Gas", String.valueOf(gas)));
        l.add(row(ts, dev, "Light", String.valueOf(light)));
        l.add(row(ts, dev, "MIC", String.valueOf(mic)));
        l.add(row(ts, dev, "RSSI", String.valueOf(rssi)));
        l.add(row(ts, dev, "Accelerometer", String.valueOf(accel)));
        return l;
    }

    @Test
    void parserGuardsDedupAndRoundAssembly() throws Exception {
        List<String> lines = fullRound(1, "A", 22.5, 45.0, 101000.0, 600.0,
                Channels.LIGHT_CENSOR_VALUE, 3.0, Channels.RSSI_SENTINEL_VALUE, 0.0);
        // 守卫注入 / guard injections:
        lines.add(row(1, "A", "IR", "123"));                 // 未知传感器 (DF-9)
        lines.add(row(1, "A", "Temperature", "99.9"));       // 重复 → 保留首值 (DEV-D7a)
        lines.add(row(1, "A", "Gas", "not_a_number"));       // 数值不可解析 → 畸形 (malformed)

        List<DeviceRound> rounds = run(lines, 100, 1e-9, 1000);
        assertEquals(1, rounds.size());
        DeviceRound r = rounds.get(0);

        assertEquals("A", r.getDevice());
        assertEquals(1, r.getTs());
        assertEquals(22.5, r.getX()[0], 1e-9, "Temperature keep-first (not 99.9)");
        assertEquals(45.0, r.getX()[1], 1e-9);
        assertEquals(101000.0, r.getX()[2], 1e-9);
        assertEquals(600.0, r.getX()[3], 1e-9, "Gas kept its first (valid) value; the bad dup is malformed");
        assertEquals(1, r.getDupKeys(), "one duplicate Temperature");
        assertEquals(1, r.getUnknownSensor(), "IR dropped and counted");
        assertEquals(1, r.getMalformed(), "unparsable Gas value counted as malformed");
        assertTrue(r.getCensoredMask()[Channels.LIGHT_INDEX], "Light == 65536 right-censored (DF-11)");
        assertEquals(1, r.getCensoredLight());
        assertEquals(1, r.getRssiSentinel(), "RSSI == 0 sentinel (DEV-D7c)");
        assertEquals(3.0, r.getMic(), 1e-9, "MIC rides along as Gas-quality metadata (DEV-D7b)");
        assertFalse(r.isIncomplete(), "all five detection channels present");
    }

    @Test
    void incompleteRoundEmittedWithMissingMask() throws Exception {
        // 只有 Temperature + Humidity，缺 Pressure/Gas/Light → incomplete，带缺失掩码，绝不丢弃。
        List<String> lines = new ArrayList<>();
        lines.add(row(5, "B", "Temperature", "20.0"));
        lines.add(row(5, "B", "Humidity", "40.0"));

        List<DeviceRound> rounds = run(lines, 100, 1e-9, 1000);
        assertEquals(1, rounds.size());
        DeviceRound r = rounds.get(0);
        assertTrue(r.isIncomplete());
        assertFalse(r.getMissingMask()[0]);   // Temperature present
        assertFalse(r.getMissingMask()[1]);   // Humidity present
        assertTrue(r.getMissingMask()[2]);     // Pressure missing
        assertTrue(r.getMissingMask()[3]);     // Gas missing
        assertTrue(r.getMissingMask()[4]);     // Light missing
        assertEquals(3, r.missingCount());
    }

    @Test
    void warmupFreezeBoundaryThenScales() throws Exception {
        // warmup=3：前 3 轮预热(原始透传)，第 4/5 轮用冻结的 中位数/IQR 缩放。
        // Temperature 10,20,30 → median 20, IQR (Q3−Q1)=25−15=10；round4 Temp=40 → (40−20)/10 = 2.0
        List<String> lines = new ArrayList<>();
        double[] temps = {10, 20, 30, 40, 50};
        for (int i = 0; i < temps.length; i++) {
            // 其它检测通道给足够方差以避免旁路 / vary other channels too so they are not bypassed
            double f = i + 1;
            lines.addAll(fullRound(i + 1, "A",
                    temps[i], 40 + f, 101000 + f, 600 + f, 1000 + f, 3.0, 70.0, 0.0));
        }
        List<DeviceRound> rounds = run(lines, 3, 1e-9, 1000);
        assertEquals(5, rounds.size());

        assertTrue(rounds.get(0).isWarmup());
        assertTrue(rounds.get(1).isWarmup());
        assertTrue(rounds.get(2).isWarmup());
        assertFalse(rounds.get(3).isWarmup(), "4th round is post-freeze");
        assertFalse(rounds.get(4).isWarmup());

        // 预热轮 xNorm 为原始透传 / warm-up rounds pass raw through
        assertEquals(10.0, rounds.get(0).getXNorm()[0], 1e-9);
        // 冻结后缩放 / scaled after freeze
        assertEquals(2.0, rounds.get(3).getXNorm()[0], 1e-9, "(40-20)/10 == 2.0");
        assertEquals(3.0, rounds.get(4).getXNorm()[0], 1e-9, "(50-20)/10 == 3.0");
    }

    @Test
    void iqrBypassOnConstantChannel() throws Exception {
        // Pressure 预热期恒定 → IQR=0 → 旁路（原值透传、置旗）。
        List<String> lines = new ArrayList<>();
        double[] temps = {10, 20, 30, 40};
        for (int i = 0; i < temps.length; i++) {
            lines.addAll(fullRound(i + 1, "A",
                    temps[i], 40 + i, 101000.0 /* constant Pressure */, 600 + i, 1000 + i, 3.0, 70.0, 0.0));
        }
        List<DeviceRound> rounds = run(lines, 3, 1e-9, 1000);
        DeviceRound r4 = rounds.get(3);
        assertFalse(r4.isWarmup());
        assertTrue(r4.getBypassMask()[2], "constant Pressure channel bypassed (IQR<=eps)");
        assertEquals(101000.0, r4.getXNorm()[2], 1e-9, "bypassed channel passes raw through");
        // Temperature (variable) still scaled, not bypassed
        assertFalse(r4.getBypassMask()[0]);
    }

    @Test
    void keyedIsolationBetweenDevices() throws Exception {
        // A 与 B 交错；各自独立预热/冻结。A: Temp 10,20,30,40; B: Temp 100,200,300,400
        List<String> lines = new ArrayList<>();
        double[] aTemps = {10, 20, 30, 40};
        double[] bTemps = {100, 200, 300, 400};
        for (int i = 0; i < 4; i++) {
            double f = i + 1;
            lines.addAll(fullRound(i + 1, "A", aTemps[i], 40 + f, 101000 + f, 600 + f, 1000 + f, 3, 70, 0));
            lines.addAll(fullRound(i + 1, "B", bTemps[i], 50 + f, 102000 + f, 700 + f, 2000 + f, 1, 60, 0));
        }
        List<DeviceRound> rounds = run(lines, 3, 1e-9, 1000);
        assertEquals(8, rounds.size());

        DeviceRound a4 = pick(rounds, "A", 4);
        DeviceRound b4 = pick(rounds, "B", 4);
        assertNotNull(a4);
        assertNotNull(b4);
        // A median 20, IQR 10 → (40-20)/10 = 2.0 ; B median 200, IQR 100 → (400-200)/100 = 2.0
        assertEquals(2.0, a4.getXNorm()[0], 1e-9);
        assertEquals(2.0, b4.getXNorm()[0], 1e-9);
        // 值域不同证明各自用了自己的统计 / different raw ranges prove independent per-device stats
        assertEquals(40.0, a4.getX()[0], 1e-9);
        assertEquals(400.0, b4.getX()[0], 1e-9);
    }

    private static DeviceRound pick(List<DeviceRound> rounds, String dev, long ts) {
        for (DeviceRound r : rounds) {
            if (r.getDevice().equals(dev) && r.getTs() == ts) {
                return r;
            }
        }
        return null;
    }
}
