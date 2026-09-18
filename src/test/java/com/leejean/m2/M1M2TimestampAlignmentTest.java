package com.leejean.m2;

import com.leejean.m1.ChannelTransform;
import com.leejean.m1.DeviceRound;
import com.leejean.m1.MonitoringAggregator;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m1.RawCacheFunction;
import com.leejean.m1.RawLineParser;
import com.leejean.m1.Reading;
import com.leejean.m1.RobustScalerFunction;
import com.leejean.m1.RoundAssembler;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 作业级对齐测试（M2 补充件 §2 测试二，2026-09-18）：在 MiniCluster 上跑 M1+M2 链，断言
 * Option A 的事件时间重对齐确实把偏移量消除到零。
 * Job-level alignment test (M2 addendum section 2, test 2): run the M1 + M2 chain on a MiniCluster
 * and assert that the Option A re-alignment really drives the offset to zero.
 *
 * <p>三条断言对应补充件 §2 的 (a)(b)(c)：重对齐下游的 DeviceRound 其 Flink 时间戳等于轮的标称时间；
 * M1 监测的 60 秒窗口标签落在标称分钟边界上；M2 的迟到丢弃为零。另加一条端到端断言：窗口几何恢复
 * 到每轮进入 W/S = 60 个窗口——这是 2026-09-18 发现报告里作废整轮基线的那个量。
 * The three assertions are the addendum's (a), (b) and (c), plus an end-to-end one: the window
 * geometry is back to W/S = 60, the quantity whose halving voided the first full-March baseline.
 */
class M1M2TimestampAlignmentTest {

    private static final int WINDOW_SEC = 3600;
    private static final int SLIDE_SEC = 60;
    private static final int PERIOD_SEC = 10;
    /** RoundAssembler 的关闭延迟；重对齐若失效，下游时间戳会整体后移这么多。/ the close delay. */
    private static final long CLOSE_DELAY_MS = 30_000L;

    static final List<Long> ROUND_TS_MS = Collections.synchronizedList(new ArrayList<>());
    static final List<Long> ROUND_FLINK_TS = Collections.synchronizedList(new ArrayList<>());
    static final List<MonitoringSnapshot> M1_SNAPS = Collections.synchronizedList(new ArrayList<>());
    static final List<MonitoringSnapshot> M2_SNAPS = Collections.synchronizedList(new ArrayList<>());
    static final List<DevicePoint> LATE = Collections.synchronizedList(new ArrayList<>());

    /** 记录每个 DeviceRound 的「标称时间」与「Flink 时间戳」两个时钟 / record both clocks per round. */
    static final class ClockProbe extends ProcessFunction<DeviceRound, DeviceRound> {
        private static final long serialVersionUID = 1L;
        @Override
        public void processElement(DeviceRound r, Context ctx, Collector<DeviceRound> out) {
            ROUND_TS_MS.add(r.getTs() * 1000L);
            ROUND_FLINK_TS.add(ctx.timestamp());
            out.collect(r);
        }
    }

    static final class M1SnapSink implements SinkFunction<MonitoringSnapshot> {
        private static final long serialVersionUID = 1L;
        @Override public void invoke(MonitoringSnapshot v, Context c) { M1_SNAPS.add(v); }
    }

    static final class M2SnapSink implements SinkFunction<MonitoringSnapshot> {
        private static final long serialVersionUID = 1L;
        @Override public void invoke(MonitoringSnapshot v, Context c) { M2_SNAPS.add(v); }
    }

    static final class LateSink implements SinkFunction<DevicePoint> {
        private static final long serialVersionUID = 1L;
        @Override public void invoke(DevicePoint v, Context c) { LATE.add(v); }
    }

    static final class NullScoreSink implements SinkFunction<ScoreEvent> {
        private static final long serialVersionUID = 1L;
        @Override public void invoke(ScoreEvent v, Context c) { }
    }

    /** 五通道原始行，与生产同格式：ts,device,sensor,value / raw CSV lines, five detection channels. */
    private static List<String> rawLines(int hours) {
        long t0 = 1_646_092_800L;                      // 2022-03-01T00:00:00Z（秒）
        int rounds = hours * 3600 / PERIOD_SEC;
        // 五个检测通道，名称与顺序取自 Channels.DETECTION / the five detection channels
        String[] sensors = {"Temperature", "Humidity", "Pressure", "Gas", "Light"};
        double[] base = {21.0, 45.0, 1013.0, 120.0, 300.0};
        List<String> lines = new ArrayList<>(rounds * sensors.length);
        for (int i = 0; i < rounds; i++) {
            long ts = t0 + (long) i * PERIOD_SEC;
            for (int c = 0; c < sensors.length; c++) {
                // 轻微确定性抖动，避免所有值完全相同导致 IQR = 0 触发旁路
                // A small deterministic wobble so the IQR is not zero and the guard does not bypass.
                double v = base[c] + ((i + c) % 7) * 0.1;
                lines.add(ts + ",A," + sensors[c] + "," + v);
            }
        }
        return lines;
    }

    @Test
    void realignmentDrivesTheOffsetToZeroAndRestoresWindowGeometry() throws Exception {
        ROUND_TS_MS.clear(); ROUND_FLINK_TS.clear();
        M1_SNAPS.clear(); M2_SNAPS.clear(); LATE.clear();

        int hours = 4;
        // 预热轮数取小值，使标定窗口在测试时长内冻结、轮能通过 M2Gate / small warm-up so rounds pass the gate
        int warmupRounds = 60;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        // 源端与生产同款：事件时间取自记录时间戳、55 秒有界乱序
        // Source-side strategy as in production: event time from the record timestamp, 55 s bound.
        WatermarkStrategy<String> srcWm = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(55))
                .withTimestampAssigner((line, ts) -> Long.parseLong(line.split(",", -1)[0]) * 1000L);

        DataStream<String> raw = env.fromCollection(rawLines(hours)).assignTimestampsAndWatermarks(srcWm);

        SingleOutputStreamOperator<DeviceRound> rounds = raw
                .process(new RawLineParser()).name("RawLineParser")
                .keyBy((KeySelector<Reading, String>) Reading::getDevice)
                .process(new RoundAssembler()).name("RoundAssembler");

        // 被测对象：与 M1Job / M2Job 中一字不差的重对齐 / the re-alignment exactly as both jobs do it
        SingleOutputStreamOperator<DeviceRound> aligned = rounds
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<DeviceRound>forMonotonousTimestamps()
                        .withIdleness(Duration.ofSeconds(10))
                        .withTimestampAssigner((round, ts) -> round.getTs() * 1000L))
                .name("AlignEventTime");

        SingleOutputStreamOperator<DeviceRound> cached = aligned
                .process(new ClockProbe()).name("ClockProbe")
                .keyBy((KeySelector<DeviceRound, String>) DeviceRound::getDevice)
                .process(new RobustScalerFunction(
                        warmupRounds, 1e-9, ChannelTransform.defaultTable(), false)).name("RobustScaler")
                .keyBy((KeySelector<DeviceRound, String>) DeviceRound::getDevice)
                .process(new RawCacheFunction(1000, PERIOD_SEC)).name("RawCache");

        // (b) M1 监测快照 / M1 monitoring snapshots
        cached.keyBy((KeySelector<DeviceRound, String>) DeviceRound::getDevice)
                .process(new MonitoringAggregator()).name("MonitoringAggregator")
                .addSink(new M1SnapSink());

        // (c) + 端到端：M2 窗口与迟到侧输出 / the M2 window and its late side output
        OutputTag<MonitoringSnapshot> monTag = new OutputTag<MonitoringSnapshot>("m2-monitoring") { };
        OutputTag<DevicePoint> lateTag = new OutputTag<DevicePoint>("m2-late") { };
        SingleOutputStreamOperator<ScoreEvent> scored = cached
                .process(new M2Gate()).name("M2Gate")
                .keyBy((KeySelector<DevicePoint, String>) DevicePoint::getDevice)
                .window(SlidingEventTimeWindows.of(Time.seconds(WINDOW_SEC), Time.seconds(SLIDE_SEC)))
                .allowedLateness(Time.seconds(0))
                .sideOutputLateData(lateTag)
                .process(new PmcodFunction(1.0, 10, SLIDE_SEC, Collections.emptyMap(), monTag, null))
                .name("Pmcod");
        scored.addSink(new NullScoreSink());
        scored.getSideOutput(monTag).addSink(new M2SnapSink());
        scored.getSideOutput(lateTag).addSink(new LateSink());

        env.execute("m1-m2-alignment");

        // ---- (a) 重对齐下游的 Flink 时间戳 == 轮的标称时间 ----
        assertFalse(ROUND_TS_MS.isEmpty(), "应有轮流经重对齐点 / rounds must reach the re-alignment");
        for (int i = 0; i < ROUND_TS_MS.size(); i++) {
            long nominal = ROUND_TS_MS.get(i);
            long flinkTs = ROUND_FLINK_TS.get(i);
            assertEquals(nominal, flinkTs,
                    "重对齐后 Flink 时间戳必须等于轮的标称时间；若相差 " + CLOSE_DELAY_MS
                            + "ms 说明重对齐失效 / must equal the nominal round time");
        }

        // ---- (b) M1 监测的 60 秒窗口标签落在标称分钟边界 ----
        assertFalse(M1_SNAPS.isEmpty(), "应有 M1 监测快照 / M1 snapshots must exist");
        for (MonitoringSnapshot s : M1_SNAPS) {
            assertEquals(0L, s.getTs() % 60L,
                    "M1 快照标签应落在标称分钟边界，实测 ts=" + s.getTs()
                            + " / the label must sit on a nominal minute boundary");
        }

        // ---- (c) M2 迟到丢弃为零 ----
        assertTrue(LATE.isEmpty(), "重对齐不得制造迟到数据，实测迟到 " + LATE.size()
                + " 条 / the re-alignment must not create late data");

        // ---- 端到端：窗口几何恢复，每轮进入 W/S 个窗口 ----
        assertFalse(M2_SNAPS.isEmpty(), "应有 M2 快照 / M2 snapshots must exist");
        long admitted = 0;
        for (MonitoringSnapshot s : M2_SNAPS) {
            admitted = Math.max(admitted, 0);
        }
        long sumWindowPoints = 0;
        for (MonitoringSnapshot s : M2_SNAPS) {
            sumWindowPoints += s.getM2WindowPoints();
        }
        // 进入 M2 的轮数 = 总轮数 − 预热旁路 / rounds admitted = total − warm-up bypass
        long totalRounds = ROUND_TS_MS.size();
        long admittedRounds = totalRounds - warmupRounds;
        double perAdmitted = sumWindowPoints / (double) admittedRounds;
        assertEquals(WINDOW_SEC / SLIDE_SEC, perAdmitted, 0.5,
                "每轮应进入 W/S = " + (WINDOW_SEC / SLIDE_SEC) + " 个窗口，实测 " + perAdmitted
                        + "；缺陷未修复时该值为 " + (WINDOW_SEC / SLIDE_SEC) / 2
                        + " / each round must enter W/S windows");
    }
}
