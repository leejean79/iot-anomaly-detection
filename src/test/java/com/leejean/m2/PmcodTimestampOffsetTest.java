package com.leejean.m2;

import com.leejean.m1.MonitoringSnapshot;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 钉住 {@link PmcodFunction} 对「窗口分配时钟」与「MCOD 准入时钟」偏移量 δ 的敏感度
 * （M2 补充件 §2 测试一，2026-09-18 发现报告）。
 * Pins PmcodFunction's sensitivity to the offset δ between the window-assignment clock and MCOD's
 * own admission clock (M2 addendum section 2, test 1).
 *
 * <p><b>缺陷机制</b>：一个点被分配到包含 {@code arrival + δ} 的那些窗口，而 MCOD 只在
 * {@code arrival >= windowEnd - slide} 时插入它。两者仅当 {@code (arrival mod slide) < slide - δ}
 * 时同时成立，故有 {@code δ / slide} 比例的点被计入 admitted、出现在窗口 elements 里，却从不进入
 * MCOD 状态。于是 {@code sum(windowPoints) / admitted == (W/S) × (1 - δ/S)}。
 * Mechanism: a point lands in the windows containing arrival + δ, but MCOD inserts it only when
 * arrival >= windowEnd - slide; a fraction δ/slide is therefore never inserted, giving
 * sum(windowPoints)/admitted == (W/S) × (1 - δ/S).
 *
 * <p><b>本测试断言的是缺陷本身，不是修复后的状态。</b>修复（Option A）在 {@code M1Job}/{@code M2Job}
 * 里把事件时间重赋为轮的标称时间，使作业运行时 δ = 0；{@link M1M2TimestampAlignmentTest} 断言这一点。
 * 这里保留 δ = 10s / 30s 的期望值，是为了让这条敏感度永远处于被监视状态：一旦将来有人去掉重对齐、
 * 或在 RoundAssembler 之后新增一个从 onTimer 发射的算子，δ 会重新出现，而届时只有本测试能说明
 * 「为什么 60 变成了 30」。补充件 §2 写的「δ=30s 时断言比值为 60」与 §7「不得改动 McodCore」互斥
 * ——见报告中的矛盾说明；此处按后者执行，把 δ=30s 的真实值 30 钉住。
 * This test pins the DEFECT, not the fixed state: the fix removes δ upstream (asserted by
 * M1M2TimestampAlignmentTest), while these expectations keep the sensitivity under watch so that a
 * future removal of the re-alignment, or a new onTimer-emitting operator after RoundAssembler, is
 * caught with an explanation rather than as an unexplained halving.
 */
class PmcodTimestampOffsetTest {

    private static final int WINDOW_SEC = 3600;
    private static final int SLIDE_SEC = 60;
    private static final int PERIOD_SEC = 10;
    private static final int HOURS = 6;

    static final List<MonitoringSnapshot> SNAPS = Collections.synchronizedList(new ArrayList<>());

    static final class SnapSink implements SinkFunction<MonitoringSnapshot> {
        private static final long serialVersionUID = 1L;
        @Override public void invoke(MonitoringSnapshot v, Context c) { SNAPS.add(v); }
    }

    static final class NullSink implements SinkFunction<ScoreEvent> {
        private static final long serialVersionUID = 1L;
        @Override public void invoke(ScoreEvent v, Context c) { }
    }

    /** 一次运行的结果 / one run's outcome. */
    private static final class Run {
        double pointsPerAdmitted;
        double steadyPointsPerWindow;
        int admitted;
    }

    /** 构造确定性输入：单设备、10 秒一轮、六小时 / deterministic input: one device, 10 s rounds, 6 h. */
    private static List<DevicePoint> input() {
        long t0 = 1_646_092_800_000L;                       // 2022-03-01T00:00:00Z
        int n = HOURS * 3600 / PERIOD_SEC;
        Random rnd = new Random(20260918L);                 // 固定种子 / fixed seed
        List<DevicePoint> pts = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            long arrival = t0 + (long) i * PERIOD_SEC * 1000L;
            double[] v = new double[5];
            for (int c = 0; c < 5; c++) {
                v[c] = rnd.nextGaussian() * 0.05;
            }
            pts.add(new DevicePoint("A", new McodPoint(v, arrival, 0, arrival / 1000L), new boolean[5]));
        }
        return pts;
    }

    /** 以 Flink 时间戳 = arrival + δ 跑一遍真实的 PmcodFunction / run the real PmcodFunction at offset δ. */
    private Run runWithOffset(long deltaMs) throws Exception {
        SNAPS.clear();
        List<DevicePoint> pts = input();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        // 关键：窗口分配用 arrival + δ，而 McodPoint.arrival 保持不变——正是缺陷的形态。
        // The window-assignment clock is arrival + δ while McodPoint.arrival is unchanged.
        WatermarkStrategy<DevicePoint> wm = WatermarkStrategy
                .<DevicePoint>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((dp, ts) -> dp.getPoint().arrival + deltaMs);

        OutputTag<MonitoringSnapshot> monTag = new OutputTag<MonitoringSnapshot>("m2-monitoring") { };
        DataStream<DevicePoint> src = env.fromCollection(pts).assignTimestampsAndWatermarks(wm);
        SingleOutputStreamOperator<ScoreEvent> scored = src
                .keyBy((KeySelector<DevicePoint, String>) DevicePoint::getDevice)
                .window(SlidingEventTimeWindows.of(Time.seconds(WINDOW_SEC), Time.seconds(SLIDE_SEC)))
                .process(new PmcodFunction(1.0, 10, SLIDE_SEC, Collections.emptyMap(), monTag, null))
                .name("Pmcod");
        scored.addSink(new NullSink());
        scored.getSideOutput(monTag).addSink(new SnapSink());
        env.execute("pmcod-offset-" + deltaMs);

        long sum = 0;
        for (MonitoringSnapshot s : SNAPS) {
            sum += s.getM2WindowPoints();
        }
        List<MonitoringSnapshot> sorted = new ArrayList<>(SNAPS);
        sorted.sort((a, b) -> Long.compare(a.getWindowEnd(), b.getWindowEnd()));
        // 跳过流首 W/S 个未填满的爬升窗口与流尾最后一个 / skip the ramp-up and the final partial window
        long steady = 0;
        int steadyN = 0;
        for (int i = WINDOW_SEC / SLIDE_SEC; i < sorted.size() - 1; i++) {
            steady += sorted.get(i).getM2WindowPoints();
            steadyN++;
        }
        Run r = new Run();
        r.admitted = pts.size();
        r.pointsPerAdmitted = sum / (double) pts.size();
        r.steadyPointsPerWindow = steadyN > 0 ? steady / (double) steadyN : 0.0;
        return r;
    }

    @Test
    void offsetZeroAdmitsEveryRound() throws Exception {
        Run r = runWithOffset(0L);
        // 无偏移：每轮进入 W/S = 60 个窗口，一个不漏 / no offset: every round enters all 60 windows
        assertEquals(60.0, r.pointsPerAdmitted, 1e-9,
                "δ=0 时每轮应进入全部 60 个窗口 / every round must enter all W/S windows");
        // 稳态每窗点数应接近 (W/S - 1) × 每桶轮数，此处 59 × 6 = 354（含边界效应故给容差）
        assertTrue(r.steadyPointsPerWindow > 300,
                "δ=0 时稳态每窗点数应在 350 量级，实测 " + r.steadyPointsPerWindow);
    }

    @Test
    void offsetTenSecondsLosesOneSixthOfRounds() throws Exception {
        Run r = runWithOffset(10_000L);
        // 丢失比例 = δ/S = 10/60 → 60 × (1 - 1/6) = 50
        assertEquals(50.0, r.pointsPerAdmitted, 1e-9,
                "δ=10s 时应丢失 1/6 的轮 / a sixth of the rounds must be lost");
    }

    @Test
    void offsetThirtySecondsLosesHalfTheRounds() throws Exception {
        Run r = runWithOffset(30_000L);
        // 丢失比例 = δ/S = 30/60 → 60 × (1 - 1/2) = 30。这正是集群实测的 29.98。
        // This is exactly the cluster's observed 29.98 before the fix.
        assertEquals(30.0, r.pointsPerAdmitted, 1e-9,
                "δ=30s（RoundAssembler 的关闭延迟）时应丢失一半的轮 / half the rounds must be lost");
    }
}
