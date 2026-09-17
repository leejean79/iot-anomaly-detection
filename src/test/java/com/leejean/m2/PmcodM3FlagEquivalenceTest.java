package com.leejean.m2;

import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m3.AnnotatedRound;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 保障测试：M3 标注轮侧输出的开关不得改变 M2 的任何产出（三月全月基线 runbook 澄清一）。
 * Safeguard: toggling the M3 annotated-round side output must not change any M2 output
 * (full-March baseline runbook, clarification 1).
 *
 * <p><b>为何需要它</b>：本次基线放弃了"从 M3 改动之前的提交构建 jar"的做法——那个提交（9236228）
 * 的 pom 仍是 Java 8 目标配 Java 11 字节码的 DL4J，且早于原生库打包修复与 enforcer 守卫，用它会引入
 * 两个不受控变量。改为用同一个 HEAD jar、靠 {@code --m3-enabled false} 关掉转发分支。该做法成立的
 * 前提是"转发分支对 M2 纯增量、只读"，本测试就是把这个前提变成可执行的断言，而不是只靠代码走读。
 * Why: the baseline runs the HEAD jar with the flag off instead of building from the last pre-M3
 * commit (whose pom would introduce two uncontrolled variables). That rests on the forwarding branch
 * being additive and read-only w.r.t. M2; this test turns that premise into an executable assertion.
 *
 * <p><b>断言三类产出逐项相同</b>：主输出 {@link ScoreEvent} 名单、监测快照 {@link MonitoringSnapshot}、
 * 以及 {@code m2_*} 计数器（经自定义 {@link MetricReporter} 真实读取，不是由输出反推）。
 * Asserts all three: the ScoreEvent list, the monitoring snapshots, and the m2_* counters (read for
 * real through a MetricReporter rather than inferred from the outputs).
 *
 * <p><b>防空跑</b>：同时断言开启侧确实产出了标注轮，否则"两边都没跑"也会让等值断言通过。
 * Anti-vacuity: also asserts the flag-on run actually emitted annotated rounds, so a run where
 * neither side produced anything cannot pass.
 */
class PmcodM3FlagEquivalenceTest {

    private static final int WINDOW_SEC = 60;    // 测试用短窗，语义与生产的 3600s 一致 / short window, same semantics
    private static final int SLIDE_SEC = 10;
    private static final int K = 3;
    private static final double R = 1.0;

    // ---- 收集器：静态是 MiniCluster 模式的既有约定（见 M1PipelineTest）/ static collectors, as in M1PipelineTest ----
    static final List<ScoreEvent> SCORES = Collections.synchronizedList(new ArrayList<>());
    static final List<MonitoringSnapshot> SNAPS = Collections.synchronizedList(new ArrayList<>());
    static final List<AnnotatedRound> ANNOTATED = Collections.synchronizedList(new ArrayList<>());
    /** 计数器名 → 值，由下面的 reporter 在作业结束时抓取 / counter name → value, captured by the reporter. */
    static final Map<String, Long> COUNTERS = Collections.synchronizedMap(new TreeMap<>());

    static final class ScoreSink implements SinkFunction<ScoreEvent> {
        private static final long serialVersionUID = 1L;
        @Override public void invoke(ScoreEvent v, Context c) { SCORES.add(v); }
    }

    static final class SnapSink implements SinkFunction<MonitoringSnapshot> {
        private static final long serialVersionUID = 1L;
        @Override public void invoke(MonitoringSnapshot v, Context c) { SNAPS.add(v); }
    }

    static final class AnnotatedSink implements SinkFunction<AnnotatedRound> {
        private static final long serialVersionUID = 1L;
        @Override public void invoke(AnnotatedRound v, Context c) { ANNOTATED.add(v); }
    }

    /**
     * 抓取 m2_* 计数器的报告器。Flink 在注册每个指标时回调本类；作业结束前 close() 里把当前值落入
     * {@link #COUNTERS}。Flink 用无参构造实例化它，故必须是 public static。
     * Reporter capturing the m2_* counters: Flink calls back on registration, and close() records the
     * final values. Flink instantiates it reflectively, so it must be public and have a no-arg ctor.
     */
    public static final class CounterGrabber implements MetricReporter {
        private final Map<String, Counter> seen = new LinkedHashMap<>();
        @Override public void open(MetricConfig config) { }
        @Override public void close() {
            // close() 在作业结束时调用，此时计数器已累计完毕 / called at shutdown, counters final by then
            for (Map.Entry<String, Counter> e : seen.entrySet()) {
                COUNTERS.merge(e.getKey(), e.getValue().getCount(), Long::sum);
            }
        }
        @Override public void notifyOfAddedMetric(Metric metric, String name, MetricGroup group) {
            if (metric instanceof Counter && name.startsWith("m2_")) {
                seen.put(name, (Counter) metric);
            }
        }
        @Override public void notifyOfRemovedMetric(Metric metric, String name, MetricGroup group) {
            // 指标注销时立即结算，避免算子先于 reporter 关闭导致漏记 / settle on removal, before shutdown
            if (metric instanceof Counter && name.startsWith("m2_") && seen.remove(name) != null) {
                COUNTERS.merge(name, ((Counter) metric).getCount(), Long::sum);
            }
        }
    }

    /** 一次运行的三类产出 / the three kinds of output from one run. */
    private static final class Result {
        final List<String> scores = new ArrayList<>();
        final List<String> snaps = new ArrayList<>();
        final Map<String, Long> counters = new TreeMap<>();
        int annotated;
    }

    /**
     * 跑一遍 PmcodFunction。{@code m3Enabled=false} 时传入 null 标签，与 M2Job 在
     * {@code --m3-enabled false} 下的接线完全一致。
     * Run PmcodFunction once; with m3Enabled=false the tag is null, exactly as M2Job wires it.
     */
    private Result run(List<DevicePoint> input, boolean m3Enabled) throws Exception {
        SCORES.clear(); SNAPS.clear(); ANNOTATED.clear(); COUNTERS.clear();

        Configuration cfg = new Configuration();
        cfg.setString("metrics.reporter.grab.class", CounterGrabber.class.getName());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, cfg);
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        WatermarkStrategy<DevicePoint> wm = WatermarkStrategy
                .<DevicePoint>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((dp, ts) -> dp.getPoint().arrival);

        OutputTag<MonitoringSnapshot> monTag = new OutputTag<MonitoringSnapshot>("m2-monitoring") { };
        // 与 M2Job 同款：关闭时标签为 null / same as M2Job: the tag is null when disabled
        OutputTag<AnnotatedRound> m3Tag = m3Enabled
                ? new OutputTag<AnnotatedRound>("m3-annotated") { } : null;

        DataStream<DevicePoint> src = env.fromCollection(input).assignTimestampsAndWatermarks(wm);
        SingleOutputStreamOperator<ScoreEvent> scored = src
                .keyBy((KeySelector<DevicePoint, String>) DevicePoint::getDevice)
                .window(SlidingEventTimeWindows.of(Time.seconds(WINDOW_SEC), Time.seconds(SLIDE_SEC)))
                .process(new PmcodFunction(R, K, SLIDE_SEC, Collections.emptyMap(), monTag, m3Tag))
                .name("Pmcod");

        scored.addSink(new ScoreSink());
        scored.getSideOutput(monTag).addSink(new SnapSink());
        if (m3Tag != null) {
            scored.getSideOutput(m3Tag).addSink(new AnnotatedSink());
        }
        env.execute("pmcod-m3-flag-" + (m3Enabled ? "on" : "off"));

        Result r = new Result();
        for (ScoreEvent s : SCORES) {
            r.scores.add(s.getDevice() + "|" + s.getRoundTs() + "|" + s.getWindowEnd()
                    + "|" + s.getChannel() + "|" + s.isOutlier());
        }
        Collections.sort(r.scores);
        for (MonitoringSnapshot s : SNAPS) {
            // 逐字段串行化：任何一个字段变动都会让比较失败 / serialize every field, so any drift fails
            r.snaps.add(String.format("%s|%d|%d|%.10f|%.10f|%.10f|%.10f|%d|%d|%s",
                    s.getDevice(), s.getTs(), s.getWindowEnd(), s.getM2OutlierRate(),
                    s.getM2McOccupancy(), s.getM2NeighborCountP10(), s.getM2NeighborCountP50(),
                    s.getM2Outliers(), s.getM2WindowPoints(), s.isM2ColdCleared()));
        }
        Collections.sort(r.snaps);
        r.counters.putAll(COUNTERS);
        r.annotated = ANNOTATED.size();
        return r;
    }

    /**
     * 构造一段确定性的输入：两台设备、五维向量，掺入少量离群点使名单非空。
     * A deterministic input: two devices, five-dimensional vectors, with a few planted outliers so
     * the outlier list is non-empty (an all-empty run would make the comparison meaningless).
     */
    private static List<DevicePoint> buildInput() {
        List<DevicePoint> pts = new ArrayList<>();
        Random rnd = new Random(20260917L);           // 固定种子保证两次运行输入逐位相同 / fixed seed
        long t0 = 1_646_092_800_000L;                 // 2022-03-01T00:00:00Z（毫秒）
        for (String dev : new String[]{"A", "B"}) {
            for (int i = 0; i < 180; i++) {
                long arrival = t0 + i * 1000L;
                double[] v = new double[5];
                boolean planted = (i % 37 == 0);      // 每 37 轮种一个远点 / plant a far point every 37 rounds
                for (int c = 0; c < 5; c++) {
                    v[c] = planted ? 40.0 + rnd.nextDouble() : rnd.nextGaussian() * 0.05;
                }
                boolean[] censored = new boolean[]{false, false, false, planted, false};
                pts.add(new DevicePoint(dev, new McodPoint(v, arrival, 0, arrival / 1000L), censored));
            }
        }
        return pts;
    }

    @Test
    void togglingTheM3TagLeavesEveryM2OutputIdentical() throws Exception {
        List<DevicePoint> input = buildInput();

        Result off = run(input, false);   // --m3-enabled false（基线运行的配置）/ the baseline's configuration
        Result on = run(input, true);     // --m3-enabled true（后续 M3 运行的配置）/ the later M3 configuration

        // 防空跑：开启侧必须真的发出了标注轮，关闭侧必须一条都没有。
        // Anti-vacuity: the flag-on run must really have emitted annotated rounds; flag-off none.
        assertTrue(on.annotated > 0, "开启 M3 时应产出标注轮，否则等值断言是空跑 / flag-on must emit annotated rounds");
        assertEquals(0, off.annotated, "关闭 M3 时不应产出任何标注轮 / flag-off must emit none");

        // 主输出：离群点名单逐条相同 / main output: the outlier list is identical
        assertFalse(off.scores.isEmpty(), "离群点名单不应为空，否则比较无意义 / the outlier list must be non-empty");
        assertEquals(off.scores, on.scores, "ScoreEvent 名单在开关前后必须逐条相同");

        // 监测快照：每个字段逐位相同 / snapshots: every field identical
        assertFalse(off.snaps.isEmpty(), "监测快照不应为空 / snapshots must be non-empty");
        assertEquals(off.snaps, on.snaps, "MonitoringSnapshot 在开关前后必须逐字段相同");

        // 计数器：m2_* 全部相同（真实读取，非由输出反推）/ counters: all m2_* identical, really read
        assertFalse(off.counters.isEmpty(), "应至少抓到一个 m2_* 计数器，否则 reporter 没生效 / reporter must capture counters");
        assertEquals(off.counters, on.counters, "m2_* 计数器在开关前后必须相同");
    }
}
