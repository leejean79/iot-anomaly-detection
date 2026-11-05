package com.leejean.m2;

import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m6.Backfill;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 点异常检测模块的换版本（M6a 交接文档第四节）：滑动步内新到的点版本号变化时，与冷启动同一处清空状态，再用附件回填窗口内的旧点；
 * 回填的点只建立邻居关系、不发出判定；离群记录带版本号。每十秒一个点，窗长六十秒、滑动十秒、K = 3。
 * PmcodFunction on a version switch: clear, backfill without judgements, versions on the outlier records.
 */
class PmcodVersionSwitchTest {

    private static final long T0 = 1_646_092_800L;
    private static final long E = T0 + 600;              // 换版本的第一个点 / first point of version 2
    static final List<ScoreEvent> SCORES = Collections.synchronizedList(new ArrayList<>());

    static final class Sink implements SinkFunction<ScoreEvent> {
        private static final long serialVersionUID = 1L;

        @Override
        public void invoke(ScoreEvent v, Context c) {
            SCORES.add(v);
        }
    }

    private static DevicePoint point(long ts, double v, Integer version) {
        DevicePoint dp = new DevicePoint("G", new McodPoint(new double[]{v, 0, 0, 0, 0}, ts * 1000L, 0, ts, false), new boolean[5]);
        dp.setVersion(version);
        return dp;
    }

    /** 旧版本的点在 0 附近，新版本在 10 附近；E + 30 的点是新版本里的离群点 / old points near 0, new near 10. */
    private static List<DevicePoint> input(boolean loopOn, boolean withBackfill) {
        List<DevicePoint> in = new ArrayList<>();
        for (long t = T0; t < E + 300; t += 10) {
            double old = 0.01 * ((t / 10) % 5);
            double v = t < E ? old : (t == E + 30 ? -40.0 : 10.0 + old);
            DevicePoint dp = point(t, v, loopOn ? (t < E ? 1 : 2) : null);
            if (loopOn && withBackfill && t == E) {
                int n = 6;
                long[] ts = new long[n];
                double[][] xn = new double[n][];
                boolean[][] ce = new boolean[n][];
                for (int i = 0; i < n; i++) {
                    ts[i] = E - 60 + 10L * i;
                    double nv = ts[i] == E - 30 ? 50.0 : 10.0 + 0.01 * ((ts[i] / 10) % 5);   // E − 30 在新参数下是孤点 / isolated
                    xn[i] = new double[]{nv, 0, 0, 0, 0};
                    ce[i] = new boolean[5];
                }
                dp.setBackfill(new Backfill(ts, xn, ce));
            }
            in.add(dp);
        }
        return in;
    }

    private static List<ScoreEvent> run(List<DevicePoint> in) throws Exception {
        SCORES.clear();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        OutputTag<MonitoringSnapshot> mon = new OutputTag<MonitoringSnapshot>("m2-monitoring") { };
        SingleOutputStreamOperator<ScoreEvent> scored = env.fromCollection(in)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<DevicePoint>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((dp, ts) -> dp.getPoint().arrival))
                .keyBy(DevicePoint::getDevice)
                .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(10)))
                .process(new PmcodFunction(1.0, 3, 10, mon));
        scored.addSink(new Sink());
        env.execute("pmcod-version-switch");
        return new ArrayList<>(SCORES);
    }

    private static boolean judged(List<ScoreEvent> s, long roundTs, long windowEnd) {
        return s.stream().anyMatch(e -> e.getRoundTs() == roundTs && e.getWindowEnd() == windowEnd);
    }

    @Test
    @DisplayName("换版本：回填使新版本第一点有邻居，回填的点不发判定，离群记录带版本 / backfill, no judgements, versions")
    void versionSwitch() throws Exception {
        List<ScoreEvent> bf = run(input(true, true));
        List<ScoreEvent> noBf = run(input(true, false));
        List<ScoreEvent> off = run(input(false, false));
        long firstWindow = E + 10;
        assertFalse(judged(bf, E, firstWindow), "有回填：E 在新参数的旧点中有邻居 / with backfill E has neighbours");
        assertTrue(judged(noBf, E, firstWindow), "无回填：清空之后 E 孤立 / without backfill E is isolated");
        assertTrue(judged(off, E, firstWindow), "回路关闭：E 远离旧点 / loop off: E is far from the old points");
        for (ScoreEvent e : bf) {
            if (e.getWindowEnd() > E) {
                assertTrue(e.getRoundTs() >= E, "回填的点不发判定 / backfilled points are not judged: " + e.getRoundTs());
            }
            assertEquals(Integer.valueOf(e.getRoundTs() < E ? 1 : 2), e.getVersion());
        }
        assertTrue(judged(bf, E + 30, E + 40), "新版本里的离群点照常判定 / a new-version outlier is judged");
        assertTrue(bf.stream().anyMatch(e -> e.getRoundTs() < E), "换版本之前的判定照常 / judgements before the switch");
        for (ScoreEvent e : off) {
            assertEquals(null, e.getVersion());
        }
    }
}
