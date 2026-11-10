package com.leejean.m3;

import com.leejean.m1.ChannelTransform;
import com.leejean.m1.Channels;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m1.RobustScalerFunction;
import com.leejean.m6.ParamSet;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * M6b 选项一（决定十三）的单元测试：上下文模块缓存数据接入模块附来的参数集，按训练版本从原始值自己标准化，输出训练版本号。
 * 三条：回路打开且无版本变化时与回路关闭逐位相同；训练之后的零级版本变化不改变上下文评分；训练版本取提交训练时的当前版本。
 * Unit tests for option 1 (decision 13): parameter-set caching, training-version standardization, version on output.
 */
class M3PinnedStandardizationTest {

    private static final int ROUNDS_PER_DAY = 40;
    private static final int WINDOW = 8;
    private static final int TO_TRAINING = (2 + 1 + 1) * ROUNDS_PER_DAY;
    private static final int ONLINE = 10 * WINDOW;
    private static final long START_TS = 1_000_000L;
    private static final ChannelTransform[] TF = ChannelTransform.defaultTable();
    private static final boolean[] NO_MISSING = new boolean[Channels.N_DET];

    private static final ParamSet V1 = new ParamSet(1, new double[]{20.0, 40.0, 101000.0, 1000.0, 5.0},
            new double[]{1.5, 10.0, 900.0, 1100.0, 7.0}, new boolean[Channels.N_DET]);
    private static final ParamSet V2 = new ParamSet(2, new double[]{21.0, 35.0, 101500.0, 2000.0, 4.5},
            new double[]{2.0, 6.0, 400.0, 2300.0, 7.5}, new boolean[Channels.N_DET]);

    private static final OutputTag<MonitoringSnapshot> MON = new OutputTag<MonitoringSnapshot>("m3-mon") { };

    private static M3Function fn(boolean shadow) {
        double[] w = new double[Channels.N_DET];
        Arrays.fill(w, 1.0);
        return new M3Function(2, 1, 1, WINDOW, 3.0, w, 20, 3, 12, 1, true, 0.001, 1.0, shadow ? MON : null, ROUNDS_PER_DAY,
                1, 200, M3Function.DEFAULT_POLL_INTERVAL_SEC).withShadowScoring(shadow);
    }

    /** 确定性的原始值序列 / deterministic raw rounds. */
    private static List<double[]> raws(int n) {
        Random rnd = new Random(5);
        List<double[]> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            double s = Math.sin(i * 0.3);
            out.add(new double[]{20.0 + s + rnd.nextGaussian() * 0.05, 40.0 + 5 * s + rnd.nextGaussian() * 0.2,
                    101000.0 + 300 * s + rnd.nextGaussian() * 5, 1000.0 + 400 * s + rnd.nextGaussian() * 10,
                    100.0 + 50 * s + rnd.nextGaussian()});
        }
        return out;
    }

    /**
     * 一条轮。loopOn 为假时只带标准化值（回路关闭）；为真时另带原始值、版本号，attach 为真时附参数集。xNorm 一律是数据接入
     * 模块按 scaleWith 给出的标准化值。/ One round; xNorm is what the data-access module emits under scaleWith.
     */
    private static AnnotatedRound round(int i, double[] raw, ParamSet scaleWith, boolean loopOn, boolean attach) {
        long ts = START_TS + i * 10L;
        double[] xn = RobustScalerFunction.normalize(raw, NO_MISSING, scaleWith.median, scaleWith.scale, scaleWith.bypass, TF);
        AnnotatedRound r = new AnnotatedRound("E", ts, xn, false, new boolean[Channels.N_DET], false, ts);
        if (loopOn) {
            r.setRaw(raw.clone());
            r.setVersion(scaleWith.version);
            if (attach) {
                r.setParamSet(scaleWith);
            }
        }
        return r;
    }

    /** 喂完收集段、收回训练结果、再喂在线段，返回评分记录 / feed, collect the result, feed online rounds. */
    private static List<M3ScoreRecord> run(List<AnnotatedRound> rs) throws Exception {
        return run(rs, false, null);
    }

    private static List<M3ScoreRecord> run(List<AnnotatedRound> rs, boolean shadow, List<MonitoringSnapshot> snaps)
            throws Exception {
        M3Function f = fn(shadow);
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h =
                     new KeyedOneInputStreamOperatorTestHarness<>(new M3Operator(f), AnnotatedRound::getDevice,
                             TypeInformation.of(String.class))) {
            h.open();
            for (int i = 0; i < TO_TRAINING; i++) {
                h.processElement(rs.get(i), rs.get(i).getTs() * 1000L);
            }
            f.awaitTraining("E", 300_000L);
            h.setProcessingTime(h.getProcessingTime() + M3Function.DEFAULT_POLL_INTERVAL_SEC * 1000L + 1);
            for (int i = TO_TRAINING; i < rs.size(); i++) {
                h.processElement(rs.get(i), rs.get(i).getTs() * 1000L);
            }
            List<M3ScoreRecord> out = new ArrayList<>();
            for (Object o : h.getOutput()) {
                if (o instanceof StreamRecord && ((StreamRecord<?>) o).getValue() instanceof M3ScoreRecord) {
                    out.add((M3ScoreRecord) ((StreamRecord<?>) o).getValue());
                }
            }
            if (snaps != null && h.getSideOutput(MON) != null) {
                for (StreamRecord<MonitoringSnapshot> s : h.getSideOutput(MON)) {
                    if (s.getValue().getM3Phase() == 3) {
                        snaps.add(s.getValue());
                    }
                }
            }
            return out;
        }
    }

    private static void assertSameScores(List<M3ScoreRecord> a, List<M3ScoreRecord> b) {
        assertEquals(ONLINE / WINDOW, a.size());
        assertEquals(a.size(), b.size());
        for (int k = 0; k < a.size(); k++) {
            assertEquals(Double.doubleToLongBits(a.get(k).getMainScore()), Double.doubleToLongBits(b.get(k).getMainScore()),
                    "窗口 " + k + " 的主分不同");
            assertEquals(Double.doubleToLongBits(a.get(k).getWmse()), Double.doubleToLongBits(b.get(k).getWmse()));
        }
    }

    @Test
    @DisplayName("回路打开且版本不变：按参数集自己标准化，与回路关闭时的评分逐位相同，输出版本号为一")
    void loopOnWithoutVersionChangeEqualsLoopOff() throws Exception {
        List<double[]> raw = raws(TO_TRAINING + ONLINE);
        List<AnnotatedRound> off = new ArrayList<>();
        List<AnnotatedRound> on = new ArrayList<>();
        for (int i = 0; i < raw.size(); i++) {
            off.add(round(i, raw.get(i), V1, false, false));
            on.add(round(i, raw.get(i), V1, true, i == 0));
        }
        List<M3ScoreRecord> a = run(off);
        List<M3ScoreRecord> b = run(on);
        assertSameScores(a, b);
        assertNull(a.get(0).getVersion());
        assertEquals(1, b.get(0).getVersion());
    }

    @Test
    @DisplayName("训练之后的零级版本变化：数据接入改按版本二标准化，上下文评分仍按训练版本一，与不变时逐位相同")
    void levelZeroAfterTrainingDoesNotMoveContextScores() throws Exception {
        List<double[]> raw = raws(TO_TRAINING + ONLINE);
        List<AnnotatedRound> stay = new ArrayList<>();
        List<AnnotatedRound> switched = new ArrayList<>();
        List<AnnotatedRound> offSwitched = new ArrayList<>();
        for (int i = 0; i < raw.size(); i++) {
            boolean after = i >= TO_TRAINING;
            stay.add(round(i, raw.get(i), V1, true, i == 0));
            switched.add(round(i, raw.get(i), after ? V2 : V1, true, i == 0 || i == TO_TRAINING));
            offSwitched.add(round(i, raw.get(i), after ? V2 : V1, false, false));
        }
        List<M3ScoreRecord> a = run(stay);
        List<M3ScoreRecord> b = run(switched);
        assertSameScores(a, b);
        for (M3ScoreRecord r : b) {
            assertEquals(1, r.getVersion(), "输出版本号应为训练版本");
        }
        // 对照：第一批的做法（直接用数据接入的标准化值）在版本变化后评分改变，说明本测试对版本敏感
        List<M3ScoreRecord> c = run(offSwitched);
        assertNotEquals(Double.doubleToLongBits(a.get(0).getWmse()), Double.doubleToLongBits(c.get(0).getWmse()));
    }

    @Test
    @DisplayName("影子评分：正式评分不变；影子按当前版本标准化，等于第一批做法（直接用数据接入的标准化值）的评分")
    void shadowScoringRecordsTheCurrentVersionScore() throws Exception {
        List<double[]> raw = raws(TO_TRAINING + ONLINE);
        List<AnnotatedRound> switched = new ArrayList<>();
        List<AnnotatedRound> offSwitched = new ArrayList<>();
        for (int i = 0; i < raw.size(); i++) {
            boolean after = i >= TO_TRAINING;
            switched.add(round(i, raw.get(i), after ? V2 : V1, true, i == 0 || i == TO_TRAINING));
            offSwitched.add(round(i, raw.get(i), after ? V2 : V1, false, false));
        }
        List<MonitoringSnapshot> snaps = new ArrayList<>();
        List<M3ScoreRecord> formal = run(switched, true, snaps);
        List<M3ScoreRecord> plain = run(switched);
        List<M3ScoreRecord> batch1 = run(offSwitched);
        assertSameScores(plain, formal);
        assertEquals(formal.size(), snaps.size());
        for (int k = 0; k < snaps.size(); k++) {
            assertEquals(Double.doubleToLongBits(batch1.get(k).getWmse()), Double.doubleToLongBits(snaps.get(k).getM3ShadowWmse()));
            assertEquals(batch1.get(k).isAboveThreshold(), snaps.get(k).getM3ShadowAbove());
            assertEquals(formal.get(k).isAboveThreshold(), snaps.get(k).getM3Above());
        }
    }

    @Test
    @DisplayName("收集期内发生零级：训练版本取提交训练时的当前版本，全部收集窗口按它标准化")
    void trainingVersionIsTheCurrentVersionAtSubmission() throws Exception {
        List<double[]> raw = raws(TO_TRAINING + ONLINE);
        int sw = TO_TRAINING / 2;
        List<AnnotatedRound> mixed = new ArrayList<>();
        List<AnnotatedRound> allV2 = new ArrayList<>();
        for (int i = 0; i < raw.size(); i++) {
            mixed.add(round(i, raw.get(i), i >= sw ? V2 : V1, true, i == 0 || i == sw));
            allV2.add(round(i, raw.get(i), V2, false, false));
        }
        List<M3ScoreRecord> a = run(mixed);
        List<M3ScoreRecord> b = run(allV2);
        assertSameScores(a, b);
        assertEquals(2, a.get(0).getVersion());
    }
}
