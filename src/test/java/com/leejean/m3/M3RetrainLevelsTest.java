package com.leejean.m3;

import com.leejean.m1.ChannelTransform;
import com.leejean.m1.Channels;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m1.RobustScalerFunction;
import com.leejean.m6.AdaptCommand;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M6b 决定十七的单元测试：一级重训（生效前侧与生效后侧、热启动与冷启动）的原子切换与逐位可复现，二级的静默与标记。
 * Unit tests for decision 17: atomic level-1 switch (before/after side, warm/cold) and level-2 silence and marking.
 *
 * <p>每天四十轮、窗口八轮（每天五窗）；一天的事件时间为四十乘十秒。冷启动收集四天，之后在线。
 * Forty rounds a day, eight-round windows; a day is 400 s of event time; four collection days, then online.
 */
class M3RetrainLevelsTest {

    private static final int ROUNDS_PER_DAY = 40;
    private static final int WINDOW = 8;
    private static final int TO_TRAINING = (2 + 1 + 1) * ROUNDS_PER_DAY;
    private static final long DAY = ROUNDS_PER_DAY * 10L;
    private static final long START_TS = 1_000_000L;
    private static final ChannelTransform[] TF = ChannelTransform.defaultTable();
    private static final boolean[] NO_MISSING = new boolean[Channels.N_DET];
    private static final OutputTag<MonitoringSnapshot> MON = new OutputTag<MonitoringSnapshot>("m3-mon") { };

    private static final ParamSet V1 = new ParamSet(1, new double[]{20.0, 40.0, 101000.0, 1000.0, 5.0},
            new double[]{1.5, 10.0, 900.0, 1100.0, 7.0}, new boolean[Channels.N_DET]);
    private static final ParamSet V2 = new ParamSet(2, new double[]{21.0, 35.0, 101500.0, 2000.0, 4.5},
            new double[]{2.0, 6.0, 400.0, 2300.0, 7.5}, new boolean[Channels.N_DET]);

    private static M3Function fn(int days, boolean warm, boolean after) {
        double[] w = new double[Channels.N_DET];
        Arrays.fill(w, 1.0);
        return new M3Function(2, 1, 1, WINDOW, 3.0, w, 20, 3, 12, 1, true, 0.001, 1.0, MON, ROUNDS_PER_DAY,
                1, 200, M3Function.DEFAULT_POLL_INTERVAL_SEC).withRetrain(days, warm, after);
    }

    private static long ts(int i) {
        return START_TS + i * 10L;
    }

    /** n 条回路打开的轮；从 switchAt 起数据接入按版本二标准化；cmdAt 处附命令 / n loop-on rounds with an optional command. */
    private static List<AnnotatedRound> rounds(int n, int switchAt, int cmdAt, AdaptCommand cmd) {
        Random rnd = new Random(9);
        List<AnnotatedRound> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            double s = Math.sin(i * 0.3);
            double[] raw = {20.0 + s + rnd.nextGaussian() * 0.05, 40.0 + 5 * s + rnd.nextGaussian() * 0.2,
                    101000.0 + 300 * s + rnd.nextGaussian() * 5, 1000.0 + 400 * s + rnd.nextGaussian() * 10,
                    100.0 + 50 * s + rnd.nextGaussian()};
            ParamSet p = i >= switchAt ? V2 : V1;
            double[] xn = RobustScalerFunction.normalize(raw, NO_MISSING, p.median, p.scale, p.bypass, TF);
            AnnotatedRound r = new AnnotatedRound("E", ts(i), xn, i % 37 == 5, new boolean[Channels.N_DET], false, ts(i));
            r.setRaw(raw);
            r.setVersion(p.version);
            if (i == 0 || i == switchAt) {
                r.setParamSet(p);
            }
            if (i == cmdAt) {
                r.setContextCommand(cmd);
            }
            out.add(r);
        }
        return out;
    }

    private static AdaptCommand command(int level, long eff) {
        AdaptCommand c = new AdaptCommand();
        c.device = "E";
        c.level = level;
        c.effectiveAt = eff;
        c.decidedAt = eff;
        c.seq = 2;
        c.raw = "{}";
        return c;
    }

    /** 运行结果：评分记录与上下文通道的监测快照 / the run's score records and monitoring snapshots. */
    private static final class Run {
        final List<M3ScoreRecord> scores = new ArrayList<>();
        final List<MonitoringSnapshot> snaps = new ArrayList<>();
    }

    /**
     * 喂入全部轮。eager 为真时，每次进入训练态都立即等训练完成并收回结果（训练「很快」）；为假时先把轮全部喂完，再收回结果
     * 并排空（训练「很慢」，期间的轮全部进缓冲）。两种节奏的输出应逐位相同。
     * eager collects each training right away; otherwise all rounds are fed first and buffered. Outputs must match.
     */
    private static Run run(M3Function f, List<AnnotatedRound> rs, boolean eager) throws Exception {
        Run run = new Run();
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h =
                     new KeyedOneInputStreamOperatorTestHarness<>(new M3Operator(f), AnnotatedRound::getDevice,
                             TypeInformation.of(String.class))) {
            h.open();
            for (int i = 0; i < rs.size(); i++) {
                h.processElement(rs.get(i), rs.get(i).getTs() * 1000L);
                if (eager || i == TO_TRAINING - 1) {
                    settle(f, h);
                }
            }
            for (int k = 0; k < 4; k++) {
                settle(f, h);
            }
            for (Object o : h.getOutput()) {
                if (o instanceof StreamRecord && ((StreamRecord<?>) o).getValue() instanceof M3ScoreRecord) {
                    run.scores.add((M3ScoreRecord) ((StreamRecord<?>) o).getValue());
                }
            }
            if (h.getSideOutput(MON) != null) {
                for (StreamRecord<MonitoringSnapshot> s : h.getSideOutput(MON)) {
                    run.snaps.add(s.getValue());
                }
            }
        }
        return run;
    }

    /** 等进行中的训练完成、收回结果并排空缓冲 / await any training, collect it and drain. */
    private static void settle(M3Function f,
                               KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h)
            throws Exception {
        f.awaitTraining("E", 300_000L);
        h.setProcessingTime(h.getProcessingTime() + M3Function.DEFAULT_POLL_INTERVAL_SEC * 1000L + 1);
        for (int k = 0; k < 20; k++) {
            h.setProcessingTime(h.getProcessingTime() + 1);
        }
    }

    private static void assertSameRuns(Run a, Run b) {
        assertEquals(a.scores.size(), b.scores.size());
        for (int k = 0; k < a.scores.size(); k++) {
            M3ScoreRecord x = a.scores.get(k);
            M3ScoreRecord y = b.scores.get(k);
            assertEquals(x.getWindowEnd(), y.getWindowEnd());
            assertEquals(Double.doubleToLongBits(x.getWmse()), Double.doubleToLongBits(y.getWmse()), "窗口 " + k);
            assertEquals(x.getVersion(), y.getVersion());
            assertEquals(x.getModel(), y.getModel());
        }
    }

    @Test
    @DisplayName("一级（生效前侧、热启动）：生效前的窗口由旧模型评分、版本一；之后由新模型评分、版本二、代数一；训练快慢不影响输出")
    void levelOneBeforeSideSwitchesAtomicallyAndReproducibly() throws Exception {
        int switchAt = TO_TRAINING + 2 * ROUNDS_PER_DAY;         // 在线两天后发生零级 / a level 0 after two online days
        int cmdAt = TO_TRAINING + 3 * ROUNDS_PER_DAY + 3;        // 再一天后一级生效（窗口中间）/ level 1 mid-window
        int n = cmdAt + 2 * ROUNDS_PER_DAY;
        long eff = ts(cmdAt);
        List<AnnotatedRound> rs = rounds(n, switchAt, cmdAt, command(1, eff));
        Run fast = run(fn(2, true, false), rs, true);
        Run slow = run(fn(2, true, false), rounds(n, switchAt, cmdAt, command(1, eff)), false);
        assertSameRuns(fast, slow);
        int before = 0;
        int after = 0;
        for (M3ScoreRecord r : fast.scores) {
            if (r.getWindowEnd() < eff) {
                before++;
                assertEquals(1, r.getVersion(), "切换前为旧训练版本");
                assertNull(r.getModel());
            } else {
                after++;
                assertEquals(2, r.getVersion(), "切换后为新训练版本（重训时的当前版本）");
                assertEquals(1, r.getModel());
            }
        }
        assertTrue(before > 0 && after > 0);
        // 生效时刻所在的窗口跨过切换：它由新模型评分（未满的部分按新版本重新标准化）/ the straddling window uses the new model
        assertEquals((n - TO_TRAINING) / WINDOW, fast.scores.size(), "每个在线窗口恰好一条评分，无丢失无重复");
    }

    @Test
    @DisplayName("一级（生效后侧、冷启动）：生效后 N 天内旧模型继续评分，收满后切换；训练快慢不影响输出")
    void levelOneAfterSideKeepsTheOldModelForNDays() throws Exception {
        int cmdAt = TO_TRAINING + 2 * ROUNDS_PER_DAY;
        int n = cmdAt + 4 * ROUNDS_PER_DAY;
        long eff = ts(cmdAt);
        Run fast = run(fn(2, false, true), rounds(n, n, cmdAt, command(1, eff)), true);
        Run slow = run(fn(2, false, true), rounds(n, n, cmdAt, command(1, eff)), false);
        assertSameRuns(fast, slow);
        boolean sawNew = false;
        for (M3ScoreRecord r : fast.scores) {
            if (r.getWindowEnd() < eff + 2 * DAY) {
                assertNull(r.getModel(), "收满 N 天之前仍是旧模型");
            } else if (r.getModel() != null) {
                sawNew = true;
                assertEquals(1, r.getVersion());
            }
        }
        assertTrue(sawNew, "收满之后切换到新模型");
    }

    @Test
    @DisplayName("二级：回到收集态，四天内上下文通道无评分并每窗发出重训中标记；新模型上线后代数为一")
    void levelTwoSilencesAndMarks() throws Exception {
        int cmdAt = TO_TRAINING + 2 * ROUNDS_PER_DAY;
        int n = cmdAt + TO_TRAINING + ROUNDS_PER_DAY;
        long eff = ts(cmdAt);
        Run r = run(fn(2, true, false), rounds(n, n, cmdAt, command(2, eff)), true);
        long markers = r.snaps.stream().filter(s -> Boolean.TRUE.equals(s.getM3Retraining())).count();
        assertEquals(TO_TRAINING / WINDOW, markers, "收集期每窗一条重训中标记");
        long newScores = 0;
        for (M3ScoreRecord s : r.scores) {
            if (s.getWindowEnd() >= eff) {
                assertTrue(s.getWindowEnd() >= ts(cmdAt + TO_TRAINING - 1), "二级收集期内静默");
                assertEquals(1, s.getModel());
                newScores++;
            }
        }
        assertEquals(ROUNDS_PER_DAY / WINDOW, newScores);
    }
}
