package com.leejean.m3;

import com.leejean.m1.Channels;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 补遗四（把训练移出任务线程）的单元测试：结果只在任务线程上收回、缓冲的顺序与上限（丢最新）、分批排空、
 * 输出水位线保持、从检查点恢复后重新提交、关闭时中断，以及异步路径与纯函数直接计算的逐位等值。
 * Unit tests for addendum 4: results collected on the task thread only, buffer order and cap (drop newest),
 * batched draining, the held output watermark, resubmission after restore, interruption on close, and bit-exact
 * equality between the asynchronous path and the pure functions computed directly.
 *
 * <p>测试用算子测试夹具驱动 {@link M3Operator}；处理时间由夹具控制，训练线程是真实线程。用
 * {@link M3TrainExecutor#testGate} 让一次训练确定地停在「进行中」。
 * Drives M3Operator through the operator test harness; processing time is controlled by the harness while the
 * training thread is real. The test gate holds a training task in flight deterministically.
 */
class M3AsyncTrainingTest {

    private static final int ROUNDS_PER_DAY = 40;
    private static final int TRAIN_DAYS = 2;
    private static final int ES_DAYS = 1;
    private static final int THRESH_DAYS = 1;
    private static final int WINDOW = 8;
    private static final int HIDDEN = 12;
    private static final int MAX_EPOCHS = 30;
    private static final int PATIENCE = 3;
    private static final double Z = 3.0;
    private static final int TO_TRAINING = (TRAIN_DAYS + ES_DAYS + THRESH_DAYS) * ROUNDS_PER_DAY;
    /** 缓冲上限 = (标定天数 + 2) × 每天轮数 / buffer cap. */
    private static final int CAP = (THRESH_DAYS + 2) * ROUNDS_PER_DAY;
    private static final long START_TS = 1_000_000L;

    @AfterEach
    void openGate() {
        M3TrainExecutor.testGate = null;
    }

    private static double[] ones() {
        double[] w = new double[Channels.N_DET];
        Arrays.fill(w, 1.0);
        return w;
    }

    private static M3Function fn(int maxEpochs, int drainBatch) {
        return fn(maxEpochs, drainBatch, PATIENCE);
    }

    private static M3Function fn(int maxEpochs, int drainBatch, int patience) {
        return new M3Function(TRAIN_DAYS, ES_DAYS, THRESH_DAYS, WINDOW, Z, ones(), maxEpochs, patience,
                HIDDEN, 1, true, 0.001, 1.0, null, ROUNDS_PER_DAY,
                1, drainBatch, M3Function.DEFAULT_POLL_INTERVAL_SEC);
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> harness(
            M3Function f) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new M3Operator(f), AnnotatedRound::getDevice, TypeInformation.of(String.class));
    }

    /** 确定性的一串轮（与 M3StateMachineTest 的生成方式相同）/ a deterministic sequence of rounds. */
    private static List<AnnotatedRound> rounds(int n, long seed) {
        Random rnd = new Random(seed);
        List<AnnotatedRound> list = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            long ts = START_TS + i * 10L;
            double[] x = new double[Channels.N_DET];
            for (int c = 0; c < x.length; c++) {
                x[c] = Math.sin((ts / 10.0 + c) * 0.3) * 0.2 + rnd.nextGaussian() * 0.01;
            }
            list.add(new AnnotatedRound("E", ts, x, false, new boolean[Channels.N_DET], false, ts));
        }
        return list;
    }

    private static void feed(KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h,
                             List<AnnotatedRound> rs, int from, int to) throws Exception {
        for (int i = from; i < to; i++) {
            h.processElement(rs.get(i), rs.get(i).getTs() * 1000L);
        }
    }

    private static List<M3ScoreRecord> scores(
            KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h) {
        List<M3ScoreRecord> out = new ArrayList<>();
        for (Object o : h.getOutput()) {
            if (o instanceof StreamRecord && ((StreamRecord<?>) o).getValue() instanceof M3ScoreRecord) {
                out.add((M3ScoreRecord) ((StreamRecord<?>) o).getValue());
            }
        }
        return out;
    }

    private static void firePoll(KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h)
            throws Exception {
        h.setProcessingTime(h.getProcessingTime() + M3Function.DEFAULT_POLL_INTERVAL_SEC * 1000L + 1);
    }

    @Test
    @DisplayName("结果只在任务线程上收回：训练线程完成后，相位不变，直到定时器在任务线程上查询结果槽")
    void resultIsCollectedOnTheTaskThreadOnly() throws Exception {
        M3Function f = fn(MAX_EPOCHS, 200);
        List<AnnotatedRound> rs = rounds(TO_TRAINING, 1);
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h = harness(f)) {
            h.open();
            feed(h, rs, 0, TO_TRAINING);
            f.awaitTraining("E", 300_000L);
            assertEquals(0, f.lastTrainModels, "训练线程已完成，但在任务线程查询之前不得写入状态");
            firePoll(h);
            assertEquals(1, f.lastTrainModels, "定时器触发后应收回结果");
            assertSame(Thread.currentThread(), f.lastApplyThread, "结果必须在任务线程（夹具线程）上写入状态");
        }
    }

    @Test
    @DisplayName("缓冲：按到达顺序保留，满了丢最新并计数；排空后评分覆盖的正是缓冲里最早的那些轮")
    void bufferKeepsOrderAndDropsNewest() throws Exception {
        M3TrainExecutor.testGate = new CountDownLatch(1);
        M3Function f = fn(MAX_EPOCHS, 200);
        int extra = 5;
        List<AnnotatedRound> rs = rounds(TO_TRAINING + CAP + extra, 2);
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h = harness(f)) {
            h.open();
            feed(h, rs, 0, TO_TRAINING + CAP + extra);
            assertEquals(CAP, f.bufferedRounds("E"), "缓冲应停在上限 " + CAP);
            assertTrue(scores(h).isEmpty(), "训练进行中不得出分");
            M3TrainExecutor.testGate.countDown();
            M3StateMachineTest.settle(h, f, "E");
            List<M3ScoreRecord> s = scores(h);
            assertEquals(CAP / WINDOW, s.size(), "缓冲的 " + CAP + " 轮应恰好组成 " + CAP / WINDOW + " 个窗口");
            for (int k = 0; k < s.size(); k++) {
                long expectedEnd = rs.get(TO_TRAINING + (k + 1) * WINDOW - 1).getWindowEnd();
                assertEquals(expectedEnd, s.get(k).getWindowEnd(), "第 " + k + " 个窗口的顺序或内容不对");
            }
        }
    }

    @Test
    @DisplayName("排空分批：每次最多排空 drainBatch 轮，排空节拍由处理时间定时器推进")
    void drainsInBatches() throws Exception {
        M3TrainExecutor.testGate = new CountDownLatch(1);
        int batch = 2 * WINDOW;
        M3Function f = fn(MAX_EPOCHS, batch);
        List<AnnotatedRound> rs = rounds(TO_TRAINING + 4 * batch, 3);
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h = harness(f)) {
            h.open();
            feed(h, rs, 0, TO_TRAINING + 4 * batch);
            M3TrainExecutor.testGate.countDown();
            f.awaitTraining("E", 300_000L);
            firePoll(h);                                   // 收回结果并排空第一批 / collect and drain batch 1
            assertEquals(2, scores(h).size(), "第一批 " + batch + " 轮 = 2 个窗口");
            assertEquals(3 * batch, f.bufferedRounds("E"));
            h.setProcessingTime(h.getProcessingTime() + 1);
            assertEquals(4, scores(h).size(), "下一个节拍再排空一批");
            h.setProcessingTime(h.getProcessingTime() + 1);
            h.setProcessingTime(h.getProcessingTime() + 1);
            assertEquals(8, scores(h).size());
            assertEquals(0, f.bufferedRounds("E"));
        }
    }

    @Test
    @DisplayName("水位线保持：缓冲非空时输出水位线停在最早缓冲轮之前，排空后恢复跟随输入；没有评分成为迟到记录")
    void watermarkIsHeldWhileBuffered() throws Exception {
        M3TrainExecutor.testGate = new CountDownLatch(1);
        M3Function f = fn(MAX_EPOCHS, 200);
        List<AnnotatedRound> rs = rounds(TO_TRAINING + 4 * WINDOW, 4);
        long headMs = rs.get(TO_TRAINING).getTs() * 1000L;
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h = harness(f)) {
            h.open();
            feed(h, rs, 0, TO_TRAINING + 4 * WINDOW);
            long inputWm = rs.get(rs.size() - 1).getTs() * 1000L;
            h.processWatermark(new Watermark(inputWm));
            assertEquals(headMs - 1, lastWatermark(h), "输出水位线应停在缓冲头之前一毫秒");
            M3TrainExecutor.testGate.countDown();
            M3StateMachineTest.settle(h, f, "E");
            assertEquals(inputWm, lastWatermark(h), "排空后输出水位线应恢复为输入水位线");
            long wm = Long.MIN_VALUE;
            int scored = 0;
            for (Object o : h.getOutput()) {
                if (o instanceof Watermark) {
                    wm = ((Watermark) o).getTimestamp();
                } else if (o instanceof StreamRecord && ((StreamRecord<?>) o).getValue() instanceof M3ScoreRecord) {
                    assertTrue(((StreamRecord<?>) o).getTimestamp() > wm, "评分记录不得落在已发出的水位线之后");
                    scored++;
                }
            }
            assertEquals(4, scored);
        }
    }

    private static long lastWatermark(
            KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h) {
        long wm = Long.MIN_VALUE;
        for (Object o : h.getOutput()) {
            if (o instanceof Watermark) {
                wm = ((Watermark) o).getTimestamp();
            }
        }
        return wm;
    }

    @Test
    @DisplayName("重启：训练中途做检查点、恢复后重新提交训练，评分与未被打断的一次逐位相同，缓冲不丢")
    void restoreResubmitsAndMatchesUninterruptedRun() throws Exception {
        List<AnnotatedRound> rs = rounds(TO_TRAINING + 6 * WINDOW, 5);
        int mid = TO_TRAINING + 3 * WINDOW;

        // 对照：不打断 / reference: uninterrupted
        M3Function ref = fn(MAX_EPOCHS, 200);
        List<M3ScoreRecord> expected;
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h = harness(ref)) {
            h.open();
            feed(h, rs, 0, rs.size());
            M3StateMachineTest.settle(h, ref, "E");
            expected = scores(h);
        }

        // 训练进行中做快照，然后关闭（中断训练）/ snapshot while training is in flight, then close
        M3TrainExecutor.testGate = new CountDownLatch(1);
        OperatorSubtaskState snap;
        M3Function first = fn(MAX_EPOCHS, 200);
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h = harness(first)) {
            h.open();
            feed(h, rs, 0, mid);
            assertEquals(mid - TO_TRAINING, first.bufferedRounds("E"));
            snap = h.snapshot(1L, 1L);
        }
        M3TrainExecutor.testGate = null;

        M3Function restored = fn(MAX_EPOCHS, 200);
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h = harness(restored)) {
            h.setup();
            h.initializeState(snap);
            h.open();
            assertEquals(mid - TO_TRAINING, restored.bufferedRounds("E"), "缓冲随检查点恢复");
            feed(h, rs, mid, rs.size());                   // 第一条元素即触发重新提交 / first element resubmits
            M3StateMachineTest.settle(h, restored, "E");
            List<M3ScoreRecord> got = scores(h);
            assertEquals(expected.size(), got.size());
            for (int i = 0; i < got.size(); i++) {
                assertEquals(expected.get(i).getWindowEnd(), got.get(i).getWindowEnd());
                assertEquals(0, Double.compare(expected.get(i).getMainScore(), got.get(i).getMainScore()),
                        "第 " + i + " 窗主分应逐位相同");
                assertEquals(0, Double.compare(expected.get(i).getWmse(), got.get(i).getWmse()));
            }
        }
    }

    @Test
    @DisplayName("关闭：训练进行中关闭算子，训练线程被中断并退出")
    void closeInterruptsTraining() throws Exception {
        M3Function f = fn(1_000_000, 200, 1_000_000);      // 实际上不会自然结束 / effectively never ends
        List<AnnotatedRound> rs = rounds(TO_TRAINING, 6);
        KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h = harness(f);
        h.open();
        feed(h, rs, 0, TO_TRAINING);
        assertTrue(waitFor(() -> trainThreads() > 0, 30_000L), "训练线程应已启动");
        long t0 = System.currentTimeMillis();
        h.close();
        assertTrue(waitFor(() -> trainThreads() == 0, 30_000L), "关闭后训练线程应在 30 秒内退出");
        assertTrue(System.currentTimeMillis() - t0 < 30_000L);
    }

    private static int trainThreads() {
        int n = 0;
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().startsWith("m3-train-") && t.isAlive()) {
                n++;
            }
        }
        return n;
    }

    private static boolean waitFor(java.util.function.BooleanSupplier cond, long timeoutMs)
            throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(20);
        }
        return cond.getAsBoolean();
    }

    @Test
    @DisplayName("等值：异步路径的训练、阈值与评分，与在本线程上直接调用纯函数的结果逐位相同")
    void asyncPathEqualsPureFunctions() throws Exception {
        int online = 5 * WINDOW;
        List<AnnotatedRound> rs = rounds(TO_TRAINING + online, 7);
        M3Function f = fn(MAX_EPOCHS, 200);
        List<M3ScoreRecord> got;
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h = harness(f)) {
            h.open();
            feed(h, rs, 0, rs.size());
            M3StateMachineTest.settle(h, f, "E");
            got = scores(h);
        }

        // 直接计算：按收集态的规则分三段（不重叠窗口，按完成时的轮数归段），训练、标定、逐窗评分。
        // Direct computation: segment as the collecting phase does, then train, calibrate and score.
        List<double[][]> tr = new ArrayList<>(), es = new ArrayList<>(), th = new ArrayList<>();
        List<boolean[][]> trM = new ArrayList<>(), thM = new ArrayList<>();
        for (int end = WINDOW; end <= TO_TRAINING; end += WINDOW) {
            double[][] w = new double[WINDOW][];
            boolean[][] m = new boolean[WINDOW][];
            for (int t = 0; t < WINDOW; t++) {
                w[t] = rs.get(end - WINDOW + t).getXNorm().clone();
                m[t] = WeightedMseLoss.buildMask(rs.get(end - WINDOW + t).getCensoredMask());
            }
            if (end <= TRAIN_DAYS * ROUNDS_PER_DAY) {
                tr.add(w);
                trM.add(m);
            } else if (end <= (TRAIN_DAYS + ES_DAYS) * ROUNDS_PER_DAY) {
                es.add(w);
            } else {
                th.add(w);
                thM.add(m);
            }
        }
        M3Training.Config cfg = new M3Training.Config(Channels.N_DET, HIDDEN, WINDOW, 1, MAX_EPOCHS, PATIENCE,
                ones(), true, 0.001, 1.0);
        M3Training.Result r = M3Training.train(cfg, tr.toArray(new double[0][][]),
                trM.toArray(new boolean[0][][]), es.toArray(new double[0][][]), null);
        M3Scorer scorer = M3Function.calibrate(r.model, th.toArray(new double[0][][]),
                thM.toArray(new boolean[0][][]), ones(), Z);
        assertEquals(r.epochs, f.lastTrainEpochs, "早停轮数应相同");

        WeightedMseLoss loss = new WeightedMseLoss(Channels.N_DET, ones());
        assertEquals(online / WINDOW, got.size());
        for (int k = 0; k < got.size(); k++) {
            double[][] w = new double[WINDOW][];
            boolean[][] m = new boolean[WINDOW][];
            for (int t = 0; t < WINDOW; t++) {
                AnnotatedRound a = rs.get(TO_TRAINING + k * WINDOW + t);
                w[t] = a.getXNorm().clone();
                m[t] = WeightedMseLoss.buildMask(a.getCensoredMask());
            }
            WeightedMseLoss.LossResult lr = loss.compute(w, r.model.reconstruct(w), m, WINDOW);
            M3Scorer.ScoreResult sr = scorer.score(lr.wmse, lr.perChannelMse);
            assertEquals(0, Double.compare(lr.wmse, got.get(k).getWmse()), "第 " + k + " 窗 WMSE 应逐位相同");
            assertEquals(0, Double.compare(sr.mainScore, got.get(k).getMainScore()), "第 " + k + " 窗主分应逐位相同");
            assertEquals(0, Double.compare(sr.mahaScore, got.get(k).getMahaScore()));
        }
        assertFalse(got.isEmpty());
    }
}
