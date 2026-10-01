package com.leejean.m3;

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 早停记录的口径测试：最后一次改善的轮次与最长平台长度（2026-09-27 裁决书第二节要求逐台上报）。
 * Early-stopping bookkeeping: the last-improvement epoch and the longest plateau.
 */
class M3TrainingTest {

    @Test
    void earlyStopBookkeepingIsConsistentWithTheStoppingRule() throws Exception {
        int patience = 3;
        M3Training.Result r = M3Training.train(config(40, patience), data(64, 1L), null, data(16, 2L), null);
        assertTrue(r.bestEpoch >= 1 && r.bestEpoch <= r.epochs, "最后一次改善应落在已跑的轮次内");
        assertTrue(r.longestPlateau < patience, "长度达到耐心的平台必然触发停止，不可能被后续改善打断");
        if (r.epochs < 40) {
            assertEquals(r.epochs - patience, r.bestEpoch, "早停时，停止轮减去耐心就是最后一次改善的轮次");
        }
    }

    @Test
    void singleEpochRunHasItsOnlyEpochAsBest() throws Exception {
        M3Training.Result r = M3Training.train(config(1, 3), data(64, 1L), null, data(16, 2L), null);
        assertEquals(1, r.epochs);
        assertEquals(1, r.bestEpoch, "第一轮总是改善（相对初始的最大值）");
        assertEquals(0, r.longestPlateau);
    }

    /**
     * 2026-10-01 裁决第三节第 1 条：训练中被中断，须在一个 epoch 之内返回。从另一个线程发出中断（与 Flink 取消
     * 任务时的做法相同），断言训练以 InterruptedException 结束，且中断之后至多再跑完当前这一个 epoch。
     * 耐心设为无穷大、上限设得很高，保证训练只可能因中断而结束。
     * Training interrupted from another thread (as Flink does on cancel) must return within one epoch.
     */
    @Test
    void interruptStopsTrainingWithinOneEpoch() throws Exception {
        M3Training.Config cfg = config(100_000, Integer.MAX_VALUE);
        AtomicInteger lastEpoch = new AtomicInteger();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        CountDownLatch firstEpochDone = new CountDownLatch(1);
        Thread trainer = new Thread(() -> {
            try {
                M3Training.train(cfg, data(64, 1L), null, data(16, 2L), (epoch, tl, es, sec) -> {
                    lastEpoch.set(epoch);
                    firstEpochDone.countDown();
                });
            } catch (Throwable t) {
                thrown.set(t);
            }
        });
        trainer.start();
        assertTrue(firstEpochDone.await(60, TimeUnit.SECONDS), "第一个 epoch 应在 60 秒内完成");
        int epochAtInterrupt = lastEpoch.get();
        trainer.interrupt();
        trainer.join(60_000);
        assertFalse(trainer.isAlive(), "中断后训练线程应当结束");
        assertTrue(thrown.get() instanceof InterruptedException,
                "应以 InterruptedException 结束，实际：" + thrown.get());
        assertTrue(lastEpoch.get() <= epochAtInterrupt + 1,
                "中断后至多再跑完当前 epoch：中断时 " + epochAtInterrupt + "，结束时 " + lastEpoch.get());
    }

    private static M3Training.Config config(int maxEpochs, int patience) {
        return new M3Training.Config(5, 8, 12, 16, maxEpochs, patience,
                new double[]{1, 1, 1, 1, 1}, true, 0.001, 0.0);
    }

    private static double[][][] data(int n, long seed) {
        Random rnd = new Random(seed);
        double[][][] d = new double[n][12][5];
        for (double[][] w : d) {
            double phase = rnd.nextDouble() * 6.28;
            for (int t = 0; t < 12; t++) {
                for (int c = 0; c < 5; c++) {
                    w[t][c] = Math.sin(phase + 0.3 * t + c) + 0.1 * rnd.nextGaussian();
                }
            }
        }
        return d;
    }
}
