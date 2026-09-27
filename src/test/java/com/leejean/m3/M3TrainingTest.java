package com.leejean.m3;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 早停记录的口径测试：最后一次改善的轮次与最长平台长度（2026-09-27 裁决书第二节要求逐台上报）。
 * Early-stopping bookkeeping: the last-improvement epoch and the longest plateau.
 */
class M3TrainingTest {

    @Test
    void earlyStopBookkeepingIsConsistentWithTheStoppingRule() {
        int patience = 3;
        M3Training.Result r = M3Training.train(config(40, patience), data(64, 1L), null, data(16, 2L), null);
        assertTrue(r.bestEpoch >= 1 && r.bestEpoch <= r.epochs, "最后一次改善应落在已跑的轮次内");
        assertTrue(r.longestPlateau < patience, "长度达到耐心的平台必然触发停止，不可能被后续改善打断");
        if (r.epochs < 40) {
            assertEquals(r.epochs - patience, r.bestEpoch, "早停时，停止轮减去耐心就是最后一次改善的轮次");
        }
    }

    @Test
    void singleEpochRunHasItsOnlyEpochAsBest() {
        M3Training.Result r = M3Training.train(config(1, 3), data(64, 1L), null, data(16, 2L), null);
        assertEquals(1, r.epochs);
        assertEquals(1, r.bestEpoch, "第一轮总是改善（相对初始的最大值）");
        assertEquals(0, r.longestPlateau);
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
