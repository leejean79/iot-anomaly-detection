package com.leejean.m3;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 评估路径成批推理的一致性测试：每个窗口的 WMSE 必须与逐窗推理相差不超过 1e-6。
 * Equality test for batched evaluation: each window's WMSE must match per-window inference
 * within 1e-6.
 */
class M3EvalBatchTest {

    private static final double TOL = 1e-6;
    private static final double[] WEIGHTS = {1.0, 1.0, 1.0, 0.5, 0.5};

    /**
     * 生产形状：隐藏层 60、窗口 60、早停集 288 窗（4 组满 64 加 32 窗的末组），逆序目标开启。
     * 此前一次原生崩溃只在生产形状下出现，小形状验证不出，故这里用生产形状。
     * Production shape, including a short tail group; an earlier native crash only showed at this shape.
     */
    @Test
    void batchedEvaluationMatchesPerWindowAtProductionShape() {
        assertMatchesPerWindow(60, 60, 288, true);
    }

    /** 逆序目标关闭，窗口数不是 64 的整数倍。/ Reverse target off, count not a multiple of 64. */
    @Test
    void batchedEvaluationMatchesPerWindowWithoutReverseTarget() {
        assertMatchesPerWindow(16, 20, 70, false);
    }

    private static void assertMatchesPerWindow(int hidden, int windowLen, int nWindows, boolean reverse) {
        LstmAutoEncoder ae = new LstmAutoEncoder(5, hidden, windowLen, reverse, 0.001, 0.0);
        // 先训练一轮，让权重离开初始值，避免输出近乎常数而掩盖顺序错误。
        // Train one epoch first so a time-order bug cannot hide behind near-constant output.
        ae.trainEpoch(windows(128, windowLen, 1L), null, 64);
        double[][][] data = windows(nWindows, windowLen, 2L);

        double[] batched = M3Training.perWindowLosses(ae, data, WEIGHTS);

        WeightedMseLoss lossCalc = new WeightedMseLoss(5, WEIGHTS);
        double sum = 0.0;
        for (int w = 0; w < nWindows; w++) {
            double expected = lossCalc.compute(data[w], ae.reconstruct(data[w]), null, windowLen).wmse;
            assertEquals(expected, batched[w], TOL, "窗口 " + w + " 的 WMSE 与逐窗推理不一致");
            sum += expected;
        }
        assertEquals(sum / nWindows, M3Training.evaluateLoss(ae, data, WEIGHTS), TOL);
    }

    /** 五通道正弦加噪声，各窗口相位不同。/ Five noisy sinusoids with a per-window phase. */
    private static double[][][] windows(int n, int len, long seed) {
        Random rnd = new Random(seed);
        double[][][] data = new double[n][len][5];
        for (int w = 0; w < n; w++) {
            double phase = rnd.nextDouble() * 2 * Math.PI;
            for (int t = 0; t < len; t++) {
                for (int c = 0; c < 5; c++) {
                    data[w][t][c] = Math.sin(phase + 0.2 * t + c) + 0.1 * rnd.nextGaussian();
                }
            }
        }
        return data;
    }
}
