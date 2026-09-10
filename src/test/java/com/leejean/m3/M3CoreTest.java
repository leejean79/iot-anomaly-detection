package com.leejean.m3;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * M3 核心组件单元测试（交接文档 §5 V-M3-2）：损失掩码、评分器标定、模型序列化/反序列化。
 * M3 core component unit tests (handover §5 V-M3-2): loss masking, scorer calibration,
 * model serialize/deserialize.
 */
class M3CoreTest {

    // ---- WeightedMseLoss 测试 / WeightedMseLoss tests ----

    @Test
    void lossMaskingZerosWeightForCensoredChannels() {
        // 有通道被删失（censored），其误差不应影响 WMSE / censored channels should not affect WMSE
        WeightedMseLoss loss = new WeightedMseLoss(3);
        double[][] input =  {{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}};
        double[][] output = {{1.1, 2.1, 3.1}, {4.1, 5.1, 6.1}};

        // 无掩码：全部通道有效 / no mask: all channels valid
        WeightedMseLoss.LossResult noMask = loss.compute(input, output, null, 2);
        assertTrue(noMask.wmse > 0, "WMSE should be positive with no mask");

        // 掩码第三通道无效 / mask channel 2 invalid
        boolean[][] masks = {{true, true, false}, {true, true, false}};
        WeightedMseLoss.LossResult withMask = loss.compute(input, output, masks, 2);
        assertTrue(withMask.wmse > 0);
        assertEquals(0.0, withMask.perChannelMse[2], 1e-12,
                "Masked channel should have zero per-channel MSE");
        assertTrue(withMask.perChannelMse[0] > 0, "Unmasked channel 0 should have positive error");
    }

    @Test
    void lossMaskingWithChannelWeights() {
        // 通道权重为零的通道不参与 WMSE（即使未被掩码）/ zero-weight channel excluded from WMSE
        double[] weights = {1.0, 1.0, 0.0};
        WeightedMseLoss loss = new WeightedMseLoss(3, weights);
        double[][] input =  {{1.0, 2.0, 100.0}};
        double[][] output = {{1.1, 2.1, 0.0}};

        WeightedMseLoss.LossResult result = loss.compute(input, output, null, 1);
        // 通道 0 误差 = 0.01, 通道 1 误差 = 0.01, 通道 2 权重为零不参与
        // WMSE = (1.0*0.01 + 1.0*0.01) / (1.0 + 1.0) = 0.01
        assertEquals(0.01, result.wmse, 1e-6);
        // 但每通道 MSE 仍记录通道 2（不受通道权重影响，因为它是未加权的）
        assertTrue(result.perChannelMse[2] > 0, "Per-channel MSE is unweighted, should still report ch2");
    }

    @Test
    void buildMaskRespectsNullAndCensored() {
        boolean[] mask1 = WeightedMseLoss.buildMask(null);
        for (boolean b : mask1) assertTrue(b, "Null censored mask → all valid");

        boolean[] censored = {false, false, false, false, true};
        boolean[] mask2 = WeightedMseLoss.buildMask(censored);
        assertTrue(mask2[0]);
        assertFalse(mask2[4], "Censored Light should be invalid");
    }

    // ---- M3Scorer 测试 / M3Scorer tests ----

    @Test
    void scorerCalibrationAndScoring() {
        M3Scorer scorer = new M3Scorer(2.22);

        // 标定集：已知分布 / calibration set with known distribution
        List<Double> wmseValues = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            wmseValues.add((double) i);
        }
        List<double[]> perChannelErrors = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            perChannelErrors.add(new double[]{i * 0.1, i * 0.2, i * 0.05, i * 0.15, i * 0.1});
        }
        boolean[] channelMask = {true, true, true, true, true};
        scorer.calibrate(wmseValues, perChannelErrors, channelMask);

        assertTrue(scorer.getMedian() > 0, "Median should be positive");
        assertTrue(scorer.getIqr() > 0, "IQR should be positive");

        // 正常值：主分应在阈值以下 / normal value: main score should be below threshold
        M3Scorer.ScoreResult normalResult = scorer.score(50.0,
                new double[]{5.0, 10.0, 2.5, 7.5, 5.0});
        assertFalse(normalResult.aboveThreshold, "Normal value should be below threshold");

        // 异常值：主分应在阈值以上 / anomalous value: main score should be above threshold
        M3Scorer.ScoreResult anomalyResult = scorer.score(200.0,
                new double[]{20.0, 40.0, 10.0, 30.0, 20.0});
        assertTrue(anomalyResult.aboveThreshold, "Anomalous value should be above threshold");
        assertTrue(anomalyResult.mahaScore > 0, "Mahalanobis score should be positive");
    }

    @Test
    void scorerHandlesEdgeCases() {
        M3Scorer scorer = new M3Scorer(2.22);
        // 所有标定值相同 → IQR 极小但不为零（下限保护）/ all same → IQR clamped to epsilon
        List<Double> same = new ArrayList<>();
        for (int i = 0; i < 50; i++) same.add(1.0);
        scorer.calibrate(same, null, null);
        assertTrue(scorer.getIqr() > 0, "IQR should be clamped above zero");
    }

    // ---- LstmAutoEncoder 测试 / LstmAutoEncoder tests ----

    @Test
    void autoEncoderTrainReducesLoss() {
        LstmAutoEncoder ae = new LstmAutoEncoder(3, 10);
        double[][][] windows = new double[5][10][3];
        for (int w = 0; w < 5; w++) {
            for (int t = 0; t < 10; t++) {
                windows[w][t][0] = Math.sin(t * 0.5);
                windows[w][t][1] = Math.cos(t * 0.5);
                windows[w][t][2] = t * 0.1;
            }
        }

        double loss1 = ae.trainEpoch(windows, null);
        for (int i = 0; i < 19; i++) ae.trainEpoch(windows, null);
        double loss20 = ae.trainEpoch(windows, null);

        assertTrue(loss20 < loss1, "Loss should decrease after training: epoch1=" + loss1 + " epoch20=" + loss20);
    }

    @Test
    void autoEncoderReconstructOutputShape() {
        LstmAutoEncoder ae = new LstmAutoEncoder(5, 20);
        double[][] window = new double[60][5];
        for (int t = 0; t < 60; t++) {
            for (int f = 0; f < 5; f++) window[t][f] = Math.random();
        }
        double[][] recon = ae.reconstruct(window);
        assertEquals(60, recon.length, "Reconstruction should have same seq length");
        assertEquals(5, recon[0].length, "Reconstruction should have same feature count");
    }

    @Test
    void autoEncoderSerializeDeserialize() throws Exception {
        LstmAutoEncoder ae = new LstmAutoEncoder(5, 20);
        // 训几轮使参数非初始化 / train a few epochs so params are non-trivial
        double[][][] windows = new double[3][10][5];
        for (int w = 0; w < 3; w++)
            for (int t = 0; t < 10; t++)
                for (int f = 0; f < 5; f++)
                    windows[w][t][f] = Math.sin(w + t + f);
        ae.trainEpoch(windows, null);

        double[][] testWindow = new double[10][5];
        for (int t = 0; t < 10; t++)
            for (int f = 0; f < 5; f++)
                testWindow[t][f] = Math.cos(t + f);

        double[][] beforeRecon = ae.reconstruct(testWindow);

        // 序列化然后反序列化 / serialize then deserialize
        byte[] bytes = ae.serializeModel();
        assertTrue(bytes.length > 0, "Serialized model should be non-empty");

        LstmAutoEncoder ae2 = new LstmAutoEncoder(5, 20);
        ae2.deserializeModel(bytes);
        double[][] afterRecon = ae2.reconstruct(testWindow);

        // 重建结果应相同 / reconstruction should be identical
        for (int t = 0; t < 10; t++) {
            for (int f = 0; f < 5; f++) {
                assertEquals(beforeRecon[t][f], afterRecon[t][f], 1e-6,
                        "Reconstruction should match after deserialize at t=" + t + " f=" + f);
            }
        }
    }
}
