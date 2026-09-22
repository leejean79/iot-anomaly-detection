package com.leejean.m3;

import org.junit.jupiter.api.DisplayName;
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
        LstmAutoEncoder ae = new LstmAutoEncoder(3, 10, 10);
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
        LstmAutoEncoder ae = new LstmAutoEncoder(5, 20, 60);
        double[][] window = new double[60][5];
        for (int t = 0; t < 60; t++) {
            for (int f = 0; f < 5; f++) window[t][f] = Math.random();
        }
        double[][] recon = ae.reconstruct(window);
        assertEquals(60, recon.length, "Reconstruction should have same seq length");
        assertEquals(5, recon[0].length, "Reconstruction should have same feature count");
    }

    // ---- M3ClusterSmoke 报告构建器测试 / cluster smoke report-builder test ----

    @Test
    void clusterSmokeReportContainsAllFourPoints() {
        // buildReport 会强制 ND4J 原生加载（开发容器为 linux-x86_64，可行）并采集四点证据。
        // buildReport forces ND4J native load (dev container is linux-x86_64) and collects the four points.
        String report = M3ClusterSmoke.buildReport(0, 1);

        // 点3：JDK / Point 3: JDK
        assertTrue(report.contains("java.version="), "report should carry java.version (Point 3)");
        assertTrue(report.contains("os.arch="), "report should carry os.arch");
        // 原生加载成功 / native load succeeded
        assertTrue(report.contains("nd4j_native_ok=true"),
                "ND4J native backend should load on the dev container: " + report);
        // 点1：JavaCPP 堆外上限/用量 / Point 1: JavaCPP off-heap ceilings/usage
        assertTrue(report.contains("javacpp.maxBytes="), "report should carry javacpp.maxBytes (Point 1)");
        assertTrue(report.contains("javacpp.maxPhysicalBytes="), "report should carry javacpp.maxPhysicalBytes");
        // 点2：解包目录 / Point 2: extraction dir
        assertTrue(report.contains("javacpp.cacheDir="), "report should carry javacpp.cacheDir (Point 2)");
        assertTrue(report.contains("javacpp.cacheDir.writable="), "report should carry cacheDir writability");
    }

    @Test
    @DisplayName("结构：解码器路径上不存在从时刻 t 的输入到时刻 t 的输出的直接连接")
    void decoderHasNoDirectPerTimestepPathFromInput() {
        // 裁决书第四节第 1 条要求的断言。做法：只扰动输入的某一个时间步，测量输出在**各个**时间步
        // 上的变化量。旧结构里每一步的输出直接看得到同一步的输入，被扰动那一步的输出变化会远大于
        // 其余各步；新结构里输入只经由一个摘要向量影响输出，因此变化应当摊到所有时间步上。
        // Perturb one input step and measure the change at every output step: a per-timestep shortcut
        // would concentrate the change at the perturbed step.
        //
        // 判别力已验证：2026-09-22 用旧拓扑（一层逐时间步 LSTM 接一个逐时间步输出层）跑同一个实验，
        // 被扰动步的输出变化为 3.173、其余各步最大 2.025，比值 1.6，下面的断言在旧结构下会失败。
        // 一条在新旧结构下都通过的断言是没有意义的，故此处记录该验证。
        // Discriminating power verified: on the former topology the same probe gave 3.173 vs 2.025,
        // so the assertion below would have failed there.
        final int L = 16, F = 5, C = 8, tPerturb = 7;
        LstmAutoEncoder ae = new LstmAutoEncoder(F, C, L);
        double[][][] windows = new double[8][L][F];
        java.util.Random rnd = new java.util.Random(5);
        for (int w = 0; w < 8; w++) {
            for (int t = 0; t < L; t++) {
                for (int f = 0; f < F; f++) {
                    windows[w][t][f] = Math.sin(0.4 * t + f) + 0.05 * rnd.nextGaussian();
                }
            }
        }
        for (int e = 0; e < 5; e++) {
            ae.trainEpoch(windows, null, 4);           // 训几轮，避免在纯随机初值上做判断
        }

        double[][] base = copy(windows[0]);
        double[][] perturbed = copy(windows[0]);
        for (int f = 0; f < F; f++) {
            perturbed[tPerturb][f] += 3.0;             // 只动一个时间步 / a single step is moved
        }

        double[][] reconBase = ae.reconstruct(base);
        double[][] reconPert = ae.reconstruct(perturbed);

        double[] delta = new double[L];                // 各时间步的输出变化量 / per-step output change
        for (int t = 0; t < L; t++) {
            for (int f = 0; f < F; f++) {
                delta[t] += Math.abs(reconPert[t][f] - reconBase[t][f]);
            }
        }
        double atPerturbed = delta[tPerturb];
        double maxElsewhere = 0.0;
        for (int t = 0; t < L; t++) {
            if (t != tPerturb) {
                maxElsewhere = Math.max(maxElsewhere, delta[t]);
            }
        }

        assertTrue(maxElsewhere > 0.0,
                "扰动应当影响到被扰动步之外的输出；若其余各步纹丝不动，说明存在逐步直连");
        assertTrue(atPerturbed <= maxElsewhere,
                "被扰动那一步的输出变化 " + atPerturbed + " 不得高于其余各步的最大变化 "
                        + maxElsewhere + "——高出即说明解码器能直接看到同一步的输入");
    }

    /** 深拷贝一个窗口，避免扰动实验改到原数组。 */
    private static double[][] copy(double[][] window) {
        double[][] out = new double[window.length][];
        for (int t = 0; t < window.length; t++) {
            out[t] = window[t].clone();
        }
        return out;
    }

    @Test
    void autoEncoderSerializeDeserialize() throws Exception {
        LstmAutoEncoder ae = new LstmAutoEncoder(5, 20, 10);
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

        LstmAutoEncoder ae2 = new LstmAutoEncoder(5, 20, 10);
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
