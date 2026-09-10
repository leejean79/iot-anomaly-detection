package com.leejean.m3;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 元素加权 MSE 损失（交接文档 §3 决策 5）：缺失/删失条目权重为零，配置通道权重表。
 * Element-weighted MSE loss (handover §3 decision 5): zero weight for missing/censored entries,
 * with a configurable per-device channel-weight table.
 *
 * <p>计算公式 / formula: WMSE = Σ_t Σ_c [ w_c · m_{t,c} · (x_{t,c} − x̂_{t,c})² ] / Σ_t Σ_c [ w_c · m_{t,c} ]
 * 其中 w_c = 通道权重（SYN_M3_CHANNEL_WEIGHTS），m_{t,c} = 掩码（0 = 缺失或删失，1 = 有效）。
 * w_c = channel weight, m_{t,c} = mask (0 = missing or censored, 1 = valid).
 *
 * <p>同时保留每通道的未加权 MSE（per-channel reconstruction error），供监测与 Mahalanobis 评分。
 * Also retains per-channel unweighted MSE (per-channel reconstruction errors) for monitoring
 * and Mahalanobis scoring.
 */
public class WeightedMseLoss implements Serializable {
    private static final long serialVersionUID = 1L;

    private final double[] channelWeights;
    private final int nChannels;

    public WeightedMseLoss(int nChannels) {
        this(nChannels, null);
    }

    /**
     * @param channelWeights 通道权重表（null = 全 1）；长度必须等于 nChannels / channel weights (null = all ones)
     */
    public WeightedMseLoss(int nChannels, double[] channelWeights) {
        this.nChannels = nChannels;
        if (channelWeights != null) {
            if (channelWeights.length != nChannels) {
                throw new IllegalArgumentException(
                        "channelWeights.length=" + channelWeights.length + " != nChannels=" + nChannels);
            }
            this.channelWeights = channelWeights.clone();
        } else {
            this.channelWeights = new double[nChannels];
            Arrays.fill(this.channelWeights, 1.0);
        }
    }

    /**
     * 结果容器：加权 MSE + 每通道 MSE。/ result container: weighted MSE + per-channel MSE.
     */
    public static class LossResult implements Serializable {
        private static final long serialVersionUID = 1L;
        public final double wmse;
        public final double[] perChannelMse;

        public LossResult(double wmse, double[] perChannelMse) {
            this.wmse = wmse;
            this.perChannelMse = perChannelMse;
        }
    }

    /**
     * 计算一个窗口的加权 MSE 与每通道 MSE。
     * Compute weighted MSE and per-channel MSE for one window.
     *
     * @param input      输入序列 [windowLen × nChannels]（行优先）/ input [windowLen × nChannels] row-major
     * @param output     重建序列（同形状）/ reconstructed output (same shape)
     * @param masks      每时间步的有效掩码（true = 有效）[windowLen]，null 元素 = 全有效 / per-step mask, null = all valid
     * @param windowLen  窗口长度 / window length
     * @return LossResult
     */
    public LossResult compute(double[][] input, double[][] output,
                              boolean[][] masks, int windowLen) {
        double[] channelSumSq = new double[nChannels];
        double[] channelCount = new double[nChannels];
        double weightedSumSq = 0.0;
        double weightedCount = 0.0;

        for (int t = 0; t < windowLen; t++) {
            for (int c = 0; c < nChannels; c++) {
                boolean valid = masks == null || masks[t] == null || masks[t][c];
                if (!valid) {
                    continue;
                }
                double diff = input[t][c] - output[t][c];
                double sq = diff * diff;
                channelSumSq[c] += sq;
                channelCount[c] += 1.0;
                weightedSumSq += channelWeights[c] * sq;
                weightedCount += channelWeights[c];
            }
        }

        double wmse = weightedCount > 0 ? weightedSumSq / weightedCount : 0.0;
        double[] perChannelMse = new double[nChannels];
        for (int c = 0; c < nChannels; c++) {
            perChannelMse[c] = channelCount[c] > 0 ? channelSumSq[c] / channelCount[c] : 0.0;
        }
        return new LossResult(wmse, perChannelMse);
    }

    /**
     * 构建有效掩码：缺失条目和删失条目均为 false。
     * Build validity mask: missing entries and censored entries are false.
     *
     * @param censoredMask 删失掩码（来自 AnnotatedRound），null = 无删失 / censored mask, null = none
     * @return 五通道有效掩码（true = 有效，参与损失计算）/ 5-channel validity mask
     */
    public static boolean[] buildMask(boolean[] censoredMask) {
        boolean[] mask = new boolean[5];
        Arrays.fill(mask, true);
        if (censoredMask != null) {
            for (int c = 0; c < Math.min(censoredMask.length, mask.length); c++) {
                if (censoredMask[c]) {
                    mask[c] = false;
                }
            }
        }
        return mask;
    }

    public double[] getChannelWeights() {
        return channelWeights.clone();
    }
}
