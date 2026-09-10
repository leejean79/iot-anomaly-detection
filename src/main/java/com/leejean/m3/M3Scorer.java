package com.leejean.m3;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * M3 评分器（交接文档 §3 决策 6）：在阈值标定集上估计分布参数，在线推理时输出主分与 Mahalanobis 分。
 * M3 scorer (handover §3 decision 6): estimate distribution parameters on the threshold-calibration
 * set, then produce main score and Mahalanobis score during online inference.
 *
 * <p>主分 / main score: z = (wmse − median) ÷ IQR；threshold 默认 2.22（3σ = 2.22 IQR，正态下
 * 1σ ≈ 0.741 IQR）。z ≥ threshold → 报警。
 *
 * <p>Mahalanobis 分 / Mahalanobis score: 在未掩码通道的每通道误差向量上，用标定集协方差矩阵（加岭正则化
 * 防奇异）计算。仅报告，不作报警依据。
 * Computed on the per-channel error vector restricted to unmasked channels, using the calibration-set
 * covariance matrix with ridge regularization. Reported only, not alarmed on.
 */
public class M3Scorer implements Serializable {
    private static final long serialVersionUID = 1L;

    private double median;
    private double iqr;
    private double[][] covInverse;
    private double[] covMean;
    private boolean[] covChannelMask;
    private final double threshold;
    private final double ridge;

    private static final double DEFAULT_RIDGE = 1e-4;

    public M3Scorer(double threshold) {
        this(threshold, DEFAULT_RIDGE);
    }

    public M3Scorer(double threshold, double ridge) {
        this.threshold = threshold;
        this.ridge = ridge;
    }

    /**
     * 在标定集上拟合：估计 WMSE 分布的 median 和 IQR，以及每通道误差的协方差逆矩阵。
     * Fit on the calibration set: estimate WMSE distribution (median, IQR) and per-channel error
     * covariance inverse matrix.
     *
     * @param wmseValues 标定窗口的 WMSE 值列表 / calibration windows' WMSE values
     * @param perChannelErrors 标定窗口的每通道误差 [nWindows][nChannels] / per-channel errors
     * @param channelMask 通道掩码（true = 该通道参与 Mahalanobis）/ channel mask for Mahalanobis
     */
    public void calibrate(List<Double> wmseValues, List<double[]> perChannelErrors,
                          boolean[] channelMask) {
        double[] sorted = new double[wmseValues.size()];
        for (int i = 0; i < sorted.length; i++) {
            sorted[i] = wmseValues.get(i);
        }
        Arrays.sort(sorted);
        int n = sorted.length;
        median = percentile(sorted, 50.0);
        double q25 = percentile(sorted, 25.0);
        double q75 = percentile(sorted, 75.0);
        iqr = q75 - q25;
        if (iqr < 1e-12) {
            iqr = 1e-12;
        }

        this.covChannelMask = channelMask != null ? channelMask.clone() : null;
        int nActive = 0;
        if (channelMask != null) {
            for (boolean b : channelMask) {
                if (b) nActive++;
            }
        }

        if (nActive > 0 && perChannelErrors != null && !perChannelErrors.isEmpty()) {
            int[] activeIdx = new int[nActive];
            int ai = 0;
            for (int c = 0; c < channelMask.length; c++) {
                if (channelMask[c]) activeIdx[ai++] = c;
            }

            int nSamples = perChannelErrors.size();
            covMean = new double[nActive];
            for (double[] e : perChannelErrors) {
                for (int j = 0; j < nActive; j++) {
                    covMean[j] += e[activeIdx[j]];
                }
            }
            for (int j = 0; j < nActive; j++) {
                covMean[j] /= nSamples;
            }

            double[][] cov = new double[nActive][nActive];
            for (double[] e : perChannelErrors) {
                for (int j = 0; j < nActive; j++) {
                    double dj = e[activeIdx[j]] - covMean[j];
                    for (int k = j; k < nActive; k++) {
                        double dk = e[activeIdx[k]] - covMean[k];
                        cov[j][k] += dj * dk;
                    }
                }
            }
            for (int j = 0; j < nActive; j++) {
                for (int k = j; k < nActive; k++) {
                    cov[j][k] /= nSamples;
                    if (j != k) cov[k][j] = cov[j][k];
                }
                cov[j][j] += ridge;
            }
            covInverse = invertMatrix(cov, nActive);
        } else {
            covInverse = null;
            covMean = null;
        }
    }

    /**
     * 对一个窗口的评分结果。/ Score result for one window.
     */
    public static class ScoreResult implements Serializable {
        private static final long serialVersionUID = 1L;
        public final double mainScore;
        public final double mahaScore;
        public final boolean aboveThreshold;

        public ScoreResult(double mainScore, double mahaScore, boolean aboveThreshold) {
            this.mainScore = mainScore;
            this.mahaScore = mahaScore;
            this.aboveThreshold = aboveThreshold;
        }
    }

    /**
     * 对一个窗口计算主分和 Mahalanobis 分。/ Compute main score and Mahalanobis score for one window.
     *
     * @param wmse 加权 MSE / weighted MSE
     * @param perChannelMse 每通道 MSE [nChannels] / per-channel MSE
     * @return ScoreResult
     */
    public ScoreResult score(double wmse, double[] perChannelMse) {
        double z = (wmse - median) / iqr;
        double maha = computeMahalanobis(perChannelMse);
        return new ScoreResult(z, maha, z >= threshold);
    }

    private double computeMahalanobis(double[] perChannelMse) {
        if (covInverse == null || covMean == null || covChannelMask == null) {
            return 0.0;
        }
        int nActive = covMean.length;
        int[] activeIdx = new int[nActive];
        int ai = 0;
        for (int c = 0; c < covChannelMask.length; c++) {
            if (covChannelMask[c]) activeIdx[ai++] = c;
        }

        double[] diff = new double[nActive];
        for (int j = 0; j < nActive; j++) {
            diff[j] = perChannelMse[activeIdx[j]] - covMean[j];
        }

        double sum = 0.0;
        for (int j = 0; j < nActive; j++) {
            double inner = 0.0;
            for (int k = 0; k < nActive; k++) {
                inner += covInverse[j][k] * diff[k];
            }
            sum += diff[j] * inner;
        }
        return Math.sqrt(Math.max(sum, 0.0));
    }

    private static double percentile(double[] sorted, double q) {
        int n = sorted.length;
        if (n == 0) return 0.0;
        double pos = q / 100.0 * (n - 1);
        int lo = (int) Math.floor(pos);
        int hi = (int) Math.ceil(pos);
        if (lo == hi || hi >= n) return sorted[Math.min(lo, n - 1)];
        double frac = pos - lo;
        return sorted[lo] * (1 - frac) + sorted[hi] * frac;
    }

    /**
     * 高斯消元法求逆矩阵（小矩阵，最多 5×5）。/ Gauss-Jordan inversion for small matrices (up to 5×5).
     */
    private static double[][] invertMatrix(double[][] mat, int n) {
        double[][] aug = new double[n][2 * n];
        for (int i = 0; i < n; i++) {
            System.arraycopy(mat[i], 0, aug[i], 0, n);
            aug[i][n + i] = 1.0;
        }
        for (int col = 0; col < n; col++) {
            int pivot = col;
            for (int row = col + 1; row < n; row++) {
                if (Math.abs(aug[row][col]) > Math.abs(aug[pivot][col])) {
                    pivot = row;
                }
            }
            double[] tmp = aug[col];
            aug[col] = aug[pivot];
            aug[pivot] = tmp;

            double diag = aug[col][col];
            if (Math.abs(diag) < 1e-15) {
                diag = 1e-15;
            }
            for (int j = 0; j < 2 * n; j++) {
                aug[col][j] /= diag;
            }
            for (int row = 0; row < n; row++) {
                if (row != col) {
                    double factor = aug[row][col];
                    for (int j = 0; j < 2 * n; j++) {
                        aug[row][j] -= factor * aug[col][j];
                    }
                }
            }
        }
        double[][] inv = new double[n][n];
        for (int i = 0; i < n; i++) {
            System.arraycopy(aug[i], n, inv[i], 0, n);
        }
        return inv;
    }

    public double getMedian() { return median; }
    public double getIqr() { return iqr; }
    public double getThreshold() { return threshold; }
}
