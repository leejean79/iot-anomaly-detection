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

    private double median;              // 标定集 WMSE 的中位数 / median of the calibration-set WMSE
    private double iqr;                 // 标定集 WMSE 的四分位距（Q75−Q25）/ interquartile range of the WMSE
    private double[][] covInverse;      // 每通道误差协方差的逆矩阵（仅活跃通道）/ inverse covariance (active channels only)
    private double[] covMean;           // 每通道误差均值（仅活跃通道）/ per-channel error means (active channels only)
    private boolean[] covChannelMask;   // 参与 Mahalanobis 的通道掩码 / channels participating in Mahalanobis
    private final double threshold;     // 报警阈值（z 分单位，默认 2.22）/ alarm threshold in z-units (default 2.22)
    private final double ridge;         // 岭正则化系数，防协方差奇异 / ridge term to keep covariance non-singular

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
        // 拷贝并排序 WMSE，用于取分位数 / copy and sort the WMSE values to read percentiles
        double[] sorted = new double[wmseValues.size()];
        for (int i = 0; i < sorted.length; i++) {
            sorted[i] = wmseValues.get(i);
        }
        Arrays.sort(sorted);
        int n = sorted.length;
        median = percentile(sorted, 50.0);          // 中位数 / median
        double q25 = percentile(sorted, 25.0);       // 下四分位 / lower quartile
        double q75 = percentile(sorted, 75.0);       // 上四分位 / upper quartile
        iqr = q75 - q25;
        if (iqr < 1e-12) {
            iqr = 1e-12;                              // 下限保护，避免 z 分除零 / floor to avoid divide-by-zero in z-score
        }

        this.covChannelMask = channelMask != null ? channelMask.clone() : null;
        int nActive = 0;                             // 活跃（参与 Mahalanobis）通道数 / count of active channels
        if (channelMask != null) {
            for (boolean b : channelMask) {
                if (b) nActive++;
            }
        }

        if (nActive > 0 && perChannelErrors != null && !perChannelErrors.isEmpty()) {
            // 活跃通道在原通道向量中的下标映射 / map active-channel positions back to original indices
            int[] activeIdx = new int[nActive];
            int ai = 0;
            for (int c = 0; c < channelMask.length; c++) {
                if (channelMask[c]) activeIdx[ai++] = c;
            }

            int nSamples = perChannelErrors.size();
            // 第一遍：估计每通道误差均值 / pass 1: estimate per-channel error means
            covMean = new double[nActive];
            for (double[] e : perChannelErrors) {
                for (int j = 0; j < nActive; j++) {
                    covMean[j] += e[activeIdx[j]];
                }
            }
            for (int j = 0; j < nActive; j++) {
                covMean[j] /= nSamples;
            }

            // 第二遍：累加去均值外积，得协方差矩阵上三角 / pass 2: accumulate demeaned outer products (upper triangle)
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
                    cov[j][k] /= nSamples;            // 归一为协方差 / normalize to covariance
                    if (j != k) cov[k][j] = cov[j][k];   // 对称补全下三角 / mirror to lower triangle
                }
                cov[j][j] += ridge;                  // 对角加岭，保证可逆 / add ridge on the diagonal for invertibility
            }
            covInverse = invertMatrix(cov, nActive);  // 预存逆矩阵供在线评分 / precompute the inverse for online scoring
        } else {
            covInverse = null;                        // 无活跃通道 → Mahalanobis 恒为 0 / no active channels → Mahalanobis stays 0
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
        double z = (wmse - median) / iqr;             // 主分：以 IQR 为单位的标准化偏离 / main score: IQR-normalized deviation
        double maha = computeMahalanobis(perChannelMse);   // Mahalanobis 分（仅报告）/ Mahalanobis score (report only)
        return new ScoreResult(z, maha, z >= threshold);   // z ≥ 阈值即报警 / alarm when z ≥ threshold
    }

    private double computeMahalanobis(double[] perChannelMse) {
        if (covInverse == null || covMean == null || covChannelMask == null) {
            return 0.0;
        }
        int nActive = covMean.length;
        int[] activeIdx = new int[nActive];           // 与标定端一致的活跃通道下标 / same active-channel indices as calibrate
        int ai = 0;
        for (int c = 0; c < covChannelMask.length; c++) {
            if (covChannelMask[c]) activeIdx[ai++] = c;
        }

        double[] diff = new double[nActive];          // 去均值误差向量 / demeaned error vector
        for (int j = 0; j < nActive; j++) {
            diff[j] = perChannelMse[activeIdx[j]] - covMean[j];
        }

        // 二次型 dᵀ · Σ⁻¹ · d / quadratic form dᵀ · Σ⁻¹ · d
        double sum = 0.0;
        for (int j = 0; j < nActive; j++) {
            double inner = 0.0;
            for (int k = 0; k < nActive; k++) {
                inner += covInverse[j][k] * diff[k];   // (Σ⁻¹ · d)_j
            }
            sum += diff[j] * inner;
        }
        return Math.sqrt(Math.max(sum, 0.0));          // 取根号得马氏距离，钳到非负防浮点误差 / sqrt, clamped ≥ 0
    }

    private static double percentile(double[] sorted, double q) {
        int n = sorted.length;
        if (n == 0) return 0.0;
        double pos = q / 100.0 * (n - 1);              // 分位在数组中的连续位置 / continuous rank position
        int lo = (int) Math.floor(pos);
        int hi = (int) Math.ceil(pos);
        if (lo == hi || hi >= n) return sorted[Math.min(lo, n - 1)];   // 整数位或越界直接取值 / exact index or clamp
        double frac = pos - lo;
        return sorted[lo] * (1 - frac) + sorted[hi] * frac;   // 相邻两点线性插值 / linear interpolation
    }

    /**
     * 高斯消元法求逆矩阵（小矩阵，最多 5×5）。/ Gauss-Jordan inversion for small matrices (up to 5×5).
     */
    private static double[][] invertMatrix(double[][] mat, int n) {
        // 构造增广矩阵 [A | I]，消元后右半即为 A⁻¹ / build augmented [A | I]; the right half becomes A⁻¹
        double[][] aug = new double[n][2 * n];
        for (int i = 0; i < n; i++) {
            System.arraycopy(mat[i], 0, aug[i], 0, n);
            aug[i][n + i] = 1.0;                       // 右半置单位矩阵 / right half is the identity
        }
        for (int col = 0; col < n; col++) {
            // 选主元：本列绝对值最大的行，提升数值稳定性 / partial pivot: largest-magnitude row in this column
            int pivot = col;
            for (int row = col + 1; row < n; row++) {
                if (Math.abs(aug[row][col]) > Math.abs(aug[pivot][col])) {
                    pivot = row;
                }
            }
            double[] tmp = aug[col];                   // 交换主元行到对角位置 / swap the pivot row into place
            aug[col] = aug[pivot];
            aug[pivot] = tmp;

            double diag = aug[col][col];
            if (Math.abs(diag) < 1e-15) {
                diag = 1e-15;                          // 近奇异保护 / guard against a near-singular pivot
            }
            for (int j = 0; j < 2 * n; j++) {
                aug[col][j] /= diag;                  // 主元行归一 / normalize the pivot row
            }
            for (int row = 0; row < n; row++) {
                if (row != col) {
                    double factor = aug[row][col];    // 消去其余行本列 / eliminate this column from other rows
                    for (int j = 0; j < 2 * n; j++) {
                        aug[row][j] -= factor * aug[col][j];
                    }
                }
            }
        }
        double[][] inv = new double[n][n];
        for (int i = 0; i < n; i++) {
            System.arraycopy(aug[i], n, inv[i], 0, n);   // 取右半为逆矩阵 / extract the right half as the inverse
        }
        return inv;
    }

    public double getMedian() { return median; }
    public double getIqr() { return iqr; }
    public double getThreshold() { return threshold; }
}
