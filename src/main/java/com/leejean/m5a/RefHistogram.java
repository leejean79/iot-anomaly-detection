package com.leejean.m5a;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 数据层长记忆的参照直方图（M5a 交接文档第三节「参照样本的保存」；开发设计日志 1.03、1.05 版）：由数据接入模块在冻结时从标定窗口的
 * 读数（变换域，光照为 log1p，缺失与光照删失除外）建成，附在冻结后的第一条轮上传给数据层，数据接入模块自留一份。
 * Reference histogram for the data-layer long-memory test, built by the scaler at freeze from the calibration readings.
 *
 * <p>箱：参照排序后取第 ⌊j(m−1)/bins⌋ 个顺序统计量（j = 0…bins），去重为边界 e₀ &lt; … &lt; e_K；箱为下溢 (−∞, e₀)、[e₀, e₁]、
 * (e₁, e₂] … (e_{K−1}, e_K]、上溢 (e_K, +∞)，共 K + 2 个（离线操作口径第二节）。另存参照的 P10、中位数、P90（线性插值，与 numpy 相同），
 * 供逐日统计量使用。普通 POJO，可随 {@code DeviceRound} 由 Flink 的 POJO 序列化器传递，也可由 Jackson 写进 JSON。
 * Order-statistic edges plus two overflow cells; also the reference P10/median/P90. A plain POJO.
 */
public class RefHistogram implements Serializable {
    private static final long serialVersionUID = 1L;

    private double[] edges;
    private long[] cells;
    private long m;
    private double p10;
    private double median;
    private double p90;

    public RefHistogram() {
    }

    /** 由已排序的参照样本建直方图 / build from a sorted reference sample. */
    public static RefHistogram build(double[] sorted, int bins) {
        int m = sorted.length;
        double[] raw = new double[bins + 1];
        for (int j = 0; j <= bins; j++) {
            raw[j] = sorted[(int) (((long) j * (m - 1)) / bins)];
        }
        double[] e = Arrays.stream(raw).distinct().toArray();      // 已有序，去重 / sorted, dedup
        RefHistogram h = new RefHistogram();
        h.edges = e;
        h.cells = h.countCells(sorted);
        h.m = m;
        h.p10 = percentile(sorted, 0.10);
        h.median = percentile(sorted, 0.50);
        h.p90 = percentile(sorted, 0.90);
        return h;
    }

    /** 已排序样本的箱频数，K + 2 个 / cell counts of a sorted sample. */
    public long[] countCells(double[] sorted) {
        int k = edges.length;
        long[] c = new long[k + 2];
        int under = lowerBound(sorted, edges[0]);
        c[0] = under;
        int prev = under;
        for (int i = 0; i < k; i++) {
            int le = upperBound(sorted, edges[i]);
            c[i + 1] = le - prev;
            prev = le;
        }
        c[k + 1] = sorted.length - prev;
        return c;
    }

    /**
     * 线性插值分位数，逐位复现 numpy 2.x 的默认方法（virtual = (n−1)·q；gamma = virtual − ⌊virtual⌋；gamma ≥ 0.5 时按
     * b − (b − a)(1 − gamma) 计算，否则 a + (b − a)·gamma），使与离线工具的 np.percentile 结果逐位一致。
     * Linear percentile reproducing numpy 2.x bit for bit.
     */
    public static double percentile(double[] sorted, double q) {
        int n = sorted.length;
        double virtual = (n - 1) * q;
        if (virtual >= n - 1) {
            return sorted[n - 1];
        }
        int lo = (int) Math.floor(virtual);
        double a = sorted[lo];
        double b = sorted[lo + 1];
        double gamma = virtual - lo;
        double diff = b - a;
        return gamma >= 0.5 ? b - diff * (1 - gamma) : a + diff * gamma;
    }

    static int lowerBound(double[] a, double v) {
        int lo = 0;
        int hi = a.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (a[mid] < v) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    static int upperBound(double[] a, double v) {
        int lo = 0;
        int hi = a.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (a[mid] <= v) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    public double[] getEdges() { return edges; }
    public void setEdges(double[] edges) { this.edges = edges; }
    public long[] getCells() { return cells; }
    public void setCells(long[] cells) { this.cells = cells; }
    public long getM() { return m; }
    public void setM(long m) { this.m = m; }
    public double getP10() { return p10; }
    public void setP10(double p10) { this.p10 = p10; }
    public double getMedian() { return median; }
    public void setMedian(double median) { this.median = median; }
    public double getP90() { return p90; }
    public void setP90(double p90) { this.p90 = p90; }
}
