package com.leejean.m5a;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 一台设备一个通道一个当地日对参照的统计量（离线操作口径第二、三节）：分箱 D、渐近显著性、当日 P10、中位数、P90、偏移与宽度倍数。
 * 运算次序与离线工具 {@code eda/m5a_extract.py} 相同，两边逐位一致。
 * Daily statistics of one channel against the reference; same arithmetic order as the Python offline tool.
 */
public final class DayStats implements Serializable {
    private static final long serialVersionUID = 1L;

    public int n;
    public long m;
    public double dBin;
    public double p;
    public double p10;
    public double median;
    public double p90;
    public double shift;
    public double widthX;
    public double maxCellMass;

    private DayStats() {
    }

    /** x 为当日样本（会被排序）/ x is the day sample (sorted in place). */
    public static DayStats of(double[] x, RefHistogram ref) {
        Arrays.sort(x);
        DayStats s = new DayStats();
        s.n = x.length;
        s.m = ref.getM();
        long[] cx = ref.countCells(x);
        long[] cr = ref.getCells();
        double cumX = 0.0;
        double cumR = 0.0;
        double d = 0.0;
        long maxCell = 0;
        for (int i = 0; i < cx.length; i++) {
            cumX += cx[i];
            cumR += cr[i];
            d = Math.max(d, Math.abs(cumX / s.n - cumR / s.m));
            maxCell = Math.max(maxCell, cx[i]);
        }
        s.dBin = d;
        s.maxCellMass = (double) maxCell / s.n;
        s.p = ksP(d, s.n, s.m);
        s.p10 = RefHistogram.percentile(x, 0.10);
        s.median = RefHistogram.percentile(x, 0.50);
        s.p90 = RefHistogram.percentile(x, 0.90);
        double w = ref.getP90() - ref.getP10();
        s.shift = w > 0 ? (s.median - ref.getMedian()) / w : Double.NaN;
        s.widthX = w > 0 ? (s.p90 - s.p10) / w : Double.NaN;
        return s;
    }

    /** 渐近显著性 Q_KS(√(nm/(n+m))·D)，与 m5a/stats.py 相同 / asymptotic KS p-value. */
    public static double ksP(double d, long n, long m) {
        double lam = Math.sqrt((double) n * m / (n + m)) * d;
        if (lam < 1e-3) {
            return 1.0;
        }
        double s = 0.0;
        for (int k = 1; k <= 100; k++) {
            s += ((k % 2 == 1) ? 1.0 : -1.0) * Math.exp(-2.0 * k * k * lam * lam);
        }
        return Math.max(0.0, Math.min(1.0, 2 * s));
    }
}
