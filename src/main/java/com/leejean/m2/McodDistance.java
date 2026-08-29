package com.leejean.m2;

/**
 * 欧氏距离：忠实迁移 {@code common_utils/Utils.scala:10-29} 的两个 distance 重载
 * （点对点、点对微簇中心）。只迁移这两个，其余变体（PmcSkyCluster 等）不迁移（交接文档 §2 第 3 条）。
 * Euclidean distance: faithful port of the two overloads in {@code Utils.scala:10-29}
 * (point-to-point and point-to-micro-cluster-centre). No other variants are ported.
 *
 * <p>口径与原文一致：按两者维数的较小值逐维平方和再开方（{@code Math.min(dims)}）。
 * Same as the source: sum of squared per-dimension differences over the min of the two dimensionalities.
 */
public final class McodDistance {

    private McodDistance() { }

    /** 点对点距离（Utils.scala:10-19）。 */
    public static double distance(McodPoint xs, McodPoint ys) {
        int min = Math.min(xs.dimensions(), ys.dimensions());
        double value = 0.0;
        for (int i = 0; i < min; i++) {
            double d = xs.value[i] - ys.value[i];
            value += d * d;
        }
        return Math.sqrt(value);
    }

    /** 点对微簇中心距离（Utils.scala:21-29）。 */
    public static double distance(McodPoint xs, MicroCluster ys) {
        int min = Math.min(xs.dimensions(), ys.center.length);
        double value = 0.0;
        for (int i = 0; i < min; i++) {
            double d = xs.value[i] - ys.center[i];
            value += d * d;
        }
        return Math.sqrt(value);
    }
}
