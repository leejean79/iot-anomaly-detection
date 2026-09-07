package com.leejean.m1;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 相对退化防护单元测试（补充指令三 step2）：健康通道不触发、G 型退化通道触发并得到合理分母、
 * 两条防护（绝对 IQR≈0 兜底 vs 相对 IQR&lt;主体/10）的先后关系。
 * Tests for the relative-degeneracy protection: healthy channel untouched; a G-type near-degenerate
 * channel triggers the substituted denominator; the ordering of the absolute vs relative protections.
 */
class RelativeDegeneracyTest {

    private static final double EPS = 1e-9;

    private static List<Double> sorted(double... xs) {
        List<Double> l = new ArrayList<>();
        for (double x : xs) {
            l.add(x);
        }
        Collections.sort(l);
        return l;
    }

    @Test
    void healthyChannelUsesIqrNoSubstitution() {
        // 均匀分布：IQR 与主体宽度成正常比例（远大于主体/10）→ 用 IQR，不替代、不旁路
        List<Double> vals = sorted(10, 20, 30, 40, 50, 60, 70, 80, 90, 100);
        RobustScalerFunction.ScaleDecision d = RobustScalerFunction.decideScale(vals, EPS);
        assertFalse(d.bypass);
        assertFalse(d.substituted, "健康通道不应触发相对退化替代");
        double iqr = percentileRef(vals, 0.75) - percentileRef(vals, 0.25);
        assertEquals(iqr, d.scale, 1e-9, "健康通道分母 = IQR");
    }

    @Test
    void gTypeNearDegenerateTriggersSubstitution() {
        // G 型：主体几乎恒定（一堆 10.0）+ 少量远端（1000）→ IQR≈0 但主体宽度(P5~P95)大。
        // 注意：中间 50% 全是 10 → IQR 恰为 0 会走绝对兜底；为演示"相对退化"，让中间 50% 有极小但非零的散布。
        List<Double> vals = new ArrayList<>();
        for (int i = 0; i < 90; i++) {
            vals.add(10.0 + (i % 3) * 1e-3);   // 主体：10.000~10.002，IQR 约 1e-3（小而非零）
        }
        for (int i = 0; i < 10; i++) {
            vals.add(1000.0);                  // 远端 10% → 抬高 P95、放大主体宽度
        }
        Collections.sort(vals);
        RobustScalerFunction.ScaleDecision d = RobustScalerFunction.decideScale(vals, EPS);
        assertFalse(d.bypass, "IQR 非零，不应绝对旁路");
        assertTrue(d.substituted, "IQR 远小于主体宽度/10 → 应触发相对退化替代");
        // 替代分母 = 主体宽度/2.44，应远大于原始 IQR（约 1e-3），使标准化不再被除爆
        double iqr = percentileRef(vals, 0.75) - percentileRef(vals, 0.25);
        assertTrue(d.scale > iqr * 100, "替代分母应远大于退化的 IQR");
        double body = percentileRef(vals, 0.95) - percentileRef(vals, 0.05);
        assertEquals(body / 2.44, d.scale, 1e-6, "替代分母 = 主体宽度/2.44");
    }

    @Test
    void absoluteZeroIqrBypassesTakesPrecedenceOverSubstitution() {
        // 中间 50% 完全恒定（IQR==0）→ 绝对兜底旁路，即使主体宽度大也不走替代
        List<Double> vals = new ArrayList<>();
        for (int i = 0; i < 90; i++) {
            vals.add(10.0);                    // 主体完全恒定 → IQR = 0
        }
        for (int i = 0; i < 10; i++) {
            vals.add(1000.0);
        }
        Collections.sort(vals);
        RobustScalerFunction.ScaleDecision d = RobustScalerFunction.decideScale(vals, EPS);
        assertTrue(d.bypass, "IQR==0 → 绝对退化兜底旁路（优先于相对替代）");
        assertFalse(d.substituted);
        assertEquals(1.0, d.scale, 0.0);
    }

    /** 与算子内一致的分位数参考实现（线性插值）/ same linear-interpolation percentile as the operator. */
    private static double percentileRef(List<Double> sorted, double q) {
        int n = sorted.size();
        if (n == 1) {
            return sorted.get(0);
        }
        double pos = q * (n - 1);
        int lo = (int) Math.floor(pos);
        int hi = (int) Math.ceil(pos);
        return sorted.get(lo) * (1 - (pos - lo)) + sorted.get(hi) * (pos - lo);
    }
}
