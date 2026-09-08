package com.leejean.m1;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 相对退化防护单元测试（补充指令三 step2 引入；补充指令四 step1 撤销为默认关闭）。
 * 本类以**显式启用**（decideScale 的 3 参重载传 true）验证防护逻辑仍然正确保留：健康通道不触发、
 * G 型退化通道触发并得到合理分母、绝对 IQR≈0 兜底优先于相对替代；另加一条：**默认关闭**（2 参重载）
 * 时即便是 G 型样本也不替代（回到健康 IQR 路径），核对撤销后的默认行为。
 * The guard is retained but OFF by default (instruction 4). These tests pass true to verify the guard
 * logic is still correct when enabled, plus one test that the default (off) path skips substitution.
 */
class RelativeDegeneracyTest {

    private static final double EPS = 1e-9;
    private static final boolean GUARD_ON = true;

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
        RobustScalerFunction.ScaleDecision d = RobustScalerFunction.decideScale(vals, EPS, GUARD_ON);
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
        RobustScalerFunction.ScaleDecision d = RobustScalerFunction.decideScale(vals, EPS, GUARD_ON);
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
        RobustScalerFunction.ScaleDecision d = RobustScalerFunction.decideScale(vals, EPS, GUARD_ON);
        assertTrue(d.bypass, "IQR==0 → 绝对退化兜底旁路（优先于相对替代）");
        assertFalse(d.substituted);
        assertEquals(1.0, d.scale, 0.0);
    }

    @Test
    void guardOffByDefaultSkipsSubstitutionForGTypeSamples() {
        // 补充指令四 step1：防护默认关闭。同一 G 型样本，走默认（2 参）路径应**不替代**，回到健康 IQR。
        List<Double> vals = new ArrayList<>();
        for (int i = 0; i < 90; i++) {
            vals.add(10.0 + (i % 3) * 1e-3);
        }
        for (int i = 0; i < 10; i++) {
            vals.add(1000.0);
        }
        Collections.sort(vals);
        RobustScalerFunction.ScaleDecision off = RobustScalerFunction.decideScale(vals, EPS);        // 默认关闭
        assertFalse(off.substituted, "默认关闭时不应触发相对退化替代（补充指令四已撤销）");
        assertFalse(off.bypass, "IQR 非零，默认路径不旁路");
        double iqr = percentileRef(vals, 0.75) - percentileRef(vals, 0.25);
        assertEquals(iqr, off.scale, 1e-9, "默认关闭时分母 = 原始 IQR");
        // 同样本显式启用则替代，证明是开关而非删除
        RobustScalerFunction.ScaleDecision on = RobustScalerFunction.decideScale(vals, EPS, GUARD_ON);
        assertTrue(on.substituted, "显式启用时仍应替代（代码保留）");
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
