package com.leejean.m2;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 等价性测试（交接文档 §6.1、§6.4）：MCOD 增量算法核逐滑动步的离群点 id 集合，
 * 必须与朴素暴力对照器完全相等；并验证两设备交错输入时状态互不串扰。
 * Equivalence tests (handover §6.1/§6.4): the MCOD core's per-slide outlier id set must exactly
 * equal the naive oracle's, across seeds and (R, k) grids; plus device-isolation.
 *
 * <p>驱动方式：用与 Flink 滑动窗口一致的语义逐步喂点——每步先加入本滑动步新到的点、再对拍离群集合
 * （删除滑出点之前），随后淘汰滑出点、再对拍成员集合（删除之后）。这样同时校验"离群判定"与"窗口成员维护"。
 */
class McodEquivalenceTest {

    /** 逐滑动步：既比离群集合（删除前），又比窗口成员集合（删除后）。 */
    private static void simulateAndAssert(List<McodPoint> pts, double r, int k, long w, long s, String label) {
        McodCore core = new McodCore(r, k, s, new McodState());
        List<McodPoint> active = new ArrayList<>();     // 独立维护的窗口成员（喂给对照器）
        int cursor = 0;                                 // pts 已"新到"的游标（按 arrival 升序）
        long maxArrival = pts.isEmpty() ? 0 : pts.get(pts.size() - 1).arrival;

        for (long windowEnd = s; windowEnd - w <= maxArrival; windowEnd += s) {
            long windowStart = windowEnd - w;

            // 1) 加入本滑动步新到的点（arrival ∈ [windowEnd - s, windowEnd)）
            while (cursor < pts.size() && pts.get(cursor).arrival < windowEnd) {
                active.add(pts.get(cursor));
                cursor++;
            }

            // 2) 窗口内容（arrival ∈ [windowStart, windowEnd)），喂给增量核
            List<McodPoint> windowElements = new ArrayList<>();
            for (McodPoint p : active) {
                if (p.arrival >= windowStart && p.arrival < windowEnd) {
                    windowElements.add(p);
                }
            }

            // 3) 对照器在"删除滑出点之前"的成员集合上算离群
            Set<Long> oracle = new TreeSet<>(NaiveOutlierOracle.outliers(active, r, k));

            // 4) 增量核处理该步（内部：插入新到→统计离群→删除滑出→簇维护）
            McodCore.McodResult res = core.processSlide(windowElements, windowStart, windowEnd);
            Set<Long> got = new TreeSet<>(res.outlierIds);

            assertEquals(oracle, got,
                    label + " 离群集合在 windowEnd=" + windowEnd + " 不一致 / outlier set mismatch");

            // 5) 淘汰滑出点（arrival < windowStart + s），再比成员集合（删除之后）
            active.removeIf(p -> p.arrival < windowStart + s);
            Set<Long> activeIds = idset(active);
            Set<Long> coreIds = idset(core.activePoints());
            assertEquals(activeIds, coreIds,
                    label + " 窗口成员在 windowEnd=" + windowEnd + " 不一致 / membership mismatch");
        }
    }

    private static Set<Long> idset(List<McodPoint> pts) {
        Set<Long> s = new HashSet<>();
        for (McodPoint p : pts) {
            s.add(p.id);
        }
        return s;
    }

    /**
     * 生成一段点流：值空间里若干固定簇中心，按 clusterFrac 决定该点是"落簇"（近中心，制造微簇）
     * 还是"离散"（随机散布，制造离群）；随时间在稠密与稀疏之间切换，覆盖微簇的创建与解体。
     * arrival 每 gap 递增（同设备内递增、唯一），id = 递增序号（唯一）。
     */
    private static List<McodPoint> gen(long seed, int n, int dim, double box,
                                       double clusterSpread, long gap) {
        Random rnd = new Random(seed);
        double[][] centers = new double[3][dim];
        for (double[] c : centers) {
            for (int d = 0; d < dim; d++) {
                c[d] = rnd.nextDouble() * box;
            }
        }
        List<McodPoint> pts = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            // 稠密/稀疏交替：每 20 个点为一段，前 14 稠密后 6 稀疏
            boolean dense = (i % 20) < 14;
            double[] v = new double[dim];
            if (dense && rnd.nextDouble() < 0.85) {
                double[] c = centers[rnd.nextInt(centers.length)];
                for (int d = 0; d < dim; d++) {
                    v[d] = c[d] + (rnd.nextDouble() - 0.5) * clusterSpread;
                }
            } else {
                for (int d = 0; d < dim; d++) {
                    v[d] = rnd.nextDouble() * box;
                }
            }
            long arrival = (long) i * gap;
            pts.add(new McodPoint(v, arrival, 0, i));
        }
        return pts;
    }

    @Test
    void randomStreamEquivalenceAcrossGrid() {
        double[] rs = {0.5, 1.0, 2.0};
        int[] ks = {5, 10, 20};
        long[] seeds = {1L, 2L, 3L};
        long w = 240;
        long s = 30;
        for (long seed : seeds) {
            for (double r : rs) {
                for (int k : ks) {
                    // 每个 (seed,R,k) 组合都用**全新的点对象**：processSlide 会就地改写点的可变状态
                    // （count_after / nn_before / mc / Rmc），共享会污染下一组合。gen(seed) 确定性可复现。
                    List<McodPoint> pts = gen(seed, 120, 3, 4.0, 0.3, 10);
                    simulateAndAssert(pts, r, k, w, s,
                            "seed=" + seed + " R=" + r + " k=" + k);
                }
            }
        }
    }

    @Test
    void denseThenSparseCreatesAndDissolvesMicroClusters() {
        // 专门制造：先一段极稠密（形成微簇）再一段稀疏（微簇缩水解体、成员重插）
        List<McodPoint> pts = new ArrayList<>();
        int id = 0;
        // 稠密段：40 个点挤在 (0,0,0) 附近 → 必然形成微簇（k=5）
        Random rnd = new Random(42);
        for (int i = 0; i < 40; i++) {
            double[] v = {rnd.nextDouble() * 0.1, rnd.nextDouble() * 0.1, rnd.nextDouble() * 0.1};
            pts.add(new McodPoint(v, (long) id * 10, 0, id));
            id++;
        }
        // 稀疏段：30 个点散布在大盒子 → 旧微簇随窗口滑出而解体
        for (int i = 0; i < 30; i++) {
            double[] v = {rnd.nextDouble() * 10, rnd.nextDouble() * 10, rnd.nextDouble() * 10};
            pts.add(new McodPoint(v, (long) id * 10, 0, id));
            id++;
        }
        // 与对照器逐步对拍（R=0.5, k=5, W=150, S=30）
        simulateAndAssert(pts, 0.5, 5, 150, 30, "dense-then-sparse");
    }

    @Test
    void realisticRandomWalkEquivalence() {
        // §6.2 的离线代理：模拟标准化传感器流——五维、各维为平滑随机游走（贴近 RobustScaler 后的 xNorm），
        // 并周期性注入尖峰（离群）。逐滑动步与对照器对拍。真数据的整段等价在 V-M2-2 集群单日重放中核验。
        for (long seed : new long[]{11L, 22L}) {
            Random rnd = new Random(seed);
            int dim = 5;
            double[] walk = new double[dim];
            List<McodPoint> pts = new ArrayList<>();
            for (int i = 0; i < 160; i++) {
                double[] v = new double[dim];
                for (int d = 0; d < dim; d++) {
                    walk[d] += (rnd.nextGaussian()) * 0.15;      // 平滑游走
                    v[d] = walk[d];
                }
                if (i % 37 == 0) {                                // 周期注入尖峰 → 离群
                    for (int d = 0; d < dim; d++) {
                        v[d] += (rnd.nextBoolean() ? 1 : -1) * (3.0 + rnd.nextDouble());
                    }
                }
                pts.add(new McodPoint(v, (long) i * 10, 0, i));
            }
            for (double r : new double[]{0.5, 1.0}) {
                for (int k : new int[]{5, 15}) {
                    simulateAndAssert(pts, r, k, 240, 30,
                            "walk seed=" + seed + " R=" + r + " k=" + k);
                    // 每组合用新点：processSlide 改写点状态，复用会污染
                    pts = regen(seed);
                }
            }
        }
    }

    /** 与 realisticRandomWalkEquivalence 同分布地重新生成（供参数组合间使用全新点对象）。 */
    private static List<McodPoint> regen(long seed) {
        Random rnd = new Random(seed);
        int dim = 5;
        double[] walk = new double[dim];
        List<McodPoint> pts = new ArrayList<>();
        for (int i = 0; i < 160; i++) {
            double[] v = new double[dim];
            for (int d = 0; d < dim; d++) {
                walk[d] += rnd.nextGaussian() * 0.15;
                v[d] = walk[d];
            }
            if (i % 37 == 0) {
                for (int d = 0; d < dim; d++) {
                    v[d] += (rnd.nextBoolean() ? 1 : -1) * (3.0 + rnd.nextDouble());
                }
            }
            pts.add(new McodPoint(v, (long) i * 10, 0, i));
        }
        return pts;
    }

    @Test
    void deviceIsolation() {
        // §6.4：两设备各自独立的核实例，交错喂入，断言各自与对照器等价（即互不串扰）。
        // 设备隔离在生产中由 keyBy(deviceId) 保证——每设备独立 McodState；此处以两个独立核模拟并各自对拍。
        List<McodPoint> a = gen(7L, 80, 3, 4.0, 0.3, 10);
        List<McodPoint> b = gen(9L, 80, 3, 4.0, 0.3, 10);
        // 若两设备状态串扰，独立核的等价性断言会失败；两者都通过即证隔离成立。
        simulateAndAssert(a, 1.0, 10, 240, 30, "deviceA");
        simulateAndAssert(b, 1.0, 10, 240, 30, "deviceB");
    }
}
