package com.leejean.m2;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 状态生命周期单测（交接文档 §6.3）：微簇创建、缩水删除与成员重插、safe_inlier 短路、
 * R/2 与 3R/2 两级剪枝分支各自触发——以白盒断言直接观察状态转移。
 * State-lifecycle tests: micro-cluster creation, shrink-dissolve-with-reinsert, safe_inlier
 * short-circuit, and the R/2 vs 3R/2 pruning branches — asserted by inspecting the state directly.
 */
class McodLifecycleTest {

    /** 手动驱动一个滑动步：喂入 [start, end) 的点，返回结果并就地更新 core 状态。 */
    private static McodCore.McodResult slide(McodCore core, List<McodPoint> pts, long start, long end) {
        List<McodPoint> window = new ArrayList<>();
        for (McodPoint p : pts) {
            if (p.arrival >= start && p.arrival < end) {
                window.add(p);
            }
        }
        return core.processSlide(window, start, end);
    }

    private static McodPoint p(double x, long arrival, long id) {
        return new McodPoint(new double[]{x}, arrival, 0, id);
    }

    @Test
    void microClusterCreation() {
        // 5 个点全在 x=0（互距 0 ≤ R/2），k=3：第 4 个点触发 createMC，第 5 个并入。
        McodCore core = new McodCore(1.0, 3, 10, new McodState());
        List<McodPoint> pts = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pts.add(p(0.0, i, i));
        }
        // 宽窗（windowStart=-90，W=100>S=10）：全部新到、无淘汰，便于观察形成的微簇
        slide(core, pts, -90, 10);
        McodState st = core.getState();
        assertEquals(1, st.mc.size(), "应形成 1 个微簇 / one micro-cluster expected");
        assertEquals(5, st.mc.get(1).points.size(), "微簇含全部 5 点");
        assertTrue(st.pd.isEmpty(), "PD 应为空（点都进了微簇）");
        assertEquals(2, st.mcCounter, "mcCounter 应前进到 2");
    }

    @Test
    void microClusterDissolveAndReinsert() {
        // k=3, R=1, W=100, S=10：先在 x=0 形成 5 点微簇，再淘汰其中 2 点 → 缩到 3(≤k) → 解体、
        // 剩余 3 点重插回 PD。
        McodCore core = new McodCore(1.0, 3, 10, new McodState());
        List<McodPoint> pts = new ArrayList<>();
        pts.add(p(0.0, 0, 0));
        pts.add(p(0.0, 1, 1));
        pts.add(p(0.0, 20, 20));
        pts.add(p(0.0, 21, 21));
        pts.add(p(0.0, 22, 22));

        slide(core, pts, 10 - 100, 10);     // windowEnd=10：插入 arrival 0,1 → PD
        slide(core, pts, 30 - 100, 30);     // windowEnd=30：插入 20,21,22 → 形成微簇（含 0,1,20,21,22）
        assertEquals(1, core.getState().mc.size(), "windowEnd=30 应有 1 个微簇");
        assertEquals(5, core.getState().mc.get(1).points.size());

        slide(core, pts, 0, 100);           // windowEnd=100：淘汰 arrival<10（即 0,1）→ 缩到 3 → 解体重插
        McodState st = core.getState();
        assertTrue(st.mc.isEmpty(), "微簇应已解体 / micro-cluster dissolved");
        assertEquals(3, st.pd.size(), "剩余 3 点应重插回 PD");
        assertTrue(st.pd.containsKey(20L) && st.pd.containsKey(21L) && st.pd.containsKey(22L),
                "重插的应是幸存的 20/21/22");
    }

    @Test
    void safeInlierShortCircuit() {
        // A 在 x=0，其后 3 个（=k）邻居在 x=0.9（≤R=1 但 >R/2，故 A 留在 PD 不成簇）。
        // A 累计 count_after=3=k → safe_inlier=true。
        McodCore core = new McodCore(1.0, 3, 10, new McodState());
        List<McodPoint> pts = new ArrayList<>();
        pts.add(p(0.0, 0, 100));    // A
        pts.add(p(0.9, 1, 1));
        pts.add(p(0.9, 2, 2));
        pts.add(p(0.9, 3, 3));
        slide(core, pts, -90, 10);   // 宽窗，无淘汰
        McodPoint a = core.getState().pd.get(100L);
        assertTrue(a != null, "A 应留在 PD");
        assertEquals(3, a.count_after, "A 应累计 3 个后到邻居");
        assertTrue(a.safe_inlier, "count_after 达到 k → safe_inlier 应为 true");
    }

    @Test
    void pruningBranchesRHalfAndThreeHalfR() {
        // 先在 x=0 形成微簇（中心 x=0）。再插两点：
        //   P 在 x=1.0：R/2(0.5) < 1.0 ≤ 3R/2(1.5) → 不并簇、但把该簇记入 P.Rmc（3R/2 分支）。
        //   Q 在 x=0.4：≤ R/2 → 并入该簇（R/2 分支）。
        McodCore core = new McodCore(1.0, 3, 10, new McodState());
        List<McodPoint> pts = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pts.add(p(0.0, i, i));           // 微簇（arrival 0..4）
        }
        pts.add(p(1.0, 10, 200));            // P
        pts.add(p(0.4, 11, 201));            // Q
        slide(core, pts, 10 - 100, 10);      // 形成微簇
        slide(core, pts, 20 - 100, 20);      // 插入 P, Q
        McodState st = core.getState();

        McodPoint pP = st.pd.get(200L);
        assertTrue(pP != null, "P 应在 PD（3R/2 内但 >R/2，不并簇）");
        assertEquals(-1, pP.mc, "P 不属于任何微簇");
        assertTrue(pP.Rmc.contains(1), "P 应把该微簇记入 Rmc（3R/2 剪枝分支）");

        assertFalse(st.pd.containsKey(201L), "Q 应已并入微簇、不在 PD");
        assertTrue(st.mc.get(1).points.stream().anyMatch(x -> x.id == 201L),
                "Q 应成为该微簇成员（R/2 分支）");
    }
}
