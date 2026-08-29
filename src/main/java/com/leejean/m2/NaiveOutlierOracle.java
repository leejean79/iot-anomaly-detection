package com.leejean.m2;

import java.util.ArrayList;
import java.util.List;

/**
 * 朴素暴力对照器（交接文档 §6，验收核心）：按定义直接计算的参考实现。
 * The naive brute-force oracle (handover §6, the core of acceptance): the reference by definition.
 *
 * <p>给定窗口内全部点与参数 (R, k)，对每个点数一遍"距离不超过 R 的邻居个数"（不含自身），少于 k 即离群。
 * 复杂度 O(n²)，不做任何剪枝或增量优化。MCOD 是精确算法，两者的逐窗口离群点 id 集合必须完全相等。
 * Given all points in a window and (R, k), count for each point the neighbours within distance R
 * (excluding itself); fewer than k means outlier. O(n²), no pruning. MCOD is exact, so the two must
 * produce identical outlier id sets per window.
 */
public final class NaiveOutlierOracle {

    private NaiveOutlierOracle() { }

    /**
     * 返回离群点 id 列表（升序，便于断言比较）。
     * @param points 窗口内的全部活跃点 / all active points in the window
     * @param r 半径 R；@param k 邻居数阈值
     */
    public static List<Long> outliers(List<McodPoint> points, double r, int k) {
        List<Long> res = new ArrayList<>();
        int n = points.size();
        for (int i = 0; i < n; i++) {
            McodPoint p = points.get(i);
            int neighbours = 0;
            for (int j = 0; j < n; j++) {
                if (i == j) {
                    continue;
                }
                if (McodDistance.distance(p, points.get(j)) <= r) {
                    neighbours++;
                    if (neighbours >= k) {
                        break;   // 已达阈值，提前停（不改变结论，仅省算）
                    }
                }
            }
            if (neighbours < k) {
                res.add(p.id);
            }
        }
        res.sort(Long::compare);
        return res;
    }
}
