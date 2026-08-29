package com.leejean.m2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * MCOD 数据点：自 pMCOD 源 {@code common_utils/Data.scala} 忠实迁移并**裁剪**（交接文档 §2.2）。
 * MCOD data point: a faithful, trimmed port of {@code common_utils/Data.scala} (handover §2.2).
 *
 * <p>只保留 MCOD 需要的字段：value、arrival、flag、id、count_after、nn_before、safe_inlier、mc、Rmc。
 * 删除的字段（为 slicing / 多参数查询 / 事件队列等其他变体准备）：slices_before、last_check、
 * nn_before_set、count_after_set、lSky、node_type；不实现 mtree 的 EuclideanCoordinate 接口（不引依赖）。
 * Only the MCOD-relevant fields are kept; the slicing / pAMCOD / event-queue fields are dropped and the
 * mtree EuclideanCoordinate interface is not implemented (that dependency is not introduced).
 *
 * <p><b>与原文的差异 / differences from the source</b>：
 * <ol>
 *   <li>点标识 {@code id} 由 {@code Int} 拓宽为 {@code long}，取轮的时间戳秒（同设备内唯一；决策六）。
 *       {@code id} widened from Int to long, taking the round timestamp in seconds (unique within a device).</li>
 *   <li>{@code value} 用 {@code double[]}（原文 {@code ListBuffer[Double]}），语义相同、更省。
 *       {@code value} is a {@code double[]} instead of a {@code ListBuffer[Double]}; same semantics.</li>
 *   <li>{@code coldStart} 为 M2 接线用的元数据（非算法字段、McodCore 从不读它），仅供窗口函数在冷启动轮
 *       到达时清空该设备状态；不参与任何距离/邻居计算。
 *       {@code coldStart} is M2 plumbing metadata (not an algorithm field; McodCore never reads it),
 *       used only by the window function to clear a device's state on a cold-start round.</li>
 * </ol>
 * {@code flag} 保留但恒为 0（原文用它区分本分区点与复制来的支援点；我们不复制点，语义退化但保留使对照成立）。
 */
public class McodPoint implements Serializable {
    private static final long serialVersionUID = 1L;

    // ---- 算法核心字段（对应 Data.scala 保留项）/ core fields kept from Data.scala ----
    public final double[] value;        // Data.scala:18 val value（标准化五维向量 xNorm）
    public final long arrival;          // Data.scala:19 val arrival（轮时间戳毫秒）
    public final int flag;              // Data.scala:23 val flag（恒为 0）
    public final long id;               // Data.scala:24 val id（轮时间戳秒，long）

    public int count_after = 0;                       // Data.scala:25 后到邻居数
    public final List<Long> nn_before = new ArrayList<>();  // Data.scala:26 先到邻居的 arrival（容量截断到 k）
    public boolean safe_inlier = false;               // Data.scala:27 安全内点短路标志
    public int mc = -1;                               // Data.scala:30 所属微簇编号（-1 = 在 PD 中）
    public final Set<Integer> Rmc = new HashSet<>();  // Data.scala:31 半径内的候选微簇编号集

    // ---- M2 接线元数据（非 Scala Data 字段）/ M2 plumbing (not from Scala Data) ----
    public final boolean coldStart;     // 冷启动轮：窗口函数据此清空该设备 MCOD 状态

    public McodPoint(double[] value, long arrival, int flag, long id) {
        this(value, arrival, flag, id, false);
    }

    public McodPoint(double[] value, long arrival, int flag, long id, boolean coldStart) {
        this.value = value;
        this.arrival = arrival;
        this.flag = flag;
        this.id = id;
        this.coldStart = coldStart;
    }

    /** 维数 / number of dimensions（Data.scala:98 dimensions()）。 */
    public int dimensions() {
        return value.length;
    }

    /**
     * 插入一个先到邻居的 arrival，容量截断到 k（保留最新的 k 个）。忠实迁移 Data.scala:49-59。
     * Insert a preceding neighbour's arrival, capping at k newest. Faithful port of Data.scala:49-59.
     */
    public void insert_nn_before(long el, int k) {
        if (nn_before.size() == k) {
            long tmp = min(nn_before);
            if (el > tmp) {
                nn_before.remove(tmp);   // 移除最旧 / drop the oldest (min)
                nn_before.add(el);
            }
        } else {
            nn_before.add(el);
        }
    }

    /** 窗口内仍有效的先到邻居数：nn_before 中 arrival ≥ windowStart 的个数（Pmcod.scala:48）。 */
    public int countNnBeforeGE(long windowStart) {
        int n = 0;
        for (long a : nn_before) {
            if (a >= windowStart) {
                n++;
            }
        }
        return n;
    }

    /**
     * 清空点的邻居元数据并设新微簇编号。忠实迁移 Data.scala:67-74（只保留 MCOD 相关三行：
     * nn_before.clear / count_after=0 / mc=newMc；原文的 AMCOD/KSKY 清理行已随字段裁剪一并删除）。
     * Port of Data.scala:67-74 keeping only the three MCOD lines.
     */
    public void clear(int newMc) {
        nn_before.clear();
        count_after = 0;
        mc = newMc;
    }

    private static long min(List<Long> xs) {
        long m = xs.get(0);
        for (int i = 1; i < xs.size(); i++) {
            if (xs.get(i) < m) {
                m = xs.get(i);
            }
        }
        return m;
    }

    /** id 唯一（同设备内），故 equals/hashCode 以 id 为准——供 MC.points 的按值删除（Pmcod.scala:143）。 */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof McodPoint)) {
            return false;
        }
        return id == ((McodPoint) o).id;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public String toString() {
        return "(" + id + "," + arrival + ")";   // 对齐 Data.scala:122
    }
}
