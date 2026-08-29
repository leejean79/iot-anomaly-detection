package com.leejean.m2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * MCOD 算法核（纯类，不依赖 Flink）：忠实迁移 {@code outlier/Pmcod.scala} 的算法逻辑
 * （insertPoint / deletePoint / createMC / insertToMC / findCloseMCs / addNeighbor 及 process 主体），
 * 便于单元测试与朴素对照器逐窗口比对（交接文档 §2、§6）。
 * The MCOD algorithm core (a pure, Flink-free class): a faithful port of the algorithm in
 * {@code Pmcod.scala}, so it can be unit-tested and pitted against the naive oracle per handover §2/§6.
 *
 * <p>剪枝两级：微簇按 3R/2 找候选、按 R/2 归属；PD 内按 R 更新邻居、R/2 划入 NC、(R/2, 3R/2] 划入 NNC。
 * safe_inlier 短路：后到邻居数达到 k 即永久内点。忠实保留 R/2 与 3R/2 两级剪枝分支。
 * Two-level pruning at 3R/2 and R/2, with the safe_inlier short-circuit — all preserved from the source.
 *
 * <p><b>与原文的差异 / difference from the source</b>：{@link #deletePoint} 不再信任传入元素的可变字段
 * {@code el.mc}（原文 Pmcod.scala:140 依赖窗口缓冲区的对象同一性——仅在堆状态后端成立），改为**按 id 在
 * 状态中定位**该点当前所在（PD 或某微簇），效果等价但对任意状态后端（含序列化）都正确、更稳健。其余逐行对照。
 * {@code deletePoint} looks the point up by id in the state instead of trusting {@code el.mc} on the
 * passed (possibly re-deserialized) window element; the effect is identical but robust to any state
 * backend. Everything else is line-for-line.
 */
public class McodCore {

    private final double r;
    private final double halfR;         // R/2
    private final double threeHalfR;    // 3R/2
    private final int k;
    private final long slide;           // time_slide（毫秒，与 arrival 同单位）
    private McodState state;

    public McodCore(double r, int k, long slide, McodState state) {
        this.r = r;
        this.halfR = r / 2.0;
        this.threeHalfR = 3.0 * r / 2.0;
        this.k = k;
        this.slide = slide;
        this.state = state;
    }

    public McodState getState() {
        return state;
    }

    public void setState(McodState state) {
        this.state = state;
    }

    /**
     * 处理一个滑动步：忠实迁移 {@code Pmcod.scala:27-76} 的 process 主体——
     * 先插入本步新到点、再统计离群、再删除滑出点、再处理缩水微簇的删除与成员重插。
     * Process one slide: faithful port of the {@code process} body — insert the new slide, count
     * outliers, remove the expiring slide, then dissolve shrunk micro-clusters and reinsert members.
     *
     * @return 本步的离群统计（含离群点 id 集合与监测信号所需的原料，均在"删除滑出点之前"抓取）。
     */
    public McodResult processSlide(Iterable<McodPoint> elements, long windowStart, long windowEnd) {
        // 插入本滑动步新到的点（arrival ≥ windowEnd - slide）/ insert new elements (Pmcod.scala:40-42)
        for (McodPoint p : elements) {
            if (p.arrival >= windowEnd - slide) {
                insertPoint(p, true, null);
            }
        }

        // 统计离群：PD 中非 safe_inlier 且 flag==0，其"窗口内邻居数 < k"即离群（Pmcod.scala:45-53）。
        // MC 内的点按构造恒有 ≥k 个 R 内邻居，永不离群，故只需遍历 PD。
        List<Long> outliers = new ArrayList<>();
        int[] pdNeighborCounts = new int[state.pd.size()];
        int idx = 0;
        for (McodPoint p : state.pd.values()) {
            int neigh = p.count_after + p.countNnBeforeGE(windowStart);
            pdNeighborCounts[idx++] = neigh;
            if (!p.safe_inlier && p.flag == 0 && neigh < k) {
                outliers.add(p.id);
            }
        }

        // 监测信号原料（在删除滑出点之前抓取，与离群计数同一时刻）/ signal inputs, captured pre-eviction
        int mcPoints = 0;
        for (MicroCluster m : state.mc.values()) {
            mcPoints += m.points.size();
        }
        int windowPoints = state.pd.size() + mcPoints;

        // 删除滑出本窗口的点（arrival < windowStart + slide），收集缩水到 ≤k 的微簇（Pmcod.scala:56-62）
        Set<Integer> deletedMCs = new HashSet<>();
        for (McodPoint p : elements) {
            if (p.arrival < windowStart + slide) {
                int delete = deletePoint(p);
                if (delete > 0) {
                    deletedMCs.add(delete);
                }
            }
        }

        // 删除缩水微簇并重插其成员（Pmcod.scala:64-75）
        if (!deletedMCs.isEmpty()) {
            List<McodPoint> reinsert = new ArrayList<>();
            for (int mcId : deletedMCs) {
                MicroCluster m = state.mc.get(mcId);
                if (m != null) {
                    reinsert.addAll(m.points);
                    state.mc.remove(mcId);
                }
            }
            Set<Long> reinsertIds = new HashSet<>();
            for (McodPoint p : reinsert) {
                reinsertIds.add(p.id);
            }
            for (McodPoint p : reinsert) {
                insertPoint(p, false, reinsertIds);
            }
        }

        return new McodResult(outliers, windowPoints, mcPoints, pdNeighborCounts);
    }

    /** 忠实迁移 {@code Pmcod.scala:78-136}。 */
    private void insertPoint(McodPoint el, boolean newPoint, Set<Long> reinsert) {
        if (!newPoint) {
            el.clear(-1);   // 重插前清空邻居元数据并回落到 PD 语义（Pmcod.scala:80）
        }
        // 先按 3R/2 找候选微簇，取最近者 / find close MCs (3R/2), pick the closest (Pmcod.scala:82-87)
        Map<Integer, Double> closeMCs = findCloseMCs(el);
        int closerMcId = 0;
        double closerMcDist = Double.MAX_VALUE;
        for (Map.Entry<Integer, Double> e : closeMCs.entrySet()) {
            if (e.getValue() < closerMcDist) {
                closerMcDist = e.getValue();
                closerMcId = e.getKey();
            }
        }

        if (closerMcDist <= halfR) {   // 最近微簇在 R/2 内 → 并入该微簇（Pmcod.scala:88-95）
            insertToMC(el, closerMcId, newPoint, reinsert);
        } else {                        // 否则对照 PD（Pmcod.scala:96-134）
            List<McodPoint> nc = new ArrayList<>();
            List<McodPoint> nnc = new ArrayList<>();
            for (McodPoint p : state.pd.values()) {
                double thisDistance = McodDistance.distance(el, p);
                if (thisDistance <= threeHalfR) {
                    if (thisDistance <= r) {          // R 内 → 互记邻居（Pmcod.scala:103-113）
                        addNeighbor(el, p);
                        if (newPoint) {
                            addNeighbor(p, el);
                        } else if (reinsert.contains(p.id)) {
                            addNeighbor(p, el);
                        }
                    }
                    if (thisDistance <= halfR) {
                        nc.add(p);
                    } else {
                        nnc.add(p);
                    }
                }
            }

            if (nc.size() >= k) {   // R/2 内近邻攒够 k 个 → 创建新微簇（Pmcod.scala:119-121）
                createMC(el, nc, nnc);
            } else {                 // 否则入 PD，并记录候选微簇、补记微簇成员邻居（Pmcod.scala:122-133）
                for (Integer mcId : closeMCs.keySet()) {
                    el.Rmc.add(mcId);
                }
                for (Map.Entry<Integer, MicroCluster> e : state.mc.entrySet()) {
                    if (closeMCs.containsKey(e.getKey())) {
                        for (McodPoint p : e.getValue().points) {
                            if (McodDistance.distance(el, p) <= r) {
                                addNeighbor(el, p);
                            }
                        }
                    }
                }
                state.pd.put(el.id, el);
            }
        }
    }

    /**
     * 忠实迁移 {@code Pmcod.scala:138-147}，但按 id 在状态中定位（见类注差异说明）。
     * @return 若删除导致某微簇缩水到 ≤k 则返回其编号，否则返回 0。
     */
    private int deletePoint(McodPoint el) {
        if (state.pd.remove(el.id) != null) {
            return 0;   // 原在 PD，已移除
        }
        for (Map.Entry<Integer, MicroCluster> e : state.mc.entrySet()) {
            List<McodPoint> pts = e.getValue().points;
            boolean removed = false;
            for (int i = 0; i < pts.size(); i++) {
                if (pts.get(i).id == el.id) {
                    pts.remove(i);
                    removed = true;
                    break;
                }
            }
            if (removed) {
                return pts.size() <= k ? e.getKey() : 0;   // 缩水到 ≤k → 标记删除（Pmcod.scala:144）
            }
        }
        return 0;
    }

    /** 忠实迁移 {@code Pmcod.scala:149-160}。使用状态内的 mcCounter（R8 修复）。 */
    private void createMC(McodPoint el, List<McodPoint> nc, List<McodPoint> nnc) {
        int counter = state.mcCounter;
        for (McodPoint p : nc) {
            p.clear(counter);
            state.pd.remove(p.id);
        }
        el.clear(counter);
        nc.add(el);
        MicroCluster newMc = new MicroCluster(el.value, nc);
        state.mc.put(counter, newMc);
        for (McodPoint p : nnc) {
            p.Rmc.add(counter);
        }
        state.mcCounter = counter + 1;
    }

    /** 忠实迁移 {@code Pmcod.scala:162-179}。 */
    private void insertToMC(McodPoint el, int mc, boolean update, Set<Long> reinsert) {
        el.clear(mc);
        state.mc.get(mc).points.add(el);
        for (McodPoint p : state.pd.values()) {
            if (!p.Rmc.contains(mc)) {
                continue;
            }
            if (!update && !reinsert.contains(p.id)) {
                continue;
            }
            if (McodDistance.distance(p, el) <= r) {
                addNeighbor(p, el);
            }
        }
    }

    /** 忠实迁移 {@code Pmcod.scala:181-188}：3R/2 内的候选微簇及其距离。 */
    private Map<Integer, Double> findCloseMCs(McodPoint el) {
        Map<Integer, Double> res = new java.util.HashMap<>();
        for (Map.Entry<Integer, MicroCluster> e : state.mc.entrySet()) {
            double thisDistance = McodDistance.distance(el, e.getValue());
            if (thisDistance <= threeHalfR) {
                res.put(e.getKey(), thisDistance);
            }
        }
        return res;
    }

    /** 忠实迁移 {@code Pmcod.scala:190-197}：按先后关系更新 nn_before 或 count_after（safe_inlier 短路）。 */
    private void addNeighbor(McodPoint el, McodPoint neigh) {
        if (el.arrival > neigh.arrival) {
            el.insert_nn_before(neigh.arrival, k);
        } else {
            el.count_after += 1;
            if (el.count_after >= k) {
                el.safe_inlier = true;
            }
        }
    }

    /** 当前活跃点（PD ∪ 所有微簇成员）——供朴素对照器与监测统计。 */
    public List<McodPoint> activePoints() {
        List<McodPoint> all = new ArrayList<>(state.pd.values());
        for (MicroCluster m : state.mc.values()) {
            all.addAll(m.points);
        }
        return all;
    }

    /** 一个滑动步的结果（离群点 id 集合 + 监测信号原料，均在删除滑出点之前抓取）。 */
    public static final class McodResult {
        public final List<Long> outlierIds;
        public final int windowPoints;      // 窗口内总点数（PD + 所有微簇成员）
        public final int mcPoints;          // 微簇内点数
        public final int[] pdNeighborCounts;// 各 PD 点的窗口内邻居数（count_after + 有效 nn_before）

        McodResult(List<Long> outlierIds, int windowPoints, int mcPoints, int[] pdNeighborCounts) {
            this.outlierIds = outlierIds;
            this.windowPoints = windowPoints;
            this.mcPoints = mcPoints;
            this.pdNeighborCounts = pdNeighborCounts;
        }
    }
}
