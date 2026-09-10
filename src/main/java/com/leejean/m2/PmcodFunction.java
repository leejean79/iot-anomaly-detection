package com.leejean.m2;

import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m3.AnnotatedRound;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * pMCOD 的 Flink 窗口算子（交接文档 §4）：把 {@link McodCore}（纯算法核）接到事件时间滑动窗口上，
 * 每设备一实例（keyBy(deviceId)），每个滑动步输出离群点名单与三路监测信号。
 * The Flink window operator for pMCOD (handover §4): wraps the pure {@link McodCore} on an event-time
 * sliding window, one instance per device, emitting the outlier list and three monitoring signals per slide.
 *
 * <p>对应原文 {@code Pmcod.scala:17,27} 的 {@code ProcessWindowFunction[(Int, Data), (Long, Int), Int, TimeWindow]}；
 * 我们把每设备的 {@link McodState}（含 mcCounter，R8 修复）放进受 checkpoint 保护的 keyed ValueState。
 *
 * <p>主输出：{@link ScoreEvent}（离群点名单）→ synergia-scores。
 * 侧输出（{@link #m2MonitoringTag}）：{@link MonitoringSnapshot}（M2 字段）→ 并入 synergia-monitoring。
 * 侧输出（{@link #m3AnnotatedTag}，可选）：{@link AnnotatedRound}——每个滑动步内的轮附带离群标记，
 * 供 M3 LSTM 自编码器的训练数据净化与推理（交接文档 §3 授权变更）。
 * Optional side output ({@link #m3AnnotatedTag}): each round in the current slide annotated with its
 * outlier flag, consumed by the M3 LSTM autoencoder for training sanitization and inference (handover §3).
 */
public class PmcodFunction
        extends ProcessWindowFunction<DevicePoint, ScoreEvent, String, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private final double r;                      // 全局半径 R（未在映射中的设备回退到它）
    private final int k;
    private final long slideMs;                 // 滑动步（毫秒）= S 秒 × 1000
    private final Map<String, Double> rPerDevice;   // 逐设备半径 R（收尾任务；空 = 全用全局 r）
    private final OutputTag<MonitoringSnapshot> m2MonitoringTag;
    private final OutputTag<AnnotatedRound> m3AnnotatedTag;   // null = M3 未启用，不发标注轮

    private transient ValueState<McodState> state;
    private transient Counter outliersTotal;
    private transient Counter pointsTotal;
    private transient Counter mcPointsTotal;
    private transient Counter windowsTotal;
    private transient Counter coldClears;

    /** 全局单 R 构造（向后兼容：所有设备用同一 R，不启用 M3 侧输出）。/ single global-R constructor (all devices same R, no M3). */
    public PmcodFunction(double r, int k, int slideSeconds, OutputTag<MonitoringSnapshot> m2MonitoringTag) {
        this(r, k, slideSeconds, new HashMap<>(), m2MonitoringTag, null);
    }

    /** 逐设备 R 构造（不启用 M3 侧输出）。/ per-device R, no M3 side output. */
    public PmcodFunction(double r, int k, int slideSeconds,
                         Map<String, Double> rPerDevice, OutputTag<MonitoringSnapshot> m2MonitoringTag) {
        this(r, k, slideSeconds, rPerDevice, m2MonitoringTag, null);
    }

    /**
     * 完整构造：逐设备 R + M3 标注轮侧输出。/ full constructor: per-device R + M3 annotated-round side output.
     * @param m3AnnotatedTag null 则不发标注轮（M3 未启用时）/ null disables annotated-round emission
     */
    public PmcodFunction(double r, int k, int slideSeconds,
                         Map<String, Double> rPerDevice, OutputTag<MonitoringSnapshot> m2MonitoringTag,
                         OutputTag<AnnotatedRound> m3AnnotatedTag) {
        this.r = r;
        this.k = k;
        this.slideMs = slideSeconds * 1000L;
        // 拷进可序列化的 HashMap（算子会被序列化分发到 TaskManager）/ copy into a serializable HashMap
        this.rPerDevice = rPerDevice == null ? new HashMap<>() : new HashMap<>(rPerDevice);
        this.m2MonitoringTag = m2MonitoringTag;
        this.m3AnnotatedTag = m3AnnotatedTag;
    }

    @Override
    public void open(Configuration parameters) {
        state = getRuntimeContext().getState(
                new ValueStateDescriptor<>("m2-mcod-state", McodState.class));
        outliersTotal = getRuntimeContext().getMetricGroup().counter("m2_outliers_total");
        pointsTotal = getRuntimeContext().getMetricGroup().counter("m2_points_total");
        mcPointsTotal = getRuntimeContext().getMetricGroup().counter("m2_mc_points_total");
        windowsTotal = getRuntimeContext().getMetricGroup().counter("m2_windows_total");
        coldClears = getRuntimeContext().getMetricGroup().counter("m2_state_cold_clears");
    }

    @Override
    public void process(String device, Context ctx, Iterable<DevicePoint> elements,
                        Collector<ScoreEvent> out) throws Exception {
        long windowStart = ctx.window().getStart();
        long windowEnd = ctx.window().getEnd();

        // 物化点（窗口 Iterable 需多次遍历：插入过滤 + 删除过滤）/ materialize (iterated twice)
        List<McodPoint> points = new ArrayList<>();
        // M3 需要本滑动步的 DevicePoint（含 censoredMask）；仅在 m3AnnotatedTag 非 null 时收集
        // M3 needs this slide's DevicePoints (with censoredMask); collected only when m3AnnotatedTag != null
        List<DevicePoint> slideDevicePoints = m3AnnotatedTag != null ? new ArrayList<>() : null;
        boolean coldSignal = false;
        for (DevicePoint dp : elements) {
            McodPoint p = dp.getPoint();
            points.add(p);
            boolean inSlide = p.arrival >= windowEnd - slideMs;
            // 冷启动信号只可能来自本滑动步新到的点 / cold signal only from this slide's new points
            if (p.coldStart && inSlide) {
                coldSignal = true;
            }
            if (slideDevicePoints != null && inSlide) {
                slideDevicePoints.add(dp);
            }
        }

        McodState st = state.value();
        if (st == null) {
            st = new McodState();
        }
        // 冷启动：先清空该设备状态再照常处理（§3.2 决策二）
        if (coldSignal) {
            st.clearForColdStart();
            coldClears.inc();
        }

        // 逐设备半径：映射里有就用，没有回退到全局 r（收尾任务；不改算法、只改该设备用哪个 R）
        double rEff = rPerDevice.getOrDefault(device, r);
        McodCore core = new McodCore(rEff, k, slideMs, st);
        McodCore.McodResult result = core.processSlide(points, windowStart, windowEnd);
        state.update(st);   // 持久化状态（含 mcCounter；对任意后端都显式写回）

        long windowEndSec = windowEnd / 1000L;
        // 主输出：离群点名单 / outlier list
        for (long id : result.outlierIds) {
            out.collect(new ScoreEvent(device, id, windowEndSec));
        }

        // 三路监测信号 / three monitoring signals
        double outlierRate = result.windowPoints > 0
                ? (double) result.outlierIds.size() / result.windowPoints : 0.0;
        double mcOccupancy = result.windowPoints > 0
                ? (double) result.mcPoints / result.windowPoints : 0.0;
        double p10 = percentile(result.pdNeighborCounts, 10);
        double p50 = percentile(result.pdNeighborCounts, 50);

        MonitoringSnapshot snap = new MonitoringSnapshot();
        snap.setDevice(device);
        snap.setTs(windowEndSec);
        snap.setWindowEnd(windowEndSec);
        snap.setM2OutlierRate(outlierRate);
        snap.setM2McOccupancy(mcOccupancy);
        snap.setM2NeighborCountP10(p10);
        snap.setM2NeighborCountP50(p50);
        snap.setM2Outliers(result.outlierIds.size());
        snap.setM2WindowPoints(result.windowPoints);
        snap.setM2ColdCleared(coldSignal);   // 本滑动步是否发生冷启动清空（供浪涌分析定位清空时刻）
        ctx.output(m2MonitoringTag, snap);

        // M3 侧输出：本滑动步的每个轮附带离群标记（M3 训练净化 + 推理输入）
        // M3 side output: each round in this slide annotated with its outlier flag
        if (m3AnnotatedTag != null && slideDevicePoints != null) {
            Set<Long> outlierSet = new HashSet<>(result.outlierIds);   // 离群 id 集合，供 O(1) 查询 / outlier id set for O(1) lookup
            for (DevicePoint dp : slideDevicePoints) {
                McodPoint mp = dp.getPoint();
                boolean isOutlier = outlierSet.contains(mp.id);        // 该轮是否被判离群 / is this round an outlier
                double[] xNorm = mp.value != null ? mp.value.clone() : new double[0];   // 防对象复用 / defensive copy
                boolean[] censored = dp.getCensoredMask() != null
                        ? dp.getCensoredMask().clone() : null;
                // 每轮发一条 AnnotatedRound 到 M3 侧输出 / emit one AnnotatedRound per round to the M3 side output
                ctx.output(m3AnnotatedTag, new AnnotatedRound(
                        device, mp.id, xNorm, isOutlier, censored, mp.coldStart, windowEndSec));
            }
        }

        outliersTotal.inc(result.outlierIds.size());
        pointsTotal.inc(result.windowPoints);
        mcPointsTotal.inc(result.mcPoints);
        windowsTotal.inc();
    }

    /**
     * 最近秩百分位（nearest-rank）：对升序数据取第 ceil(q/100 × n) 个（1 基）。空数据返回 0。
     * Nearest-rank percentile over the neighbour-count multiset (PD points), per handover §5.2.
     */
    static double percentile(int[] counts, double q) {
        int n = counts.length;
        if (n == 0) {
            return 0.0;
        }
        int[] sorted = counts.clone();
        java.util.Arrays.sort(sorted);
        int rank = (int) Math.ceil(q / 100.0 * n);   // 1-based
        if (rank < 1) {
            rank = 1;
        }
        if (rank > n) {
            rank = n;
        }
        return sorted[rank - 1];
    }
}
