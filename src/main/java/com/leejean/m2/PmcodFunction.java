package com.leejean.m2;

import com.leejean.m1.MonitoringSnapshot;
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
import java.util.List;
import java.util.Map;

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
 */
public class PmcodFunction
        extends ProcessWindowFunction<DevicePoint, ScoreEvent, String, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private final double r;                      // 全局半径 R（未在映射中的设备回退到它）
    private final int k;
    private final long slideMs;                 // 滑动步（毫秒）= S 秒 × 1000
    private final Map<String, Double> rPerDevice;   // 逐设备半径 R（收尾任务；空 = 全用全局 r）
    private final OutputTag<MonitoringSnapshot> m2MonitoringTag;

    private transient ValueState<McodState> state;
    private transient Counter outliersTotal;
    private transient Counter pointsTotal;
    private transient Counter mcPointsTotal;
    private transient Counter windowsTotal;
    private transient Counter coldClears;

    /** 全局单 R 构造（向后兼容：所有设备用同一 R）。/ single global-R constructor (all devices same R). */
    public PmcodFunction(double r, int k, int slideSeconds, OutputTag<MonitoringSnapshot> m2MonitoringTag) {
        this(r, k, slideSeconds, new HashMap<>(), m2MonitoringTag);
    }

    /** 逐设备 R 构造：rPerDevice 给出 设备→R；未列设备回退到全局 r。/ per-device R; unlisted falls back to r. */
    public PmcodFunction(double r, int k, int slideSeconds,
                         Map<String, Double> rPerDevice, OutputTag<MonitoringSnapshot> m2MonitoringTag) {
        this.r = r;
        this.k = k;
        this.slideMs = slideSeconds * 1000L;
        // 拷进可序列化的 HashMap（算子会被序列化分发到 TaskManager）/ copy into a serializable HashMap
        this.rPerDevice = rPerDevice == null ? new HashMap<>() : new HashMap<>(rPerDevice);
        this.m2MonitoringTag = m2MonitoringTag;
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
        boolean coldSignal = false;
        for (DevicePoint dp : elements) {
            McodPoint p = dp.getPoint();
            points.add(p);
            // 冷启动信号只可能来自本滑动步新到的点 / cold signal only from this slide's new points
            if (p.coldStart && p.arrival >= windowEnd - slideMs) {
                coldSignal = true;
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
