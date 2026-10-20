package com.leejean.m5;

import com.leejean.m1.DeviceKeys;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m3.M3ScoreRecord;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 性能层漂移监测算子（M5b 交接文档决定四、五）：按设备消费上下文通道的评分窗，把主分（或每小时主分的中位数）送进最优窗口漂移
 * 检测器，经持续性核对后把候选、确认与撤销事件作为主输出发出（写到 {@code synergia-drift}），并每 60 秒（处理时间）发出一条
 * 监测快照（{@code windowEnd = -2}，写到 {@code synergia-monitoring}）。
 * Performance-layer drift operator: per device, feeds the main score (or the hourly median) into the OPTWIN detector and
 * the persistence gate, emits candidate/confirm/revoke events, and a monitoring snapshot every 60 s of processing time.
 *
 * <p>检测器、持续性核对与小时累积都是受检查点保护的分键状态；常量表在 {@link #open} 中按参数构建，是常量，不进状态。
 * 评分窗由 M3 的输出水位线保持保证按事件时间有序到达（补遗四）。
 * Detector, gate and hourly accumulator are keyed checkpointed state; the tables are constants built in open().
 *
 * <p>小时粒度：元素为按世界协调时整点小时分组的主分中位数，事件时间取该小时的起点；下一个小时的第一个评分窗到达时关闭上一个
 * 小时，另在该小时起点之后两小时登记一个事件时间定时器作兜底（例如数据中断时）。分组与离线工具 {@code m5b/prepare_streams.py}
 * 相同。/ Hourly: median per UTC hour, closed by the next hour's first window or by an event-time timer two hours on.
 *
 * <p>交接文档决定五要求设备冷启动、相位回到收集态时清空该台的状态；现有的上下文异常检测模块在线之后不会回到收集态（重训属于
 * 适应执行模块），评分流里也没有相位信号，所以本阶段没有触发这一清空的输入，{@link OptWinDetector#clear} 与
 * {@link DriftGate#clear} 留作接口。/ No cold-start signal exists in this stage; the clear() hooks are kept for the adaptation module.
 */
public class M5bFunction extends KeyedProcessFunction<String, M3ScoreRecord, DriftEvent> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(M5bFunction.class);

    /** 性能层监测快照的 windowEnd 标记（M3 训练态快照为 −1）/ windowEnd marker of M5b snapshots. */
    public static final long M5B_SNAPSHOT_WINDOW_END = -2L;
    private static final long SNAPSHOT_INTERVAL_MS = 60_000L;
    private static final long HOUR_FALLBACK_SEC = 7200L;

    /** 元素粒度 / element granularity. */
    public enum Granularity { TEN_MINUTES, HOURLY }

    private final double confidence;
    private final double robustness;
    private final int wMax;
    private final int k;
    private final Granularity granularity;
    private final OutputTag<MonitoringSnapshot> monitoringTag;

    private transient OptWinTables tables;
    private transient ValueState<OptWinDetector> detector;
    private transient ValueState<DriftGate> gate;
    private transient ValueState<HourBucket> bucket;
    private transient ValueState<long[]> marks;      // [最近元素时刻, 最近候选, 最近确认, 快照定时器] / last element, candidate, confirm, timer

    /** 小时累积 / hourly accumulator. */
    public static final class HourBucket implements Serializable {
        private static final long serialVersionUID = 1L;
        public long hour;
        public double[] values = new double[8];
        public int n;

        void add(double x) {
            if (n == values.length) {
                values = Arrays.copyOf(values, n * 2);
            }
            values[n++] = x;
        }

        /** 中位数，偶数个时取中间两个的平均（与 Python statistics.median 相同）/ median, as statistics.median. */
        double median() {
            double[] s = Arrays.copyOf(values, n);
            Arrays.sort(s);
            return n % 2 == 1 ? s[n / 2] : (s[n / 2 - 1] + s[n / 2]) / 2.0;
        }
    }

    public M5bFunction(double confidence, double robustness, int wMax, int k, Granularity granularity,
                       OutputTag<MonitoringSnapshot> monitoringTag) {
        if (k < DriftGate.MEDIAN_SPAN) {
            throw new IllegalArgumentException("--m5b-k 不得小于 " + DriftGate.MEDIAN_SPAN);
        }
        this.confidence = confidence;
        this.robustness = robustness;
        this.wMax = wMax;
        this.k = k;
        this.granularity = granularity;
        this.monitoringTag = monitoringTag;
    }

    @Override
    public void open(Configuration parameters) {
        long t0 = System.currentTimeMillis();
        tables = OptWinTables.build(confidence, robustness, wMax);
        LOG.info("[M5b] tables built in {} ms: confidence={}, robustness={}, wmax={}, K={}, granularity={}",
                 System.currentTimeMillis() - t0, confidence, robustness, wMax, k, granularity);
        detector = getRuntimeContext().getState(new ValueStateDescriptor<>("m5b-detector",
                TypeInformation.of(OptWinDetector.class)));
        gate = getRuntimeContext().getState(new ValueStateDescriptor<>("m5b-gate", TypeInformation.of(DriftGate.class)));
        bucket = getRuntimeContext().getState(new ValueStateDescriptor<>("m5b-hour-bucket",
                TypeInformation.of(HourBucket.class)));
        marks = getRuntimeContext().getState(new ValueStateDescriptor<>("m5b-marks", TypeInformation.of(long[].class)));
    }

    @Override
    public void processElement(M3ScoreRecord r, Context ctx, Collector<DriftEvent> out) throws Exception {
        long[] m = marksOrNew();
        if (m[3] == 0) {                                // 首个元素时开始快照定时器 / start the snapshot timer
            m[3] = ctx.timerService().currentProcessingTime() + SNAPSHOT_INTERVAL_MS;
            ctx.timerService().registerProcessingTimeTimer(m[3]);
        }
        if (granularity == Granularity.TEN_MINUTES) {
            feed(r.getDevice(), r.getWindowEnd(), r.getMainScore(), m, out);
        } else {
            long hour = r.getWindowEnd() / 3600L * 3600L;
            HourBucket b = bucket.value();
            if (b != null && b.hour != hour) {
                feed(r.getDevice(), b.hour, b.median(), m, out);
                b = null;
            }
            if (b == null) {
                b = new HourBucket();
                b.hour = hour;
                ctx.timerService().registerEventTimeTimer((hour + HOUR_FALLBACK_SEC) * 1000L);
            }
            b.add(r.getMainScore());
            bucket.update(b);
        }
        marks.update(m);
    }

    /** 一个元素进检测器与持续性核对 / feed one element. */
    private void feed(String device, long ts, double x, long[] m, Collector<DriftEvent> out) throws Exception {
        OptWinDetector d = detector.value();
        DriftGate g = gate.value();
        if (d == null) {
            d = new OptWinDetector(wMax);
            g = new DriftGate(k, robustness);
        }
        for (DriftEvent e : g.onElement(device, ts, x, d.update(x, tables))) {
            out.collect(e);
            if (e.level == DriftEvent.Level.CANDIDATE) {
                m[1] = ts;
            } else if (e.level == DriftEvent.Level.CONFIRM) {
                m[2] = ts;
            }
        }
        m[0] = ts;
        detector.update(d);
        gate.update(g);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DriftEvent> out) throws Exception {
        long[] m = marksOrNew();
        if (ctx.timeDomain() == TimeDomain.EVENT_TIME) {
            HourBucket b = bucket.value();
            if (b != null && (b.hour + HOUR_FALLBACK_SEC) * 1000L == timestamp) {   // 兜底关闭小时 / fallback close
                feed(DeviceKeys.deviceOf(ctx.getCurrentKey()), b.hour, b.median(), m, out);
                bucket.clear();
                marks.update(m);
            }
            return;
        }
        if (timestamp != m[3]) {
            return;
        }
        OptWinDetector d = detector.value();
        if (monitoringTag != null) {
            MonitoringSnapshot s = new MonitoringSnapshot();
            s.setDevice(DeviceKeys.deviceOf(ctx.getCurrentKey()));   // 代理键还原为设备 / surrogate key → device
            s.setTs(m[0]);
            s.setWindowEnd(M5B_SNAPSHOT_WINDOW_END);
            s.setM5bWindowLen(d == null ? 0 : d.windowLength());
            s.setM5bLastCandidateTs(m[1]);
            s.setM5bLastConfirmTs(m[2]);
            s.setM5bFWithoutRise(d == null ? 0 : d.fWithoutRiseCount());
            s.setM5bTSkippedConstant(d == null ? 0 : d.tSkippedConstantCount());
            ctx.output(monitoringTag, s);
        }
        m[3] = timestamp + SNAPSHOT_INTERVAL_MS;
        ctx.timerService().registerProcessingTimeTimer(m[3]);
        marks.update(m);
    }

    private long[] marksOrNew() throws Exception {
        long[] m = marks.value();
        return m == null ? new long[4] : m;
    }
}
