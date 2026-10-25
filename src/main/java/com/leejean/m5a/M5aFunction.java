package com.leejean.m5a;

import com.leejean.m1.DeviceKeys;
import com.leejean.m1.DeviceRound;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m2.ScoreEvent;
import com.leejean.m5.DriftEvent;
import com.leejean.m5.OptWinTables;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据层漂移监测算子（M5a 交接文档第三节）：第一路输入是数据接入模块的标准化轮，第二路是点异常检测模块的离群记录，两路按同一张
 * 设备代理键表分组。逻辑全部在 {@link M5aCore}；本类只负责状态与事件时间定时器：当地日结束时关闭当日（日记录写监测主题，状态转移
 * 写漂移主题），十分钟末加一个滑动步之后关闭该十分钟（离群率送入检测器，爆发写漂移主题）。
 * Data-layer drift operator: rounds and point-channel outliers, keyed by the same surrogate keys; logic lives in M5aCore,
 * this class handles state and event-time timers (day ends and ten-minute bins).
 *
 * <p>状态是一个 {@link M5aCore} 对象（受检查点保护）；参照直方图随冻结后的第一条轮到达后存入其中。检测器常量表在 {@link #open}
 * 中按参数构建，不进状态。/ State is one M5aCore object; the OPTWIN tables are constants built in open().
 */
public class M5aFunction extends KeyedCoProcessFunction<String, DeviceRound, ScoreEvent, DriftEvent> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(M5aFunction.class);

    private final M5aCore.Params params;
    private final double confidence;
    private final double robustness;
    private final int wMax;
    private final OutputTag<MonitoringSnapshot> recordTag;

    private transient OptWinTables tables;
    private transient ValueState<M5aCore> core;

    public M5aFunction(M5aCore.Params params, double confidence, double robustness, int wMax,
                       OutputTag<MonitoringSnapshot> recordTag) {
        this.params = params;
        this.confidence = confidence;
        this.robustness = robustness;
        this.wMax = wMax;
        this.recordTag = recordTag;
    }

    @Override
    public void open(Configuration parameters) {
        long t0 = System.currentTimeMillis();
        tables = OptWinTables.build(confidence, robustness, wMax);
        LOG.info("[M5a] tables built in {} ms: confidence={}, robustness={}, wmax={}", System.currentTimeMillis() - t0,
                 confidence, robustness, wMax);
        core = getRuntimeContext().getState(new ValueStateDescriptor<>("m5a-core", TypeInformation.of(M5aCore.class)));
    }

    private M5aCore coreOrNew() throws Exception {
        M5aCore c = core.value();
        return c == null ? new M5aCore() : c;
    }

    @Override
    public void processElement1(DeviceRound r, Context ctx, Collector<DriftEvent> out) throws Exception {
        M5aCore c = coreOrNew();
        List<DriftEvent> events = new ArrayList<>();
        List<MonitoringSnapshot> records = new ArrayList<>();
        long before = c.openDayEnd();
        long dayEnd = c.onRound(r, params, events, records);
        if (dayEnd != before) {
            ctx.timerService().registerEventTimeTimer(dayEnd * 1000L);
        }
        // 该轮所在十分钟的关闭定时器（重复登记由 Flink 去重）/ close timer of this round's bin (deduplicated by Flink)
        ctx.timerService().registerEventTimeTimer(M5aCore.binCloseTime(latestBin(r.getTs()), params) * 1000L);
        emit(events, records, out, ctx);
        core.update(c);
    }

    @Override
    public void processElement2(ScoreEvent e, Context ctx, Collector<DriftEvent> out) throws Exception {
        M5aCore c = coreOrNew();
        c.onOutlier(e.getRoundTs(), e.getWindowEnd(), params);
        core.update(c);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DriftEvent> out) throws Exception {
        M5aCore c = core.value();
        if (c == null) {
            return;
        }
        String device = DeviceKeys.deviceOf(ctx.getCurrentKey());
        List<DriftEvent> events = new ArrayList<>();
        List<MonitoringSnapshot> records = new ArrayList<>();
        long t = timestamp / 1000L;
        c.closeBinsUpTo(t, device, params, tables, events);
        if (c.openDayEnd() > 0 && c.openDayEnd() <= t) {
            c.closeDay(device, params, events, records);
        }
        for (MonitoringSnapshot s : records) {
            ctx.output(recordTag, s);
        }
        for (DriftEvent e : events) {
            out.collect(e);
        }
        core.update(c);
    }

    private void emit(List<DriftEvent> events, List<MonitoringSnapshot> records, Collector<DriftEvent> out, Context ctx) {
        for (MonitoringSnapshot s : records) {
            ctx.output(recordTag, s);
        }
        for (DriftEvent e : events) {
            out.collect(e);
        }
    }

    private static long latestBin(long ts) {
        return Math.floorDiv(ts, 600L) * 600L + 600L;
    }
}
