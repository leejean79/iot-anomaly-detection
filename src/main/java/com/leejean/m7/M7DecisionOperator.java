package com.leejean.m7;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.leejean.m1.DeviceRound;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m2.ScoreEvent;
import com.leejean.m3.M3ScoreRecord;
import com.leejean.m5.DriftEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 聚合模块的在线算子（交接文档 1.25 版第六节；实现方案 {@code docs/reports/m7_online_plan.md} 第二节）：并行度为一，按常量键分区；
 * 合并后的 {@link M7Input} 送入 {@link M7InputAssembler}，事件时间定时器在每个告警窗口终点后六十秒触发节拍（1.23 版裁决第三点），
 * 决策节拍与日界复核随之进行。告警与复核写主输出（JSON，{@code type} 为 alert 或 review），命令写侧输出（JSON，字段即命令契约）。
 * 本算子只消费上游的输出，不改变上游任何输出。
 * The M7 online operator: parallelism one, constant key, event-time timers one slide after each alert-window end.
 */
public class M7DecisionOperator extends KeyedProcessFunction<Integer, M7Input, String> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(M7DecisionOperator.class);

    private final FleetDecisionFunction.Params params;
    private final OutputTag<String> commandTag;
    private transient ValueState<M7InputAssembler> asm;
    private transient ValueState<Long> next;
    private transient ObjectMapper mapper;
    private transient long dayNanos;
    private transient long dayMaxNanos;
    private transient int dayTicks;

    public M7DecisionOperator(FleetDecisionFunction.Params params, OutputTag<String> commandTag) {
        this.params = params;
        this.commandTag = commandTag;
    }

    @Override
    public void open(Configuration parameters) {
        asm = getRuntimeContext().getState(new ValueStateDescriptor<>("m7-assembler", TypeInformation.of(M7InputAssembler.class)));
        next = getRuntimeContext().getState(new ValueStateDescriptor<>("m7-next-tick", Types.LONG));
        mapper = new ObjectMapper();
    }

    @Override
    public void processElement(M7Input in, Context ctx, Collector<String> out) throws Exception {
        M7InputAssembler a = asm.value();
        if (a == null) {
            a = new M7InputAssembler(params);
        }
        a.accept(in);
        if (next.value() == null) {
            long at = ctx.timestamp() == null ? in.ts : Math.floorDiv(ctx.timestamp(), 1000L);
            long t = a.windowOf(at) + params.alertSec;
            next.update(t);
            ctx.timerService().registerEventTimeTimer((t + FleetDecisionFunction.SLIDE) * 1000L);
        }
        asm.update(a);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        M7InputAssembler a = asm.value();
        Long t = next.value();
        if (a == null || t == null || (t + FleetDecisionFunction.SLIDE) * 1000L != timestamp) {
            return;
        }
        long t0 = System.nanoTime();
        M7InputAssembler.Out o = new M7InputAssembler.Out();
        a.tick(t, o);
        long nanos = System.nanoTime() - t0;
        for (FleetDecisionFunction.Alert x : o.alerts) {
            out.collect(json("alert", x));
        }
        for (FleetDecisionFunction.Review x : o.review) {
            out.collect(json("review", x));
        }
        for (FleetDecisionFunction.Command x : o.commands) {
            ctx.output(commandTag, json("command", x));
        }
        dayNanos += nanos;
        dayMaxNanos = Math.max(dayMaxNanos, nanos);
        dayTicks++;
        String day = FleetDecisionFunction.localDay(t - 1);
        if (t == FleetDecisionFunction.dayStart(FleetDecisionFunction.addDays(day, 1))) {
            // 性能（验收项五）：日界节拍的耗时与当日各节拍的合计、最大值，以及状态的 Java 序列化字节数 / V-M7-5
            int bytes;
            try {
                bytes = InstantiationUtil.serializeObject(a).length;
            } catch (java.io.IOException e) {
                bytes = -1;
            }
            LOG.info("[M7] Day {} closed: boundary tick {} us; {} ticks total {} us, max {} us; alerts {} commands {} review {};"
                            + " state {} bytes", day, nanos / 1000L, dayTicks, dayNanos / 1000L, dayMaxNanos / 1000L,
                    o.alerts.size(), o.commands.size(), o.review.size(), bytes);
            dayNanos = 0;
            dayMaxNanos = 0;
            dayTicks = 0;
        }
        long nt = t + params.alertSec;
        next.update(nt);
        ctx.timerService().registerEventTimeTimer((nt + FleetDecisionFunction.SLIDE) * 1000L);
        asm.update(a);
    }

    private String json(String type, Object x) throws java.io.IOException {
        ObjectNode n = mapper.valueToTree(x);
        n.put("type", type);
        return mapper.writeValueAsString(n);
    }

    // ================================================================ 输入映射 / input mappers
    /** 标准化轮 / standardized rounds. */
    public static final class Rounds extends ProcessFunction<DeviceRound, M7Input> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(DeviceRound r, Context ctx, Collector<M7Input> out) {
            out.collect(M7Input.fromRound(r));
        }
    }

    /** 点通道评分：只留到达滑动步的离群记录 / point scores, arrival-slide outliers only. */
    public static final class Outliers extends ProcessFunction<ScoreEvent, M7Input> {
        private static final long serialVersionUID = 1L;
        private final long slideSec;

        public Outliers(long slideSec) {
            this.slideSec = slideSec;
        }

        @Override
        public void processElement(ScoreEvent e, Context ctx, Collector<M7Input> out) {
            M7Input in = M7Input.fromScore(e, slideSec);
            if (in != null) {
                out.collect(in);
            }
        }
    }

    /** 上下文评分 / contextual scores. */
    public static final class Contexts extends ProcessFunction<M3ScoreRecord, M7Input> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(M3ScoreRecord r, Context ctx, Collector<M7Input> out) {
            out.collect(M7Input.fromCtx(r));
        }
    }

    /** 漂移事件（性能层与数据层）；爆发以记录的事件时间戳为发出时刻 / drift events; a burst's emission is the record time. */
    public static final class Drifts extends ProcessFunction<DriftEvent, M7Input> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(DriftEvent e, Context ctx, Collector<M7Input> out) {
            long recordTs = ctx.timestamp() == null ? e.ts : Math.floorDiv(ctx.timestamp(), 1000L);
            M7Input in = M7Input.fromDrift(e, recordTs);
            if (in != null) {
                out.collect(in);
            }
        }
    }

    /** 数据层日记录 / data-layer daily records. */
    public static final class Records extends ProcessFunction<MonitoringSnapshot, M7Input> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(MonitoringSnapshot s, Context ctx, Collector<M7Input> out) {
            M7Input in = M7Input.fromRecord(s);
            if (in != null) {
                out.collect(in);
            }
        }
    }
}
