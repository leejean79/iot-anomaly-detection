package com.leejean.m3;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 上下文异常检测算子（补遗四第二节第 3 条）：在 {@link KeyedProcessOperator} 之上只改水位线的转发。
 * 训练期间与排空期间，排空时发出的评分带着比输入水位线更早的事件时间戳；若照常转发水位线，这些评分对下游就是
 * 迟到记录。因此对外发出的水位线取「输入水位线」与「本子任务所有缓冲中最早一轮的时间戳减一毫秒」两者的较小值，
 * 缓冲排空后恢复跟随输入。代理键使每个子任务只承载一台设备，这个最小值实际上就是该设备的缓冲头。
 * The M3 operator: KeyedProcessOperator with held watermark forwarding. While rounds are buffered, the emitted
 * watermark is min(input watermark, earliest buffered record timestamp - 1 ms), so drained scores are not late.
 *
 * <p>本算子没有事件时间定时器，向内部定时器服务推进的也是保持后的水位线，这不影响任何行为。
 * M3 registers no event-time timers, so advancing the timer service with the held watermark changes nothing.
 */
public class M3Operator extends KeyedProcessOperator<String, AnnotatedRound, M3ScoreRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(M3Operator.class);

    private final M3Function fn;
    private transient long inputWatermark;
    private transient long emittedWatermark;
    /** 输出水位线开始被保持的处理时间（毫秒），0 表示未保持 / processing time the hold began, 0 if none. */
    private transient volatile long holdSinceMs;

    public M3Operator(M3Function fn) {
        super(fn);
        this.fn = fn;
    }

    @Override
    public void open() throws Exception {
        super.open();
        inputWatermark = Long.MIN_VALUE;
        emittedWatermark = Long.MIN_VALUE;
        holdSinceMs = 0L;
        // 从检查点恢复：逐分键回填缓冲头时间戳与占用，否则恢复后第一条水位线会越过缓冲中的轮。
        // On restore, refill the per-key buffer heads, or the first watermark would overtake buffered rounds.
        this.<String>getKeyedStateBackend().applyToAllKeys(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                M3Function.PENDING_HEAD_TS_DESC, (String key, ValueState<Long> head) ->
                        fn.restoreBuffer(key, sizeOf(key), head.value()));
        getMetricGroup().gauge("m3_watermark_hold_sec", (Gauge<Double>) () ->
                holdSinceMs == 0L ? 0.0 : (System.currentTimeMillis() - holdSinceMs) / 1000.0);
    }

    private Integer sizeOf(String key) throws Exception {
        setCurrentKey(key);
        return getPartitionedState(M3Function.PENDING_SIZE_DESC).value();
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        inputWatermark = Math.max(inputWatermark, mark.getTimestamp());
        forwardWatermark();
    }

    @Override
    public void processElement(StreamRecord<AnnotatedRound> element) throws Exception {
        super.processElement(element);
        forwardWatermark();
    }

    @Override
    public void onProcessingTime(InternalTimer<String, VoidNamespace> timer) throws Exception {
        super.onProcessingTime(timer);
        forwardWatermark();
    }

    /** 发出 min(输入水位线, 最早缓冲轮 - 1)，只增不减 / emit min(input, earliest buffered - 1), monotonically. */
    private void forwardWatermark() throws Exception {
        long earliest = fn.earliestBufferedTs();
        long target = earliest == Long.MAX_VALUE ? inputWatermark : Math.min(inputWatermark, earliest - 1);
        if (target > emittedWatermark) {
            emittedWatermark = target;
            super.processWatermark(new Watermark(target));
        }
        boolean held = emittedWatermark < inputWatermark;
        if (held && holdSinceMs == 0L) {
            holdSinceMs = System.currentTimeMillis();
            LOG.info("[M3] Output watermark held at the earliest buffered round (subtask {})",
                     getRuntimeContext().getIndexOfThisSubtask());
        } else if (!held && holdSinceMs != 0L) {
            LOG.info("[M3] Output watermark hold released after {}s (subtask {})",
                     String.format("%.1f", (System.currentTimeMillis() - holdSinceMs) / 1000.0),
                     getRuntimeContext().getIndexOfThisSubtask());
            holdSinceMs = 0L;
        }
    }
}
