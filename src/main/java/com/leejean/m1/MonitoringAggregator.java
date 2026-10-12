package com.leejean.m1;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 监测聚合（交接文档 §4.6b）：按 device、每 60s 事件时间产出一份 MonitoringSnapshot。
 * Monitoring aggregation (handover §4.6b): one MonitoringSnapshot per device per 60 s of event time.
 *
 * <p>用 MapState&lt;windowEndMs, long[]&gt; 分窗累积，事件时间定时器在窗口末触发发出快照，天然容忍
 * 边界内乱序、不混窗。Per-window accumulation via MapState keyed by the window-end (ms); an
 * event-time timer at the window end emits the snapshot, tolerating in-window reordering without mixing.
 */
public class MonitoringAggregator extends KeyedProcessFunction<String, DeviceRound, MonitoringSnapshot> {
    private static final long serialVersionUID = 1L;

    private static final long WINDOW_MS = 60_000L;   // 60s 事件时间窗 / 60 s event-time window

    // 累积向量下标 / accumulator indices
    private static final int ROUNDS = 0;
    private static final int INCOMPLETE = 1;
    private static final int MALFORMED = 2;
    private static final int UNKNOWN = 3;
    private static final int DUP = 4;
    private static final int CENS_LIGHT = 5;
    private static final int RSSI_SENT = 6;
    private static final int WARMUP = 7;
    private static final int BYPASSED = 8;
    private static final int COLD = 9;
    private static final int SUBSTITUTED = 10;    // 替代分母通道计数（补充指令三）
    private static final int DEGRADED_MASK = 11;  // 旁路|替代 通道位图（按位或）
    private static final int STUCK = 12;          // 重复值游程达阈值的轮数（开发决策第十条）
    private static final int STUCK_MASK = 13;     // 游程达阈值的通道位图（按位或）
    private static final int FLOOR = 14;          // 底值游程达阈值的轮数（2026-10-12 裁决第二节）
    private static final int FLOOR_MASK = 15;     // 底值游程达阈值的通道位图（按位或）
    private static final int LAST_TS = 16;        // 本窗口已见的最晚轮时间戳，用来取最后一轮的游程信息
    private static final int RUN_LEN = 17;        // 起 5 格：最后一轮的游程长度 / 5 slots: run length at the last round
    private static final int RUN_START = 22;      // 起 5 格：最后一轮的游程起始时刻 / 5 slots: run start
    private static final int WIDTH = 27;

    private transient MapState<Long, long[]> windows;   // windowEndMs → 累积 / accumulators
    private transient Counter snapshotsEmitted;

    @Override
    public void open(Configuration parameters) {
        windows = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "m1-monitor-windows", Types.LONG, PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO));
        snapshotsEmitted = getRuntimeContext().getMetricGroup().counter("m1_monitor_snapshots");
    }

    @Override
    public void processElement(DeviceRound round, Context ctx, Collector<MonitoringSnapshot> out) throws Exception {
        Long eventTs = ctx.timestamp();
        long tsMs = eventTs != null ? eventTs : round.getTs() * 1000L;
        long windowEnd = (tsMs / WINDOW_MS + 1) * WINDOW_MS;

        long[] acc = windows.get(windowEnd);
        if (acc == null) {
            acc = new long[WIDTH];
            ctx.timerService().registerEventTimeTimer(windowEnd);
        }
        acc[ROUNDS] += 1;
        acc[INCOMPLETE] += round.isIncomplete() ? 1 : 0;
        acc[MALFORMED] += round.getMalformed();
        acc[UNKNOWN] += round.getUnknownSensor();
        acc[DUP] += round.getDupKeys();
        acc[CENS_LIGHT] += round.getCensoredLight();
        acc[RSSI_SENT] += round.getRssiSentinel();
        acc[WARMUP] += round.isWarmup() ? 1 : 0;
        acc[BYPASSED] += round.bypassCount();
        acc[SUBSTITUTED] += round.substitutedCount();
        acc[DEGRADED_MASK] |= round.degradedMaskBits();   // 位图按或累积 / OR-accumulate the mask
        acc[COLD] += round.isColdStart() ? 1 : 0;
        int stuckBits = round.stuckMaskBits();
        acc[STUCK] += stuckBits != 0 ? 1 : 0;
        acc[STUCK_MASK] |= stuckBits;
        int floorBits = round.floorMaskBits();
        acc[FLOOR] += floorBits != 0 ? 1 : 0;
        acc[FLOOR_MASK] |= floorBits;
        if (round.getTs() >= acc[LAST_TS]) {
            acc[LAST_TS] = round.getTs();
            int[] len = round.getGuardRunLength();
            long[] start = round.getGuardRunStartTs();
            for (int c = 0; c < Channels.N_DET; c++) {
                acc[RUN_LEN + c] = len != null ? len[c] : 0;
                acc[RUN_START + c] = start != null ? start[c] : 0;
            }
        }
        windows.put(windowEnd, acc);
    }

    @Override
    public void onTimer(long timerTs, OnTimerContext ctx, Collector<MonitoringSnapshot> out) throws Exception {
        long[] acc = windows.get(timerTs);
        if (acc == null) {
            return;
        }
        windows.remove(timerTs);

        MonitoringSnapshot s = new MonitoringSnapshot();
        s.setTs(timerTs / 1000L);
        s.setDevice(DeviceKeys.deviceOf(ctx.getCurrentKey()));   // 键可能是代理键「设备号#序号」/ key may be a surrogate
        s.setRoundsTotal(acc[ROUNDS]);
        s.setIncompleteRounds(acc[INCOMPLETE]);
        s.setMalformed(acc[MALFORMED]);
        s.setUnknownSensor(acc[UNKNOWN]);
        s.setDupKeys(acc[DUP]);
        s.setCensoredLight(acc[CENS_LIGHT]);
        s.setRssiSentinel(acc[RSSI_SENT]);
        s.setWarmup(acc[WARMUP]);
        s.setBypassedChannels(acc[BYPASSED]);
        s.setSubstitutedChannels(acc[SUBSTITUTED]);
        s.setDegradedChannelsMask(acc[DEGRADED_MASK]);
        s.setColdStart(acc[COLD]);
        s.setStuckRounds(acc[STUCK]);
        s.setStuckChannelsMask(acc[STUCK_MASK]);
        s.setFloorRounds(acc[FLOOR]);
        s.setFloorChannelsMask(acc[FLOOR_MASK]);
        s.setGuardRunLength(java.util.Arrays.copyOfRange(acc, RUN_LEN, RUN_LEN + Channels.N_DET));
        s.setGuardRunStartTs(java.util.Arrays.copyOfRange(acc, RUN_START, RUN_START + Channels.N_DET));
        snapshotsEmitted.inc();
        out.collect(s);
    }
}
