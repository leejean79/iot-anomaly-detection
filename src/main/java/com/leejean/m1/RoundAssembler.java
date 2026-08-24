package com.leejean.m1;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 采样轮装配（交接文档 §4.3），按 device keyBy。
 * Round assembly (handover §4.3), keyed by device.
 *
 * <p>规则 / rules:
 * <ul>
 *   <li>按**精确时间戳相等**分组（DF-1）。 Group readings by exact timestamp equality.</li>
 *   <li>同轮重复传感器**保留首值**（DEV-D7a，命中计数）。 Duplicate sensor keeps the first value.</li>
 *   <li>**事件时间定时器 ts+30s** 关闭该轮（approved decision 2）。 Event-time timer at ts+30s closes the round.</li>
 *   <li>不齐备的轮带缺失掩码照常输出，绝不丢弃（计数）。 Incomplete rounds are emitted with a missing mask, never dropped.</li>
 * </ul>
 *
 * <p>时间单位 / time units：Flink 事件时间为毫秒（Kafka 记录时间 = 读数秒 × 1000）。轮以毫秒时间戳
 * 作 MapState 键，定时器注册于 ts_ms + 30000。DeviceRound.ts 存回 epoch 秒。
 * Event time is in ms (Kafka record ts = reading-second × 1000). Rounds are keyed in MapState by the
 * millisecond timestamp; the timer fires at ts_ms + 30000; DeviceRound.ts is stored back as epoch seconds.
 *
 * <p>畸形读数（DF）：有 device 但时间/数值不可解析者无法归入某一轮，累加到逐设备
 * pendingMalformed，在下一轮关闭时并入该轮的 malformed 计数，从而进入监测快照。
 * Malformed readings cannot join a specific round, so they accumulate per device and are folded
 * into the next closing round's malformed count.
 */
public class RoundAssembler extends KeyedProcessFunction<String, Reading, DeviceRound> {
    private static final long serialVersionUID = 1L;

    private static final long ROUND_CLOSE_DELAY_MS = 30_000L;   // approved decision 2

    private transient MapState<Long, PartialRound> rounds;      // key = 轮的毫秒时间戳 / round ts in ms
    private transient ValueState<Long> pendingMalformed;

    private transient Counter roundsTotal;
    private transient Counter incompleteRounds;
    private transient Counter dupKeysMetric;

    @Override
    public void open(Configuration parameters) {
        rounds = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("m1-rounds", Types.LONG, Types.POJO(PartialRound.class)));
        pendingMalformed = getRuntimeContext().getState(
                new ValueStateDescriptor<>("m1-pending-malformed", Types.LONG));
        roundsTotal = getRuntimeContext().getMetricGroup().counter("m1_assembler_rounds_total");
        incompleteRounds = getRuntimeContext().getMetricGroup().counter("m1_assembler_incomplete_rounds");
        dupKeysMetric = getRuntimeContext().getMetricGroup().counter("m1_assembler_dup_keys");
    }

    @Override
    public void processElement(Reading r, Context ctx, Collector<DeviceRound> out) throws Exception {
        if (r.getKind() == Channels.KIND_MALFORMED) {
            long p = pendingMalformed.value() == null ? 0L : pendingMalformed.value();
            pendingMalformed.update(p + 1);
            return;
        }

        // 轮的毫秒时间戳：优先用 Kafka 事件时间（同轮各读数一致）；测试无时间戳时回退到 ts×1000。
        // Round ts in ms: prefer the Kafka event time (identical across a round); fall back to ts×1000.
        Long eventTs = ctx.timestamp();
        long roundTsMs = eventTs != null ? eventTs : r.getTs() * 1000L;

        PartialRound pr = rounds.get(roundTsMs);
        boolean isNew = pr == null;
        if (isNew) {
            pr = new PartialRound();
            // 只在首次为该轮注册定时器 / register the close timer only once per round
            ctx.timerService().registerEventTimeTimer(roundTsMs + ROUND_CLOSE_DELAY_MS);
        }

        switch (r.getKind()) {
            case Channels.KIND_UNKNOWN:
                pr.unknownSensor++;
                break;
            case Channels.KIND_DETECTION: {
                int ch = r.getChannelIndex();
                if (ch >= 0 && ch < Channels.N_DET) {
                    if (pr.present[ch]) {
                        pr.dupKeys++;            // DEV-D7a：保留首值 / keep first
                    } else {
                        pr.x[ch] = r.getValue();
                        pr.present[ch] = true;
                        if (r.isCensored()) {
                            pr.censored[ch] = true;
                            pr.censoredLight++;   // 仅 Light 会置删失 / only Light is censored
                        }
                    }
                }
                break;
            }
            case Channels.KIND_QUALITY:
                if (Channels.MIC.equals(r.getSensor())) {
                    if (pr.micPresent) {
                        pr.dupKeys++;
                    } else {
                        pr.mic = r.getValue();
                        pr.micPresent = true;
                    }
                } else if (Channels.RSSI.equals(r.getSensor())) {
                    if (pr.rssiPresent) {
                        pr.dupKeys++;
                    } else {
                        pr.rssi = r.getValue();
                        pr.rssiPresent = true;
                        if (r.isRssiSentinel()) {
                            pr.rssiSentinel++;
                        }
                    }
                } else if (Channels.ACCELEROMETER.equals(r.getSensor())) {
                    if (pr.accelPresent) {
                        pr.dupKeys++;
                    } else {
                        pr.accel = r.getValue();
                        pr.accelPresent = true;
                    }
                }
                break;
            default:
                break;
        }
        rounds.put(roundTsMs, pr);
    }

    @Override
    public void onTimer(long timerTs, OnTimerContext ctx, Collector<DeviceRound> out) throws Exception {
        long roundTsMs = timerTs - ROUND_CLOSE_DELAY_MS;
        PartialRound pr = rounds.get(roundTsMs);
        if (pr == null) {
            return;
        }
        rounds.remove(roundTsMs);

        DeviceRound dr = new DeviceRound();
        dr.setDevice(ctx.getCurrentKey());
        dr.setTs(roundTsMs / 1000L);

        boolean incomplete = false;
        double[] x = new double[Channels.N_DET];
        boolean[] missing = new boolean[Channels.N_DET];
        boolean[] censored = new boolean[Channels.N_DET];
        for (int i = 0; i < Channels.N_DET; i++) {
            x[i] = pr.x[i];
            missing[i] = !pr.present[i];
            censored[i] = pr.censored[i];
            if (!pr.present[i]) {
                incomplete = true;
            }
        }
        dr.setX(x);
        dr.setMissingMask(missing);
        dr.setCensoredMask(censored);
        dr.setMic(pr.mic);
        dr.setRssi(pr.rssi);
        dr.setAccel(pr.accel);
        dr.setIncomplete(incomplete);
        dr.setDupKeys(pr.dupKeys);
        dr.setUnknownSensor(pr.unknownSensor);
        dr.setCensoredLight(pr.censoredLight);
        dr.setRssiSentinel(pr.rssiSentinel);

        // 并入自上次关闭以来累计的畸形计数 / fold in malformed accumulated since last close
        long pend = pendingMalformed.value() == null ? 0L : pendingMalformed.value();
        if (pend > 0) {
            dr.setMalformed((int) Math.min(Integer.MAX_VALUE, pend));
            pendingMalformed.update(0L);
        }

        roundsTotal.inc();
        if (incomplete) {
            incompleteRounds.inc();
        }
        if (pr.dupKeys > 0) {
            dupKeysMetric.inc(pr.dupKeys);
        }
        out.collect(dr);
    }

    /**
     * 部分装配中的轮，存于 MapState（Flink POJO：public 字段 + public 无参构造）。
     * A partially-assembled round held in MapState (Flink POJO: public fields + public no-arg ctor).
     */
    public static class PartialRound {
        public double[] x = new double[Channels.N_DET];
        public boolean[] present = new boolean[Channels.N_DET];
        public boolean[] censored = new boolean[Channels.N_DET];
        public double mic;
        public boolean micPresent;
        public double rssi;
        public boolean rssiPresent;
        public double accel;
        public boolean accelPresent;
        public int dupKeys;
        public int unknownSensor;
        public int censoredLight;
        public int rssiSentinel;

        public PartialRound() { }
    }
}
