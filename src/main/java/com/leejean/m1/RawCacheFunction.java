package com.leejean.m1;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 原始缓存（交接文档 §4.5，approved decision 5）：每设备一个环形缓冲，存**原始未归一化**轮。
 * Raw cache (handover §4.5): a per-device ring buffer of RAW, un-normalized rounds.
 *
 * <p>深度 = cacheDepth（默认 1000 轮）。设备缺席超过缓存深度后返场，则在其流上标记
 * coldStart=true（冷启动的具体行为由后续消费方按 DEV-D7d 实现）。
 * Depth = cacheDepth (default 1000 rounds). If a device returns after an absence longer than the
 * cache depth, its stream is marked coldStart=true (consumers implement the behavior later, DEV-D7d).
 *
 * <p>「缺席超过缓存深度」的量化 / quantifying "absence longer than the cache depth"：缓存深度以
 * **轮**计，本算子用 事件时间间隔 > cacheDepth × nominalPeriodSec 作等价判据（nominalPeriodSec
 * 默认 10s，即 DF-2 标称采样周期）。**此为实现取舍，若与设计意图不符请交回设计会话。**
 * The cache depth is in rounds; this operator uses (event-time gap > cacheDepth × nominalPeriodSec)
 * as the equivalent test (nominalPeriodSec defaults to the 10 s nominal period, DF-2). This is an
 * implementation choice — report back to the design session if it misreads the intent.
 *
 * <p>缓存内容为**原始 x**（不含 xNorm），忠实「un-normalized」（本算子位于 RobustScaler 之后，
 * 但 DeviceRound 同时携带原始 x，故仍能缓存原始值）。
 * The cache stores the RAW x only (no xNorm); although this operator sits after the scaler, the
 * DeviceRound still carries the raw x.
 */
public class RawCacheFunction extends KeyedProcessFunction<String, DeviceRound, DeviceRound> {
    private static final long serialVersionUID = 1L;

    private final int cacheDepth;
    private final int nominalPeriodSec;

    private transient ValueState<RawRing> ring;
    private transient ValueState<Long> lastTsSec;
    private transient Counter coldStarts;

    public RawCacheFunction(int cacheDepth, int nominalPeriodSec) {
        if (cacheDepth <= 0) {
            throw new IllegalArgumentException("cacheDepth must be > 0, got " + cacheDepth);
        }
        if (nominalPeriodSec <= 0) {
            throw new IllegalArgumentException("nominalPeriodSec must be > 0, got " + nominalPeriodSec);
        }
        this.cacheDepth = cacheDepth;
        this.nominalPeriodSec = nominalPeriodSec;
    }

    @Override
    public void open(Configuration parameters) {
        ring = getRuntimeContext().getState(
                new ValueStateDescriptor<>("m1-raw-ring", Types.POJO(RawRing.class)));
        lastTsSec = getRuntimeContext().getState(new ValueStateDescriptor<>("m1-raw-last-ts", Types.LONG));
        coldStarts = getRuntimeContext().getMetricGroup().counter("m1_cache_cold_starts");
    }

    @Override
    public void processElement(DeviceRound round, Context ctx, Collector<DeviceRound> out) throws Exception {
        Long last = lastTsSec.value();
        long absenceThreshold = (long) cacheDepth * nominalPeriodSec;
        if (last != null && (round.getTs() - last) > absenceThreshold) {
            round.setColdStart(true);
            coldStarts.inc();
        }

        RawRing r = ring.value();
        if (r == null) {
            r = new RawRing();
            r.init(cacheDepth);
        }
        r.add(round.getTs(), round.getX());   // 缓存原始 x / cache the raw x
        ring.update(r);
        lastTsSec.update(round.getTs());

        out.collect(round);
    }

    /**
     * 定长环形缓冲（Flink POJO：public 字段 + 1D 原始数组，避免 Kryo 回退）。
     * Fixed-capacity ring buffer (Flink POJO: public fields + flat 1D primitive arrays, avoiding
     * a Kryo fallback). Stores ts and the raw F_det vector for the last `capacity` rounds.
     */
    public static class RawRing {
        public int capacity;
        public int head;
        public int size;
        public long[] ts;
        public double[] xflat;   // capacity × Channels.N_DET，行主序 / row-major

        public RawRing() { }

        public void init(int capacity) {
            this.capacity = capacity;
            this.head = 0;
            this.size = 0;
            this.ts = new long[capacity];
            this.xflat = new double[capacity * Channels.N_DET];
        }

        /** O(1) 追加，满则覆盖最旧 / O(1) append, overwrite-oldest when full. */
        public void add(long t, double[] x) {
            int base = head * Channels.N_DET;
            ts[head] = t;
            for (int i = 0; i < Channels.N_DET; i++) {
                xflat[base + i] = x[i];
            }
            head = (head + 1) % capacity;
            if (size < capacity) {
                size++;
            }
        }

        public int size() {
            return size;
        }
    }
}
