package com.leejean.m1;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 原始缓存算子（交接文档 §4.5，approved decision 5）：冷启动标记与重复值游程守卫。
 * Raw-cache operator (handover §4.5): cold-start marking and the repeated-value run-length guard.
 *
 * <p>原来的一千轮环形缓冲只写不读，适应执行第一批（`ruling_m6a_kickoff_zh.md` 第一项）将其删除；真正被读取的紧凑缓存放在
 * 标准化算子里。本算子在作业中移到标准化算子之前，守卫与冷启动标记先写到轮上，供标准化算子的零级重估剔除守卫轮。两个算子
 * 互不读取对方写的字段，调换顺序不改变输出。
 * The write-only 1000-round ring was removed (M6a ruling item 1); the compact cache that is actually read lives in the
 * scaler. This operator now runs before the scaler so the guard and cold-start flags are on the round when it is cached.
 *
 * <p>设备缺席超过 cacheDepth 轮（默认 1000）后返场，则在其流上标记 coldStart=true（冷启动的具体行为由后续消费方按 DEV-D7d 实现）。
 * If a device returns after an absence longer than cacheDepth rounds (default 1000), its stream is marked coldStart=true.
 *
 * <p>「缺席超过缓存深度」的量化 / quantifying "absence longer than the cache depth"：缓存深度以
 * **轮**计，本算子用 事件时间间隔 > cacheDepth × nominalPeriodSec 作等价判据（nominalPeriodSec
 * 默认 10s，即 DF-2 标称采样周期）。**此为实现取舍，若与设计意图不符请交回设计会话。**
 * The cache depth is in rounds; this operator uses (event-time gap > cacheDepth × nominalPeriodSec)
 * as the equivalent test (nominalPeriodSec defaults to the 10 s nominal period, DF-2). This is an
 * implementation choice — report back to the design session if it misreads the intent.
 *
 * <p>本算子同时承载重复值游程守卫（开发决策第十条，见 {@link RepeatRunGuard}）：给出阈值表时，逐设备逐通道统计
 * 连续完全相同读数的游程，达到阈值即在轮上写 stuckMask，并累加 m1_guard_stuck_runs / m1_guard_stuck_rounds。
 * This operator also hosts the repeated-value run-length guard (development decision 10, see RepeatRunGuard).
 */
public class RawCacheFunction extends KeyedProcessFunction<String, DeviceRound, DeviceRound> {
    private static final long serialVersionUID = 1L;

    private final int cacheDepth;
    private final int nominalPeriodSec;
    // 重复值游程守卫的阈值表（设备 → 五个通道的阈值，单位：轮）；空表时守卫关闭（开发决策第十条）。
    // Run-length guard thresholds (device → five per-channel thresholds in rounds); empty disables the guard.
    private final HashMap<String, int[]> stuckThresholds;
    // 底值集合（按检测通道顺序；2026-10-12 裁决第二节）/ floor-value sets per detection channel
    private final HashMap<String, double[][]> stuckFloors;

    private transient ValueState<Long> lastTsSec;
    private transient ValueState<RepeatRunGuard.RunState> runState;
    private transient Counter coldStarts;
    private transient Counter stuckRuns;
    private transient Counter stuckRounds;
    private transient Counter floorRuns;

    public RawCacheFunction(int cacheDepth, int nominalPeriodSec) {
        this(cacheDepth, nominalPeriodSec, Collections.emptyMap());
    }

    public RawCacheFunction(int cacheDepth, int nominalPeriodSec, Map<String, int[]> stuckThresholds) {
        this(cacheDepth, nominalPeriodSec, stuckThresholds, Collections.emptyMap());
    }

    public RawCacheFunction(int cacheDepth, int nominalPeriodSec, Map<String, int[]> stuckThresholds,
                            Map<String, double[][]> stuckFloors) {
        if (cacheDepth <= 0) {
            throw new IllegalArgumentException("cacheDepth must be > 0, got " + cacheDepth);
        }
        if (nominalPeriodSec <= 0) {
            throw new IllegalArgumentException("nominalPeriodSec must be > 0, got " + nominalPeriodSec);
        }
        this.cacheDepth = cacheDepth;
        this.nominalPeriodSec = nominalPeriodSec;
        this.stuckThresholds = new HashMap<>(stuckThresholds);
        this.stuckFloors = new HashMap<>(stuckFloors);
    }

    @Override
    public void open(Configuration parameters) {
        lastTsSec = getRuntimeContext().getState(new ValueStateDescriptor<>("m1-raw-last-ts", Types.LONG));
        coldStarts = getRuntimeContext().getMetricGroup().counter("m1_cache_cold_starts");
        runState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("m1-repeat-run", Types.POJO(RepeatRunGuard.RunState.class)));
        stuckRuns = getRuntimeContext().getMetricGroup().counter("m1_guard_stuck_runs");
        stuckRounds = getRuntimeContext().getMetricGroup().counter("m1_guard_stuck_rounds");
        floorRuns = getRuntimeContext().getMetricGroup().counter("m1_guard_floor_runs");
    }

    @Override
    public void processElement(DeviceRound round, Context ctx, Collector<DeviceRound> out) throws Exception {
        Long last = lastTsSec.value();
        long absenceThreshold = (long) cacheDepth * nominalPeriodSec;
        if (last != null && (round.getTs() - last) > absenceThreshold) {
            round.setColdStart(true);
            coldStarts.inc();
        }

        lastTsSec.update(round.getTs());

        // 重复值游程守卫：只打标与计数，不改变下游行为 / run-length guard: flag and count only
        int[] thr = stuckThresholds.get(round.getDevice());
        if (thr != null) {
            RepeatRunGuard.RunState st = runState.value();
            if (st == null) {
                st = new RepeatRunGuard.RunState();
            }
            int[] crossed = RepeatRunGuard.update(st, round, thr,
                    stuckFloors.getOrDefault(round.getDevice(), RepeatRunGuard.emptyFloors()));
            stuckRuns.inc(crossed[0]);
            floorRuns.inc(crossed[1]);
            if (round.stuckMaskBits() != 0) {
                stuckRounds.inc();
            }
            runState.update(st);
        }

        out.collect(round);
    }
}
