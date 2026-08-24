package com.leejean.m1;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 每设备每通道的 RobustScaler：预热收集 → 冻结 中位数/IQR（交接文档 §4.4，approved decision 3）。
 * Per-device, per-channel RobustScaler: warm-up collect → freeze median/IQR (handover §4.4).
 *
 * <p>预热-冻结模式仿 flink/PerFeatureHDDMFunction：前 warmupRounds 轮只累积样本（预热期
 * 输出 warmup=true 且 xNorm 为原始透传，因统计尚未冻结）；到达边界即冻结每通道 中位数/IQR、
 * 清空蓄水池，此后不再自适应。
 * The warm-up-then-freeze pattern mirrors PerFeatureHDDMFunction: for the first warmupRounds rounds
 * only accumulate (emit warmup=true, xNorm = raw pass-through since stats are not frozen yet); at the
 * boundary, freeze per-channel median/IQR and clear the reservoirs; statistics never adapt afterwards.
 *
 * <p>右删失的 Light 值不计入校准统计（§4.4）。IQR ≤ ε 的通道旁路缩放（原值透传、置旗、计数）。
 * Right-censored Light values are excluded from calibration; a channel with IQR ≤ ε bypasses scaling
 * (pass-through, flagged, counted).
 *
 * <p>recalibrate() 为将来的重估入口（无触发逻辑——那是 M6 的职责；其协议为理论 B §12.4 的状态重建）。
 * recalibrate() is the future recalibration entry point (no trigger logic — that is M6's job later;
 * its protocol is the state rebuild of theory B §12.4).
 */
public class RobustScalerFunction extends KeyedProcessFunction<String, DeviceRound, DeviceRound> {
    private static final long serialVersionUID = 1L;

    private final int warmupRounds;   // approved decision 3: 8640
    private final double epsilon;     // IQR ≤ ε 判据 / bypass threshold

    private transient ValueState<Long> count;
    private transient ValueState<Boolean> frozen;
    private transient ValueState<double[]> median;
    private transient ValueState<double[]> iqr;
    private transient ValueState<boolean[]> bypass;
    private transient List<ListState<Double>> reservoirs;   // 每通道一个蓄水池 / one reservoir per channel

    private transient Counter warmupRoundsMetric;
    private transient Counter frozenDevices;
    private transient Counter bypassedChannelsMetric;

    public RobustScalerFunction(int warmupRounds, double epsilon) {
        if (warmupRounds <= 0) {
            throw new IllegalArgumentException("warmupRounds must be > 0, got " + warmupRounds);
        }
        if (epsilon < 0) {
            throw new IllegalArgumentException("epsilon must be >= 0, got " + epsilon);
        }
        this.warmupRounds = warmupRounds;
        this.epsilon = epsilon;
    }

    @Override
    public void open(Configuration parameters) {
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("rs-count", Types.LONG));
        frozen = getRuntimeContext().getState(new ValueStateDescriptor<>("rs-frozen", Types.BOOLEAN));
        median = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "rs-median", PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));
        iqr = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "rs-iqr", PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));
        bypass = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "rs-bypass", PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO));
        reservoirs = new ArrayList<>(Channels.N_DET);
        for (int c = 0; c < Channels.N_DET; c++) {
            reservoirs.add(getRuntimeContext().getListState(
                    new ListStateDescriptor<>("rs-reservoir-" + c, Types.DOUBLE)));
        }
        warmupRoundsMetric = getRuntimeContext().getMetricGroup().counter("m1_scaler_warmup_rounds");
        frozenDevices = getRuntimeContext().getMetricGroup().counter("m1_scaler_frozen_devices");
        bypassedChannelsMetric = getRuntimeContext().getMetricGroup().counter("m1_scaler_bypassed_channels");
    }

    @Override
    public void processElement(DeviceRound round, Context ctx, Collector<DeviceRound> out) throws Exception {
        boolean isFrozen = frozen.value() != null && frozen.value();

        if (!isFrozen) {
            // 预热：累积样本（排除缺失与右删失 Light）/ warm-up: accumulate (exclude missing and censored Light)
            for (int c = 0; c < Channels.N_DET; c++) {
                if (round.getMissingMask()[c]) {
                    continue;
                }
                if (c == Channels.LIGHT_INDEX && round.getCensoredMask()[c]) {
                    continue;   // 右删失 Light 不计入校准 / censored Light excluded from calibration
                }
                reservoirs.get(c).add(round.getX()[c]);
            }
            long newCount = (count.value() == null ? 0L : count.value()) + 1;
            count.update(newCount);

            // 预热期输出：原始透传 + warmup 标志 / warm-up output: raw pass-through + warmup flag
            round.setWarmup(true);
            round.setXNorm(round.getX().clone());
            warmupRoundsMetric.inc();
            out.collect(round);

            if (newCount == warmupRounds) {
                freeze();
            }
            return;
        }

        // 已冻结：应用 RobustScaler / frozen: apply RobustScaler
        double[] med = median.value();
        double[] scale = iqr.value();
        boolean[] bp = bypass.value();
        double[] xn = new double[Channels.N_DET];
        boolean[] outBypass = new boolean[Channels.N_DET];
        for (int c = 0; c < Channels.N_DET; c++) {
            if (round.getMissingMask()[c]) {
                xn[c] = 0.0;              // 缺失通道置 0，依赖 missingMask / missing → 0, rely on mask
                continue;
            }
            if (bp[c]) {
                xn[c] = round.getX()[c]; // 旁路：原值透传 / bypass: pass through
                outBypass[c] = true;
            } else {
                xn[c] = (round.getX()[c] - med[c]) / scale[c];
            }
        }
        round.setWarmup(false);
        round.setXNorm(xn);
        round.setBypassMask(outBypass);
        out.collect(round);
    }

    /** 到达预热边界，冻结每通道 中位数/IQR，并清空蓄水池。 Freeze at the warm-up boundary. */
    private void freeze() throws Exception {
        double[] med = new double[Channels.N_DET];
        double[] scale = new double[Channels.N_DET];
        boolean[] bp = new boolean[Channels.N_DET];
        int nBypass = 0;
        for (int c = 0; c < Channels.N_DET; c++) {
            List<Double> vals = new ArrayList<>();
            for (Double v : reservoirs.get(c).get()) {
                vals.add(v);
            }
            if (vals.isEmpty()) {
                // 无样本（通道全程缺失/删失）→ 旁路 / no samples → bypass
                med[c] = 0.0;
                scale[c] = 1.0;
                bp[c] = true;
                nBypass++;
            } else {
                Collections.sort(vals);
                med[c] = percentile(vals, 0.50);
                double q1 = percentile(vals, 0.25);
                double q3 = percentile(vals, 0.75);
                double range = q3 - q1;
                if (range <= epsilon) {
                    scale[c] = 1.0;
                    bp[c] = true;    // IQR ≤ ε → 旁路（防退化除零）/ bypass to avoid divide-by-zero
                    nBypass++;
                } else {
                    scale[c] = range;
                    bp[c] = false;
                }
            }
            reservoirs.get(c).clear();   // 释放蓄水池 / release the reservoir
        }
        median.update(med);
        iqr.update(scale);
        bypass.update(bp);
        frozen.update(true);
        frozenDevices.inc();
        if (nBypass > 0) {
            bypassedChannelsMetric.inc(nBypass);
        }
    }

    /**
     * 排序后的线性插值分位数 / linear-interpolation percentile over a sorted list.
     */
    private static double percentile(List<Double> sorted, double q) {
        int n = sorted.size();
        if (n == 1) {
            return sorted.get(0);
        }
        double pos = q * (n - 1);
        int lo = (int) Math.floor(pos);
        int hi = (int) Math.ceil(pos);
        double frac = pos - lo;
        return sorted.get(lo) * (1 - frac) + sorted.get(hi) * frac;
    }

    /**
     * 重估入口（M6 将来触发；M1 不接线）：清空冻结统计与计数，重启预热。
     * Recalibration entry point (triggered by M6 later; not wired in M1): clears frozen stats and
     * count, restarting warm-up. Must be invoked within this operator's keyed context.
     */
    public void recalibrate() throws Exception {
        frozen.clear();
        median.clear();
        iqr.clear();
        bypass.clear();
        count.update(0L);
        for (ListState<Double> res : reservoirs) {
            res.clear();
        }
    }
}
