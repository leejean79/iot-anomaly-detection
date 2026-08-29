package com.leejean.m2;

import com.leejean.m1.DeviceRound;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 进入 M2 前的三道闸（交接文档 §3），把通过的 M1 标准化轮转换成 {@link DevicePoint}。
 * The three input gates before M2 (handover §3); passing rounds become {@link DevicePoint}.
 *
 * <p>规则 / rules：
 * <ol>
 *   <li>warmup=true 的轮不进 M2（校准期检测静默）——计数并丢弃。</li>
 *   <li>缺失掩码非空的轮不进 M2（距离在缺维向量上无定义，决策五）——计数并丢弃。</li>
 *   <li>coldStart=true 的轮**照常进入**，并在 {@link DevicePoint} 上带 coldStart 标志：窗口函数据此
 *       先清空该设备 MCOD 状态再处理（本阶段只做"清空重来"，完整回填属 M6）——计数。</li>
 *   <li>censoredMask 非空（光照删失）的轮**照常进入**（65536 是确定数值，距离有定义），单列计数供评估。</li>
 * </ol>
 * 计数以 Flink 自定义指标暴露（与 M1 的守卫计数同口径，作为权威值）；迟到丢弃在窗口侧输出单独计数。
 * Counts are exposed as Flink custom metrics (authoritative, same convention as M1's guard counters);
 * late-data drops are counted separately on the window's side output.
 *
 * <p><b>已知简化 / known simplification</b>：若某轮**同时** coldStart 且缺失掩码非空，按第 2 条丢弃、
 * 该次显式清空信号丢失；但由于冷启动意味着长缺席，其陈旧点的 arrival 远早于恢复后的窗口，会在随后的
 * 滑动步内被自然淘汰（最多 W 事件时间内自愈），故不影响正确性，仅延迟清理。已在此标注。
 */
public class M2Gate extends ProcessFunction<DeviceRound, DevicePoint> {
    private static final long serialVersionUID = 1L;

    private transient Counter warmupBypass;      // warmup 旁路数
    private transient Counter missingBypass;     // 缺失掩码旁路数
    private transient Counter coldStartClear;    // 冷启动清空次数（实际执行）
    private transient Counter censoredEntered;   // 删失轮进入数
    private transient Counter admitted;          // 进入 M2 的轮数（供对账）

    @Override
    public void open(Configuration parameters) {
        warmupBypass = metric("m2_gate_warmup_bypass");
        missingBypass = metric("m2_gate_missing_bypass");
        coldStartClear = metric("m2_gate_coldstart_clear");
        censoredEntered = metric("m2_gate_censored_entered");
        admitted = metric("m2_gate_admitted");
    }

    private Counter metric(String name) {
        return getRuntimeContext().getMetricGroup().counter(name);
    }

    @Override
    public void processElement(DeviceRound round, Context ctx, Collector<DevicePoint> out) {
        // 闸 1：校准期静默 / gate 1: silent during warm-up
        if (round.isWarmup()) {
            warmupBypass.inc();
            return;
        }
        // 闸 3（决策五）：缺维向量上距离无定义 / gate: missing mask → distance undefined
        if (round.missingCount() > 0) {
            missingBypass.inc();
            return;
        }
        // 通过：删失单列计数（照常进入），冷启动带清空信号并计数
        if (censoredCount(round) > 0) {
            censoredEntered.inc();
        }
        boolean cold = round.isColdStart();
        if (cold) {
            coldStartClear.inc();
        }
        admitted.inc();

        double[] xNorm = round.getXNorm();
        double[] value = xNorm == null ? new double[0] : xNorm.clone();   // 防对象复用 / defensive copy
        long ts = round.getTs();
        // arrival = 轮时间戳毫秒；id = 轮时间戳秒（同设备内唯一）；flag 恒 0（§3）
        McodPoint p = new McodPoint(value, ts * 1000L, 0, ts, cold);
        out.collect(new DevicePoint(round.getDevice(), p));
    }

    private static int censoredCount(DeviceRound round) {
        int n = 0;
        boolean[] mask = round.getCensoredMask();
        if (mask != null) {
            for (boolean b : mask) {
                if (b) {
                    n++;
                }
            }
        }
        return n;
    }
}
