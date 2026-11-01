package com.leejean.m7;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * 聚合模块的输入整理器（实现方案第二节）：按设备累计每个告警节拍的观测（轮数、入口轮数、守卫轮数、到达滑动步的离群轮去重数、
 * 上下文超阈数与主分最大值、冻结之后的变换域读数数与和），把事件转交 {@link FleetDecisionFunction}，并在节拍时刻驱动它。
 * 在线算子 {@link M7DecisionOperator} 与离线转储运行器 {@link M7DumpRunner} 共用本类，所以两者的差别只可能来自输入在节拍时刻
 * 是否已经到达。观测的口径与离线输入表 {@code hours.csv} 相同（eda/m7_extract.py 与 m7/prepare_inputs.py）。
 * The M7 input assembler shared by the online operator and the dump runner.
 *
 * <p>检查点里整份状态用 Java 序列化（决策函数只实现了 Serializable）/ Java serialization in checkpoints.
 */
@DefaultSerializer(JavaSerializer.class)
public class M7InputAssembler implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 一次节拍的全部输出 / outputs of one tick. */
    public static final class Out {
        public final List<FleetDecisionFunction.Alert> alerts = new ArrayList<>();
        public final List<FleetDecisionFunction.Command> commands = new ArrayList<>();
        public final List<FleetDecisionFunction.Review> review = new ArrayList<>();
    }

    private final FleetDecisionFunction.Params p;
    private final FleetDecisionFunction f;
    private final TreeMap<Long, Map<String, FleetDecisionFunction.HourObs>> obs = new TreeMap<>();   // 窗口起点 → 设备 → 观测
    private final Set<String> outlierSeen = new HashSet<>();                                          // 设备|轮时刻
    private final Map<String, String[]> dataDays = new HashMap<>();                                   // 设备|日|通道 → 状态
    private final Set<String> frozen = new HashSet<>();

    public M7InputAssembler(FleetDecisionFunction.Params p) {
        this.p = p;
        this.f = new FleetDecisionFunction(p);
    }

    /** 事件时间 ts 所在告警节拍的起点 / start of the alert window containing ts. */
    public long windowOf(long ts) {
        return Math.floorDiv(ts, p.alertSec) * p.alertSec;
    }

    private FleetDecisionFunction.HourObs obsOf(String dev, long ws) {
        return obs.computeIfAbsent(ws, k -> new TreeMap<>()).computeIfAbsent(dev, k -> new FleetDecisionFunction.HourObs());
    }

    /** 送入一条输入 / accept one input. */
    public void accept(M7Input in) {
        switch (in.kind) {
            case M7Input.ROUND: {
                f.addDevice(in.device);
                if (in.refs != null && frozen.add(in.device)) {
                    f.setFreeze(in.device, in.ts);
                    for (int k = 0; k < FleetDecisionFunction.ACTIVE.length; k++) {
                        f.setRef(in.device, FleetDecisionFunction.ACTIVE[k], in.refs[2 * k], in.refs[2 * k + 1]);
                    }
                }
                FleetDecisionFunction.HourObs o = obsOf(in.device, windowOf(in.ts));
                o.rounds++;
                o.admitted += in.flag2 ? 1 : 0;
                o.guardRounds += in.flag1 ? 1 : 0;
                for (int k = 0; k < in.tv.length; k++) {
                    if (!Double.isNaN(in.tv[k])) {
                        o.n[k]++;
                        o.sum[k] += in.tv[k];
                    }
                }
                break;
            }
            case M7Input.OUTLIER:
                if (outlierSeen.add(in.device + "|" + in.ts)) {
                    obsOf(in.device, windowOf(in.ts)).outlierRounds++;
                }
                break;
            case M7Input.CTX: {
                // 窗口末落在 (起点, 终点] 的窗口计入该节拍 / window ends in (start, end]
                FleetDecisionFunction.HourObs o = obsOf(in.device, windowOf(in.ts - 1));
                o.ctxAbove += in.flag1 ? 1 : 0;
                if (o.ctxMax.isEmpty() || in.v1 > Double.parseDouble(o.ctxMax)) {
                    o.ctxMax = Double.toString(in.v1);
                }
                f.addPerfElement(in.device, in.ts, in.v1);
                break;
            }
            case M7Input.PERF:
                f.addPerfEvent(in.device, in.ts, in.s1, in.v1, in.v2);
                break;
            case M7Input.DAY_RECORD: {
                String[] s = dataDays.computeIfAbsent(in.device + "|" + in.s1 + "|" + in.s2, k -> new String[]{"0", "0", ""});
                s[0] = in.flag1 ? "1" : "0";
                s[1] = in.flag2 ? "1" : "0";
                f.addDataDay(in.device, in.s1, in.s2, in.flag1, in.flag2, s[2]);
                break;
            }
            case M7Input.TRANSITION: {
                String[] s = dataDays.computeIfAbsent(in.device + "|" + in.s1 + "|" + in.s2, k -> new String[]{"0", "0", ""});
                s[2] = in.s3;
                f.addDataDay(in.device, in.s1, in.s2, "1".equals(s[0]), "1".equals(s[1]), s[2]);
                break;
            }
            case M7Input.BURST:
                f.addBurst(in.device, in.ts, in.ts2);
                break;
            default:
                throw new IllegalArgumentException("未知的输入类别 / unknown kind " + in.kind);
        }
    }

    /** t 是否为决策节拍（从当地零点起每 τ_cmd 一次，日界总是）/ whether t is a decision tick. */
    public boolean isCmdTick(long t) {
        long dayStart = FleetDecisionFunction.dayStart(FleetDecisionFunction.localDay(t - 1));
        long next = FleetDecisionFunction.dayStart(FleetDecisionFunction.addDays(FleetDecisionFunction.localDay(t - 1), 1));
        return t == next || (p.cmdSec != 0 && (t - dayStart) % p.cmdSec == 0);
    }

    /**
     * 节拍 t（一个告警窗口的终点）：先做 [t − τ_alert, t) 的告警节拍，再在决策节拍上做决策（日界含复核）。没有读数的设备不进入观测，
     * 与离线 hours.csv 只有有读数的设备小时相同。
     * Tick at t: the alert tick of [t − τ_alert, t), then the decision tick if due.
     */
    public void tick(long t, Out out) {
        long ws = t - p.alertSec;
        Map<String, FleetDecisionFunction.HourObs> o = obs.remove(ws);
        Map<String, FleetDecisionFunction.HourObs> present = new HashMap<>();
        if (o != null) {
            for (Map.Entry<String, FleetDecisionFunction.HourObs> e : o.entrySet()) {
                if (e.getValue().rounds > 0) {
                    present.put(e.getKey(), e.getValue());
                }
            }
        }
        // 早于本窗口的残留（不应出现）一并丢弃 / drop stale windows (should not happen)
        obs.headMap(ws).clear();
        outlierSeen.removeIf(k -> Long.parseLong(k.substring(k.indexOf('|') + 1)) < ws);
        out.alerts.addAll(f.alertTick(ws, present));
        if (isCmdTick(t)) {
            f.cmdTick(t, out.commands, out.review);
        }
    }

    public long alertSec() {
        return p.alertSec;
    }

    public FleetDecisionFunction function() {
        return f;
    }
}
