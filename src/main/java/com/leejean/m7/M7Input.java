package com.leejean.m7;

import com.leejean.m1.Channels;
import com.leejean.m1.DeviceRound;
import com.leejean.m2.ScoreEvent;
import com.leejean.m3.M3ScoreRecord;
import com.leejean.m5.DriftEvent;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m5a.RefHistogram;

import java.io.Serializable;

/**
 * 聚合模块在线算子的统一输入记录（实现方案 {@code docs/reports/m7_online_plan.md} 第一节）：作业里六路已有的流各自映射为本类，
 * 合并后按事件时间送入 {@link M7DecisionOperator}；离线转储运行器 {@link M7DumpRunner} 从转储做同样的映射。映射只取决策函数要用的
 * 字段，使算子状态与网络传输都小。Flink POJO：public 字段加无参构造。
 * The unified input record of the M7 operator; each upstream stream maps to it. Flink POJO.
 */
public class M7Input implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int ROUND = 0;          // 标准化轮 / standardized round
    public static final int OUTLIER = 1;        // 到达滑动步的点通道离群记录 / arrival-slide point outlier
    public static final int CTX = 2;            // 上下文评分（也是性能层元素）/ contextual score, also a perf element
    public static final int PERF = 3;           // 性能层候选、确认、撤销 / perf candidate, confirm, revoke
    public static final int DAY_RECORD = 4;     // 数据层日记录（按通道）/ data-layer daily record per channel
    public static final int TRANSITION = 5;     // 数据层状态转移 DEVIATE、RECOVER / data-layer transition
    public static final int BURST = 6;          // 离群率爆发 / outlier-rate burst

    /** 温度、气体、光照在检测通道中的下标，与 {@link FleetDecisionFunction#ACTIVE} 同序 / indices of the active channels. */
    static final int[] ACTIVE_INDEX = {0, 3, Channels.LIGHT_INDEX};

    public int kind;
    public String device;
    public long ts;             // 事件时间（秒）：轮时刻、离群轮时刻、窗口末、事件时刻、日界、报警元素 / event time (s)
    public long ts2;            // 离群记录的窗口末；爆发的发出时刻 / window end of an outlier; emission of a burst
    public boolean flag1;       // 轮：守卫打标；上下文：超阈；日记录：可判 / guard; above threshold; evaluable
    public boolean flag2;       // 轮：入口；日记录：日末偏离 / admitted; deviated at day end
    public boolean flag3;       // 轮：冻结之后 / post-freeze
    public double v1;           // 上下文：主分；性能层：门限 / main score; threshold
    public double v2;           // 性能层：幅度 / magnitude
    public double[] tv;         // 轮：温度、气体、光照的变换域读数，不可用为 NaN / transformed readings, NaN if unusable
    public double[] refs;       // 冻结后第一条轮：三个通道的参照 P10、P90 交替 / reference P10, P90 per active channel
    public String s1;           // 性能层：级别；日记录与转移：当地日 / level; local day
    public String s2;           // 日记录与转移：通道 / channel
    public String s3;           // 转移：DEVIATE 或 RECOVER / transition
    public int model;           // 上下文：模型代数，从未换过为零（1.41 版裁决第二节第五条）/ model generation, 0 if never changed
    public int version = 1;     // 轮、日记录与转移：参数版本号，命令回路关闭时为一（M6b 决定十四）/ parameter version, 1 when the loop is off

    public M7Input() {
    }

    private static M7Input of(int kind, String device, long ts) {
        M7Input in = new M7Input();
        in.kind = kind;
        in.device = device;
        in.ts = ts;
        return in;
    }

    /** 标准化轮：入口与冻结的判据与离线提取（eda/m7_extract.py）相同 / a standardized round. */
    public static M7Input fromRound(DeviceRound r) {
        M7Input in = of(ROUND, r.getDevice(), r.getTs());
        in.version = r.getVersion() == null ? 1 : r.getVersion();
        boolean[] missing = r.getMissingMask();
        boolean[] censored = r.getCensoredMask();
        boolean[] stuck = r.getStuckMask();
        in.flag3 = !r.isWarmup();
        in.flag2 = in.flag3 && r.missingCount() == 0;
        if (stuck != null) {
            for (boolean s : stuck) {
                in.flag1 |= s;
            }
        }
        in.tv = new double[ACTIVE_INDEX.length];
        for (int k = 0; k < ACTIVE_INDEX.length; k++) {
            int c = ACTIVE_INDEX[k];
            boolean ok = in.flag3 && !(missing != null && missing[c])
                    && !(c == Channels.LIGHT_INDEX && censored != null && censored[c]);
            double x = r.getX()[c];
            in.tv[k] = ok ? (c == Channels.LIGHT_INDEX ? Math.log1p(x) : x) : Double.NaN;
        }
        RefHistogram[] h = r.getRefHistograms();
        if (h != null) {
            in.refs = new double[2 * ACTIVE_INDEX.length];
            for (int k = 0; k < ACTIVE_INDEX.length; k++) {
                RefHistogram rh = h[ACTIVE_INDEX[k]];
                in.refs[2 * k] = rh == null ? Double.NaN : rh.getP10();
                in.refs[2 * k + 1] = rh == null ? Double.NaN : rh.getP90();
            }
        }
        return in;
    }

    /** 点通道评分：只取离群且轮时刻落在窗口末之前一个滑动步之内的；其余返回 null / arrival-slide outliers only. */
    public static M7Input fromScore(ScoreEvent e, long slideSec) {
        if (!"m2_point".equals(e.getChannel()) || !e.isOutlier()
                || e.getRoundTs() < e.getWindowEnd() - slideSec || e.getRoundTs() >= e.getWindowEnd()) {
            return null;
        }
        M7Input in = of(OUTLIER, e.getDevice(), e.getRoundTs());
        in.ts2 = e.getWindowEnd();
        return in;
    }

    /** 上下文评分 / a contextual score. */
    public static M7Input fromCtx(M3ScoreRecord r) {
        M7Input in = of(CTX, r.getDevice(), r.getWindowEnd());
        in.v1 = r.getMainScore();
        in.flag1 = r.isAboveThreshold();
        in.model = r.getModel() == null ? 0 : r.getModel();
        return in;
    }

    /**
     * 漂移主题的事件：性能层的候选、确认、撤销，数据层的转移与爆发；recordTs 为该记录的事件时间戳（秒），爆发以它为发出时刻。
     * 其余返回 null。/ Drift events; recordTs (s) is the record timestamp, used as a burst's emission time.
     */
    public static M7Input fromDrift(DriftEvent e, long recordTs) {
        if (e.level == null) {
            return null;
        }
        switch (e.level) {
            case CANDIDATE:
            case CONFIRM:
            case REVOKE: {
                M7Input in = of(PERF, e.device, e.ts);
                in.s1 = e.level.name();
                in.v1 = e.threshold;
                in.v2 = e.magnitude;
                return in;
            }
            case DEVIATE:
            case RECOVER: {
                M7Input in = of(TRANSITION, e.device, e.ts);
                in.s1 = e.dataDay;
                in.s2 = e.channel;
                in.s3 = e.level.name();
                in.version = e.version == null ? 1 : e.version;
                return in;
            }
            case BURST: {
                M7Input in = of(BURST, e.device, e.ts);
                in.ts2 = recordTs;
                return in;
            }
            default:
                return null;
        }
    }

    /** 数据层日记录；不是日记录的监测快照返回 null / a data-layer daily record. */
    public static M7Input fromRecord(MonitoringSnapshot s) {
        if (s.getM5aDay() == null || s.getM5aChannel() == null) {
            return null;
        }
        M7Input in = of(DAY_RECORD, s.getDevice(), s.getTs());
        in.s1 = s.getM5aDay();
        in.s2 = s.getM5aChannel();
        in.flag1 = s.isM5aEvaluable();
        in.flag2 = s.isM5aDeviated();
        in.version = s.getVersion() == null ? 1 : s.getVersion();
        return in;
    }
}
