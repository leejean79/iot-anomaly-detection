package com.leejean.m1;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 逐设备、每 60s 事件时间的监测快照（交接文档 §4.6b），发往 synergia-monitoring。
 * Per-device monitoring snapshot, one per 60 s of event time (handover §4.6b), sent to
 * synergia-monitoring.
 *
 * <p>字段为该 60s 窗口内的增量计数 / fields are incremental counts over the 60 s window.
 */
public class MonitoringSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    private long ts;                 // 窗口结束的事件时间（epoch 秒）/ window-end event time (epoch seconds)
    private String device;
    private long roundsTotal;
    private long incompleteRounds;
    private long malformed;
    private long unknownSensor;
    private long dupKeys;
    private long censoredLight;
    private long rssiSentinel;
    private long warmup;
    private long bypassedChannels;
    private long substitutedChannels;   // 用替代分母（相对退化防护）的通道计数（补充指令三）
    private long degradedChannelsMask;  // 被旁路或替代分母的通道位图（列表，第 c 位=通道 c）
    private long coldStart;
    private long stuckRounds;           // 本窗口内至少一个通道重复值游程达阈值的轮数（开发决策第十条）
    private long stuckChannelsMask;     // 本窗口内游程达阈值的通道位图（第 c 位=通道 c）
    private long floorRounds;           // 本窗口内至少一个通道底值游程达阈值的轮数（2026-10-12 裁决第二节）
    private long floorChannelsMask;     // 本窗口内底值游程达阈值的通道位图
    private long[] guardRunLength;      // 本窗口最后一轮：处于疑似卡死或底值游程的通道的游程长度（轮），其余为 0
    private long[] guardRunStartTs;     // 同上：游程起始轮时间戳（秒），其余为 0

    // ---- M2 追加字段（交接文档 §5.2；并入同一 topic 的设备快照，不另建 topic）----
    // M2-appended fields (handover §5.2): folded into the same monitoring topic; M1 快照保持这些为 0。
    // M2 的快照按滑动步（每设备每 S 秒）产出，M2 字段有值、M1 字段为 0；两类消息共存于 synergia-monitoring。
    private double m2OutlierRate;        // 本窗口离群点数 ÷ 窗口内总点数
    private double m2McOccupancy;        // 微簇内点数 ÷ 窗口内总点数（尺度漂移灵敏指示）
    private double m2NeighborCountP10;   // 窗口内各 PD 点"R 内邻居数"的 P10
    private double m2NeighborCountP50;   // 同上的 P50（中位）
    private long m2Outliers;             // 本窗口离群点数
    private long m2WindowPoints;         // 本窗口总点数
    private long windowEnd;              // 滑动窗口末（事件时间秒；供区分 M2 快照）
    private boolean m2ColdCleared;       // 本滑动步是否发生冷启动清空（供 DF-12 浪涌分析精确定位清空时刻）

    // ---- M3 追加字段（交接文档 §3 决策 7）：上下文通道的重建误差 ----
    // M3-appended fields (handover §3 decision 7): contextual-channel reconstruction errors.
    private double m3ReconError;         // 窗口加权 MSE / window weighted MSE
    private double[] m3PerChannelErrors; // 每通道 MSE [5] / per-channel MSE
    private int m3Phase;                 // 上下文通道相位（0 收集、1 训练、3 在线；补遗四）/ M3 phase
    private long m3BufferRounds;         // 上下文通道训练缓冲占用（轮；补遗四）/ M3 training-buffer occupancy

    // ---- M5b 追加字段（M5b 交接文档决定四；windowEnd = -2 的快照才有意义）/ M5b fields (snapshots with windowEnd = -2) ----
    private int m5bWindowLen;            // 检测器当前窗长 / detector window length
    private long m5bLastCandidateTs;     // 最近一次候选的事件时间（秒）/ last candidate event time (s)
    private long m5bLastConfirmTs;       // 最近一次确认的事件时间（秒）/ last confirmation event time (s)
    private long m5bFWithoutRise;        // F 显著而均值未升高的累计次数（10 月 19 日裁决第二点）/ significant F without a rise
    private long m5bTSkippedConstant;    // 两个子窗都为常数、不做 t 检验的累计次数（0.94 版第八条）/ t-test skipped, both constant

    // ---- M5a 日记录字段（M5a 交接文档决定六；windowEnd = -3 的记录才有意义，每台每通道每个当地日一条）/ M5a daily-record fields ----
    private String m5aDay;          // 当地日
    private String m5aChannel;      // 检测通道
    private boolean m5aEvaluable;   // 可判
    private String m5aReason;       // 不可判的原因
    private int m5aRounds;          // 当日实到轮数
    private long m5aExpected;       // 当日应有轮数
    private int m5aGuardRounds;     // 守卫打标轮数
    private int m5aN;               // 当日该通道的读数数
    private double m5aD;            // 分箱 D
    private double m5aP;            // 渐近显著性
    private double m5aP10;          // 当日 P10
    private double m5aMedian;       // 当日中位数
    private double m5aP90;          // 当日 P90
    private double m5aShift;        // 偏移（参照宽度单位）
    private double m5aWidthX;       // 宽度倍数
    private double m5aMaxCellMass;  // 当日单箱最大质量
    private boolean m5aDeviated;    // 日末状态：已偏离

    /** Jackson 反序列化需要无参构造 / No-arg constructor required by Jackson. */
    public MonitoringSnapshot() { }

    @JsonProperty
    public long getTs() { return ts; }
    public void setTs(long ts) { this.ts = ts; }

    @JsonProperty
    public String getDevice() { return device; }
    public void setDevice(String device) { this.device = device; }

    @JsonProperty
    public long getRoundsTotal() { return roundsTotal; }
    public void setRoundsTotal(long roundsTotal) { this.roundsTotal = roundsTotal; }

    @JsonProperty
    public long getIncompleteRounds() { return incompleteRounds; }
    public void setIncompleteRounds(long incompleteRounds) { this.incompleteRounds = incompleteRounds; }

    @JsonProperty
    public long getMalformed() { return malformed; }
    public void setMalformed(long malformed) { this.malformed = malformed; }

    @JsonProperty
    public long getUnknownSensor() { return unknownSensor; }
    public void setUnknownSensor(long unknownSensor) { this.unknownSensor = unknownSensor; }

    @JsonProperty
    public long getDupKeys() { return dupKeys; }
    public void setDupKeys(long dupKeys) { this.dupKeys = dupKeys; }

    @JsonProperty
    public long getCensoredLight() { return censoredLight; }
    public void setCensoredLight(long censoredLight) { this.censoredLight = censoredLight; }

    @JsonProperty
    public long getRssiSentinel() { return rssiSentinel; }
    public void setRssiSentinel(long rssiSentinel) { this.rssiSentinel = rssiSentinel; }

    @JsonProperty
    public long getWarmup() { return warmup; }
    public void setWarmup(long warmup) { this.warmup = warmup; }

    @JsonProperty
    public long getBypassedChannels() { return bypassedChannels; }
    public void setBypassedChannels(long bypassedChannels) { this.bypassedChannels = bypassedChannels; }

    @JsonProperty
    public long getSubstitutedChannels() { return substitutedChannels; }
    public void setSubstitutedChannels(long substitutedChannels) { this.substitutedChannels = substitutedChannels; }

    @JsonProperty
    public long getDegradedChannelsMask() { return degradedChannelsMask; }
    public void setDegradedChannelsMask(long degradedChannelsMask) { this.degradedChannelsMask = degradedChannelsMask; }

    @JsonProperty
    public long getColdStart() { return coldStart; }
    public void setColdStart(long coldStart) { this.coldStart = coldStart; }

    @JsonProperty
    public long getStuckRounds() { return stuckRounds; }
    public void setStuckRounds(long stuckRounds) { this.stuckRounds = stuckRounds; }

    @JsonProperty
    public long getStuckChannelsMask() { return stuckChannelsMask; }
    public void setStuckChannelsMask(long stuckChannelsMask) { this.stuckChannelsMask = stuckChannelsMask; }

    @JsonProperty
    public long getFloorRounds() { return floorRounds; }
    public void setFloorRounds(long floorRounds) { this.floorRounds = floorRounds; }

    @JsonProperty
    public long getFloorChannelsMask() { return floorChannelsMask; }
    public void setFloorChannelsMask(long floorChannelsMask) { this.floorChannelsMask = floorChannelsMask; }

    @JsonProperty
    public long[] getGuardRunLength() { return guardRunLength; }
    public void setGuardRunLength(long[] guardRunLength) { this.guardRunLength = guardRunLength; }

    @JsonProperty
    public long[] getGuardRunStartTs() { return guardRunStartTs; }
    public void setGuardRunStartTs(long[] guardRunStartTs) { this.guardRunStartTs = guardRunStartTs; }

    @JsonProperty
    public double getM2OutlierRate() { return m2OutlierRate; }
    public void setM2OutlierRate(double m2OutlierRate) { this.m2OutlierRate = m2OutlierRate; }

    @JsonProperty
    public double getM2McOccupancy() { return m2McOccupancy; }
    public void setM2McOccupancy(double m2McOccupancy) { this.m2McOccupancy = m2McOccupancy; }

    @JsonProperty
    public double getM2NeighborCountP10() { return m2NeighborCountP10; }
    public void setM2NeighborCountP10(double v) { this.m2NeighborCountP10 = v; }

    @JsonProperty
    public double getM2NeighborCountP50() { return m2NeighborCountP50; }
    public void setM2NeighborCountP50(double v) { this.m2NeighborCountP50 = v; }

    @JsonProperty
    public long getM2Outliers() { return m2Outliers; }
    public void setM2Outliers(long m2Outliers) { this.m2Outliers = m2Outliers; }

    @JsonProperty
    public long getM2WindowPoints() { return m2WindowPoints; }
    public void setM2WindowPoints(long m2WindowPoints) { this.m2WindowPoints = m2WindowPoints; }

    @JsonProperty
    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

    @JsonProperty
    public boolean isM2ColdCleared() { return m2ColdCleared; }
    public void setM2ColdCleared(boolean m2ColdCleared) { this.m2ColdCleared = m2ColdCleared; }

    @JsonProperty
    public int getM3Phase() { return m3Phase; }
    public void setM3Phase(int m3Phase) { this.m3Phase = m3Phase; }
    public long getM3BufferRounds() { return m3BufferRounds; }
    public void setM3BufferRounds(long m3BufferRounds) { this.m3BufferRounds = m3BufferRounds; }

    @JsonProperty
    public int getM5bWindowLen() { return m5bWindowLen; }
    public void setM5bWindowLen(int m5bWindowLen) { this.m5bWindowLen = m5bWindowLen; }
    @JsonProperty
    public long getM5bLastCandidateTs() { return m5bLastCandidateTs; }
    public void setM5bLastCandidateTs(long m5bLastCandidateTs) { this.m5bLastCandidateTs = m5bLastCandidateTs; }
    @JsonProperty
    public long getM5bLastConfirmTs() { return m5bLastConfirmTs; }
    public void setM5bLastConfirmTs(long m5bLastConfirmTs) { this.m5bLastConfirmTs = m5bLastConfirmTs; }
    @JsonProperty
    public long getM5bFWithoutRise() { return m5bFWithoutRise; }
    public void setM5bFWithoutRise(long m5bFWithoutRise) { this.m5bFWithoutRise = m5bFWithoutRise; }
    @JsonProperty
    public long getM5bTSkippedConstant() { return m5bTSkippedConstant; }
    public void setM5bTSkippedConstant(long m5bTSkippedConstant) { this.m5bTSkippedConstant = m5bTSkippedConstant; }

    @JsonProperty
    public String getM5aDay() { return m5aDay; }
    public void setM5aDay(String m5aDay) { this.m5aDay = m5aDay; }
    @JsonProperty
    public String getM5aChannel() { return m5aChannel; }
    public void setM5aChannel(String m5aChannel) { this.m5aChannel = m5aChannel; }
    @JsonProperty
    public boolean isM5aEvaluable() { return m5aEvaluable; }
    public void setM5aEvaluable(boolean m5aEvaluable) { this.m5aEvaluable = m5aEvaluable; }
    @JsonProperty
    public String getM5aReason() { return m5aReason; }
    public void setM5aReason(String m5aReason) { this.m5aReason = m5aReason; }
    @JsonProperty
    public int getM5aRounds() { return m5aRounds; }
    public void setM5aRounds(int m5aRounds) { this.m5aRounds = m5aRounds; }
    @JsonProperty
    public long getM5aExpected() { return m5aExpected; }
    public void setM5aExpected(long m5aExpected) { this.m5aExpected = m5aExpected; }
    @JsonProperty
    public int getM5aGuardRounds() { return m5aGuardRounds; }
    public void setM5aGuardRounds(int m5aGuardRounds) { this.m5aGuardRounds = m5aGuardRounds; }
    @JsonProperty
    public int getM5aN() { return m5aN; }
    public void setM5aN(int m5aN) { this.m5aN = m5aN; }
    @JsonProperty
    public double getM5aD() { return m5aD; }
    public void setM5aD(double m5aD) { this.m5aD = m5aD; }
    @JsonProperty
    public double getM5aP() { return m5aP; }
    public void setM5aP(double m5aP) { this.m5aP = m5aP; }
    @JsonProperty
    public double getM5aP10() { return m5aP10; }
    public void setM5aP10(double m5aP10) { this.m5aP10 = m5aP10; }
    @JsonProperty
    public double getM5aMedian() { return m5aMedian; }
    public void setM5aMedian(double m5aMedian) { this.m5aMedian = m5aMedian; }
    @JsonProperty
    public double getM5aP90() { return m5aP90; }
    public void setM5aP90(double m5aP90) { this.m5aP90 = m5aP90; }
    @JsonProperty
    public double getM5aShift() { return m5aShift; }
    public void setM5aShift(double m5aShift) { this.m5aShift = m5aShift; }
    @JsonProperty
    public double getM5aWidthX() { return m5aWidthX; }
    public void setM5aWidthX(double m5aWidthX) { this.m5aWidthX = m5aWidthX; }
    @JsonProperty
    public double getM5aMaxCellMass() { return m5aMaxCellMass; }
    public void setM5aMaxCellMass(double m5aMaxCellMass) { this.m5aMaxCellMass = m5aMaxCellMass; }
    @JsonProperty
    public boolean isM5aDeviated() { return m5aDeviated; }
    public void setM5aDeviated(boolean m5aDeviated) { this.m5aDeviated = m5aDeviated; }

    public double getM3ReconError() { return m3ReconError; }
    public void setM3ReconError(double m3ReconError) { this.m3ReconError = m3ReconError; }

    @JsonProperty
    public double[] getM3PerChannelErrors() { return m3PerChannelErrors; }
    public void setM3PerChannelErrors(double[] m3PerChannelErrors) { this.m3PerChannelErrors = m3PerChannelErrors; }

    @Override
    public String toString() {
        return String.format(
                "MonitoringSnapshot{ts=%d, dev=%s, rounds=%d, incomplete=%d, malformed=%d, "
                        + "unknown=%d, dup=%d, censLight=%d, rssiSent=%d, warmup=%d, bypassed=%d, cold=%d}",
                ts, device, roundsTotal, incompleteRounds, malformed, unknownSensor, dupKeys,
                censoredLight, rssiSentinel, warmup, bypassedChannels, coldStart);
    }
}
