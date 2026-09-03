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
    private long coldStart;

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
    public long getColdStart() { return coldStart; }
    public void setColdStart(long coldStart) { this.coldStart = coldStart; }

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

    @Override
    public String toString() {
        return String.format(
                "MonitoringSnapshot{ts=%d, dev=%s, rounds=%d, incomplete=%d, malformed=%d, "
                        + "unknown=%d, dup=%d, censLight=%d, rssiSent=%d, warmup=%d, bypassed=%d, cold=%d}",
                ts, device, roundsTotal, incompleteRounds, malformed, unknownSensor, dupKeys,
                censoredLight, rssiSentinel, warmup, bypassedChannels, coldStart);
    }
}
