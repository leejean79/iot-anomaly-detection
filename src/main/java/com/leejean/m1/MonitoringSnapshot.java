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

    @Override
    public String toString() {
        return String.format(
                "MonitoringSnapshot{ts=%d, dev=%s, rounds=%d, incomplete=%d, malformed=%d, "
                        + "unknown=%d, dup=%d, censLight=%d, rssiSent=%d, warmup=%d, bypassed=%d, cold=%d}",
                ts, device, roundsTotal, incompleteRounds, malformed, unknownSensor, dupKeys,
                censoredLight, rssiSentinel, warmup, bypassedChannels, coldStart);
    }
}
