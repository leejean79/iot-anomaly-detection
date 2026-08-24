package com.leejean.m1;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 单条传感器读数：由 RawLineParser 从一行原始 CSV 解析而来。
 * A single sensor reading, parsed by RawLineParser from one raw CSV line.
 *
 * <p>字段 / fields：ts 为行内 Unix epoch 秒（唯一权威时间，DF-8）；device 为 DeviceId；
 * sensor 为原始传感器名；value 为读数。kind 为分类结果（Channels.KIND_*）。
 * 删失/哨兵标志随读数传播（DF-11 / DEV-D7c）。
 * ts is the in-row Unix epoch second (the authoritative time, DF-8); kind is the
 * classification (Channels.KIND_*); censoring/sentinel flags ride along.
 */
public class Reading implements Serializable {
    private static final long serialVersionUID = 1L;

    private long ts;                 // 行内 epoch 秒 / in-row epoch second
    private String device;
    private String sensor;
    private double value;
    private int kind;                // Channels.KIND_DETECTION / KIND_QUALITY / KIND_UNKNOWN / KIND_MALFORMED
    private int channelIndex;        // 检测通道下标；非检测 = -1 / detection index, or -1
    private boolean censored;        // DF-11：Light == 65536 右删失 / Light right-censored
    private boolean rssiSentinel;    // DEV-D7c：RSSI == 0 已替换为缺失哨兵 / RSSI replaced by missing sentinel

    /** Jackson 反序列化需要无参构造 / No-arg constructor required by Jackson + Flink POJO. */
    public Reading() { }

    public Reading(long ts, String device, String sensor, double value,
                   int kind, int channelIndex, boolean censored, boolean rssiSentinel) {
        this.ts = ts;
        this.device = device;
        this.sensor = sensor;
        this.value = value;
        this.kind = kind;
        this.channelIndex = channelIndex;
        this.censored = censored;
        this.rssiSentinel = rssiSentinel;
    }

    @JsonProperty
    public long getTs() { return ts; }
    public void setTs(long ts) { this.ts = ts; }

    @JsonProperty
    public String getDevice() { return device; }
    public void setDevice(String device) { this.device = device; }

    @JsonProperty
    public String getSensor() { return sensor; }
    public void setSensor(String sensor) { this.sensor = sensor; }

    @JsonProperty
    public double getValue() { return value; }
    public void setValue(double value) { this.value = value; }

    @JsonProperty
    public int getKind() { return kind; }
    public void setKind(int kind) { this.kind = kind; }

    @JsonProperty
    public int getChannelIndex() { return channelIndex; }
    public void setChannelIndex(int channelIndex) { this.channelIndex = channelIndex; }

    @JsonProperty
    public boolean isCensored() { return censored; }
    public void setCensored(boolean censored) { this.censored = censored; }

    @JsonProperty
    public boolean isRssiSentinel() { return rssiSentinel; }
    public void setRssiSentinel(boolean rssiSentinel) { this.rssiSentinel = rssiSentinel; }

    @Override
    public String toString() {
        return String.format("Reading{ts=%d, dev=%s, sensor=%s, value=%.4f, kind=%d, ch=%d, cens=%b, rssiSent=%b}",
                ts, device, sensor, value, kind, channelIndex, censored, rssiSentinel);
    }
}
