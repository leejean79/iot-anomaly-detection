package com.leejean.m2;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 离群点名单消息（交接文档 §5.1）：每个当前判定为离群的点输出一条 → {@code synergia-scores}。
 * One outlier-point record (handover §5.1): emitted per currently-outlier point to {@code synergia-scores}.
 *
 * <p>JSON 形态：{device, roundTs, windowEnd, channel:"m2_point", outlier:true}。序列化沿用
 * M1 的 Jackson bean + KafkaSerializationSchema 成对模板。
 */
public class ScoreEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String device;
    private long roundTs;        // 离群点所属轮的时间戳秒（= McodPoint.id）
    private long windowEnd;      // 判定所在滑动窗口的窗口末（事件时间秒）
    private String channel;      // 恒 "m2_point"
    private boolean outlier;     // 恒 true（名单只列离群点）

    public ScoreEvent() {
    }

    public ScoreEvent(String device, long roundTs, long windowEnd) {
        this.device = device;
        this.roundTs = roundTs;
        this.windowEnd = windowEnd;
        this.channel = "m2_point";
        this.outlier = true;
    }

    @JsonProperty
    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    @JsonProperty
    public long getRoundTs() {
        return roundTs;
    }

    public void setRoundTs(long roundTs) {
        this.roundTs = roundTs;
    }

    @JsonProperty
    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @JsonProperty
    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    @JsonProperty
    public boolean isOutlier() {
        return outlier;
    }

    public void setOutlier(boolean outlier) {
        this.outlier = outlier;
    }
}
