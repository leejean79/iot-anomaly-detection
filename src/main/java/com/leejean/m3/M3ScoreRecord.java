package com.leejean.m3;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * M3 上下文通道的评分记录（交接文档 §3 决策 7）：一条/每窗口 → {@code synergia-scores}。
 * M3 contextual-channel score record (handover §3 decision 7): one per window to synergia-scores.
 *
 * <p>JSON 形态 / JSON shape:
 * {device, windowEnd, channel:"m3_context", mainScore, mahaScore, wmse, perChannelErrors[5],
 *  aboveThreshold, hiddenSize, windowLength}。
 */
public class M3ScoreRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private String device;             // 设备 ID / device id
    private long windowEnd;            // 评分窗口末事件时间（秒）/ end event time of the scored window (seconds)
    private String channel;            // 通道标识，固定 "m3_context" 供下游区分信号来源 / fixed "m3_context" tag
    private double mainScore;          // 主分 z =（wmse−median）÷IQR / main score z = (wmse−median)/IQR
    private double mahaScore;          // Mahalanobis 分（仅报告，不报警）/ Mahalanobis score (report only)
    private double wmse;               // 窗口加权 MSE / window weighted MSE
    private double[] perChannelErrors; // 每通道 MSE [5] / per-channel MSE [5]
    private boolean aboveThreshold;    // 主分是否越阈值（是否报警）/ whether the main score crosses the threshold
    private int hiddenSize;            // 该设备选中的 LSTM 隐藏层宽度 / selected LSTM hidden width for this device
    private int windowLength;          // 窗口长度（轮数）/ window length in rounds

    public M3ScoreRecord() {
        this.channel = "m3_context";   // 无参构造也固定通道标识 / no-arg ctor still sets the channel tag
    }

    public M3ScoreRecord(String device, long windowEnd, double mainScore, double mahaScore,
                         double wmse, double[] perChannelErrors, boolean aboveThreshold,
                         int hiddenSize, int windowLength) {
        this.device = device;
        this.windowEnd = windowEnd;
        this.channel = "m3_context";
        this.mainScore = mainScore;
        this.mahaScore = mahaScore;
        this.wmse = wmse;
        this.perChannelErrors = perChannelErrors;
        this.aboveThreshold = aboveThreshold;
        this.hiddenSize = hiddenSize;
        this.windowLength = windowLength;
    }

    @JsonProperty public String getDevice() { return device; }
    public void setDevice(String device) { this.device = device; }

    @JsonProperty public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

    @JsonProperty public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }

    @JsonProperty public double getMainScore() { return mainScore; }
    public void setMainScore(double mainScore) { this.mainScore = mainScore; }

    @JsonProperty public double getMahaScore() { return mahaScore; }
    public void setMahaScore(double mahaScore) { this.mahaScore = mahaScore; }

    @JsonProperty public double getWmse() { return wmse; }
    public void setWmse(double wmse) { this.wmse = wmse; }

    @JsonProperty public double[] getPerChannelErrors() { return perChannelErrors; }
    public void setPerChannelErrors(double[] perChannelErrors) { this.perChannelErrors = perChannelErrors; }

    @JsonProperty public boolean isAboveThreshold() { return aboveThreshold; }
    public void setAboveThreshold(boolean aboveThreshold) { this.aboveThreshold = aboveThreshold; }

    @JsonProperty public int getHiddenSize() { return hiddenSize; }
    public void setHiddenSize(int hiddenSize) { this.hiddenSize = hiddenSize; }

    @JsonProperty public int getWindowLength() { return windowLength; }
    public void setWindowLength(int windowLength) { this.windowLength = windowLength; }
}
