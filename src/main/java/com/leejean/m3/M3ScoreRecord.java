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

    private String device;
    private long windowEnd;
    private String channel;
    private double mainScore;
    private double mahaScore;
    private double wmse;
    private double[] perChannelErrors;
    private boolean aboveThreshold;
    private int hiddenSize;
    private int windowLength;

    public M3ScoreRecord() {
        this.channel = "m3_context";
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
