package com.leejean.m3;

import java.io.Serializable;

/**
 * M2→M3 桥接类型：标准化后的轮 + M2 点通道的离群标记。
 * Bridge type from M2 to M3: a normalized round annotated with the M2 point-channel outlier flag.
 *
 * <p>PmcodFunction 在每个滑动步结束后，对该步的每个轮产出一条 AnnotatedRound，附带 MCOD 判定的
 * 离群状态。M3 的训练数据净化据此排除含离群轮的窗口。
 *
 * <p>PmcodFunction emits one AnnotatedRound per round in each slide, carrying the MCOD-determined
 * outlier flag. M3 uses this to sanitize training data by excluding windows that contain an
 * outlier-flagged round.
 */
public class AnnotatedRound implements Serializable {
    private static final long serialVersionUID = 1L;

    private String device;          // 设备 ID（A–H）/ device id (A–H)
    private long ts;                // 轮的事件时间（epoch 秒）/ round event time (epoch seconds)
    private double[] xNorm;         // M1 标准化后的五通道特征向量 / M1-normalized 5-channel feature vector
    private boolean outlier;        // M2 MCOD 是否判该轮为离群 / whether M2's MCOD flagged this round as an outlier
    private boolean[] censoredMask; // 删失掩码（true = 该通道删失，如 Light 右删失）/ censored mask (true = channel censored)
    private boolean coldStart;      // 该轮是否处于冷启动 / whether this round is in cold-start
    private long windowEnd;         // 产出该轮的滑动窗口末事件时间（秒）/ end event time of the emitting slide window

    // 参数版本号（M6a）：命令回路关闭时为空且不写进 JSON / parameter version (M6a), null and omitted when the loop is off
    private Integer version;
    // 原始检测值与参数集附件（M6b 选项一）：回路关闭时为空且不写进 JSON；上下文模块有它们时按训练版本自己标准化。
    // Raw values and the parameter-set attachment (option 1); null and omitted when the loop is off.
    private double[] raw;
    private com.leejean.m6.ParamSet paramSet;
    // 数据接入模块转发的一级或二级命令（M6b 第五节）/ the level-1/2 command forwarded by the data-access module
    private com.leejean.m6.AdaptCommand contextCommand;

    public AnnotatedRound() {
    }

    public AnnotatedRound(String device, long ts, double[] xNorm, boolean outlier,
                          boolean[] censoredMask, boolean coldStart, long windowEnd) {
        this.device = device;
        this.ts = ts;
        this.xNorm = xNorm;
        this.outlier = outlier;
        this.censoredMask = censoredMask;
        this.coldStart = coldStart;
        this.windowEnd = windowEnd;
    }

    public String getDevice() { return device; }
    public void setDevice(String device) { this.device = device; }

    public long getTs() { return ts; }
    public void setTs(long ts) { this.ts = ts; }

    public double[] getXNorm() { return xNorm; }
    public void setXNorm(double[] xNorm) { this.xNorm = xNorm; }

    public boolean isOutlier() { return outlier; }
    public void setOutlier(boolean outlier) { this.outlier = outlier; }

    public boolean[] getCensoredMask() { return censoredMask; }
    public void setCensoredMask(boolean[] censoredMask) { this.censoredMask = censoredMask; }

    public boolean isColdStart() { return coldStart; }
    public void setColdStart(boolean coldStart) { this.coldStart = coldStart; }

    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
    public double[] getRaw() {
        return raw;
    }

    public void setRaw(double[] raw) {
        this.raw = raw;
    }

    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
    public com.leejean.m6.ParamSet getParamSet() {
        return paramSet;
    }

    public void setParamSet(com.leejean.m6.ParamSet paramSet) {
        this.paramSet = paramSet;
    }

    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
    public com.leejean.m6.AdaptCommand getContextCommand() {
        return contextCommand;
    }

    public void setContextCommand(com.leejean.m6.AdaptCommand contextCommand) {
        this.contextCommand = contextCommand;
    }
}
