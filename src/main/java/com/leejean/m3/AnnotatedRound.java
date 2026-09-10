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

    private String device;
    private long ts;
    private double[] xNorm;
    private boolean outlier;
    private boolean[] censoredMask;
    private boolean coldStart;
    private long windowEnd;

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
}
