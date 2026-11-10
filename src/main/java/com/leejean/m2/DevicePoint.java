package com.leejean.m2;

import java.io.Serializable;

/**
 * 设备键 + MCOD 点的配对：对应原文 {@code Pmcod.scala:17} 的流元素类型 {@code (Int, Data)}——
 * MCOD 按 key（这里是 deviceId）分区，点本身不带设备字段（与裁剪后的 {@link McodPoint} 一致）。
 * A (deviceId, McodPoint) pair mirroring the source's stream element type {@code (Int, Data)}:
 * MCOD is keyed by device, and the point itself carries no device field (matching the trimmed Data).
 */
public class DevicePoint implements Serializable {
    private static final long serialVersionUID = 1L;

    private String device;
    private McodPoint point;
    private boolean[] censoredMask;
    private Integer version;                          // 参数版本号（M6a），回路关闭时为空 / parameter version, null when off
    private com.leejean.m6.Backfill backfill;         // 零级回填附件（M6a）/ level-0 backfill
    private com.leejean.m6.ParamSet paramSet;         // 参数集附件（M6b 选项一）/ parameter-set attachment
    private double[] raw;                             // 原始检测值，回路打开时随点下发给上下文模块 / raw values (loop on)
    private com.leejean.m6.AdaptCommand contextCommand;   // 转发给上下文模块的一级或二级命令（M6b）/ level-1/2 command

    public DevicePoint() {
    }

    public DevicePoint(String device, McodPoint point) {
        this.device = device;
        this.point = point;
    }

    public DevicePoint(String device, McodPoint point, boolean[] censoredMask) {
        this.device = device;
        this.point = point;
        this.censoredMask = censoredMask;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public McodPoint getPoint() {
        return point;
    }

    public void setPoint(McodPoint point) {
        this.point = point;
    }

    public boolean[] getCensoredMask() {
        return censoredMask;
    }

    public void setCensoredMask(boolean[] censoredMask) {
        this.censoredMask = censoredMask;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public com.leejean.m6.Backfill getBackfill() {
        return backfill;
    }

    public void setBackfill(com.leejean.m6.Backfill backfill) {
        this.backfill = backfill;
    }

    public com.leejean.m6.ParamSet getParamSet() {
        return paramSet;
    }

    public void setParamSet(com.leejean.m6.ParamSet paramSet) {
        this.paramSet = paramSet;
    }

    public double[] getRaw() {
        return raw;
    }

    public com.leejean.m6.AdaptCommand getContextCommand() {
        return contextCommand;
    }

    public void setContextCommand(com.leejean.m6.AdaptCommand contextCommand) {
        this.contextCommand = contextCommand;
    }

    public void setRaw(double[] raw) {
        this.raw = raw;
    }
}
