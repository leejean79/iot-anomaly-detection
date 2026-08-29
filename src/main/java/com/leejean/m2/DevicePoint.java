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

    public DevicePoint() {
    }

    public DevicePoint(String device, McodPoint point) {
        this.device = device;
        this.point = point;
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
}
