package com.leejean.m1;

import java.io.Serializable;

/**
 * 通道定义与传感器分类（M1 数据事实 DF-1/DF-9/DF-11、决策 DEV-D7）。
 * Channel definitions and sensor classification (M1 data facts DF-1/DF-9/DF-11, decision DEV-D7).
 *
 * <p>F_det 检测特征集固定顺序（交接文档 §4.3）：Temperature, Humidity, Pressure, Gas, Light。
 * F_det detection feature set in fixed order (handover §4.3).
 *
 * <p>质量通道 / quality channels：MIC（Gas 质量元数据，DEV-D7b）、RSSI（DEV-D7c）、Accelerometer。
 * 未知通道 / unknown channels：IR 及其它一切（DF-9，计数并丢弃 / count and drop）。
 */
public final class Channels implements Serializable {
    private static final long serialVersionUID = 1L;

    private Channels() { }

    // ---- F_det 五个检测通道，固定顺序 / the five detection channels, fixed order ----
    public static final String[] DETECTION = {
            "Temperature", "Humidity", "Pressure", "Gas", "Light"
    };
    public static final int N_DET = DETECTION.length;   // 5

    // Light 通道在 x[] 中的下标（右删失判据用）/ Light index (for right-censoring, DF-11)
    public static final int LIGHT_INDEX = 4;

    // ---- 质量通道 / quality channels ----
    public static final String MIC = "MIC";
    public static final String RSSI = "RSSI";
    public static final String ACCELEROMETER = "Accelerometer";

    // ---- 传感器分类结果 / sensor classification kinds ----
    public static final int KIND_DETECTION = 0;   // 进入 x[] / enters the detection vector
    public static final int KIND_QUALITY = 1;     // 进入 quality 元数据 / quality metadata
    public static final int KIND_UNKNOWN = 2;     // DF-9：计数丢弃 / count and drop
    public static final int KIND_MALFORMED = 3;   // 行畸形 / malformed line

    // ---- 哨兵/删失阈值（数据事实）/ sentinel & censoring thresholds (data facts) ----
    public static final double LIGHT_CENSOR_VALUE = 65536.0;  // DF-11：Light 右删失 / right-censored
    public static final double RSSI_SENTINEL_VALUE = 0.0;     // DEV-D7c：RSSI==0 → 缺失哨兵 / missing sentinel

    /**
     * 返回检测通道下标；非检测通道返回 -1。
     * Detection-channel index, or -1 if the sensor is not a detection channel.
     */
    public static int detectionIndex(String sensor) {
        if (sensor == null) {
            return -1;
        }
        for (int i = 0; i < DETECTION.length; i++) {
            if (DETECTION[i].equals(sensor)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 传感器分类：检测 / 质量 / 未知。畸形由解析器单独判定，不在此。
     * Classify a sensor as detection / quality / unknown. Malformed is decided by the parser.
     */
    public static int classify(String sensor) {
        if (detectionIndex(sensor) >= 0) {
            return KIND_DETECTION;
        }
        if (MIC.equals(sensor) || RSSI.equals(sensor) || ACCELEROMETER.equals(sensor)) {
            return KIND_QUALITY;
        }
        return KIND_UNKNOWN;   // IR 及其它 / IR and everything else (DF-9)
    }
}
