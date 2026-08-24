package com.leejean.m1;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 一个采样轮：同一设备、同一权威时间戳（DF-1，精确相等）下装配的一组读数。
 * One sampling round: the readings of a single device that share one exact authoritative
 * timestamp (DF-1, exact equality).
 *
 * <p>结构（交接文档 §4.3）/ structure (handover §4.3)：
 * x[5] = F_det 固定顺序 (Temperature, Humidity, Pressure, Gas, Light) 的**原始**值；
 * xNorm[5] = RobustScaler 归一化后的值（预热期为原始透传，warmup=true）；
 * quality = {mic, rssi, accel}；掩码 missingMask/censoredMask/bypassMask 各 5 位。
 * x[5] holds the RAW F_det values; xNorm[5] the RobustScaler-normalized values (raw pass-through
 * during warm-up); quality carries mic/rssi/accel; three 5-bit masks accompany them.
 *
 * <p>RawCache 缓存的是**原始未归一化**轮（§4.5）——本 bean 同时携带 x（原始）与 xNorm，
 * 缓存侧只取 x。RawCache stores the RAW round; this bean carries both x (raw) and xNorm and
 * the cache reads only x.
 *
 * <p>逐轮计数（供 §4.6b 监测快照聚合）/ per-round counters (aggregated by the monitoring sink)：
 * dupKeys, unknownSensor, malformed, censoredLight, rssiSentinel。
 */
public class DeviceRound implements Serializable {
    private static final long serialVersionUID = 1L;

    private String device;
    private long ts;                    // 行内 epoch 秒 / in-row epoch second (round identity)

    private double[] x;                 // 原始 F_det / raw F_det, length 5
    private double[] xNorm;             // 归一化 F_det / normalized F_det, length 5
    private double mic;                 // DEV-D7b：MIC 作 Gas 质量元数据 / MIC as Gas-quality metadata
    private double rssi;                // DEV-D7c 后的 RSSI（可能为哨兵）/ RSSI (may be sentinel)
    private double accel;               // Accelerometer（侧信道，仅监测）/ side-channel

    private boolean[] missingMask;      // 该检测通道本轮缺失 / detection channel missing this round
    private boolean[] censoredMask;     // 该检测通道右删失（当前仅 Light）/ right-censored (Light only)
    private boolean[] bypassMask;       // RobustScaler 因 IQR≤ε 旁路该通道 / scaling bypassed (IQR<=eps)

    private boolean warmup;             // RobustScaler 预热期（统计未冻结）/ scaler still calibrating
    private boolean coldStart;          // 缺席超过缓存深度后返场 / returned after absence > cache depth
    private boolean incomplete;         // 5 检测通道未齐备 / not all 5 detection channels present

    // 逐轮计数 / per-round counters
    private int dupKeys;                // 同轮重复传感器（保留首值，DEV-D7a）/ duplicate sensors (kept first)
    private int unknownSensor;          // 未知传感器（DF-9）/ unknown sensors dropped
    private int malformed;              // 归属到本设备本轮的畸形读数 / malformed readings attributed here
    private int censoredLight;          // Light 右删失计数 / Light right-censored count
    private int rssiSentinel;           // RSSI 哨兵替换计数 / RSSI sentinel substitutions

    /** Jackson 反序列化需要无参构造 / No-arg constructor required by Jackson + Flink POJO. */
    public DeviceRound() {
        this.x = new double[Channels.N_DET];
        this.xNorm = new double[Channels.N_DET];
        this.missingMask = new boolean[Channels.N_DET];
        this.censoredMask = new boolean[Channels.N_DET];
        this.bypassMask = new boolean[Channels.N_DET];
    }

    @JsonProperty
    public String getDevice() { return device; }
    public void setDevice(String device) { this.device = device; }

    @JsonProperty
    public long getTs() { return ts; }
    public void setTs(long ts) { this.ts = ts; }

    @JsonProperty
    public double[] getX() { return x; }
    public void setX(double[] x) { this.x = x; }

    @JsonProperty
    public double[] getXNorm() { return xNorm; }
    public void setXNorm(double[] xNorm) { this.xNorm = xNorm; }

    @JsonProperty
    public double getMic() { return mic; }
    public void setMic(double mic) { this.mic = mic; }

    @JsonProperty
    public double getRssi() { return rssi; }
    public void setRssi(double rssi) { this.rssi = rssi; }

    @JsonProperty
    public double getAccel() { return accel; }
    public void setAccel(double accel) { this.accel = accel; }

    @JsonProperty
    public boolean[] getMissingMask() { return missingMask; }
    public void setMissingMask(boolean[] missingMask) { this.missingMask = missingMask; }

    @JsonProperty
    public boolean[] getCensoredMask() { return censoredMask; }
    public void setCensoredMask(boolean[] censoredMask) { this.censoredMask = censoredMask; }

    @JsonProperty
    public boolean[] getBypassMask() { return bypassMask; }
    public void setBypassMask(boolean[] bypassMask) { this.bypassMask = bypassMask; }

    @JsonProperty
    public boolean isWarmup() { return warmup; }
    public void setWarmup(boolean warmup) { this.warmup = warmup; }

    @JsonProperty
    public boolean isColdStart() { return coldStart; }
    public void setColdStart(boolean coldStart) { this.coldStart = coldStart; }

    @JsonProperty
    public boolean isIncomplete() { return incomplete; }
    public void setIncomplete(boolean incomplete) { this.incomplete = incomplete; }

    @JsonProperty
    public int getDupKeys() { return dupKeys; }
    public void setDupKeys(int dupKeys) { this.dupKeys = dupKeys; }

    @JsonProperty
    public int getUnknownSensor() { return unknownSensor; }
    public void setUnknownSensor(int unknownSensor) { this.unknownSensor = unknownSensor; }

    @JsonProperty
    public int getMalformed() { return malformed; }
    public void setMalformed(int malformed) { this.malformed = malformed; }

    @JsonProperty
    public int getCensoredLight() { return censoredLight; }
    public void setCensoredLight(int censoredLight) { this.censoredLight = censoredLight; }

    @JsonProperty
    public int getRssiSentinel() { return rssiSentinel; }
    public void setRssiSentinel(int rssiSentinel) { this.rssiSentinel = rssiSentinel; }

    /** 缺失检测通道个数 / number of missing detection channels. */
    public int missingCount() {
        int n = 0;
        for (boolean m : missingMask) {
            if (m) {
                n++;
            }
        }
        return n;
    }

    /** 被旁路的通道个数 / number of bypassed channels. */
    public int bypassCount() {
        int n = 0;
        for (boolean b : bypassMask) {
            if (b) {
                n++;
            }
        }
        return n;
    }

    @Override
    public String toString() {
        return String.format(
                "DeviceRound{dev=%s, ts=%d, x=%s, xNorm=%s, mic=%.1f, rssi=%.1f, accel=%.3f, "
                        + "missing=%s, censored=%s, bypass=%s, warmup=%b, cold=%b, incomplete=%b, "
                        + "dup=%d, unk=%d, malformed=%d, censLight=%d, rssiSent=%d}",
                device, ts, Arrays.toString(x), Arrays.toString(xNorm), mic, rssi, accel,
                Arrays.toString(missingMask), Arrays.toString(censoredMask), Arrays.toString(bypassMask),
                warmup, coldStart, incomplete, dupKeys, unknownSensor, malformed, censoredLight, rssiSentinel);
    }
}
