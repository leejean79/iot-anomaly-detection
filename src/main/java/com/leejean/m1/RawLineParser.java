package com.leejean.m1;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 原始行解析（交接文档 §4.2）：一行 CSV → Reading，带四类守卫（每类计数）。
 * Raw-line parser (handover §4.2): one CSV line → Reading, with four guards (each counted).
 *
 * <p>守卫 / guards:
 * <ul>
 *   <li>畸形行（列数不符 / 数值不可解析）→ 计数并跳过（DF）。
 *       Malformed line (bad column count / unparsable numbers) → count and skip.</li>
 *   <li>未知传感器（含 IR）→ 计数并丢弃（DF-9）。
 *       Unknown sensor type (including IR) → count and drop.</li>
 *   <li>Light == 65536 → 标记右删失（DF-11）。 Light == 65536 → mark right-censored.</li>
 *   <li>RSSI == 0 → 替换为缺失哨兵（DEV-D7c）。 RSSI == 0 → replace with a missing sentinel.</li>
 * </ul>
 *
 * <p>本算子**不 keyBy**（畸形行可能无法取得 device）。能取到 device 的行发往下游按 device
 * 分组；完全取不到 device 的畸形行只计入全局 Flink 指标（见 malformedNoDevice）。
 * This operator is NOT keyed (a malformed line may have no device). Lines with a recoverable
 * device are forwarded for per-device grouping; device-less malformed lines only bump a global
 * Flink metric.
 *
 * <p>决策 / decision（交接文档 §4.2）：解析器**不静默丢弃** unknown/censored/sentinel——它们
 * 打上 kind/flag 后照常下发，由 RoundAssembler 做逐设备计数与装配，从而进入监测快照。
 * The parser does not silently drop unknown/censored/sentinel readings; it tags them and forwards
 * so RoundAssembler can attribute per-device counts that reach the monitoring snapshot.
 */
public class RawLineParser extends ProcessFunction<String, Reading> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RawLineParser.class);

    // Flink 自定义指标（Prometheus 9249）/ Flink custom metrics (Prometheus, port 9249)
    private transient Counter linesIn;
    private transient Counter malformed;          // 有 device 的畸形（下发）+ 无 device 的畸形（此处）
    private transient Counter malformedNoDevice;  // 无法取得 device 的畸形，仅全局可见
    private transient Counter unknownSensor;
    private transient Counter censoredLight;
    private transient Counter rssiSentinel;

    @Override
    public void open(Configuration parameters) {
        // Prometheus 会转义/截断算子名，仪表盘阈值须凭经验校准（交接文档 §4.6c）。
        // Prometheus escapes/truncates operator names; calibrate dashboards empirically.
        linesIn = getRuntimeContext().getMetricGroup().counter("m1_parser_lines_in");
        malformed = getRuntimeContext().getMetricGroup().counter("m1_parser_malformed");
        malformedNoDevice = getRuntimeContext().getMetricGroup().counter("m1_parser_malformed_no_device");
        unknownSensor = getRuntimeContext().getMetricGroup().counter("m1_parser_unknown_sensor");
        censoredLight = getRuntimeContext().getMetricGroup().counter("m1_parser_censored_light");
        rssiSentinel = getRuntimeContext().getMetricGroup().counter("m1_parser_rssi_sentinel");
    }

    @Override
    public void processElement(String line, Context ctx, Collector<Reading> out) {
        linesIn.inc();
        if (line == null) {
            malformed.inc();
            malformedNoDevice.inc();
            return;
        }

        // 期望格式 / expected: Time,DeviceId,Sensor,Value ；跳过表头 / skip header row
        String[] f = line.split(",", -1);
        if (f.length >= 2 && "Time".equalsIgnoreCase(f[0].trim()) && "DeviceId".equalsIgnoreCase(f[1].trim())) {
            return;   // 表头行不计入畸形 / a header line is not counted as malformed
        }

        // 无法取得 device → 全局畸形计数 / cannot recover a device → global malformed only
        if (f.length < 2 || f[1].trim().isEmpty()) {
            malformed.inc();
            malformedNoDevice.inc();
            return;
        }
        String device = f[1].trim();

        // 列数不符或时间/数值不可解析 → 归属到该 device 的畸形（下发，供逐设备计数）
        // Wrong column count or unparsable time/value → malformed attributed to this device.
        if (f.length != 4) {
            malformed.inc();
            out.collect(new Reading(0L, device, null, Double.NaN, Channels.KIND_MALFORMED, -1, false, false));
            return;
        }
        long ts;
        double value;
        try {
            ts = Long.parseLong(f[0].trim());
            value = Double.parseDouble(f[3].trim());
        } catch (NumberFormatException e) {
            malformed.inc();
            out.collect(new Reading(0L, device, f[2].trim(), Double.NaN, Channels.KIND_MALFORMED, -1, false, false));
            return;
        }

        String sensor = f[2].trim();
        int kind = Channels.classify(sensor);

        if (kind == Channels.KIND_UNKNOWN) {
            unknownSensor.inc();   // DF-9：计数并丢弃（下发让 assembler 逐设备计数）/ count; forwarded for per-device tally
            out.collect(new Reading(ts, device, sensor, value, Channels.KIND_UNKNOWN, -1, false, false));
            return;
        }

        int channelIndex = Channels.detectionIndex(sensor);   // -1 for quality channels
        boolean censored = false;
        boolean rssiSent = false;

        if (channelIndex == Channels.LIGHT_INDEX && value == Channels.LIGHT_CENSOR_VALUE) {
            censored = true;   // DF-11：Light 右删失 / Light right-censored
            censoredLight.inc();
        }
        if (Channels.RSSI.equals(sensor) && value == Channels.RSSI_SENTINEL_VALUE) {
            rssiSent = true;   // DEV-D7c：RSSI==0 → 缺失哨兵 / missing sentinel
            rssiSentinel.inc();
        }

        out.collect(new Reading(ts, device, sensor, value, kind, channelIndex, censored, rssiSent));
    }
}
