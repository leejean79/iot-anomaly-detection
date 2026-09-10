package com.leejean.source;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * 最小注入模式（交接文档 §4 决策 8）：对重放原始值施加四种注入，使注入流经 M1 归一化后自然出现于 M3 视角。
 * Minimal injection mode (handover §4 decision 8): applies four injection types to raw values
 * before sending, so injections flow through M1's normalization naturally and are visible to M3.
 *
 * <p>四种注入类型 / four injection types:
 * <ol>
 *   <li><b>spike</b> — 振幅尖峰（一轮或几轮）/ amplitude spike (one or a few rounds)</li>
 *   <li><b>step</b> — 持续偏移 / sustained offset</li>
 *   <li><b>ramp</b> — 缓慢增长（斜率线性，假数据注入攻击模拟）/ slow ramp (FDI attack analogue)</li>
 *   <li><b>stuck</b> — 传感器卡死（值冻结，数据质量场景 S6 模拟）/ stuck sensor (frozen value, S6 analogue)</li>
 * </ol>
 *
 * <p>注入规格格式（--inject 参数）/ injection spec format (--inject parameter):
 * {@code device:channel:startTs:durationSec:type:magnitude[;...]}
 * 多个注入用分号分隔 / multiple injections separated by semicolons.
 *
 * <p>========================= 脚本交付五要素 / Five delivery elements =========================
 * <ul>
 *   <li><b>执行环境</b>：同 CsvKafkaReplayer（集群 master 容器内 JDK 8）。</li>
 *   <li><b>调用命令</b>：{@code --inject "E:Temperature:1711929600:300:spike:10;E:Humidity:1711929600:600:step:5"}</li>
 *   <li><b>前置条件</b>：注入规格的 device/channel 必须在数据集中存在。</li>
 *   <li><b>期望产出</b>：注入后的消息流（原始值被改写）；地面真值日志到 --inject-log（默认 inject-truth.csv）。</li>
 *   <li><b>失败兜底</b>：注入规格解析失败 → 快速失败报错；注入期间无匹配行 → 告警不崩溃。</li>
 * </ul>
 */
public class Injector {

    /** 注入规格 / injection specification. */
    public static class Spec {
        public final String device;
        public final String channel;
        public final long startTs;
        public final long endTs;
        public final String type;
        public final double magnitude;

        public Spec(String device, String channel, long startTs, long durationSec,
                    String type, double magnitude) {
            this.device = device;
            this.channel = channel;
            this.startTs = startTs;
            this.endTs = startTs + durationSec;
            this.type = type.toLowerCase();
            this.magnitude = magnitude;
        }
    }

    private final List<Spec> specs;
    private final Path truthLog;
    private BufferedWriter truthWriter;
    private final double[] stuckValues;
    private final boolean[] stuckInitialized;
    private int applied;

    public Injector(List<Spec> specs, Path truthLog) throws IOException {
        this.specs = specs;
        this.truthLog = truthLog;
        this.stuckValues = new double[specs.size()];
        this.stuckInitialized = new boolean[specs.size()];
        if (truthLog != null) {
            truthWriter = Files.newBufferedWriter(truthLog, StandardCharsets.UTF_8);
            truthWriter.write("device,channel,start_ts,end_ts,type,magnitude");
            truthWriter.newLine();
            for (Spec s : specs) {
                truthWriter.write(String.format("%s,%s,%d,%d,%s,%.6f",
                        s.device, s.channel, s.startTs, s.endTs, s.type, s.magnitude));
                truthWriter.newLine();
            }
            truthWriter.flush();
        }
    }

    /**
     * 对一行原始 CSV 施加注入。如果该行的 device/channel/ts 匹配某个注入规格，修改其 value 字段并返回
     * 修改后的行；否则原样返回。
     * Apply injection to one raw CSV line. If the row's device/channel/ts matches a spec,
     * modify the value field and return the altered line; otherwise return the original.
     *
     * @param rawLine 原始行 "Time,DeviceId,SensorType,Value" / original line
     * @param ts      该行的时间戳秒 / row timestamp in seconds
     * @param device  该行的设备 ID / row device ID
     * @return 可能被修改的行 / possibly modified line
     */
    public String apply(String rawLine, long ts, String device) {
        String[] fields = rawLine.split(",", -1);
        if (fields.length < 4) return rawLine;

        String sensor = fields[2].trim();
        boolean modified = false;

        for (int i = 0; i < specs.size(); i++) {
            Spec s = specs.get(i);
            if (!s.device.equals(device) || !s.channel.equals(sensor)) continue;
            if (ts < s.startTs || ts >= s.endTs) continue;

            double original;
            try {
                original = Double.parseDouble(fields[3].trim());
            } catch (NumberFormatException e) {
                continue;
            }

            double injected;
            switch (s.type) {
                case "spike":
                    injected = original + s.magnitude;
                    break;
                case "step":
                    injected = original + s.magnitude;
                    break;
                case "ramp":
                    double progress = (double) (ts - s.startTs) / (s.endTs - s.startTs);
                    injected = original + s.magnitude * progress;
                    break;
                case "stuck":
                    if (!stuckInitialized[i]) {
                        stuckValues[i] = original;
                        stuckInitialized[i] = true;
                    }
                    injected = stuckValues[i];
                    break;
                default:
                    continue;
            }

            fields[3] = String.valueOf(injected);
            modified = true;
            applied++;
        }

        return modified ? String.join(",", fields) : rawLine;
    }

    /**
     * 解析注入规格字符串 "device:channel:startTs:durationSec:type:magnitude[;...]"。
     * Parse injection spec string "device:channel:startTs:durationSec:type:magnitude[;...]".
     */
    public static List<Spec> parse(String specStr) {
        List<Spec> specs = new ArrayList<>();
        if (specStr == null || specStr.trim().isEmpty()) return specs;

        for (String part : specStr.split(";")) {
            String s = part.trim();
            if (s.isEmpty()) continue;

            String[] f = s.split(":");
            if (f.length != 6) {
                throw new IllegalArgumentException(
                        "注入规格需要 6 个字段（device:channel:startTs:durationSec:type:magnitude），"
                        + "得到 " + f.length + " 个：'" + s + "' / injection spec needs 6 fields, got " + f.length);
            }
            String device = f[0].trim();
            String channel = f[1].trim();
            long startTs;
            long durationSec;
            double magnitude;
            try {
                startTs = Long.parseLong(f[2].trim());
                durationSec = Long.parseLong(f[3].trim());
                magnitude = Double.parseDouble(f[5].trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "注入规格数值字段解析失败：'" + s + "' / injection spec numeric field parse error", e);
            }
            String type = f[4].trim().toLowerCase();
            if (!type.equals("spike") && !type.equals("step")
                    && !type.equals("ramp") && !type.equals("stuck")) {
                throw new IllegalArgumentException(
                        "未知注入类型 '" + type + "'，有效值：spike/step/ramp/stuck / unknown injection type");
            }
            specs.add(new Spec(device, channel, startTs, durationSec, type, magnitude));
        }
        return specs;
    }

    public int getApplied() { return applied; }

    public void close() throws IOException {
        if (truthWriter != null) {
            truthWriter.close();
        }
    }
}
