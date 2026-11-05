package com.leejean.m6;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;

/**
 * 命令主题 {@code synergia-adapt} 上的一条适应命令（M6a 交接文档第三节命令契约）。字段与聚合模块发出的 JSON 相同：设备号、级别、
 * 生效事件时间、决定时刻、命令序号；其余字段（决定依据）不解释，原样保存在 {@link #raw} 里写进执行日志。
 * One adaptation command on synergia-adapt (M6a handover section 3); the decision basis is kept verbatim in {@link #raw}.
 */
public class AdaptCommand implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String device;
    /** 级别：本阶段只执行零级 / level: only level 0 is executed in batch 1. */
    public int level;
    /**
     * 生效事件时间（秒），须为点通道滑动步（60 秒）的整数倍（1.29 版裁决第五项写入命令契约）；聚合模块的日界满足，手工工具拒绝不满足的值。
     * Effective event time (s); must be a multiple of the 60 s slide (command contract, ruling 1.29 item 5).
     */
    public long effectiveAt;
    /** 决定时刻（秒）/ decision event time (s). */
    public long decidedAt;
    /** 命令序号，按设备从一起计；执行后参数版本号为序号加一 / per-device sequence from 1; new version = seq + 1. */
    public int seq;
    /** 原始 JSON（决定依据原样保存）/ the raw JSON line. */
    public String raw;

    public AdaptCommand() {
    }

    /** 解析一行 JSON；缺少必需字段时抛出异常 / parse one JSON line; throws when a required field is missing. */
    public static AdaptCommand parse(String json) throws IOException {
        JsonNode n = MAPPER.readTree(json);
        AdaptCommand c = new AdaptCommand();
        c.device = required(n, "device").asText();
        c.level = required(n, "level").asInt();
        c.effectiveAt = required(n, "effectiveAt").asLong();
        c.decidedAt = required(n, "decidedAt").asLong();
        c.seq = required(n, "seq").asInt();
        c.raw = json;
        return c;
    }

    private static JsonNode required(JsonNode n, String field) throws IOException {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) {
            throw new IOException("命令缺少字段 / command lacks field " + field + ": " + n);
        }
        return v;
    }

    public String getDevice() {
        return device;
    }
}
