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
    /** 级别：零级由数据接入模块执行；一级与二级转发给上下文模块（M6b）/ level 0 executed here; 1 and 2 forwarded. */
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
    // 以下两项只供标准化算子内部使用（命令原文已在 raw 里）；不进 JSON，否则转发命令所附的轮在 m1-out 上会多出字段，第三批全部开关
    // 关闭时与标签 jar 不再逐位相同（第一组短重放查明）。/ Internal only, never serialized: m1-out must stay bit-identical.
    /** 触发通道（可无）/ trigger channels, if present. */
    @com.fasterxml.jackson.annotation.JsonIgnore
    public String triggers;
    /**
     * 数据层触发通道各自的进入偏离之日（第三批第一节，零级窗口锚定；聚合模块打开锚定时才有）/ entry days of the data triggers.
     */
    @com.fasterxml.jackson.annotation.JsonIgnore
    public java.util.List<String> entryDays;

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
        if (n.hasNonNull("triggers")) {
            c.triggers = n.get("triggers").asText();
        }
        if (n.hasNonNull("entryDays")) {
            c.entryDays = new java.util.ArrayList<>();
            for (JsonNode d : n.get("entryDays")) {
                c.entryDays.add(d.asText());
            }
        }
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
