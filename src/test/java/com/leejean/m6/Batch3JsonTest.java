package com.leejean.m6;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.m1.DeviceRound;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m7.FleetDecisionFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 第三批新增字段在开关关闭时不进入任何输出主题的 JSON（第一组短重放查明 m1-out 因转发命令多出字段而与标签 jar 不同）。
 * Batch-3 fields never reach the output JSON while their switches are off.
 */
class Batch3JsonTest {

    private static final ObjectMapper OM = new ObjectMapper();

    @Test
    @DisplayName("转发命令所附的轮：命令对象只序列化第二批的六个字段（不含 triggers、entryDays）")
    void forwardedCommandOnM1OutKeepsTheBatch2Fields() throws Exception {
        AdaptCommand c = AdaptCommand.parse("{\"device\":\"G\",\"level\":1,\"effectiveAt\":1648594800,\"decidedAt\":1648508400,"
                + "\"seq\":4,\"triggers\":\"Temperature|Gas\",\"entryDays\":[\"2022-03-27\"]}");
        assertEquals("Temperature|Gas", c.triggers, "解析照常 / still parsed for the scaler");
        DeviceRound r = new DeviceRound();
        r.setDevice("G");
        r.setContextCommand(c);
        java.util.List<String> keys = new java.util.ArrayList<>();
        OM.readTree(OM.writeValueAsString(r)).get("contextCommand").fieldNames().forEachRemaining(keys::add);
        assertEquals(java.util.Arrays.asList("device", "level", "effectiveAt", "decidedAt", "seq", "raw"), keys);
    }

    @Test
    @DisplayName("命令的 entryDays 与监测快照的 m3SwitchHeld 为空时不写进 JSON")
    void nullBatch3FieldsAreOmitted() throws Exception {
        FleetDecisionFunction.Command c = new FleetDecisionFunction.Command();
        c.device = "A";
        assertFalse(OM.writeValueAsString(c).contains("entryDays"));
        MonitoringSnapshot s = new MonitoringSnapshot();
        assertFalse(OM.writeValueAsString(s).contains("m3SwitchHeld"));
    }
}
