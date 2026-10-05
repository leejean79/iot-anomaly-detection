package com.leejean.m1;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 代理键的单元断言（2026-10-05 裁决第三节第 3 条）：并行度八时八台设备落到八个不同的子任务。
 * Unit assertions for surrogate keys (ruling of 2026-10-05, section 3.3).
 */
class DeviceKeysTest {

    private static final List<String> DEVICES = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H");

    /** 与 deploy/env.example 中 SYN_DEVICE_SURROGATE_KEYS 相同的表 / the same table as in env.example. */
    static final String CONFIGURED = "A=A#11,B=B#1,C=C#0,D=D#10,E=E#3,F=F#3,G=G#7,H=H#8";

    @Test
    void configuredTablePlacesEightDevicesOnEightSubtasks() {
        Map<String, String> table = DeviceKeys.parse(CONFIGURED);
        int maxP = DeviceKeys.effectiveMaxParallelism(-1, 8);
        assertEquals(128, maxP);
        Map<String, Integer> placement = DeviceKeys.placement(table, DEVICES, maxP, 8);
        assertEquals(8, new HashSet<>(placement.values()).size(), "八台设备应落到八个不同的子任务：" + placement);
    }

    @Test
    void rawIdsShareSubtasksWhichIsWhySurrogatesExist() {
        // 原始编号只落到五个子任务（B、C、E 同在 1；D、G 同在 7），与集群实测一致。
        Map<String, Integer> placement = DeviceKeys.placement(DeviceKeys.parse(""), DEVICES, 128, 8);
        assertEquals(5, new HashSet<>(placement.values()).size(), placement.toString());
        assertEquals(placement.get("B"), placement.get("E"));
        assertEquals(placement.get("D"), placement.get("G"));
    }

    @Test
    void generatedTableMatchesTheConfiguredOne() {
        assertEquals(CONFIGURED, DeviceKeys.format(DeviceKeys.generate(DEVICES, 128, 8)));
    }

    @Test
    void deviceIdIsRecoveredFromTheKey() {
        for (Map.Entry<String, String> e : DeviceKeys.parse(CONFIGURED).entrySet()) {
            assertEquals(e.getKey(), DeviceKeys.deviceOf(e.getValue()));
        }
        assertEquals("A", DeviceKeys.deviceOf("A"));
        assertEquals("A", DeviceKeys.keyOf(DeviceKeys.parse(""), "A"));
    }

    @Test
    void malformedTableIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> DeviceKeys.parse("A=B#1"));
        assertThrows(IllegalArgumentException.class, () -> DeviceKeys.parse("A"));
        assertTrue(DeviceKeys.parse(" ").isEmpty());
    }
}
