package com.leejean.m2;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 逐设备半径 R 规格解析测试（收尾任务）——{@link M2Job#parseRPerDevice} 的直接验证。
 * Tests for the per-device radius spec parser: empty → global fallback, valid map, and fail-fast on
 * malformed input (a wrong radius must never be swallowed and run for a whole month).
 */
class M2JobConfigTest {

    @Test
    void emptyOrBlankSpecYieldsEmptyMap() {
        assertTrue(M2Job.parseRPerDevice("").isEmpty());
        assertTrue(M2Job.parseRPerDevice("   ").isEmpty());
        assertTrue(M2Job.parseRPerDevice(null).isEmpty());
    }

    @Test
    void parsesTheFinalizedFleetSpec() {
        // 设计会话裁决后写入默认配置的那张表（5 定稿 + 3 临时）
        Map<String, Double> m = M2Job.parseRPerDevice(
                "A=1.0,B=1.0,C=1.75,D=1.75,E=0.75,F=1.75,G=0.75,H=1.0");
        assertEquals(8, m.size());
        assertEquals(1.0, m.get("A"), 1e-9);
        assertEquals(1.0, m.get("B"), 1e-9);
        assertEquals(1.75, m.get("C"), 1e-9);
        assertEquals(1.75, m.get("D"), 1e-9);
        assertEquals(0.75, m.get("E"), 1e-9);
        assertEquals(1.75, m.get("F"), 1e-9);
        assertEquals(0.75, m.get("G"), 1e-9);
        assertEquals(1.0, m.get("H"), 1e-9);
    }

    @Test
    void toleratesWhitespaceAndTrailingComma() {
        Map<String, Double> m = M2Job.parseRPerDevice(" A = 1.0 , B=0.75 , ");
        assertEquals(2, m.size());
        assertEquals(1.0, m.get("A"), 1e-9);
        assertEquals(0.75, m.get("B"), 1e-9);
    }

    @Test
    void unlistedDeviceFallsBackToGlobalViaGetOrDefault() {
        // parse 本身不做回退；回退语义由 PmcodFunction 的 getOrDefault(device, globalR) 承担，此处核验其前提：
        // 未列设备不在 map 中，getOrDefault 便返回全局 R。
        Map<String, Double> m = M2Job.parseRPerDevice("A=1.0");
        assertEquals(1.0, m.getOrDefault("A", 9.9), 1e-9);
        assertEquals(9.9, m.getOrDefault("Z", 9.9), 1e-9);   // 未列 → 回退
    }

    @Test
    void failsFastOnMalformedEntries() {
        assertThrows(IllegalArgumentException.class, () -> M2Job.parseRPerDevice("A"));        // 无 =
        assertThrows(IllegalArgumentException.class, () -> M2Job.parseRPerDevice("=1.0"));     // 无设备名
        assertThrows(IllegalArgumentException.class, () -> M2Job.parseRPerDevice("A="));       // 无半径
        assertThrows(IllegalArgumentException.class, () -> M2Job.parseRPerDevice("A=abc"));    // 非数字
        assertThrows(IllegalArgumentException.class, () -> M2Job.parseRPerDevice("A=0"));      // 非正
        assertThrows(IllegalArgumentException.class, () -> M2Job.parseRPerDevice("A=-1.0"));   // 负
    }
}
