package com.leejean.source;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DevicePartition 显式映射单测（交接文档 §3.2，approved decision 1）。
 * Unit tests for the explicit device→partition mapping.
 */
class DevicePartitionTest {

    @Test
    void knownDevicesMapAToZeroThroughHToSeven() {
        String[] devs = {"A", "B", "C", "D", "E", "F", "G", "H"};
        for (int i = 0; i < devs.length; i++) {
            assertTrue(DevicePartition.isKnown(devs[i]));
            assertEquals(i, DevicePartition.partitionFor(devs[i], 8),
                    "device " + devs[i] + " must map to partition " + i);
        }
    }

    @Test
    void knownDevicesOccupyDistinctPartitions() {
        // ENV 验收暴露的默认哈希碰撞（A 与 F 同分区）在显式映射下必须消失。
        // The default-hash collision (A and F) from ENV acceptance must be gone under explicit mapping.
        Set<Integer> seen = new HashSet<>();
        for (String d : DevicePartition.DEVICES) {
            assertTrue(seen.add(DevicePartition.partitionFor(d, 8)),
                    "device " + d + " collided with another device");
        }
        assertEquals(8, seen.size());
    }

    @Test
    void unknownDeviceFallsToHashAndIsFlagged() {
        assertFalse(DevicePartition.isKnown("Z"));
        assertFalse(DevicePartition.isKnown(null));
        assertFalse(DevicePartition.isKnown("AA"));
        int p = DevicePartition.partitionFor("Z", 8);
        assertTrue(p >= 0 && p < 8, "hash fallback must be a valid partition, got " + p);
        // 确定性 / deterministic
        assertEquals(p, DevicePartition.partitionFor("Z", 8));
    }
}
