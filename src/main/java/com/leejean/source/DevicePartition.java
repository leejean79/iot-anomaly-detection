package com.leejean.source;

import org.apache.kafka.common.utils.Utils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * 显式分区映射：A→0, B→1, …, H→7（交接文档 §3.2，approved decision 1）。
 * Explicit partition mapping: A→0, B→1, …, H→7 (handover §3.2).
 *
 * <p>ENV 验收实测默认 hash(key)%8 会碰撞（A 与 F 同分区、partition 6 空置），故 M1 重放器改用
 * 显式映射实现「一设备一分区」与均衡负载。未知 DeviceId 落 hash%numPartitions 并计数。
 * The ENV acceptance proved default hashing collides (A and F shared a partition, partition 6 idle),
 * so the replayer uses an explicit mapping for one-device-one-partition. Unknown DeviceIds fall to
 * hash%numPartitions and are counted by the caller.
 */
public final class DevicePartition implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 已知设备集 A–H / the known device set A–H. */
    public static final String[] DEVICES = {"A", "B", "C", "D", "E", "F", "G", "H"};

    private DevicePartition() { }

    /** 是否已知设备 / whether the device is in the known A–H set. */
    public static boolean isKnown(String device) {
        return indexOf(device) >= 0;
    }

    private static int indexOf(String device) {
        if (device == null || device.length() != 1) {
            return -1;
        }
        char c = device.charAt(0);
        if (c >= 'A' && c <= 'H') {
            return c - 'A';
        }
        return -1;
    }

    /**
     * 返回该 device 应落的分区。已知设备用显式映射；未知设备用 Kafka 默认 murmur2 哈希取模
     * （与默认分区器同口径），返回值恒非负。
     * The partition for a device: explicit mapping for known devices; unknown devices use Kafka's
     * default murmur2 hash modulo numPartitions (same as the default partitioner), always non-negative.
     */
    public static int partitionFor(String device, int numPartitions) {
        int idx = indexOf(device);
        if (idx >= 0 && idx < numPartitions) {
            return idx;
        }
        byte[] keyBytes = (device == null ? "" : device).getBytes(StandardCharsets.UTF_8);
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }
}
