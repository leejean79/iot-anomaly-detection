package com.leejean.m5a;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 当日缓冲的序列化只写已用部分（M5a 阶段报告第九节第 1 条，设计会话 2026-10-26 选乙）：经算子状态所用的 Flink 序列化器（Kryo）与 Java 序列化往返后内容不变，
 * 体积按已用读数计，而不是按扩容后的容量计。
 * The day buffer serializes only its used part, through the operator's Flink (Kryo) serializer and Java serialization.
 */
class DayBufferTest {

    private static DayBuffer filled(int perChannel) {
        DayBuffer b = new DayBuffer(5);
        for (int c = 0; c < 5; c++) {
            for (int i = 0; i < perChannel + c; i++) {
                b.append(c, c * 1000 + i * 0.1);
            }
        }
        return b;
    }

    private static void assertSame(DayBuffer a, DayBuffer b) {
        for (int c = 0; c < 5; c++) {
            assertEquals(a.size(c), b.size(c));
            assertArrayEquals(a.values(c), b.values(c), 0.0);
        }
    }

    @Test
    @DisplayName("算子状态的 Kryo 序列化：内容不变，体积按已用读数计 / Kryo: same content, size by used readings")
    void kryoWritesUsedPartOnly() throws Exception {
        TypeSerializer<M5aCore> ser = TypeInformation.of(M5aCore.class).createSerializer(new ExecutionConfig());
        assertTrue(ser instanceof KryoSerializer, "M5aCore 的状态由 Kryo 序列化 / M5aCore state goes through Kryo");
        TypeSerializer<DayBuffer> bs = TypeInformation.of(DayBuffer.class).createSerializer(new ExecutionConfig());
        DayBuffer b = filled(8600);                           // 扩容到 16,384 / capacity grows to 16,384
        assertEquals(16384, b.capacity(0));
        DataOutputSerializer out = new DataOutputSerializer(1 << 16);
        bs.serialize(b, out);
        long used = 8L * (5 * 8600 + 0 + 1 + 2 + 3 + 4);
        assertTrue(out.length() < used + 200, "serialized " + out.length() + " vs used " + used);
        DayBuffer back = bs.deserialize(new DataInputDeserializer(out.getSharedBuffer(), 0, out.length()));
        assertSame(b, back);
        assertEquals(8601, back.capacity(1));                 // 恢复后数组长度等于已用长度 / restored at the used length
        back.append(1, 1.5);                                  // 之后照常扩容 / grows as usual afterwards
        assertEquals(8602, back.size(1));
        assertSame(b, bs.copy(b));

        // 作为 M5aCore 的字段时同样只写已用部分 / the same holds when the buffer is a field of M5aCore
        M5aCore core = new M5aCore();
        java.lang.reflect.Field f = M5aCore.class.getDeclaredField("buf");
        f.setAccessible(true);
        f.set(core, b);
        DataOutputSerializer co = new DataOutputSerializer(1 << 16);
        ser.serialize(core, co);
        assertTrue(co.length() < used + 2000, "core serialized " + co.length() + " vs used " + used);
        M5aCore coreBack = ser.deserialize(new DataInputDeserializer(co.getSharedBuffer(), 0, co.length()));
        assertSame(b, (DayBuffer) f.get(coreBack));
    }

    @Test
    @DisplayName("Java 序列化：内容不变，体积按已用读数计 / Java serialization: same content, size by used readings")
    void javaWritesUsedPartOnly() throws Exception {
        DayBuffer b = filled(5000);                           // 容量 8,192 / capacity 8,192
        byte[] bytes = InstantiationUtil.serializeObject(b);
        // 已用 25,010 个读数约 200 KB，另有约 1 KB 的类描述；按容量写会是 5 × 8,192 × 8 ≈ 328 KB / used ≈ 200 KB, capacity ≈ 328 KB
        assertTrue(bytes.length < 8L * 25010 + 2000, "serialized " + bytes.length);
        DayBuffer back = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        assertSame(b, back);
        DayBuffer empty = InstantiationUtil.deserializeObject(InstantiationUtil.serializeObject(new DayBuffer(5)),
                getClass().getClassLoader());
        empty.append(3, 2.0);
        assertEquals(1, empty.size(3));
    }
}
