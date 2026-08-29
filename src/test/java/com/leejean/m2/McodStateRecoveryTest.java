package com.leejean.m2;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * checkpoint 恢复测试（交接文档 §6.5）——R8 修复的直接验证。
 * Checkpoint-recovery test (handover §6.5): the direct verification of the R8 fix.
 *
 * <p>原文把微簇计数器 {@code mc_counter} 放在普通成员变量（Pmcod.scala:22），故障恢复后会归零、
 * 导致新微簇与恢复前的编号相撞。本项目把它并入 {@link McodState}（受 checkpoint 保护）。Flink 的
 * savepoint/恢复本质是对 keyed 状态做序列化再反序列化；本测试以对 {@link McodState} 做**序列化往返**
 * 作为等价代理，断言：恢复后 mcCounter 延续（不归零），据此新建的微簇编号不与既有编号相撞，且 PD/MC 内容保持。
 * The source kept the counter in a plain member var, which reset to zero on recovery and collided ids.
 * We fold it into {@link McodState}. A Flink savepoint is exactly a serialize/deserialize of keyed state,
 * so this test round-trips {@link McodState} through Java serialization and asserts the counter continues.
 */
class McodStateRecoveryTest {

    private static McodPoint p(double x, long arrival, long id) {
        return new McodPoint(new double[]{x}, arrival, 0, id);
    }

    private static McodCore.McodResult slide(McodCore core, List<McodPoint> pts, long start, long end) {
        List<McodPoint> window = new ArrayList<>();
        for (McodPoint p : pts) {
            if (p.arrival >= start && p.arrival < end) {
                window.add(p);
            }
        }
        return core.processSlide(window, start, end);
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T obj) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
        }
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            return (T) ois.readObject();
        }
    }

    @Test
    void mcCounterSurvivesCheckpointAndAvoidsIdCollision() throws Exception {
        McodState state = new McodState();
        McodCore core = new McodCore(1.0, 3, 10, state);

        // 先制造两个互相远离的稠密簇 → mcCounter 前进到 3（用过编号 1、2）。
        List<McodPoint> pts = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            pts.add(p(0.0, i, i));            // 簇 A 在 x=0
        }
        for (int i = 0; i < 5; i++) {
            pts.add(p(100.0, i, 1000 + i));   // 簇 B 在 x=100（远离，独立成簇）
        }
        slide(core, pts, -90, 10);   // 宽窗（W=100>S=10），无淘汰
        assertEquals(2, state.mc.size(), "应形成 2 个微簇");
        assertEquals(3, state.mcCounter, "用过编号 1、2 后 mcCounter=3");

        // —— 模拟 checkpoint/恢复：序列化往返 ——
        McodState restored = roundTrip(state);
        assertEquals(3, restored.mcCounter, "恢复后 mcCounter 必须延续为 3（R8：不归零）");
        assertEquals(2, restored.mc.size(), "恢复后微簇集保持");
        assertTrue(restored.mc.containsKey(1) && restored.mc.containsKey(2), "编号 1、2 应保留");

        // 恢复后继续：新建第三个簇（x=-100，远离），其编号必须是 3——不与既有 1、2 相撞。
        McodCore resumed = new McodCore(1.0, 3, 10, restored);
        List<McodPoint> more = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            more.add(p(-100.0, 20 + i, 2000 + i));
        }
        // 用一个足够大的窗口继续（沿用已恢复状态，windowEnd=30）
        slide(resumed, more, 30 - 1000, 30);
        assertTrue(restored.mc.containsKey(3), "恢复后新建微簇应获得延续编号 3");
        assertEquals(4, restored.mcCounter, "再建一簇后 mcCounter=4");
        // 编号 3 是全新的，未与恢复前的 1、2 冲突：
        assertFalse(restored.mc.get(3).points.isEmpty(), "新簇 3 应有成员");
        assertEquals(3, restored.mc.size(), "共 3 个互不撞号的微簇");
    }
}
