package com.leejean.m1;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 通道级预变换单元测试（补充指令二第一步要求的三情形：零值、常态值、删失顶格值）。
 * Unit tests for the channel pre-transform, covering the three cases the instruction calls out:
 * zero, a normal value, and the censored top value.
 */
class ChannelTransformTest {

    @Test
    void log1pZeroMapsToZero() {
        // 加一使零照度映射到 0（而非 log 的 −∞）
        assertEquals(0.0, ChannelTransform.LOG1P.apply(0.0), 1e-12);
    }

    @Test
    void log1pNormalValue() {
        // 常态值：log1p(v) = ln(1+v)
        assertEquals(Math.log(1.0 + 600.0), ChannelTransform.LOG1P.apply(600.0), 1e-12);
        assertEquals(2.0, ChannelTransform.LOG1P.apply(Math.exp(2.0) - 1.0), 1e-12);
    }

    @Test
    void log1pCensoredTopValueIsModerate() {
        // 删失顶格值 65536 → ln(65537) ≈ 11.0904，不再是把距离/损失拽飞的极端数
        double t = ChannelTransform.LOG1P.apply(Channels.LIGHT_CENSOR_VALUE);
        assertEquals(Math.log(65537.0), t, 1e-9);
        assertTrue(t > 11.0 && t < 11.2, "log1p(65536) ≈ 11.09");
        // 与原始顶格值相比，量级被极大压缩（原始 65536 vs 变换后 ~11）
        assertTrue(Channels.LIGHT_CENSOR_VALUE / t > 5000.0);
    }

    @Test
    void identityIsIdentity() {
        for (double v : new double[]{0.0, 1.0, 600.0, 65536.0, -3.5}) {
            assertEquals(v, ChannelTransform.IDENTITY.apply(v), 0.0);
        }
    }

    @Test
    void defaultTableLog1pOnlyOnLight() {
        ChannelTransform[] t = ChannelTransform.defaultTable();
        assertEquals(Channels.N_DET, t.length);
        for (int c = 0; c < t.length; c++) {
            if (c == Channels.LIGHT_INDEX) {
                assertEquals(ChannelTransform.LOG1P, t[c], "Light 取 log1p");
            } else {
                assertEquals(ChannelTransform.IDENTITY, t[c], "非 Light 恒等");
            }
        }
        // 变换确实改变 Light 域、不改其余（举证：同一顶格值在 Light 上被压缩，在别的通道上原样）
        assertNotEquals(65536.0, t[Channels.LIGHT_INDEX].apply(65536.0));
        assertEquals(65536.0, t[0].apply(65536.0), 0.0);
    }
}
