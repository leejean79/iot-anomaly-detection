package com.leejean.m1;

import com.leejean.m6.CompactCache;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 零级窗口锚定的单元测试（第三批交接文档第一节，验收 V-M6c-1 的单元测试部分）：起点取较晚者、回退、两档作用范围，以及带逐通道
 * 起点的零级只用各自窗口里的轮。多触发取最早的进入之日在 {@code com.leejean.m7.FleetBatch3Test} 中核对。
 * Unit tests of the anchored level-0 window: later start wins, fallback, the two scopes, and per-channel windows in level 0.
 */
class AnchoredWindowTest {

    private static final long DAY = 86400L;
    private static final long EFF = 1_700_000_000L;
    private static final long D7 = 7 * DAY;
    private static final long MIN = 2 * DAY;
    private static final boolean[] TEMP = {true, false, false, false, false};

    @Test
    @DisplayName("起点取「进入之日零点」与「生效前七天」中较晚者 / the later of the entry midnight and seven days before")
    void laterStartWins() {
        long[] a = AdaptiveScalerFunction.anchoredStarts(EFF, D7, EFF - 3 * DAY, TEMP, false, MIN);
        assertEquals(EFF - 3 * DAY, a[0]);
        long[] b = AdaptiveScalerFunction.anchoredStarts(EFF, D7, EFF - 20 * DAY, TEMP, false, MIN);
        assertEquals(EFF - D7, b[0], "进入之日早于七天时窗口不长于七天 / never longer than seven days");
    }

    @Test
    @DisplayName("窗口不足下限或没有进入之日时回退 / fall back when shorter than the minimum or without an entry day")
    void fallback() {
        assertNull(AdaptiveScalerFunction.anchoredStarts(EFF, D7, EFF - DAY, TEMP, false, MIN));
        assertNull(AdaptiveScalerFunction.anchoredStarts(EFF, D7, null, TEMP, false, MIN));
        long[] exact = AdaptiveScalerFunction.anchoredStarts(EFF, D7, EFF - MIN, TEMP, false, MIN);
        assertEquals(EFF - MIN, exact[0], "恰为下限时不回退 / exactly the minimum is kept");
    }

    @Test
    @DisplayName("乙档只锚定触发通道，丙档锚定全部通道 / scope: trigger channels only versus all channels")
    void scopes() {
        long[] trig = AdaptiveScalerFunction.anchoredStarts(EFF, D7, EFF - 3 * DAY, TEMP, false, MIN);
        long[] all = AdaptiveScalerFunction.anchoredStarts(EFF, D7, EFF - 3 * DAY, TEMP, true, MIN);
        for (int c = 0; c < Channels.N_DET; c++) {
            assertEquals(c == 0 ? EFF - 3 * DAY : EFF - D7, trig[c], "trigger " + c);
            assertEquals(EFF - 3 * DAY, all[c], "all " + c);
        }
    }

    @Test
    @DisplayName("逐通道起点：锚定通道的参数等于只用锚定窗口重估的结果，其余通道等于七天重估；起点为空时与原函数逐位相同")
    void perChannelWindows() {
        CompactCache c = new CompactCache(D7);
        // 前四天温度在 10 附近、后三天在 20 附近；其余通道两段不同 / the two segments differ on every channel
        for (long t = EFF - D7; t < EFF; t += 600) {
            boolean late = t >= EFF - 3 * DAY;
            double w = (t / 600) % 7;
            double[] x = {(late ? 20 : 10) + w * 0.1, 50 + (late ? 5 : 0) + w, 100000 + w * 10, (late ? 300 : 200) + w, 100 + w};
            c.add(t, x, 0);
        }
        ChannelTransform[] tf = ChannelTransform.defaultTable();
        AdaptiveScalerFunction.Level0 full = AdaptiveScalerFunction.level0(c, EFF, D7, 3600, 1e-9, false, tf, null, null);
        AdaptiveScalerFunction.Level0 same = AdaptiveScalerFunction.level0(c, EFF, D7, 3600, 1e-9, false, tf, null, null, null);
        assertArrayEquals(full.cal.median, same.cal.median, 0.0);
        assertArrayEquals(full.cal.scale, same.cal.scale, 0.0);
        AdaptiveScalerFunction.Level0 short3 = AdaptiveScalerFunction.level0(c, EFF, 3 * DAY, 3600, 1e-9, false, tf, null, null);
        long[] st = AdaptiveScalerFunction.anchoredStarts(EFF, D7, EFF - 3 * DAY, TEMP, false, MIN);
        AdaptiveScalerFunction.Level0 anch = AdaptiveScalerFunction.level0(c, EFF, D7, 3600, 1e-9, false, tf, null, null, st);
        assertEquals(short3.cal.median[0], anch.cal.median[0], 0.0, "温度用三天窗口 / temperature uses the anchored window");
        assertEquals(short3.cal.scale[0], anch.cal.scale[0], 0.0);
        assertArrayEquals(short3.cal.hist[0].getEdges(), anch.cal.hist[0].getEdges(), 0.0, "参照随同一窗口 / same window");
        for (int ch = 1; ch < Channels.N_DET; ch++) {
            assertEquals(full.cal.median[ch], anch.cal.median[ch], 0.0, "其余通道用七天 / others use seven days " + ch);
        }
        assertNotEquals(full.cal.median[0], anch.cal.median[0], "用例对窗口敏感 / the case is sensitive to the window");
        assertEquals(full.windowRounds, anch.windowRounds, "窗口轮数按最早的起点计 / counted from the earliest start");
        assertArrayEquals(full.backfill.ts, anch.backfill.ts, "回填仍为生效前一小时 / backfill unchanged");
    }
}
