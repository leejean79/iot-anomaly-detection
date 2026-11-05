package com.leejean.m5a;

import com.leejean.m1.ChannelTransform;
import com.leejean.m1.Channels;
import com.leejean.m1.DeviceRound;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m5.DriftEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 数据层收到零级之后的新参照（M6a 交接文档第四节、开工核对裁决第六项）：替换参照、清空状态转移与当日缓冲、不发回落事件；生效时刻不在
 * 日界时生效当日记为「重估日」，不可判；日记录与事件带版本号；命令回路关闭（版本为空）时不写版本。携带新参照的那一条轮有缺失通道，
 * 检验数据层读取附件先于任何缺失过滤（1.29 版裁决第三项的核对：数据层不丢弃有缺失通道的轮，直方图不会随之丢失）。
 * The data layer on a level-0 reference: replace, reset without RECOVER, the re-estimation day, versions.
 */
class M5aReferenceReplaceTest {

    private static final long DAY0 = 1_646_092_800L;     // 2022-03-01 00:00 UTC（冬令时，当地日界）/ a local day boundary
    private static final int PERIOD = 600;               // 测试用十分钟一轮，每日 144 轮 / 144 rounds a day
    private static final M5aCore.Params P = new M5aCore.Params(new double[]{0.38, 0.73, 0.84, 0.43, 0.51}, 0.01, 0.2, 0.2,
            PERIOD, 60);

    private static double[] reading(Random rng, double shift) {
        return new double[]{21 + shift + rng.nextGaussian(), 40 + shift + rng.nextGaussian(), 1010 + shift + rng.nextGaussian(),
                700 + 30 * (shift + rng.nextGaussian()), Math.expm1(4 + shift + rng.nextGaussian())};
    }

    private static RefHistogram[] refs(double shift, long seed) {
        Random rng = new Random(seed);
        ChannelTransform[] tf = ChannelTransform.defaultTable();
        double[][] v = new double[Channels.N_DET][2000];
        for (int i = 0; i < 2000; i++) {
            double[] x = reading(rng, shift);
            for (int c = 0; c < Channels.N_DET; c++) {
                v[c][i] = tf[c].apply(x[c]);
            }
        }
        RefHistogram[] h = new RefHistogram[Channels.N_DET];
        for (int c = 0; c < Channels.N_DET; c++) {
            Arrays.sort(v[c]);
            h[c] = RefHistogram.build(v[c], 400);
        }
        return h;
    }

    /** 六天：第一天标定，第二、三天偏移，第四天中午换参照（版本二），第五、六天仍偏移 / six days with a mid-day replacement. */
    private static void run(boolean loopOn, long switchAt, List<DriftEvent> events, List<MonitoringSnapshot> records) {
        Random rng = new Random(5);
        M5aCore core = new M5aCore();
        for (long t = DAY0; t < DAY0 + 6 * 86400L; t += PERIOD) {
            DeviceRound r = new DeviceRound();
            r.setDevice("B");
            r.setTs(t);
            r.setX(reading(rng, t >= DAY0 + 86400 ? 3.0 : 0.0));
            if (t == DAY0) {
                r.setRefHistograms(refs(0.0, 1));
            }
            if (loopOn) {
                r.setVersion(t < switchAt ? 1 : 2);
                if (t == switchAt) {
                    r.setRefHistograms(refs(3.0, 2));
                    r.getMissingMask()[3] = true;     // 携带新参照的轮缺气体（裁决第三项）/ the carrying round lacks Gas
                }
            }
            core.onRound(r, P, events, records);
        }
        core.closeDay("B", P, events, records);
    }

    @Test
    @DisplayName("日中换参照（携带参照的轮有缺失通道）：重估日不可判，状态转移清零且不发回落，记录与事件带版本 / mid-day replacement")
    void midDayReplacement() {
        long switchAt = DAY0 + 3 * 86400L + 12 * 3600L;
        List<DriftEvent> ev = new ArrayList<>();
        List<MonitoringSnapshot> rec = new ArrayList<>();
        run(true, switchAt, ev, rec);
        int deviate = 0;
        for (DriftEvent e : ev) {
            if (e.level == DriftEvent.Level.RECOVER) {
                throw new AssertionError("换参照不得发出回落事件 / no RECOVER on replacement: " + e.channel + " " + e.dataDay);
            }
            if (e.level == DriftEvent.Level.DEVIATE) {
                deviate++;
                assertEquals("2022-03-02", e.dataDay);
                assertEquals(Integer.valueOf(1), e.version);
            }
        }
        assertTrue(deviate >= 3, "偏移之后应进入偏离 / shifted days deviate: " + deviate);
        for (MonitoringSnapshot s : rec) {
            String day = s.getM5aDay();
            assertEquals(day.compareTo("2022-03-04") < 0 ? Integer.valueOf(1) : Integer.valueOf(2), s.getVersion(), day);
            if (day.equals("2022-03-04")) {
                assertEquals("重估日", s.getM5aReason());
                assertFalse(s.isM5aEvaluable());
                assertFalse(s.isM5aDeviated(), "换参照后状态转移清零 / state reset");
            }
            if (day.compareTo("2022-03-04") > 0) {
                assertTrue(s.isM5aEvaluable(), day);
                assertFalse(s.isM5aDeviated(), "新参照上不再偏离 / no deviation against the new reference: " + day);
            }
        }
    }

    @Test
    @DisplayName("日界换参照：生效当日可判；回路关闭时不写版本 / boundary replacement and loop off")
    void boundaryReplacementAndLoopOff() {
        List<DriftEvent> ev = new ArrayList<>();
        List<MonitoringSnapshot> rec = new ArrayList<>();
        run(true, DAY0 + 3 * 86400L, ev, rec);
        for (MonitoringSnapshot s : rec) {
            if (s.getM5aDay().equals("2022-03-04")) {
                assertTrue(s.isM5aEvaluable());
                assertEquals("", s.getM5aReason());
            }
        }
        ev.clear();
        rec.clear();
        run(false, Long.MAX_VALUE, ev, rec);
        for (MonitoringSnapshot s : rec) {
            assertNull(s.getVersion());
        }
        for (DriftEvent e : ev) {
            assertNull(e.version);
        }
    }
}
