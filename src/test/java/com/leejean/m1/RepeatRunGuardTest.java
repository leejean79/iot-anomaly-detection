package com.leejean.m1;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重复值游程守卫（开发决策第十条）的测试：阈值表解析、游程计数规则、在原始缓存算子中的打标与计数、监测快照计数。
 * Tests for the repeated-value run-length guard (development decision 10).
 */
class RepeatRunGuardTest {

    private static DeviceRound round(String dev, long ts, double... x) {
        DeviceRound r = new DeviceRound();
        r.setDevice(dev);
        r.setTs(ts);
        r.setX(x.clone());
        return r;
    }

    @Test
    @DisplayName("阈值表：五个通道按固定顺序解析；空串关闭守卫；格式错误立即报错")
    void parseThresholds() {
        Map<String, int[]> m = RepeatRunGuard.parse("A=120/90/0/300/0, B=1/2/3/4/5");
        assertArrayEquals(new int[]{120, 90, 0, 300, 0}, m.get("A"));
        assertArrayEquals(new int[]{1, 2, 3, 4, 5}, m.get("B"));
        assertTrue(RepeatRunGuard.parse("").isEmpty());
        assertTrue(RepeatRunGuard.parse(null).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> RepeatRunGuard.parse("A=1/2/3/4"));
        assertThrows(IllegalArgumentException.class, () -> RepeatRunGuard.parse("A=1/2/3/4/-1"));
        assertThrows(IllegalArgumentException.class, () -> RepeatRunGuard.parse("A=1/2/3/4/5,A=1/2/3/4/5"));
        assertThrows(IllegalArgumentException.class, () -> RepeatRunGuard.parse("=1/2/3/4/5"));
        assertThrows(NumberFormatException.class, () -> RepeatRunGuard.parse("A=1/2/x/4/5"));
    }

    @Test
    @DisplayName("游程：连续相同读数累加，达到阈值起打标，只在恰好达到时计一次新游程")
    void runReachesThreshold() {
        RepeatRunGuard.RunState s = new RepeatRunGuard.RunState();
        int[] thr = {3, 0, 0, 0, 0};
        int crossed = 0;
        boolean[] flagged = new boolean[6];
        double[] temps = {20.0, 20.0, 20.0, 20.0, 20.0, 20.1};
        for (int i = 0; i < temps.length; i++) {
            DeviceRound r = round("A", i * 10L, temps[i], i, i, i, i);
            crossed += RepeatRunGuard.update(s, r, thr);
            flagged[i] = r.getStuckMask()[0];
        }
        assertArrayEquals(new boolean[]{false, false, true, true, true, false}, flagged);
        assertEquals(1, crossed, "一段卡死只计一次新游程");
        assertEquals(1, s.run[0], "读数变化后游程从 1 重新开始");
    }

    @Test
    @DisplayName("游程：缺失通道既不延长也不打断；阈值为 0 的通道不打标")
    void missingChannelIsSkipped() {
        RepeatRunGuard.RunState s = new RepeatRunGuard.RunState();
        int[] thr = {3, 0, 0, 0, 0};
        DeviceRound r1 = round("A", 0, 5.0, 1, 1, 1, 1);
        DeviceRound r2 = round("A", 10, 0.0, 1, 1, 1, 1);
        r2.getMissingMask()[0] = true;           // 温度缺失，值无意义 / temperature missing
        DeviceRound r3 = round("A", 20, 5.0, 1, 1, 1, 1);
        DeviceRound r4 = round("A", 30, 5.0, 1, 1, 1, 1);
        RepeatRunGuard.update(s, r1, thr);
        RepeatRunGuard.update(s, r2, thr);
        assertEquals(1, s.run[0], "缺失轮不延长游程");
        RepeatRunGuard.update(s, r3, thr);
        assertEquals(2, s.run[0], "缺失轮也不打断游程");
        RepeatRunGuard.update(s, r4, thr);
        assertTrue(r4.getStuckMask()[0]);
        assertFalse(r4.getStuckMask()[1], "阈值为 0 的通道不设守卫，即使湿度连续 4 轮相同");
    }

    @Test
    @DisplayName("游程：相邻两条读数相隔超过 60 秒即断开")
    void gapBreaksRun() {
        RepeatRunGuard.RunState s = new RepeatRunGuard.RunState();
        int[] thr = {3, 0, 0, 0, 0};
        RepeatRunGuard.update(s, round("A", 0, 5.0, 0, 0, 0, 0), thr);
        RepeatRunGuard.update(s, round("A", 10, 5.0, 0, 0, 0, 0), thr);
        assertEquals(2, s.run[0]);
        RepeatRunGuard.update(s, round("A", 71, 5.0, 0, 0, 0, 0), thr);
        assertEquals(1, s.run[0], "间隔 61 秒，跨缺口的相同读数不连成一个游程");
        RepeatRunGuard.update(s, round("A", 131, 5.0, 0, 0, 0, 0), thr);
        assertEquals(2, s.run[0], "间隔恰为 60 秒仍算连续");
    }

    @Test
    @DisplayName("游程：冷启动清零全部游程")
    void coldStartResets() {
        RepeatRunGuard.RunState s = new RepeatRunGuard.RunState();
        int[] thr = {3, 0, 0, 0, 0};
        RepeatRunGuard.update(s, round("A", 0, 5.0, 0, 0, 0, 0), thr);
        RepeatRunGuard.update(s, round("A", 10, 5.0, 0, 0, 0, 0), thr);
        DeviceRound back = round("A", 99_999, 5.0, 0, 0, 0, 0);
        back.setColdStart(true);
        RepeatRunGuard.update(s, back, thr);
        assertEquals(1, s.run[0]);
        assertFalse(back.getStuckMask()[0]);
    }

    @Test
    @DisplayName("底值集合：解析按检测通道顺序；通道名错误立即报错")
    void parseFloors() {
        Map<String, double[][]> m = RepeatRunGuard.parseFloors("A.Light=0/1, A.Gas=400, B.Light=5");
        assertArrayEquals(new double[]{0, 1}, m.get("A")[Channels.LIGHT_INDEX]);
        assertArrayEquals(new double[]{400}, m.get("A")[3]);
        assertArrayEquals(new double[]{5}, m.get("B")[Channels.LIGHT_INDEX]);
        assertEquals(0, m.get("B")[0].length);
        assertTrue(RepeatRunGuard.parseFloors("").isEmpty());
        assertThrows(IllegalArgumentException.class, () -> RepeatRunGuard.parseFloors("A.Lux=0"));
        assertThrows(IllegalArgumentException.class, () -> RepeatRunGuard.parseFloors("Light=0"));
        assertThrows(IllegalArgumentException.class, () -> RepeatRunGuard.parseFloors("A.Light="));
    }

    @Test
    @DisplayName("底值游程：达到阈值只写 floorMask 并计数，不写 stuckMask；两类都给出游程起始与长度")
    void floorRunIsCountedNotFlagged() {
        RepeatRunGuard.RunState s = new RepeatRunGuard.RunState();
        int[] thr = {3, 0, 0, 0, 3};
        double[][] floors = RepeatRunGuard.parseFloors("A.Light=0").get("A");
        int[] crossed = new int[2];
        DeviceRound last = null;
        for (int i = 0; i < 4; i++) {
            // 温度卡在 20.0（普通游程），光照停在底值 0（底值游程）/ temperature stuck, light on its floor
            last = round("A", 100 + i * 10L, 20.0, i, i, i, 0.0);
            int[] c = RepeatRunGuard.update(s, last, thr, floors);
            crossed[0] += c[0];
            crossed[1] += c[1];
        }
        assertTrue(last.getStuckMask()[0], "温度：普通游程达到阈值，疑似卡死");
        assertFalse(last.getStuckMask()[4], "光照：底值游程不写 stuckMask");
        assertTrue(last.getFloorMask()[4], "光照：底值游程写 floorMask");
        assertArrayEquals(new int[]{1, 1}, crossed, "各计一次新游程");
        assertEquals(4, last.getGuardRunLength()[0]);
        assertEquals(100, last.getGuardRunStartTs()[0], "游程起始为第一轮的时间戳");
        assertEquals(4, last.getGuardRunLength()[4]);
        assertEquals(0, last.getGuardRunLength()[1], "未达阈值的通道为 0");
    }

    @Test
    @DisplayName("原始缓存算子：按设备查阈值、打标并输出；没有阈值的设备不打标")
    void rawCacheFlagsRounds() throws Exception {
        Map<String, int[]> thr = RepeatRunGuard.parse("A=3/0/0/0/0");
        KeyedOneInputStreamOperatorTestHarness<String, DeviceRound, DeviceRound> h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new RawCacheFunction(1000, 10, thr)),
                        DeviceRound::getDevice, TypeInformation.of(String.class));
        h.open();
        for (int i = 0; i < 4; i++) {
            h.processElement(round("A", i * 10L, 7.0, i, i, i, i), i * 10_000L);
            h.processElement(round("B", i * 10L, 7.0, i, i, i, i), i * 10_000L);
        }
        List<DeviceRound> out = new ArrayList<>();
        h.getOutput().forEach(o -> out.add((DeviceRound) ((StreamRecord<?>) o).getValue()));
        long flaggedA = out.stream().filter(r -> "A".equals(r.getDevice()) && r.stuckMaskBits() != 0).count();
        long flaggedB = out.stream().filter(r -> "B".equals(r.getDevice()) && r.stuckMaskBits() != 0).count();
        assertEquals(2, flaggedA, "A 的第 3、4 轮达到阈值 3");
        assertEquals(0, flaggedB, "B 没有阈值，不打标");
        h.close();
    }

    @Test
    @DisplayName("监测快照：统计本窗口打标的轮数与通道位图")
    void monitoringCountsStuckRounds() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, DeviceRound, MonitoringSnapshot> h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new MonitoringAggregator()),
                        DeviceRound::getDevice, TypeInformation.of(String.class));
        h.open();
        for (int i = 0; i < 5; i++) {
            DeviceRound r = round("A", i * 10L, 1, 1, 1, 1, 1);
            if (i >= 3) {
                r.getStuckMask()[0] = true;
                r.getStuckMask()[3] = i == 4;
                r.getGuardRunLength()[0] = i + 1;
                r.getGuardRunStartTs()[0] = 0;
            }
            h.processElement(r, i * 10_000L);
        }
        h.processWatermark(60_000L);
        List<MonitoringSnapshot> out = new ArrayList<>();
        h.getOutput().forEach(o -> {
            if (o instanceof StreamRecord) {
                out.add((MonitoringSnapshot) ((StreamRecord<?>) o).getValue());
            }
        });
        assertEquals(1, out.size());
        assertEquals(2, out.get(0).getStuckRounds());
        assertEquals(0b1001, out.get(0).getStuckChannelsMask(), "温度（第 0 位）与气体（第 3 位）");
        assertEquals(0, out.get(0).getFloorRounds());
        assertEquals(5, out.get(0).getGuardRunLength()[0], "取窗口最后一轮的游程长度");
        h.close();
    }
}
