package com.leejean.m7;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 聚合模块第三批两项的单元测试（交接文档第一、三节）：零级命令的进入之日（多触发各自给出、不早于最近一次零级生效的当地日），
 * 以及振荡规则（两条偏移符号相反的零级之后，同一通道再命中零级时只记录；回落时解除；关闭时不变）。
 * Unit tests of the entry days on level-0 commands and of the oscillation rule.
 */
class FleetBatch3Test {

    private static final String[] DEVS = {"A", "B", "C", "D"};
    private static final String FIRST = "2022-03-01";

    /** 设备 A 某通道的逐日设定：偏离、偏移、转移 / per-day setting of one channel of device A. */
    private static final class Day {
        final boolean dev;
        final double shift;
        final String trans;

        Day(boolean dev, double shift, String trans) {
            this.dev = dev;
            this.shift = shift;
            this.trans = trans;
        }
    }

    private static Day on(double shift) {
        return new Day(true, shift, "");
    }

    private static final Day OFF = new Day(false, 0.0, "");

    /** 跑若干当地日：A 的 Gas 与 Temperature 按给定逐日设定，其余设备通道都可判且正常 / run with device A's two channels. */
    private static List<FleetDecisionFunction.Command> run(FleetDecisionFunction.Params p, Day[] gas, Day[] temp,
                                                         List<FleetDecisionFunction.Review> review) {
        FleetDecisionFunction f = new FleetDecisionFunction(p);
        for (String d : DEVS) {
            f.addDevice(d);
        }
        List<FleetDecisionFunction.Command> cmds = new ArrayList<>();
        String day = FIRST;
        for (int i = 0; i < gas.length; i++, day = FleetDecisionFunction.addDays(day, 1)) {
            for (String d : DEVS) {
                for (String ch : FleetDecisionFunction.CHANNELS) {
                    Day x = !d.equals("A") ? OFF : ch.equals("Gas") ? gas[i] : ch.equals("Temperature") ? temp[i] : OFF;
                    f.addDataDay(d, day, ch, true, x.dev, x.trans, 1, x.shift);
                }
            }
            f.dayTick(day, cmds, review);
        }
        return cmds;
    }

    private static FleetDecisionFunction.Params params() {
        FleetDecisionFunction.Params p = new FleetDecisionFunction.Params();
        p.pPoint = 0.05;
        return p;
    }

    @Test
    @DisplayName("进入之日：多触发各给一天；不早于最近一次零级生效的当地日；关闭时为空 / entry days per trigger, with the floor")
    void entryDays() {
        Day[] gas = {on(1), on(1), on(1), on(1), OFF, OFF};
        Day[] temp = {OFF, OFF, on(1), on(1), OFF, OFF};
        FleetDecisionFunction.Params p = params();
        assertNull(run(p, gas, temp, new ArrayList<>()).get(0).entryDays, "关闭时不填 / null when off");
        p.anchorEntryDays = true;
        List<FleetDecisionFunction.Command> c = run(p, gas, temp, new ArrayList<>());
        assertEquals(2, c.size());
        assertEquals("Gas", c.get(0).triggers);
        assertEquals(Arrays.asList("2022-03-01"), c.get(0).entryDays, "第一条：从决定日向前连续偏离的第一天");
        // 第二条在 03-04 日界决定，触发 Temperature 与 Gas（按通道次序）；上一条零级在 03-02 日界决定、03-04 当地零点生效，所以两通道的进入之日都截在 03-04
        assertEquals("Temperature|Gas", c.get(1).triggers);
        assertEquals(Arrays.asList("2022-03-04", "2022-03-04"), c.get(1).entryDays, "不早于上一条零级生效的当地日 / the floor");
    }

    @Test
    @DisplayName("振荡规则：两条偏移相反的零级之后第三条只记录；关闭时照发；回落（RECOVER）后解除 / oscillation rule")
    void oscillation() {
        Day[] gas = {on(1), on(1), OFF, OFF, on(-1), on(-1), OFF, on(1), on(1), OFF};
        Day[] temp = new Day[gas.length];
        Arrays.fill(temp, OFF);
        FleetDecisionFunction.Params p = params();
        assertEquals(3, run(p, gas, temp, new ArrayList<>()).size(), "关闭时三条照发 / three commands when off");
        p.oscSuppress = true;
        List<FleetDecisionFunction.Review> rv = new ArrayList<>();
        List<FleetDecisionFunction.Command> c = run(p, gas, temp, rv);
        assertEquals(2, c.size(), "第三条被抑制 / the third is suppressed");
        assertTrue(rv.stream().anyMatch(r -> r.kind.equals("零级被抑制：基线振荡") && r.detail.startsWith("Gas")
                && r.day.equals("2022-03-09")));
        Day[] rec = gas.clone();
        rec[6] = new Day(false, 0.0, "RECOVER");                   // 03-07 回落 / falls back on 03-07
        assertEquals(3, run(p, rec, temp, new ArrayList<>()).size(), "回落之后解除 / released after a fallback");
        Day[] same = {on(1), on(1), OFF, OFF, on(1), on(1), OFF, on(1), on(1), OFF};
        assertEquals(3, run(p, same, temp, new ArrayList<>()).size(), "同号不构成振荡 / same sign is not an oscillation");
    }
}
