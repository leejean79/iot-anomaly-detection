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
        // 只认第二条生效（03-08 当地零点）之后的回落（1.56 版裁决第二项）/ only fallbacks after the second took effect count
        Day[] early = gas.clone();
        early[6] = new Day(false, 0.0, "RECOVER");                 // 03-07 回落：第二条已决定、未生效 / before the effect
        assertEquals(2, run(p, early, temp, new ArrayList<>()).size(), "生效前的回落不解除 / a fallback before the effect does not release");
        Day[] rec = {on(1), on(1), OFF, OFF, on(-1), on(-1), OFF, new Day(false, 0.0, "RECOVER"), on(1), on(1), OFF};
        Day[] temp2 = new Day[rec.length];
        Arrays.fill(temp2, OFF);
        assertEquals(3, run(p, rec, temp2, new ArrayList<>()).size(), "生效之后回落即解除 / released by a fallback after the effect");
        Day[] same = {on(1), on(1), OFF, OFF, on(1), on(1), OFF, on(1), on(1), OFF};
        assertEquals(3, run(p, same, temp, new ArrayList<>()).size(), "同号不构成振荡 / same sign is not an oscillation");
    }

    @Test
    @DisplayName("二级预案：重建期告警加标记与剩余天数、每日一条重建期日报、上线时恢复告警、第一周后超阈率对照；关闭时都没有")
    void rebuildPlan() {
        for (boolean on : new boolean[]{false, true}) {
            FleetDecisionFunction.Params p = params();
            p.modelEvents = true;
            p.level2Days = 3;
            p.rebuildPlan = on;
            FleetDecisionFunction f = new FleetDecisionFunction(p);
            for (String d : DEVS) {
                f.addDevice(d);
                f.addModelEvent(d, FleetDecisionFunction.dayStart(FIRST), "online");
            }
            long off = FleetDecisionFunction.dayStart("2022-03-03");
            long onl = FleetDecisionFunction.dayStart("2022-03-06");
            f.addModelEvent("A", off, "offline");
            f.addModelEvent("A", onl, "online");
            for (int k = 0; k < 10; k++) {                          // 上一代最后一周 2 / 10 / previous generation's last week
                f.addCtxWindow("A", off - 86400 + k * 600, k < 2);
            }
            for (int k = 0; k < 20; k++) {                          // 新模型第一周 10 / 20 / the new model's first week
                f.addCtxWindow("A", onl + 3600 + k * 600, k % 2 == 0);
            }
            List<FleetDecisionFunction.Alert> alerts = new ArrayList<>();
            List<FleetDecisionFunction.Review> review = new ArrayList<>();
            List<FleetDecisionFunction.Command> cmds = new ArrayList<>();
            String day = FIRST;
            for (int i = 0; i < 14; i++, day = FleetDecisionFunction.addDays(day, 1)) {
                for (String d : DEVS) {
                    for (String ch : FleetDecisionFunction.CHANNELS) {
                        f.addDataDay(d, day, ch, true, false, "");
                    }
                }
                for (long hs : FleetDecisionFunction.localHours(day)) {
                    java.util.Map<String, FleetDecisionFunction.HourObs> obs = new java.util.HashMap<>();
                    for (String d : DEVS) {
                        FleetDecisionFunction.HourObs o = new FleetDecisionFunction.HourObs();
                        o.rounds = 360;
                        o.admitted = 360;
                        boolean spike = d.equals("A") && FleetDecisionFunction.localHour(hs) == 12 && day.equals("2022-03-04");
                        o.outlierRounds = spike ? 100 : 0;
                        obs.put(d, o);
                    }
                    alerts.addAll(f.alertTick(hs, obs));
                }
                f.dayTick(day, cmds, review);
            }
            FleetDecisionFunction.Alert spike = alerts.stream().filter(a -> a.localHour.equals("2022-03-04 12")).findFirst().get();
            List<FleetDecisionFunction.Alert> rec = new ArrayList<>();
            alerts.stream().filter(a -> a.label.equals("上下文通道恢复")).forEach(rec::add);
            List<FleetDecisionFunction.Review> daily = new ArrayList<>();
            review.stream().filter(r -> r.kind.equals("重建期日报")).forEach(daily::add);
            List<FleetDecisionFunction.Review> cmp = new ArrayList<>();
            review.stream().filter(r -> r.kind.startsWith("上下文通道恢复")).forEach(cmp::add);
            if (!on) {
                assertTrue(spike.note.isEmpty() && rec.isEmpty() && daily.isEmpty() && cmp.isEmpty(), "关闭时没有 / nothing when off");
                continue;
            }
            assertEquals("上下文重建中（剩余 2 天）", spike.note, "下线 03-03 零点、收集三天，03-04 13 时还剩 35 小时");
            assertEquals(1, rec.size());
            assertEquals("2022-03-06 00", rec.get(0).localHour);
            assertEquals(Arrays.asList("2022-03-03", "2022-03-04", "2022-03-05"),
                    Arrays.asList(daily.stream().map(r -> r.day).toArray()), "重建期每个日界一条");
            assertTrue(daily.get(1).detail.contains("点通道离群率 1.16%") && daily.get(1).detail.contains("Temperature 正常"),
                    daily.get(1).detail);
            assertEquals(1, cmp.size());
            assertEquals("新模型第一周 10 / 20 = 0.5000；上一代最后一周 2 / 10 = 0.2000", cmp.get(0).detail);
            assertEquals("2022-03-12", cmp.get(0).day, "第一周满之后的日界写入 / written once the first week is complete");
        }
    }

    @Test
    @DisplayName("丁档：零级推迟到最早进入之日加七天的日界、在生效前一日界发出并给序号；生效前触发通道回落则撤销并解除冷却")
    void deferredLevelZero() {
        FleetDecisionFunction.Params p = params();
        p.deferDays = 7;
        Day[] gas = new Day[10];
        Arrays.fill(gas, on(1));
        Day[] temp = new Day[gas.length];
        Arrays.fill(temp, OFF);
        List<FleetDecisionFunction.Command> c = run(p, gas, temp, new ArrayList<>());
        assertEquals(1, c.size());
        assertEquals("2022-03-02", c.get(0).day, "决定日不变 / the decision day is kept");
        assertEquals(FleetDecisionFunction.dayStart("2022-03-08"), c.get(0).effectiveAt, "进入之日 03-01 加七天 / entry plus seven");
        assertEquals(1, c.get(0).seq);
        assertEquals(Arrays.asList("2022-03-01"), c.get(0).entryDays);
        // 03-01 至 03-04 偏离、03-05 回落：撤销；03-07 起再偏离，03-08 日界再决定，03-14 生效 / cancelled, then decided again
        Day[] back = new Day[14];
        Arrays.fill(back, on(1));
        back[4] = OFF;
        back[5] = OFF;
        Day[] temp2 = new Day[back.length];
        Arrays.fill(temp2, OFF);
        List<FleetDecisionFunction.Review> rv = new ArrayList<>();
        List<FleetDecisionFunction.Command> d = run(p, back, temp2, rv);
        assertTrue(rv.stream().anyMatch(r -> r.kind.equals("零级撤销：生效前回落") && r.day.equals("2022-03-05")), rv.toString());
        assertEquals(1, d.size());
        assertEquals("2022-03-08", d.get(0).day);
        assertEquals(FleetDecisionFunction.dayStart("2022-03-14"), d.get(0).effectiveAt);
        assertEquals(1, d.get(0).seq, "撤销的命令不占序号 / a cancelled command takes no sequence number");
    }
}
