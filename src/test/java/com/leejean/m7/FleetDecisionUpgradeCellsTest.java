package com.leejean.m7;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 决策矩阵升级格的单元测试（1.41 版裁决第三节）：只有性能层持续为一级；两层同报为一级；只有数据层持续为零级；零级之后 K_up 天内
 * 同一通道新持续段为一级；一级之后 K_up 天内性能层新确认段为二级；各级冷却只阻止同级；命令与告警带模型代数（第二节第五条）。
 * Unit tests of the upgrade cells of the decision matrix (ruling 1.41 §3) and the model generation on outputs (§2.5).
 */
class FleetDecisionUpgradeCellsTest {

    private static final String[] DEVS = {"A", "B", "C", "D"};
    private static final String FIRST = "2022-03-01";

    private static FleetDecisionFunction.Params params(boolean upgrade) {
        FleetDecisionFunction.Params p = new FleetDecisionFunction.Params();
        p.pPoint = 0.05;
        p.upgradeCells = upgrade;
        return p;
    }

    private static long at(String day, int hour) {
        return FleetDecisionFunction.dayStart(day) + hour * 3600L;
    }

    /** 跑 days 个当地日，每台每小时都是正常的整点；返回命令 / run normal hours for a number of days; return commands. */
    private static List<FleetDecisionFunction.Command> run(FleetDecisionFunction.Params p, int days,
                                                           Consumer<FleetDecisionFunction> setup,
                                                           List<FleetDecisionFunction.Alert> alerts) {
        FleetDecisionFunction f = new FleetDecisionFunction(p);
        for (String d : DEVS) {
            f.addDevice(d);
            f.setFreeze(d, FleetDecisionFunction.dayStart(FIRST) + 60);
            for (String ch : FleetDecisionFunction.ACTIVE) {
                f.setRef(d, ch, 9.0, 11.0);
            }
        }
        setup.accept(f);
        List<FleetDecisionFunction.Command> cmds = new ArrayList<>();
        String day = FIRST;
        for (int i = 0; i < days; i++, day = FleetDecisionFunction.addDays(day, 1)) {
            for (long hs : FleetDecisionFunction.localHours(day)) {
                Map<String, FleetDecisionFunction.HourObs> obs = new HashMap<>();
                for (String d : DEVS) {
                    FleetDecisionFunction.HourObs o = new FleetDecisionFunction.HourObs();
                    o.rounds = 360;
                    o.admitted = 360;
                    for (int k = 0; k < FleetDecisionFunction.ACTIVE.length; k++) {
                        o.n[k] = 360;
                        o.sum[k] = 3600.0;
                    }
                    obs.put(d, o);
                }
                List<FleetDecisionFunction.Alert> a = f.alertTick(hs, obs);
                if (alerts != null) {
                    alerts.addAll(a);
                }
            }
            f.dayTick(day, cmds, new ArrayList<>());
        }
        return cmds;
    }

    /** 数据层日结论：custom 改动「设备|日|通道」的 {可判, 偏离, 转移} / data-layer day records. */
    private static void dataDays(FleetDecisionFunction f, int total, BiConsumer<String, String[]> custom) {
        String day = FIRST;
        for (int i = 0; i < total; i++, day = FleetDecisionFunction.addDays(day, 1)) {
            for (String d : DEVS) {
                for (String ch : FleetDecisionFunction.CHANNELS) {
                    String[] v = {"1", "0", ""};
                    custom.accept(d + "|" + day + "|" + ch, v);
                    f.addDataDay(d, day, ch, "1".equals(v[0]), "1".equals(v[1]), v[2]);
                }
            }
        }
    }

    /** dev 的 ch 在给定的当地日偏离 / dev's channel deviates on the given days. */
    private static BiConsumer<String, String[]> deviate(String dev, String ch, String... days) {
        return (k, v) -> {
            for (String d : days) {
                if (k.equals(dev + "|" + d + "|" + ch)) {
                    v[1] = "1";
                }
            }
        };
    }

    /** 性能层在 t 确认，并在 fallAt 之后凑满六个低于门限的元素而回落（fallAt 为负则不回落）/ confirm, optionally fall back. */
    private static void perf(FleetDecisionFunction f, String dev, long t, long fallAt) {
        f.addPerfEvent(dev, t, "CONFIRM", 1.0, 2.0);
        if (fallAt > 0) {
            for (int k = 1; k <= 6; k++) {
                f.addPerfElement(dev, fallAt + 600L * k, 0.5);
            }
        }
    }

    @Test
    @DisplayName("只有性能层持续：升级格打开为一级，关闭时仍为零级（与收口基线相同）")
    void perfOnlyIsLevelOne() {
        Consumer<FleetDecisionFunction> setup = f -> {
            dataDays(f, 3, (k, v) -> { });
            perf(f, "B", at(FIRST, 20), -1);
        };
        FleetDecisionFunction.Command off = run(params(false), 3, setup, null).get(0);
        assertEquals(0, off.level);
        assertNull(off.cell, "升级格关闭时不写决定依据");
        FleetDecisionFunction.Command on = run(params(true), 3, setup, null).get(0);
        assertEquals("2022-03-02", on.day);
        assertEquals(1, on.level);
        assertEquals("一级：只有性能层持续", on.cell);
    }

    @Test
    @DisplayName("两层同报（当日偏离且性能层同日已确认）为一级；只有数据层持续为零级")
    void twoLayersAndDataOnly() {
        List<FleetDecisionFunction.Command> c = run(params(true), 3, f -> {
            dataDays(f, 3, deviate("A", "Gas", "2022-03-02"));
            perf(f, "A", at("2022-03-02", 12), -1);
        }, null);
        assertEquals(1, c.get(0).level);
        assertEquals("一级：两层同报", c.get(0).cell);
        List<FleetDecisionFunction.Command> d = run(params(true), 4, f -> dataDays(f, 4,
                deviate("A", "Gas", "2022-03-02", "2022-03-03")), null);
        assertEquals(0, d.get(0).level);
        assertEquals("零级：只有数据层持续", d.get(0).cell);
    }

    @Test
    @DisplayName("零级之后 K_up 天内同一通道的新持续段为一级；K_up 之外开始的新段仍为零级")
    void sameChannelAfterLevelZero() {
        // 零级：03-02、03-03 偏离，03-03 决定，03-05 生效；新段 03-06 起，在 K_up 之内 / new segment within K_up
        List<FleetDecisionFunction.Command> c = run(params(true), 8, f -> dataDays(f, 8,
                deviate("A", "Gas", "2022-03-02", "2022-03-03", "2022-03-06", "2022-03-07")), null);
        assertEquals(2, c.size());
        assertEquals(0, c.get(0).level);
        assertEquals("2022-03-07", c.get(1).day);
        assertEquals(1, c.get(1).level);
        assertEquals("一级：零级之后 K_up 天内同一通道新持续段", c.get(1).cell);
        // 新段 03-13 起，晚于生效后七个当地日（03-12 零时）/ a new segment after K_up
        List<FleetDecisionFunction.Command> d = run(params(true), 15, f -> dataDays(f, 15,
                deviate("A", "Gas", "2022-03-02", "2022-03-03", "2022-03-13", "2022-03-14")), null);
        assertEquals(2, d.size());
        assertEquals(0, d.get(1).level);
        // 另一个通道的新持续段不算「同一通道」/ another channel does not count
        List<FleetDecisionFunction.Command> e = run(params(true), 8, f -> dataDays(f, 8, (k, v) -> {
            deviate("A", "Gas", "2022-03-02", "2022-03-03").accept(k, v);
            deviate("A", "Light", "2022-03-06", "2022-03-07").accept(k, v);
        }), null);
        assertEquals(0, e.get(1).level);
    }

    @Test
    @DisplayName("一级之后 K_up 天内性能层新确认段为二级；一级的冷却不阻止零级")
    void levelTwoAfterLevelOneAndCrossLevelCooldown() {
        // 一级：03-01 20 时确认，03-02 决定，03-04 生效，生效后侧 N=3，重训完成 03-07 零时（03-07 当地零时，夏令时之前）
        long done = FleetDecisionFunction.dayStart("2022-03-04") + 3 * 86400L;
        List<FleetDecisionFunction.Command> c = run(params(true), 9, f -> {
            dataDays(f, 9, deviate("A", "Gas", "2022-03-04", "2022-03-05"));
            perf(f, "A", at(FIRST, 20), at("2022-03-03", 6));            // 03-03 回落 / falls back on 03-03
            perf(f, "A", done + 12 * 3600L, -1);                         // 完成后半天新确认 / new confirmation after completion
        }, null);
        assertEquals(3, c.size());
        assertEquals(1, c.get(0).level);
        assertEquals(FleetDecisionFunction.dayStart("2022-03-04"), c.get(0).effectiveAt);
        // 03-05 的气体只有数据层持续：一级冷却期内照常发零级 / level 0 is not blocked by the level-1 cooldown
        assertEquals("2022-03-05", c.get(1).day);
        assertEquals(0, c.get(1).level);
        assertEquals("2022-03-08", c.get(2).day);
        assertEquals(2, c.get(2).level);
        assertEquals("二级：一级之后 K_up 天内性能层新确认段", c.get(2).cell);
    }

    @Test
    @DisplayName("一级的冷却期（重训完成后七个当地日）阻止新的一级，不阻止二级")
    void levelOneCooldownBlocksOnlyLevelOne() {
        // 第一段 03-01 确认 → 一级（03-04 生效，03-07 完成，冷却到 03-14）；第二段 03-04 确认，早于完成，只有性能层持续本应是
        // 一级，被冷却挡住；第三段 03-10 确认，在完成后 K_up 之内，为二级 / second segment blocked, third is level 2
        List<FleetDecisionFunction.Command> c = run(params(true), 12, f -> {
            dataDays(f, 12, (k, v) -> { });
            perf(f, "B", at(FIRST, 20), at("2022-03-03", 6));
            perf(f, "B", at("2022-03-04", 6), at("2022-03-06", 6));
            perf(f, "B", at("2022-03-10", 6), -1);
        }, null);
        assertEquals(2, c.size());
        assertEquals(1, c.get(0).level);
        assertEquals("2022-03-11", c.get(1).day);
        assertEquals(2, c.get(1).level);
    }

    @Test
    @DisplayName("命令与告警带上下文模块的模型代数；从未换过模型时为空")
    void modelGenerationOnOutputs() {
        List<FleetDecisionFunction.Alert> alerts = new ArrayList<>();
        List<FleetDecisionFunction.Command> c = run(params(true), 3, f -> {
            dataDays(f, 3, (k, v) -> { });
            perf(f, "B", at(FIRST, 20), -1);
            f.setModel("B", at(FIRST, 10), 1);
        }, alerts);
        assertEquals(Integer.valueOf(1), c.get(0).model);
        for (FleetDecisionFunction.Alert a : alerts) {
            if (a.device.equals("B") && a.hourStart >= at(FIRST, 10)) {
                assertEquals(Integer.valueOf(1), a.model);
            } else if (!a.device.equals("B")) {
                assertNull(a.model);
            }
        }
    }
}
