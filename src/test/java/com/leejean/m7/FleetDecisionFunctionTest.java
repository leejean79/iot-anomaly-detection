package com.leejean.m7;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 聚合决策函数的单元测试（交接文档 1.23 版第六节「测试」所列各项中，与默认时间参数相关的部分）。每个用例用合成观测驱动
 * {@link FleetDecisionFunction}：四台设备 A 至 D 默认每小时都是正常的整点（360 轮、无离群、无超阈），由用例改动其中几项。
 * Unit tests of the M7 decision function with synthetic observations; devices A to D are normal unless a test changes them.
 */
class FleetDecisionFunctionTest {

    private static final String[] DEVS = {"A", "B", "C", "D"};
    private static final String FIRST = "2022-03-01";

    /** 一次运行的全部输出 / all outputs of one run. */
    private static final class Run {
        final List<FleetDecisionFunction.Alert> alerts = new ArrayList<>();
        final List<FleetDecisionFunction.Command> commands = new ArrayList<>();
        final List<FleetDecisionFunction.Review> review = new ArrayList<>();

        String label(String dev, String localHour) {
            for (FleetDecisionFunction.Alert a : alerts) {
                if (a.device.equals(dev) && a.localHour.equals(localHour)) {
                    return a.label;
                }
            }
            return null;
        }

        long commands(String dev) {
            return commands.stream().filter(c -> c.device.equals(dev)).count();
        }
    }

    private static FleetDecisionFunction.Params params() {
        FleetDecisionFunction.Params p = new FleetDecisionFunction.Params();
        p.pPoint = 0.05;
        return p;
    }

    private static FleetDecisionFunction.HourObs normal() {
        FleetDecisionFunction.HourObs o = new FleetDecisionFunction.HourObs();
        o.rounds = 360;
        o.admitted = 360;
        for (int k = 0; k < FleetDecisionFunction.ACTIVE.length; k++) {
            o.n[k] = 360;
            o.sum[k] = 360 * 10.0;
        }
        return o;
    }

    /**
     * 跑 days 个当地日；setup 在开始前送入事件，hour 可改动每台每小时的观测（参数为设备与当地「日 时」）。
     * Runs a number of local days; setup feeds events first, hour may modify each device-hour observation.
     */
    private static Run run(FleetDecisionFunction.Params p, int days, java.util.function.Consumer<FleetDecisionFunction> setup,
                           HourEdit hour) {
        FleetDecisionFunction f = new FleetDecisionFunction(p);
        for (String d : DEVS) {
            f.addDevice(d);
            f.setFreeze(d, FleetDecisionFunction.dayStart(FIRST) + 60);
            for (String ch : FleetDecisionFunction.ACTIVE) {
                f.setRef(d, ch, 9.0, 11.0);
            }
        }
        setup.accept(f);
        Run r = new Run();
        String day = FIRST;
        for (int i = 0; i < days; i++, day = FleetDecisionFunction.addDays(day, 1)) {
            for (long hs : FleetDecisionFunction.localHours(day)) {
                Map<String, FleetDecisionFunction.HourObs> obs = new HashMap<>();
                String lh = day + String.format(" %02d", FleetDecisionFunction.localHour(hs));
                for (String d : DEVS) {
                    FleetDecisionFunction.HourObs o = normal();
                    if (hour.edit(d, lh, o)) {
                        obs.put(d, o);
                    }
                }
                r.alerts.addAll(f.hourTick(hs, obs));
            }
            f.dayTick(day, r.commands, r.review);
        }
        return r;
    }

    /** 改动一台设备一个小时的观测；返回 false 表示该台该小时没有读数 / edit one observation; false = no readings. */
    private interface HourEdit {
        boolean edit(String dev, String localHour, FleetDecisionFunction.HourObs o);
    }

    private static final HourEdit NONE = (d, h, o) -> true;

    /** 数据层：dev 的 ch 从 from 起偏离 days 天，其余设备通道都可判且正常 / data layer: dev's ch deviates from 'from' for days. */
    private static void dataDays(FleetDecisionFunction f, int total, BiConsumer<String, String[]> custom) {
        String day = FIRST;
        for (int i = 0; i < total; i++, day = FleetDecisionFunction.addDays(day, 1)) {
            for (String d : DEVS) {
                for (String ch : FleetDecisionFunction.CHANNELS) {
                    String[] v = {"1", "0", ""};                    // evaluable, deviated, transition
                    custom.accept(d + "|" + day + "|" + ch, v);
                    f.addDataDay(d, day, ch, "1".equals(v[0]), "1".equals(v[1]), v[2]);
                }
            }
        }
    }

    // ---------------------------------------------------------------- 门槛 / threshold
    @Test
    @DisplayName("共模门槛：在场 0 至 8 台时为 max(2, ⌈3N/4⌉) / common-mode threshold")
    void threshold() {
        FleetDecisionFunction.Params p = params();
        int[] expected = {2, 2, 2, 3, 3, 4, 5, 6, 6};
        for (int n = 0; n <= 8; n++) {
            assertEquals(expected[n], p.threshold(n), "N=" + n);
        }
    }

    // ---------------------------------------------------------------- 性能层的确认与回落 / perf fallback
    @Test
    @DisplayName("性能层确认之后，凑满六个元素且中位数不高于门限即回落 / perf confirmation falls back")
    void perfFallback() {
        long t0 = FleetDecisionFunction.dayStart(FIRST) + 6 * 3600;
        Run r = run(params(), 1, f -> {
            f.addPerfEvent("A", t0, "CONFIRM", 1.0, 3.0);
            double[] v = {5, 5, 5, 0.5, 0.5, 0.5, 0.5, 0.5};       // 第六个元素（07:00）中位数 2.75，第七个（07:10）0.5 / falls at 07:10
            for (int k = 0; k < v.length; k++) {
                f.addPerfElement("A", t0 + 600L * (k + 1), v[k]);
            }
        }, NONE);
        assertEquals(FleetDecisionFunction.TRANSIENT, r.label("A", FIRST + " 06"), "07:00 时凑满六个，中位数 2.75 仍高于门限");
        assertEquals(null, r.label("A", FIRST + " 07"), "07:10 的元素使最近六个的中位数降到 0.5，已回落 / fallen at 07:10");
    }

    // ---------------------------------------------------------------- 时间形态三条规则 / three persistence rules
    @Test
    @DisplayName("持续：数据层连续两日偏离 → 局部漂移，发一条零级命令 / two consecutive deviated days")
    void ruleTwoDays() {
        Run r = run(params(), 4, f -> dataDays(f, 4, (k, v) -> {
            if (k.startsWith("A|") && k.endsWith("|Gas") && k.compareTo("A|2022-03-02") >= 0) {
                v[1] = "1";
                v[2] = k.startsWith("A|2022-03-02") ? "DEVIATE" : "";
            }
        }), NONE);
        assertEquals(1, r.commands("A"));
        FleetDecisionFunction.Command c = r.commands.get(0);
        assertEquals("2022-03-03", c.day);
        assertEquals("Gas", c.triggers);
        assertEquals("局部", c.scope);
        assertTrue(c.forms.contains("连续两日偏离"));
    }

    @Test
    @DisplayName("持续：当日偏离且性能层同日已确认 → 两层同日同报 / same-day data and perf")
    void ruleSameDay() {
        long t = FleetDecisionFunction.dayStart("2022-03-02") + 12 * 3600;
        Run r = run(params(), 3, f -> {
            dataDays(f, 3, (k, v) -> {
                if (k.equals("A|2022-03-02|Gas")) {
                    v[1] = "1";
                    v[2] = "DEVIATE";
                }
            });
            f.addPerfEvent("A", t, "CONFIRM", 1.0, 2.0);
        }, NONE);
        assertEquals(1, r.commands("A"));
        assertEquals("2022-03-02", r.commands.get(0).day);
        assertTrue(r.commands.get(0).forms.contains("两层同日同报"));
    }

    @Test
    @DisplayName("持续：性能层确认跨过日界仍未回落 → 由性能层触发 / perf confirmation crossing a boundary")
    void rulePerfCross() {
        long t = FleetDecisionFunction.dayStart(FIRST) + 20 * 3600;
        Run r = run(params(), 3, f -> {
            dataDays(f, 3, (k, v) -> { });
            f.addPerfEvent("B", t, "CONFIRM", 1.0, 2.0);
        }, NONE);
        assertEquals(1, r.commands("B"));
        assertEquals("2022-03-02", r.commands.get(0).day, "第一个日界时尚未跨过日界 / not yet crossed at the first boundary");
        assertEquals("perf", r.commands.get(0).triggers);
    }

    // ---------------------------------------------------------------- 只发一次与冷却期 / once per episode and cooldown
    @Test
    @DisplayName("同一段偏离只发一次；关闭这条规则时按冷却期每两天一条 / once per episode; cooldown otherwise")
    void onceAndCooldown() {
        java.util.function.Consumer<FleetDecisionFunction> setup = f -> dataDays(f, 8, (k, v) -> {
            if (k.startsWith("C|") && k.endsWith("|Light")) {
                v[1] = "1";
                v[2] = k.startsWith("C|2022-03-01") ? "DEVIATE" : "";
            }
        });
        assertEquals(1, run(params(), 8, setup, NONE).commands("C"));
        FleetDecisionFunction.Params p = params();
        p.oncePerEpisode = false;
        Run r = run(p, 8, setup, NONE);
        List<String> days = new ArrayList<>();
        for (FleetDecisionFunction.Command c : r.commands) {
            days.add(c.day);
        }
        assertEquals(List.of("2022-03-02", "2022-03-04", "2022-03-06", "2022-03-08"), days, "冷却期内不重发 / none within cooldown");
        assertEquals(4, r.commands.get(3).episodeN);
    }

    @Test
    @DisplayName("触发信号回落之后，同一台可以再收到命令 / a fallback ends the episode")
    void fallbackEndsEpisode() {
        Run r = run(params(), 8, f -> dataDays(f, 8, (k, v) -> {
            if (k.startsWith("C|") && k.endsWith("|Light") && !k.startsWith("C|2022-03-04")) {
                v[1] = "1";
            }
        }), NONE);
        assertEquals(2, r.commands("C"));
    }

    // ---------------------------------------------------------------- 优先次序 / priority
    @Test
    @DisplayName("优先次序：质量问题压过一切；分布切换压过异常（点通道静默）；漂移压过现场条件 / priority")
    void priority() {
        long burstEl = FleetDecisionFunction.dayStart("2022-03-03") + 9 * 3600 - 600;    // 08:50 的元素 / element ending 08:50
        Run r = run(params(), 3, f -> {
            dataDays(f, 3, (k, v) -> {
                if (k.equals("B|2022-03-02|Temperature")) {
                    v[1] = "1";
                    v[2] = "DEVIATE";
                }
            });
            f.addBurst("B", burstEl, burstEl + 60);
        }, (d, h, o) -> {
            if (d.equals("A") && h.equals("2022-03-03 05")) {
                o.rounds = 200;                                  // 缺轮 44% / quality problem
                o.outlierRounds = 100;
            }
            if (d.equals("B") && (h.equals("2022-03-03 09") || h.equals("2022-03-03 10"))) {
                o.outlierRounds = 100;                           // 点通道越门槛 / point flag
            }
            return true;
        });
        assertEquals(FleetDecisionFunction.QUALITY, r.label("A", "2022-03-03 05"));
        assertEquals(FleetDecisionFunction.SWITCH, r.label("B", "2022-03-03 09"));
        assertEquals(FleetDecisionFunction.SWITCH, r.label("B", "2022-03-03 10"), "爆发元素落在前两个小时之内 / within two hours");
    }

    // ---------------------------------------------------------------- 现场条件（累计计数）/ site condition
    @Test
    @DisplayName("现场条件按累计计数：七天内另有两天同一钟点有标志且光照同步偏离 → 现场条件；只有一天 → 异常")
    void siteCumulative() {
        HourEdit noon = (d, h, o) -> {
            boolean flagDay = h.equals("2022-03-12 12") || h.equals("2022-03-14 12") || h.equals("2022-03-17 12");
            if (d.equals("C") && flagDay) {
                o.ctxAbove = 4;
                o.sum[2] = 360 * 12.0;                           // 光照均值 12，基线 10，宽度 2 的一半为 1 / light 2 above baseline
            }
            return true;
        };
        Run r = run(params(), 17, f -> dataDays(f, 17, (k, v) -> { }), noon);
        assertEquals(FleetDecisionFunction.ANOMALY, r.label("C", "2022-03-12 12"));
        assertEquals(FleetDecisionFunction.ANOMALY, r.label("C", "2022-03-14 12"), "此前只有一天 / only one earlier day");
        assertEquals(FleetDecisionFunction.SITE, r.label("C", "2022-03-17 12"), "七天内另有两天（不连续）/ two earlier days");
    }

    @Test
    @DisplayName("漂移压过现场条件：同样的正午标志与光照偏离，C 处于漂移状态时归为漂移 / drift outranks site condition")
    void driftOutranksSite() {
        HourEdit noon = (d, h, o) -> {
            boolean flagDay = h.equals("2022-03-12 12") || h.equals("2022-03-14 12") || h.equals("2022-03-17 12");
            if (d.equals("C") && flagDay) {
                o.ctxAbove = 4;
                o.sum[2] = 360 * 12.0;
            }
            return true;
        };
        Run r = run(params(), 17, f -> dataDays(f, 17, (k, v) -> {
            if (k.startsWith("C|") && k.endsWith("|Gas") && k.compareTo("C|2022-03-14") >= 0) {
                v[1] = "1";                                      // C 的气体 03-14 起偏离，03-15 日界判为漂移
            }
        }), noon);
        assertEquals(FleetDecisionFunction.DRIFT, r.label("C", "2022-03-17 12"));
    }

    @Test
    @DisplayName("现场条件在基线冻结之前不成立（冻结日起十个当地日）/ no site condition while the baseline accumulates")
    void siteNeedsBaseline() {
        HourEdit noon = (d, h, o) -> {
            if (d.equals("C") && h.endsWith(" 12")) {
                o.ctxAbove = 4;                                  // 每天正午都有标志 / a flag every noon
                if (h.compareTo("2022-03-11") >= 0) {
                    o.sum[2] = 360 * 12.0;                       // 基线冻结之后光照偏高 / light high after the baseline froze
                }
            }
            return true;
        };
        Run r = run(params(), 12, f -> dataDays(f, 12, (k, v) -> { }), noon);
        assertEquals(FleetDecisionFunction.ANOMALY, r.label("C", "2022-03-10 12"), "第十个当地日仍在累积 / still accumulating");
        assertEquals(FleetDecisionFunction.SITE, r.label("C", "2022-03-11 12"));
    }

    // ---------------------------------------------------------------- 机队计数 / fleet counting
    @Test
    @DisplayName("不可判的设备不计入在场设备数：两台持续偏离，在场四台时为局部，在场两台时为全局 / non-evaluable devices lower N")
    void evaluableReducesN() {
        BiConsumer<String, String[]> ab = (k, v) -> {
            if (k.endsWith("|Gas") && (k.startsWith("A|") || k.startsWith("B|"))) {
                v[1] = "1";
            }
        };
        Run all = run(params(), 3, f -> dataDays(f, 3, ab), NONE);
        assertEquals("局部", all.commands.get(0).scope, "N = 4，门槛 3 / threshold 3");
        Run two = run(params(), 3, f -> dataDays(f, 3, (k, v) -> {
            ab.accept(k, v);
            if (k.startsWith("C|") || k.startsWith("D|")) {
                v[0] = "0";                                      // C、D 整日不可判 / C and D not evaluable
            }
        }), NONE);
        assertEquals("全局", two.commands.get(0).scope, "N = 2，门槛 2 / threshold 2");
        assertEquals(2, two.commands.get(0).co);
    }

    // ---------------------------------------------------------------- 复核 / review
    @Test
    @DisplayName("日界复核：补标当日不可判；当日进入偏离时把爆发小时的异常改记为分布切换 / daily review")
    void review() {
        long el = FleetDecisionFunction.dayStart("2022-03-02") + 15 * 3600;           // 15:00 结束的元素 / element ending 15:00
        Run r = run(params(), 2, f -> {
            dataDays(f, 2, (k, v) -> {
                if (k.startsWith("D|2022-03-02")) {
                    v[0] = "0";
                }
                if (k.equals("B|2022-03-02|Gas")) {
                    v[1] = "1";
                    v[2] = "DEVIATE";
                }
            });
            f.addBurst("B", el, el + 4000);
        }, (d, h, o) -> {
            if (d.equals("B") && h.equals("2022-03-02 14")) {
                o.outlierRounds = 100;
            }
            return true;
        });
        assertEquals(FleetDecisionFunction.ANOMALY, r.label("B", "2022-03-02 14"));
        assertTrue(r.review.stream().anyMatch(x -> x.device.equals("D") && x.kind.equals("当日不可判")));
        assertTrue(r.review.stream().anyMatch(x -> x.device.equals("B") && x.kind.equals("异常改记为分布切换")
                && x.detail.equals("2022-03-02 14")));
    }

    // ---------------------------------------------------------------- 夏令时与生效时刻 / DST and effective time
    @Test
    @DisplayName("夏令时：03-27 只有 23 个整点；03-26 日界决定的命令在 23 小时后生效 / DST day and effective time")
    void dstAndEffective() {
        assertEquals(23, FleetDecisionFunction.localHours("2022-03-27").size());
        assertEquals(24, FleetDecisionFunction.localHours("2022-03-26").size());
        FleetDecisionFunction.Params p = params();
        FleetDecisionFunction f = new FleetDecisionFunction(p);
        for (String d : DEVS) {
            f.addDevice(d);
        }
        for (String day : new String[]{"2022-03-25", "2022-03-26"}) {
            for (String d : DEVS) {
                for (String ch : FleetDecisionFunction.CHANNELS) {
                    f.addDataDay(d, day, ch, true, d.equals("A") && ch.equals("Gas"), "");
                }
            }
        }
        List<FleetDecisionFunction.Command> cmds = new ArrayList<>();
        f.dayTick("2022-03-25", cmds, new ArrayList<>());
        f.dayTick("2022-03-26", cmds, new ArrayList<>());
        assertEquals(1, cmds.size());
        FleetDecisionFunction.Command c = cmds.get(0);
        assertEquals(FleetDecisionFunction.dayStart("2022-03-27"), c.decidedAt);
        assertEquals(23 * 3600L, c.effectiveAt - c.decidedAt, "Δ 默认一个当地日，03-27 只有 23 小时 / one local day");
        p.deltaSec = 3600;
        FleetDecisionFunction g = new FleetDecisionFunction(p);
        for (String d : DEVS) {
            g.addDevice(d);
            for (String day : new String[]{"2022-03-25", "2022-03-26"}) {
                for (String ch : FleetDecisionFunction.CHANNELS) {
                    g.addDataDay(d, day, ch, true, d.equals("A") && ch.equals("Gas"), "");
                }
            }
        }
        cmds.clear();
        g.dayTick("2022-03-25", cmds, new ArrayList<>());
        g.dayTick("2022-03-26", cmds, new ArrayList<>());
        assertEquals(3600L, cmds.get(0).effectiveAt - cmds.get(0).decidedAt, "Δ = 1 小时 / one hour");
        assertFalse(cmds.get(0).forms.isEmpty());
    }
}
