package com.leejean.m1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.leejean.m6.AdaptCommand;
import com.leejean.m6.CompactCache;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 适应执行第一批（M6a）标准化算子的单元测试：紧凑缓存、零级纯函数、命令回路打开但无命令时与关闭时的输出、定时器触发与轮触发的
 * 执行逐位相同、迟到命令不执行、版本号与参照直方图和回填附件的挂载位置。
 * Unit tests of the M6a scaler: compact cache, the level-0 function, determinism of execution, late commands and tagging.
 */
class AdaptiveScalerFunctionTest {

    private static final int WARMUP = 300;
    private static final long T0 = 1_646_092_800L;          // 2022-03-01 00:00 UTC
    private static final long WINDOW = 600;                 // 测试用回填窗长 / test backfill window
    private static final long D = 1800;                     // 测试用重估窗口 / test re-estimation window
    private static final ObjectMapper OM = new ObjectMapper();

    /** 合成轮：前半段与后半段的位置与尺度不同，含守卫游程、缺失与光照删失 / synthetic rounds with a level shift. */
    private static List<DeviceRound> rounds() {
        Random rng = new Random(11);
        List<DeviceRound> out = new ArrayList<>();
        for (int i = 0; i < 1200; i++) {
            double shift = i >= 600 ? 3.0 : 0.0;
            DeviceRound r = new DeviceRound();
            r.setDevice("G");
            r.setTs(T0 + 10L * i);
            double[] x = {
                    Math.round((21 + shift + rng.nextGaussian()) * 100) / 100.0,
                    40 + shift + rng.nextGaussian(),
                    101000 + Math.round(rng.nextGaussian() * 20),
                    700 + 30 * rng.nextGaussian() + 100 * shift,
                    rng.nextDouble() < 0.01 ? 65536.0 : Math.floor(rng.nextDouble() * 400 * (1 + shift))};
            boolean[] miss = new boolean[5];
            if (rng.nextDouble() < 0.02) {
                miss[rng.nextInt(5)] = true;
            }
            boolean[] cens = new boolean[5];
            cens[Channels.LIGHT_INDEX] = x[4] == 65536.0;
            boolean[] stuck = new boolean[5];
            stuck[0] = i >= 800 && i < 830;                 // 一段守卫打标 / a guard-flagged run
            r.setX(x);
            r.setMissingMask(miss);
            r.setCensoredMask(cens);
            r.setStuckMask(stuck);
            out.add(r);
        }
        return out;
    }

    private static RobustScalerFunction scaler() {
        return new RobustScalerFunction(WARMUP, 1e-9, ChannelTransform.defaultTable(), false);
    }

    private static AdaptiveScalerFunction adaptive() {
        return new AdaptiveScalerFunction(scaler(), 7 * 86400L, D, WINDOW);
    }

    private static AdaptCommand command(long eff, int seq) {
        AdaptCommand c = new AdaptCommand();
        c.device = "G";
        c.level = 0;
        c.effectiveAt = eff;
        c.decidedAt = eff - 86400;
        c.seq = seq;
        c.raw = "{\"device\":\"G\",\"seq\":" + seq + "}";
        return c;
    }

    private static KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, AdaptCommand, DeviceRound> harness(
            AdaptiveScalerFunction f) throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, AdaptCommand, DeviceRound> h =
                new KeyedTwoInputStreamOperatorTestHarness<>(new KeyedCoProcessOperator<>(f), DeviceRound::getDevice,
                        AdaptCommand::getDevice, TypeInformation.of(String.class));
        h.open();
        h.processWatermark2(new Watermark(Long.MAX_VALUE));   // 与作业相同：命令流的水位线恒为最大值 / as in the job
        return h;
    }

    private static List<DeviceRound> outputs(KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, AdaptCommand, DeviceRound> h) {
        List<DeviceRound> out = new ArrayList<>();
        for (Object o : h.getOutput()) {
            if (o instanceof StreamRecord) {
                out.add((DeviceRound) ((StreamRecord<?>) o).getValue());
            }
        }
        return out;
    }

    /** 逐轮送入，每轮之后把数据流水位线推进到该轮时刻 / feed rounds, advancing the data watermark after each. */
    private static void feed(KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, AdaptCommand, DeviceRound> h,
                             List<DeviceRound> in) throws Exception {
        for (DeviceRound r : in) {
            h.processElement1(new StreamRecord<>(r, r.getTs() * 1000L));
            h.processWatermark1(new Watermark(r.getTs() * 1000L - 1));
        }
    }

    private static String json(DeviceRound r) throws Exception {
        return OM.writeValueAsString(r);
    }

    @Test
    @DisplayName("命令回路打开但没有命令时，除版本号外输出与关闭时逐字节相同 / loop on without commands equals loop off except the version")
    void loopOnWithoutCommandsEqualsOff() throws Exception {
        List<DeviceRound> off = new ArrayList<>();
        try (org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<String, DeviceRound, DeviceRound> h =
                     new org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness<>(
                             new org.apache.flink.streaming.api.operators.KeyedProcessOperator<>(scaler()),
                             DeviceRound::getDevice, TypeInformation.of(String.class))) {
            h.open();
            for (DeviceRound r : rounds()) {
                h.processElement(new StreamRecord<>(r, r.getTs() * 1000L));
            }
            for (Object o : h.getOutput()) {
                if (o instanceof StreamRecord) {
                    off.add((DeviceRound) ((StreamRecord<?>) o).getValue());
                }
            }
        }
        List<DeviceRound> on;
        try (KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, AdaptCommand, DeviceRound> h = harness(adaptive())) {
            feed(h, rounds());
            on = outputs(h);
        }
        assertEquals(off.size(), on.size());
        int paramRound = -1;
        for (int i = 0; i < off.size(); i++) {
            DeviceRound a = off.get(i);
            DeviceRound b = on.get(i);
            assertNull(a.getVersion());
            assertEquals(a.isWarmup() ? null : Integer.valueOf(1), b.getVersion(), "round " + i);
            assertNull(b.getBackfill());
            // 冻结参数集（版本一）附在第一条点通道会接纳的冻结轮上（M6b 选项一）/ the freeze parameter set, once
            if (b.getParamSet() != null) {
                assertTrue(paramRound < 0, "参数集只附一次 / attached once");
                paramRound = i;
                assertEquals(1, b.getParamSet().version);
                assertEquals(0, b.missingCount());
                assertArrayEquals(RobustScalerFunction.normalize(b.getX(), b.getMissingMask(), b.getParamSet().median,
                        b.getParamSet().scale, b.getParamSet().bypass, ChannelTransform.defaultTable()), b.getXNorm(), 0.0);
                b.setParamSet(null);
            }
            b.setVersion(null);
            assertEquals(json(a), json(b), "round " + i);
            assertTrue(!json(a).contains("version") && !json(a).contains("backfill") && !json(a).contains("paramSet"));
        }
        assertTrue(paramRound >= 0, "冻结之后应附版本一的参数集 / the version-1 parameter set must be attached");
        for (int i = 0; i < paramRound; i++) {
            assertTrue(on.get(i).isWarmup() || on.get(i).missingCount() > 0, "附在第一条无缺失的冻结轮上 / first admitted frozen round");
        }
    }

    @Test
    @DisplayName("湿度与气压不随零级重估：两通道换回冻结参数与冻结参照直方图，其余三通道照常重估，回填按换回后的参数标准化")
    void keptChannelsKeepTheFreezeParametersAndReference() throws Exception {
        List<DeviceRound> in = rounds();
        long eff = T0 + 10L * 1000;
        boolean[] keep = {false, true, true, false, false};
        List<DeviceRound> a;
        try (KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, AdaptCommand, DeviceRound> h =
                     harness(adaptive().withKeepChannels(keep))) {
            h.processElement2(new StreamRecord<>(command(eff, 1)));
            feed(h, in);
            a = outputs(h);
        }
        com.leejean.m6.ParamSet v1 = null;
        com.leejean.m6.ParamSet v2 = null;
        com.leejean.m5a.RefHistogram[] freezeRef = null;
        com.leejean.m5a.RefHistogram[] newRef = null;
        DeviceRound bfRound = null;
        for (DeviceRound r : a) {
            if (r.getParamSet() != null && r.getParamSet().version == 1) {
                v1 = r.getParamSet();
            }
            if (r.getParamSet() != null && r.getParamSet().version == 2) {
                v2 = r.getParamSet();
                bfRound = r;
            }
            if (r.getRefHistograms() != null) {
                if (freezeRef == null) {
                    freezeRef = r.getRefHistograms();
                } else {
                    newRef = r.getRefHistograms();
                }
            }
        }
        assertNotNull(v1);
        assertNotNull(v2);
        assertNotNull(newRef);
        CompactCache c = new CompactCache(7 * 86400L);
        for (DeviceRound r : in) {
            if (r.getTs() < eff) {
                c.add(r.getTs(), r.getX(), CompactCache.maskOf(r.getMissingMask(), r.getCensoredMask(), r.getStuckMask()));
            }
        }
        AdaptiveScalerFunction.Level0 full = AdaptiveScalerFunction.level0(c, eff, D, WINDOW, 1e-9, false,
                ChannelTransform.defaultTable());
        for (int ch = 0; ch < Channels.N_DET; ch++) {
            double expMed = keep[ch] ? v1.median[ch] : full.cal.median[ch];
            double expScale = keep[ch] ? v1.scale[ch] : full.cal.scale[ch];
            assertEquals(expMed, v2.median[ch], 0.0, "median " + ch);
            assertEquals(expScale, v2.scale[ch], 0.0, "scale " + ch);
            double[] expEdges = (keep[ch] ? freezeRef[ch] : full.cal.hist[ch]).getEdges();
            assertArrayEquals(expEdges, newRef[ch].getEdges(), 0.0, "reference edges " + ch);
        }
        // 前半段与后半段湿度的位置不同，所以全部重估时湿度的中位数会变；这里核对它确实没变 / the test is sensitive
        assertTrue(full.cal.median[1] != v1.median[1]);
        // 回填与生效后的轮按换回后的参数标准化；湿度与气压的标准化值与版本一相同 / backfill and later rounds
        for (int k = 0; k < bfRound.getBackfill().ts.length; k++) {
            assertEquals(full.backfill.ts[k], bfRound.getBackfill().ts[k]);
        }
        for (DeviceRound r : a) {
            if (r.getTs() >= eff) {
                double[] e2 = RobustScalerFunction.normalize(r.getX(), r.getMissingMask(), v2.median, v2.scale, v2.bypass,
                        ChannelTransform.defaultTable());
                double[] e1 = RobustScalerFunction.normalize(r.getX(), r.getMissingMask(), v1.median, v1.scale, v1.bypass,
                        ChannelTransform.defaultTable());
                assertArrayEquals(e2, r.getXNorm(), 0.0);
                assertEquals(e1[1], r.getXNorm()[1], 0.0);
                assertEquals(e1[2], r.getXNorm()[2], 0.0);
            }
        }
        // 集合为全假时与原函数逐位相同 / an all-false set equals the original function
        AdaptiveScalerFunction.Level0 none = AdaptiveScalerFunction.level0(c, eff, D, WINDOW, 1e-9, false,
                ChannelTransform.defaultTable(), null, new boolean[Channels.N_DET]);
        assertArrayEquals(full.cal.median, none.cal.median, 0.0);
        assertArrayEquals(full.cal.scale, none.cal.scale, 0.0);
        // 冻结参数作为算子状态须能经 Kryo 往返（检查点）/ the kept freeze calibration survives a Kryo round trip
        KryoSerializer<RobustScalerFunction.Calibration> ks = new KryoSerializer<>(RobustScalerFunction.Calibration.class,
                new ExecutionConfig());
        DataOutputSerializer out = new DataOutputSerializer(4096);
        ks.serialize(full.cal, out);
        RobustScalerFunction.Calibration back = ks.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
        assertArrayEquals(full.cal.median, back.median, 0.0);
        assertArrayEquals(full.cal.scale, back.scale, 0.0);
        assertArrayEquals(full.cal.hist[1].getEdges(), back.hist[1].getEdges(), 0.0);
    }

    @Test
    @DisplayName("命令早到（轮触发）与命令在水位线之前到达（定时器触发），输出逐位相同 / round-triggered equals timer-triggered execution")
    void executionIsDeterministic() throws Exception {
        List<DeviceRound> in = rounds();
        long eff = T0 + 10L * 1000;                          // 第 1000 轮 / round 1000
        // 甲：命令在任何轮之前到达，由第一条不早于生效时刻的轮触发 / A: command first, executed by the first round ≥ eff
        List<DeviceRound> a;
        try (KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, AdaptCommand, DeviceRound> h = harness(adaptive())) {
            h.processElement2(new StreamRecord<>(command(eff, 1)));
            feed(h, in);
            a = outputs(h);
        }
        // 乙：命令在生效前一刻到达，水位线越过生效时刻时由定时器执行 / B: command just before, executed by the timer
        List<DeviceRound> b;
        try (KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, AdaptCommand, DeviceRound> h = harness(adaptive())) {
            feed(h, in.subList(0, 1000));
            h.processElement2(new StreamRecord<>(command(eff, 1)));
            h.processWatermark1(new Watermark(eff * 1000L));
            feed(h, in.subList(1000, in.size()));
            b = outputs(h);
        }
        assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            assertEquals(json(a.get(i)), json(b.get(i)), "round " + i);
        }
        // 版本号、参照直方图与回填附件的位置 / where version, histograms and backfill go
        int firstNew = -1;
        int firstBackfill = -1;
        for (int i = 0; i < a.size(); i++) {
            DeviceRound r = a.get(i);
            if (r.getTs() < eff) {
                assertEquals(r.isWarmup() ? null : Integer.valueOf(1), r.getVersion());
            } else {
                assertEquals(Integer.valueOf(2), r.getVersion());
                if (firstNew < 0) {
                    firstNew = i;
                }
                if (r.getBackfill() != null) {
                    assertTrue(firstBackfill < 0, "回填附件只挂一次 / backfill attached once");
                    firstBackfill = i;
                }
            }
        }
        assertNotNull(a.get(firstNew).getRefHistograms(), "第一条新版本轮带新参照直方图 / first new-version round carries histograms");
        DeviceRound bfRound = a.get(firstBackfill);
        assertEquals(0, bfRound.missingCount());
        for (int i = firstNew; i < firstBackfill; i++) {
            assertTrue(a.get(i).missingCount() > 0, "回填附件挂在第一条无缺失的新版本轮上 / first round without missing channels");
        }
        long[] ts = bfRound.getBackfill().ts;
        assertTrue(ts.length > 0);
        for (int k = 0; k < ts.length; k++) {
            assertTrue(ts[k] >= eff - WINDOW && ts[k] < eff);
            assertTrue(k == 0 || ts[k] > ts[k - 1]);
        }
        // 新参数等于缓存上的零级纯函数 / the new parameters equal the level-0 function on the same rounds
        CompactCache c = new CompactCache(7 * 86400L);
        for (DeviceRound r : in) {
            if (r.getTs() < eff) {
                c.add(r.getTs(), r.getX(), CompactCache.maskOf(r.getMissingMask(), r.getCensoredMask(), r.getStuckMask()));
            }
        }
        AdaptiveScalerFunction.Level0 l0 = AdaptiveScalerFunction.level0(c, eff, D, WINDOW, 1e-9, false,
                ChannelTransform.defaultTable());
        DeviceRound after = a.get(firstNew);
        double[] expect = RobustScalerFunction.normalize(after.getX(), after.getMissingMask(), l0.cal.median, l0.cal.scale,
                l0.cal.bypass, ChannelTransform.defaultTable());
        assertArrayEquals(expect, after.getXNorm(), 0.0);
        assertEquals(10, l0.guardExcluded, "重估窗口含守卫游程的后十轮 / the window holds the last ten guard rounds");
        assertArrayEquals(l0.backfill.ts, ts);
        // 版本二的参数集与回填附件挂在同一条轮上，等于零级纯函数的新参数（M6b 选项一）/ the version-2 parameter set
        assertEquals(2, bfRound.getParamSet().version);
        assertArrayEquals(l0.cal.median, bfRound.getParamSet().median, 0.0);
        assertArrayEquals(l0.cal.scale, bfRound.getParamSet().scale, 0.0);
        for (int i = 0; i < a.size(); i++) {
            assertTrue(a.get(i).getParamSet() == null || i == firstBackfill || a.get(i).getParamSet().version == 1);
        }
    }

    @Test
    @DisplayName("一级与二级命令：数据接入不重估、版本不变，只把命令附在生效后第一条点通道会接纳的轮上转发 / levels 1 and 2 are forwarded")
    void levelOneAndTwoAreForwarded() throws Exception {
        List<DeviceRound> plain;
        try (KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, AdaptCommand, DeviceRound> h = harness(adaptive())) {
            feed(h, rounds());
            plain = outputs(h);
        }
        long eff = T0 + 10L * 1000;
        for (int level = 1; level <= 2; level++) {
            AdaptCommand cmd = command(eff, 1);
            cmd.level = level;
            List<DeviceRound> fwd;
            try (KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, AdaptCommand, DeviceRound> h = harness(adaptive())) {
                h.processElement2(new StreamRecord<>(cmd));
                feed(h, rounds());
                fwd = outputs(h);
            }
            assertEquals(plain.size(), fwd.size());
            int carrier = -1;
            for (int i = 0; i < fwd.size(); i++) {
                DeviceRound r = fwd.get(i);
                if (r.getContextCommand() != null) {
                    assertTrue(carrier < 0, "只转发一次 / forwarded once");
                    carrier = i;
                    assertEquals(level, r.getContextCommand().level);
                    assertTrue(r.getTs() >= eff && r.missingCount() == 0);
                    r.setContextCommand(null);
                }
                assertEquals(json(plain.get(i)), json(r), "除转发的命令外逐字节相同，版本号不变 / round " + i);
            }
            assertTrue(carrier >= 0);
            for (int i = 0; i < carrier; i++) {
                assertTrue(fwd.get(i).getTs() < eff || fwd.get(i).missingCount() > 0, "第一条生效后的入口轮 / first admitted round");
            }
        }
    }

    @Test
    @DisplayName("迟到命令不执行：生效时刻不晚于水位线，或已处理过生效时刻之后的轮 / late commands are not executed")
    void lateCommandsAreNotExecuted() throws Exception {
        List<DeviceRound> in = rounds();
        long eff = T0 + 10L * 1000;
        for (int stop : new int[]{1001, 1000}) {
            try (KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, AdaptCommand, DeviceRound> h = harness(adaptive())) {
                for (DeviceRound r : in.subList(0, stop)) {
                    h.processElement1(new StreamRecord<>(r, r.getTs() * 1000L));
                }
                if (stop == 1000) {
                    h.processWatermark1(new Watermark(eff * 1000L));   // 水位线已到生效时刻 / watermark at eff
                }
                h.processElement2(new StreamRecord<>(command(eff, 1)));
                feed(h, in.subList(stop, in.size()));
                for (DeviceRound r : outputs(h)) {
                    if (!r.isWarmup()) {
                        assertEquals(Integer.valueOf(1), r.getVersion(), "stop " + stop + " ts " + r.getTs());
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("零级纯函数：按时间取窗、剔除守卫轮、缺失与删失不计入，与标定函数一致 / the level-0 function")
    void level0Function() {
        CompactCache c = new CompactCache(7 * 86400L);
        List<List<Double>> expect = new ArrayList<>();
        for (int ch = 0; ch < 5; ch++) {
            expect.add(new ArrayList<>());
        }
        long eff = T0 + 1000;
        ChannelTransform[] tf = ChannelTransform.defaultTable();
        int window = 0;
        int guard = 0;
        for (int i = 0; i < 200; i++) {
            long t = T0 + 10L * i;
            double[] x = {20 + i * 0.01, 40 - i * 0.02, 101000 + i, 700 + (i % 7), i % 5 == 0 ? 65536.0 : i};
            boolean[] miss = new boolean[5];
            miss[1] = i % 11 == 0;
            boolean[] cens = new boolean[5];
            cens[4] = x[4] == 65536.0;
            boolean[] stuck = new boolean[5];
            stuck[2] = i % 13 == 0;
            c.add(t, x, CompactCache.maskOf(miss, cens, stuck));
            if (t >= eff - D && t < eff) {
                window++;
                if (stuck[2]) {
                    guard++;
                    continue;
                }
                for (int ch = 0; ch < 5; ch++) {
                    if (!miss[ch] && !(ch == 4 && cens[4])) {
                        expect.get(ch).add(tf[ch].apply(x[ch]));
                    }
                }
            }
        }
        AdaptiveScalerFunction.Level0 r = AdaptiveScalerFunction.level0(c, eff, D, 300, 1e-9, false, tf);
        RobustScalerFunction.Calibration cal = RobustScalerFunction.calibrate(expect, 1e-9, false);
        assertEquals(window, r.windowRounds);
        assertEquals(guard, r.guardExcluded);
        assertArrayEquals(cal.median, r.cal.median, 0.0);
        assertArrayEquals(cal.scale, r.cal.scale, 0.0);
        for (int ch = 0; ch < 5; ch++) {
            assertArrayEquals(cal.hist[ch].getEdges(), r.cal.hist[ch].getEdges(), 0.0);
        }
        for (long t : r.backfill.ts) {
            assertTrue(t >= eff - 300 && t < eff && (t - T0) / 10 % 11 != 0, "回填只含无缺失的轮 / no missing rounds");
        }
        assertEquals(30 - 3, r.backfill.size());           // 300 秒 30 轮，其中 3 轮缺失 / 30 rounds, 3 with a missing channel
    }

    @Test
    @DisplayName("紧凑缓存：按时间淘汰；检查点只写已用的行，读回逐位相同 / compact cache eviction and serialization")
    void compactCache() throws Exception {
        CompactCache c = new CompactCache(3600);
        for (int i = 0; i < 2000; i++) {
            c.add(T0 + 10L * i, new double[]{i, -i, i * 0.5, 1.0 / (i + 1), Math.PI * i}, i & 0x7fff);
        }
        assertEquals(360, c.size());                        // 最近一小时 / the last hour
        assertEquals(T0 + 10L * 1640, c.ts(0));
        KryoSerializer<CompactCache> ks = new KryoSerializer<>(CompactCache.class, new ExecutionConfig());
        DataOutputSerializer out = new DataOutputSerializer(1024);
        ks.serialize(c, out);
        assertTrue(out.length() < 360 * 52 + 64, "只写已用的行 / only used rows: " + out.length());
        CompactCache back = ks.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
        CompactCache copy = ks.copy(c);
        for (CompactCache d : Arrays.asList(back, copy)) {
            assertEquals(c.size(), d.size());
            for (int k = 0; k < c.size(); k++) {
                assertEquals(c.ts(k), d.ts(k));
                assertEquals(c.mask(k), d.mask(k));
                for (int ch = 0; ch < 5; ch++) {
                    assertEquals(Double.doubleToRawLongBits(c.x(k, ch)), Double.doubleToRawLongBits(d.x(k, ch)));
                }
            }
        }
    }

    @Test
    @DisplayName("命令解析：聚合模块的命令 JSON / parsing the M7 command JSON")
    void parseCommand() throws Exception {
        String line = "{\"device\":\"A\",\"day\":\"2022-03-27\",\"decidedAt\":1648422000,\"effectiveAt\":1648508400,\"seq\":1,"
                + "\"level\":0,\"scope\":\"局部\",\"label\":\"漂移\",\"triggers\":\"Gas\",\"type\":\"command\"}";
        AdaptCommand c = AdaptCommand.parse(line);
        assertEquals("A", c.device);
        assertEquals(1648508400L, c.effectiveAt);
        assertEquals(1, c.seq);
        assertEquals(line, c.raw);
        ObjectNode n = (ObjectNode) OM.readTree(line);
        n.remove("seq");
        try {
            AdaptCommand.parse(n.toString());
            throw new AssertionError("缺少序号应当失败 / missing seq must fail");
        } catch (java.io.IOException expected) {
            assertTrue(expected.getMessage().contains("seq"));
        }
    }
}
