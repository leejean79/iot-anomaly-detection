package com.leejean.m5a;

import com.leejean.m1.ChannelTransform;
import com.leejean.m1.Channels;
import com.leejean.m1.DeviceRound;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m2.ScoreEvent;
import com.leejean.m5.DriftEvent;
import com.leejean.m5.OptWinTables;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 数据层核心 {@link M5aCore} 与在线算子 {@link M5aFunction} 的测试。对照数据由 {@code m5a/make_core_fixtures.py} 用 Python 离线实现生成：
 * 一台设备 13 天的合成轮（跨夏令时切换日，含标定日、缺轮日、守卫打标日、两段漂移）与离群记录（含之后滑动步的重复）。
 * Tests of M5aCore and M5aFunction against fixtures produced by the Python offline implementation.
 *
 * <p>检查三件事：参照直方图与 numpy 逐位相同；Java 离线运行（{@link M5aOfflineRunner#runDevice}）的日记录、状态转移与爆发与 Python
 * 相同（显著性允许 1e-12 的相对差，其余逐位）；在线算子（含中途检查点恢复）的输出与 Java 离线运行逐条相同。
 * Checks: reference histograms equal numpy; the Java offline run equals Python; the online operator (with a restore) equals it.
 */
class M5aCoreTest {

    private static final String DEV = "E";
    private static final OutputTag<MonitoringSnapshot> TAG = new OutputTag<MonitoringSnapshot>("m5a-rec") { };
    /**
     * 光照在 log1p 域：Java 的 Math.log1p 与 numpy 的 log1p 在少数输入上相差一个最低有效位，分箱边界与分位数随之可差一位，偏移与宽度倍数
     * 因相减可差到 1e-15 量级；分箱 D 不受影响（参照与当日用同一个变换，相等关系不变）。
     * Light is in the log1p domain; Java and numpy log1p differ by one ulp on a few inputs, so its quantiles may too.
     */
    private static final double LIGHT_TOL = 1e-12;
    private static final M5aCore.Params P = new M5aCore.Params(new double[]{0.38, 0.73, 0.84, 0.43, 0.51}, 0.01, 0.2, 0.2, 10, 60);

    private static List<DeviceRound> rounds;
    private static List<long[]> outliers;
    private static long finalTime;
    private static OptWinTables tables;

    private static List<String[]> csv(String name) throws Exception {
        List<String[]> out = new ArrayList<>();
        try (InputStream raw = M5aCoreTest.class.getResourceAsStream("/m5a/" + name);
             InputStream in = name.endsWith(".gz") ? new java.util.zip.GZIPInputStream(raw) : raw;
             BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                out.add(line.split(",", -1));
            }
        }
        return out;
    }

    @BeforeAll
    static void load() throws Exception {
        rounds = new ArrayList<>();
        for (String[] f : csv("core_rounds.csv.gz")) {
            DeviceRound r = new DeviceRound();
            r.setDevice(DEV);
            r.setTs(Long.parseLong(f[0]));
            double[] x = new double[5];
            boolean[] miss = new boolean[5];
            boolean[] cens = new boolean[5];
            boolean[] stuck = new boolean[5];
            for (int c = 0; c < 5; c++) {
                x[c] = Double.parseDouble(f[1 + c]);
                miss[c] = f[6 + c].equals("1");
            }
            cens[Channels.LIGHT_INDEX] = f[11].equals("1");
            stuck[0] = f[12].equals("1");
            r.setX(x);
            r.setMissingMask(miss);
            r.setCensoredMask(cens);
            r.setStuckMask(stuck);
            r.setWarmup(f[13].equals("1"));
            rounds.add(r);
        }
        // 参照直方图：与 RobustScalerFunction 冻结时相同的取值（变换域，排除缺失与光照删失），挂到冻结后的第一条轮
        // reference histograms built as at freeze, attached to the first frozen round
        ChannelTransform[] tf = ChannelTransform.defaultTable();
        RefHistogram[] ref = new RefHistogram[5];
        int first = -1;
        for (int c = 0; c < 5; c++) {
            List<Double> v = new ArrayList<>();
            for (int i = 0; i < rounds.size(); i++) {
                DeviceRound r = rounds.get(i);
                if (!r.isWarmup()) {
                    first = first < 0 ? i : first;
                    break;
                }
                if (!r.getMissingMask()[c] && !(c == Channels.LIGHT_INDEX && r.getCensoredMask()[c])) {
                    v.add(tf[c].apply(r.getX()[c]));
                }
            }
            Collections.sort(v);
            ref[c] = RefHistogram.build(v.stream().mapToDouble(Double::doubleValue).toArray(), 400);
        }
        rounds.get(first).setRefHistograms(ref);
        outliers = new ArrayList<>();
        for (String[] f : csv("core_outliers.csv")) {
            outliers.add(new long[]{Long.parseLong(f[0]), Long.parseLong(f[1])});
        }
        finalTime = rounds.get(rounds.size() - 1).getTs() - 1;
        tables = OptWinTables.build(0.99, 0.5, 2016);
    }

    private static RefHistogram[] refs() {
        for (DeviceRound r : rounds) {
            if (r.getRefHistograms() != null) {
                return r.getRefHistograms();
            }
        }
        throw new AssertionError("no reference");
    }

    @Test
    @DisplayName("参照直方图的分箱边界与 P10、中位数、P90 与 numpy 逐位相同 / reference histograms equal numpy")
    void referenceEqualsNumpy() throws Exception {
        RefHistogram[] ref = refs();
        for (String[] f : csv("core_ref.csv")) {
            int c = Integer.parseInt(f[0]);
            assertEquals(Long.parseLong(f[1]), ref[c].getM(), "m " + c);
            double tol = c == Channels.LIGHT_INDEX ? LIGHT_TOL : 0.0;
            assertEquals(Double.parseDouble(f[2]), ref[c].getP10(), tol, "p10 " + c);
            assertEquals(Double.parseDouble(f[3]), ref[c].getMedian(), tol, "median " + c);
            assertEquals(Double.parseDouble(f[4]), ref[c].getP90(), tol, "p90 " + c);
            double[] e = Arrays.stream(f[5].split(" ")).mapToDouble(Double::parseDouble).toArray();
            assertArrayEquals(e, ref[c].getEdges(), tol, "edges " + c);
        }
    }

    @Test
    @DisplayName("带参照直方图的轮经 JSON（与 m1-out 序列化相同的 ObjectMapper）往返后逐位不变 / JSON round trip of the reference")
    void referenceSurvivesJson() throws Exception {
        DeviceRound r = null;
        for (DeviceRound x : rounds) {
            if (x.getRefHistograms() != null) {
                r = x;
                break;
            }
        }
        com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
        DeviceRound back = om.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readValue(om.writeValueAsBytes(r), DeviceRound.class);
        for (int c = 0; c < 5; c++) {
            RefHistogram a = r.getRefHistograms()[c];
            RefHistogram b = back.getRefHistograms()[c];
            assertArrayEquals(a.getEdges(), b.getEdges(), 0.0);
            assertArrayEquals(a.getCells(), b.getCells());
            assertEquals(a.getM(), b.getM());
            assertEquals(a.getP10(), b.getP10(), 0.0);
            assertEquals(a.getMedian(), b.getMedian(), 0.0);
            assertEquals(a.getP90(), b.getP90(), 0.0);
        }
    }

    private static void offline(List<DriftEvent> ev, List<MonitoringSnapshot> rec) {
        List<long[]> os = new ArrayList<>();
        for (long[] o : outliers) {
            os.add(o.clone());
        }
        M5aOfflineRunner.runDevice(DEV, new ArrayList<>(rounds), os, finalTime, P, tables, ev, rec);
    }

    /** 显著性允许 1e-12 的相对差（级数求和的次序不同）；其余逐位，光照的分位数类字段见 LIGHT_TOL / p relative 1e-12, else exact. */
    private static void assertNum(String exp, String act, boolean pValue, boolean light, String what) {
        double e = Double.parseDouble(exp);
        double a = Double.parseDouble(act);
        assertEquals(e, a, pValue ? Math.abs(e) * 1e-12 : (light ? LIGHT_TOL : 0.0), what);
    }

    @Test
    @DisplayName("日记录、状态转移与爆发与 Python 离线实现相同 / records, transitions and bursts equal Python")
    void offlineEqualsPython() throws Exception {
        List<DriftEvent> ev = new ArrayList<>();
        List<MonitoringSnapshot> rec = new ArrayList<>();
        offline(ev, rec);

        List<String[]> expRec = csv("core_records.csv");
        assertEquals(expRec.size(), rec.size(), "record count");
        int evaluable = 0;
        for (int i = 0; i < rec.size(); i++) {
            String[] e = expRec.get(i);
            String[] a = M5aOfflineRunner.recordCsv(rec.get(i)).split(",", -1);   // 首列为设备 / leading device column
            String at = e[0] + "/" + e[1];
            assertEquals(DEV, a[0]);
            for (int k = 0; k < 8; k++) {
                assertEquals(e[k], a[k + 1], at + " col " + k);
            }
            for (int k = 8; k < 16; k++) {
                assertNum(e[k], a[k + 1], k == 9, k >= 10 && k <= 14 && e[1].equals("Light"), at + " col " + k);
            }
            assertEquals(e[16], a[17], at + " deviated");
            evaluable += e[2].equals("1") ? 1 : 0;
        }
        assertTrue(evaluable > 0);

        List<String[]> expEv = new ArrayList<>();
        for (String[] e : csv("core_events.csv")) {
            String[] w = new String[e.length + 1];                // 补上设备列，与 toDataCsv 对齐 / align with toDataCsv
            w[0] = DEV;
            System.arraycopy(e, 0, w, 1, e.length);
            expEv.add(w);
        }
        List<String[]> act = new ArrayList<>();
        for (DriftEvent d : ev) {
            act.add(d.toDataCsv().split(",", -1));
        }
        java.util.Comparator<String[]> order = java.util.Comparator.<String[], Long>comparing(x -> Long.parseLong(x[1]))
                .thenComparing(x -> x[2]).thenComparing(x -> x[3]);
        expEv.sort(order);
        act.sort(order);
        assertEquals(expEv.size(), act.size(), "event count");
        for (int i = 0; i < act.size(); i++) {
            String[] e = expEv.get(i);
            String[] a = act.get(i);
            String at = e[1] + "/" + e[2] + "/" + e[3];
            for (int k : new int[]{0, 1, 2, 3, 4, 6, 8, 9}) {
                assertEquals(e[k], a[k], at + " col " + k);
            }
            for (int k : new int[]{5, 7, 10, 11, 12, 13}) {
                assertNum(e[k], a[k], k == 11, k >= 12 && e[3].equals("Light"), at + " col " + k);
            }
        }
    }

    @Test
    @DisplayName("命令行入口的流式运行（读 m1-out 与 scores 的 JSON 转储）与逐台运行相同 / file-based run equals runDevice")
    void fileRunEqualsRunDevice() throws Exception {
        com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("m5a-run");
        List<String> rl = new ArrayList<>();
        for (DeviceRound r : rounds) {
            rl.add(om.writeValueAsString(r));
        }
        List<String> sl = new ArrayList<>();
        for (long[] o : outliers) {
            sl.add(om.writeValueAsString(new ScoreEvent(DEV, o[0], o[1])));
        }
        java.nio.file.Files.write(dir.resolve("m1out.jsonl"), rl, StandardCharsets.UTF_8);
        java.nio.file.Files.write(dir.resolve("scores.jsonl"), sl, StandardCharsets.UTF_8);
        List<DriftEvent> ev = new ArrayList<>();
        List<MonitoringSnapshot> rec = new ArrayList<>();
        M5aOfflineRunner.run(dir.resolve("m1out.jsonl"), dir.resolve("scores.jsonl"), P, tables, ev, rec);
        List<DriftEvent> ev0 = new ArrayList<>();
        List<MonitoringSnapshot> rec0 = new ArrayList<>();
        offline(ev0, rec0);
        List<String> a = new ArrayList<>();
        List<String> b = new ArrayList<>();
        ev.forEach(e -> a.add(e.toDataCsv()));
        ev0.forEach(e -> b.add(e.toDataCsv()));
        rec.forEach(r -> a.add(M5aOfflineRunner.recordCsv(r)));
        rec0.forEach(r -> b.add(M5aOfflineRunner.recordCsv(r)));
        Collections.sort(a);
        Collections.sort(b);
        assertEquals(b, a);
    }

    // ================================================================ 在线 / online
    private static KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, ScoreEvent, DriftEvent> harness() throws Exception {
        return new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(new M5aFunction(P, 0.99, 0.5, 2016, TAG)),
                DeviceRound::getDevice, ScoreEvent::getDevice, TypeInformation.of(String.class));
    }

    /** 按事件时间推进：每条轮之前先送入窗口末不晚于它的离群记录，再推进水位线到轮时刻前一毫秒 / feed like event time. */
    private static int feed(KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, ScoreEvent, DriftEvent> h,
                            List<long[]> os, int oi, int from, int to) throws Exception {
        for (int i = from; i < to; i++) {
            DeviceRound r = rounds.get(i);
            while (oi < os.size() && os.get(oi)[1] <= r.getTs()) {
                h.processElement2(new StreamRecord<>(new ScoreEvent(DEV, os.get(oi)[0], os.get(oi)[1]), os.get(oi)[1] * 1000L - 1));
                oi++;
            }
            h.processBothWatermarks(new Watermark(r.getTs() * 1000L - 1));
            h.processElement1(new StreamRecord<>(r, r.getTs() * 1000L));
        }
        return oi;
    }

    /** 输出经 JSON（与作业的 Kafka 序列化相同）写盘，再用运行器的转换模式读回，模拟集群上的转储与转换 / via JSON, as on the cluster. */
    private static void collect(KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, ScoreEvent, DriftEvent> h,
                                List<String> ev, List<String> rec) throws Exception {
        com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("m5a");
        java.nio.file.Path drift = dir.resolve("drift.jsonl");
        java.nio.file.Path mon = dir.resolve("monitoring.jsonl");
        List<String> dl = new ArrayList<>();
        List<String> ml = new ArrayList<>();
        for (Object o : h.getOutput()) {
            if (o instanceof StreamRecord) {
                dl.add(om.writeValueAsString(((StreamRecord<?>) o).getValue()));
            }
        }
        for (StreamRecord<MonitoringSnapshot> s : h.getSideOutput(TAG)) {
            ml.add(om.writeValueAsString(s.getValue()));
        }
        java.nio.file.Files.write(drift, dl, StandardCharsets.UTF_8);
        java.nio.file.Files.write(mon, ml, StandardCharsets.UTF_8);
        List<DriftEvent> e = new ArrayList<>();
        List<MonitoringSnapshot> r = new ArrayList<>();
        M5aOfflineRunner.convertOnline(drift, mon, e, r);
        for (DriftEvent d : e) {
            ev.add(d.toDataCsv());
        }
        for (MonitoringSnapshot s : r) {
            rec.add(M5aOfflineRunner.recordCsv(s));
        }
    }

    @Test
    @DisplayName("在线算子（中途检查点恢复）与 Java 离线运行逐条相同 / online with a restore equals the offline run")
    void onlineEqualsOffline() throws Exception {
        List<DriftEvent> ev = new ArrayList<>();
        List<MonitoringSnapshot> rec = new ArrayList<>();
        offline(ev, rec);
        List<String> expEv = new ArrayList<>();
        List<String> expRec = new ArrayList<>();
        for (DriftEvent e : ev) {
            expEv.add(e.toDataCsv());
        }
        for (MonitoringSnapshot s : rec) {
            expRec.add(M5aOfflineRunner.recordCsv(s));
        }

        List<long[]> os = new ArrayList<>(outliers);
        os.sort((a, b) -> Long.compare(a[1], b[1]));
        int half = rounds.size() / 2 + 777;                  // 某个当地日中间、十分钟中间 / mid-day, mid-bin
        List<String> actEv = new ArrayList<>();
        List<String> actRec = new ArrayList<>();
        OperatorSubtaskState snap;
        int oi;
        try (KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, ScoreEvent, DriftEvent> h = harness()) {
            h.open();
            oi = feed(h, os, 0, 0, half);
            collect(h, actEv, actRec);
            snap = h.snapshot(1L, 1L);
        }
        try (KeyedTwoInputStreamOperatorTestHarness<String, DeviceRound, ScoreEvent, DriftEvent> h = harness()) {
            h.setup();
            h.initializeState(snap);
            h.open();
            oi = feed(h, os, oi, half, rounds.size());
            while (oi < os.size()) {
                h.processElement2(new StreamRecord<>(new ScoreEvent(DEV, os.get(oi)[0], os.get(oi)[1]), os.get(oi)[1] * 1000L - 1));
                oi++;
            }
            h.processBothWatermarks(new Watermark(finalTime * 1000L + 999));
            collect(h, actEv, actRec);
        }
        assertTrue(expEv.size() >= 10 && expRec.size() >= 50);
        Collections.sort(expEv);
        Collections.sort(actEv);
        Collections.sort(expRec);
        Collections.sort(actRec);
        assertEquals(expRec, actRec);
        assertEquals(expEv, actEv);
    }
}
