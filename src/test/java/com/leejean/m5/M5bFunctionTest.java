package com.leejean.m5;

import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m3.M3ScoreRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 在线算子 {@link M5bFunction} 的测试：十分钟与小时粒度下，在线发出的事件与离线逻辑逐条相同；检查点恢复后继续运行与不中断的
 * 一次相同；监测快照按处理时间每 60 秒发出、windowEnd 为 −2。
 * Online operator tests: events equal the offline logic at both granularities, survive a checkpoint/restore, and the
 * monitoring snapshot is emitted every 60 s with windowEnd = −2.
 */
class M5bFunctionTest {

    private static final long START = 1_648_000_200L;      // 某个整点之后 / some time after a full hour
    private static final OutputTag<MonitoringSnapshot> TAG = new OutputTag<MonitoringSnapshot>("m5b-mon") { };

    private static List<Double> meanShift() throws Exception {
        List<Double> v = new ArrayList<>();
        try (InputStream in = M5bFunctionTest.class.getResourceAsStream("/m5b/synthetic_streams.csv");
             BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split(",");
                if (f[0].equals("mean_shift")) {
                    v.add(Double.parseDouble(f[2]));
                }
            }
        }
        return v;
    }

    private static M3ScoreRecord rec(long windowEnd, double score) {
        return new M3ScoreRecord("E", windowEnd, score, 0.0, 0.0, new double[5], false, 60, 60);
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, M3ScoreRecord, DriftEvent> harness(
            M5bFunction.Granularity g, int wMax) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new M5bFunction(0.99, 0.5, wMax, 36, g, TAG)),
                M3ScoreRecord::getDevice, TypeInformation.of(String.class));
    }

    private static List<String> events(KeyedOneInputStreamOperatorTestHarness<String, M3ScoreRecord, DriftEvent> h) {
        List<String> out = new ArrayList<>();
        for (Object o : h.getOutput()) {
            if (o instanceof StreamRecord) {
                out.add(((DriftEvent) ((StreamRecord<?>) o).getValue()).toCsv());
            }
        }
        return out;
    }

    /** 离线逻辑：与 OptWinOfflineRunner 相同 / offline logic, as the runner. */
    private static List<String> offline(List<long[]> ts, List<Double> xs, int wMax) {
        OptWinTables t = OptWinTables.build(0.99, 0.5, wMax);
        OptWinDetector d = new OptWinDetector(wMax);
        DriftGate g = new DriftGate(36, 0.5);
        List<String> out = new ArrayList<>();
        for (int i = 0; i < xs.size(); i++) {
            for (DriftEvent e : g.onElement("E", ts.get(i)[0], xs.get(i), d.update(xs.get(i), t))) {
                out.add(e.toCsv());
            }
        }
        return out;
    }

    @Test
    @DisplayName("十分钟粒度：在线事件与离线逻辑逐条相同；中途做检查点并恢复，事件不变")
    void tenMinutesEqualsOfflineAndSurvivesRestore() throws Exception {
        List<Double> xs = meanShift();
        List<long[]> ts = new ArrayList<>();
        for (int i = 0; i < xs.size(); i++) {
            ts.add(new long[]{START + 600L * i});
        }
        List<String> want = offline(ts, xs, 2016);
        assertFalse(want.isEmpty(), "均值漂移流上应有事件");

        List<String> got = new ArrayList<>();
        OperatorSubtaskState snap;
        try (KeyedOneInputStreamOperatorTestHarness<String, M3ScoreRecord, DriftEvent> h =
                     harness(M5bFunction.Granularity.TEN_MINUTES, 2016)) {
            h.open();
            for (int i = 0; i < 610; i++) {
                h.processElement(rec(ts.get(i)[0], xs.get(i)), ts.get(i)[0] * 1000L);
            }
            got.addAll(events(h));
            snap = h.snapshot(1L, 1L);
        }
        try (KeyedOneInputStreamOperatorTestHarness<String, M3ScoreRecord, DriftEvent> h =
                     harness(M5bFunction.Granularity.TEN_MINUTES, 2016)) {
            h.initializeState(snap);
            h.open();
            for (int i = 610; i < xs.size(); i++) {
                h.processElement(rec(ts.get(i)[0], xs.get(i)), ts.get(i)[0] * 1000L);
            }
            got.addAll(events(h));
        }
        assertEquals(want, got);
    }

    @Test
    @DisplayName("小时粒度：按世界协调时整点小时取中位数后的在线事件与离线分组逐条相同；最后一个小时由事件时间定时器兜底关闭")
    void hourlyEqualsOfflineGrouping() throws Exception {
        List<Double> xs = meanShift();
        Map<Long, List<Double>> hours = new LinkedHashMap<>();
        try (KeyedOneInputStreamOperatorTestHarness<String, M3ScoreRecord, DriftEvent> h =
                     harness(M5bFunction.Granularity.HOURLY, 336)) {
            h.open();
            for (int i = 0; i < xs.size(); i++) {
                long we = START + 600L * i;
                hours.computeIfAbsent(we / 3600 * 3600, z -> new ArrayList<>()).add(xs.get(i));
                h.processElement(rec(we, xs.get(i)), we * 1000L);
            }
            long last = START + 600L * (xs.size() - 1);
            h.processWatermark(new Watermark((last / 3600 * 3600 + 7200) * 1000L));
            List<long[]> ts = new ArrayList<>();
            List<Double> med = new ArrayList<>();
            for (Map.Entry<Long, List<Double>> e : hours.entrySet()) {
                double[] s = e.getValue().stream().mapToDouble(Double::doubleValue).toArray();
                Arrays.sort(s);
                int n = s.length;
                ts.add(new long[]{e.getKey()});
                med.add(n % 2 == 1 ? s[n / 2] : (s[n / 2 - 1] + s[n / 2]) / 2.0);
            }
            assertEquals(offline(ts, med, 336), events(h));
        }
    }

    @Test
    @DisplayName("监测快照：处理时间每 60 秒发出一条，windowEnd 为 −2，带当前窗长")
    void snapshotEverySixtySeconds() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, M3ScoreRecord, DriftEvent> h =
                     harness(M5bFunction.Granularity.TEN_MINUTES, 2016)) {
            h.open();
            h.setProcessingTime(0L);
            for (int i = 0; i < 40; i++) {
                h.processElement(rec(START + 600L * i, 0.1 * (i % 3)), (START + 600L * i) * 1000L);
            }
            h.setProcessingTime(60_000L);
            h.setProcessingTime(120_000L);
            List<MonitoringSnapshot> snaps = new ArrayList<>();
            h.getSideOutput(TAG).forEach(r -> snaps.add(r.getValue()));
            assertEquals(2, snaps.size());
            MonitoringSnapshot s = snaps.get(1);
            assertEquals(M5bFunction.M5B_SNAPSHOT_WINDOW_END, s.getWindowEnd());
            assertEquals("E", s.getDevice());
            assertEquals(40, s.getM5bWindowLen());
            assertEquals(START + 600L * 39, s.getTs());
            assertTrue(s.getM5bTSkippedConstant() >= 0);
        }
    }
}
