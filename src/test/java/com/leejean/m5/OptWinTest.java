package com.leejean.m5;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M5b 检测器与持续性核对的单元测试（交接文档第三节「测试」、验收项 V-M5b-1）：常量表与 scipy 逐个穷举一致；合成流上的
 * 事件与 Python 参考实现逐位一致；独立同分布的平稳合成流上候选率不超过论文上界 1−δ；核对的三种状态转移；序列化恢复后
 * 继续运行与不中断的一次相同。参照数据由 {@code m5b/make_test_resources.py} 生成。
 * Unit tests: tables vs exhaustive scipy, events vs the Python reference, false-alarm rate on an i.i.d. stream,
 * gate transitions, and serialization round trip. Reference data come from m5b/make_test_resources.py.
 */
class OptWinTest {

    private static OptWinTables tables;

    @BeforeAll
    static void build() {
        tables = OptWinTables.build(0.99, 0.5, 2016);
    }

    private static List<String[]> csv(String resource) throws Exception {
        List<String[]> rows = new ArrayList<>();
        try (InputStream in = OptWinTest.class.getResourceAsStream(resource);
             BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                rows.add(line.split(","));
            }
        }
        return rows;
    }

    private static List<DriftEvent> runSynthetic(List<String[]> rows) {
        Map<String, OptWinDetector> det = new LinkedHashMap<>();
        Map<String, DriftGate> gate = new LinkedHashMap<>();
        List<DriftEvent> out = new ArrayList<>();
        for (String[] r : rows) {
            double x = Double.parseDouble(r[2]);
            OptWinDetector d = det.computeIfAbsent(r[0], z -> new OptWinDetector(2016));
            DriftGate g = gate.computeIfAbsent(r[0], z -> new DriftGate(36, 0.5));
            out.addAll(g.onElement(r[0], Long.parseLong(r[1]), x, d.update(x, tables)));
        }
        return out;
    }

    @Test
    @DisplayName("常量表：切点与 scipy 逐个穷举完全相同，F 与 t 临界值相对误差不超过一百万分之一")
    void tablesMatchScipy() throws Exception {
        List<String[]> ref = csv("/m5b/scipy_thesis_0.99_0.5_2016.csv");
        assertEquals(2016 - 30 + 1, ref.size());
        for (String[] r : ref) {
            int w = Integer.parseInt(r[0]);
            assertEquals(Integer.parseInt(r[1]), tables.cut[w], "窗长 " + w + " 的切点");
            assertEquals("1".equals(r[2]), tables.rootFound[w], "窗长 " + w + " 是否有根");
            double f = Double.parseDouble(r[3]);
            double t = Double.parseDouble(r[4]);
            assertTrue(Math.abs(tables.fCrit[w] - f) / f <= 1e-6, "窗长 " + w + " 的 F 临界值");
            assertTrue(Math.abs(tables.tCrit[w] - t) / t <= 1e-6, "窗长 " + w + " 的 t 临界值");
        }
    }

    @Test
    @DisplayName("合成流：均值漂移、方差漂移与平稳流上的事件与 Python 参考实现逐位相同")
    void syntheticEventsMatchPythonReference() throws Exception {
        List<DriftEvent> got = runSynthetic(csv("/m5b/synthetic_streams.csv"));
        List<String[]> want = csv("/m5b/synthetic_events.csv");
        assertEquals(want.size(), got.size(), "事件数");
        for (int i = 0; i < want.size(); i++) {
            String[] w = want.get(i);
            String[] g = got.get(i).toCsv().split(",");
            for (int j = 0; j < w.length; j++) {
                if (j == 0 || j == 2 || j == 3) {
                    assertEquals(w[j], g[j], "第 " + i + " 条事件第 " + j + " 列");
                } else {
                    assertEquals(Double.parseDouble(w[j]), Double.parseDouble(g[j]), 0.0, "第 " + i + " 条事件第 " + j + " 列");
                }
            }
        }
        DriftEvent firstMean = got.stream().filter(e -> e.device.equals("mean_shift")).findFirst().orElse(null);
        assertNotNull(firstMean);
        assertTrue(firstMean.ts >= 600 && firstMean.ts < 700, "均值漂移（第 600 个元素起）应在其后不久报警");
        DriftEvent firstVar = got.stream().filter(e -> e.device.equals("var_shift")).findFirst().orElse(null);
        assertNotNull(firstVar);
        assertEquals("F", firstVar.test, "方差漂移应由 F 检验先报警");
    }

    @Test
    @DisplayName("独立同分布的平稳合成流上，候选率不超过论文上界 1−δ（每个元素）")
    void stationaryRateWithinBound() throws Exception {
        List<String[]> rows = new ArrayList<>();
        for (String[] r : csv("/m5b/synthetic_streams.csv")) {
            if (r[0].equals("stationary")) {
                rows.add(r);
            }
        }
        long candidates = runSynthetic(rows).stream().filter(e -> e.level == DriftEvent.Level.CANDIDATE).count();
        double rate = (double) candidates / rows.size();
        System.out.printf("平稳合成流：%d 个元素，候选 %d 个，候选率 %.5f（上界 %.2f）%n", rows.size(), candidates, rate, 1 - 0.99);
        assertTrue(rate <= 1 - 0.99);
    }

    private static OptWinDetector.Alarm alarm(double histMedian, double histSd) {
        return new OptWinDetector.Alarm("T", 100, 50, 50, histMedian, histSd, histMedian + 1, histSd,
                                        histMedian, 1.0, 5.0);
    }

    @Test
    @DisplayName("持续性核对：只用候选之后的元素，满六个才比较；K 个元素内一直高于门限则确认，回落则撤销；核对中的新候选不打断")
    void gateTransitions() {
        DriftGate g = new DriftGate(6, 0.5);                 // 门限 = 0 + 0.5 × 2 = 1
        for (int i = 0; i < 6; i++) {
            assertTrue(g.onElement("X", i, 5.0, null).isEmpty());
        }
        List<DriftEvent> ev = g.onElement("X", 6, 5.0, alarm(0.0, 2.0));
        assertEquals(DriftEvent.Level.CANDIDATE, ev.get(0).level);
        assertFalse(ev.get(0).duringCheck);
        assertTrue(g.onElement("X", 7, 5.0, null).isEmpty());
        assertTrue(g.onElement("X", 8, 5.0, null).isEmpty());
        ev = g.onElement("X", 9, 5.0, alarm(10.0, 1.0));       // 核对中的新候选 / candidate during the check
        assertEquals(1, ev.size());
        assertTrue(ev.get(0).duringCheck);
        for (int i = 10; i < 12; i++) {
            assertTrue(g.onElement("X", i, 5.0, null).isEmpty());
        }
        ev = g.onElement("X", 12, 5.0, null);                   // 候选之后第 6 个元素 = 第 K 个 / the K-th
        assertEquals(DriftEvent.Level.CONFIRM, ev.get(0).level, "第 K 个元素确认，参照与门限沿用第一个候选");
        assertEquals(1.0, ev.get(0).threshold, 0.0);
        assertEquals(6, ev.get(0).candidateTs);
        assertEquals(6, ev.get(0).checkedElements);

        DriftGate early = new DriftGate(36, 0.5);
        early.onElement("Z", 0, 5.0, alarm(0.0, 2.0));
        for (int i = 1; i <= 5; i++) {
            assertTrue(early.onElement("Z", i, 0.0, null).isEmpty(), "候选之后不满六个元素时不比较，不得撤销");
        }
        ev = early.onElement("Z", 6, 0.0, null);
        assertEquals(DriftEvent.Level.REVOKE, ev.get(0).level, "满六个后中位数 0 ≤ 门限 1，撤销");
        assertEquals(6, ev.get(0).checkedElements);

        DriftGate r = new DriftGate(36, 0.5);
        for (int i = 0; i < 6; i++) {
            r.onElement("Y", i, 9.0, null);                     // 候选之前的高值不得混入 / pre-candidate values excluded
        }
        r.onElement("Y", 6, 5.0, alarm(0.0, 2.0));
        double[] post = {5, 5, 5, 0, 0, 0};
        for (int i = 0; i < post.length; i++) {
            assertTrue(r.onElement("Y", 7 + i, post[i], null).isEmpty(), "中位数 2.5 仍高于门限");
        }
        ev = r.onElement("Y", 13, 0.0, null);                   // 最近六个 5,5,0,0,0,0 的中位数 0 / median 0
        assertEquals(DriftEvent.Level.REVOKE, ev.get(0).level);
        assertEquals(7, ev.get(0).checkedElements);
        assertFalse(r.isPending());
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T o) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(o);
        }
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            return (T) ois.readObject();
        }
    }

    @Test
    @DisplayName("检查点恢复：中途序列化检测器与核对状态再继续，事件与不中断的一次相同")
    void serializationRoundTrip() throws Exception {
        List<String[]> rows = new ArrayList<>();
        for (String[] r : csv("/m5b/synthetic_streams.csv")) {
            if (r[0].equals("mean_shift")) {
                rows.add(r);
            }
        }
        List<DriftEvent> whole = runSynthetic(rows);
        OptWinDetector d = new OptWinDetector(2016);
        DriftGate g = new DriftGate(36, 0.5);
        List<DriftEvent> split = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            if (i == 610) {                                  // 漂移开始后、报警之前 / after the shift, before the alarm
                assertTrue(d.windowLength() > 0, "恢复时窗口内容应非空");
                d = roundTrip(d);
                g = roundTrip(g);
            }
            double x = Double.parseDouble(rows.get(i)[2]);
            split.addAll(g.onElement("mean_shift", i, x, d.update(x, tables)));
        }
        assertEquals(whole.size(), split.size());
        for (int i = 0; i < whole.size(); i++) {
            assertEquals(whole.get(i).toCsv(), split.get(i).toCsv());
        }
    }
}
