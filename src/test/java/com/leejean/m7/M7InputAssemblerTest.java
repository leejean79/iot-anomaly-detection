package com.leejean.m7;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 输入整理器与按到达时刻推进的回归（实现方案第二、三节）：用在库的全量离线输入合成一条在线到达序列——每个设备小时的轮按
 * {@code hours.csv} 的计数合成（变换域的和放在该小时最后一个有效读数上，使逐轮累加的结果与表中的和逐位相同），点通道离群记录取自
 * 评分转储，上下文评分取自性能层的十分钟流，事件、日结论与爆发取自整理好的表并按在线到达的时刻排序——经 {@link M7InputAssembler}
 * 与 {@link M7DumpRunner#drive} 驱动，三份日志须与 {@code docs/m7/run}（Python 参考实现，与 Java 离线运行器逐条相同）逐条相同。
 * Regression of the assembler and the arrival-ordered driver against the full offline run.
 */
class M7InputAssemblerTest {

    private static final Path IN = Paths.get("docs/m7/inputs");
    private static final long FIRST = FleetDecisionFunction.dayStart("2022-03-08");
    private static final long END = FleetDecisionFunction.dayStart("2022-05-01") + 60;

    private static List<Map<String, String>> read(Path p) throws IOException {
        return M7OfflineRunner.read(p);
    }

    private static M7Input input(int kind, String dev, long ts) {
        M7Input in = new M7Input();
        in.kind = kind;
        in.device = dev;
        in.ts = ts;
        return in;
    }

    @Test
    @DisplayName("全量合成到达序列经整理器驱动，与离线三份日志逐条相同 / full regression against docs/m7/run")
    void fullRegression() throws IOException {
        List<M7DumpRunner.Arrival> xs = arrivals(FIRST, END - 60);
        M7InputAssembler asm = new M7InputAssembler(M7OfflineRunner.params(defaultArgs()));
        M7InputAssembler.Out out = new M7InputAssembler.Out();
        M7DumpRunner.drive(asm, xs, END, out);

        Path tmp = Files.createDirectories(Paths.get("target/m7asm"));
        M7OfflineRunner.writeAlerts(out.alerts, tmp.resolve("alerts.csv"));
        M7OfflineRunner.writeCommands(out.commands, tmp.resolve("commands.csv"));
        M7OfflineRunner.writeReview(out.review, tmp.resolve("review.csv"));
        compare("docs/m7/run/commands.csv", tmp.resolve("commands.csv"), false);
        compare("docs/m7/run/review.csv", tmp.resolve("review.csv"), false);
        compare("docs/m7/run/alerts.csv", tmp.resolve("alerts.csv"), true);
    }

    static Map<String, String> defaultArgs() {
        Map<String, String> args = new HashMap<>();
        args.put("p-point", M7DumpRunner.DEFAULT_P_POINT);
        return args;
    }

    /**
     * 由在库的全量离线输入合成在线到达序列，只留到达时刻在 [from, to] 之内的 / synthesized arrivals within [from, to].
     */
    static List<M7DumpRunner.Arrival> arrivals(long from, long to) throws IOException {
        List<M7DumpRunner.Arrival> xs = new ArrayList<>();
        Map<String, Long> freeze = new HashMap<>();
        for (Map<String, String> r : read(IN.resolve("freeze.csv"))) {
            freeze.put(r.get("device"), Long.parseLong(r.get("freeze_ts")));
        }
        Map<String, double[]> refs = new HashMap<>();
        for (Map<String, String> r : read(IN.resolve("refs.csv"))) {
            double[] v = refs.computeIfAbsent(r.get("device"), k -> new double[6]);
            int k = java.util.Arrays.asList(FleetDecisionFunction.ACTIVE).indexOf(r.get("channel"));
            v[2 * k] = Double.parseDouble(r.get("p10"));
            v[2 * k + 1] = Double.parseDouble(r.get("p90"));
        }
        for (Map<String, String> r : read(IN.resolve("hours.csv"))) {
            String dev = r.get("device");
            long hs = Long.parseLong(r.get("hour_start"));
            int rounds = Integer.parseInt(r.get("rounds"));
            int adm = Integer.parseInt(r.get("admitted"));
            int guard = Integer.parseInt(r.get("guard_rounds"));
            int[] n = new int[3];
            double[] sum = new double[3];
            for (int k = 0; k < 3; k++) {
                n[k] = Integer.parseInt(r.get("n_" + FleetDecisionFunction.ACTIVE[k]));
                sum[k] = Double.parseDouble(r.get("sum_" + FleetDecisionFunction.ACTIVE[k]));
            }
            long tf = freeze.getOrDefault(dev, -1L);
            for (int i = 0; i < rounds; i++) {
                M7Input in = input(M7Input.ROUND, dev, hs + i * 3600L / rounds);
                if (tf >= hs && tf < hs + 3600 && i == 0) {
                    in.ts = tf;                                  // 冻结时刻落在本小时：第一轮带参照 / carries the references
                    in.refs = refs.get(dev);
                }
                in.flag2 = i < adm;
                in.flag1 = i < guard;
                in.tv = new double[3];
                for (int k = 0; k < 3; k++) {
                    in.tv[k] = i < n[k] ? (i == n[k] - 1 ? sum[k] : 0.0) : Double.NaN;
                }
                xs.add(new M7DumpRunner.Arrival(in, in.ts));
            }
        }
        // 点通道：到达滑动步的离群记录，按窗口末到达 / arrival-slide outliers, arriving at the window end
        ObjectMapper om = new ObjectMapper();
        try (BufferedReader br = Files.newBufferedReader(Paths.get("docs/m3_a4/full_fix/scores.jsonl"), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("{")) {
                    continue;
                }
                JsonNode j = om.readTree(line);
                long rt = j.path("roundTs").asLong();
                long we = j.path("windowEnd").asLong();
                if ("m2_point".equals(j.path("channel").asText()) && j.path("outlier").asBoolean(false) && we - 60 <= rt && rt < we) {
                    M7Input in = input(M7Input.OUTLIER, j.path("device").asText(), rt);
                    in.ts2 = we;
                    xs.add(new M7DumpRunner.Arrival(in, we));
                }
            }
        }
        for (Map<String, String> r : read(Paths.get("docs/m5b/streams_10min_with_injection.csv"))) {
            M7Input in = input(M7Input.CTX, r.get("device"), Long.parseLong(r.get("window_end")));
            in.v1 = Double.parseDouble(r.get("main_score"));
            in.flag1 = "1".equals(r.get("above_threshold"));
            xs.add(new M7DumpRunner.Arrival(in, in.ts));
        }
        for (Map<String, String> r : read(IN.resolve("perf_events.csv"))) {
            M7Input in = input(M7Input.PERF, r.get("device"), Long.parseLong(r.get("ts")));
            in.s1 = r.get("level");
            in.v1 = Double.parseDouble(r.get("threshold"));
            in.v2 = Double.parseDouble(r.get("magnitude"));
            xs.add(new M7DumpRunner.Arrival(in, in.ts));
        }
        for (Map<String, String> r : read(IN.resolve("data_days.csv"))) {
            long at = FleetDecisionFunction.dayStart(FleetDecisionFunction.addDays(r.get("day"), 1));
            M7Input in = input(M7Input.DAY_RECORD, r.get("device"), at);
            in.s1 = r.get("day");
            in.s2 = r.get("channel");
            in.flag1 = "1".equals(r.get("evaluable"));
            in.flag2 = "1".equals(r.get("deviated"));
            xs.add(new M7DumpRunner.Arrival(in, at));
            if (!r.get("transition").isEmpty()) {
                M7Input t = input(M7Input.TRANSITION, r.get("device"), at);
                t.s1 = r.get("day");
                t.s2 = r.get("channel");
                t.s3 = r.get("transition");
                xs.add(new M7DumpRunner.Arrival(t, at));
            }
        }
        for (Map<String, String> r : read(IN.resolve("bursts.csv"))) {
            M7Input in = input(M7Input.BURST, r.get("device"), Long.parseLong(r.get("ts")));
            in.ts2 = Long.parseLong(r.get("emitted_at"));
            xs.add(new M7DumpRunner.Arrival(in, in.ts2));
        }
        xs.removeIf(x -> x.at < from || x.at > to);
        return xs;
    }

    /**
     * 逐行比较；告警日志的 point_rate、ctx_max、perf_magnitude 三列按数值比较（Java 与 Python 的浮点文本写法可能不同）。
     * Line by line; the three floating columns of the alert log compare as numbers.
     */
    private static void compare(String expected, Path actual, boolean alerts) throws IOException {
        List<String> e = Files.readAllLines(Paths.get(expected), StandardCharsets.UTF_8);
        List<String> a = Files.readAllLines(actual, StandardCharsets.UTF_8);
        assertEquals(e.size(), a.size(), expected + " 行数 / line count");
        for (int i = 1; i < e.size(); i++) {
            String[] x = e.get(i).split(",", -1);
            String[] y = a.get(i).split(",", -1);
            assertEquals(x.length, y.length, expected + " 第 " + i + " 行列数");
            for (int c = 0; c < x.length; c++) {
                if (alerts && (c == 6 || c == 7 || c == 8)) {
                    assertEquals(x[c].isEmpty() ? null : Double.valueOf(x[c]), y[c].isEmpty() ? null : Double.valueOf(y[c]),
                            expected + " 第 " + i + " 行第 " + c + " 列");
                } else {
                    assertEquals(x[c], y[c], expected + " 第 " + i + " 行第 " + c + " 列");
                }
            }
        }
    }

    @Test
    @DisplayName("爆发的发出时刻按箱规则推出，与离线模拟逐条相同 / burst emission times match the offline simulation")
    void burstEmission() throws IOException {
        Map<String, TreeMap<Long, long[]>> bins = new HashMap<>();
        for (Map<String, String> r : read(Paths.get("docs/m5a/extract/denominators.csv"))) {
            bins.computeIfAbsent(r.get("device"), k -> new TreeMap<>())
                    .computeIfAbsent(Long.parseLong(r.get("bin_end")), k -> new long[2])[0] = Long.parseLong(r.get("admitted"));
        }
        ObjectMapper om = new ObjectMapper();
        java.util.Set<String> seen = new java.util.HashSet<>();
        try (BufferedReader br = Files.newBufferedReader(Paths.get("docs/m3_a4/full_fix/scores.jsonl"), StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("{")) {
                    continue;
                }
                JsonNode j = om.readTree(line);
                long rt = j.path("roundTs").asLong();
                long we = j.path("windowEnd").asLong();
                String dev = j.path("device").asText();
                if ("m2_point".equals(j.path("channel").asText()) && j.path("outlier").asBoolean(false) && we - 60 <= rt && rt < we
                        && seen.add(dev + "|" + rt)) {
                    bins.computeIfAbsent(dev, k -> new TreeMap<>()).computeIfAbsent(M7DumpRunner.binEnd(rt), k -> new long[2])[1]++;
                }
            }
        }
        int n = 0;
        for (Map<String, String> r : read(IN.resolve("bursts.csv"))) {
            assertEquals(Long.parseLong(r.get("emitted_at")),
                    M7DumpRunner.burstEmission(bins.get(r.get("device")), Long.parseLong(r.get("ts"))), r.toString());
            n++;
        }
        // 不剔除注入的一遍共 250 次爆发：2026-11-07 起按 1.31 版裁决第一节以修正后的提取为基线（原为 247，见 docs/reports/m6a_extract_impact.md）
        // 250 bursts in the injected pass since the 2026-11-07 baseline correction (was 247).
        assertEquals(250, n);
    }
    @Test
    @DisplayName("输入映射带参数版本号：轮、日记录与数据层转移读版本号，命令回路关闭（为空）时为一 / version mapping")
    void versionMapping() {
        com.leejean.m1.DeviceRound r = new com.leejean.m1.DeviceRound();
        r.setDevice("G");
        r.setTs(FIRST);
        r.setX(new double[5]);
        assertEquals(1, M7Input.fromRound(r).version, "回路关闭 / loop off");
        r.setVersion(3);
        assertEquals(3, M7Input.fromRound(r).version);
        com.leejean.m1.MonitoringSnapshot s = new com.leejean.m1.MonitoringSnapshot();
        s.setDevice("G");
        s.setM5aDay("2022-03-28");
        s.setM5aChannel("Gas");
        assertEquals(1, M7Input.fromRecord(s).version);
        s.setVersion(2);
        assertEquals(2, M7Input.fromRecord(s).version);
        com.leejean.m5.DriftEvent e = com.leejean.m5.DriftEvent.dataState("G", FIRST, com.leejean.m5.DriftEvent.Level.DEVIATE, "Gas",
                "2022-03-28", 0.3, 0.001, 1.0, 1.0);
        assertEquals(1, M7Input.fromDrift(e, FIRST).version);
        e.version = 2;
        assertEquals(2, M7Input.fromDrift(e, FIRST).version);
    }
}
