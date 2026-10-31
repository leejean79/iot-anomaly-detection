package com.leejean.m7;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 聚合决策的 Java 离线运行器（验收项一的 Java 一侧）：读 {@code m7/prepare_inputs.py} 整理的七张表，按当地小时与当地日驱动
 * {@link FleetDecisionFunction}，写告警、命令、复核三份日志，格式与 Python 参考实现 {@code m7/decide.py} 相同。
 * Java offline runner for M7: drives FleetDecisionFunction over the prepared inputs and writes the three logs.
 *
 * <pre>
 * 执行环境：任意装有 JDK 11 及以上的机器，仓库根目录。
 * 调用：java -cp &lt;作业 jar&gt; com.leejean.m7.M7OfflineRunner --inputs docs/m7/inputs --out-dir docs/m7/run_java
 *       可选：--no-once（关闭「同一段偏离只发一次」）、--layers data,perf,detect（消融）、--common-ratio 3/4、--k-site 3、--p-point 0.06、
 *             --delta-sec 0（生效提前量，0 为一个决策节拍）、--wait-sec 0（等待时长 W，0 为一个日界）、
 *             --cmd-sec 0（决策节拍，0 为一个当地日）、--p-ctx 0.5（上下文通道标志门槛）
 * 前置条件：已运行 m7/prepare_inputs.py。
 * 产出：out-dir 下 alerts.csv、commands.csv、review.csv；终端打印三份日志的条数与 p_point。缺输入时退出码 2。
 * </pre>
 */
public final class M7OfflineRunner {

    public static final String ALERT_HEADER = "device,hour_start,local_hour,label,scope,co,point_rate,ctx_max,perf_magnitude,"
            + "data_channels,burst,point_flag,ctx_flag,note";
    public static final String COMMAND_HEADER = "device,day,decided_at,effective_at,seq,level,scope,label,triggers,forms,co,"
            + "co_devices,episode_n";
    public static final String REVIEW_HEADER = "device,day,kind,hour_start,detail";

    private M7OfflineRunner() { }

    public static void main(String[] args) throws IOException {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String k = args[i].replaceFirst("^--", "");
            if ("no-once".equals(k)) {
                a.put(k, "1");
            } else if (i + 1 < args.length) {
                a.put(k, args[++i]);
            }
        }
        Path in = Paths.get(a.getOrDefault("inputs", "docs/m7/inputs"));
        Path out = Paths.get(a.getOrDefault("out-dir", "docs/m7/run_java"));
        for (String f : new String[]{"hours.csv", "perf_events.csv", "perf_elements.csv", "data_days.csv", "bursts.csv", "refs.csv",
                "freeze.csv"}) {
            if (!Files.exists(in.resolve(f))) {
                System.err.println("缺少 " + in.resolve(f) + "；先运行 m7/prepare_inputs.py");
                System.exit(2);
            }
        }
        FleetDecisionFunction.Params p = new FleetDecisionFunction.Params();
        p.oncePerEpisode = !a.containsKey("no-once");
        String layers = a.getOrDefault("layers", "data,perf,detect");
        p.useData = layers.contains("data");
        p.usePerf = layers.contains("perf");
        p.useDetect = layers.contains("detect");
        String[] cr = a.getOrDefault("common-ratio", "3/4").split("/");
        p.commonNum = Integer.parseInt(cr[0]);
        p.commonDen = Integer.parseInt(cr[1]);
        p.kSite = Integer.parseInt(a.getOrDefault("k-site", "3"));
        p.deltaSec = Long.parseLong(a.getOrDefault("delta-sec", "0"));
        p.waitSec = Long.parseLong(a.getOrDefault("wait-sec", "0"));
        p.cmdSec = Long.parseLong(a.getOrDefault("cmd-sec", "0"));
        p.pCtx = Double.parseDouble(a.getOrDefault("p-ctx", "0.5"));
        Map<String, Map<Long, FleetDecisionFunction.HourObs>> hours = readHours(in.resolve("hours.csv"));
        p.pPoint = a.containsKey("p-point") ? Double.parseDouble(a.get("p-point")) : derivePPoint(in.resolve("hours.csv"));
        List<FleetDecisionFunction.Alert> alerts = new ArrayList<>();
        List<FleetDecisionFunction.Command> commands = new ArrayList<>();
        List<FleetDecisionFunction.Review> review = new ArrayList<>();
        run(in, hours, p, "2022-03-08", "2022-04-30", alerts, commands, review);
        Files.createDirectories(out);
        writeAlerts(alerts, out.resolve("alerts.csv"));
        writeCommands(commands, out.resolve("commands.csv"));
        writeReview(review, out.resolve("review.csv"));
        System.out.println("p_point = " + p.pPoint);
        System.out.println("告警 " + alerts.size() + " 条；命令 " + commands.size() + " 条；复核 " + review.size() + " 条");
    }

    /** 按当地日驱动决策函数 / drive the decision function day by day. */
    public static void run(Path in, Map<String, Map<Long, FleetDecisionFunction.HourObs>> hours, FleetDecisionFunction.Params p,
                           String firstDay, String lastDay, List<FleetDecisionFunction.Alert> alerts,
                           List<FleetDecisionFunction.Command> commands, List<FleetDecisionFunction.Review> review)
            throws IOException {
        FleetDecisionFunction f = new FleetDecisionFunction(p);
        for (String dev : hours.keySet()) {
            f.addDevice(dev);
        }
        for (Map<String, String> r : read(in.resolve("freeze.csv"))) {
            f.setFreeze(r.get("device"), Long.parseLong(r.get("freeze_ts")));
        }
        for (Map<String, String> r : read(in.resolve("refs.csv"))) {
            f.setRef(r.get("device"), r.get("channel"), Double.parseDouble(r.get("p10")), Double.parseDouble(r.get("p90")));
        }
        for (Map<String, String> r : read(in.resolve("perf_events.csv"))) {
            f.addPerfEvent(r.get("device"), Long.parseLong(r.get("ts")), r.get("level"), Double.parseDouble(r.get("threshold")),
                    Double.parseDouble(r.get("magnitude")));
        }
        for (Map<String, String> r : read(in.resolve("perf_elements.csv"))) {
            f.addPerfElement(r.get("device"), Long.parseLong(r.get("ts")), Double.parseDouble(r.get("main_score")));
        }
        for (Map<String, String> r : read(in.resolve("bursts.csv"))) {
            f.addBurst(r.get("device"), Long.parseLong(r.get("ts")), Long.parseLong(r.get("emitted_at")));
        }
        for (Map<String, String> r : read(in.resolve("data_days.csv"))) {
            f.addDataDay(r.get("device"), r.get("day"), r.get("channel"), "1".equals(r.get("evaluable")),
                    "1".equals(r.get("deviated")), r.get("transition"));
        }
        for (String day = firstDay; day.compareTo(lastDay) <= 0; day = FleetDecisionFunction.addDays(day, 1)) {
            for (long hs : FleetDecisionFunction.localHours(day)) {
                Map<String, FleetDecisionFunction.HourObs> obs = new HashMap<>();
                for (Map.Entry<String, Map<Long, FleetDecisionFunction.HourObs>> e : hours.entrySet()) {
                    FleetDecisionFunction.HourObs o = e.getValue().get(hs);
                    if (o != null) {
                        obs.put(e.getKey(), o);
                    }
                }
                alerts.addAll(f.alertTick(hs, obs));
                long he = hs + 3600;
                if (p.cmdSec != 0 && he < FleetDecisionFunction.dayStart(FleetDecisionFunction.addDays(day, 1))
                        && (he - FleetDecisionFunction.dayStart(day)) % p.cmdSec == 0) {
                    f.cmdTick(he, commands, review);          // 日内决策节拍 / intra-day decision tick
                }
            }
            f.dayTick(day, commands, review);
        }
    }

    public static Map<String, Map<Long, FleetDecisionFunction.HourObs>> readHours(Path path) throws IOException {
        Map<String, Map<Long, FleetDecisionFunction.HourObs>> out = new TreeMap<>();
        for (Map<String, String> r : read(path)) {
            FleetDecisionFunction.HourObs o = new FleetDecisionFunction.HourObs();
            o.rounds = Integer.parseInt(r.get("rounds"));
            o.admitted = Integer.parseInt(r.get("admitted"));
            o.guardRounds = Integer.parseInt(r.get("guard_rounds"));
            o.outlierRounds = Integer.parseInt(r.get("outlier_rounds"));
            o.ctxAbove = Integer.parseInt(r.get("ctx_above"));
            o.ctxMax = r.get("ctx_max");
            for (int k = 0; k < FleetDecisionFunction.ACTIVE.length; k++) {
                o.n[k] = Integer.parseInt(r.get("n_" + FleetDecisionFunction.ACTIVE[k]));
                o.sum[k] = Double.parseDouble(r.get("sum_" + FleetDecisionFunction.ACTIVE[k]));
            }
            out.computeIfAbsent(r.get("device"), k -> new HashMap<>()).put(Long.parseLong(r.get("hour_start")), o);
        }
        return out;
    }

    /** 平稳日参照期（03-08 至 03-17）可算设备小时离群率的第九十九百分位（与 decide.py 的 derive_p_point 相同）。 */
    public static double derivePPoint(Path hoursCsv) throws IOException {
        FleetDecisionFunction.Params d = new FleetDecisionFunction.Params();
        List<Double> rates = new ArrayList<>();
        for (Map<String, String> r : read(hoursCsv)) {
            String day = FleetDecisionFunction.localDay(Long.parseLong(r.get("hour_start")));
            if (day.compareTo("2022-03-08") < 0 || day.compareTo("2022-03-17") > 0) {
                continue;
            }
            int rounds = Integer.parseInt(r.get("rounds"));
            int adm = Integer.parseInt(r.get("admitted"));
            int guard = Integer.parseInt(r.get("guard_rounds"));
            if (d.qualityBad(rounds, guard) || adm < d.pointMinFrac * FleetDecisionFunction.NOMINAL_PER_HOUR) {
                continue;
            }
            rates.add((double) Integer.parseInt(r.get("outlier_rounds")) / adm);
        }
        rates.sort(Double::compare);
        double pos = (rates.size() - 1) * 0.99;
        int lo = (int) pos;
        int hi = Math.min(lo + 1, rates.size() - 1);
        double g = pos - lo;
        double x = rates.get(lo);
        double y = rates.get(hi);
        return g >= 0.5 ? y - (y - x) * (1 - g) : x + (y - x) * g;
    }

    static List<Map<String, String>> read(Path path) throws IOException {
        List<Map<String, String>> out = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String[] head = br.readLine().split(",", -1);
            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split(",", -1);
                Map<String, String> m = new LinkedHashMap<>();
                for (int i = 0; i < head.length; i++) {
                    m.put(head[i], i < f.length ? f[i] : "");
                }
                out.add(m);
            }
        }
        return out;
    }

    private static String d(Double v) {
        return v == null ? "" : Double.toString(v);
    }

    public static void writeAlerts(List<FleetDecisionFunction.Alert> alerts, Path path) throws IOException {
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8))) {
            w.print(ALERT_HEADER + "\n");
            for (FleetDecisionFunction.Alert x : alerts) {
                w.print(String.join(",", x.device, Long.toString(x.hourStart), x.localHour, x.label, x.scope, x.co, d(x.pointRate),
                        x.ctxMax, d(x.perfMagnitude), x.dataChannels, Integer.toString(x.burst), Integer.toString(x.pointFlag), Integer.toString(x.ctxFlag), x.note) + "\n");
            }
        }
    }

    public static void writeCommands(List<FleetDecisionFunction.Command> commands, Path path) throws IOException {
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8))) {
            w.print(COMMAND_HEADER + "\n");
            for (FleetDecisionFunction.Command c : commands) {
                w.print(String.join(",", c.device, c.day, Long.toString(c.decidedAt), Long.toString(c.effectiveAt),
                        Integer.toString(c.seq), Integer.toString(c.level), c.scope, c.label, c.triggers, c.forms,
                        Integer.toString(c.co), c.coDevices, Integer.toString(c.episodeN)) + "\n");
            }
        }
    }

    public static void writeReview(List<FleetDecisionFunction.Review> review, Path path) throws IOException {
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8))) {
            w.print(REVIEW_HEADER + "\n");
            for (FleetDecisionFunction.Review r : review) {
                w.print(String.join(",", r.device, r.day, r.kind, r.hourStart, r.detail) + "\n");
            }
        }
    }
}
