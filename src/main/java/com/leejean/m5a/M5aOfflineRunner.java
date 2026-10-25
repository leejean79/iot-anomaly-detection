package com.leejean.m5a;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.m1.DeviceRound;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m5.DriftEvent;
import com.leejean.m5.OptWinTables;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 数据层的 Java 离线运行器（验收项 V-M5a-3 的离线一侧）：读同一次运行的标准化轮转储（{@code synergia-m1-out}，JSON 每行一条）与点通道
 * 离群记录转储（{@code synergia-scores}），逐台按时间顺序送进与在线算子相同的 {@link M5aCore}，写出数据层事件与日记录两份 CSV。
 * Offline runner for V-M5a-3: replays the m1-out and scores dumps through the same M5aCore as the online operator.
 *
 * <p>输入结束时的处理与在线一致：以全部设备的最后一条轮时刻为最终的事件时间，关闭此前已到期的十分钟与当地日；更晚的不关闭。
 * At the end of input, bins and days whose close time has passed (by the last round of any device) are closed.
 *
 * <pre>
 * 执行环境：任意装有 JDK 11 及以上的机器（集群上在 jobmanager 容器内用作业 jar 运行）。
 * 调用：java -cp &lt;作业 jar&gt; com.leejean.m5a.M5aOfflineRunner --rounds m1out.jsonl --scores scores.jsonl \
 *        --events-out data_events.csv --records-out day_records.csv
 *       可选：--d0 0.38/0.73/0.84/0.43/0.51 --alpha 0.01 --missing-max 0.2 --guard-max 0.2 --period 10 --slide 60
 *             --confidence 0.99 --robustness 0.5 --wmax 2016（与作业参数的默认值相同）
 * 产出：两份 CSV；终端打印事件数、日记录数与最后一条轮的时刻。退出码 2 表示参数缺失；某台设备的轮不按时刻递增时抛异常退出。
 *
 * 转换模式（把在线输出转成同一格式，便于逐行比较）：
 * java -cp &lt;作业 jar&gt; com.leejean.m5a.M5aOfflineRunner --online-drift drift.jsonl --online-monitoring monitoring.jsonl \
 *        --events-out online_events.csv --records-out online_records.csv
 * 只取漂移主题中 layer 为 data 的事件与监测主题中 windowEnd 为 −3 的日记录，排序与离线相同。
 * </pre>
 */
public final class M5aOfflineRunner {

    /** 日记录 CSV 表头 / header of the daily-record CSV. */
    public static final String RECORD_HEADER = "device,day,channel,evaluable,reason,rounds,expected,guard_rounds,n,d,p,p10,median,p90,"
            + "shift,width_x,max_cell_mass,deviated";

    private M5aOfflineRunner() { }

    public static void main(String[] args) throws IOException {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            a.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        if (a.containsKey("online-drift")) {
            for (String req : new String[]{"online-monitoring", "events-out", "records-out"}) {
                if (!a.containsKey(req)) {
                    System.err.println("缺少参数 --" + req);
                    System.exit(2);
                }
            }
            List<DriftEvent> events = new ArrayList<>();
            List<MonitoringSnapshot> records = new ArrayList<>();
            convertOnline(Paths.get(a.get("online-drift")), Paths.get(a.get("online-monitoring")), events, records);
            writeEvents(events, Paths.get(a.get("events-out")));
            writeRecords(records, Paths.get(a.get("records-out")));
            System.out.println("在线数据层事件 " + events.size() + " 条，日记录 " + records.size() + " 条");
            return;
        }
        for (String req : new String[]{"rounds", "scores", "events-out", "records-out"}) {
            if (!a.containsKey(req)) {
                System.err.println("缺少参数 --" + req);
                System.exit(2);
            }
        }
        double[] d0 = java.util.Arrays.stream(a.getOrDefault("d0", "0.38/0.73/0.84/0.43/0.51").split("/"))
                .mapToDouble(Double::parseDouble).toArray();
        M5aCore.Params p = new M5aCore.Params(d0, Double.parseDouble(a.getOrDefault("alpha", "0.01")),
                Double.parseDouble(a.getOrDefault("missing-max", "0.2")), Double.parseDouble(a.getOrDefault("guard-max", "0.2")),
                Integer.parseInt(a.getOrDefault("period", "10")), Integer.parseInt(a.getOrDefault("slide", "60")));
        OptWinTables tables = OptWinTables.build(Double.parseDouble(a.getOrDefault("confidence", "0.99")),
                Double.parseDouble(a.getOrDefault("robustness", "0.5")), Integer.parseInt(a.getOrDefault("wmax", "2016")));
        List<DriftEvent> events = new ArrayList<>();
        List<MonitoringSnapshot> records = new ArrayList<>();
        long lastTs = run(Paths.get(a.get("rounds")), Paths.get(a.get("scores")), p, tables, events, records);
        writeEvents(events, Paths.get(a.get("events-out")));
        writeRecords(records, Paths.get(a.get("records-out")));
        System.out.println("数据层事件 " + events.size() + " 条，日记录 " + records.size() + " 条");
        System.out.println("最后一条轮时刻 " + lastTs);
    }

    /**
     * 按文件顺序流式运行（不把轮读进内存），事件与日记录按设备、时刻排序后放进两个列表，返回全部设备最后一条轮的时刻（秒）。
     * 每台设备的轮须按时刻递增，否则报错退出。
     * Streams the rounds file (rounds are not kept in memory); returns the last round time. Rounds must be in time order.
     */
    public static long run(Path roundsPath, Path scoresPath, M5aCore.Params p, OptWinTables tables,
                           List<DriftEvent> events, List<MonitoringSnapshot> records) throws IOException {
        ObjectMapper om = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Map<String, List<long[]>> outliers = new HashMap<>();
        try (BufferedReader br = Files.newBufferedReader(scoresPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty() || line.charAt(0) != '{') {
                    continue;
                }
                JsonNode n = om.readTree(line);
                if (!"m2_point".equals(n.path("channel").asText()) || !n.path("outlier").asBoolean(false)) {
                    continue;
                }
                outliers.computeIfAbsent(n.path("device").asText(), k -> new ArrayList<>())
                        .add(new long[]{n.path("roundTs").asLong(), n.path("windowEnd").asLong()});
            }
        }
        Map<String, Driver> drivers = new TreeMap<>();
        long lastTs = Long.MIN_VALUE;
        try (BufferedReader br = Files.newBufferedReader(roundsPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty() || line.charAt(0) != '{') {
                    continue;
                }
                DeviceRound r = om.readValue(line, DeviceRound.class);
                drivers.computeIfAbsent(r.getDevice(), d -> new Driver(d, outliers.getOrDefault(d, new ArrayList<>()), p, tables))
                        .feed(r, events, records);
                lastTs = Math.max(lastTs, r.getTs());
            }
        }
        long finalTime = lastTs - 1;                      // 在线的最终水位线约为最后一条轮时刻减一毫秒 / final watermark
        for (Driver d : drivers.values()) {
            d.finish(finalTime, events, records);
        }
        sort(events, records);
        return lastTs;
    }

    /**
     * 一台设备：每条轮之前先送入窗口末不晚于它的离群记录并关闭到期的十分钟（模拟在线按事件时间推进）；结束时关闭到 finalTime 为止
     * 到期的十分钟与当地日。/ One device, advancing like event time online.
     */
    private static final class Driver {
        private final String dev;
        private final List<long[]> os;
        private final M5aCore.Params p;
        private final OptWinTables tables;
        private final M5aCore core = new M5aCore();
        private int oi;
        private long lastTs = Long.MIN_VALUE;

        Driver(String dev, List<long[]> os, M5aCore.Params p, OptWinTables tables) {
            this.dev = dev;
            this.os = os;
            this.os.sort(Comparator.comparingLong(x -> x[1]));
            this.p = p;
            this.tables = tables;
        }

        void feed(DeviceRound r, List<DriftEvent> events, List<MonitoringSnapshot> records) {
            if (r.getTs() <= lastTs) {
                throw new IllegalStateException("设备 " + dev + " 的轮不是按时刻递增：" + r.getTs() + " 在 " + lastTs + " 之后");
            }
            lastTs = r.getTs();
            while (oi < os.size() && os.get(oi)[1] <= r.getTs()) {
                core.onOutlier(os.get(oi)[0], os.get(oi)[1], p);
                oi++;
            }
            core.closeBinsUpTo(r.getTs() - 1, dev, p, tables, events);
            core.onRound(r, p, events, records);                 // 新的当地日的第一条轮会先关闭前一日 / closes the previous day
        }

        void finish(long finalTime, List<DriftEvent> events, List<MonitoringSnapshot> records) {
            while (oi < os.size()) {
                core.onOutlier(os.get(oi)[0], os.get(oi)[1], p);
                oi++;
            }
            core.closeBinsUpTo(finalTime, dev, p, tables, events);
            if (core.openDayEnd() > 0 && core.openDayEnd() <= finalTime) {
                core.closeDay(dev, p, events, records);
            }
        }
    }

    /** 一台设备的全部轮（测试用）/ all rounds of one device (used by tests). */
    public static void runDevice(String dev, List<DeviceRound> rs, List<long[]> os, long finalTime, M5aCore.Params p,
                                 OptWinTables tables, List<DriftEvent> events, List<MonitoringSnapshot> records) {
        rs.sort(Comparator.comparingLong(DeviceRound::getTs));
        Driver d = new Driver(dev, os, p, tables);
        for (DeviceRound r : rs) {
            d.feed(r, events, records);
        }
        d.finish(finalTime, events, records);
    }

    /** 在线转储（JSON 每行一条）转成离线同一格式 / convert the online dumps into the offline format. */
    public static void convertOnline(Path drift, Path monitoring, List<DriftEvent> events, List<MonitoringSnapshot> records)
            throws IOException {
        ObjectMapper om = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        for (String line : Files.readAllLines(drift, StandardCharsets.UTF_8)) {
            if (line.startsWith("{")) {
                DriftEvent e = om.readValue(line, DriftEvent.class);
                if ("data".equals(e.layer)) {
                    events.add(e);
                }
            }
        }
        for (String line : Files.readAllLines(monitoring, StandardCharsets.UTF_8)) {
            if (line.startsWith("{")) {
                JsonNode n = om.readTree(line);
                if (n.path("windowEnd").asLong() == M5aCore.DAY_RECORD_WINDOW_END) {
                    records.add(om.treeToValue(n, MonitoringSnapshot.class));
                }
            }
        }
        sort(events, records);
    }

    private static void sort(List<DriftEvent> events, List<MonitoringSnapshot> records) {
        events.sort(Comparator.comparing((DriftEvent x) -> x.device).thenComparingLong(x -> x.ts)
                .thenComparing(x -> x.channel).thenComparing(x -> x.level.name()));
        records.sort(Comparator.comparing(MonitoringSnapshot::getDevice).thenComparing(MonitoringSnapshot::getM5aDay)
                .thenComparing(MonitoringSnapshot::getM5aChannel));
    }

    public static void writeEvents(List<DriftEvent> events, Path out) throws IOException {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8))) {
            pw.println(DriftEvent.DATA_CSV_HEADER);
            for (DriftEvent e : events) {
                pw.println(e.toDataCsv());
            }
        }
    }

    public static void writeRecords(List<MonitoringSnapshot> records, Path out) throws IOException {
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8))) {
            pw.println(RECORD_HEADER);
            for (MonitoringSnapshot s : records) {
                pw.println(recordCsv(s));
            }
        }
    }

    /** 日记录的 CSV 行（在线转储转成的 CSV 用同一格式）/ CSV row of a daily record. */
    public static String recordCsv(MonitoringSnapshot s) {
        return String.join(",", s.getDevice(), s.getM5aDay(), s.getM5aChannel(), s.isM5aEvaluable() ? "1" : "0",
                s.getM5aReason() == null ? "" : s.getM5aReason(), Integer.toString(s.getM5aRounds()), Long.toString(s.getM5aExpected()),
                Integer.toString(s.getM5aGuardRounds()), Integer.toString(s.getM5aN()), Double.toString(s.getM5aD()),
                Double.toString(s.getM5aP()), Double.toString(s.getM5aP10()), Double.toString(s.getM5aMedian()),
                Double.toString(s.getM5aP90()), Double.toString(s.getM5aShift()), Double.toString(s.getM5aWidthX()),
                Double.toString(s.getM5aMaxCellMass()), s.isM5aDeviated() ? "1" : "0");
    }
}
