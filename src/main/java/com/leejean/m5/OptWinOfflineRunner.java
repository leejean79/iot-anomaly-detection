package com.leejean.m5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 离线运行器（M5b 交接文档第三节「离线工具」）：读取逐台元素流 CSV，按同一份检测器与持续性核对算出事件清单，供与 Python
 * 参考实现、以及与在线算子做逐条等值比较。顺带把所用的常量表写出，Python 参考实现读同一张表。
 * Offline runner: reads per-device element streams, runs the same detector and gate, writes the event list for equality
 * checks against the Python reference and the online operator; also writes the table the Python reference reads.
 *
 * <p>调用 / usage（本地或开发机，仓库根目录；类路径为 target/classes 加 commons-math3-3.5.jar）：
 * <pre>
 * java -cp target/classes:commons-math3-3.5.jar com.leejean.m5.OptWinOfflineRunner \
 *   --streams docs/m5b/streams_10min.csv --time-col window_end --value-col main_score \
 *   --confidence 0.99 --robustness 0.5 --wmax 2016 --k 36 \
 *   --table-out docs/m5b/tables/thesis_0.99_0.5_2016.csv --out /tmp/events_java.csv
 * </pre>
 * 小时粒度用 {@code --streams docs/m5b/streams_hourly.csv --time-col hour_start --value-col median_main_score --wmax 336}。
 * 输入须按设备、时间升序（prepare_streams.py 的输出即是）。退出码 2 表示参数缺失。
 */
public final class OptWinOfflineRunner {

    private OptWinOfflineRunner() { }

    public static void main(String[] args) throws IOException {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            a.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        for (String req : new String[]{"streams", "time-col", "value-col", "confidence", "robustness", "wmax", "k", "out"}) {
            if (!a.containsKey(req)) {
                System.err.println("缺少参数 --" + req);
                System.exit(2);
            }
        }
        double conf = Double.parseDouble(a.get("confidence"));
        double rho = Double.parseDouble(a.get("robustness"));
        int wMax = Integer.parseInt(a.get("wmax"));
        int k = Integer.parseInt(a.get("k"));
        OptWinTables tables = OptWinTables.build(conf, rho, wMax);
        if (a.containsKey("table-out")) {
            tables.writeCsv(Paths.get(a.get("table-out")));
        }
        int events = run(Paths.get(a.get("streams")), a.get("time-col"), a.get("value-col"), tables, k,
                         Paths.get(a.get("out")));
        System.out.println("事件 " + events + " 条 → " + a.get("out"));
    }

    /** 逐台运行并写出事件清单，返回事件数 / run per device and write the events; returns the count. */
    public static int run(Path streams, String timeCol, String valueCol, OptWinTables tables, int k, Path out)
            throws IOException {
        Map<String, OptWinDetector> det = new LinkedHashMap<>();
        Map<String, DriftGate> gate = new LinkedHashMap<>();
        int count = 0;
        Path parent = out.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (BufferedReader br = Files.newBufferedReader(streams, StandardCharsets.UTF_8);
             PrintWriter pw = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8))) {
            String[] header = br.readLine().split(",");
            int iDev = indexOf(header, "device");
            int iTs = indexOf(header, timeCol);
            int iVal = indexOf(header, valueCol);
            pw.println(DriftEvent.CSV_HEADER);
            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split(",");
                String dev = f[iDev];
                long ts = Long.parseLong(f[iTs]);
                double x = Double.parseDouble(f[iVal]);
                OptWinDetector d = det.computeIfAbsent(dev, z -> new OptWinDetector(tables.wMax));
                DriftGate g = gate.computeIfAbsent(dev, z -> new DriftGate(k, tables.robustness));
                List<DriftEvent> evs = g.onElement(dev, ts, x, d.update(x, tables));
                for (DriftEvent e : evs) {
                    pw.println(e.toCsv());
                    count++;
                }
            }
        }
        return count;
    }

    private static int indexOf(String[] header, String name) {
        for (int i = 0; i < header.length; i++) {
            if (header[i].equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("输入里没有列 " + name);
    }
}
