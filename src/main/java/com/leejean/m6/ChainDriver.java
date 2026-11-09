package com.leejean.m6;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.leejean.m1.Channels;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m1.RobustScalerFunction;
import com.leejean.m5.DriftEvent;
import com.leejean.m5.OptWinTables;
import com.leejean.m5a.RefHistogram;
import com.leejean.m7.FleetDecisionFunction;
import com.leejean.m7.M7OfflineRunner;

import java.io.File;
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
 * 链式离线实验的驱动程序（M6b 交接文档第四节，决定十五）：按当地日推进，把聚合模块的决策函数、八台设备的模拟器与数据层重算串成离线
 * 闭环。第 D 日：八台模拟器推进到 D 日日界后一个滑动步（D 日最后一分钟的轮的离群记录都已到达、D 日的日记录已关闭）；驱动把新的
 * 日记录、状态转移与爆发交给决策函数，用收口输入中不随零级变化的观测（轮数、入口轮数、守卫轮数、上下文超阈与主分、变换域的小时
 * 和）加上模拟器重算的离群轮数与参数版本，逐小时做告警节拍，再做日节拍；新命令按生效时刻排进对应设备的模拟器。性能层的确认与回落、
 * 上下文主分、参照与冻结日都取自收口输入（选项一下上下文通道不随零级变化；参照宽度按 1.37 版裁决第五条取首次冻结的值）。
 * Driver of the chained offline experiment: decision function, eight simulators and the data-layer recomputation, day by day.
 *
 * <p>脚本交付五要素：
 * <ol>
 *   <li>执行环境：开发机或本地 Mac，仓库根目录，JDK 11，先 mvn package；堆至少 4 GB（-Xmx6g）。</li>
 *   <li>调用命令：{@code java -Xmx6g -cp target/iot-anomaly-detection-1.0-SNAPSHOT.jar com.leejean.m6.ChainDriver --variant D1
 *       --out-dir docs/m6b/chain/D1}；--variant 取 open（命令只发出不执行，驱动正确性对照）、D1、D7、pos（只重估位置）；可选
 *       --rounds-dir docs/m6a/extract、--rounds-E docs/m6b/extract/rounds_E_inj.csv.gz（E 的注入轮表）、--inputs docs/m7/inputs，
 *       以及聚合模块的决策参数（与 M7OfflineRunner 相同的选项名）。</li>
 *   <li>前置条件：八台轮表已提取；E 的注入轮表已由 m6a/inject_rounds.py 生成；docs/m7/inputs 已由 m7/prepare_inputs.py 生成。</li>
 *   <li>期望产出：out-dir 下告警、命令、复核三份日志（格式同 M7OfflineRunner）；hours.csv（逐设备小时的离群轮数、入口点数与版本）、
 *       data_days.csv（逐日逐通道的数据层记录与版本）、bursts.csv、level0/（每条执行的命令一个 JSON，格式同 M6aOfflineRunner）；
 *       终端打印三份日志的条数与每台执行的零级次数。</li>
 *   <li>失败兜底：缺输入时退出码 2；内存不足时加大 -Xmx。</li>
 * </ol>
 */
public final class ChainDriver {
    private ChainDriver() {
    }

    private static final Map<String, Double> R = new LinkedHashMap<>();

    static {
        String[] d = {"A", "B", "C", "D", "E", "F", "G", "H"};
        double[] r = {1.0, 1.0, 1.0, 0.75, 1.0, 1.0, 1.5, 1.0};   // 全量运行的每台半径（.env 终值）/ per-device radius of the full run
        for (int i = 0; i < d.length; i++) {
            R.put(d[i], r[i]);
        }
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = M7OfflineRunner.parseArgs(args);
        ChainDeviceSim.Variant variant = ChainDeviceSim.Variant.valueOf(a.getOrDefault("variant", "open").toUpperCase());
        Path in = Paths.get(a.getOrDefault("inputs", "docs/m7/inputs"));
        Path out = Paths.get(a.getOrDefault("out-dir", "docs/m6b/chain/" + variant.name()));
        String roundsDir = a.getOrDefault("rounds-dir", "docs/m6a/extract");
        String roundsE = a.getOrDefault("rounds-E", "docs/m6b/extract/rounds_E_inj.csv.gz");
        for (String f : new String[]{"hours.csv", "perf_events.csv", "perf_elements.csv", "refs.csv", "freeze.csv"}) {
            if (!Files.exists(in.resolve(f))) {
                System.err.println("缺少 " + in.resolve(f));
                System.exit(2);
            }
        }
        FleetDecisionFunction.Params p = M7OfflineRunner.params(a);
        p.alertSec = 3600;
        p.pPoint = a.containsKey("p-point") ? Double.parseDouble(a.get("p-point")) : M7OfflineRunner.derivePPoint(in.resolve("hours.csv"));
        Map<String, Map<Long, FleetDecisionFunction.HourObs>> base = M7OfflineRunner.readHours(in.resolve("hours.csv"));

        FleetDecisionFunction f = new FleetDecisionFunction(p);
        for (String dev : base.keySet()) {
            f.addDevice(dev);
        }
        for (Map<String, String> r : M7OfflineRunner.read(in.resolve("freeze.csv"))) {
            f.setFreeze(r.get("device"), Long.parseLong(r.get("freeze_ts")));
        }
        for (Map<String, String> r : M7OfflineRunner.read(in.resolve("refs.csv"))) {
            f.setRef(r.get("device"), r.get("channel"), Double.parseDouble(r.get("p10")), Double.parseDouble(r.get("p90")));
        }
        for (Map<String, String> r : M7OfflineRunner.read(in.resolve("perf_events.csv"))) {
            f.addPerfEvent(r.get("device"), Long.parseLong(r.get("ts")), r.get("level"), Double.parseDouble(r.get("threshold")),
                    Double.parseDouble(r.get("magnitude")));
        }
        for (Map<String, String> r : M7OfflineRunner.read(in.resolve("perf_elements.csv"))) {
            f.addPerfElement(r.get("device"), Long.parseLong(r.get("ts")), Double.parseDouble(r.get("main_score")));
        }

        OptWinTables tables = OptWinTables.build(0.99, 0.5, 2016);
        Map<String, ChainDeviceSim> sims = new TreeMap<>();
        for (String dev : R.keySet()) {
            String path = "E".equals(dev) ? roundsE : roundsDir + "/rounds_" + dev + ".csv.gz";
            sims.put(dev, new ChainDeviceSim(dev, path, variant, R.get(dev), tables));
        }
        Map<String, int[]> fed = new HashMap<>();                          // 设备 → {已送记录数, 已送转移数, 已送爆发数}
        List<FleetDecisionFunction.Alert> alerts = new ArrayList<>();
        List<FleetDecisionFunction.Command> commands = new ArrayList<>();
        List<FleetDecisionFunction.Review> review = new ArrayList<>();
        Map<String, String> trans = new HashMap<>();                       // 设备|日|通道 → DEVIATE 或 RECOVER

        String first = a.getOrDefault("first-day", "2022-03-08");
        String last = a.getOrDefault("last-day", "2022-04-30");
        for (String day = first; day.compareTo(last) <= 0; day = FleetDecisionFunction.addDays(day, 1)) {
            long t1 = FleetDecisionFunction.dayStart(FleetDecisionFunction.addDays(day, 1)) + 60;
            for (ChainDeviceSim s : sims.values()) {
                s.advanceTo(t1);
                int[] k = fed.computeIfAbsent(s.dev, x -> new int[3]);
                for (; k[1] < s.transitions.size(); k[1]++) {
                    DriftEvent e = s.transitions.get(k[1]);
                    trans.put(s.dev + "|" + e.dataDay + "|" + e.channel, e.level.name());
                }
                for (; k[0] < s.records.size(); k[0]++) {
                    MonitoringSnapshot m = s.records.get(k[0]);
                    f.addDataDay(s.dev, m.getM5aDay(), m.getM5aChannel(), m.isM5aEvaluable(), m.isM5aDeviated(),
                            trans.getOrDefault(s.dev + "|" + m.getM5aDay() + "|" + m.getM5aChannel(), ""),
                            m.getVersion() == null ? 1 : m.getVersion());
                }
                for (; k[2] < s.bursts.size(); k[2]++) {
                    ChainDeviceSim.Burst b = s.bursts.get(k[2]);
                    f.addBurst(s.dev, b.ts, b.emittedAt);
                }
            }
            for (long hs : FleetDecisionFunction.localHours(day)) {
                Map<String, FleetDecisionFunction.HourObs> obs = new HashMap<>();
                for (Map.Entry<String, Map<Long, FleetDecisionFunction.HourObs>> e : base.entrySet()) {
                    FleetDecisionFunction.HourObs o = e.getValue().get(hs);
                    if (o == null) {
                        continue;
                    }
                    int[] h = sims.get(e.getKey()).hours.get(hs);
                    obs.put(e.getKey(), withSim(o, h == null ? 0 : h[0], h == null ? 1 : h[1]));
                }
                alerts.addAll(f.alertTick(hs, obs));
                long he = hs + 3600;
                if (p.cmdSec != 0 && he < FleetDecisionFunction.dayStart(FleetDecisionFunction.addDays(day, 1))
                        && (he - FleetDecisionFunction.dayStart(day)) % p.cmdSec == 0) {
                    int n = commands.size();
                    f.cmdTick(he, commands, review);
                    schedule(commands, n, sims);
                }
            }
            int n = commands.size();
            f.dayTick(day, commands, review);
            schedule(commands, n, sims);
        }

        Files.createDirectories(out.resolve("level0"));
        M7OfflineRunner.writeAlerts(alerts, out.resolve("alerts.csv"));
        M7OfflineRunner.writeCommands(commands, out.resolve("commands.csv"));
        M7OfflineRunner.writeReview(review, out.resolve("review.csv"));
        writeTables(sims, out);
        StringBuilder ex = new StringBuilder();
        for (ChainDeviceSim s : sims.values()) {
            for (ChainDeviceSim.Exec x : s.execs) {
                writeExec(s, x, out.resolve("level0").resolve(s.dev + "_" + x.seq + ".json").toFile());
            }
            ex.append(s.dev).append('=').append(s.execs.size()).append(' ');
        }
        System.out.println("p_point = " + p.pPoint);
        System.out.println(variant + "：告警 " + alerts.size() + " 条；命令 " + commands.size() + " 条；复核 " + review.size()
                + " 条；执行的零级 " + ex.toString().trim());
    }

    private static void schedule(List<FleetDecisionFunction.Command> commands, int from, Map<String, ChainDeviceSim> sims) {
        for (FleetDecisionFunction.Command c : commands.subList(from, commands.size())) {
            sims.get(c.device).schedule(c.effectiveAt, c.seq);
        }
    }

    /** 收口输入的观测换上模拟器重算的离群轮数与版本 / the closeout observation with the simulated outliers and version. */
    private static FleetDecisionFunction.HourObs withSim(FleetDecisionFunction.HourObs o, int outliers, int version) {
        FleetDecisionFunction.HourObs x = new FleetDecisionFunction.HourObs();
        x.rounds = o.rounds;
        x.admitted = o.admitted;
        x.guardRounds = o.guardRounds;
        x.outlierRounds = outliers;
        x.ctxAbove = o.ctxAbove;
        x.ctxMax = o.ctxMax;
        System.arraycopy(o.n, 0, x.n, 0, o.n.length);
        System.arraycopy(o.sum, 0, x.sum, 0, o.sum.length);
        x.version = version;
        return x;
    }

    private static void writeTables(Map<String, ChainDeviceSim> sims, Path out) throws IOException {
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("hours.csv"), StandardCharsets.UTF_8))) {
            w.print("device,hour_start,outlier_rounds,points,version\n");
            for (ChainDeviceSim s : sims.values()) {
                for (Map.Entry<Long, int[]> e : new TreeMap<>(s.hours).entrySet()) {
                    w.print(s.dev + "," + e.getKey() + "," + e.getValue()[0] + "," + e.getValue()[2] + "," + e.getValue()[1] + "\n");
                }
            }
        }
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("data_days.csv"), StandardCharsets.UTF_8))) {
            w.print("device,day,channel,evaluable,reason,deviated,transition,n,d,p,p10,median,p90,shift,version\n");
            for (ChainDeviceSim s : sims.values()) {
                Map<String, String> tr = new HashMap<>();
                for (DriftEvent e : s.transitions) {
                    tr.put(e.dataDay + "|" + e.channel, e.level.name());
                }
                for (MonitoringSnapshot m : s.records) {
                    w.print(String.join(",", s.dev, m.getM5aDay(), m.getM5aChannel(), m.isM5aEvaluable() ? "1" : "0", m.getM5aReason(),
                            m.isM5aDeviated() ? "1" : "0", tr.getOrDefault(m.getM5aDay() + "|" + m.getM5aChannel(), ""),
                            Long.toString(m.getM5aN()), Double.toString(m.getM5aD()), Double.toString(m.getM5aP()),
                            Double.toString(m.getM5aP10()), Double.toString(m.getM5aMedian()), Double.toString(m.getM5aP90()),
                            Double.toString(m.getM5aShift()), m.getVersion() == null ? "" : m.getVersion().toString()) + "\n");
                }
            }
        }
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("bursts.csv"), StandardCharsets.UTF_8))) {
            w.print("device,ts,emitted_at,size,peak,duration\n");
            for (ChainDeviceSim s : sims.values()) {
                for (ChainDeviceSim.Burst b : s.bursts) {
                    w.print(s.dev + "," + b.ts + "," + b.emittedAt + "," + b.size + "," + b.peak + "," + b.duration + "\n");
                }
            }
        }
    }

    /** 一条执行的零级，格式与 M6aOfflineRunner 相同（new 为实际换上的参数）/ one executed level-0 in the runner's format. */
    private static void writeExec(ChainDeviceSim s, ChainDeviceSim.Exec x, File file) throws IOException {
        ObjectMapper om = new ObjectMapper();
        ObjectNode o = om.createObjectNode();
        o.put("device", s.dev);
        o.put("effectiveAt", x.eff);
        o.put("seq", x.seq);
        o.put("version", x.version);
        o.put("freezeTs", s.freezeTs());
        o.put("windowRounds", x.l0.windowRounds);
        o.put("guardExcluded", x.l0.guardExcluded);
        o.set("freeze", params(om, s.frozen()));
        o.set("new", params(om, x.used));
        ArrayNode hist = o.putArray("hist");
        for (RefHistogram h : x.used.hist) {
            if (h == null) {
                hist.addNull();
                continue;
            }
            ObjectNode n = hist.addObject();
            ArrayNode e = n.putArray("edges");
            for (double v : h.getEdges()) {
                e.add(v);
            }
            ArrayNode cl = n.putArray("cells");
            for (long v : h.getCells()) {
                cl.add(v);
            }
            n.put("m", h.getM());
            n.put("p10", h.getP10());
            n.put("median", h.getMedian());
            n.put("p90", h.getP90());
        }
        ObjectNode bf = o.putObject("backfill");
        ArrayNode bts = bf.putArray("ts");
        ArrayNode bx = bf.putArray("xNorm");
        for (int i = 0; i < x.backfill.size(); i++) {
            bts.add(x.backfill.ts[i]);
            ArrayNode r = bx.addArray();
            for (double v : x.backfill.xNorm[i]) {
                r.add(v);
            }
        }
        om.writeValue(file, o);
    }

    private static ObjectNode params(ObjectMapper om, RobustScalerFunction.Calibration c) {
        ObjectNode n = om.createObjectNode();
        ArrayNode med = n.putArray("median");
        ArrayNode sc = n.putArray("scale");
        ArrayNode bp = n.putArray("bypass");
        for (int i = 0; i < Channels.N_DET; i++) {
            med.add(c.median[i]);
            sc.add(c.scale[i]);
            bp.add(c.bypass[i]);
        }
        return n;
    }
}
