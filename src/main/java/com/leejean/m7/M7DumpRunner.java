package com.leejean.m7;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.m1.DeviceRound;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m5.DriftEvent;
import com.leejean.m5a.M5aCore;

import java.io.BufferedReader;
import java.io.IOException;
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
 * 聚合模块的离线转储运行器（实现方案第三节，验收项三的离线一侧）：读一次短重放的四份转储（标准化轮、评分、漂移事件、监测快照），
 * 映射为 {@link M7Input}，按每条输入在线到达的事件时间排序，经与在线算子相同的 {@link M7InputAssembler} 驱动决策函数，写出与
 * {@link M7OfflineRunner} 同格式的三份日志。节拍按事件时间定时器的语义推进：节拍 t 在小时终点后六十秒触发，在此之前到达的输入
 * 都已送入。
 * Offline dump runner for V-M7-3: maps the four dumps to M7Input, orders them by online arrival time and drives the shared
 * assembler with event-time tick semantics.
 *
 * <p>爆发的发出时刻不在漂移主题的载荷里（DriftEvent.ts 是报警元素），本类按数据层的箱规则从轮与离群记录推出：报警元素之后、
 * 报警加一小时之后第一个有入口轮且离群游程已结束的十分钟，在它的关闭时刻（十分钟末加一个滑动步）发出（M5aCore.element）。
 *
 * <pre>
 * 执行环境：集群 master（短重放编排脚本调用）或任意装有 JDK 11 的机器，仓库根目录。
 * 调用：java -cp &lt;作业 jar&gt; com.leejean.m7.M7DumpRunner --rounds m1-out.jsonl --scores scores.jsonl --drift drift.jsonl
 *       --monitoring monitoring.jsonl --out-dir offline/ [--p-point 0.061343306938749904] [其余决策参数同 M7OfflineRunner]
 *       转换在线输出：--online-alerts alerts.jsonl --online-adapt adapt.jsonl --out-dir online/
 * 前置条件：四份转储为每行一条 JSON（kafka-console-consumer 的输出，非 JSON 行忽略）；标准化轮按设备时间递增。
 * 产出：out-dir 下 alerts.csv、commands.csv、review.csv；终端打印条数与最后一个节拍。缺参数时退出码 2。
 * </pre>
 */
public final class M7DumpRunner {

    /** 默认 p_point：全量离线推导的值（离线报告第二节）/ default p_point from the full offline derivation. */
    public static final String DEFAULT_P_POINT = "0.061343306938749904";
    static final long SLIDE = FleetDecisionFunction.SLIDE;

    /** 一条输入与它在线到达的事件时间（秒）/ an input and its online arrival time. */
    public static final class Arrival {
        public final M7Input in;
        public final long at;

        public Arrival(M7Input in, long at) {
            this.in = in;
            this.at = at;
        }
    }

    private M7DumpRunner() { }

    public static void main(String[] args) throws IOException {
        Map<String, String> a = M7OfflineRunner.parseArgs(args);
        if (a.containsKey("online-alerts")) {
            convertOnline(Paths.get(a.get("online-alerts")), Paths.get(a.get("online-adapt")), Paths.get(a.get("out-dir")));
            return;
        }
        for (String k : new String[]{"rounds", "scores", "drift", "monitoring", "out-dir"}) {
            if (!a.containsKey(k)) {
                System.err.println("缺少 --" + k);
                System.exit(2);
            }
        }
        a.putIfAbsent("p-point", DEFAULT_P_POINT);
        FleetDecisionFunction.Params p = M7OfflineRunner.params(a);
        List<Arrival> xs = new ArrayList<>();
        long last = readDumps(Paths.get(a.get("rounds")), Paths.get(a.get("scores")), Paths.get(a.get("drift")),
                Paths.get(a.get("monitoring")), xs);
        M7InputAssembler asm = new M7InputAssembler(p);
        M7InputAssembler.Out out = new M7InputAssembler.Out();
        long lastTick = drive(asm, xs, last - 1, out);       // 在线最终水位线约为最后一条轮时刻减一毫秒 / final watermark
        Path dir = Paths.get(a.get("out-dir"));
        Files.createDirectories(dir);
        M7OfflineRunner.writeAlerts(out.alerts, dir.resolve("alerts.csv"));
        M7OfflineRunner.writeCommands(out.commands, dir.resolve("commands.csv"));
        M7OfflineRunner.writeReview(out.review, dir.resolve("review.csv"));
        System.out.println("告警 " + out.alerts.size() + " 条；命令 " + out.commands.size() + " 条；复核 " + out.review.size()
                + " 条；最后一个节拍 " + lastTick);
        System.out.println("最后一条轮时刻 " + last);
    }

    /**
     * 按到达时刻推进：送入到达时刻为 a 的输入之前，先触发所有触发时刻（节拍加六十秒）早于 a 的节拍；最后触发到 end 为止。
     * 第一个节拍是第一条输入所在窗口的终点。返回最后一个节拍。
     * Drives the assembler in arrival order with event-time timer semantics; returns the last tick.
     */
    public static long drive(M7InputAssembler asm, List<Arrival> xs, long end, M7InputAssembler.Out out) {
        xs.sort(Comparator.comparingLong(x -> x.at));
        if (xs.isEmpty()) {
            return Long.MIN_VALUE;
        }
        long len = asm.alertSec();
        long next = asm.windowOf(xs.get(0).at) + len;
        for (Arrival x : xs) {
            while (next + SLIDE < x.at) {
                asm.tick(next, out);
                next += len;
            }
            asm.accept(x.in);
        }
        while (next + SLIDE <= end) {
            asm.tick(next, out);
            next += len;
        }
        return next - len;
    }

    /** 读四份转储，生成到达序列；返回最后一条轮的时刻 / read the four dumps; returns the last round time. */
    public static long readDumps(Path rounds, Path scores, Path drift, Path monitoring, List<Arrival> xs) throws IOException {
        ObjectMapper om = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        long last = Long.MIN_VALUE;
        Map<String, TreeMap<Long, long[]>> bins = new HashMap<>();          // 设备 → 十分钟末 → {入口轮, 离群轮}
        try (BufferedReader br = Files.newBufferedReader(rounds, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("{")) {
                    continue;
                }
                DeviceRound r = om.readValue(line, DeviceRound.class);
                M7Input in = M7Input.fromRound(r);
                xs.add(new Arrival(in, r.getTs()));
                last = Math.max(last, r.getTs());
                if (in.flag2) {
                    bins.computeIfAbsent(r.getDevice(), k -> new TreeMap<>()).computeIfAbsent(binEnd(r.getTs()),
                            k -> new long[2])[0]++;
                }
            }
        }
        try (BufferedReader br = Files.newBufferedReader(scores, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("{")) {
                    continue;
                }
                JsonNode n = om.readTree(line);
                String ch = n.path("channel").asText();
                String dev = n.path("device").asText();
                if ("m2_point".equals(ch)) {
                    long rt = n.path("roundTs").asLong();
                    long we = n.path("windowEnd").asLong();
                    if (n.path("outlier").asBoolean(false) && we - SLIDE <= rt && rt < we) {
                        M7Input in = new M7Input();
                        in.kind = M7Input.OUTLIER;
                        in.device = dev;
                        in.ts = rt;
                        in.ts2 = we;
                        xs.add(new Arrival(in, we));
                        bins.computeIfAbsent(dev, k -> new TreeMap<>()).computeIfAbsent(binEnd(rt), k -> new long[2])[1]++;
                    }
                } else if ("m3_context".equals(ch)) {
                    M7Input in = new M7Input();
                    in.kind = M7Input.CTX;
                    in.device = dev;
                    in.ts = n.path("windowEnd").asLong();
                    in.v1 = n.path("mainScore").asDouble();
                    in.flag1 = n.path("aboveThreshold").asBoolean(false);
                    xs.add(new Arrival(in, in.ts));
                }
            }
        }
        List<DriftEvent> bursts = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(drift, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("{")) {
                    continue;
                }
                DriftEvent e = om.readValue(line, DriftEvent.class);
                if (e.level == DriftEvent.Level.BURST) {
                    bursts.add(e);
                    continue;
                }
                M7Input in = M7Input.fromDrift(e, e.ts);
                if (in != null) {
                    xs.add(new Arrival(in, e.ts));
                }
            }
        }
        for (DriftEvent e : bursts) {
            long emitted = burstEmission(bins.getOrDefault(e.device, new TreeMap<>()), e.ts);
            if (emitted > 0) {
                M7Input in = M7Input.fromDrift(e, emitted);
                xs.add(new Arrival(in, emitted));
            }
        }
        try (BufferedReader br = Files.newBufferedReader(monitoring, StandardCharsets.UTF_8)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.startsWith("{")) {
                    continue;
                }
                JsonNode n = om.readTree(line);
                if (n.path("windowEnd").asLong() != M5aCore.DAY_RECORD_WINDOW_END) {
                    continue;
                }
                M7Input in = M7Input.fromRecord(om.treeToValue(n, MonitoringSnapshot.class));
                if (in != null) {
                    xs.add(new Arrival(in, in.ts));
                }
            }
        }
        return last;
    }

    /**
     * 在线输出（告警主题与命令主题的 JSON 转储）转成与离线同格式的三份日志 / convert the online JSON dumps into the three logs.
     */
    public static void convertOnline(Path alerts, Path adapt, Path dir) throws IOException {
        ObjectMapper om = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        M7InputAssembler.Out out = new M7InputAssembler.Out();
        for (String line : Files.readAllLines(alerts, StandardCharsets.UTF_8)) {
            if (!line.startsWith("{")) {
                continue;
            }
            JsonNode n = om.readTree(line);
            if ("alert".equals(n.path("type").asText())) {
                out.alerts.add(om.treeToValue(n, FleetDecisionFunction.Alert.class));
            } else if ("review".equals(n.path("type").asText())) {
                out.review.add(om.treeToValue(n, FleetDecisionFunction.Review.class));
            }
        }
        for (String line : Files.readAllLines(adapt, StandardCharsets.UTF_8)) {
            if (line.startsWith("{")) {
                out.commands.add(om.treeToValue(om.readTree(line), FleetDecisionFunction.Command.class));
            }
        }
        Files.createDirectories(dir);
        M7OfflineRunner.writeAlerts(out.alerts, dir.resolve("alerts.csv"));
        M7OfflineRunner.writeCommands(out.commands, dir.resolve("commands.csv"));
        M7OfflineRunner.writeReview(out.review, dir.resolve("review.csv"));
        System.out.println("在线：告警 " + out.alerts.size() + " 条；命令 " + out.commands.size() + " 条；复核 " + out.review.size() + " 条");
    }

    static long binEnd(long ts) {
        return Math.floorDiv(ts, 600L) * 600L + 600L;
    }

    /**
     * 爆发的发出时刻：报警元素 b0 之后的元素（只有有入口轮的十分钟才是元素）依次核对，离群游程（报警元素起连续离群率大于零）结束、
     * 且元素晚于 b0 加一小时时发出，发出时刻为该十分钟末加一个滑动步；数据末尾仍未发出的返回 0。
     * Emission time of a burst under M5aCore.element's rule; 0 if never emitted.
     */
    static long burstEmission(TreeMap<Long, long[]> bins, long b0) {
        long[] first = bins.get(b0);
        boolean runOpen = first != null && first[0] > 0 && first[1] > 0;
        for (Map.Entry<Long, long[]> e : bins.tailMap(b0, false).entrySet()) {
            long[] v = e.getValue();
            if (v[0] <= 0) {
                continue;
            }
            if (runOpen && v[1] == 0) {
                runOpen = false;
            }
            if (e.getKey() > b0 + 3600 && !runOpen) {
                return e.getKey() + SLIDE;
            }
        }
        return 0;
    }
}
