package com.leejean.m3;

import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m6.AdaptCommand;
import com.leejean.m6.ContextInputBuilder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 第三批交接文档第二节：一级切换的离线复算。对一次全量重放的每台设备，按那次运行自己的命令序列（迟到未执行的命令剔除）用链式模拟器
 * 重建上下文模块的输入（{@link ContextInputBuilder}），喂给与在线相同的上下文模块（{@link M3Operator} 与 {@link M3Function}，同一套
 * 参数），得到每次一级切换的新模型与切换前验证比值；在每条一级命令生效之前对算子状态做快照，从快照恢复后把同一段轮去掉命令再喂一遍，
 * 得到旧模型（上一代）在切换后七天的评分。两者在切换后七天的窗口上比较超阈率、主分中位数与第九十五百分位，并列出七天内是否有新的
 * 一级或二级。训练快慢不影响输出（补遗四），所以每次进入训练都立即等训练完成并收回。
 * Batch 3 §2: offline recompute of every level-1 switch, with the old model scored on the same week via a state snapshot.
 *
 * <p>脚本交付五要素：
 * <ol>
 *   <li>执行环境：开发机，仓库根目录，JDK 11；先 mvn test-compile 与 mvn dependency:build-classpath -Dmdep.includeScope=test
 *       -Dmdep.outputFile=target/test-cp.txt；每个进程设 OMP_NUM_THREADS=2。</li>
 *   <li>调用命令：{@code java -cp target/classes:target/test-classes:$(cat target/test-cp.txt) com.leejean.m3.M6cSwitchRecompute
 *       --run-dir docs/m6b/full/a --label rerun_a --out docs/m6c/recompute/rerun_a.csv}；可选 --devices A,B（默认为有一级命令的
 *       设备）、--tm-log（默认 run-dir 下 tm_log.txt，用于取在线比值与剔除迟到命令）、--rounds-dir docs/m6a/extract、
 *       --rounds-E docs/m6b/extract/rounds_E_inj.csv.gz、--last-day 2022-04-30。</li>
 *   <li>前置条件：run-dir 下有 adapt.jsonl 与 tm_log.txt；E 的注入轮表已生成（m6a/inject_rounds.py）。</li>
 *   <li>期望产出：out 处每次切换一行（表头见 {@link #HEADER}）；终端逐台打印进度。</li>
 *   <li>失败兜底：缺输入时退出码 2。</li>
 * </ol>
 */
public final class M6cSwitchRecompute {
    private M6cSwitchRecompute() {
    }

    static final OutputTag<MonitoringSnapshot> MON = new OutputTag<MonitoringSnapshot>("m3-monitoring") { };
    static final String HEADER = "run,device,seq,effective_at,switch_at,online_ratio,offline_ratio,new_windows,new_above_rate,"
            + "new_median,new_p95,old_windows,old_above_rate,old_median,old_p95,new_level12_within_7d";
    private static final long WEEK = 7 * 86400L;

    /** 与在线作业相同的上下文模块（M2Job 的默认参数与全量重放的设置）/ the context module as in the online job. */
    static M3Function newFunction() {
        return new M3Function(7, 2, 7, 60, 2.22, null, 300, 20, 60, 64, true, 0.001, 39072.0, MON,
                M3Function.DEFAULT_ROUNDS_PER_DAY, 2, M3Function.DEFAULT_DRAIN_BATCH, M3Function.DEFAULT_POLL_INTERVAL_SEC)
                .withRetrain(3, false, true)
                .withRetrainDaySec(86400L)
                .withSwitchCheck(3.0, false)
                .withShadowScoring(false);
    }

    private static Map<String, String> args(String[] a) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < a.length; i += 2) {
            m.put(a[i].replaceFirst("^--", ""), a[i + 1]);
        }
        return m;
    }

    public static void main(String[] argv) throws Exception {
        Map<String, String> a = args(argv);
        Path runDir = Paths.get(a.getOrDefault("run-dir", "docs/m6b/full/a"));
        Path tmLog = Paths.get(a.getOrDefault("tm-log", runDir.resolve("tm_log.txt").toString()));
        Path adapt = runDir.resolve("adapt.jsonl");
        if (!Files.exists(adapt) || !Files.exists(tmLog)) {
            System.err.println("缺少 " + adapt + " 或 " + tmLog);
            System.exit(2);
        }
        String label = a.getOrDefault("label", runDir.getFileName().toString());
        String roundsDir = a.getOrDefault("rounds-dir", "docs/m6a/extract");
        String roundsE = a.getOrDefault("rounds-E", "docs/m6b/extract/rounds_E_inj.csv.gz");
        String lastDay = a.getOrDefault("last-day", "2022-04-30");
        List<String> log = Files.readAllLines(tmLog, StandardCharsets.UTF_8);
        // 迟到未执行的命令剔除（第一次全量重放）/ drop late commands that were never executed
        TreeSet<String> late = new TreeSet<>();
        Pattern lp = Pattern.compile("Late command \\(not executed[^)]*\\): device (\\S+) seq (\\d+)");
        Pattern sp = Pattern.compile("\\[M3\\] Device (\\S+) level-1 switch check: .* ratio ([0-9.Ee-]+) ");
        Map<String, List<Double>> online = new LinkedHashMap<>();
        for (String l : log) {
            Matcher m = lp.matcher(l);
            if (m.find()) {
                late.add(m.group(1) + "|" + m.group(2));
            }
            Matcher s = sp.matcher(l);
            if (s.find()) {
                online.computeIfAbsent(s.group(1), k -> new ArrayList<>()).add(Double.parseDouble(s.group(2)));
            }
        }
        Map<String, List<AdaptCommand>> cmds = new LinkedHashMap<>();
        for (String l : Files.readAllLines(adapt, StandardCharsets.UTF_8)) {
            if (l.isBlank() || !l.contains("\"type\":\"command\"")) {
                continue;
            }
            AdaptCommand c = AdaptCommand.parse(l);
            if (!late.contains(c.device + "|" + c.seq)) {
                cmds.computeIfAbsent(c.device, k -> new ArrayList<>()).add(c);
            }
        }
        List<String> devices = new ArrayList<>();
        if (a.containsKey("devices")) {
            devices.addAll(Arrays.asList(a.get("devices").split(",")));
        } else {
            for (Map.Entry<String, List<AdaptCommand>> e : cmds.entrySet()) {
                if (e.getValue().stream().anyMatch(c -> c.level == 1)) {
                    devices.add(e.getKey());
                }
            }
        }
        Path out = Paths.get(a.getOrDefault("out", "docs/m6c/recompute/" + label + ".csv"));
        Files.createDirectories(out.toAbsolutePath().getParent());
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8))) {
            w.println(HEADER);
            for (String dev : devices) {
                String path = "E".equals(dev) ? roundsE : roundsDir + "/rounds_" + dev + ".csv.gz";
                List<AdaptCommand> dc = cmds.getOrDefault(dev, new ArrayList<>());
                System.out.println("[recompute] " + label + " " + dev + ": " + dc.size() + " commands; building the context input");
                List<AnnotatedRound> rounds = ContextInputBuilder.build(dev, path, dc, lastDay);
                for (String row : device(label, dev, rounds, dc, online.getOrDefault(dev, new ArrayList<>()))) {
                    w.println(row);
                    w.flush();
                    System.out.println("[recompute] " + row);
                }
            }
        }
    }

    /** 一台设备：全程运行、每条一级命令前快照，再逐条做旧模型的对照 / one device: full run, then the old-model counterfactuals. */
    static List<String> device(String label, String dev, List<AnnotatedRound> rounds, List<AdaptCommand> cmds,
                               List<Double> onlineRatios) throws Exception {
        List<AnnotatedRound> plain = new ArrayList<>();
        for (AnnotatedRound r : rounds) {
            plain.add(copyWithoutCommand(r));
        }
        Map<Integer, OperatorSubtaskState> snaps = new LinkedHashMap<>();
        Map<Integer, Integer> snapIdx = new LinkedHashMap<>();
        Map<Integer, Long> snapProc = new LinkedHashMap<>();
        Map<Integer, Long> snapEff = new LinkedHashMap<>();
        List<M3ScoreRecord> scores = new ArrayList<>();
        List<MonitoringSnapshot> mons = new ArrayList<>();
        M3Function f = newFunction();
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h = harness(f)) {
            h.open();
            for (int i = 0; i < rounds.size(); i++) {
                AnnotatedRound r = rounds.get(i);
                if (r.getContextCommand() != null && r.getContextCommand().level == 1) {
                    int seq = r.getContextCommand().seq;
                    snaps.put(seq, h.snapshot(i, r.getTs() * 1000L));
                    snapIdx.put(seq, i);
                    snapProc.put(seq, h.getProcessingTime());
                    snapEff.put(seq, r.getContextCommand().effectiveAt);
                }
                h.processElement(r, r.getTs() * 1000L);
                settleIfBuffering(f, h, dev);
            }
            settleIfBuffering(f, h, dev);
            collect(h, scores, mons);
        }
        // 切换事件与切换前验证比值（监测快照）/ switch events and pre-switch check ratios
        List<MonitoringSnapshot> switches = new ArrayList<>();
        List<MonitoringSnapshot> checks = new ArrayList<>();
        for (MonitoringSnapshot s : mons) {
            if (s.getWindowEnd() == M3Function.MODEL_EVENT_WINDOW_END && "switch".equals(s.getM3ModelEvent())) {
                switches.add(s);
            }
            if (s.getM3SwitchCheckRatio() != null) {
                checks.add(s);
            }
        }
        List<String> rows = new ArrayList<>();
        int k = 0;
        for (Map.Entry<Integer, Long> e : snapEff.entrySet()) {
            int seq = e.getKey();
            long eff = e.getValue();
            MonitoringSnapshot sw = null;
            for (MonitoringSnapshot s : switches) {
                if (s.getTs() >= eff && (sw == null || s.getTs() < sw.getTs())) {
                    sw = s;
                }
            }
            if (sw == null) {
                continue;                                     // 这条一级没有切换（被忽略或失败）/ no switch for this level 1
            }
            final long at = sw.getTs();
            // 切换之前还有更晚生效的一级，说明这条被忽略（例如冷启动训练中到达），切换属于后者
            // A later level 1 took effect before this switch: this one was ignored and the switch belongs to the later one
            boolean superseded = false;
            for (Map.Entry<Integer, Long> o : snapEff.entrySet()) {
                superseded |= o.getKey() != seq && o.getValue() > eff && o.getValue() <= at;
            }
            if (superseded) {
                continue;
            }
            Double ratio = null;
            for (MonitoringSnapshot c : checks) {
                if (c.getTs() == at) {
                    ratio = c.getM3SwitchCheckRatio();
                }
            }
            List<Double> newer = new ArrayList<>();
            List<Boolean> newAbove = new ArrayList<>();
            for (M3ScoreRecord s : scores) {
                if (s.getWindowEnd() >= at && s.getWindowEnd() < at + WEEK && sw.getM3ModelGen().equals(s.getModel())) {
                    newer.add(s.getMainScore());
                    newAbove.add(s.isAboveThreshold());
                }
            }
            // 旧模型：从命令之前的快照恢复，同一段轮去掉命令再喂 / old model: restore and replay the same rounds without commands
            List<Double> older = new ArrayList<>();
            List<Boolean> oldAbove = new ArrayList<>();
            M3Function g = newFunction();
            try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h2 = harness(g)) {
                h2.setup();
                h2.initializeState(snaps.get(seq));
                h2.open();
                h2.setProcessingTime(snapProc.get(seq));
                for (int i = snapIdx.get(seq); i < plain.size() && plain.get(i).getTs() < at + WEEK; i++) {
                    AnnotatedRound r = copyWithoutCommand(plain.get(i));
                    h2.processElement(r, r.getTs() * 1000L);
                    settleIfBuffering(g, h2, dev);
                }
                List<M3ScoreRecord> s2 = new ArrayList<>();
                collect(h2, s2, new ArrayList<>());
                for (M3ScoreRecord s : s2) {
                    if (s.getWindowEnd() >= at && s.getWindowEnd() < at + WEEK) {
                        older.add(s.getMainScore());
                        oldAbove.add(s.isAboveThreshold());
                    }
                }
            }
            boolean next = false;
            for (AdaptCommand c : cmds) {
                next |= c.level >= 1 && c.decidedAt > at && c.decidedAt <= at + WEEK;
            }
            Double on = k < onlineRatios.size() ? onlineRatios.get(k) : null;
            k++;
            rows.add(String.join(",", label, dev, Integer.toString(seq), Long.toString(eff), Long.toString(at),
                    on == null ? "" : Double.toString(on), ratio == null ? "" : Double.toString(ratio),
                    Integer.toString(newer.size()), Double.toString(rate(newAbove)), Double.toString(quantile(newer, 0.5)),
                    Double.toString(quantile(newer, 0.95)), Integer.toString(older.size()), Double.toString(rate(oldAbove)),
                    Double.toString(quantile(older, 0.5)), Double.toString(quantile(older, 0.95)), next ? "1" : "0"));
        }
        return rows;
    }

    static KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> harness(M3Function f) throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h =
                new KeyedOneInputStreamOperatorTestHarness<>(new M3Operator(f), AnnotatedRound::getDevice,
                        TypeInformation.of(String.class));
        // 快照放在内存里，默认上限 5 MB 不够（F 约 5.9 MB）。Flink 1.13.6 测试工具的 setCheckpointStorage 在状态后端本身是
        // 检查点存储（默认 MemoryStateBackend）时直接返回，所以先换成同为堆内的 HashMapStateBackend，再设存储。
        // In-memory snapshots exceed the 5 MB default (F is about 5.9 MB). The 1.13.6 harness ignores setCheckpointStorage while
        // the backend is itself a checkpoint storage (the default MemoryStateBackend), so switch to the heap-based HashMapStateBackend first.
        h.setStateBackend(new HashMapStateBackend());
        h.setCheckpointStorage(new JobManagerCheckpointStorage(256 * 1024 * 1024));
        return h;
    }

    /** 进入训练（开始缓冲）时等训练完成、收回结果并排空 / on buffering, await the training, collect it and drain. */
    static void settleIfBuffering(M3Function f, KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h,
                                  String dev) throws Exception {
        if (f.bufferedRounds(dev) == 0) {
            return;
        }
        f.awaitTraining(dev, 3_600_000L);
        h.setProcessingTime(h.getProcessingTime() + M3Function.DEFAULT_POLL_INTERVAL_SEC * 1000L + 1);
        for (int k = 0; k < 100_000 && f.bufferedRounds(dev) > 0; k++) {
            h.setProcessingTime(h.getProcessingTime() + 1);
        }
    }

    static void collect(KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h,
                        List<M3ScoreRecord> scores, List<MonitoringSnapshot> mons) {
        for (Object o : h.getOutput()) {
            if (o instanceof StreamRecord && ((StreamRecord<?>) o).getValue() instanceof M3ScoreRecord) {
                scores.add((M3ScoreRecord) ((StreamRecord<?>) o).getValue());
            }
        }
        if (h.getSideOutput(MON) != null) {
            for (StreamRecord<MonitoringSnapshot> s : h.getSideOutput(MON)) {
                mons.add(s.getValue());
            }
        }
    }

    static AnnotatedRound copyWithoutCommand(AnnotatedRound r) {
        AnnotatedRound c = new AnnotatedRound(r.getDevice(), r.getTs(), r.getXNorm(), r.isOutlier(), r.getCensoredMask(),
                r.isColdStart(), r.getWindowEnd());
        c.setVersion(r.getVersion());
        c.setRaw(r.getRaw());
        c.setParamSet(r.getParamSet());
        return c;
    }

    static double rate(List<Boolean> xs) {
        if (xs.isEmpty()) {
            return Double.NaN;
        }
        long n = xs.stream().filter(b -> b).count();
        return (double) n / xs.size();
    }

    /** 最近秩分位数 / nearest-rank quantile. */
    static double quantile(List<Double> xs, double q) {
        if (xs.isEmpty()) {
            return Double.NaN;
        }
        List<Double> s = new ArrayList<>(xs);
        s.sort(Double::compare);
        int i = (int) Math.ceil(q * s.size()) - 1;
        return s.get(Math.max(0, Math.min(s.size() - 1, i)));
    }
}
