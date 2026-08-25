package com.leejean.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 将 Erol/SYNERGIA 数据集重放进 synergia-source，仿佛实时流（交接文档 §3，Deliverable A）。
 * Replays the Erol/SYNERGIA dataset into synergia-source as if it were live (handover §3).
 *
 * <p>骨架（ParameterTool、producer 属性、offset 断点续跑、进度打印）借自 FA-iForest 的
 * FileToKafkaProducer；核心逻辑为新增：全局归并 + 显式分区 + 节奏控制/空闲压缩。
 * The skeleton (ParameterTool, producer properties, offset-file resume, progress printing) is
 * borrowed from FA-iForest's FileToKafkaProducer; the core logic is new.
 *
 * <p>========================= 脚本交付五要素 / Five delivery elements =========================
 * <ul>
 *   <li><b>执行环境 / Environment</b>：集群 master 上 docker-exec 进 jobmanager/kafka 容器内运行
 *       （前台，见操作规则 §3.5）；JDK 8；shaded jar 在 /opt/fa-iforest/jars/。</li>
 *   <li><b>调用命令 / Invocation</b>：
 *       {@code java -cp <jar> com.leejean.source.CsvKafkaReplayer --data-dir <dir>
 *       --brokers <b1:9092,...> --topic synergia-source --speedup 3600 --max-idle-wall 2000}</li>
 *   <li><b>前置条件 / Preconditions</b>：数据集已上传至 --data-dir；synergia-source 已建（8 分区）。</li>
 *   <li><b>期望产出 / Expected output</b>：全局非降序事件时间的消息流；进度与压缩审计日志到 stdout；
 *       offset 文件到 &lt;data-dir&gt;/.replayer.offset。</li>
 *   <li><b>失败兜底 / Failure fallback</b>：--dry-run 只归并+日志不生产；--resume 从 offset 续跑；
 *       畸形文件/行计数并跳过不崩溃；须**前台**运行（后台 producer 收 SIGHUP 会死，继承的坑）。</li>
 * </ul>
 * ==========================================================================================
 */
public class CsvKafkaReplayer {

    private static final int PROGRESS_INTERVAL = 50_000;   // 每 N 条打印进度 / progress every N records
    private static final String OFFSET_FILE = ".replayer.offset";
    private static final long CARRY_OVER_SEC = 60L;        // 跨文件归并缓冲 / cross-file carry-over

    // 文件名两种模式 + 紧凑变体（EDA 实测）/ two filename patterns + the compact variant (per EDA)
    private static final Pattern NAME_RE = Pattern.compile(
            "^(\\d{4})_(\\d{2})_(\\d{2})_(\\d{2})[-_]?(\\d{2})[-_]?(\\d{2})(?:_data)?\\.csv$",
            Pattern.CASE_INSENSITIVE);

    // ------------------------------------------------------------------
    // main
    // ------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String dataDir = params.getRequired("data-dir");
        String topic = params.get("topic", "synergia-source");
        String brokers = params.get("brokers", params.get("broker", "localhost:9092"));
        double speedup = params.getDouble("speedup", 1.0);
        long maxIdleWallMs = params.getLong("max-idle-wall", 2000L);
        int numPartitions = params.getInt("num-partitions", 8);
        // 裸标志（--dry-run / --resume，无值）ParameterTool.getBoolean 会解析成 false，故用 flag() 判定存在性。
        // Valueless flags (--dry-run / --resume) parse to false under getBoolean; use flag() for presence.
        boolean dryRun = flag(params, "dry-run");
        boolean resume = flag(params, "resume");
        boolean strictNames = flag(params, "strict-csv-names");
        long startSec = parseInstant(params.get("start", ""), Long.MIN_VALUE);
        long endSec = parseInstant(params.get("end", ""), Long.MAX_VALUE);

        Path dir = Paths.get(dataDir);
        // offset 文件可覆写到可写路径（容器内挂载目录可能只读/属主不符）/ offset path is overridable
        Path offsetPath = Paths.get(params.get("offset-file", dir.resolve(OFFSET_FILE).toString()));

        Stats stats = new Stats();
        List<FileEntry> files = discoverFiles(dir, stats, strictNames);
        long[] resumeState = resume ? loadOffset(offsetPath) : new long[]{-1L, -1L};

        System.out.println("========================================");
        System.out.println("CsvKafkaReplayer");
        System.out.println("Data dir:      " + dir.toAbsolutePath());
        System.out.println("Topic:         " + topic);
        System.out.println("Brokers:       " + brokers);
        System.out.println("Speedup k:     " + speedup);
        System.out.println("Max idle wall: " + maxIdleWallMs + " ms");
        System.out.println("Partitions:    " + numPartitions + " (explicit A->0..H->7)");
        System.out.println("Segment:       [" + (startSec == Long.MIN_VALUE ? "-inf" : startSec)
                + ", " + (endSec == Long.MAX_VALUE ? "+inf" : endSec) + "] (epoch seconds)");
        System.out.println("Data files:    " + files.size()
                + " (csv-named=" + stats.namedDataFiles + ", sniffed non-.csv=" + stats.sniffedDataFiles
                + ", skipped non-data=" + stats.skippedNonData + ")");
        System.out.println("Strict names:  " + strictNames);
        System.out.println("Mode:          " + (dryRun ? "DRY-RUN (no produce)" : "PRODUCE")
                + (resume ? " / RESUME from fileIdx=" + resumeState[0] + " line=" + resumeState[1] : ""));
        System.out.println("========================================");

        RecordSink sink = dryRun
                ? new DryRunSink()
                : new KafkaSink(brokers, topic, numPartitions);

        Pacer pacer = new Pacer(speedup, maxIdleWallMs, dryRun);

        try {
            GlobalMerger merger = new GlobalMerger(CARRY_OVER_SEC, row -> {
                if (row.ts < startSec || row.ts > endSec) {
                    return;   // 事件时间段选择 / event-time segment selection
                }
                if (resume && !afterResume(row, resumeState)) {
                    return;   // 断点续跑：跳过已发送 / resume: skip already-sent
                }
                pacer.pace(row.ts);
                int partition = DevicePartition.partitionFor(row.device, numPartitions);
                if (!DevicePartition.isKnown(row.device)) {
                    stats.unknownDevice++;
                }
                sink.emit(topic, partition, row.ts * 1000L, row.device, row.rawLine);
                stats.produced++;
                stats.lastFileIdx = row.fileIdx;
                stats.lastLineNo = row.lineNo;
                if (stats.produced % PROGRESS_INTERVAL == 0) {
                    System.out.println("[Progress] produced=" + stats.produced
                            + " fileIdx=" + row.fileIdx + " line=" + row.lineNo
                            + " eventTs=" + row.ts);
                    saveOffset(offsetPath, row.fileIdx, row.lineNo);
                }
            });

            for (int fi = 0; fi < files.size(); fi++) {
                FileEntry fe = files.get(fi);
                List<Row> rows = loadFile(fe.path, fi, stats);
                merger.offer(rows);
            }
            merger.flush();
        } finally {
            sink.close();
            saveOffset(offsetPath, stats.lastFileIdx, stats.lastLineNo);
        }

        System.out.println("========================================");
        System.out.println("Finished. Produced:        " + stats.produced);
        System.out.println("Skipped malformed lines:   " + stats.malformedLines);
        System.out.println("Data files (csv/sniffed):  " + stats.namedDataFiles + " / " + stats.sniffedDataFiles);
        System.out.println("Skipped non-data files:    " + stats.skippedNonData);
        System.out.println("File read errors:          " + stats.skippedFiles);
        System.out.println("Unknown-device records:    " + stats.unknownDevice);
        System.out.println("Idle-compression events:   " + pacer.compressionEvents);
        System.out.println("Total compressed wall:     " + pacer.totalCompressedMs + " ms");
        System.out.println("========================================");
    }

    // ------------------------------------------------------------------
    // 文件发现与解析 / file discovery and parsing
    // ------------------------------------------------------------------

    /** 一个待重放文件及其排序键 / a file plus its ordering key. */
    static final class FileEntry {
        final Path path;
        final long orderKey;   // 文件名时间；内容嗅探数据文件用首行时间 / filename time, or first-row time for sniffed data
        final boolean sniffed; // true=非 .csv 命名但内容为数据 / non-.csv-named but content is data
        FileEntry(Path path, long orderKey, boolean sniffed) {
            this.path = path;
            this.orderKey = orderKey;
            this.sniffed = sniffed;
        }
    }

    /**
     * 发现 data-dir 下的数据文件，按时间排序；**无声跳过是 EDA 阶段的教训禁区**。
     * Discover data files under data-dir, ordered by time. Silent skipping is the EDA-stage lesson to avoid.
     *
     * <p>三类归属并逐类计数 / three classes, each counted:
     * <ul>
     *   <li>CSV 命名数据文件（两种模式 + 紧凑变体）/ CSV-named data files (two patterns + compact variant);</li>
     *   <li>非 .csv 命名但**内容为四列 schema** 的数据文件——默认**照常纳入并告警**（EDA 补丁 01 已证
     *       非 .csv 扩展名的数据文件存在，静默丢弃会造成数据损失）；`--strict-csv-names` 可只取 .csv 命名；
     *       non-.csv-named files whose content matches the 4-col schema — included by default WITH A WARNING
     *       (EDA patch 01 proved such files exist; dropping them silently loses data); `--strict-csv-names`
     *       restricts discovery to .csv-named files;</li>
     *   <li>非数据文件（说明/压缩包/二进制）——计数并报告样例 / non-data files, counted with sample names.</li>
     * </ul>
     */
    static List<FileEntry> discoverFiles(Path dir, Stats stats, boolean strictNames) throws IOException {
        List<FileEntry> out = new ArrayList<>();
        List<String> nonDataSamples = new ArrayList<>();
        List<String> sniffedSamples = new ArrayList<>();
        List<Path> all;
        try (Stream<Path> walk = Files.walk(dir)) {
            all = walk.filter(Files::isRegularFile).collect(Collectors.toList());
        }
        for (Path p : all) {
            String name = p.getFileName().toString();
            if (name.startsWith(".")) {
                continue;   // 跳过隐藏/状态文件（如 .replayer.offset）/ skip hidden/state files
            }
            long nameTs = parseFilenameTime(name);
            if (nameTs != Long.MAX_VALUE) {
                out.add(new FileEntry(p, nameTs, false));
                stats.namedDataFiles++;
                continue;
            }
            if (strictNames) {
                stats.skippedNonData++;
                if (nonDataSamples.size() < 5) {
                    nonDataSamples.add(name);
                }
                continue;
            }
            // 非 .csv 命名 → 内容嗅探首行时间 / non-.csv name → sniff first-row time
            long firstTs = sniffFirstRowTs(p);
            if (firstTs != Long.MAX_VALUE) {
                out.add(new FileEntry(p, firstTs, true));
                stats.sniffedDataFiles++;
                if (sniffedSamples.size() < 10) {
                    sniffedSamples.add(name);
                }
            } else {
                stats.skippedNonData++;
                if (nonDataSamples.size() < 5) {
                    nonDataSamples.add(name);
                }
            }
        }
        out.sort(Comparator.comparingLong((FileEntry fe) -> fe.orderKey)
                .thenComparing(fe -> fe.path.toString()));

        // 逐类计数与告警（"零静默跳过"）/ per-class counts and warnings ("zero silent skips")
        System.out.println("[discover] csv-named data files: " + stats.namedDataFiles
                + " ; sniffed non-.csv data files: " + stats.sniffedDataFiles
                + " ; skipped non-data files: " + stats.skippedNonData);
        if (stats.sniffedDataFiles > 0) {
            System.out.println("[discover][WARN] 发现 " + stats.sniffedDataFiles
                    + " 个非 .csv 命名但内容为数据的文件，已纳入重放（EDA 补丁 01 现象）；样例="
                    + sniffedSamples + " ；如需只取 .csv 命名请加 --strict-csv-names。");
        }
        if (!nonDataSamples.isEmpty()) {
            System.out.println("[discover] non-data sample names: " + nonDataSamples);
        }
        return out;
    }

    /**
     * 嗅探文件首个非空非表头行的时间字段；像四列数据则返回其 epoch 秒，否则 Long.MAX_VALUE。
     * Sniff the first non-empty, non-header line: return its epoch-second time if it looks like the
     * 4-column schema, else Long.MAX_VALUE.
     */
    static long sniffFirstRowTs(Path p) {
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(Files.newInputStream(p), StandardCharsets.UTF_8))) {
            String line;
            int probed = 0;
            while ((line = r.readLine()) != null && probed < 5) {
                if (line.isEmpty()) {
                    continue;
                }
                probed++;
                String[] f = line.split(",", -1);
                if (f.length >= 2 && "Time".equalsIgnoreCase(f[0].trim())
                        && "DeviceId".equalsIgnoreCase(f[1].trim())) {
                    continue;   // 表头 / header
                }
                if (f.length == 4) {
                    try {
                        long ts = Long.parseLong(f[0].trim());
                        Double.parseDouble(f[3].trim());
                        return ts;   // 四列且时间/数值可解析 → 数据文件 / looks like data
                    } catch (NumberFormatException ignore) {
                        return Long.MAX_VALUE;
                    }
                }
                return Long.MAX_VALUE;
            }
        } catch (IOException e) {
            return Long.MAX_VALUE;
        }
        return Long.MAX_VALUE;
    }

    /** 解析文件名时间为 epoch 秒（UTC，仅排序用）；不匹配返回 Long.MAX_VALUE。 */
    static long parseFilenameTime(String name) {
        Matcher m = NAME_RE.matcher(name);
        if (!m.matches()) {
            return Long.MAX_VALUE;
        }
        try {
            int y = Integer.parseInt(m.group(1));
            int mo = Integer.parseInt(m.group(2));
            int d = Integer.parseInt(m.group(3));
            int h = Integer.parseInt(m.group(4));
            int mi = Integer.parseInt(m.group(5));
            int s = Integer.parseInt(m.group(6));
            return LocalDate.of(y, mo, d).atTime(h, mi, s).toEpochSecond(ZoneOffset.UTC);
        } catch (RuntimeException e) {
            return Long.MAX_VALUE;
        }
    }

    /** 归并单元：一行的时间、设备与原始文本 / a merge unit: time, device, and the raw line. */
    static final class Row {
        final long ts;
        final String device;
        final String rawLine;
        final int fileIdx;
        final int lineNo;
        Row(long ts, String device, String rawLine, int fileIdx, int lineNo) {
            this.ts = ts;
            this.device = device;
            this.rawLine = rawLine;
            this.fileIdx = fileIdx;
            this.lineNo = lineNo;
        }
    }

    /**
     * 载入单文件，解析出可重放行（跳过表头与无法取得时间/设备的畸形行），按时间排序。
     * Load one file into replayable rows (skipping the header and lines lacking a time/device),
     * sorted by time. Malformed lines are counted, not fatal.
     */
    static List<Row> loadFile(Path path, int fileIdx, Stats stats) {
        List<Row> rows = new ArrayList<>();
        int lineNo = 0;
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(Files.newInputStream(path), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                lineNo++;
                if (line.isEmpty()) {
                    continue;
                }
                String[] f = line.split(",", -1);
                if (f.length >= 2 && "Time".equalsIgnoreCase(f[0].trim())
                        && "DeviceId".equalsIgnoreCase(f[1].trim())) {
                    continue;   // 表头 / header
                }
                if (f.length < 2 || f[1].trim().isEmpty()) {
                    stats.malformedLines++;
                    continue;
                }
                long ts;
                try {
                    ts = Long.parseLong(f[0].trim());
                } catch (NumberFormatException e) {
                    stats.malformedLines++;   // 时间不可解析：重放器无法定位，计数并跳过 / cannot place; skip
                    continue;
                }
                rows.add(new Row(ts, f[1].trim(), line, fileIdx, lineNo));
            }
        } catch (IOException e) {
            stats.skippedFiles++;
            System.err.println("[skip file] " + path + ": " + e.getMessage());
            return new ArrayList<>();
        }
        rows.sort(Comparator.comparingLong(row -> row.ts));
        return rows;
    }

    // ------------------------------------------------------------------
    // 全局归并 / global merge (Deliverable A.1)
    // ------------------------------------------------------------------

    /** 归并输出回调 / merged-output callback. */
    interface RowConsumer {
        void accept(Row row) throws Exception;
    }

    /**
     * 全局归并器：以上一文件末尾 CARRY_OVER_SEC 秒为跨文件缓冲，保证发出的流在事件时间上全局非降序。
     * Global merger: a CARRY_OVER_SEC-second cross-file carry-over buffer guarantees the emitted
     * stream is globally non-decreasing in event time.
     */
    static final class GlobalMerger {
        private final long carryOverSec;
        private final RowConsumer out;
        private final List<Row> buffer = new ArrayList<>();
        private long lastEmittedTs = Long.MIN_VALUE;

        GlobalMerger(long carryOverSec, RowConsumer out) {
            this.carryOverSec = carryOverSec;
            this.out = out;
        }

        /** 接入一个文件的行（已按 ts 排序），并发出可安全提交的前缀。 */
        void offer(List<Row> fileRows) throws Exception {
            if (fileRows.isEmpty()) {
                return;
            }
            buffer.addAll(fileRows);
            buffer.sort(Comparator.comparingLong(r -> r.ts));
            long maxTs = buffer.get(buffer.size() - 1).ts;
            long cutoff = maxTs - carryOverSec;
            int i = 0;
            while (i < buffer.size() && buffer.get(i).ts <= cutoff) {
                emit(buffer.get(i));
                i++;
            }
            // 保留尾部（ts > cutoff，规模 ≤ 60s 数据）/ keep the tail (within carry-over window)
            buffer.subList(0, i).clear();
        }

        /** 收尾：发出缓冲区剩余全部行 / flush all remaining rows. */
        void flush() throws Exception {
            buffer.sort(Comparator.comparingLong(r -> r.ts));
            for (Row row : buffer) {
                emit(row);
            }
            buffer.clear();
        }

        private void emit(Row row) throws Exception {
            // 单调钳制：极端跨界乱序时不倒退（不改事件时间，只保证发出顺序单调）。
            // Monotonic clamp: never regress the emission order on an extreme cross-boundary reorder.
            lastEmittedTs = Math.max(lastEmittedTs, row.ts);
            out.accept(row);
        }
    }

    // ------------------------------------------------------------------
    // 节奏控制与空闲压缩 / pacing and idle compression (Deliverable A.3, DEV-D6)
    // ------------------------------------------------------------------

    /**
     * 挂钟节奏器：目标挂钟 = wallBaseline + (t − eventBaseline)/k。若等待超过 maxIdleWall，则只睡
     * maxIdleWall 并重设基线（空闲压缩），**事件时间戳绝不修改**。空闲以合并后的全局流判定。
     * Wall-clock pacer: target = wallBaseline + (t − eventBaseline)/k. If the wait exceeds maxIdleWall,
     * sleep only maxIdleWall and rebaseline (idle compression); the event timestamp is never modified.
     * Idleness is judged on the merged global stream.
     */
    static final class Pacer {
        private final double speedup;
        private final long maxIdleWallMs;
        private final boolean dryRun;
        private boolean started;
        private long wallBaselineMs;
        private long eventBaselineSec;
        long compressionEvents;
        long totalCompressedMs;

        Pacer(double speedup, long maxIdleWallMs, boolean dryRun) {
            this.speedup = speedup <= 0 ? 1.0 : speedup;
            this.maxIdleWallMs = maxIdleWallMs;
            this.dryRun = dryRun;
        }

        void pace(long eventTsSec) {
            if (!started) {
                started = true;
                wallBaselineMs = now();
                eventBaselineSec = eventTsSec;
                return;
            }
            long targetMs = wallBaselineMs + (long) ((eventTsSec - eventBaselineSec) * 1000.0 / speedup);
            long waitMs = targetMs - now();
            if (waitMs > maxIdleWallMs) {
                // 空闲压缩：只睡 maxIdleWall，重设基线，审计一条 / compress: sleep the cap, rebaseline, audit
                compressionEvents++;
                totalCompressedMs += maxIdleWallMs;
                sleep(maxIdleWallMs);
                long eventSpan = eventTsSec - eventBaselineSec;
                System.out.println("[idle-compress] eventSpanSec=" + eventSpan
                        + " originalWallMs=" + waitMs + " compressedWallMs=" + maxIdleWallMs
                        + " atEventTs=" + eventTsSec);
                wallBaselineMs = now();
                eventBaselineSec = eventTsSec;
            } else if (waitMs > 0) {
                sleep(waitMs);
            }
            // waitMs <= 0：落后于计划，不睡 / behind schedule: do not sleep
        }

        private long now() {
            return System.currentTimeMillis();
        }

        private void sleep(long ms) {
            if (dryRun || ms <= 0) {
                return;   // dry-run 不真正睡 / dry-run does not actually sleep
            }
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ------------------------------------------------------------------
    // 输出汇 / record sinks
    // ------------------------------------------------------------------

    interface RecordSink {
        void emit(String topic, int partition, long tsMs, String key, String value);
        void close();
    }

    /** Kafka 生产汇（显式分区 + CreateTime 记录时间戳）/ Kafka sink (explicit partition + CreateTime). */
    static final class KafkaSink implements RecordSink {
        private final KafkaProducer<String, String> producer;
        KafkaSink(String brokers, String topic, int numPartitions) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");   // 保序 / keep order
            this.producer = new KafkaProducer<>(props);
        }
        @Override
        public void emit(String topic, int partition, long tsMs, String key, String value) {
            producer.send(new ProducerRecord<>(topic, partition, tsMs, key, value));
        }
        @Override
        public void close() {
            producer.flush();
            producer.close();
        }
    }

    /** 干跑汇：只计数不生产 / dry-run sink: count only. */
    static final class DryRunSink implements RecordSink {
        long n;
        @Override
        public void emit(String topic, int partition, long tsMs, String key, String value) {
            n++;
        }
        @Override
        public void close() {
            System.out.println("[dry-run] would have produced " + n + " records");
        }
    }

    // ------------------------------------------------------------------
    // 断点续跑 offset / resume offset
    // ------------------------------------------------------------------

    static boolean afterResume(Row row, long[] resumeState) {
        long fi = resumeState[0];
        long ln = resumeState[1];
        if (row.fileIdx > fi) {
            return true;
        }
        return row.fileIdx == fi && row.lineNo > ln;
    }

    static long[] loadOffset(Path offsetPath) {
        try {
            if (Files.exists(offsetPath)) {
                String[] p = new String(Files.readAllBytes(offsetPath), StandardCharsets.UTF_8).trim().split("\\s+");
                if (p.length == 2) {
                    return new long[]{Long.parseLong(p[0]), Long.parseLong(p[1])};
                }
            }
        } catch (IOException | NumberFormatException e) {
            System.err.println("[resume] failed to read offset, starting from beginning: " + e.getMessage());
        }
        return new long[]{-1L, -1L};
    }

    static void saveOffset(Path offsetPath, long fileIdx, long lineNo) {
        try {
            Files.write(offsetPath, (fileIdx + " " + lineNo).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            System.err.println("[resume] failed to save offset: " + e.getMessage());
        }
    }

    /**
     * 裸开关标志：--flag（无值）视为 true；--flag false 视为 false；--flag true 视为 true。
     * A bare on/off flag: --flag (no value) is true; --flag false is false; --flag true is true.
     * （ParameterTool.getBoolean 对无值键返回 false，故不能直接用它判定裸标志。）
     */
    static boolean flag(ParameterTool params, String key) {
        return params.has(key) && !"false".equalsIgnoreCase(params.get(key, "true"));
    }

    /** 解析 epoch 秒或 YYYY-MM-DD（当地日界按 UTC 简化）/ parse epoch seconds or YYYY-MM-DD. */
    static long parseInstant(String s, long fallback) {
        if (s == null || s.trim().isEmpty()) {
            return fallback;
        }
        s = s.trim();
        try {
            if (s.contains("-")) {
                return LocalDate.parse(s).atStartOfDay().toEpochSecond(ZoneOffset.UTC);
            }
            return Long.parseLong(s);
        } catch (RuntimeException e) {
            System.err.println("[segment] cannot parse '" + s + "', using fallback");
            return fallback;
        }
    }

    /** 运行统计 / run statistics. */
    static final class Stats {
        long produced;
        long malformedLines;
        long skippedFiles;        // loadFile 读失败的文件 / files that failed to read
        long namedDataFiles;      // .csv 命名数据文件 / csv-named data files
        long sniffedDataFiles;    // 非 .csv 命名但内容为数据 / non-.csv-named, content is data
        long skippedNonData;      // 发现阶段判为非数据的文件 / non-data files skipped at discovery
        long unknownDevice;
        int lastFileIdx = -1;
        int lastLineNo = -1;
    }
}
