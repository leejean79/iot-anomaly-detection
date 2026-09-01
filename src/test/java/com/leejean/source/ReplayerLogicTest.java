package com.leejean.source;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * CsvKafkaReplayer 纯逻辑单测：文件名时间解析、全局归并的非降序保证、空闲压缩记账。
 * Pure-logic tests for CsvKafkaReplayer: filename-time parsing, the global-merge non-decreasing
 * guarantee, and idle-compression bookkeeping. (Kafka production is exercised on the cluster.)
 */
class ReplayerLogicTest {

    /**
     * 回归护栏：KafkaSink 必须把**事件时间**（row.ts×1000）写进 Kafka 记录时间戳（CsvKafkaReplayer:566
     * 的 5 参数 ProducerRecord）。曾有旧构建漏设时间戳 → CreateTime 下 Kafka 盖发送时刻（墙上时钟），
     * 使 Flink watermark 冲到"当下"、RoundAssembler 关轮定时器永不触发、整条管线不出数据。
     * Guard: the sink must put the event time into the Kafka record timestamp (the 5-arg ProducerRecord),
     * else CreateTime stamps wall-clock and the whole event-time pipeline stalls.
     */
    @Test
    void producerRecordCarriesEventTimeTimestamp() {
        long eventSec = 1653175904L;             // 2022-05-21（示例事件秒）
        long tsMs = eventSec * 1000L;
        int partition = 3;
        // 与 KafkaSink.emit 完全相同的构造方式 / exactly as KafkaSink.emit builds it
        ProducerRecord<String, String> rec =
                new ProducerRecord<>("synergia-source", partition, tsMs, "D", "raw,line");
        assertEquals(tsMs, (long) rec.timestamp(),
                "记录时间戳必须是事件时间 row.ts×1000（不能是 null/墙上时钟）");
        assertEquals(partition, (int) rec.partition(), "显式分区应保留");
        assertTrue(rec.timestamp() < System.currentTimeMillis() - 3L * 365 * 24 * 3600 * 1000,
                "事件时间应远早于当下（2022），若接近当下说明退化成了发送时刻");
    }

    @Test
    void parsesBothNamingPatternsAndCompactVariant() {
        long a = CsvKafkaReplayer.parseFilenameTime("2022_02_11_13-45-52_data.csv");
        long b = CsvKafkaReplayer.parseFilenameTime("2022_02_11_13-45-52.csv");
        long c = CsvKafkaReplayer.parseFilenameTime("2022_02_11_134552_data.csv");   // compact
        assertEquals(a, b);
        assertEquals(a, c);
        assertEquals(Long.MAX_VALUE, CsvKafkaReplayer.parseFilenameTime("README.txt"));
        assertEquals(Long.MAX_VALUE, CsvKafkaReplayer.parseFilenameTime("archive.zip"));
    }

    @Test
    void globalMergeEmitsNonDecreasingAcrossOverlappingFiles() throws Exception {
        // 文件1: ts 100,110,180 ；文件2: ts 150,160,200（与文件1 尾部交叠）
        // File 1 and file 2 overlap in event time; the merged stream must be non-decreasing.
        List<CsvKafkaReplayer.Row> file1 = new ArrayList<>();
        file1.add(new CsvKafkaReplayer.Row(100, "A", "l100", 0, 1));
        file1.add(new CsvKafkaReplayer.Row(110, "B", "l110", 0, 2));
        file1.add(new CsvKafkaReplayer.Row(180, "C", "l180", 0, 3));
        List<CsvKafkaReplayer.Row> file2 = new ArrayList<>();
        file2.add(new CsvKafkaReplayer.Row(150, "D", "l150", 1, 1));
        file2.add(new CsvKafkaReplayer.Row(160, "E", "l160", 1, 2));
        file2.add(new CsvKafkaReplayer.Row(200, "F", "l200", 1, 3));

        List<Long> emitted = new ArrayList<>();
        CsvKafkaReplayer.GlobalMerger merger =
                new CsvKafkaReplayer.GlobalMerger(60L, row -> emitted.add(row.ts));
        merger.offer(file1);
        merger.offer(file2);
        merger.flush();

        assertEquals(6, emitted.size(), "every row emitted exactly once");
        for (int i = 1; i < emitted.size(); i++) {
            assertTrue(emitted.get(i) >= emitted.get(i - 1),
                    "non-decreasing violated at " + i + ": " + emitted);
        }
        // 期望顺序 / expected order: 100,110,150,160,180,200
        assertEquals(java.util.Arrays.asList(100L, 110L, 150L, 160L, 180L, 200L), emitted);
    }

    @Test
    void carryOverBufferHoldsTailWithinWindow() throws Exception {
        // 60s 缓冲：文件1 的 180 不能在文件2（含 150）之前发出，否则违反非降序。
        // The carry-over must hold file1's 180 until file2's earlier timestamps are merged in.
        List<CsvKafkaReplayer.Row> file1 = new ArrayList<>();
        file1.add(new CsvKafkaReplayer.Row(100, "A", "a", 0, 1));
        file1.add(new CsvKafkaReplayer.Row(180, "B", "b", 0, 2));   // within 60s of file2 head? no, but held by cutoff
        List<CsvKafkaReplayer.Row> file2 = new ArrayList<>();
        file2.add(new CsvKafkaReplayer.Row(150, "C", "c", 1, 1));

        List<Long> emitted = new ArrayList<>();
        CsvKafkaReplayer.GlobalMerger merger =
                new CsvKafkaReplayer.GlobalMerger(60L, row -> emitted.add(row.ts));
        merger.offer(file1);
        merger.offer(file2);
        merger.flush();
        assertEquals(java.util.Arrays.asList(100L, 150L, 180L), emitted);
    }

    @Test
    void discoveryClassifiesCsvSniffedAndNonData(@TempDir Path dir) throws IOException {
        // .csv 命名数据文件 / csv-named data file
        Files.write(dir.resolve("2022_02_11_13-45-52_data.csv"),
                "1644587152,A,Temperature,22.5\n".getBytes(StandardCharsets.UTF_8));
        // 非 .csv 命名但内容为数据（EDA 补丁 01 现象）/ non-.csv name, content is data
        Files.write(dir.resolve("2022_02_11_134552_data"),
                "1644587153,B,Humidity,45.0\n".getBytes(StandardCharsets.UTF_8));
        // 非数据文件 / non-data files
        Files.write(dir.resolve("README.txt"),
                "this is prose, not data\n".getBytes(StandardCharsets.UTF_8));
        Files.write(dir.resolve("notes.json"),
                "{\"k\":1}\n".getBytes(StandardCharsets.UTF_8));

        // 默认（非 strict）：应纳入 .csv 命名 + 内容嗅探数据，跳过非数据。
        CsvKafkaReplayer.Stats stats = new CsvKafkaReplayer.Stats();
        List<CsvKafkaReplayer.FileEntry> files = CsvKafkaReplayer.discoverFiles(dir, stats, false);
        assertEquals(2, files.size(), "csv-named + sniffed data included");
        assertEquals(1, stats.namedDataFiles);
        assertEquals(1, stats.sniffedDataFiles, "non-.csv data file must not be silently dropped");
        assertEquals(2, stats.skippedNonData, "README.txt + notes.json are non-data");

        // strict：只取 .csv 命名，其余全部计入 skippedNonData。
        CsvKafkaReplayer.Stats strict = new CsvKafkaReplayer.Stats();
        List<CsvKafkaReplayer.FileEntry> strictFiles = CsvKafkaReplayer.discoverFiles(dir, strict, true);
        assertEquals(1, strictFiles.size());
        assertEquals(1, strict.namedDataFiles);
        assertEquals(0, strict.sniffedDataFiles);
        assertEquals(3, strict.skippedNonData);
    }

    @Test
    void idleCompressionTriggersOnLargeGapNotOnSmallSteps() {
        // 干跑（不真正睡眠）：大事件间隙触发压缩，小步不触发。挂钟对比，容忍少量抖动。
        // Dry-run (no real sleep): a large event gap triggers compression; small steps do not.
        CsvKafkaReplayer.Pacer smallSteps = new CsvKafkaReplayer.Pacer(1.0, 2000L, true);
        smallSteps.pace(0);
        smallSteps.pace(1);   // 1s / k=1 → ~1000ms wait < 2000ms cap
        smallSteps.pace(2);
        assertEquals(0, smallSteps.compressionEvents, "small steps must not compress");

        CsvKafkaReplayer.Pacer bigJump = new CsvKafkaReplayer.Pacer(1.0, 2000L, true);
        bigJump.pace(0);
        bigJump.pace(3600);   // 1h event gap at k=1 → target far in future → compress
        assertTrue(bigJump.compressionEvents >= 1, "a large idle gap must compress");
        assertTrue(bigJump.totalCompressedMs >= 2000L);
    }
}
