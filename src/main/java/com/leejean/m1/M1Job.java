package com.leejean.m1;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

/**
 * M1 管线作业（交接文档 §4，Deliverable B）：source → 解析 → 采样轮装配 → RobustScaler 归一化
 * → 原始缓存 → 监测/验收 sink。本阶段**不含任何检测逻辑**（§7 边界）。
 * The M1 pipeline job (handover §4): source → parse → round assembly → robust normalization →
 * raw cache → monitoring/acceptance sinks. No detection logic in this stage (§7 boundary).
 *
 * <p>算子链 / operator chain：
 * <pre>
 *   FlinkKafkaConsumer(synergia-source, event time = Kafka 记录时间,
 *       forBoundedOutOfOrderness(55s).withIdleness(idle))
 *     → RawLineParser (ProcessFunction, 四守卫计数)
 *     → keyBy(device)
 *     → RoundAssembler (精确时间戳成轮, 保留首值, 事件时间 ts+30s 关闭)
 *     → RobustScalerFunction (预热 8640 轮冻结 中位数/IQR, IQR≤ε 旁路)
 *     → RawCacheFunction (原始环形缓存, 冷启动标记)
 *     ├→ synergia-m1-out       归一化 DeviceRound 的 JSON（仅验收）
 *     └→ MonitoringAggregator → synergia-monitoring  逐设备每 60s 监测快照
 * </pre>
 * 注意：**不复制旧项目的 noWatermarks() 接线**（交接文档 §4.1）。
 * Note: this deliberately does NOT copy the old project's noWatermarks() wiring.
 */
public class M1Job {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String brokers = params.get("brokers", params.get("broker", "localhost:9092"));
        String sourceTopic = params.get("source-topic", "synergia-source");
        String outTopic = params.get("out-topic", "synergia-m1-out");
        String monitoringTopic = params.get("monitoring-topic", "synergia-monitoring");
        String startupMode = params.get("start-offset", "earliest");   // earliest | latest
        int parallelism = params.getInt("parallelism", 8);
        long idleWallSec = params.getLong("idle-wall", 10L);           // withIdleness 默认 10s
        int warmupRounds = params.getInt("warmup-rounds", 8640);       // approved decision 3
        double epsilon = params.getDouble("iqr-epsilon", 1e-9);
        int cacheDepth = params.getInt("cache-depth", 1000);           // approved decision 5
        int nominalPeriodSec = params.getInt("nominal-period-sec", 10);
        long checkpointMs = params.getLong("checkpoint-ms", 10000L);

        System.out.println("========================================");
        System.out.println("M1Job");
        System.out.println("Brokers:         " + brokers);
        System.out.println("Source topic:    " + sourceTopic);
        System.out.println("Out topic:       " + outTopic);
        System.out.println("Monitoring topic:" + monitoringTopic);
        System.out.println("Start offset:    " + startupMode);
        System.out.println("Parallelism:     " + parallelism);
        System.out.println("Idle-wall:       " + idleWallSec + " s");
        System.out.println("Warmup rounds:   " + warmupRounds);
        System.out.println("IQR epsilon:     " + epsilon);
        System.out.println("Cache depth:     " + cacheDepth);
        System.out.println("========================================");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(checkpointMs);   // 启用 checkpoint（交接文档 §4.1）/ checkpointing enabled

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", brokers);
        consumerProps.setProperty("group.id", "m1-job-" + UUID.randomUUID().toString().substring(0, 8));

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(sourceTopic, new SimpleStringSchema(), consumerProps);
        // 起始位移为显式作业参数（runbook：先提交作业再重放）/ explicit start offset (submit job, then replay)
        if ("latest".equalsIgnoreCase(startupMode)) {
            consumer.setStartFromLatest();
        } else {
            consumer.setStartFromEarliest();
        }

        // 事件时间取自 Kafka 记录时间；55s 有界乱序（DF-2）；withIdleness 防离线设备分区拖停水位线。
        // Event time from the Kafka record timestamp; 55 s bounded out-of-orderness (DF-2);
        // withIdleness so an offline device's partition cannot stall the global watermark.
        WatermarkStrategy<String> wm = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(55))
                .withIdleness(Duration.ofSeconds(idleWallSec))
                .withTimestampAssigner((line, kafkaTs) -> kafkaTs);

        DataStream<String> raw = env.addSource(consumer)
                .assignTimestampsAndWatermarks(wm)
                .name("Kafka Source [" + sourceTopic + "]");

        SingleOutputStreamOperator<Reading> readings = raw
                .process(new RawLineParser())
                .name("RawLineParser");

        KeyedStream<Reading, String> byDevice = readings.keyBy(
                (KeySelector<Reading, String>) Reading::getDevice);

        SingleOutputStreamOperator<DeviceRound> rounds = byDevice
                .process(new RoundAssembler())
                .name("RoundAssembler");

        SingleOutputStreamOperator<DeviceRound> scaled = rounds
                .keyBy((KeySelector<DeviceRound, String>) DeviceRound::getDevice)
                .process(new RobustScalerFunction(warmupRounds, epsilon))
                .name("RobustScaler");

        SingleOutputStreamOperator<DeviceRound> cached = scaled
                .keyBy((KeySelector<DeviceRound, String>) DeviceRound::getDevice)
                .process(new RawCacheFunction(cacheDepth, nominalPeriodSec))
                .name("RawCache");

        // (a) 归一化流 → synergia-m1-out（JSON，仅验收）/ normalized stream → m1-out (acceptance only)
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", brokers);
        cached.addSink(new FlinkKafkaProducer<>(
                        outTopic,
                        new DeviceRoundSerializationSchema(outTopic),
                        producerProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name("Kafka Sink [" + outTopic + "]");

        // (b) 逐设备每 60s 监测快照 → synergia-monitoring / per-device 60 s snapshots → monitoring
        cached.keyBy((KeySelector<DeviceRound, String>) DeviceRound::getDevice)
                .process(new MonitoringAggregator())
                .name("MonitoringAggregator")
                .addSink(new FlinkKafkaProducer<>(
                        monitoringTopic,
                        new MonitoringSerializationSchema(monitoringTopic),
                        producerProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name("Kafka Sink [" + monitoringTopic + "]");

        env.execute("M1Job - ingestion/round-assembly/normalization/raw-cache");
    }

    // ------------------------------------------------------------------
    // Kafka 序列化 schema（仿 FA-iForest：嵌套 static class + transient ObjectMapper）
    // Kafka serialization schemas (FA-iForest pattern: nested static class + transient ObjectMapper)
    // ------------------------------------------------------------------

    private static class DeviceRoundSerializationSchema implements KafkaSerializationSchema<DeviceRound> {
        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;
        DeviceRoundSerializationSchema(String topic) {
            this.topic = topic;
        }
        @Override
        public ProducerRecord<byte[], byte[]> serialize(DeviceRound round, @Nullable Long timestamp) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                byte[] key = round.getDevice() == null ? null
                        : round.getDevice().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                byte[] value = mapper.writeValueAsBytes(round);
                return new ProducerRecord<>(topic, null, round.getTs() * 1000L, key, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize DeviceRound to JSON", e);
            }
        }
    }

    private static class MonitoringSerializationSchema implements KafkaSerializationSchema<MonitoringSnapshot> {
        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;
        MonitoringSerializationSchema(String topic) {
            this.topic = topic;
        }
        @Override
        public ProducerRecord<byte[], byte[]> serialize(MonitoringSnapshot snap, @Nullable Long timestamp) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                byte[] key = snap.getDevice() == null ? null
                        : snap.getDevice().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                byte[] value = mapper.writeValueAsBytes(snap);
                return new ProducerRecord<>(topic, null, snap.getTs() * 1000L, key, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize MonitoringSnapshot to JSON", e);
            }
        }
    }
}
