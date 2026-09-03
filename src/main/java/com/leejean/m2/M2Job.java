package com.leejean.m2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.m1.DeviceRound;
import com.leejean.m1.MonitoringAggregator;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m1.RawCacheFunction;
import com.leejean.m1.RawLineParser;
import com.leejean.m1.Reading;
import com.leejean.m1.RobustScalerFunction;
import com.leejean.m1.RoundAssembler;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

/**
 * M2 联合作业（交接文档 §3-§5）：在 M1 管线之后接续 pMCOD 点异常检测，同一作业内算子链接续、不经 Kafka
 * 中转。M1 段完全复用已验收的算子（RawLineParser→RoundAssembler→RobustScaler→RawCache），M2 段为
 * 三道闸 → keyBy(device) → 事件时间滑动窗口 → PmcodFunction → synergia-scores / synergia-monitoring。
 * The joint M2 job: pMCOD point-anomaly detection chained after the (already-accepted) M1 pipeline in
 * one job. M1 operators are reused as-is; M2 adds the gates, the sliding window and PmcodFunction.
 *
 * <pre>
 *   Kafka(synergia-source, event time, 55s OOO)
 *     → RawLineParser → keyBy → RoundAssembler → keyBy → RobustScaler → keyBy → RawCache  [= M1 标准化流]
 *     ├→ MonitoringAggregator → synergia-monitoring        （M1 逐设备 60s 快照，保留）
 *     └→ M2Gate（三道闸）→ keyBy(device)
 *          → SlidingEventTimeWindows(W, S), allowedLateness(0), sideOutputLateData
 *          → PmcodFunction
 *              ├→ synergia-scores                            （离群点名单）
 *              └(side)→ synergia-monitoring                  （M2 三路信号快照）
 * </pre>
 * 注：M1Job 保持独立不动（已验收）；本作业为"M1+M2 联合"入口，二者不同时运行（都消费 synergia-source）。
 */
public class M2Job {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String brokers = params.get("brokers", params.get("broker", "localhost:9092"));
        String sourceTopic = params.get("source-topic", "synergia-source");
        String scoresTopic = params.get("scores-topic", "synergia-scores");
        String monitoringTopic = params.get("monitoring-topic", "synergia-monitoring");
        // m1-out（标准化 DeviceRound）：默认关闭以精简联合作业；开启后一次月度重放即可喂 V-M2-3 探针。
        // 传 --out-topic synergia-m1-out 打开（空字符串 = 关闭）。
        String outTopic = params.get("out-topic", "");
        String startupMode = params.get("start-offset", "earliest");
        int parallelism = params.getInt("parallelism", 8);
        long idleWallSec = params.getLong("idle-wall", 10L);
        int warmupRounds = params.getInt("warmup-rounds", 8640);
        double epsilon = params.getDouble("iqr-epsilon", 1e-9);
        int cacheDepth = params.getInt("cache-depth", 1000);
        int nominalPeriodSec = params.getInt("nominal-period-sec", 10);
        long checkpointMs = params.getLong("checkpoint-ms", 10000L);
        // M2 窗口与算法参数（占位默认；(R,k) 终值由探针交回设计会话裁决）
        int windowSec = params.getInt("window-sec", 3600);       // W
        int slideSec = params.getInt("slide-sec", 60);           // S
        double r = params.getDouble("mcod-r", 1.0);              // 半径 R（全局默认/回退值）
        int k = params.getInt("mcod-k", 10);                     // 邻居阈值 k
        // 逐设备半径 R（收尾任务：按标定表逐设备取 R）。格式 "A=1.0,B=1.0,C=1.75,..."；
        // 未在映射中的设备回退到全局 --mcod-r。空串 = 全部用全局 R（向后兼容）。
        // Per-device radius R: "A=1.0,B=1.0,..."; a device not listed falls back to the global --mcod-r.
        java.util.Map<String, Double> rPerDevice = parseRPerDevice(params.get("mcod-r-per-device", ""));

        System.out.println("========================================");
        System.out.println("M2Job (M1+pMCOD joint)");
        System.out.println("Brokers:         " + brokers);
        System.out.println("Source topic:    " + sourceTopic);
        System.out.println("Scores topic:    " + scoresTopic);
        System.out.println("Monitoring topic:" + monitoringTopic);
        System.out.println("Start offset:    " + startupMode);
        System.out.println("Parallelism:     " + parallelism);
        System.out.println("Window W/S:      " + windowSec + "s / " + slideSec + "s");
        System.out.println("MCOD R/k:        " + r + " / " + k
                + (rPerDevice.isEmpty() ? " (global R for all devices)"
                        : "  per-device R=" + new java.util.TreeMap<>(rPerDevice)));
        System.out.println("========================================");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(checkpointMs);

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", brokers);
        consumerProps.setProperty("group.id", "m2-job-" + UUID.randomUUID().toString().substring(0, 8));

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(sourceTopic, new SimpleStringSchema(), consumerProps);
        if ("latest".equalsIgnoreCase(startupMode)) {
            consumer.setStartFromLatest();
        } else {
            consumer.setStartFromEarliest();
        }

        WatermarkStrategy<String> wm = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(55))
                .withIdleness(Duration.ofSeconds(idleWallSec))
                .withTimestampAssigner((line, kafkaTs) -> kafkaTs);

        DataStream<String> raw = env.addSource(consumer)
                .assignTimestampsAndWatermarks(wm)
                .name("Kafka Source [" + sourceTopic + "]");

        // ---- M1 段（复用已验收算子）/ M1 stage (reused, accepted operators) ----
        SingleOutputStreamOperator<DeviceRound> cached = raw
                .process(new RawLineParser()).name("RawLineParser")
                .keyBy((KeySelector<Reading, String>) Reading::getDevice)
                .process(new RoundAssembler()).name("RoundAssembler")
                .keyBy((KeySelector<DeviceRound, String>) DeviceRound::getDevice)
                .process(new RobustScalerFunction(warmupRounds, epsilon)).name("RobustScaler")
                .keyBy((KeySelector<DeviceRound, String>) DeviceRound::getDevice)
                .process(new RawCacheFunction(cacheDepth, nominalPeriodSec)).name("RawCache");

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", brokers);

        // (M1) 逐设备 60s 监测快照 → synergia-monitoring（保留 M1 信号）
        cached.keyBy((KeySelector<DeviceRound, String>) DeviceRound::getDevice)
                .process(new MonitoringAggregator()).name("MonitoringAggregator")
                .addSink(new FlinkKafkaProducer<>(monitoringTopic,
                        new MonitoringSerializationSchema(monitoringTopic),
                        producerProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name("Kafka Sink [" + monitoringTopic + " / M1]");

        // (可选) 归一化 DeviceRound → synergia-m1-out（供 V-M2-3 探针离线读；--out-topic 打开）
        if (!outTopic.isEmpty()) {
            cached.addSink(new FlinkKafkaProducer<>(outTopic,
                    new DeviceRoundSerializationSchema(outTopic),
                    producerProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                    .name("Kafka Sink [" + outTopic + "]");
        }

        // ---- M2 段 ----
        OutputTag<DevicePoint> lateTag = new OutputTag<DevicePoint>("m2-late-drops") { };
        OutputTag<MonitoringSnapshot> m2MonTag = new OutputTag<MonitoringSnapshot>("m2-monitoring") { };

        DataStream<DevicePoint> gated = cached.process(new M2Gate()).name("M2Gate");

        SingleOutputStreamOperator<ScoreEvent> scored = gated
                .keyBy((KeySelector<DevicePoint, String>) DevicePoint::getDevice)
                .window(SlidingEventTimeWindows.of(Time.seconds(windowSec), Time.seconds(slideSec)))
                .allowedLateness(Time.seconds(0))                 // 允许迟到 = 0（§4）
                .sideOutputLateData(lateTag)
                .process(new PmcodFunction(r, k, slideSec, rPerDevice, m2MonTag))
                .name("Pmcod");

        // 离群点名单 → synergia-scores
        scored.addSink(new FlinkKafkaProducer<>(scoresTopic,
                        new ScoreEventSerializationSchema(scoresTopic),
                        producerProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name("Kafka Sink [" + scoresTopic + "]");

        // M2 三路信号快照（侧输出）→ 并入 synergia-monitoring
        scored.getSideOutput(m2MonTag)
                .addSink(new FlinkKafkaProducer<>(monitoringTopic,
                        new MonitoringSerializationSchema(monitoringTopic),
                        producerProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name("Kafka Sink [" + monitoringTopic + " / M2]");

        // 迟到丢弃计数（侧输出）：仅计 Flink 指标，不落库（§4）
        scored.getSideOutput(lateTag).process(new LateDropCounter()).name("M2LateDrops");

        env.execute("M2Job - M1 ingestion/normalization + pMCOD point-anomaly detection");
    }

    /**
     * 解析逐设备半径 R 规格串 "A=1.0,B=1.0,C=1.75"（收尾任务）。空串/空项跳过；格式错误的项直接抛出（
     * 校准值不容静默吞错——宁可快速失败也不要拿错半径跑一整月）。返回可序列化的 HashMap。
     * Parse the per-device radius spec "A=1.0,B=1.0,..."; empty entries skipped, a malformed entry throws
     * (a wrong radius must never be swallowed silently). Returns a serializable HashMap.
     */
    static java.util.Map<String, Double> parseRPerDevice(String spec) {
        java.util.Map<String, Double> map = new java.util.HashMap<>();
        if (spec == null || spec.trim().isEmpty()) {
            return map;
        }
        for (String item : spec.split(",")) {
            String s = item.trim();
            if (s.isEmpty()) {
                continue;
            }
            int eq = s.indexOf('=');
            if (eq <= 0 || eq == s.length() - 1) {
                throw new IllegalArgumentException(
                        "非法逐设备 R 项 '" + s + "'，应为 设备=半径（如 A=1.0） / malformed device=R item");
            }
            String device = s.substring(0, eq).trim();
            double radius;
            try {
                radius = Double.parseDouble(s.substring(eq + 1).trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "逐设备 R 项 '" + s + "' 的半径无法解析为数字 / R value not a number", e);
            }
            if (radius <= 0) {
                throw new IllegalArgumentException(
                        "逐设备 R 项 '" + s + "' 的半径必须为正 / R must be positive");
            }
            map.put(device, radius);
        }
        return map;
    }

    /** 迟到数据计数器：只增 Flink 指标 m2_gate_late_drop，不向下游发射 / count-only, no emit. */
    public static class LateDropCounter
            extends org.apache.flink.streaming.api.functions.ProcessFunction<DevicePoint, DevicePoint> {
        private static final long serialVersionUID = 1L;
        private transient org.apache.flink.metrics.Counter lateDrop;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            lateDrop = getRuntimeContext().getMetricGroup().counter("m2_gate_late_drop");
        }

        @Override
        public void processElement(DevicePoint value, Context ctx, org.apache.flink.util.Collector<DevicePoint> out) {
            lateDrop.inc();
        }
    }

    // ------------------------------------------------------------------
    // Kafka 序列化 schema（沿用 M1 的嵌套 static class + transient ObjectMapper 模板）
    // ------------------------------------------------------------------

    private static class ScoreEventSerializationSchema implements KafkaSerializationSchema<ScoreEvent> {
        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;
        ScoreEventSerializationSchema(String topic) {
            this.topic = topic;
        }
        @Override
        public ProducerRecord<byte[], byte[]> serialize(ScoreEvent e, @Nullable Long timestamp) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                byte[] key = e.getDevice() == null ? null : e.getDevice().getBytes(StandardCharsets.UTF_8);
                byte[] value = mapper.writeValueAsBytes(e);
                // 记录时间戳 = 窗口末（事件时间秒→毫秒），与 M1 输出口径一致
                return new ProducerRecord<>(topic, null, e.getWindowEnd() * 1000L, key, value);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to serialize ScoreEvent to JSON", ex);
            }
        }
    }

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
                        : round.getDevice().getBytes(StandardCharsets.UTF_8);
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
                        : snap.getDevice().getBytes(StandardCharsets.UTF_8);
                byte[] value = mapper.writeValueAsBytes(snap);
                return new ProducerRecord<>(topic, null, snap.getTs() * 1000L, key, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize MonitoringSnapshot to JSON", e);
            }
        }
    }
}
