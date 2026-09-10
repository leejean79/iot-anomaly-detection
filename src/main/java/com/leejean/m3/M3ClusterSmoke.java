package com.leejean.m3;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.util.Properties;

/**
 * M3 集群冒烟作业（交接文档 §2 决策 1 第三部分：真实集群侧验证）。
 * M3 cluster smoke job (handover §2 decision 1, part 3: verification on the real cluster).
 *
 * <p>MiniCluster 测试（{@code DL4JCompatSmokeTest}）已覆盖第 (1)(2) 部分——JDK 8 打包无冲突、
 * 算子线程内训练+推理可行。第 (3) 部分只能在真实容器内证明，因为两个预警的风险只在那里出现：
 * ND4J 通过 JavaCPP 在 Java 堆外分配张量（Flink 默认 off-heap 为 0），以及容器用户 9999 对
 * JavaCPP 原生库解包目录的写权限。本作业在**每个并行子任务**（覆盖两个 TaskManager）内：
 * MiniCluster tests already cover parts (1)(2). Part (3) can only be proven inside the real
 * containers, where the two flagged risks live: ND4J allocates tensors off-heap via JavaCPP
 * (Flink's default off-heap is 0), and container user 9999's write access to JavaCPP's native
 * extraction directory. In each parallel subtask (spanning both TaskManagers) this job:
 * <ol>
 *   <li>记录实际使用的 JDK（java.version/vendor/vm，os.arch）——门槛是 Java 8；
 *       records the JDK actually used (the gate is Java 8);</li>
 *   <li>强制 ND4J 原生后端加载（小 LSTM 训练+推理），触发 .so 解包与堆外分配；
 *       forces the ND4J native backend to load (tiny LSTM train + inference);</li>
 *   <li>读取 JavaCPP 堆外内存上限与用量（maxBytes/maxPhysicalBytes/totalBytes）——证明堆外预算；
 *       reads JavaCPP off-heap ceilings and usage — evidence for the off-heap budget;</li>
 *   <li>读取并检验 JavaCPP 解包目录（cacheDir）的可写性——证明用户 9999 权限。
 *       reads and checks the writability of JavaCPP's extraction directory.</li>
 * </ol>
 * 每个子任务把一行报告写入 synergia-smoke（只碰 synergia- 前缀 topic，符合共存规则），
 * 同时 LOG.info 到 TaskManager 日志（即便 Kafka 消费失败也能 `docker logs` 查到）。
 * Each subtask writes one report line to synergia-smoke (only a synergia- topic — coexistence-safe)
 * and also LOG.info's it to the TaskManager log.
 *
 * <p>========================= 脚本交付五要素 / Five delivery elements =========================
 * <ul>
 *   <li><b>执行环境 / Environment</b>：真实集群 jobmanager 容器内 `flink run`（Flink 1.13.6，
 *       镜像 flink:1.13.6-scala_2.12-java8）；jar 已上传至 /opt/flink/usrlib/。由 syn-m3-smoke.sh 驱动。</li>
 *   <li><b>调用命令 / Invocation</b>：
 *       {@code flink run -c com.leejean.m3.M3ClusterSmoke <jar> --brokers <b1:9092,...>
 *       --smoke-topic synergia-smoke --parallelism 8}</li>
 *   <li><b>前置条件 / Preconditions</b>：synergia-smoke 已建（或脚本代建）；有 ≥ parallelism 个空闲 slot
 *       （**不占用、不取消**任何在跑作业）；TaskManager 已按 M3 预算重配（off-heap/JavaCPP -D，见
 *       docker-compose.worker.yml）并重启生效。</li>
 *   <li><b>期望产出 / Expected output</b>：作业跑至 FINISHED；synergia-smoke 内每子任务一行报告，
 *       含 JDK 版本、nd4j_native_ok=true、JavaCPP 堆外上限/用量、cacheDir 及其可写性。</li>
 *   <li><b>失败兜底 / Failure fallback</b>：原生加载失败不使整个作业静默崩溃——探针捕获所有异常并把
 *       失败原因写进报告；无空闲 slot → 作业 SCHEDULED 挂起（脚本超时报错，绝不取消旧作业）。</li>
 * </ul>
 * ==========================================================================================
 */
public class M3ClusterSmoke {

    private static final Logger LOG = LoggerFactory.getLogger(M3ClusterSmoke.class);
    private static final String MARKER = "[M3-CLUSTER-SMOKE]";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String brokers = params.get("brokers", params.get("broker", "localhost:9092"));
        String smokeTopic = params.get("smoke-topic", "synergia-smoke");
        int parallelism = params.getInt("parallelism", 8);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // 源：每个并行子任务发一条自己的下标（有界，发完即结束）。
        // Source: each parallel subtask emits its own index once (bounded — the job then finishes).
        DataStream<Long> seeds = env.addSource(new ProbeSource()).setParallelism(parallelism);

        // 探针：在子任务内强制原生加载并生成报告；不重分区，保证探针跑遍每个 slot（覆盖两个 TM）。
        // Probe: force native load inside the subtask and build the report; no reshuffle, so the probe
        // runs on every slot (spanning both TaskManagers).
        DataStream<String> reports = seeds.map(new NativeProbe()).setParallelism(parallelism);

        reports.addSink(new KafkaReportSink(brokers, smokeTopic)).setParallelism(parallelism);

        env.execute("M3 cluster smoke — native load + off-heap/cachedir/JDK evidence");
    }

    /** 每个并行子任务发一条其下标 / each parallel subtask emits its index once. */
    public static final class ProbeSource extends RichParallelSourceFunction<Long> {
        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> ctx) {
            if (running) {
                int idx = getRuntimeContext().getIndexOfThisSubtask();
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect((long) idx);
                }
            }
            // 发完即返回 → 有界流，作业自然结束 / return after emitting → bounded stream, job finishes
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /** 在子任务内强制 ND4J 原生加载并采集证据 / force native load in the subtask and collect evidence. */
    public static final class NativeProbe extends RichMapFunction<Long, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String map(Long seed) {
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            int total = getRuntimeContext().getNumberOfParallelSubtasks();
            String report = buildReport(subtask, total);
            LOG.info("{}\n{}", MARKER, report);
            System.out.println(MARKER + " " + report.replace("\n", " | "));
            return report;
        }
    }

    /** 采集四点证据，组装成一行（字段以换行分隔）/ collect the four points into one report. */
    static String buildReport(int subtask, int total) {
        StringBuilder sb = new StringBuilder();
        String host = "unknown";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception ignore) {
            String envHost = System.getenv("HOSTNAME");
            if (envHost != null) host = envHost;
        }
        sb.append("subtask=").append(subtask).append('/').append(total).append('\n');
        sb.append("host=").append(host).append('\n');

        // ---- 点 3：实际使用的 JDK / Point 3: the JDK actually used ----
        sb.append("java.version=").append(System.getProperty("java.version")).append('\n');
        sb.append("java.vendor=").append(System.getProperty("java.vendor")).append('\n');
        sb.append("java.vm.name=").append(System.getProperty("java.vm.name")).append('\n');
        sb.append("os.name=").append(System.getProperty("os.name")).append('\n');
        sb.append("os.arch=").append(System.getProperty("os.arch")).append('\n');

        // ---- 点 1 前置：显式传入的 JavaCPP -D 参数（证明 TM JVM opts 生效）----
        // Point 1 preamble: the explicit JavaCPP -D properties (proving TM JVM opts took effect).
        sb.append("D.maxbytes=").append(System.getProperty("org.bytedeco.javacpp.maxbytes")).append('\n');
        sb.append("D.maxphysicalbytes=")
                .append(System.getProperty("org.bytedeco.javacpp.maxphysicalbytes")).append('\n');
        sb.append("D.cachedir=").append(System.getProperty("org.bytedeco.javacpp.cachedir")).append('\n');

        // ---- 点 2 前置：强制 ND4J 原生后端加载（触发 .so 解包 + 堆外分配）----
        // Point 2 preamble: force the ND4J native backend to load (triggers .so extraction + off-heap alloc).
        boolean nd4jOk = false;
        String nd4jErr = null;
        try {
            LstmAutoEncoder ae = new LstmAutoEncoder(5, 20);
            double[][][] windows = new double[3][10][5];
            for (int w = 0; w < 3; w++) {
                for (int t = 0; t < 10; t++) {
                    for (int f = 0; f < 5; f++) {
                        windows[w][t][f] = Math.sin(w + t + f);
                    }
                }
            }
            ae.trainEpoch(windows, null);
            double[][] recon = ae.reconstruct(windows[0]);
            nd4jOk = recon.length == 10 && recon[0].length == 5;
        } catch (Throwable t) {
            nd4jErr = t.getClass().getName() + ": " + t.getMessage();
        }
        sb.append("nd4j_native_ok=").append(nd4jOk).append('\n');
        if (nd4jErr != null) {
            sb.append("nd4j_err=").append(nd4jErr).append('\n');
        }

        // ---- 点 1：JavaCPP 堆外内存上限与用量（原生加载后读取）----
        // Point 1: JavaCPP off-heap memory ceilings and usage (read after native load).
        try {
            sb.append("javacpp.maxBytes=").append(Pointer.maxBytes()).append('\n');
            sb.append("javacpp.maxPhysicalBytes=").append(Pointer.maxPhysicalBytes()).append('\n');
            sb.append("javacpp.totalBytes=").append(Pointer.totalBytes()).append('\n');
            sb.append("javacpp.totalCount=").append(Pointer.totalCount()).append('\n');
        } catch (Throwable t) {
            sb.append("javacpp.mem_err=").append(t).append('\n');
        }

        // ---- 点 2：JavaCPP 原生库解包目录及其可写性 ----
        // Point 2: JavaCPP native-library extraction directory and its writability.
        try {
            File cacheDir = Loader.getCacheDir();
            boolean writable = cacheDir != null && cacheDir.canWrite();
            sb.append("javacpp.cacheDir=").append(cacheDir).append('\n');
            sb.append("javacpp.cacheDir.writable=").append(writable).append('\n');
        } catch (Throwable t) {
            sb.append("javacpp.cacheDir_err=").append(t).append('\n');
        }

        // ---- 辅助：堆上限（对照堆外，说明张量确实在堆外）----
        // Aux: heap ceiling (contrast with off-heap, to show tensors live off-heap).
        sb.append("heap.maxBytes=").append(Runtime.getRuntime().maxMemory()).append('\n');
        sb.append("user.name=").append(System.getProperty("user.name"));
        return sb.toString();
    }

    /** 把每子任务的报告写入 synergia-smoke / write each subtask's report to synergia-smoke. */
    public static final class KafkaReportSink extends RichSinkFunction<String> {
        private static final long serialVersionUID = 1L;
        private final String brokers;
        private final String topic;
        private transient KafkaProducer<String, String> producer;

        public KafkaReportSink(String brokers, String topic) {
            this.brokers = brokers;
            this.topic = topic;
        }

        @Override
        public void open(Configuration parameters) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            producer = new KafkaProducer<>(props);
        }

        @Override
        public void invoke(String value, Context context) {
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            // 单行化便于 kafka-console-consumer 逐条显示 / single-line for clean console consumption
            producer.send(new ProducerRecord<>(topic, "subtask-" + subtask, value.replace("\n", " | ")));
        }

        @Override
        public void close() {
            if (producer != null) {
                producer.flush();
                producer.close();
            }
        }
    }
}
