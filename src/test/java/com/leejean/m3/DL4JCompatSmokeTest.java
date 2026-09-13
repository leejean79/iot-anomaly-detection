package com.leejean.m3;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.LSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M3 兼容性冒烟测试（DEV-D1 门槛）：证明 DL4J 1.0.0-M2.1 能在 Flink 1.13.6 MiniCluster 内
 * 完成 LSTM 自编码器的构建、训练和推断，且与 Java 11 + 现有依赖无冲突。
 * （M2.1 字节码为 Java 11，主版本 55；集群运行时已迁到 Java 11，可正常加载。）
 *
 * M3 compatibility smoke test (DEV-D1 gate): proves DL4J 1.0.0-M2.1 can build, train, and infer
 * with a micro LSTM autoencoder inside a Flink 1.13.6 MiniCluster KeyedProcessFunction,
 * with no conflicts against Java 8 or existing dependencies.
 *
 * <p>测试三个层面 / three test levels:
 * <ol>
 *   <li>纯 ND4J 张量运算（不经 Flink）/ pure ND4J tensor ops (no Flink)</li>
 *   <li>纯 DL4J 模型构建 + 训练 + 推断（不经 Flink）/ pure DL4J model build+train+infer (no Flink)</li>
 *   <li>在 Flink MiniCluster 的 KeyedProcessFunction 里完成上述全部 / all of the above inside
 *       a Flink MiniCluster KeyedProcessFunction</li>
 * </ol>
 */
class DL4JCompatSmokeTest {

    // ---- 静态收集 sink / static collecting sink ----
    static final class CollectSink implements SinkFunction<String> {
        private static final long serialVersionUID = 1L;
        static final List<String> VALUES = Collections.synchronizedList(new ArrayList<>());
        @Override
        public void invoke(String value, Context context) {
            VALUES.add(value);
        }
    }

    @BeforeEach
    void setUp() {
        CollectSink.VALUES.clear();
    }

    /**
     * 第一层：纯 ND4J 张量运算——证明原生库加载正常、基本线代可用。
     * Level 1: pure ND4J tensor ops — proves native library loads and basic linear algebra works.
     */
    @Test
    void nd4jTensorOpsWork() {
        INDArray a = Nd4j.create(new double[]{1, 2, 3, 4}, new int[]{2, 2});
        INDArray b = Nd4j.eye(2).castTo(a.dataType());
        INDArray c = a.mmul(b);
        assertEquals(a, c, "A × I = A");

        INDArray row = Nd4j.create(new double[]{10, 20, 30});
        assertEquals(20.0, row.meanNumber().doubleValue(), 1e-9);
    }

    /**
     * 第二层：纯 DL4J LSTM 自编码器——构建、微训练、推断，不经 Flink。
     * Level 2: pure DL4J LSTM autoencoder — build, micro-train, infer, no Flink.
     */
    @Test
    void dl4jLstmAutoEncoderTrainsAndInfers() {
        int features = 5;
        int seqLen = 4;
        int hiddenSize = 8;

        MultiLayerNetwork model = buildMicroAutoEncoder(features, hiddenSize);

        INDArray input = Nd4j.randn(1, features, seqLen);
        INDArray target = input.dup();
        DataSet ds = new DataSet(input, target);

        double lossBefore = model.score(ds);
        for (int i = 0; i < 50; i++) {
            model.fit(ds);
        }
        double lossAfter = model.score(ds);

        assertTrue(lossAfter < lossBefore,
                "训练后损失应下降 / loss should decrease after training: before="
                        + lossBefore + " after=" + lossAfter);

        INDArray output = model.output(input);
        assertEquals(3, output.rank(), "输出应为 3 维 [batch, features, seqLen]");
        assertEquals(features, output.size(1));
        assertEquals(seqLen, output.size(2));
    }

    /**
     * 第三层：在 Flink MiniCluster 的 KeyedProcessFunction 内完成 LSTM 构建、训练和推断。
     * Level 3: LSTM build + train + infer inside a Flink MiniCluster KeyedProcessFunction.
     */
    @Test
    void lstmInFlinkKeyedProcessFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        List<String> inputs = Arrays.asList("dev-A:1.0,2.0,3.0,4.0,5.0",
                                            "dev-B:10.0,20.0,30.0,40.0,50.0");

        WatermarkStrategy<String> wm = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((line, ts) -> System.currentTimeMillis());

        DataStream<String> src = env.fromCollection(inputs).assignTimestampsAndWatermarks(wm);

        src.keyBy((KeySelector<String, String>) line -> line.split(":")[0])
           .process(new LstmSmokeFunction())
           .addSink(new CollectSink());

        env.execute("dl4j-compat-smoke");

        assertEquals(2, CollectSink.VALUES.size(),
                "两个设备各产出一条结果 / one result per device");
        for (String result : CollectSink.VALUES) {
            assertTrue(result.contains("PASS"), "每条结果标记 PASS / each result marked PASS: " + result);
        }
    }

    // ---- 辅助 / helpers ----

    static MultiLayerNetwork buildMicroAutoEncoder(int features, int hiddenSize) {
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(42)
                .updater(new Adam(0.01))
                .weightInit(WeightInit.XAVIER)
                .list()
                .layer(new LSTM.Builder()
                        .nIn(features)
                        .nOut(hiddenSize)
                        .activation(Activation.TANH)
                        .build())
                .layer(new RnnOutputLayer.Builder(LossFunctions.LossFunction.MSE)
                        .nIn(hiddenSize)
                        .nOut(features)
                        .activation(Activation.IDENTITY)
                        .build())
                .build();

        MultiLayerNetwork model = new MultiLayerNetwork(conf);
        model.init();
        return model;
    }

    /**
     * 冒烟用的 KeyedProcessFunction：在 open() 中构建并微训练一个 LSTM 自编码器，
     * 在 processElement() 中做推断并输出结果标记。
     *
     * Smoke-test KeyedProcessFunction: builds and micro-trains an LSTM autoencoder in open(),
     * runs inference in processElement() and emits a result tag.
     */
    static class LstmSmokeFunction extends KeyedProcessFunction<String, String, String> {
        private static final long serialVersionUID = 1L;
        private transient MultiLayerNetwork model;

        @Override
        public void open(Configuration parameters) {
            model = buildMicroAutoEncoder(5, 8);
            INDArray dummy = Nd4j.randn(1, 5, 4);
            DataSet ds = new DataSet(dummy, dummy.dup());
            for (int i = 0; i < 10; i++) {
                model.fit(ds);
            }
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            String device = value.split(":")[0];
            String[] parts = value.split(":")[1].split(",");
            double[] vals = new double[parts.length];
            for (int i = 0; i < parts.length; i++) {
                vals[i] = Double.parseDouble(parts[i]);
            }

            INDArray input = Nd4j.create(vals, new int[]{1, vals.length, 1});
            INDArray output = model.output(input);

            boolean shapeOk = output.rank() == 3
                    && output.size(1) == vals.length
                    && output.size(2) == 1;

            double recon = output.getDouble(0, 0, 0);
            boolean finiteOk = Double.isFinite(recon);

            String tag = (shapeOk && finiteOk) ? "PASS" : "FAIL";
            out.collect(device + ":" + tag + ":recon[0]=" + String.format("%.4f", recon));
        }
    }
}
