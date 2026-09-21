package com.leejean.m3;

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

import java.io.*;

/**
 * 浅层 LSTM 自编码器（交接文档 §3 决策 4）：一层 LSTM 编码器 + 一层 LSTM 解码器。
 * Shallow LSTM autoencoder (handover §3 decision 4): one LSTM encoder layer + one LSTM decoder layer.
 *
 * <p>架构 / architecture: input(nFeatures) → LSTM(hiddenSize, tanh) → RnnOutputLayer(nFeatures, MSE, IDENTITY)。
 * 自编码器训练目标：输入序列即标签（重建自身）。
 * Autoencoder objective: input sequence IS the label (reconstruct itself).
 *
 * <p>序列化 / serialization: 整个 MultiLayerNetwork 通过 DL4J 的 ModelSerializer 存为字节数组，
 * 嵌入 Flink 的 checkpoint 状态（决策 7）。
 * The entire MultiLayerNetwork is serialized via DL4J's params + config into a byte array
 * embedded in Flink's checkpointed state (decision 7).
 */
public class LstmAutoEncoder implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int nFeatures;                       // 输入/输出维度（检测通道数，通常 5）/ input & output dims
    private final int hiddenSize;                       // LSTM 隐藏层宽度（网格 {40,60,90} 之一）/ LSTM hidden width
    private static final long SEED = 42L;               // 固定随机种子，保证可复现 / fixed seed for reproducibility
    private static final double LEARNING_RATE = 0.01;   // Adam 学习率 / Adam learning rate

    // model 不参与 Java 序列化（transient）；跨 checkpoint 用 serializeModel/deserializeModel 手工搬运。
    // model is transient (excluded from Java serialization); moved across checkpoints via (de)serializeModel.
    private transient MultiLayerNetwork model;

    public LstmAutoEncoder(int nFeatures, int hiddenSize) {
        this.nFeatures = nFeatures;
        this.hiddenSize = hiddenSize;
        this.model = buildModel(nFeatures, hiddenSize);   // 构造即建网并初始化 / build & init the network on construction
    }

    private static MultiLayerNetwork buildModel(int nFeatures, int hiddenSize) {
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(SEED)                                // 可复现 / reproducible
                .updater(new Adam(LEARNING_RATE))          // Adam 优化器 / Adam optimizer
                .weightInit(WeightInit.XAVIER)             // Xavier 初始化 / Xavier weight init
                .list()
                // 第 0 层：LSTM 编码器，tanh 激活，把 nFeatures 维序列压到 hiddenSize 维隐状态
                // Layer 0: LSTM encoder (tanh), compresses the nFeatures-dim sequence to a hiddenSize state
                .layer(0, new LSTM.Builder()
                        .nIn(nFeatures)
                        .nOut(hiddenSize)
                        .activation(Activation.TANH)
                        .build())
                // 第 1 层：RnnOutputLayer 解码器，MSE 损失 + 恒等激活，把隐状态重建回 nFeatures 维
                // Layer 1: RnnOutputLayer decoder (MSE loss, identity activation), reconstructs nFeatures dims
                .layer(1, new RnnOutputLayer.Builder(LossFunctions.LossFunction.MSE)
                        .nIn(hiddenSize)
                        .nOut(nFeatures)
                        .activation(Activation.IDENTITY)
                        .build())
                .build();
        MultiLayerNetwork net = new MultiLayerNetwork(conf);
        net.init();                                        // 分配参数并初始化权重 / allocate & init weights
        return net;
    }

    /**
     * 训练一个 epoch：输入即标签（自编码器），返回该 epoch 的平均损失。
     * Train one epoch: input is its own label (autoencoder); returns the epoch's average loss.
     *
     * @param windows 训练窗口列表 [nWindows][windowLen][nFeatures] / training windows
     * @param weightMasks 每窗口每时间步的有效掩码 [nWindows][windowLen][nFeatures]，null = 全有效
     *                    / per-window per-step validity mask, null = all valid
     * @return 平均损失 / average loss
     */
    public double trainEpoch(double[][][] windows, boolean[][][] weightMasks) {
        return trainEpoch(windows, weightMasks, 1);        // 默认逐窗更新，即历史行为 / default: per-window updates
    }

    /**
     * 训练一个 epoch，按给定的小批量大小成组更新权重。
     *
     * <p><b>小批量大小的含义</b>：一次权重更新用到多少个窗口。取 1 时每个窗口单独产生一次更新，
     * 即本方法的历史行为，也是 2026-09-21 参照读数所用的口径；取 32 时 32 个窗口被堆成一个
     * {@code [32, nFeatures, seqLen]} 张量，梯度在这 32 个窗口上取平均后只更新一次权重。
     * 这不只是工程提速，它改变优化行为，因此需经补遗三 §2 的 5% 判据把关。
     * Mini-batch size = how many windows contribute to one weight update. This changes the
     * optimization behaviour, not only the speed.
     *
     * <p>两条刻意固定的口径，改动它们会使小批量大小不再是唯一变量：
     * <ul>
     *   <li><b>不打乱窗口顺序</b>：参照读数是按时间顺序逐窗更新的，引入洗牌就等于同时改了两个变量。</li>
     *   <li><b>保留末尾不足一批的窗口</b>：1,001 个窗口按 32 切分得到 31 个完整小批量加一个只含 9 个
     *       窗口的末尾小批量，它照常参与训练——丢弃它等于悄悄少用了一部分数据。</li>
     * </ul>
     * Two pinned details: windows are never shuffled, and the short tail mini-batch is kept.
     *
     * @param batchSize 小批量大小，须为正 / mini-batch size, must be positive
     */
    public double trainEpoch(double[][][] windows, boolean[][][] weightMasks, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("小批量大小须为正，收到 " + batchSize);
        }
        if (windows.length == 0) {
            return 0.0;
        }
        double totalLoss = 0.0;
        int batches = 0;
        for (int start = 0; start < windows.length; start += batchSize) {
            // 末尾不足一批时 actual 小于 batchSize，该小批量照常训练 / short tail batch is trained as-is
            int actual = Math.min(batchSize, windows.length - start);
            int seqLen = windows[start].length;
            INDArray input = toRnnInput(windows, start, actual, seqLen);
            INDArray labels = input.dup();                 // 自编码器：标签即输入的副本 / label is a copy of the input

            if (weightMasks != null) {
                // 有掩码：用标签掩码把删失/缺失元素的损失权重置零（决策 3/6）
                // With a mask: a label mask zeroes the loss weight of censored/missing elements
                INDArray mask = toMaskArray(weightMasks, start, actual, seqLen);
                model.fit(new DataSet(input, labels, null, mask));
            } else {
                model.fit(new DataSet(input, labels));     // 无掩码：全元素参与 / no mask: all elements count
            }
            totalLoss += model.score();                    // 累加本小批量训练后损失 / accumulate this batch's score
            batches++;
        }
        return batches > 0 ? totalLoss / batches : 0.0;
    }

    /**
     * 推理：给定一个窗口，返回重建结果 [windowLen][nFeatures]。
     * Inference: given one window, return reconstruction [windowLen][nFeatures].
     */
    public double[][] reconstruct(double[][] window) {
        int seqLen = window.length;
        INDArray input = toRnnInput(window, seqLen);       // 转 RNN 张量 / to RNN tensor
        INDArray output = model.output(input);             // 前向推理得到重建序列 / forward pass → reconstruction
        return fromRnnOutput(output, seqLen);              // 转回二维数组 / back to 2-D array
    }

    /**
     * 转换为 RNN 输入张量 [1, nFeatures, seqLen]（DL4J 的 RNN 输入格式）。
     * Convert to RNN input tensor [1, nFeatures, seqLen] (DL4J's RNN input format).
     */
    private INDArray toRnnInput(double[][] window, int seqLen) {
        // DL4J RNN 约定 [miniBatch, features, timeSteps]，此处 miniBatch=1 / DL4J RNN layout: batch=1
        INDArray arr = Nd4j.create(1, nFeatures, seqLen);
        for (int t = 0; t < seqLen; t++) {                 // t = 时间步 / time step
            for (int f = 0; f < nFeatures; f++) {          // f = 特征通道 / feature channel
                arr.putScalar(new int[]{0, f, t}, window[t][f]);   // window[t][f] → [0,f,t]
            }
        }
        return arr;
    }

    /**
     * 构建一个小批量的 RNN 输入张量 {@code [count, nFeatures, seqLen]}，取 windows 中从 from 起的
     * count 个窗口，顺序原样保留。
     * Build one mini-batch input tensor from `count` windows starting at `from`, order preserved.
     */
    private INDArray toRnnInput(double[][][] windows, int from, int count, int seqLen) {
        INDArray arr = Nd4j.create(count, nFeatures, seqLen);
        for (int b = 0; b < count; b++) {                  // b = 小批量内的序号 / index within the mini-batch
            double[][] window = windows[from + b];
            for (int t = 0; t < seqLen; t++) {
                for (int f = 0; f < nFeatures; f++) {
                    arr.putScalar(new int[]{b, f, t}, window[t][f]);
                }
            }
        }
        return arr;
    }

    /**
     * 构建一个小批量的标签掩码张量 {@code [count, nFeatures, seqLen]}。某个窗口的掩码为 null 时，
     * 该窗口整段记为全有效，与逐窗版本的处置一致。
     * Build one mini-batch label mask; a null per-window mask means that window is fully valid,
     * matching the per-window path.
     */
    private INDArray toMaskArray(boolean[][][] weightMasks, int from, int count, int seqLen) {
        INDArray arr = Nd4j.ones(count, nFeatures, seqLen);
        for (int b = 0; b < count; b++) {
            boolean[][] mask = weightMasks[from + b];
            if (mask == null) {
                continue;                                  // 该窗口全有效 / this window is fully valid
            }
            for (int t = 0; t < seqLen; t++) {
                if (mask[t] == null) {
                    continue;
                }
                for (int f = 0; f < nFeatures; f++) {
                    if (!mask[t][f]) {                     // 无效元素置 0 → 该项不计入损失 / excluded from loss
                        arr.putScalar(new int[]{b, f, t}, 0.0);
                    }
                }
            }
        }
        return arr;
    }

    /**
     * 构建 DL4J 标签掩码张量 [1, nFeatures, seqLen]（false → 0.0，true → 1.0）。
     * Build a DL4J label mask tensor [1, nFeatures, seqLen] (false → 0, true → 1).
     */
    private INDArray toMaskArray(boolean[][] mask, int seqLen) {
        INDArray arr = Nd4j.ones(1, nFeatures, seqLen);    // 默认全 1（全部有效）/ default all-ones (all valid)
        for (int t = 0; t < seqLen; t++) {
            if (mask[t] != null) {
                for (int f = 0; f < nFeatures; f++) {
                    if (!mask[t][f]) {                     // 无效元素置 0 → 该项不计入损失 / invalid → 0 → excluded from loss
                        arr.putScalar(new int[]{0, f, t}, 0.0);
                    }
                }
            }
        }
        return arr;
    }

    /**
     * RNN 输出 [1, nFeatures, seqLen] 转回 [seqLen][nFeatures]。
     * Convert RNN output [1, nFeatures, seqLen] back to [seqLen][nFeatures].
     */
    private double[][] fromRnnOutput(INDArray output, int seqLen) {
        double[][] result = new double[seqLen][nFeatures];
        for (int t = 0; t < seqLen; t++) {
            for (int f = 0; f < nFeatures; f++) {
                result[t][f] = output.getDouble(0, f, t);   // [0,f,t] → result[t][f]
            }
        }
        return result;
    }

    /**
     * 序列化模型为字节数组（嵌入 Flink checkpoint 状态）。
     * Serialize model to byte array (to embed in Flink checkpointed state).
     */
    public byte[] serializeModel() throws IOException {
        // 只存"配置 JSON + 扁平参数向量"而非用 ModelSerializer 存整个模型：避免把 DL4J 的临时文件依赖
        // 带进 Flink 状态，字节更小、更适合内存型 checkpoint（决策 7）。
        // Store "config JSON + flat param vector" instead of ModelSerializer's full dump: keeps DL4J's
        // temp-file dependency out of Flink state — smaller bytes, better for memory-backed checkpoints.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(model.getLayerWiseConfigurations().toJson());   // 网络结构 / network config
            INDArray params = model.params();                              // 扁平参数向量 / flat param vector
            oos.writeInt(params.columns());                                // 参数长度 / param length
            double[] flat = new double[params.columns()];
            for (int i = 0; i < flat.length; i++) {
                flat[i] = params.getDouble(i);                            // 逐元素拷出 / copy element by element
            }
            oos.writeObject(flat);
        }
        return baos.toByteArray();
    }

    /**
     * 从字节数组反序列化模型（checkpoint 恢复）。
     * Deserialize model from byte array (checkpoint restore).
     */
    public void deserializeModel(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            String json = (String) ois.readObject();       // 读回结构 / read back the config
            int paramLen = ois.readInt();                  // 读回参数长度 / param length
            double[] flat = (double[]) ois.readObject();   // 读回扁平参数 / flat params
            MultiLayerConfiguration conf = MultiLayerConfiguration.fromJson(json);
            model = new MultiLayerNetwork(conf);
            model.init();                                  // 按结构重建网络 / rebuild network from config
            // 用 [1, paramLen] 形状回填参数：与 serialize 端对称，保证权重逐位一致
            // Restore params as a [1, paramLen] row — symmetric with serialize, weights match bit-for-bit
            INDArray params = Nd4j.create(flat, new int[]{1, paramLen});
            model.setParameters(params);
        }
    }

    public int getNFeatures() { return nFeatures; }
    public int getHiddenSize() { return hiddenSize; }
    public MultiLayerNetwork getModel() { return model; }
}
