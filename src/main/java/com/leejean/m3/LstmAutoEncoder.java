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

    private final int nFeatures;
    private final int hiddenSize;
    private static final long SEED = 42L;
    private static final double LEARNING_RATE = 0.01;

    private transient MultiLayerNetwork model;

    public LstmAutoEncoder(int nFeatures, int hiddenSize) {
        this.nFeatures = nFeatures;
        this.hiddenSize = hiddenSize;
        this.model = buildModel(nFeatures, hiddenSize);
    }

    private static MultiLayerNetwork buildModel(int nFeatures, int hiddenSize) {
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(SEED)
                .updater(new Adam(LEARNING_RATE))
                .weightInit(WeightInit.XAVIER)
                .list()
                .layer(0, new LSTM.Builder()
                        .nIn(nFeatures)
                        .nOut(hiddenSize)
                        .activation(Activation.TANH)
                        .build())
                .layer(1, new RnnOutputLayer.Builder(LossFunctions.LossFunction.MSE)
                        .nIn(hiddenSize)
                        .nOut(nFeatures)
                        .activation(Activation.IDENTITY)
                        .build())
                .build();
        MultiLayerNetwork net = new MultiLayerNetwork(conf);
        net.init();
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
        double totalLoss = 0.0;
        int count = 0;
        for (int w = 0; w < windows.length; w++) {
            double[][] window = windows[w];
            int seqLen = window.length;
            INDArray input = toRnnInput(window, seqLen);
            INDArray labels = input.dup();

            if (weightMasks != null && weightMasks[w] != null) {
                INDArray mask = toMaskArray(weightMasks[w], seqLen);
                DataSet ds = new DataSet(input, labels, null, mask);
                model.fit(ds);
            } else {
                model.fit(new DataSet(input, labels));
            }
            totalLoss += model.score();
            count++;
        }
        return count > 0 ? totalLoss / count : 0.0;
    }

    /**
     * 推理：给定一个窗口，返回重建结果 [windowLen][nFeatures]。
     * Inference: given one window, return reconstruction [windowLen][nFeatures].
     */
    public double[][] reconstruct(double[][] window) {
        int seqLen = window.length;
        INDArray input = toRnnInput(window, seqLen);
        INDArray output = model.output(input);
        return fromRnnOutput(output, seqLen);
    }

    /**
     * 转换为 RNN 输入张量 [1, nFeatures, seqLen]（DL4J 的 RNN 输入格式）。
     * Convert to RNN input tensor [1, nFeatures, seqLen] (DL4J's RNN input format).
     */
    private INDArray toRnnInput(double[][] window, int seqLen) {
        INDArray arr = Nd4j.create(1, nFeatures, seqLen);
        for (int t = 0; t < seqLen; t++) {
            for (int f = 0; f < nFeatures; f++) {
                arr.putScalar(new int[]{0, f, t}, window[t][f]);
            }
        }
        return arr;
    }

    /**
     * 构建 DL4J 标签掩码张量 [1, nFeatures, seqLen]（false → 0.0，true → 1.0）。
     * Build a DL4J label mask tensor [1, nFeatures, seqLen] (false → 0, true → 1).
     */
    private INDArray toMaskArray(boolean[][] mask, int seqLen) {
        INDArray arr = Nd4j.ones(1, nFeatures, seqLen);
        for (int t = 0; t < seqLen; t++) {
            if (mask[t] != null) {
                for (int f = 0; f < nFeatures; f++) {
                    if (!mask[t][f]) {
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
                result[t][f] = output.getDouble(0, f, t);
            }
        }
        return result;
    }

    /**
     * 序列化模型为字节数组（嵌入 Flink checkpoint 状态）。
     * Serialize model to byte array (to embed in Flink checkpointed state).
     */
    public byte[] serializeModel() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(model.getLayerWiseConfigurations().toJson());
            INDArray params = model.params();
            oos.writeInt(params.columns());
            double[] flat = new double[params.columns()];
            for (int i = 0; i < flat.length; i++) {
                flat[i] = params.getDouble(i);
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
            String json = (String) ois.readObject();
            int paramLen = ois.readInt();
            double[] flat = (double[]) ois.readObject();
            MultiLayerConfiguration conf = MultiLayerConfiguration.fromJson(json);
            model = new MultiLayerNetwork(conf);
            model.init();
            INDArray params = Nd4j.create(flat, new int[]{1, paramLen});
            model.setParameters(params);
        }
    }

    public int getNFeatures() { return nFeatures; }
    public int getHiddenSize() { return hiddenSize; }
    public MultiLayerNetwork getModel() { return model; }
}
