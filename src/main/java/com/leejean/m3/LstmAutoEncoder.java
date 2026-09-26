package com.leejean.m3;

import org.deeplearning4j.nn.conf.GradientNormalization;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.LSTM;
import org.deeplearning4j.nn.conf.layers.RnnOutputLayer;
import org.deeplearning4j.nn.conf.layers.misc.RepeatVector;
import org.deeplearning4j.nn.conf.layers.recurrent.LastTimeStep;
import org.deeplearning4j.nn.conf.inputs.InputType;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.indexing.INDArrayIndex;
import org.nd4j.linalg.indexing.NDArrayIndex;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;

import java.io.*;

/**
 * 编码器—解码器式 LSTM 自编码器（裁决书《上下文异常检测模块的网络结构改正与步骤 A 重做》第二节）。
 * Encoder–decoder LSTM autoencoder, per the 2026-09-22 architecture ruling.
 *
 * <p><b>为什么是这个拓扑。</b>旧结构是「一层逐时间步的 LSTM 接一个逐时间步的输出层」，每一步的输出
 * 都能直接看到当前时刻的输入，六十个隐单元足以把五个输入值原样传递过去，重构任务因而是**退化的**：
 * 网络可以学成恒等映射。2026-09-22 的小批量扫描实测到这一点——批处理让训练稳定下来之后，早停集
 * 误差比参照低了十二到三十倍，正是学成近乎恒等的表现。
 * The former topology let every output step see its own input step, so the reconstruction task was
 * degenerate and the network could learn the identity map.
 *
 * <p>现在改为：编码器把**整段窗口**压成一个定长的摘要向量，解码器**只凭这个向量**重构整段窗口。
 * 解码器在任何时间步都接触不到窗口的原始输入——这是本结构的全部意义所在。压缩比也因此是真实的：
 * 一个窗口 L × 5 个数（窗长 60 时为 300 个）被压到 c 个数（隐单元数，网格 {40, 60, 90}）。
 * The encoder now compresses the whole window into one fixed-size vector and the decoder reconstructs
 * from that vector alone, never seeing the raw input. This is the point of the topology.
 *
 * <p>五层堆叠 / five stacked layers:
 * <ol>
 *   <li>编码器 LSTM：5 → c，沿时间正向处理整个窗口 / encoder LSTM over the whole window</li>
 *   <li>取末态（LastTimeStep）：取最后一个时间步的隐状态作为摘要向量，输出降为二维 [批, c]
 *       / take the final hidden state as the summary vector</li>
 *   <li>重复向量（RepeatVector）：把摘要向量在 L 个时间步上重复，还原为三维 [批, c, L]
 *       / repeat the summary across L steps</li>
 *   <li>解码器 LSTM：c → c / decoder LSTM</li>
 *   <li>输出层 RnnOutputLayer：c → 5，恒等激活，带掩码的均方误差 / identity activation, masked MSE</li>
 * </ol>
 *
 * <p><b>窗口长度是结构参数。</b>第 3 层的重复次数等于窗口长度，因此窗长一变网络形状就变，
 * 不能像旧结构那样只在喂数据时体现。构造函数据此多收一个 {@code windowLength}。
 * The window length is now structural: it is the RepeatVector's repetition factor.
 *
 * <p>序列化 / serialization: 参数数组存为字节数组嵌入 Flink 的 checkpoint 状态（决策 7）。
 */
public class LstmAutoEncoder implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int nFeatures;                       // 输入/输出维度（检测通道数，通常 5）/ input & output dims
    private final int hiddenSize;                       // 摘要向量维度 c（网格 {40,60,90} 之一）/ summary vector width
    /** 窗口长度 L。它是结构参数：第 3 层要把摘要向量在 L 个时间步上重复。/ structural: RepeatVector factor. */
    private final int windowLength;
    /**
     * 重构目标是否取逆序（把窗口倒过来作为解码目标）。裁决书第二节按参考文献定为默认开启，
     * 并要求做成可关闭的参数以便将来做消融实验。
     * Whether the reconstruction target is the reversed window; default on, switchable for ablation.
     */
    private final boolean reverseTarget;
    private static final long SEED = 42L;               // 固定随机种子，保证可复现 / fixed seed for reproducibility
    /**
     * Adam 学习率。原为写死的 0.01（为旧的两层结构所定）；2026-09-22 的裁决书第三节明文解除
     * 「不改学习率」的边界，范围限于诊断及其后的选值，故改为构造参数。默认仍是 0.01，
     * 以免在诊断出结论之前悄悄改变既有行为。
     * The Adam learning rate, formerly hard-coded at 0.01 for the two-layer architecture.
     */
    private final double learningRate;
    /** 梯度裁剪阈值，0 表示不裁剪 / L2 clipping threshold; 0 disables. */
    private final double gradClip;
    /** 梯度范数记录器，null 表示不记录。只读，不影响训练结果 / read-only norm recorder. */
    private transient GradientNormRecorder normRecorder;
    private static final double DEFAULT_LEARNING_RATE = 0.001;
    /**
     * 梯度裁剪阈值（按二范数逐层裁剪），0 表示不裁剪。
     *
     * <p>默认值的沿革：起初未启用；2026-09-23 前一份裁决书定为 1.0；同日实测表明 1.0 明确拖慢学习
     * （同轮次区间对照，未裁剪 0.024296 对裁剪 0.040633，差距为抖动带的 1.46 倍），遂改回 0。
     * 2026-09-24 按实测的梯度范数分布定为 **39072.0**：选定批量 64 那次运行的**逐层**第 99.9
     * 百分位为 13024.016204，三倍即 39072.048612，取整为 39072.0。
     *
     * <p><b>必须知情的一点：这个阈值在本次数据上从不触发。</b>该运行的逐层范数最大值为 35636.97，
     * 低于阈值，1184 次更新无一被裁。它是防备日后出现本次未见过的异常大梯度的**保险**，
     * 而不是一个起作用的约束。且余量并不宽裕——该分布尾巴很重，最大值本身就是第 99.9 百分位的
     * 2.74 倍，阈值只比观测到的最大值高一成。
     * This threshold never fires on the measured data: it is insurance against future excursions,
     * not an active constraint, and its margin over the observed maximum is only 10%.
     *
     * <p>阈值与小批量大小绑定：范数随小批量增大而系统性升高（逐层中位数 296.9 / 793.5 / 1107.8
     * 对应小批量 16 / 32 / 64），换批量须重测。
     * The threshold is tied to the batch size; norms grow systematically with it.
     * L2-norm gradient clipping threshold; 0 disables it. Formerly absent, which is what let the
     * 0.01 learning rate diverge for all 60 epochs.
     */
    private static final double DEFAULT_GRAD_CLIP = 39072.0;

    // model 不参与 Java 序列化（transient）；跨 checkpoint 用 serializeModel/deserializeModel 手工搬运。
    // model is transient (excluded from Java serialization); moved across checkpoints via (de)serializeModel.
    private transient MultiLayerNetwork model;

    public LstmAutoEncoder(int nFeatures, int hiddenSize, int windowLength) {
        this(nFeatures, hiddenSize, windowLength, true, DEFAULT_LEARNING_RATE, DEFAULT_GRAD_CLIP);
    }

    public LstmAutoEncoder(int nFeatures, int hiddenSize, int windowLength, boolean reverseTarget) {
        this(nFeatures, hiddenSize, windowLength, reverseTarget,
                DEFAULT_LEARNING_RATE, DEFAULT_GRAD_CLIP);
    }

    public LstmAutoEncoder(int nFeatures, int hiddenSize, int windowLength,
                           boolean reverseTarget, double learningRate) {
        this(nFeatures, hiddenSize, windowLength, reverseTarget, learningRate, DEFAULT_GRAD_CLIP);
    }

    public LstmAutoEncoder(int nFeatures, int hiddenSize, int windowLength,
                           boolean reverseTarget, double learningRate, double gradClip) {
        if (windowLength < 1) {
            throw new IllegalArgumentException("窗口长度须为正，收到 " + windowLength);
        }
        this.nFeatures = nFeatures;
        this.hiddenSize = hiddenSize;
        this.windowLength = windowLength;
        this.reverseTarget = reverseTarget;
        this.learningRate = learningRate;
        this.gradClip = gradClip;
        this.model = buildModel(nFeatures, hiddenSize, windowLength, learningRate, gradClip);
    }

    private static MultiLayerNetwork buildModel(int nFeatures, int hiddenSize, int windowLength,
                                               double learningRate, double gradClip) {
        NeuralNetConfiguration.Builder builder = new NeuralNetConfiguration.Builder()
                .seed(SEED)                                // 可复现 / reproducible
                .updater(new Adam(learningRate))           // Adam 优化器 / Adam optimizer
                .weightInit(WeightInit.XAVIER);            // Xavier 初始化 / Xavier weight init
        if (gradClip > 0) {
            // 按二范数逐层裁剪：某一层的梯度范数超过阈值时整体按比例缩小，不超过则原样通过。
            // 它约束的是更新的步长上界，不改变梯度方向，因此不属于优化器形式的改动。
            // Per-layer L2 clipping: rescale only when the norm exceeds the threshold.
            builder = builder.gradientNormalization(GradientNormalization.ClipL2PerLayer)
                    .gradientNormalizationThreshold(gradClip);
        }
        MultiLayerConfiguration conf = builder
                .list()
                // 第 0 层：编码器 LSTM，沿时间正向读完整个窗口 / encoder LSTM over the whole window
                .layer(0, new LSTM.Builder()
                        .nIn(nFeatures)
                        .nOut(hiddenSize)
                        .activation(Activation.TANH)
                        .build())
                // 第 1 层：取末态。包装层输出最后一个时间步的隐状态，形状由 [批, c, L] 降为 [批, c]，
                // 这个 c 维向量就是整段窗口的摘要——压缩在此发生。
                // LastTimeStep: the summary vector; this is where the compression happens.
                .layer(1, new LastTimeStep(new LSTM.Builder()
                        .nIn(hiddenSize)
                        .nOut(hiddenSize)
                        .activation(Activation.TANH)
                        .build()))
                // 第 2 层：重复向量，把摘要在 L 个时间步上复制，形状回到 [批, c, L]，
                // 供解码器逐步展开。解码器此后看到的每一步输入都相同，与原始窗口无关。
                // RepeatVector: every decoder step sees the same summary, never the raw input.
                .layer(2, new RepeatVector.Builder().repetitionFactor(windowLength).build())
                // 第 3 层：解码器 LSTM / decoder LSTM
                .layer(3, new LSTM.Builder()
                        .nIn(hiddenSize)
                        .nOut(hiddenSize)
                        .activation(Activation.TANH)
                        .build())
                // 第 4 层：逐时间步的输出层，恒等激活，带掩码的均方误差
                // Per-step output layer, identity activation, masked MSE
                .layer(4, new RnnOutputLayer.Builder(LossFunctions.LossFunction.MSE)
                        .nIn(hiddenSize)
                        .nOut(nFeatures)
                        .activation(Activation.IDENTITY)
                        .build())
                .setInputType(InputType.recurrent(nFeatures, windowLength))
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
            // 标签即输入本身；reverseTarget 为真时取时间逆序（裁决书第二节，按参考文献）。
            // 掩码必须跟着一起逆序，否则某个时间步的删失标记会落到别的时间步上。
            // The label is the input itself, time-reversed when reverseTarget is on; the mask must be
            // reversed with it, or a censored step's flag would land on a different step.
            INDArray labels = reverseTarget ? reverseTime(input) : input.dup();

            if (weightMasks != null) {
                // 有掩码：用标签掩码把删失/缺失元素的损失权重置零（决策 3/6）
                // With a mask: a label mask zeroes the loss weight of censored/missing elements
                INDArray mask = toMaskArray(weightMasks, start, actual, seqLen);
                if (reverseTarget) {
                    mask = reverseTime(mask);
                }
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
        // 训练目标是逆序时，输出也是逆序的，必须转回正序才能与原窗口逐步对齐比较。
        // 漏掉这一步会让重建误差凭空变大，且不会有任何报错。
        // When the target is reversed so is the output; it must be un-reversed before comparison.
        if (reverseTarget) {
            output = reverseTime(output);
        }
        return fromRnnOutput(output, seqLen);              // 转回二维数组 / back to 2-D array
    }

    /**
     * 成批推理：取 windows 中从 from 起的 count 个窗口，一次前向传播得到各自的重建
     * {@code [count][windowLen][nFeatures]}。推理不改权重，结果与逐个调用 {@link #reconstruct}
     * 在数学上相同，只差浮点求和顺序；一致性由单元测试把关。
     * Batched inference over `count` windows starting at `from`, one forward pass. Mathematically the
     * same as calling reconstruct per window; equality is guarded by a unit test.
     */
    public double[][][] reconstructBatch(double[][][] windows, int from, int count) {
        int seqLen = windows[from].length;
        INDArray output = model.output(toRnnInput(windows, from, count, seqLen));
        if (reverseTarget) {
            output = reverseTime(output);                  // 与逐窗版本一样转回正序 / un-reverse, as per window
        }
        double[][][] result = new double[count][seqLen][nFeatures];
        for (int b = 0; b < count; b++) {
            for (int t = 0; t < seqLen; t++) {
                for (int f = 0; f < nFeatures; f++) {
                    result[b][t][f] = output.getDouble(b, f, t);   // [b,f,t] → result[b][t][f]
                }
            }
        }
        return result;
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
     * 把 [批, 通道, 时间步] 张量在**时间维**上倒转。用于按参考文献把重构目标取逆序，
     * 以及把逆序的输出转回正序。
     * Reverse a [batch, channels, time] tensor along the time axis.
     */
    private static INDArray reverseTime(INDArray arr) {
        long seqLen = arr.size(2);
        INDArray out = Nd4j.createUninitialized(arr.shape());
        for (long t = 0; t < seqLen; t++) {
            out.put(new INDArrayIndex[]{NDArrayIndex.all(), NDArrayIndex.all(), NDArrayIndex.point(t)},
                    arr.get(NDArrayIndex.all(), NDArrayIndex.all(),
                            NDArrayIndex.point(seqLen - 1 - t)));
        }
        return out;
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
    public int getWindowLength() { return windowLength; }
    /** 参数总数。裁决书第二节要求把参数量写入设计活文档。/ total parameter count, required by the ruling. */
    public long paramCount() { return model.numParams(); }
    public boolean isReverseTarget() { return reverseTarget; }
    public double getLearningRate() { return learningRate; }
    public double getGradClip() { return gradClip; }

    /**
     * 开启梯度范数记录。挂的是 onGradientCalculation，即更新器作用之前——那才是裁剪所作用的量。
     * Attach the recorder; it observes the raw gradient, which is what clipping acts on.
     */
    public GradientNormRecorder recordGradientNorms() {
        normRecorder = new GradientNormRecorder();
        model.addListeners(normRecorder);
        return normRecorder;
    }
    public MultiLayerNetwork getModel() { return model; }
}
