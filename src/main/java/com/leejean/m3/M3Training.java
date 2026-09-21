package com.leejean.m3;

import java.io.Serializable;

/**
 * M3 训练核心：给定窗口集合与超参数，训练出一个 LSTM 自编码器并返回其统计量。
 * The M3 training core: given windows and hyper-parameters, train one LSTM autoencoder.
 *
 * <p><b>这是一个纯函数</b>（补遗三 §0）：它只依赖入参，不读写 Flink 状态，也不引用任何 Flink 类型。
 * 这条约束有两个用途。其一，离线网格 {@link M3Grid} 与在线算子 {@link M3Function} 必须调用同一份
 * 实现，否则补遗三 §6 要求的「在线冷启动误差与网格值相差不超过 5%」的等值核验无从成立——两份平行
 * 实现只要在小批量大小、epoch 上限、早停判据中任意一项不一致，核验就会失败而查不出原因。其二，
 * 后续把训练移出任务线程（补遗三 §7，待补遗四）时，搬动的是这个不带状态的函数，改动因而是机械的。
 * This is a pure function of its inputs: it touches no Flink state and references no Flink type, so
 * the offline grid and the online operator cannot diverge, and moving training off the task thread
 * later is mechanical.
 *
 * <p>缩写自查 / abbreviations: epoch = 在整个训练集上完整跑一遍；
 * 早停（early stopping）= 早停集误差不再下降时提前结束训练；
 * 小批量大小（mini-batch size）= 一次权重更新用到多少个窗口。
 */
public final class M3Training {

    private M3Training() {
        // 工具类，不实例化 / utility class, not instantiable
    }

    /** 训练超参数。不含任何设备或数据相关的内容，故可安全复用于多次训练。 */
    public static final class Config implements Serializable {
        private static final long serialVersionUID = 1L;

        public final int nFeatures;
        public final int hiddenSize;
        /** 小批量大小。1 表示逐窗更新，即 2026-09-21 参照点所用的口径 / 1 = per-window updates. */
        public final int batchSize;
        public final int maxEpochs;
        /** 早停耐心：早停集误差连续这么多个 epoch 没有改善就停 / consecutive no-improvement epochs. */
        public final int patience;
        public final double[] channelWeights;

        public Config(int nFeatures, int hiddenSize, int batchSize,
                      int maxEpochs, int patience, double[] channelWeights) {
            this.nFeatures = nFeatures;
            this.hiddenSize = hiddenSize;
            this.batchSize = batchSize;
            this.maxEpochs = maxEpochs;
            this.patience = patience;
            this.channelWeights = channelWeights;
        }
    }

    /** 训练产物：模型本身，以及用于记录与选型的三个统计量。 */
    public static final class Result {
        public final LstmAutoEncoder model;
        /** 实际跑完的 epoch 数（早停可能使其小于上限）/ epochs actually run. */
        public final int epochs;
        /** 训练结束后在早停集上的误差，即选型判据 / the early-stopping loss, the selection criterion. */
        public final double earlyStopLoss;
        public final double seconds;

        Result(LstmAutoEncoder model, int epochs, double earlyStopLoss, double seconds) {
            this.model = model;
            this.epochs = epochs;
            this.earlyStopLoss = earlyStopLoss;
            this.seconds = seconds;
        }
    }

    /**
     * 每个 epoch 结束时的回调，供调用方输出进度。离线网格用它打印到标准输出，在线算子用它写日志。
     * 传 null 表示不需要进度。训练本身不依赖它。
     * Per-epoch callback for progress reporting; null means no reporting.
     */
    public interface EpochListener {
        void onEpoch(int epoch, double earlyStopLoss, double epochSeconds);
    }

    /**
     * 训练一个模型，带早停。
     *
     * <p>早停的语义是**停止**而非**回滚**：连续 {@code patience} 个 epoch 没有改善即中止训练，
     * 返回的是中止时刻的模型，而不是历史上误差最低那个 epoch 的权重。这是既有行为，2026-09-21 的
     * 参照读数（26 个 epoch、早停集误差 0.160610）就是在此语义下测出的，不得在本轮改动。
     * Early stopping here means stop, not roll back: the returned model is the one at the moment of
     * stopping, not the weights of the best epoch. This is existing behaviour and the 2026-09-21
     * reference reading was measured under it.
     *
     * @param cfg              超参数 / hyper-parameters
     * @param trainWindows     训练集 [窗口][时间步][通道]，已做训练净化 / sanitized training set
     * @param trainMasks       训练集掩码，null = 全有效 / training masks, null = all valid
     * @param earlyStopWindows 早停集，**不做**净化 / early-stopping set, NOT sanitized
     * @param listener         每个 epoch 的进度回调，可为 null / per-epoch callback, may be null
     */
    public static Result train(Config cfg,
                               double[][][] trainWindows,
                               boolean[][][] trainMasks,
                               double[][][] earlyStopWindows,
                               EpochListener listener) {
        long t0 = System.currentTimeMillis();
        LstmAutoEncoder ae = new LstmAutoEncoder(cfg.nFeatures, cfg.hiddenSize);

        double prevLoss = Double.MAX_VALUE;
        int noImprove = 0;                                 // 连续无改善的 epoch 计数 / consecutive no-improvement epochs
        int epochsRun = 0;

        for (int epoch = 0; epoch < cfg.maxEpochs; epoch++) {
            long epochStart = System.currentTimeMillis();
            ae.trainEpoch(trainWindows, trainMasks, cfg.batchSize);
            epochsRun = epoch + 1;

            double esLoss = evaluateLoss(ae, earlyStopWindows, cfg.channelWeights);
            if (listener != null) {
                listener.onEpoch(epochsRun, esLoss, (System.currentTimeMillis() - epochStart) / 1000.0);
            }

            if (esLoss < prevLoss - 1e-6) {
                prevLoss = esLoss;
                noImprove = 0;                             // 有改善则重置耐心 / improvement resets patience
            } else {
                noImprove++;
                if (noImprove >= cfg.patience) {
                    break;
                }
            }
        }

        double finalEsLoss = evaluateLoss(ae, earlyStopWindows, cfg.channelWeights);
        return new Result(ae, epochsRun, finalEsLoss, (System.currentTimeMillis() - t0) / 1000.0);
    }

    /**
     * 早停集误差：逐窗重建，按通道权重算加权均方误差（WMSE），再对窗口取平均。
     *
     * <p><b>本方法刻意保持逐窗推理，不组小批量</b>（补遗三经设计会话确认的步骤 A 口径）。把它也改成
     * 小批量在数值上是等价的（只改变求和顺序），但属于步骤 A 之后才授权的改动，且必须附带一致性测试。
     * Deliberately per-window, not batched: batching it is numerically equivalent but is authorized
     * only after step A and must ship with an equality test.
     */
    public static double evaluateLoss(LstmAutoEncoder ae, double[][][] data, double[] channelWeights) {
        if (data.length == 0) {
            return Double.MAX_VALUE;                       // 空集视为最差损失 / empty set → worst possible loss
        }
        WeightedMseLoss lossCalc = new WeightedMseLoss(ae.getNFeatures(), channelWeights);
        double total = 0.0;
        for (double[][] window : data) {
            double[][] recon = ae.reconstruct(window);
            WeightedMseLoss.LossResult lr = lossCalc.compute(window, recon, null, window.length);
            total += lr.wmse;
        }
        return total / data.length;
    }
}
