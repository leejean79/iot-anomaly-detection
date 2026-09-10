package com.leejean.m3;

import com.leejean.m1.Channels;
import com.leejean.m1.MonitoringSnapshot;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * M3 LSTM 自编码器的 Flink 算子（交接文档 §3 决策 2/3/4/5/6/7）：按设备分键，消费 M2 的
 * {@link AnnotatedRound} 侧输出，每设备独立走状态机 COLLECTING → TRAINING → CALIBRATING → ONLINE。
 * The M3 LSTM autoencoder's Flink operator (handover §3 decisions 2–7): keyed by device,
 * consuming M2's AnnotatedRound side output, each device independently walking the state machine
 * COLLECTING → TRAINING → CALIBRATING → ONLINE.
 *
 * <p>状态机（决策 2，仿 LocalProcessorFunction 模板）：
 * <ol>
 *   <li><b>COLLECTING</b>：校准器冻结后开始，累积训练集（7 天）+ 早停集（2 天）+ 阈值标定集（2 天）；
 *       含离群轮的窗口从训练集剔除（决策 3 训练净化）。</li>
 *   <li><b>TRAINING</b>：同步训练（阻塞当前轮处理），用早停集选最佳 hidden size 与 epoch；单次。</li>
 *   <li><b>CALIBRATING</b>：在阈值标定集上推理，估计 WMSE 的 median/IQR 与协方差逆矩阵。</li>
 *   <li><b>ONLINE</b>：每满窗推理 → 计算主分与 Mahalanobis 分 → 发 M3ScoreRecord。</li>
 * </ol>
 *
 * <p>M1 的校准期（warmupRounds，默认 7 天 = 60,480 轮）后的轮才到达 M2/M3，因此 M3 不再需要
 * 单独跳过校准期——第一条 AnnotatedRound 意味着该设备的校准器已冻结。
 * Rounds only reach M2/M3 after M1's calibration period (warmupRounds, default 7 days = 60,480 rounds),
 * so M3 does not skip a calibration period separately — the first AnnotatedRound means the device's
 * scaler is already frozen.
 */
public class M3Function extends KeyedProcessFunction<String, AnnotatedRound, M3ScoreRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(M3Function.class);

    private static final int N_FEATURES = Channels.N_DET;    // 5
    private static final int ROUNDS_PER_DAY = 8640;          // 10s 轮 × 86,400s/天 = 8,640 / 10s rounds per day
    private static final String DEVICE_G = "G";              // 设备 G Light 通道需特殊处理 / device G Light needs special handling

    private final int trainDays;
    private final int earlyStopDays;
    private final int threshDays;
    private final int windowLength;
    private final double zThreshold;
    private final double[] channelWeights;
    private final int maxEpochs;
    private final int earlyStopPatience;
    private final int[] hiddenSizeGrid;
    private final OutputTag<MonitoringSnapshot> m3MonitoringTag;

    // ---- Flink 状态 / Flink state ----
    private transient ValueState<Integer> statePhase;           // 0=COLLECTING, 1=TRAINING, 2=CALIBRATING, 3=ONLINE
    private transient ValueState<Long> roundCount;
    private transient ListState<double[]> windowBuffer;         // 当前窗口的轮 xNorm 缓冲 / current window round buffer
    private transient ListState<byte[]> windowMaskBuffer;       // 当前窗口的掩码缓冲（boolean[] → byte[]）/ mask buffer
    private transient ListState<Boolean> windowOutlierBuffer;   // 当前窗口的离群标记缓冲 / outlier flag buffer
    private transient ListState<double[]> trainWindows;         // 训练集窗口 [flat: windowLen*nFeatures] / training windows
    private transient ListState<byte[]> trainMasks;             // 训练集掩码 / training masks
    private transient ListState<double[]> earlyStopWindows;
    private transient ListState<byte[]> earlyStopMasks;
    private transient ListState<double[]> threshWindows;
    private transient ListState<byte[]> threshMasks;
    private transient ValueState<byte[]> modelBytes;            // 训练后的模型参数 / trained model bytes
    private transient ValueState<byte[]> scorerBytes;           // 标定后的评分器 / calibrated scorer
    private transient ValueState<Integer> selectedHidden;
    private transient ValueState<Long> trainExcluded;           // 离群净化排除的窗口数 / outlier-sanitized excluded windows

    private transient Counter collectingCount;
    private transient Counter trainingCount;
    private transient Counter onlineCount;

    /**
     * @param trainDays      训练集天数（默认 7）/ training-set days
     * @param earlyStopDays  早停集天数（默认 2）/ early-stopping-set days
     * @param threshDays     阈值标定集天数（默认 2）/ threshold-calibration-set days
     * @param windowLength   窗口长度（轮数，默认 60 = 10 分钟）/ window length in rounds
     * @param zThreshold     z 分阈值（默认 2.22）/ z-score threshold
     * @param channelWeights 通道权重表（null = 全 1）/ channel weights (null = all ones)
     * @param maxEpochs      最大训练 epoch 数 / maximum training epochs
     * @param earlyStopPatience 早停耐心（连续无改善的 epoch 数）/ early-stopping patience
     * @param m3MonitoringTag 监测侧输出标签 / monitoring side-output tag
     */
    public M3Function(int trainDays, int earlyStopDays, int threshDays,
                      int windowLength, double zThreshold, double[] channelWeights,
                      int maxEpochs, int earlyStopPatience,
                      OutputTag<MonitoringSnapshot> m3MonitoringTag) {
        this.trainDays = trainDays;
        this.earlyStopDays = earlyStopDays;
        this.threshDays = threshDays;
        this.windowLength = windowLength;
        this.zThreshold = zThreshold;
        this.channelWeights = channelWeights;
        this.maxEpochs = maxEpochs;
        this.earlyStopPatience = earlyStopPatience;
        this.hiddenSizeGrid = new int[]{40, 60, 90};
        this.m3MonitoringTag = m3MonitoringTag;
    }

    @Override
    public void open(Configuration parameters) {
        statePhase = getRuntimeContext().getState(
                new ValueStateDescriptor<>("m3-phase", Types.INT));
        roundCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("m3-round-count", Types.LONG));
        windowBuffer = getRuntimeContext().getListState(
                new ListStateDescriptor<>("m3-win-buf", PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));
        windowMaskBuffer = getRuntimeContext().getListState(
                new ListStateDescriptor<>("m3-win-mask", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        windowOutlierBuffer = getRuntimeContext().getListState(
                new ListStateDescriptor<>("m3-win-outlier", Types.BOOLEAN));
        trainWindows = getRuntimeContext().getListState(
                new ListStateDescriptor<>("m3-train-win", PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));
        trainMasks = getRuntimeContext().getListState(
                new ListStateDescriptor<>("m3-train-mask", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        earlyStopWindows = getRuntimeContext().getListState(
                new ListStateDescriptor<>("m3-es-win", PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));
        earlyStopMasks = getRuntimeContext().getListState(
                new ListStateDescriptor<>("m3-es-mask", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        threshWindows = getRuntimeContext().getListState(
                new ListStateDescriptor<>("m3-th-win", PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));
        threshMasks = getRuntimeContext().getListState(
                new ListStateDescriptor<>("m3-th-mask", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        modelBytes = getRuntimeContext().getState(
                new ValueStateDescriptor<>("m3-model", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        scorerBytes = getRuntimeContext().getState(
                new ValueStateDescriptor<>("m3-scorer", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
        selectedHidden = getRuntimeContext().getState(
                new ValueStateDescriptor<>("m3-hidden", Types.INT));
        trainExcluded = getRuntimeContext().getState(
                new ValueStateDescriptor<>("m3-train-excluded", Types.LONG));

        collectingCount = getRuntimeContext().getMetricGroup().counter("m3_collecting_rounds");
        trainingCount = getRuntimeContext().getMetricGroup().counter("m3_training_events");
        onlineCount = getRuntimeContext().getMetricGroup().counter("m3_online_windows");
    }

    @Override
    public void processElement(AnnotatedRound round, Context ctx,
                               Collector<M3ScoreRecord> out) throws Exception {
        Integer phase = statePhase.value();
        if (phase == null) phase = 0;
        Long count = roundCount.value();
        if (count == null) count = 0L;

        String device = round.getDevice();
        count++;
        roundCount.update(count);

        switch (phase) {
            case 0:
                handleCollecting(round, ctx, out, count, device);
                break;
            case 3:
                handleOnline(round, ctx, out, device);
                break;
            default:
                break;
        }
    }

    private void handleCollecting(AnnotatedRound round, Context ctx,
                                  Collector<M3ScoreRecord> out, long count,
                                  String device) throws Exception {
        collectingCount.inc();

        double[] xNorm = round.getXNorm().clone();
        zeroDeviceGLight(xNorm, device);
        windowBuffer.add(xNorm);
        boolean[] mask = WeightedMseLoss.buildMask(round.getCensoredMask());
        windowMaskBuffer.add(boolToBytes(mask));
        windowOutlierBuffer.add(round.isOutlier());

        int bufSize = 0;
        for (double[] ignored : windowBuffer.get()) bufSize++;
        if (bufSize < windowLength) return;

        List<double[]> xNorms = new ArrayList<>();
        for (double[] x : windowBuffer.get()) xNorms.add(x);
        List<byte[]> masks = new ArrayList<>();
        for (byte[] m : windowMaskBuffer.get()) masks.add(m);
        List<Boolean> outliers = new ArrayList<>();
        for (Boolean o : windowOutlierBuffer.get()) outliers.add(o);

        boolean hasOutlier = false;
        for (Boolean o : outliers) {
            if (o) { hasOutlier = true; break; }
        }

        double[] flat = flattenWindow(xNorms);
        byte[] flatMask = flattenMasks(masks);

        long trainRounds = (long) trainDays * ROUNDS_PER_DAY;
        long esRounds = (long) earlyStopDays * ROUNDS_PER_DAY;
        long thRounds = (long) threshDays * ROUNDS_PER_DAY;

        if (count <= trainRounds) {
            if (!hasOutlier) {
                trainWindows.add(flat);
                trainMasks.add(flatMask);
            } else {
                Long ex = trainExcluded.value();
                trainExcluded.update(ex == null ? 1L : ex + 1L);
            }
        } else if (count <= trainRounds + esRounds) {
            earlyStopWindows.add(flat);
            earlyStopMasks.add(flatMask);
        } else if (count <= trainRounds + esRounds + thRounds) {
            threshWindows.add(flat);
            threshMasks.add(flatMask);
        }

        windowBuffer.clear();
        windowMaskBuffer.clear();
        windowOutlierBuffer.clear();

        if (count >= trainRounds + esRounds + thRounds) {
            trainAndCalibrate(device, ctx, out);
        }
    }

    private void trainAndCalibrate(String device, Context ctx,
                                   Collector<M3ScoreRecord> out) throws Exception {
        statePhase.update(1);
        trainingCount.inc();

        List<double[]> trainFlats = new ArrayList<>();
        for (double[] f : trainWindows.get()) trainFlats.add(f);
        List<byte[]> trainMaskFlats = new ArrayList<>();
        for (byte[] m : trainMasks.get()) trainMaskFlats.add(m);

        List<double[]> esFlats = new ArrayList<>();
        for (double[] f : earlyStopWindows.get()) esFlats.add(f);
        List<byte[]> esMaskFlats = new ArrayList<>();
        for (byte[] m : earlyStopMasks.get()) esMaskFlats.add(m);

        double[][][] trainData = unflattenWindows(trainFlats);
        boolean[][][] trainMaskData = unflattenMasks(trainMaskFlats);
        double[][][] esData = unflattenWindows(esFlats);

        Long excluded = trainExcluded.value();
        LOG.info("[M3] Device {} entering TRAINING: {} train windows ({} excluded by outlier sanitization), "
                 + "{} early-stop windows",
                 device, trainFlats.size(), excluded != null ? excluded : 0, esFlats.size());

        int bestHidden = hiddenSizeGrid[0];
        double bestEsLoss = Double.MAX_VALUE;
        LstmAutoEncoder bestModel = null;

        for (int hs : hiddenSizeGrid) {
            LstmAutoEncoder ae = new LstmAutoEncoder(N_FEATURES, hs);
            double prevLoss = Double.MAX_VALUE;
            int patience = 0;

            for (int epoch = 0; epoch < maxEpochs; epoch++) {
                ae.trainEpoch(trainData, trainMaskData);

                double esLoss = evaluateLoss(ae, esData);
                if (esLoss < prevLoss - 1e-6) {
                    prevLoss = esLoss;
                    patience = 0;
                } else {
                    patience++;
                    if (patience >= earlyStopPatience) break;
                }
            }

            double finalEsLoss = evaluateLoss(ae, esData);
            LOG.info("[M3] Device {} hidden={}: early-stop loss={}", device, hs, finalEsLoss);

            if (finalEsLoss < bestEsLoss) {
                bestEsLoss = finalEsLoss;
                bestHidden = hs;
                bestModel = ae;
            }
        }

        LOG.info("[M3] Device {} selected hidden={} (loss={})", device, bestHidden, bestEsLoss);
        selectedHidden.update(bestHidden);

        modelBytes.update(bestModel.serializeModel());

        statePhase.update(2);
        calibrate(device, bestModel, ctx);

        trainWindows.clear();
        trainMasks.clear();
        earlyStopWindows.clear();
        earlyStopMasks.clear();

        statePhase.update(3);
        LOG.info("[M3] Device {} entering ONLINE", device);
    }

    private void calibrate(String device, LstmAutoEncoder ae, Context ctx) throws Exception {
        List<double[]> thFlats = new ArrayList<>();
        for (double[] f : threshWindows.get()) thFlats.add(f);
        List<byte[]> thMaskFlats = new ArrayList<>();
        for (byte[] m : threshMasks.get()) thMaskFlats.add(m);

        double[][][] thData = unflattenWindows(thFlats);
        boolean[][][] thMaskData = unflattenMasks(thMaskFlats);

        WeightedMseLoss lossCalc = new WeightedMseLoss(N_FEATURES, channelWeights);
        List<Double> wmseValues = new ArrayList<>();
        List<double[]> perChannelErrors = new ArrayList<>();

        boolean[] activeChannels = new boolean[N_FEATURES];
        for (int c = 0; c < N_FEATURES; c++) {
            activeChannels[c] = channelWeights == null || channelWeights[c] > 0;
        }

        for (int w = 0; w < thData.length; w++) {
            double[][] recon = ae.reconstruct(thData[w]);
            boolean[][] masks = thMaskData != null && thMaskData[w] != null ? thMaskData[w] : null;
            WeightedMseLoss.LossResult lr = lossCalc.compute(thData[w], recon, masks, thData[w].length);
            wmseValues.add(lr.wmse);
            perChannelErrors.add(lr.perChannelMse);
        }

        M3Scorer scorer = new M3Scorer(zThreshold);
        scorer.calibrate(wmseValues, perChannelErrors, activeChannels);

        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos)) {
            oos.writeObject(scorer);
        }
        scorerBytes.update(baos.toByteArray());

        LOG.info("[M3] Device {} calibrated: median={}, IQR={}, threshold={}",
                 device, scorer.getMedian(), scorer.getIqr(), scorer.getThreshold());

        threshWindows.clear();
        threshMasks.clear();
    }

    private void handleOnline(AnnotatedRound round, Context ctx,
                              Collector<M3ScoreRecord> out, String device) throws Exception {
        double[] xNorm = round.getXNorm().clone();
        zeroDeviceGLight(xNorm, device);
        windowBuffer.add(xNorm);
        boolean[] mask = WeightedMseLoss.buildMask(round.getCensoredMask());
        windowMaskBuffer.add(boolToBytes(mask));

        int bufSize = 0;
        for (double[] ignored : windowBuffer.get()) bufSize++;
        if (bufSize < windowLength) return;

        List<double[]> xNorms = new ArrayList<>();
        for (double[] x : windowBuffer.get()) xNorms.add(x);
        List<byte[]> maskBytes = new ArrayList<>();
        for (byte[] m : windowMaskBuffer.get()) maskBytes.add(m);

        windowBuffer.clear();
        windowMaskBuffer.clear();

        byte[] mBytes = modelBytes.value();
        byte[] sBytes = scorerBytes.value();
        if (mBytes == null || sBytes == null) return;

        Integer hs = selectedHidden.value();
        if (hs == null) hs = 60;

        LstmAutoEncoder ae = new LstmAutoEncoder(N_FEATURES, hs);
        ae.deserializeModel(mBytes);

        M3Scorer scorer;
        try (java.io.ObjectInputStream ois = new java.io.ObjectInputStream(
                new java.io.ByteArrayInputStream(sBytes))) {
            scorer = (M3Scorer) ois.readObject();
        }

        double[][] window = new double[windowLength][N_FEATURES];
        boolean[][] masks = new boolean[windowLength][N_FEATURES];
        for (int t = 0; t < windowLength; t++) {
            window[t] = xNorms.get(t);
            masks[t] = bytesToBool(maskBytes.get(t));
        }

        double[][] recon = ae.reconstruct(window);
        WeightedMseLoss lossCalc = new WeightedMseLoss(N_FEATURES, channelWeights);
        WeightedMseLoss.LossResult lr = lossCalc.compute(window, recon, masks, windowLength);
        M3Scorer.ScoreResult sr = scorer.score(lr.wmse, lr.perChannelMse);

        out.collect(new M3ScoreRecord(device, round.getWindowEnd(), sr.mainScore,
                sr.mahaScore, lr.wmse, lr.perChannelMse, sr.aboveThreshold, hs, windowLength));

        onlineCount.inc();

        if (m3MonitoringTag != null) {
            MonitoringSnapshot snap = new MonitoringSnapshot();
            snap.setDevice(device);
            snap.setTs(round.getWindowEnd());
            snap.setWindowEnd(round.getWindowEnd());
            snap.setM3ReconError(lr.wmse);
            snap.setM3PerChannelErrors(lr.perChannelMse);
            ctx.output(m3MonitoringTag, snap);
        }
    }

    private double evaluateLoss(LstmAutoEncoder ae, double[][][] data) {
        if (data.length == 0) return Double.MAX_VALUE;
        WeightedMseLoss lossCalc = new WeightedMseLoss(N_FEATURES, channelWeights);
        double total = 0.0;
        for (double[][] window : data) {
            double[][] recon = ae.reconstruct(window);
            WeightedMseLoss.LossResult lr = lossCalc.compute(window, recon, null, window.length);
            total += lr.wmse;
        }
        return total / data.length;
    }

    private double[] flattenWindow(List<double[]> xNorms) {
        double[] flat = new double[windowLength * N_FEATURES];
        for (int t = 0; t < windowLength; t++) {
            System.arraycopy(xNorms.get(t), 0, flat, t * N_FEATURES, N_FEATURES);
        }
        return flat;
    }

    private byte[] flattenMasks(List<byte[]> masks) {
        byte[] flat = new byte[windowLength * N_FEATURES];
        for (int t = 0; t < windowLength; t++) {
            System.arraycopy(masks.get(t), 0, flat, t * N_FEATURES, N_FEATURES);
        }
        return flat;
    }

    private double[][][] unflattenWindows(List<double[]> flats) {
        double[][][] result = new double[flats.size()][windowLength][N_FEATURES];
        for (int w = 0; w < flats.size(); w++) {
            double[] flat = flats.get(w);
            for (int t = 0; t < windowLength; t++) {
                System.arraycopy(flat, t * N_FEATURES, result[w][t], 0, N_FEATURES);
            }
        }
        return result;
    }

    private boolean[][][] unflattenMasks(List<byte[]> flats) {
        if (flats.isEmpty()) return null;
        boolean[][][] result = new boolean[flats.size()][windowLength][N_FEATURES];
        for (int w = 0; w < flats.size(); w++) {
            byte[] flat = flats.get(w);
            for (int t = 0; t < windowLength; t++) {
                for (int c = 0; c < N_FEATURES; c++) {
                    result[w][t][c] = flat[t * N_FEATURES + c] != 0;
                }
            }
        }
        return result;
    }

    private static byte[] boolToBytes(boolean[] mask) {
        byte[] bytes = new byte[mask.length];
        for (int i = 0; i < mask.length; i++) {
            bytes[i] = mask[i] ? (byte) 1 : (byte) 0;
        }
        return bytes;
    }

    private static boolean[] bytesToBool(byte[] bytes) {
        boolean[] mask = new boolean[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            mask[i] = bytes[i] != 0;
        }
        return mask;
    }

    /**
     * 设备 G 的 Light 通道输入归零（交接文档 §3 决策 5）：Light 通道在 device G 上的量化分辨率
     * 不足，即使权重为零（损失不计入），非零的输入仍会污染编码器的隐藏表示。归零输入使编码器
     * 在该通道上不编码任何信息。
     * Zero device G's Light channel input (handover §3 decision 5): even with zero loss weight,
     * a non-zero input would pollute the encoder's hidden representation. Zeroing the input ensures
     * the encoder encodes no information on that channel for device G.
     *
     * @param xNorm 归一化特征向量（原地修改）/ normalized feature vector (modified in place)
     * @param device 设备 ID / device ID
     */
    private static void zeroDeviceGLight(double[] xNorm, String device) {
        if (DEVICE_G.equals(device) && xNorm.length > Channels.LIGHT_INDEX) {
            xNorm[Channels.LIGHT_INDEX] = 0.0;
        }
    }
}
