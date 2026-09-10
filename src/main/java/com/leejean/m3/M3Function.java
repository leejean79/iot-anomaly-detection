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
        // 读取本设备的状态机相位与已见轮数（首次为空则从 0 起）/ read this device's phase & round count (0 if first)
        Integer phase = statePhase.value();
        if (phase == null) phase = 0;
        Long count = roundCount.value();
        if (count == null) count = 0L;

        String device = round.getDevice();
        count++;                                       // 本轮计入总数 / this round counts toward the total
        roundCount.update(count);

        // 只有 COLLECTING 与 ONLINE 两相位处理输入；TRAINING/CALIBRATING 是同步瞬态，不会在此看到
        // Only COLLECTING and ONLINE handle input; TRAINING/CALIBRATING are synchronous transients
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

        // 拷贝归一化特征，设备 G 的 Light 通道输入归零（决策 5），再入当前窗口缓冲
        // Copy normalized features, zero device G's Light input (decision 5), then buffer into the window
        double[] xNorm = round.getXNorm().clone();
        zeroDeviceGLight(xNorm, device);
        windowBuffer.add(xNorm);
        boolean[] mask = WeightedMseLoss.buildMask(round.getCensoredMask());   // 删失 → 无效掩码 / censored → invalid mask
        windowMaskBuffer.add(boolToBytes(mask));       // boolean[] 存为 byte[] 以适配 Flink 状态 / store as byte[] for Flink state
        windowOutlierBuffer.add(round.isOutlier());    // 记录本轮是否被 M2 判为离群 / record M2's outlier flag

        // 尚未攒满一个窗口则等待下一轮 / wait for more rounds until a full window has accumulated
        int bufSize = 0;
        for (double[] ignored : windowBuffer.get()) bufSize++;
        if (bufSize < windowLength) return;

        // 窗口已满：把缓冲导出为列表 / window is full: drain the buffers into lists
        List<double[]> xNorms = new ArrayList<>();
        for (double[] x : windowBuffer.get()) xNorms.add(x);
        List<byte[]> masks = new ArrayList<>();
        for (byte[] m : windowMaskBuffer.get()) masks.add(m);
        List<Boolean> outliers = new ArrayList<>();
        for (Boolean o : windowOutlierBuffer.get()) outliers.add(o);

        // 训练净化（决策 3）：窗口内只要含一个离群轮，整窗从训练集剔除 / sanitization: any outlier round excludes the whole window
        boolean hasOutlier = false;
        for (Boolean o : outliers) {
            if (o) { hasOutlier = true; break; }
        }

        double[] flat = flattenWindow(xNorms);         // 窗口摊平为一维供状态存储 / flatten the window for state storage
        byte[] flatMask = flattenMasks(masks);

        // 按已见轮数把窗口分配到三段：训练 / 早停 / 阈值标定 / route windows into train / early-stop / threshold segments
        long trainRounds = (long) trainDays * ROUNDS_PER_DAY;
        long esRounds = (long) earlyStopDays * ROUNDS_PER_DAY;
        long thRounds = (long) threshDays * ROUNDS_PER_DAY;

        if (count <= trainRounds) {
            if (!hasOutlier) {
                trainWindows.add(flat);                // 干净窗口入训练集 / clean window → training set
                trainMasks.add(flatMask);
            } else {
                Long ex = trainExcluded.value();       // 含离群的窗口计入被剔除数 / count the excluded window
                trainExcluded.update(ex == null ? 1L : ex + 1L);
            }
        } else if (count <= trainRounds + esRounds) {
            earlyStopWindows.add(flat);                // 早停集（不做净化）/ early-stop set (no sanitization)
            earlyStopMasks.add(flatMask);
        } else if (count <= trainRounds + esRounds + thRounds) {
            threshWindows.add(flat);                   // 阈值标定集 / threshold-calibration set
            threshMasks.add(flatMask);
        }

        // 清空当前窗口缓冲，开始攒下一窗（本阶段为不重叠窗口）/ clear the window buffer for the next (non-overlapping) window
        windowBuffer.clear();
        windowMaskBuffer.clear();
        windowOutlierBuffer.clear();

        // 三段数据齐备 → 触发一次性同步训练与标定 / all three segments collected → trigger one-shot train + calibrate
        if (count >= trainRounds + esRounds + thRounds) {
            trainAndCalibrate(device, ctx, out);
        }
    }

    private void trainAndCalibrate(String device, Context ctx,
                                   Collector<M3ScoreRecord> out) throws Exception {
        statePhase.update(1);                          // 进入 TRAINING 相位 / enter TRAINING phase
        trainingCount.inc();

        // 把状态里的扁平窗口读回并还原为 [窗口][时间步][通道] / read flat windows from state, restore to 3-D
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
        double[][][] esData = unflattenWindows(esFlats);   // 早停集只需输入，掩码不参与选型 / early-stop needs inputs only

        Long excluded = trainExcluded.value();
        LOG.info("[M3] Device {} entering TRAINING: {} train windows ({} excluded by outlier sanitization), "
                 + "{} early-stop windows",
                 device, trainFlats.size(), excluded != null ? excluded : 0, esFlats.size());

        // 在隐藏层宽度网格上各训练一个模型，按早停集损失选最优 / grid-search hidden size, pick the best by early-stop loss
        int bestHidden = hiddenSizeGrid[0];
        double bestEsLoss = Double.MAX_VALUE;
        LstmAutoEncoder bestModel = null;

        for (int hs : hiddenSizeGrid) {
            LstmAutoEncoder ae = new LstmAutoEncoder(N_FEATURES, hs);
            double prevLoss = Double.MAX_VALUE;
            int patience = 0;                          // 连续无改善的 epoch 计数 / consecutive no-improvement epochs

            for (int epoch = 0; epoch < maxEpochs; epoch++) {
                ae.trainEpoch(trainData, trainMaskData);   // 训练一轮 / one training epoch

                // 早停：早停集损失若不再下降超过 patience 轮则停 / early stop when early-stop loss stalls for `patience` epochs
                double esLoss = evaluateLoss(ae, esData);
                if (esLoss < prevLoss - 1e-6) {
                    prevLoss = esLoss;
                    patience = 0;                      // 有改善则重置耐心 / improvement resets patience
                } else {
                    patience++;
                    if (patience >= earlyStopPatience) break;
                }
            }

            double finalEsLoss = evaluateLoss(ae, esData);
            LOG.info("[M3] Device {} hidden={}: early-stop loss={}", device, hs, finalEsLoss);

            if (finalEsLoss < bestEsLoss) {            // 记录早停损失最低的模型 / keep the lowest-early-stop-loss model
                bestEsLoss = finalEsLoss;
                bestHidden = hs;
                bestModel = ae;
            }
        }

        LOG.info("[M3] Device {} selected hidden={} (loss={})", device, bestHidden, bestEsLoss);
        selectedHidden.update(bestHidden);             // 记住选中的隐藏层宽度供在线复原 / remember hidden size for online restore

        modelBytes.update(bestModel.serializeModel()); // 模型参数写入 Flink 状态 / persist model params to Flink state

        statePhase.update(2);                          // 进入 CALIBRATING 相位 / enter CALIBRATING phase
        calibrate(device, bestModel, ctx);

        // 训练/早停集已用完，清空释放状态 / training & early-stop sets consumed, clear to free state
        trainWindows.clear();
        trainMasks.clear();
        earlyStopWindows.clear();
        earlyStopMasks.clear();

        statePhase.update(3);                          // 进入 ONLINE 相位 / enter ONLINE phase
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
        List<Double> wmseValues = new ArrayList<>();       // 标定集每窗口的 WMSE / per-window WMSE over the calibration set
        List<double[]> perChannelErrors = new ArrayList<>();   // 每窗口每通道 MSE / per-window per-channel MSE

        // 活跃通道 = 权重为正的通道（权重 0 的不参与 Mahalanobis）/ active = positive-weight channels (0-weight excluded)
        boolean[] activeChannels = new boolean[N_FEATURES];
        for (int c = 0; c < N_FEATURES; c++) {
            activeChannels[c] = channelWeights == null || channelWeights[c] > 0;
        }

        // 用选中的模型在标定集上逐窗推理，收集误差分布 / run the chosen model over the calibration set, collect the error distribution
        for (int w = 0; w < thData.length; w++) {
            double[][] recon = ae.reconstruct(thData[w]);
            boolean[][] masks = thMaskData != null && thMaskData[w] != null ? thMaskData[w] : null;
            WeightedMseLoss.LossResult lr = lossCalc.compute(thData[w], recon, masks, thData[w].length);
            wmseValues.add(lr.wmse);
            perChannelErrors.add(lr.perChannelMse);
        }

        M3Scorer scorer = new M3Scorer(zThreshold);
        scorer.calibrate(wmseValues, perChannelErrors, activeChannels);   // 拟合 median/IQR 与协方差逆 / fit median/IQR & covariance inverse

        // 评分器整体 Java 序列化后写入 Flink 状态 / serialize the scorer and persist to Flink state
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos)) {
            oos.writeObject(scorer);
        }
        scorerBytes.update(baos.toByteArray());

        LOG.info("[M3] Device {} calibrated: median={}, IQR={}, threshold={}",
                 device, scorer.getMedian(), scorer.getIqr(), scorer.getThreshold());

        threshWindows.clear();                             // 标定集已用完，清空 / calibration set consumed, clear it
        threshMasks.clear();
    }

    private void handleOnline(AnnotatedRound round, Context ctx,
                              Collector<M3ScoreRecord> out, String device) throws Exception {
        // 与 COLLECTING 相同的入窗逻辑（含设备 G Light 归零）/ same window-fill as COLLECTING (incl. device G Light zeroing)
        double[] xNorm = round.getXNorm().clone();
        zeroDeviceGLight(xNorm, device);
        windowBuffer.add(xNorm);
        boolean[] mask = WeightedMseLoss.buildMask(round.getCensoredMask());
        windowMaskBuffer.add(boolToBytes(mask));

        int bufSize = 0;
        for (double[] ignored : windowBuffer.get()) bufSize++;
        if (bufSize < windowLength) return;            // 未满窗则等待 / wait until the window is full

        List<double[]> xNorms = new ArrayList<>();
        for (double[] x : windowBuffer.get()) xNorms.add(x);
        List<byte[]> maskBytes = new ArrayList<>();
        for (byte[] m : windowMaskBuffer.get()) maskBytes.add(m);

        windowBuffer.clear();                          // 立即清空以攒下一窗 / clear immediately for the next window
        windowMaskBuffer.clear();

        // 从状态取回模型与评分器；缺任一则跳过（理论上进入 ONLINE 后必有）/ fetch model & scorer; skip if either is missing
        byte[] mBytes = modelBytes.value();
        byte[] sBytes = scorerBytes.value();
        if (mBytes == null || sBytes == null) return;

        Integer hs = selectedHidden.value();
        if (hs == null) hs = 60;                        // 缺失时回退到网格中值 / fall back to the grid's middle size

        LstmAutoEncoder ae = new LstmAutoEncoder(N_FEATURES, hs);
        ae.deserializeModel(mBytes);                   // 按选中宽度复原模型 / restore the model at the selected width

        M3Scorer scorer;
        try (java.io.ObjectInputStream ois = new java.io.ObjectInputStream(
                new java.io.ByteArrayInputStream(sBytes))) {
            scorer = (M3Scorer) ois.readObject();      // 复原评分器 / restore the scorer
        }

        // 组装二维窗口与掩码供推理 / assemble the 2-D window and mask for inference
        double[][] window = new double[windowLength][N_FEATURES];
        boolean[][] masks = new boolean[windowLength][N_FEATURES];
        for (int t = 0; t < windowLength; t++) {
            window[t] = xNorms.get(t);
            masks[t] = bytesToBool(maskBytes.get(t));
        }

        double[][] recon = ae.reconstruct(window);     // 推理得重建 / reconstruct
        WeightedMseLoss lossCalc = new WeightedMseLoss(N_FEATURES, channelWeights);
        WeightedMseLoss.LossResult lr = lossCalc.compute(window, recon, masks, windowLength);   // 计算误差 / compute errors
        M3Scorer.ScoreResult sr = scorer.score(lr.wmse, lr.perChannelMse);   // 评分 / score

        // 发出评分记录到 synergia-scores / emit the score record to synergia-scores
        out.collect(new M3ScoreRecord(device, round.getWindowEnd(), sr.mainScore,
                sr.mahaScore, lr.wmse, lr.perChannelMse, sr.aboveThreshold, hs, windowLength));

        onlineCount.inc();

        // 如启用监测侧输出，附带重建误差写入 synergia-monitoring / if enabled, also emit recon errors to synergia-monitoring
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

    /** 在数据集上求平均 WMSE（早停选型用，掩码不参与）/ mean WMSE over a dataset (for model selection; no mask). */
    private double evaluateLoss(LstmAutoEncoder ae, double[][][] data) {
        if (data.length == 0) return Double.MAX_VALUE;   // 空集视为最差损失 / empty set → worst possible loss
        WeightedMseLoss lossCalc = new WeightedMseLoss(N_FEATURES, channelWeights);
        double total = 0.0;
        for (double[][] window : data) {
            double[][] recon = ae.reconstruct(window);
            WeightedMseLoss.LossResult lr = lossCalc.compute(window, recon, null, window.length);
            total += lr.wmse;
        }
        return total / data.length;
    }

    /** 窗口 [时间步][通道] 摊平成一维（行优先）供 Flink 状态存储 / flatten [step][channel] to 1-D (row-major) for state. */
    private double[] flattenWindow(List<double[]> xNorms) {
        double[] flat = new double[windowLength * N_FEATURES];
        for (int t = 0; t < windowLength; t++) {
            System.arraycopy(xNorms.get(t), 0, flat, t * N_FEATURES, N_FEATURES);
        }
        return flat;
    }

    /** 掩码字节数组按相同布局摊平 / flatten the mask byte arrays with the same layout. */
    private byte[] flattenMasks(List<byte[]> masks) {
        byte[] flat = new byte[windowLength * N_FEATURES];
        for (int t = 0; t < windowLength; t++) {
            System.arraycopy(masks.get(t), 0, flat, t * N_FEATURES, N_FEATURES);
        }
        return flat;
    }

    /** 一维扁平窗口还原为 [窗口][时间步][通道] / restore flat 1-D windows to [window][step][channel]. */
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

    /** 掩码字节还原为三维布尔（非 0 即 true）；空列表返回 null / restore mask bytes to 3-D booleans (nonzero → true). */
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

    /** boolean[] 编码为 byte[]（Flink 状态不直接支持 boolean[]）/ encode boolean[] as byte[] (Flink state lacks boolean[]). */
    private static byte[] boolToBytes(boolean[] mask) {
        byte[] bytes = new byte[mask.length];
        for (int i = 0; i < mask.length; i++) {
            bytes[i] = mask[i] ? (byte) 1 : (byte) 0;
        }
        return bytes;
    }

    /** byte[] 解码回 boolean[]（非 0 即 true）/ decode byte[] back to boolean[] (nonzero → true). */
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
