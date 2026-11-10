package com.leejean.m3;

import com.leejean.m1.ChannelTransform;
import com.leejean.m1.Channels;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m1.RobustScalerFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * M3 LSTM 自编码器的 Flink 算子（交接文档 §3 决策 2/3/4/5/6/7）：按设备分键，消费 M2 的
 * {@link AnnotatedRound} 侧输出，每设备独立走状态机 COLLECTING → TRAINING → CALIBRATING → ONLINE。
 * The M3 LSTM autoencoder's Flink operator (handover §3 decisions 2–7): keyed by device,
 * consuming M2's AnnotatedRound side output, each device independently walking the state machine
 * COLLECTING → TRAINING → CALIBRATING → ONLINE.
 *
 * <p>状态机（决策 2，仿 LocalProcessorFunction 模板）：
 * <ol>
 *   <li><b>COLLECTING</b>：校准器冻结后开始，累积训练集（7 天）+ 早停集（2 天）+ 阈值标定集（7 天，2026-10-02 裁决起）；
 *       含离群轮的窗口从训练集剔除（决策 3 训练净化）。</li>
 *   <li><b>TRAINING</b>：训练在训练执行器的专用线程上异步进行（补遗四），任务线程照常收元素、推水位线、过检查点
 *       屏障；此间到达的轮进入受检查点保护的有界缓冲。隐藏层宽度固定（离线网格选定 60），早停集只决定训练到第几个
 *       epoch；单次。</li>
 *   <li><b>CALIBRATING</b>：在阈值标定集上推理，估计 WMSE 的 median/IQR 与协方差逆矩阵。标定集在训练前已收齐，
 *       所以标定紧接训练在训练线程上完成，任务线程上只是瞬态。</li>
 *   <li><b>ONLINE</b>：每满窗推理 → 计算主分与 Mahalanobis 分 → 发 M3ScoreRecord。训练期间缓冲的轮先按到达
 *       顺序分批排空（每批 {@code --drain-batch} 轮），排空期间新到的轮追加到缓冲尾部。</li>
 * </ol>
 *
 * <p>补遗四（训练移出任务线程）：结果的收回、模型写入状态、缓冲的读写都只在任务线程上进行；训练线程只运行
 * 纯函数。训练期间与排空期间，配套的 {@link M3Operator} 把输出水位线保持在缓冲中最早一轮之前，使排空时发出的
 * 评分对下游不是迟到记录。作业从检查点恢复时，若设备处于训练态而状态里没有模型，就用受检查点保护的收集窗口
 * 重新提交一次训练（纯函数、种子固定，结果与未被打断时逐位相同）。
 * Addendum 4: results are collected and state is touched on the task thread only; the training thread runs a
 * pure function. While training and draining, {@link M3Operator} holds the output watermark just before the
 * earliest buffered round. On restore, a device in TRAINING without a model resubmits from its checkpointed
 * collection windows.
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
    /** 生产取值：10 秒一轮 × 86,400 秒/天 = 8,640 轮/天。/ production value. */
    public static final int DEFAULT_ROUNDS_PER_DAY = 8640;

    /**
     * 每天折合多少轮。生产上恒为 {@link #DEFAULT_ROUNDS_PER_DAY}；只有单元测试与集群冷启动短重放验证
     * （补遗三步骤 C，经 M2Job 的 --m3-rounds-per-day）才调小它，否则验证一次相位跃迁就要喂 8,640 条
     * 以上的轮。调小它不改变任何算法语义——它只决定「几轮算一天」这个换算，相位跃迁的判据、训练、
     * 标定逻辑一概不变。
     * Rounds per day; production is always the default. Only unit tests and the cluster cold-start probe
     * (addendum 3 step C, via M2Job's --m3-rounds-per-day) shrink it. Shrinking it changes no algorithmic
     * semantics — only how many rounds count as a day.
     */
    private final int roundsPerDay;
    private static final String DEVICE_G = "G";              // 设备 G Light 通道需特殊处理 / device G Light needs special handling
    private static final boolean[] NO_MISSING = new boolean[Channels.N_DET];   // 入口轮五个通道齐全 / admitted rounds
    private static final ChannelTransform[] TRANSFORMS = ChannelTransform.defaultTable();

    private final int trainDays;
    private final int earlyStopDays;
    private final int threshDays;
    private final int windowLength;
    private final double zThreshold;
    private final double[] channelWeights;
    private final int maxEpochs;
    private final int earlyStopPatience;
    /**
     * 隐藏层宽度。补遗三 §3 裁定它是**全机队统一**的参数，由设计会话依离线网格 V-M3-3 定死，
     * 与窗口长度同级。此前此处是硬编码的 {40, 60, 90} 三点搜索，冷启动因而要训练三个模型；
     * 该搜索已按裁定移除，冷启动只训练一个模型。
     * Hidden size is a fleet-wide parameter fixed by the design session from the offline grid;
     * the former built-in {40, 60, 90} search is removed and cold start trains a single model.
     */
    private final int hiddenSize;
    /**
     * 小批量大小。必须与离线网格所用的值一致，否则补遗三 §6 的等值核验（在线冷启动误差与网格值
     * 相差不超过 5%）无从成立。
     * Must match the offline grid's value, or the parity check of addendum 3 §6 cannot hold.
     */
    private final int batchSize;
    /** 重构目标是否取逆序，须与离线网格一致，否则等值核验不成立。/ must match the offline grid. */
    private final boolean reverseTarget;
    /** Adam 学习率，须与离线网格一致，否则等值核验不成立。/ must match the offline grid. */
    private final double learningRate;
    /** 梯度裁剪阈值，须与离线网格一致 / must match the offline grid. */
    private final double gradClip;
    private final OutputTag<MonitoringSnapshot> m3MonitoringTag;
    /** 训练执行器线程数（每个任务管理器）/ training threads per TaskManager (--train-threads). */
    private final int trainThreads;
    /** 每次排空的轮数 / rounds drained per step (--drain-batch). */
    private final int drainBatch;
    /** 训练态下查询结果槽的处理时间间隔（秒）/ result-poll interval in processing time (--poll-interval-sec). */
    private final int pollIntervalSec;

    public static final int DEFAULT_TRAIN_THREADS = 2;
    public static final int DEFAULT_DRAIN_BATCH = 200;
    public static final int DEFAULT_POLL_INTERVAL_SEC = 30;
    /** 失败几次后放弃（补遗四第二节第 2 条第四点：失败后重新提交一次，仍失败则停在训练态并上报）。 */
    private static final int MAX_FAILURES = 2;
    /** 监测状态快照的 windowEnd 取值，用于与点通道、上下文通道、数据接入快照区分 / marks status snapshots. */
    public static final long STATUS_SNAPSHOT_WINDOW_END = -1L;

    // 缓冲状态的描述符供 M3Operator 在恢复时遍历全部分键 / descriptors shared with M3Operator for restore
    static final ValueStateDescriptor<Integer> PENDING_SIZE_DESC =
            new ValueStateDescriptor<>("m3-pending-size", Types.INT);
    static final ValueStateDescriptor<Long> PENDING_HEAD_TS_DESC =
            new ValueStateDescriptor<>("m3-pending-head-ts", Types.LONG);

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
    /**
     * 模型训练时所用的窗口长度。与 {@link #selectedHidden} 一并落在 checkpoint 状态里，目的是
     * 让日后修改参数无法「悄悄地」用不匹配的模型去打分（补遗三 §3）。恢复时若与当前配置不符，
     * 在 {@link #handleOnline} 处直接抛异常而不是凑合打分。
     * The window length the model was trained with; a mismatch on restore fails loudly.
     */
    private transient ValueState<Integer> trainedWindow;
    private transient ValueState<Long> trainExcluded;           // 离群净化排除的窗口数 / outlier-sanitized excluded windows
    // 补遗四：训练期间到达的轮的有界缓冲与训练任务记录 / addendum 4: bounded buffer and training-task records
    private transient ListState<BufferedRound> pendingRounds;   // 按到达顺序 / in arrival order
    private transient ValueState<Integer> pendingSize;
    private transient ValueState<Long> pendingHeadTs;           // 缓冲头那一轮的记录时间戳（毫秒）/ head record ts (ms)
    private transient ValueState<Long> trainTaskId;
    private transient ValueState<Long> trainSubmitMs;           // 提交时刻（处理时间毫秒）/ submit time (processing ms)
    private transient ValueState<Integer> trainFailures;        // 已失败次数 / failed attempts so far
    private transient ValueState<String> deviceName;            // 定时器里没有元素，日志需要设备号 / for timer logs
    // M6b 选项一（决定十三）：缓存数据接入模块附在每个版本第一条轮上的参数集，按训练版本自己从原始值标准化。
    // Option 1 (decision 13): cache every version's parameter set and standardize raw values with the training version.
    private transient MapState<Integer, com.leejean.m6.ParamSet> paramSets;
    private transient ValueState<Integer> curVersion;           // 最近一条轮的参数版本 / version of the latest round
    private transient ValueState<Integer> trainVersion;         // 训练版本（首次提交训练时取当前版本）/ training version
    private transient ValueState<Boolean> rawCollected;         // 收集窗口存的是原始值（回路打开）/ windows hold raw values
    private transient ListState<double[]> shadowBuffer;         // 影子评分的窗口缓冲（按当前版本）/ shadow window buffer
    /** 影子评分开关（决定十三，只在全量重放打开）/ shadow scoring switch (full replay only). */
    private boolean shadowScoring;
    // M6b 决定十七：一级重训的训练长度（天）、热启动与否、训练窗口在生效之前还是之后（开放问题第十二条补跑之后定默认值）。
    // Decision 17: level-1 training length (days), warm start, and whether the window lies before or after the effective time.
    private int retrainDays = 7;
    private boolean retrainWarm = true;
    private boolean retrainAfter = false;
    private transient ListState<HistWindow> history;            // 在线窗口的原始值历史（一级训练集来源）/ raw online windows
    private transient ListState<double[]> windowRawBuffer;      // 当前在线窗口的原始值 / raw rounds of the current window
    private transient ValueState<long[]> retrainSpan;           // 待做的一级训练集范围 {起, 止}（秒）/ pending level-1 span
    private transient ValueState<Integer> retrainVersion;       // 一级训练集所用版本 / version of the level-1 training set
    private transient ValueState<Boolean> retraining;           // 训练态中的这次训练是一级重训 / TRAINING is a level-1 retrain
    private transient ValueState<Integer> modelGen;             // 模型代数，一级切换与二级完成各加一 / model generation
    private transient ValueState<Boolean> level2;               // 二级重训的收集与训练期间 / during a level-2 retrain
    private transient ValueState<Long> collectCount;            // 收集态已收的轮数（二级时归零）/ rounds collected so far

    private transient M3TrainExecutor executor;
    /** 本子任务各分键的缓冲头时间戳（毫秒），供 M3Operator 计算输出水位线；只在任务线程上读写。 */
    private transient Map<String, Long> holdHeadTs;
    /** 本子任务各分键的缓冲轮数，供指标读取（指标线程读，故用并发表）/ buffer sizes for the gauge. */
    private transient Map<String, Integer> bufferSizes;
    /** 各分键缓冲开始排空的处理时间（毫秒）/ processing time when draining started, per key. */
    private transient Map<String, Long> drainStartMs;
    private transient volatile double lastQueueWaitSec;
    private transient volatile double lastTrainWallSec;
    private transient Counter bufferOverflow;

    /**
     * 最近一次训练实际跑完的 epoch 数。**仅供测试**断言早停确实截断了训练——用处理时间下的耗时来判断早停
     * 既不确定也会在慢机器上假失败，用实际 epoch 数则是精确的。生产路径不读取它。
     * Epochs actually run in the most recent training. Test-only: asserting on processing-time duration would
     * be both imprecise and flaky on slow machines, while the epoch count is exact.
     */
    transient int lastTrainEpochs;

    /**
     * 最近一次冷启动里训练了几个模型。**仅供测试**断言算子确实只训练一个模型——补遗三 §3 取消了
     * 算子自带的隐藏层搜索，此前每台设备冷启动要训练三个。本计数器把「只训练一个」这件事变成可断言的
     * 事实，防止日后有人重新引入搜索循环而无人察觉。生产路径不读取它。
     * Models trained in the most recent cold start. Test-only: it makes "exactly one model" assertable.
     */
    transient int lastTrainModels;

    /** 最近一次把训练结果写入状态的线程。**仅供测试**断言结果只在任务线程上收回（补遗四）。 */
    transient Thread lastApplyThread;

    private transient Counter collectingCount;
    private transient Counter trainingCount;
    private transient Counter onlineCount;
    // V-M3-6 的三项测量（2026-10-02 裁决第五节），由监控系统在运行期间采集。
    // The three V-M3-6 measurements (ruling of 2026-10-02 section 5), scraped while the job runs.
    /** 逐窗推理时延：前向传播两侧计时（毫秒）/ per-window forward-pass latency in ms. */
    private transient Histogram inferenceLatencyMs;
    /** 点异常通道转发引入的延迟：滑动步末减轮时间戳（事件时间秒）/ slide end minus round ts, event-time s. */
    private transient Histogram forwardDelaySec;
    /** 直方图保留的最近样本数 / samples kept by each histogram. */
    private static final int HISTOGRAM_WINDOW = 10_000;

    /**
     * @param trainDays      训练集天数（默认 7）/ training-set days
     * @param earlyStopDays  早停集天数（默认 2）/ early-stopping-set days
     * @param threshDays     阈值标定集天数（默认 2）/ threshold-calibration-set days
     * @param windowLength   窗口长度（轮数，默认 60 = 10 分钟）/ window length in rounds
     * @param zThreshold     z 分阈值（默认 2.22）/ z-score threshold
     * @param channelWeights 通道权重表（null = 全 1）/ channel weights (null = all ones)
     * @param maxEpochs      最大训练 epoch 数 / maximum training epochs
     * @param earlyStopPatience 早停耐心（连续无改善的 epoch 数）/ early-stopping patience
     * @param hiddenSize     隐藏层宽度，全机队统一 / fleet-wide hidden size
     * @param batchSize      小批量大小，须与离线网格一致 / mini-batch size, must match the grid
     * @param m3MonitoringTag 监测侧输出标签 / monitoring side-output tag
     */
    public M3Function(int trainDays, int earlyStopDays, int threshDays,
                      int windowLength, double zThreshold, double[] channelWeights,
                      int maxEpochs, int earlyStopPatience, int hiddenSize, int batchSize,
                      boolean reverseTarget, double learningRate, double gradClip,
                      OutputTag<MonitoringSnapshot> m3MonitoringTag) {
        this(trainDays, earlyStopDays, threshDays, windowLength, zThreshold, channelWeights,
                maxEpochs, earlyStopPatience, hiddenSize, batchSize, reverseTarget,
                learningRate, gradClip, m3MonitoringTag, DEFAULT_ROUNDS_PER_DAY);
    }

    /**
     * 可以指定 {@code roundsPerDay} 的构造函数。原为包级私有、仅供测试；冷启动短重放验证（补遗三步骤 C）
     * 要在集群作业里调小它，而 M2Job 在另一个包，因此改为 public。生产与验收运行不传
     * --m3-rounds-per-day，M2Job 传入的就是 {@link #DEFAULT_ROUNDS_PER_DAY}。
     * Constructor taking {@code roundsPerDay}. Formerly package-private for tests; made public because the
     * cold-start probe (addendum 3 step C) shrinks it from M2Job, which lives in another package.
     */
    public M3Function(int trainDays, int earlyStopDays, int threshDays,
               int windowLength, double zThreshold, double[] channelWeights,
               int maxEpochs, int earlyStopPatience, int hiddenSize, int batchSize,
               boolean reverseTarget, double learningRate, double gradClip,
               OutputTag<MonitoringSnapshot> m3MonitoringTag, int roundsPerDay) {
        this(trainDays, earlyStopDays, threshDays, windowLength, zThreshold, channelWeights,
                maxEpochs, earlyStopPatience, hiddenSize, batchSize, reverseTarget,
                learningRate, gradClip, m3MonitoringTag, roundsPerDay,
                DEFAULT_TRAIN_THREADS, DEFAULT_DRAIN_BATCH, DEFAULT_POLL_INTERVAL_SEC);
    }

    /**
     * 带补遗四三个参数的构造函数 / constructor with the three addendum-4 parameters.
     *
     * @param trainThreads    训练执行器线程数（每个任务管理器）/ training threads per TaskManager
     * @param drainBatch      每次排空的轮数 / rounds drained per step
     * @param pollIntervalSec 训练态下查询结果的间隔（处理时间秒）/ result-poll interval (processing-time seconds)
     */
    public M3Function(int trainDays, int earlyStopDays, int threshDays,
               int windowLength, double zThreshold, double[] channelWeights,
               int maxEpochs, int earlyStopPatience, int hiddenSize, int batchSize,
               boolean reverseTarget, double learningRate, double gradClip,
               OutputTag<MonitoringSnapshot> m3MonitoringTag, int roundsPerDay,
               int trainThreads, int drainBatch, int pollIntervalSec) {
        if (drainBatch < 1 || pollIntervalSec < 1) {
            throw new IllegalArgumentException("--drain-batch 与 --poll-interval-sec 必须为正");
        }
        this.trainThreads = trainThreads;
        this.drainBatch = drainBatch;
        this.pollIntervalSec = pollIntervalSec;
        this.roundsPerDay = roundsPerDay;
        this.trainDays = trainDays;
        this.earlyStopDays = earlyStopDays;
        this.threshDays = threshDays;
        this.windowLength = windowLength;
        this.zThreshold = zThreshold;
        this.channelWeights = channelWeights;
        this.maxEpochs = maxEpochs;
        this.earlyStopPatience = earlyStopPatience;
        this.hiddenSize = hiddenSize;
        this.batchSize = batchSize;
        this.reverseTarget = reverseTarget;
        this.learningRate = learningRate;
        this.gradClip = gradClip;
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
        trainedWindow = getRuntimeContext().getState(
                new ValueStateDescriptor<>("m3-trained-window", Types.INT));
        trainExcluded = getRuntimeContext().getState(
                new ValueStateDescriptor<>("m3-train-excluded", Types.LONG));
        pendingRounds = getRuntimeContext().getListState(
                new ListStateDescriptor<>("m3-pending-rounds", TypeInformation.of(BufferedRound.class)));
        pendingSize = getRuntimeContext().getState(PENDING_SIZE_DESC);
        pendingHeadTs = getRuntimeContext().getState(PENDING_HEAD_TS_DESC);
        trainTaskId = getRuntimeContext().getState(new ValueStateDescriptor<>("m3-train-task-id", Types.LONG));
        trainSubmitMs = getRuntimeContext().getState(new ValueStateDescriptor<>("m3-train-submit-ms", Types.LONG));
        trainFailures = getRuntimeContext().getState(new ValueStateDescriptor<>("m3-train-failures", Types.INT));
        deviceName = getRuntimeContext().getState(new ValueStateDescriptor<>("m3-device", Types.STRING));
        paramSets = getRuntimeContext().getMapState(new MapStateDescriptor<>("m3-param-sets", Types.INT,
                TypeInformation.of(com.leejean.m6.ParamSet.class)));
        curVersion = getRuntimeContext().getState(new ValueStateDescriptor<>("m3-cur-version", Types.INT));
        trainVersion = getRuntimeContext().getState(new ValueStateDescriptor<>("m3-train-version", Types.INT));
        rawCollected = getRuntimeContext().getState(new ValueStateDescriptor<>("m3-raw-collected", Types.BOOLEAN));
        shadowBuffer = getRuntimeContext().getListState(
                new ListStateDescriptor<>("m3-shadow-buf", PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));
        history = getRuntimeContext().getListState(new ListStateDescriptor<>("m3-history", TypeInformation.of(HistWindow.class)));
        windowRawBuffer = getRuntimeContext().getListState(
                new ListStateDescriptor<>("m3-win-raw", PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO));
        retrainSpan = getRuntimeContext().getState(new ValueStateDescriptor<>("m3-retrain-span", TypeInformation.of(long[].class)));
        retrainVersion = getRuntimeContext().getState(new ValueStateDescriptor<>("m3-retrain-version", Types.INT));
        retraining = getRuntimeContext().getState(new ValueStateDescriptor<>("m3-retraining", Types.BOOLEAN));
        modelGen = getRuntimeContext().getState(new ValueStateDescriptor<>("m3-model-gen", Types.INT));
        level2 = getRuntimeContext().getState(new ValueStateDescriptor<>("m3-level2", Types.BOOLEAN));
        collectCount = getRuntimeContext().getState(new ValueStateDescriptor<>("m3-collect-count", Types.LONG));
        if (shadowScoring && m3MonitoringTag == null) {
            LOG.warn("[M3] Shadow scoring is on but the monitoring side output is off: shadow scores are not recorded");
        }

        executor = new M3TrainExecutor(trainThreads);
        holdHeadTs = new HashMap<>();
        bufferSizes = new ConcurrentHashMap<>();
        drainStartMs = new HashMap<>();
        bufferOverflow = getRuntimeContext().getMetricGroup().counter("m3_train_buffer_overflow");
        getRuntimeContext().getMetricGroup().gauge("m3_train_buffer_size",
                (Gauge<Integer>) () -> bufferSizes.values().stream().mapToInt(Integer::intValue).sum());
        getRuntimeContext().getMetricGroup().gauge("m3_train_queue_wait_sec", (Gauge<Double>) () -> lastQueueWaitSec);
        getRuntimeContext().getMetricGroup().gauge("m3_train_wall_sec", (Gauge<Double>) () -> lastTrainWallSec);

        collectingCount = getRuntimeContext().getMetricGroup().counter("m3_collecting_rounds");
        trainingCount = getRuntimeContext().getMetricGroup().counter("m3_training_events");
        onlineCount = getRuntimeContext().getMetricGroup().counter("m3_online_windows");
        inferenceLatencyMs = getRuntimeContext().getMetricGroup().histogram(
                "m3_inference_latency_ms", new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW));
        forwardDelaySec = getRuntimeContext().getMetricGroup().histogram(
                "m3_forward_delay_sec", new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW));
        // 任务管理器堆外内存：JavaCPP 读到的进程物理内存（RSS），以及 JavaCPP 自己分配、尚未释放的堆外字节数。
        // 同一个 TaskManager 上的各子任务读到的是同一个进程的值。
        // Physical memory of the TaskManager process as JavaCPP reads it, and JavaCPP's own off-heap bytes;
        // every subtask in one TaskManager reports the same process-wide value.
        getRuntimeContext().getMetricGroup().gauge("m3_javacpp_physical_bytes",
                (Gauge<Long>) org.bytedeco.javacpp.Pointer::physicalBytes);
        getRuntimeContext().getMetricGroup().gauge("m3_javacpp_total_bytes",
                (Gauge<Long>) org.bytedeco.javacpp.Pointer::totalBytes);
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
        // 点异常通道转发引入的延迟：本轮所在滑动步的窗口末减去本轮时间戳，按事件时间计（秒）。
        // Delay added by the point channel's forwarding: slide end minus round timestamp, in event time.
        forwardDelaySec.update(round.getWindowEnd() - round.getTs());
        count++;                                       // 本轮计入总数 / this round counts toward the total
        roundCount.update(count);
        // 选项一：到达即缓存参数集与当前版本（训练与排空期间的轮也在到达时登记）/ cache on arrival
        if (round.getParamSet() != null) {
            paramSets.put(round.getParamSet().version, round.getParamSet());
        }
        if (round.getVersion() != null) {
            curVersion.update(round.getVersion());
        }

        // COLLECTING 收集三段数据；TRAINING 期间到达的轮入缓冲并查询一次结果槽；ONLINE 时缓冲非空则只追加到
        // 尾部，缓冲为空则直接评分。元素路径不排空、不登记定时器：排空与查询只由每台设备唯一的一条定时器链推进，
        // 使批与批之间任务线程能处理输入与检查点屏障（2026-10-17 裁决，开发设计日志 0.86 版第四节）。
        // CALIBRATING 只是任务线程上的瞬态，不会在此看到。
        // COLLECTING collects; in TRAINING a round is buffered and the result slot polled; in ONLINE a non-empty
        // buffer is only appended to, otherwise the round is scored directly. The element path never drains and
        // never registers timers: one timer chain per device drives polling and draining, so input and checkpoint
        // barriers are processed between batches.
        switch (phase) {
            case 0:
                handleCollecting(round, ctx, out, device);
                break;
            case 1:
                bufferRound(round, ctx, device);
                pollTraining(ctx);                     // 结果就绪则转为在线；排空由定时器链开始 / drain starts on the timer
                break;
            case 3:
                Integer pending = pendingSize.value();
                if (pending != null && pending > 0) {
                    bufferRound(round, ctx, device);
                } else if (!handleOnline(round, ctx, out, device)) {
                    redispatch(round, ctx, out, device);   // 一级或二级改变了相位 / a level-1/2 command changed the phase
                }
                break;
            default:
                break;
        }
    }

    /**
     * 处理时间定时器（补遗四）：训练态下查询结果槽，排空态下继续排空，并在需要时重新登记下一次。
     * Processing-time timer: poll the result slot while training, keep draining while the buffer is non-empty.
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<M3ScoreRecord> out) throws Exception {
        advance(ctx, out);
        Integer phase = statePhase.value();
        if (phase != null && phase == 1 && m3MonitoringTag != null) {
            emitStatusSnapshot(ctx, phase);
        }
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            executor.close();                          // 中断本实例的训练 / interrupt this instance's training
        }
        super.close();
    }

    /**
     * 推进训练与排空（只在定时器回调中调用，即每台设备唯一的定时器链）：训练态查询结果，结果就绪则写入状态并
     * 转为在线态；在线态缓冲非空则排空一批；最后按相位续登下一次定时器。链由提交训练时的那次登记开始。
     * Advance training and draining from the timer callback only (one timer chain per device, started when
     * training is submitted), then re-register the next timer for the phase.
     */
    private void advance(Context ctx, Collector<M3ScoreRecord> out) throws Exception {
        Integer phase = statePhase.value();
        if (phase != null && phase == 1) {
            pollTraining(ctx);
            phase = statePhase.value();
        }
        Integer pending = pendingSize.value();
        boolean draining = phase != null && phase == 3 && pending != null && pending > 0;
        if (draining) {
            drainBatch(ctx, out);
            pending = pendingSize.value();
            draining = pending != null && pending > 0;
        }
        long now = ctx.timerService().currentProcessingTime();
        if (phase != null && phase == 1) {
            ctx.timerService().registerProcessingTimeTimer(now + pollIntervalSec * 1000L);
        } else if (draining) {
            // 排空节拍：下一个邮箱轮次继续，期间元素、检查点屏障照常处理 / next mailbox turn keeps draining
            ctx.timerService().registerProcessingTimeTimer(now + 1);
        }
    }

    /** 训练态：查询结果槽；未提交（例如从检查点恢复后）则重新提交 / poll; resubmit if nothing is in flight. */
    private void pollTraining(Context ctx) throws Exception {
        String key = ctx.getCurrentKey();
        String device = deviceName.value();
        M3TrainExecutor.Outcome o = executor.poll(key);
        if (o == M3TrainExecutor.PENDING) {
            return;
        }
        if (o == M3TrainExecutor.NOT_SUBMITTED) {
            Integer failures = trainFailures.value();
            if ((modelBytes.value() == null || Boolean.TRUE.equals(retraining.value()))
                    && (failures == null || failures < MAX_FAILURES)) {
                // 训练线程里的进行中训练不进检查点；恢复后结果槽为空，用受检查点保护的收集窗口重新提交。
                // In-flight training is not checkpointed; after a restore the slot is empty, so resubmit.
                LOG.info("[M3] Device {} is in TRAINING without a model and nothing in flight "
                         + "(restored from a checkpoint): resubmitting", device);
                submitTraining(key, device, ctx);
            }
            return;
        }
        Long taskId = trainTaskId.value();
        if (o.error != null || taskId == null || o.taskId != taskId) {
            int failures = (trainFailures.value() == null ? 0 : trainFailures.value()) + 1;
            trainFailures.update(failures);
            LOG.warn("[M3] Device {} training task {} failed ({} of {}): {}", device,
                     o.taskId, failures, MAX_FAILURES, o.error == null ? "stale task id" : o.error.toString());
            if (failures < MAX_FAILURES) {
                submitTraining(key, device, ctx);
            } else if (Boolean.TRUE.equals(retraining.value())) {
                // 一级重训失败两次：放弃这次重训，旧模型继续服务（缓冲的轮由旧模型排空）/ abandon; the old model drains
                LOG.warn("[M3] Device {} REPORT: level-1 retrain failed {} times; abandoned, the old model keeps serving",
                         device, failures);
                endRetrain();
                statePhase.update(3);
            } else {
                LOG.warn("[M3] Device {} REPORT: training failed {} times; the device stays in TRAINING",
                         device, failures);
            }
            return;
        }
        applyOutcome(key, device, o);
    }


    private void handleCollecting(AnnotatedRound round, Context ctx,
                                  Collector<M3ScoreRecord> out,
                                  String device) throws Exception {
        collectingCount.inc();
        if (round.getContextCommand() != null) {
            onContextCommand(round.getContextCommand(), ctx, device);   // 收集态只接受二级（重新开始收集）/ only level 2 here
            round.setContextCommand(null);             // 只执行一次（本轮可能被重新分派或留在缓冲）/ execute once
            if (statePhase.value() == null || statePhase.value() != 0) {
                return;
            }
        }
        // 收集态自己的计数：冷启动时与已见轮数相同；二级重训时从零起 / own counter: equals the round count at cold start
        long count = (collectCount.value() == null ? 0L : collectCount.value()) + 1;
        collectCount.update(count);

        // 拷贝归一化特征，设备 G 的 Light 通道输入归零（决策 5），再入当前窗口缓冲。回路打开时存原始值，提交训练时按训练版本
        // 统一标准化（选项一），归零也在那时做。
        // Copy normalized features, zero device G's Light input (decision 5), then buffer into the window. With the loop
        // on, raw values are stored and standardized with the training version at submission (option 1).
        double[] xNorm;
        if (round.getRaw() != null) {
            xNorm = round.getRaw().clone();
            rawCollected.update(true);
        } else {
            xNorm = round.getXNorm().clone();
            zeroDeviceGLight(xNorm, device);
        }
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
        long trainRounds = (long) trainDays * roundsPerDay;
        long esRounds = (long) earlyStopDays * roundsPerDay;
        long thRounds = (long) threshDays * roundsPerDay;

        if (count <= trainRounds) {
            if (!hasOutlier) {
                trainWindows.add(flat);                // 干净窗口入训练集 / clean window → training set
                trainMasks.add(flatMask);
            } else {
                Long ex = trainExcluded.value();       // 含离群的窗口计入被剔除数 / count the excluded window
                trainExcluded.update(ex == null ? 1L : ex + 1L);
                // 记下被剔除窗口的标识（最后一轮的时间戳），供等值核验与离线网格逐一比对剔除集合
                // （2026-10-02 裁决第二节第 2 条）。/ log the excluded window's id for the parity check.
                LOG.info("[M3] Device {} excluded training window ending at round {}", device, round.getTs());
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

        if (Boolean.TRUE.equals(level2.value()) && m3MonitoringTag != null) {
            emitRetrainingMarker(ctx, round.getWindowEnd(), device);   // 二级重训中的质量标记（每窗一条）/ per window
        }

        // 三段数据齐备 → 把训练提交给训练执行器（补遗四）/ all three segments collected → submit training
        if (count >= trainRounds + esRounds + thRounds) {
            deviceName.update(device);
            submitTraining(ctx.getCurrentKey(), device, ctx);
            ctx.timerService().registerProcessingTimeTimer(
                    ctx.timerService().currentProcessingTime() + pollIntervalSec * 1000L);
        }
    }

    /**
     * 把三段窗口从状态复制成不可变快照，连同配置组成训练任务提交给训练执行器（只在任务线程上调用）。
     * 三段窗口保留在状态里，直到结果写入状态为止，这样从检查点恢复后可以用同一份数据重新提交。
     * Copy the three segments into an immutable snapshot and submit it; the segments stay in state until the
     * result is applied, so a restore can resubmit from the same data.
     */
    private void submitTraining(String key, String device, Context ctx) throws Exception {
        statePhase.update(1);                          // 进入 TRAINING 相位 / enter TRAINING phase
        trainingCount.inc();
        long taskId = trainTaskId.value() == null ? 1L : trainTaskId.value() + 1L;
        trainTaskId.update(taskId);
        trainSubmitMs.update(ctx.timerService().currentProcessingTime());

        // 把状态里的扁平窗口读回并还原为 [窗口][时间步][通道]（新数组即快照）/ restore to 3-D; new arrays = snapshot
        List<double[]> trainFlats = new ArrayList<>();
        for (double[] f : trainWindows.get()) trainFlats.add(f);
        List<byte[]> trainMaskFlats = new ArrayList<>();
        for (byte[] m : trainMasks.get()) trainMaskFlats.add(m);
        List<double[]> esFlats = new ArrayList<>();
        for (double[] f : earlyStopWindows.get()) esFlats.add(f);
        List<double[]> thFlats = new ArrayList<>();
        for (double[] f : threshWindows.get()) thFlats.add(f);
        List<byte[]> thMaskFlats = new ArrayList<>();
        for (byte[] m : threshMasks.get()) thMaskFlats.add(m);

        // 选项一：收集窗口存的是原始值时，按训练版本统一标准化。训练版本在第一次提交时取当前版本并记入状态，从检查点恢复后
        // 重新提交沿用同一版本，保证逐位可复现。/ Option 1: standardize raw windows with the training version, fixed at
        // the first submission so a resubmission after a restore uses the same version.
        boolean isRetrain = Boolean.TRUE.equals(retraining.value());
        if (!isRetrain && Boolean.TRUE.equals(rawCollected.value())) {
            Integer tv = trainVersion.value();
            if (tv == null) {
                tv = curVersion.value();
                trainVersion.update(tv);
            }
            com.leejean.m6.ParamSet ps = params(tv, device);
            trainFlats = standardize(trainFlats, ps, device);
            esFlats = standardize(esFlats, ps, device);
            thFlats = standardize(thFlats, ps, device);
            LOG.info("[M3] Device {} training version {}: collected raw windows standardized with its parameter set",
                     device, tv);
        }

        Long excluded = trainExcluded.value();
        // 记下子任务编号：同一子任务上的设备共用一个任务线程（代理键使每个子任务只承载一台设备）。
        // The subtask index is logged; surrogate keys put one device on each subtask.
        LOG.info("[M3] Device {} (subtask {}) entering TRAINING: {} train windows ({} excluded by outlier "
                 + "sanitization), {} early-stop windows",
                 device, getRuntimeContext().getIndexOfThisSubtask(), trainFlats.size(),
                 excluded != null ? excluded : 0, esFlats.size());

        // 训练单个模型（补遗三 §3：隐藏层宽度是全机队统一参数，冷启动不再自行搜索）。训练本身交给 M3Training
        // 这个纯函数，与离线网格 M3Grid 调用同一份实现，使等值核验有意义。
        // Train a single model via the shared pure function, so the online path and the offline grid cannot diverge.
        M3Training.Config cfg = new M3Training.Config(
                N_FEATURES, hiddenSize, windowLength, batchSize, maxEpochs,
                earlyStopPatience, channelWeights, reverseTarget, learningRate, gradClip);
        // 一级重训（决定十七）：热启动从当前模型的权重出发，冷启动从同一种子新建；三段已按当前版本标准化。
        // Level-1 retrain: warm start from the current weights, or a fresh model; segments already standardized.
        byte[] init = isRetrain && retrainWarm ? modelBytes.value() : null;
        M3TrainExecutor.TrainTask task = new M3TrainExecutor.TrainTask(
                key, device, taskId, isRetrain ? M3TrainExecutor.Purpose.RETRAIN : M3TrainExecutor.Purpose.COLD_START,
                cfg, LstmAutoEncoder.SEED, zThreshold, new M3TrainExecutor.SanitizeOptions(),
                unflattenWindows(trainFlats), unflattenMasks(trainMaskFlats), unflattenWindows(esFlats),
                unflattenWindows(thFlats), unflattenMasks(thMaskFlats), init);
        final String dev = device;
        executor.submit(task, (epoch, trainLoss, esLoss, epochSeconds) ->
                // 逐 epoch 落日志是看到训练仍在推进的唯一途径，也是逐设备训练墙钟时间的数据来源。
                // Per-epoch logging is the only visibility into training progress.
                LOG.info("[M3] Device {} training epoch {}/{}: train loss={}, early-stop loss={}, {}s",
                         dev, epoch, maxEpochs, trainLoss, esLoss, epochSeconds));
        LOG.info("[M3] Device {} training task {} submitted to the executor", device, taskId);
    }

    /**
     * 把训练结果写入状态并转为在线态（只在任务线程上调用）/ apply a result to state and go ONLINE (task thread).
     */
    private void applyOutcome(String key, String device, M3TrainExecutor.Outcome o) throws Exception {
        lastApplyThread = Thread.currentThread();
        lastTrainEpochs = o.result.epochs;
        lastTrainModels++;
        lastQueueWaitSec = o.queueWaitSec;
        lastTrainWallSec = o.wallSec;

        statePhase.update(2);                          // CALIBRATING：标定已在训练线程上完成 / calibrated already
        selectedHidden.update(hiddenSize);             // 记住训练时的隐藏层宽度供在线复原 / hidden size used
        trainedWindow.update(windowLength);            // 一并记住窗口长度，防止参数变更后静默错配 / and the window length
        modelBytes.update(o.modelBytes);               // 模型参数写入 Flink 状态 / persist model params to Flink state
        scorerBytes.update(o.scorerBytes);             // 评分器写入 Flink 状态 / persist the calibrated scorer

        // 一级切换（决定十七）：取当前版本参数集，输出版本号改为新训练版本；未满的窗口按新版本从原始值重新标准化，使切换
        // 落在窗口边界上。二级完成同样计一代。/ Level-1 switch: adopt the retrain version; rebuild the partial window.
        boolean wasRetrain = Boolean.TRUE.equals(retraining.value());
        if (wasRetrain || Boolean.TRUE.equals(level2.value())) {
            int gen = (modelGen.value() == null ? 0 : modelGen.value()) + 1;
            modelGen.update(gen);
            if (wasRetrain) {
                Integer old = trainVersion.value();
                trainVersion.update(retrainVersion.value());
                List<double[]> raws = new ArrayList<>();
                for (double[] r : windowRawBuffer.get()) raws.add(r);
                List<double[]> rebuilt = new ArrayList<>();
                com.leejean.m6.ParamSet ps = params(retrainVersion.value(), device);
                for (double[] r : raws) rebuilt.add(standardize(r, ps, device));
                windowBuffer.update(rebuilt);
                LOG.info("[M3] Device {} level-1 switch: model generation {}, training version {} -> {}; "
                         + "{} rounds of the open window re-standardized", device, gen, old, retrainVersion.value(),
                         rebuilt.size());
                endRetrain();
            } else {
                level2.clear();
                LOG.info("[M3] Device {} level-2 retrain complete: model generation {}, training version {}",
                         device, gen, trainVersion.value());
            }
        }

        // 三段数据已用完，清空释放状态 / segments consumed, clear to free state
        trainWindows.clear();
        trainMasks.clear();
        earlyStopWindows.clear();
        earlyStopMasks.clear();
        threshWindows.clear();
        threshMasks.clear();

        statePhase.update(3);                          // 进入 ONLINE 相位 / enter ONLINE phase
        Integer pending = pendingSize.value();
        LOG.info("[M3] Device {} result collected on the task thread (queue wait {}s, training+calibration {}s); "
                 + "{} buffered rounds to drain", device, String.format("%.1f", o.queueWaitSec),
                 String.format("%.1f", o.wallSec), pending == null ? 0 : pending);
        LOG.info("[M3] Device {} entering ONLINE", device);
    }

    /** 训练结束的日志与两条上报判据（训练线程上调用）/ the "trained" log line and the two report rules. */
    static void logTrained(String device, M3Training.Config cfg, M3Training.Result trained) {
        LOG.info("[M3] Device {} trained: hidden={}, window={}, batch={}, {} epochs "
                 + "(last improvement at epoch {}, longest plateau {}), early-stop loss={}, {}s",
                 device, cfg.hiddenSize, cfg.windowLength, cfg.batchSize, trained.epochs,
                 trained.bestEpoch, trained.longestPlateau, trained.earlyStopLoss, trained.seconds);
        // 2026-09-27 裁决书第二节：触到上限，或曾有接近耐心的平台（差不足五轮就会被停在平台中途），
        // 都须上报设计会话——耐心 20 只在 E、G 的曲线上推出过。
        // Ruling of 2026-09-27 §2: hitting the cap, or a plateau within five epochs of the patience, is reported.
        if (trained.epochs >= cfg.maxEpochs) {
            LOG.warn("[M3] Device {} REPORT: training hit the epoch cap {} without early stopping",
                     device, cfg.maxEpochs);
        }
        if (trained.longestPlateau >= cfg.patience - 5) {
            LOG.warn("[M3] Device {} REPORT: longest plateau {} epochs is within 5 of the patience {}",
                     device, trained.longestPlateau, cfg.patience);
        }
    }

    /**
     * 在阈值标定集上标定评分器（纯函数，训练线程上调用）：逐窗推理，拟合 WMSE 的 median/IQR 与协方差逆矩阵。
     * Calibrate the scorer on the threshold set (pure function, called on the training thread).
     */
    static M3Scorer calibrate(LstmAutoEncoder ae, double[][][] thData, boolean[][][] thMaskData,
                              double[] channelWeights, double zThreshold) {
        WeightedMseLoss lossCalc = new WeightedMseLoss(N_FEATURES, channelWeights);
        List<Double> wmseValues = new ArrayList<>();       // 标定集每窗口的 WMSE / per-window WMSE over the calibration set
        List<double[]> perChannelErrors = new ArrayList<>();   // 每窗口每通道 MSE / per-window per-channel MSE

        // 活跃通道 = 权重为正的通道（权重 0 的不参与 Mahalanobis）/ active = positive-weight channels (0-weight excluded)
        boolean[] activeChannels = new boolean[N_FEATURES];
        for (int c = 0; c < N_FEATURES; c++) {
            activeChannels[c] = channelWeights == null || channelWeights[c] > 0;
        }

        // 用训练好的模型在标定集上逐窗推理，收集误差分布 / run the model over the calibration set
        for (int w = 0; w < thData.length; w++) {
            double[][] recon = ae.reconstruct(thData[w]);
            boolean[][] masks = thMaskData != null && thMaskData[w] != null ? thMaskData[w] : null;
            WeightedMseLoss.LossResult lr = lossCalc.compute(thData[w], recon, masks, thData[w].length);
            wmseValues.add(lr.wmse);
            perChannelErrors.add(lr.perChannelMse);
        }

        M3Scorer scorer = new M3Scorer(zThreshold);
        scorer.calibrate(wmseValues, perChannelErrors, activeChannels);   // 拟合 median/IQR 与协方差逆 / fit
        return scorer;
    }

    /** 评分器整体 Java 序列化（写入 Flink 状态用）/ Java-serialise the scorer for Flink state. */
    static byte[] serializeScorer(M3Scorer scorer) throws IOException {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos)) {
            oos.writeObject(scorer);
        }
        return baos.toByteArray();
    }

    /**
     * 缓冲上限（轮）：标定天数加两天，即九天事件时间（0.83 版裁决第二节）。生产中它是内存上限（每台约八兆）；
     * 重放中它与训练时长一起决定倍速上限。/ Buffer cap: nine days of event time; a memory cap in production,
     * and together with training time it bounds the replay speed.
     */
    int bufferCap() {
        return (threshDays + 2) * roundsPerDay;
    }

    /**
     * 把一轮追加到缓冲尾部；缓冲满则丢弃这一轮（丢最新，保证已缓冲部分从训练段结束处连续）并计数。
     * Append a round; when full, drop it (newest) and count the overflow.
     */
    private void bufferRound(AnnotatedRound round, Context ctx, String device) throws Exception {
        String key = ctx.getCurrentKey();
        int size = pendingSize.value() == null ? 0 : pendingSize.value();
        if (size >= bufferCap()) {
            bufferOverflow.inc();
            if (bufferOverflow.getCount() == 1) {
                LOG.warn("[M3] Device {} REPORT: training buffer full at {} rounds; newest rounds are dropped",
                         device, size);
            }
            return;
        }
        Long ts = ctx.timestamp();
        long recordTs = ts != null ? ts : round.getWindowEnd() * 1000L;
        pendingRounds.add(new BufferedRound(round, recordTs));
        size++;
        pendingSize.update(size);
        if (size == 1) {
            pendingHeadTs.update(recordTs);
            holdHeadTs.put(key, recordTs);
        }
        bufferSizes.put(key, size);
    }

    /**
     * 从缓冲头部取至多 drainBatch 轮按顺序送入在线评分；评分记录带回该轮自己的记录时间戳。
     * Score up to drainBatch rounds from the head, in order; each output carries its round's own record timestamp.
     */
    private void drainBatch(Context ctx, Collector<M3ScoreRecord> out) throws Exception {
        String key = ctx.getCurrentKey();
        List<BufferedRound> all = new ArrayList<>();
        for (BufferedRound b : pendingRounds.get()) all.add(b);
        if (!drainStartMs.containsKey(key)) {
            drainStartMs.put(key, System.currentTimeMillis());
        }
        int n = Math.min(drainBatch, all.size());
        int done = 0;
        for (int i = 0; i < n; i++) {
            BufferedRound b = all.get(i);
            if (out instanceof TimestampedCollector) {
                ((TimestampedCollector<M3ScoreRecord>) out).setAbsoluteTimestamp(b.recordTs);
            }
            Integer ph = statePhase.value();
            if (ph != null && ph == 1) {
                break;                                 // 一级重训开始：其余的轮留在缓冲里 / a retrain started: keep the rest
            }
            if (ph != null && ph == 0) {
                handleCollecting(b.round, ctx, out, b.round.getDevice());   // 二级之后回到收集 / after level 2
            } else if (!handleOnline(b.round, ctx, out, b.round.getDevice())) {
                Integer now = statePhase.value();
                if (now != null && now == 1) {
                    break;                             // 本轮留在缓冲头 / this round stays at the head
                }
                handleCollecting(b.round, ctx, out, b.round.getDevice());
            }
            done = i + 1;
        }
        List<BufferedRound> rest = new ArrayList<>(all.subList(done, all.size()));
        pendingRounds.update(rest);
        pendingSize.update(rest.size());
        bufferSizes.put(key, rest.size());
        if (rest.isEmpty()) {
            pendingHeadTs.clear();
            holdHeadTs.remove(key);
            bufferSizes.remove(key);
            Long started = drainStartMs.remove(key);
            LOG.info("[M3] Device {} buffer drained in {}s", deviceName.value(),
                     String.format("%.1f", (System.currentTimeMillis() - started) / 1000.0));
        } else {
            pendingHeadTs.update(rest.get(0).recordTs);
            holdHeadTs.put(key, rest.get(0).recordTs);
        }
    }

    /** 训练态的监测状态快照：相位与缓冲占用（windowEnd 取 -1 以区别于其余快照）/ status snapshot while training. */
    private void emitStatusSnapshot(Context ctx, int phase) throws Exception {
        MonitoringSnapshot snap = new MonitoringSnapshot();
        snap.setDevice(deviceName.value());
        long wm = ctx.timerService().currentWatermark();
        snap.setTs(wm > 0 ? wm / 1000L : 0L);
        snap.setWindowEnd(STATUS_SNAPSHOT_WINDOW_END);
        snap.setM3Phase(phase);
        Integer pending = pendingSize.value();
        snap.setM3BufferRounds(pending == null ? 0 : pending);
        ctx.output(m3MonitoringTag, snap);
    }

    /** 测试用：等待某个分键的训练线程完成（不取出结果）/ test hook: wait for a key's training to finish. */
    void awaitTraining(String key, long timeoutMs) throws Exception {
        executor.awaitDone(key, timeoutMs);
    }

    /** 测试用：某个分键当前的缓冲轮数 / test hook: buffered rounds of a key. */
    int bufferedRounds(String key) {
        Integer n = bufferSizes.get(key);
        return n == null ? 0 : n;
    }

    /** 本子任务缓冲中最早一轮的记录时间戳；没有缓冲时为 Long.MAX_VALUE（供 M3Operator）。 */
    long earliestBufferedTs() {
        long min = Long.MAX_VALUE;
        for (long ts : holdHeadTs.values()) {
            min = Math.min(min, ts);
        }
        return min;
    }

    /** 从检查点恢复后由 M3Operator 逐分键回填缓冲的水位线保持与占用（任务线程）/ restore hold and size per key. */
    void restoreBuffer(String key, Integer size, Long headTs) {
        if (size != null && size > 0 && headTs != null) {
            holdHeadTs.put(key, headTs);
            bufferSizes.put(key, size);
        }
    }

    /**
     * 在线评分一轮。返回假表示本轮携带的一级或二级命令改变了相位、本轮未被消费，由调用方按新相位重新分派（一级生效前侧的
     * 重训开始后进缓冲，二级之后进收集）。/ Score one round; false when a level-1/2 command changed the phase first.
     */
    private boolean handleOnline(AnnotatedRound round, Context ctx,
                                 Collector<M3ScoreRecord> out, String device) throws Exception {
        if (round.getContextCommand() != null) {
            onContextCommand(round.getContextCommand(), ctx, device);
            round.setContextCommand(null);             // 只执行一次（本轮可能被重新分派或留在缓冲）/ execute once
            Integer ph = statePhase.value();
            if (ph == null || ph != 3) {
                return false;
            }
        }
        // 与 COLLECTING 相同的入窗逻辑（含设备 G Light 归零）；选项一下按训练版本从原始值标准化
        // Same window-fill as COLLECTING (incl. device G Light zeroing); under option 1, standardize raw values with the
        // training version.
        Integer tv = trainVersion.value();
        double[] xNorm;
        if (round.getRaw() != null && tv != null) {
            xNorm = standardize(round.getRaw(), params(tv, device), device);
            windowRawBuffer.add(round.getRaw().clone());   // 一级训练集与切换时的重新标准化用 / for level 1
            windowOutlierBuffer.add(round.isOutlier());
            if (shadowScoring) {
                // 影子：数据接入模块按当前版本给出的标准化值（与第一批的输入相同）/ shadow: the current-version values
                double[] sx = round.getXNorm().clone();
                zeroDeviceGLight(sx, device);
                shadowBuffer.add(sx);
            }
        } else {
            xNorm = round.getXNorm().clone();
            zeroDeviceGLight(xNorm, device);
        }
        windowBuffer.add(xNorm);
        boolean[] mask = WeightedMseLoss.buildMask(round.getCensoredMask());
        windowMaskBuffer.add(boolToBytes(mask));

        int bufSize = 0;
        for (double[] ignored : windowBuffer.get()) bufSize++;
        if (bufSize < windowLength) return true;       // 未满窗则等待 / wait until the window is full

        List<double[]> xNorms = new ArrayList<>();
        for (double[] x : windowBuffer.get()) xNorms.add(x);
        List<byte[]> maskBytes = new ArrayList<>();
        for (byte[] m : windowMaskBuffer.get()) maskBytes.add(m);

        windowBuffer.clear();                          // 立即清空以攒下一窗 / clear immediately for the next window
        windowMaskBuffer.clear();
        List<double[]> shadowRows = new ArrayList<>();
        for (double[] x : shadowBuffer.get()) shadowRows.add(x);
        shadowBuffer.clear();
        appendHistory(round.getTs(), maskBytes, device);

        // 从状态取回模型与评分器；缺任一则跳过（理论上进入 ONLINE 后必有）/ fetch model & scorer; skip if either is missing
        byte[] mBytes = modelBytes.value();
        byte[] sBytes = scorerBytes.value();
        if (mBytes == null || sBytes == null) return true;

        // 模型是按某一组 (隐藏层宽度, 窗口长度) 训练出来的。若作业参数此后被改动，用旧模型继续打分
        // 会得到无意义的分数，而且不会有任何外在症状。补遗三 §3 要求这种情况不得静默发生，故在此
        // 直接抛异常中止，而不是凑合打分——错配的分数比作业失败更难发现。
        // Fail loudly rather than score with a model trained under different parameters.
        Integer hs = selectedHidden.value();
        Integer tw = trainedWindow.value();
        if (hs == null || tw == null) {
            throw new IllegalStateException("设备 " + device + " 已进入 ONLINE，但状态里缺少训练时的"
                    + "隐藏层宽度或窗口长度，无法确认模型与当前参数匹配。");
        }
        if (hs != hiddenSize || tw != windowLength) {
            throw new IllegalStateException("设备 " + device + " 的模型是按 hidden=" + hs
                    + "、window=" + tw + " 训练的，当前作业参数为 hidden=" + hiddenSize
                    + "、window=" + windowLength + "，两者不符，拒绝用错配的模型打分。"
                    + "若确需更换参数，请清除该作业的状态后重新冷启动。");
        }

        LstmAutoEncoder ae = new LstmAutoEncoder(
                N_FEATURES, hs, tw, reverseTarget, learningRate, gradClip);
        ae.deserializeModel(mBytes);                   // 按训练时的宽度复原模型 / restore at the trained width

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

        long t0 = System.nanoTime();
        double[][] recon = ae.reconstruct(window);     // 推理得重建 / reconstruct
        inferenceLatencyMs.update((System.nanoTime() - t0) / 1_000_000L);   // 前向传播两侧计时 / forward pass only
        WeightedMseLoss lossCalc = new WeightedMseLoss(N_FEATURES, channelWeights);
        WeightedMseLoss.LossResult lr = lossCalc.compute(window, recon, masks, windowLength);   // 计算误差 / compute errors
        M3Scorer.ScoreResult sr = scorer.score(lr.wmse, lr.perChannelMse);   // 评分 / score

        // 发出评分记录到 synergia-scores / emit the score record to synergia-scores
        M3ScoreRecord rec = new M3ScoreRecord(device, round.getWindowEnd(), sr.mainScore,
                sr.mahaScore, lr.wmse, lr.perChannelMse, sr.aboveThreshold, hs, windowLength);
        // 选项一：输出版本号为训练版本；回路关闭时仍为轮上的版本（空）/ option 1: the training version; null when the loop is off
        rec.setVersion(tv != null ? tv : round.getVersion());
        rec.setModel(modelGen.value());                // 一级或二级之后的模型代数 / model generation after level 1/2
        out.collect(rec);

        onlineCount.inc();

        // 如启用监测侧输出，附带重建误差写入 synergia-monitoring / if enabled, also emit recon errors to synergia-monitoring
        if (m3MonitoringTag != null) {
            MonitoringSnapshot snap = new MonitoringSnapshot();
            snap.setDevice(device);
            snap.setTs(round.getWindowEnd());
            snap.setWindowEnd(round.getWindowEnd());
            snap.setM3ReconError(lr.wmse);
            snap.setM3PerChannelErrors(lr.perChannelMse);
            snap.setM3Phase(3);
            Integer buffered = bufferSizes.get(ctx.getCurrentKey());
            snap.setM3BufferRounds(buffered == null ? 0 : buffered);   // 排空期间为本批开始前的占用 / pre-batch size
            if (shadowScoring && shadowRows.size() == windowLength) {
                // 影子评分：同一模型与评分器，按当前版本标准化的窗口；只记录，不发评分记录 / record only
                double[][] sw = shadowRows.toArray(new double[0][]);
                WeightedMseLoss.LossResult sl = lossCalc.compute(sw, ae.reconstruct(sw), masks, windowLength);
                snap.setM3Above(sr.aboveThreshold);
                snap.setM3ShadowAbove(scorer.score(sl.wmse, sl.perChannelMse).aboveThreshold);
                snap.setM3ShadowWmse(sl.wmse);
            }
            ctx.output(m3MonitoringTag, snap);
        }
        // 一级生效后侧：训练集收满之后提交（本窗已由旧模型评分）/ after-side level 1: submit once the span is covered
        long[] span = retrainSpan.value();
        if (span != null && round.getTs() >= span[1]) {
            submitRetrain(ctx, device);
        }
        return true;
    }

    /** 本轮在新相位下重新分派 / re-dispatch a round after a phase change. */
    private void redispatch(AnnotatedRound round, Context ctx, Collector<M3ScoreRecord> out, String device)
            throws Exception {
        Integer ph = statePhase.value();
        if (ph != null && ph == 1) {
            bufferRound(round, ctx, device);
        } else {
            handleCollecting(round, ctx, out, device);
        }
    }

    /**
     * 一级或二级命令（M6b 决定十七）。一级：只在在线态接受；生效前侧立即用历史中生效前 N 天的窗口提交重训，生效后侧记下范围，
     * 收满 N 天后提交。二级：回到收集态，按首次训练的数据段从头训练，期间上下文输出与性能层静默。
     * Level 1 (online only): before-side submits at once from history, after-side waits N days. Level 2: back to COLLECTING.
     */
    private void onContextCommand(com.leejean.m6.AdaptCommand cmd, Context ctx, String device) throws Exception {
        int ph = statePhase.value() == null ? 0 : statePhase.value();
        if (cmd.level == 2) {
            if (ph == 1) {
                LOG.warn("[M3] Device {} REPORT: level-2 command seq {} ignored while a training is in flight", device, cmd.seq);
                return;
            }
            statePhase.update(0);
            collectCount.update(0L);
            windowBuffer.clear();
            windowMaskBuffer.clear();
            windowOutlierBuffer.clear();
            windowRawBuffer.clear();
            shadowBuffer.clear();
            trainWindows.clear();
            trainMasks.clear();
            earlyStopWindows.clear();
            earlyStopMasks.clear();
            threshWindows.clear();
            threshMasks.clear();
            trainExcluded.clear();
            modelBytes.clear();
            scorerBytes.clear();
            trainVersion.clear();
            rawCollected.clear();
            trainFailures.clear();
            history.clear();
            endRetrain();
            level2.update(true);
            LOG.info("[M3] Device {} level 2 (seq {}, effective {}): back to COLLECTING; context output and the "
                     + "performance layer are silent until the new model is online", device, cmd.seq, cmd.effectiveAt);
            return;
        }
        if (ph != 3 || retrainSpan.value() != null) {
            LOG.warn("[M3] Device {} REPORT: level-1 command seq {} ignored (phase {}, retrain pending {})", device, cmd.seq,
                     ph, retrainSpan.value() != null);
            return;
        }
        long n = retrainDays * daySec();
        long from = retrainAfter ? cmd.effectiveAt : cmd.effectiveAt - n;
        long to = retrainAfter ? cmd.effectiveAt + n : cmd.effectiveAt;
        retrainSpan.update(new long[]{from, to});
        LOG.info("[M3] Device {} level 1 (seq {}, effective {}): training set [{}, {}) ({} days {} the effective time, {} start)",
                 device, cmd.seq, cmd.effectiveAt, from, to, retrainDays, retrainAfter ? "after" : "before",
                 retrainWarm ? "warm" : "cold");
        if (!retrainAfter) {
            submitRetrain(ctx, device);
        }
    }

    /**
     * 提交一级重训：取历史中末轮落在范围内的窗口，按当前版本标准化，七比二比七划成训练、早停、标定三段，训练段剔除含离群轮
     * 的窗口；写入三段状态后走与冷启动相同的训练态（缓冲、保持水位线、结果在任务线程收回），因此两次运行逐位相同。
     * Submit a level-1 retrain through the same TRAINING path as the cold start, so runs are bit-reproducible.
     */
    private void submitRetrain(Context ctx, String device) throws Exception {
        long[] span = retrainSpan.value();
        List<HistWindow> ws = new ArrayList<>();
        for (HistWindow h : history.get()) {
            if (h.endTs >= span[0] && h.endTs < span[1]) {
                ws.add(h);
            }
        }
        int w = ws.size();
        int tr = w * 7 / 16;
        int es = w * 2 / 16;
        if (tr == 0 || es == 0 || w - tr - es == 0) {
            LOG.warn("[M3] Device {} REPORT: level-1 retrain abandoned, only {} history windows in [{}, {})",
                     device, w, span[0], span[1]);
            endRetrain();
            return;
        }
        Integer v = curVersion.value();
        com.leejean.m6.ParamSet ps = params(v, device);
        List<double[]> trW = new ArrayList<>();
        List<byte[]> trM = new ArrayList<>();
        List<double[]> esW = new ArrayList<>();
        List<byte[]> esM = new ArrayList<>();
        List<double[]> thW = new ArrayList<>();
        List<byte[]> thM = new ArrayList<>();
        long excluded = 0;
        List<double[]> one = new ArrayList<>(1);
        for (int k = 0; k < w; k++) {
            HistWindow h = ws.get(k);
            one.clear();
            one.add(h.raw);
            double[] x = standardize(one, ps, device).get(0);
            if (k < tr) {
                if (h.outlier) {
                    excluded++;
                } else {
                    trW.add(x);
                    trM.add(h.mask);
                }
            } else if (k < tr + es) {
                esW.add(x);
                esM.add(h.mask);
            } else {
                thW.add(x);
                thM.add(h.mask);
            }
        }
        trainWindows.update(trW);
        trainMasks.update(trM);
        earlyStopWindows.update(esW);
        earlyStopMasks.update(esM);
        threshWindows.update(thW);
        threshMasks.update(thM);
        trainExcluded.update(excluded);
        retrainVersion.update(v);
        retraining.update(true);
        trainFailures.clear();
        LOG.info("[M3] Device {} level-1 retrain: {} history windows standardized with version {}", device, w, v);
        submitTraining(ctx.getCurrentKey(), device, ctx);
        ctx.timerService().registerProcessingTimeTimer(
                ctx.timerService().currentProcessingTime() + pollIntervalSec * 1000L);
    }

    /**
     * 一天的事件时间秒数：每天轮数乘十秒一轮。生产上为 86,400；单元测试调小每天轮数时随之缩短，与训练段按轮数折天的口径一致。
     * Seconds per day: rounds per day times the 10 s round period (86,400 in production).
     */
    private long daySec() {
        return roundsPerDay * 10L;
    }

    /** 一级重训的状态复位 / reset the level-1 retrain state. */
    private void endRetrain() throws Exception {
        retrainSpan.clear();
        retrainVersion.clear();
        retraining.clear();
    }

    /**
     * 在线窗口的原始值入历史，并淘汰早于「最近窗口减 N 加一天」的窗口（待做的一级范围内的不淘汰）；超出两天的量时才重写，
     * 使重写的代价摊薄。/ Append the window's raw values to history and evict old windows (amortized).
     */
    private void appendHistory(long endTs, List<byte[]> maskBytes, String device) throws Exception {
        List<double[]> raws = new ArrayList<>();
        for (double[] r : windowRawBuffer.get()) raws.add(r);
        List<Boolean> outs = new ArrayList<>();
        for (Boolean o : windowOutlierBuffer.get()) outs.add(o);
        windowRawBuffer.clear();
        windowOutlierBuffer.clear();
        if (raws.size() != windowLength) {
            return;                                    // 回路关闭（没有原始值）/ loop off: no raw values
        }
        HistWindow h = new HistWindow();
        h.endTs = endTs;
        h.raw = flattenWindow(raws);
        h.mask = flattenMasks(maskBytes);
        h.outlier = outs.contains(Boolean.TRUE);
        history.add(h);
        int perDay = Math.max(1, roundsPerDay / windowLength);
        long size = 0;
        for (HistWindow ignored : history.get()) size++;
        if (size > (long) (retrainDays + 2) * perDay) {
            long keepFrom = endTs - (retrainDays + 1) * daySec();
            long[] span = retrainSpan.value();
            if (span != null) {
                keepFrom = Math.min(keepFrom, span[0]);
            }
            List<HistWindow> kept = new ArrayList<>();
            for (HistWindow x : history.get()) {
                if (x.endTs >= keepFrom) {
                    kept.add(x);
                }
            }
            history.update(kept);
        }
    }

    /** 二级重训中的质量标记：相位为收集、重训中为真 / the level-2 quality marker. */
    private void emitRetrainingMarker(Context ctx, long ts, String device) {
        MonitoringSnapshot snap = new MonitoringSnapshot();
        snap.setDevice(device);
        snap.setTs(ts);
        snap.setWindowEnd(STATUS_SNAPSHOT_WINDOW_END);
        snap.setM3Phase(0);
        snap.setM3Retraining(true);
        ctx.output(m3MonitoringTag, snap);
    }

    /**
     * 在线窗口的原始值历史（一级训练集来源）。Flink POJO。/ A raw online window kept for level-1 retraining.
     */
    public static class HistWindow implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        /** 窗口末轮时间戳（秒）/ timestamp of the window's last round (s). */
        public long endTs;
        /** 原始值，扁平 [时间步 × 通道] / raw values, flat. */
        public double[] raw;
        /** 掩码，扁平 / masks, flat. */
        public byte[] mask;
        /** 窗口内是否有点通道离群轮 / whether the window holds a point-channel outlier round. */
        public boolean outlier;

        public HistWindow() {
        }
    }

    /** 在数据集上求平均 WMSE（早停选型用，掩码不参与）/ mean WMSE over a dataset (for model selection; no mask). */
    /**
     * 早停集误差，委派给训练核心 {@link M3Training#evaluateLoss}。
     * 保留这两个包级可见的入口是为了不改动 {@link M3Grid} 与既有测试的调用点；实现只有一份。
     * Delegates to the training core; a single implementation, two call-compatible entry points.
     */
    static double evaluateLoss(LstmAutoEncoder ae, double[][][] data, double[] channelWeights) {
        return M3Training.evaluateLoss(ae, data, channelWeights);
    }

    /**
     * 一级重训的参数（决定十七）：训练长度（天）、热启动与否、训练窗口在生效之后（真）还是之前（假）。默认七天、热启动、
     * 生效之前，开放问题第十二条补跑裁决之后填入选定值。/ Level-1 parameters: days, warm start, window after the effective time.
     */
    public M3Function withRetrain(int days, boolean warm, boolean after) {
        this.retrainDays = days;
        this.retrainWarm = warm;
        this.retrainAfter = after;
        return this;
    }

    /** 打开影子评分（决定十三；只在全量重放打开，须同时打开监测侧输出）/ turn shadow scoring on (full replay only). */
    public M3Function withShadowScoring(boolean on) {
        this.shadowScoring = on;
        return this;
    }

    /** 训练版本的参数集；缺失时直接失败而不是凑合评分 / the parameter set of a version; fail loudly when absent. */
    private com.leejean.m6.ParamSet params(Integer v, String device) throws Exception {
        com.leejean.m6.ParamSet ps = v == null ? null : paramSets.get(v);
        if (ps == null) {
            throw new IllegalStateException("设备 " + device + " 缺少版本 " + v + " 的参数集，无法按训练版本标准化 / "
                    + "missing the parameter set of version " + v);
        }
        return ps;
    }

    /**
     * 按参数集标准化一轮原始值（与数据接入模块同一个函数 {@link RobustScalerFunction#normalize}；入口轮无缺失），再做设备 G 的
     * 光照归零。/ Standardize one admitted raw round with a parameter set, then zero device G's Light.
     */
    static double[] standardize(double[] raw, com.leejean.m6.ParamSet ps, String device) {
        double[] x = RobustScalerFunction.normalize(raw, NO_MISSING, ps.median, ps.scale, ps.bypass, TRANSFORMS);
        zeroDeviceGLight(x, device);
        return x;
    }

    /** 逐窗逐步标准化扁平的原始窗口 / standardize flat raw windows step by step. */
    private List<double[]> standardize(List<double[]> flats, com.leejean.m6.ParamSet ps, String device) {
        List<double[]> out = new ArrayList<>(flats.size());
        double[] step = new double[N_FEATURES];
        for (double[] f : flats) {
            double[] g = new double[f.length];
            for (int t = 0; t < windowLength; t++) {
                System.arraycopy(f, t * N_FEATURES, step, 0, N_FEATURES);
                System.arraycopy(standardize(step, ps, device), 0, g, t * N_FEATURES, N_FEATURES);
            }
            out.add(g);
        }
        return out;
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
    /**
     * 包级可见（非 private）：离线网格程序 {@link M3Grid} 需要用**完全相同**的口径预处理输入，
     * 否则网格选出的超参数与在线算子跑的不是同一件事。共用同一份实现，避免两处实现随时间漂移。
     * Package-private so {@link M3Grid} preprocesses input with exactly the same caliber; sharing one
     * implementation keeps the offline grid and the online operator from drifting apart.
     */
    static void zeroDeviceGLight(double[] xNorm, String device) {
        if (DEVICE_G.equals(device) && xNorm.length > Channels.LIGHT_INDEX) {
            xNorm[Channels.LIGHT_INDEX] = 0.0;
        }
    }
}
