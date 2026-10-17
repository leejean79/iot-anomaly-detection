package com.leejean.m3;

import com.leejean.m1.Channels;
import com.leejean.m1.MonitoringSnapshot;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
                handleCollecting(round, ctx, out, count, device);
                break;
            case 1:
                bufferRound(round, ctx, device);
                pollTraining(ctx);                     // 结果就绪则转为在线；排空由定时器链开始 / drain starts on the timer
                break;
            case 3:
                Integer pending = pendingSize.value();
                if (pending != null && pending > 0) {
                    bufferRound(round, ctx, device);
                } else {
                    handleOnline(round, ctx, out, device);
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
            if (modelBytes.value() == null && (failures == null || failures < MAX_FAILURES)) {
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
            } else {
                LOG.warn("[M3] Device {} REPORT: training failed {} times; the device stays in TRAINING",
                         device, failures);
            }
            return;
        }
        applyOutcome(key, device, o);
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
        M3TrainExecutor.TrainTask task = new M3TrainExecutor.TrainTask(
                key, device, taskId, M3TrainExecutor.Purpose.COLD_START, cfg, LstmAutoEncoder.SEED, zThreshold,
                new M3TrainExecutor.SanitizeOptions(),
                unflattenWindows(trainFlats), unflattenMasks(trainMaskFlats), unflattenWindows(esFlats),
                unflattenWindows(thFlats), unflattenMasks(thMaskFlats));
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
        for (int i = 0; i < n; i++) {
            BufferedRound b = all.get(i);
            if (out instanceof TimestampedCollector) {
                ((TimestampedCollector<M3ScoreRecord>) out).setAbsoluteTimestamp(b.recordTs);
            }
            handleOnline(b.round, ctx, out, b.round.getDevice());
        }
        List<BufferedRound> rest = new ArrayList<>(all.subList(n, all.size()));
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
            snap.setM3Phase(3);
            Integer buffered = bufferSizes.get(ctx.getCurrentKey());
            snap.setM3BufferRounds(buffered == null ? 0 : buffered);   // 排空期间为本批开始前的占用 / pre-batch size
            ctx.output(m3MonitoringTag, snap);
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
