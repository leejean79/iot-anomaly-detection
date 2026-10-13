package com.leejean.m3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 训练执行器（补遗四第二节）：把冷启动训练从 Flink 任务线程移到专用的训练线程上执行。
 * Training executor (addendum 4, section 2): runs cold-start training on dedicated training threads
 * instead of the Flink task thread.
 *
 * <p>线程池在同一个任务管理器进程（同一个作业的用户类加载器）内共享，线程数由 {@code --train-threads} 给出，
 * 超出的训练任务在队列中按提交顺序等待。每个算子实例只持有自己提交的任务的结果槽（按分键索引的
 * {@link Future}）；结果的收回与 Flink 状态的读写只在任务线程上进行，训练线程不触碰任何 Flink 状态。
 * The pool is shared within one TaskManager process (one job's user class loader); excess tasks wait in
 * submission order. Each operator instance only holds the result slots of its own tasks; collecting a result
 * and touching Flink state happen on the task thread only.
 *
 * <p>训练线程上做两件事：调用既有的纯函数 {@link M3Training#train}，然后在阈值标定集上标定评分器
 * （{@link M3Function#calibrate}）。标定也放在训练线程上，原因是标定集在训练开始之前就已收齐（2026-10-02
 * 裁决起，收集态包含七天标定集），而在任务线程上标定一千多个窗口会把任务线程占住一到三分钟。
 * The training thread trains via the existing pure function and then calibrates the scorer on the threshold
 * set, which is already collected before training starts; calibrating ~1,000 windows on the task thread would
 * block it for one to three minutes.
 *
 * <p>BLAS 线程数：ND4J 的 OpenMP 线程数是进程级设置，由任务管理器容器的环境变量 {@code OMP_NUM_THREADS=1}
 * 决定（2026-09-30 用户确认，`syn-reset-env.sh` 核对），训练线程沿用它；本类在第一次建池时把读到的值记入日志。
 * BLAS threads are process-wide, set by the container's OMP_NUM_THREADS=1; the value is logged once here.
 */
public final class M3TrainExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(M3TrainExecutor.class);

    /** 训练目的：冷启动或（留给适应执行模块的）重训 / purpose: cold start, or a retrain for the adaptation module. */
    public enum Purpose { COLD_START, RETRAIN }

    /**
     * 净化选项（补遗三约定其强度定义留给适应执行模块）。本补遗只在接口上为它留位置，目前没有字段。
     * Sanitisation options; their definition is left to the adaptation module, this only reserves the slot.
     */
    public static final class SanitizeOptions implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    /**
     * 一个训练任务：数据是从 Flink 状态复制出来的不可变快照，训练线程不持有任何 Flink 状态对象的引用。
     * A training task; the data is an immutable copy of Flink state, so the training thread holds no state objects.
     */
    public static final class TrainTask {
        public final String key;                 // 分键（代理键）/ the keyBy key (surrogate key)
        public final String device;              // 设备号，用于日志 / device id, for logs
        public final long taskId;
        public final Purpose purpose;
        public final M3Training.Config config;
        public final long seed;                  // 模型的确定性种子（记录用）/ the model's fixed seed (recorded)
        public final double zThreshold;
        public final SanitizeOptions sanitize;
        final double[][][] trainData;
        final boolean[][][] trainMask;
        final double[][][] earlyStopData;
        final double[][][] threshData;
        final boolean[][][] threshMask;
        final long submitNanos = System.nanoTime();

        public TrainTask(String key, String device, long taskId, Purpose purpose, M3Training.Config config,
                         long seed, double zThreshold, SanitizeOptions sanitize,
                         double[][][] trainData, boolean[][][] trainMask, double[][][] earlyStopData,
                         double[][][] threshData, boolean[][][] threshMask) {
            this.key = key;
            this.device = device;
            this.taskId = taskId;
            this.purpose = purpose;
            this.config = config;
            this.seed = seed;
            this.zThreshold = zThreshold;
            this.sanitize = sanitize;
            this.trainData = trainData;
            this.trainMask = trainMask;
            this.earlyStopData = earlyStopData;
            this.threshData = threshData;
            this.threshMask = threshMask;
        }
    }

    /** 训练结果（成功或失败）/ the outcome of one training task (success or failure). */
    public static final class Outcome {
        public final long taskId;
        public final Throwable error;            // 非空表示失败 / non-null means failure
        public final byte[] modelBytes;
        public final byte[] scorerBytes;
        public final M3Training.Result result;
        public final M3Scorer scorer;
        public final double queueWaitSec;        // 提交到开始训练的等待 / wait from submission to start
        public final double wallSec;             // 训练加标定的墙钟时长 / wall time of training plus calibration

        Outcome(long taskId, Throwable error, byte[] modelBytes, byte[] scorerBytes, M3Training.Result result,
                M3Scorer scorer, double queueWaitSec, double wallSec) {
            this.taskId = taskId;
            this.error = error;
            this.modelBytes = modelBytes;
            this.scorerBytes = scorerBytes;
            this.result = result;
            this.scorer = scorer;
            this.queueWaitSec = queueWaitSec;
            this.wallSec = wallSec;
        }
    }

    /** 查询结果槽时的三种非结果状态 / the non-result states of a slot. */
    public static final Outcome NOT_SUBMITTED = new Outcome(-1, null, null, null, null, null, 0, 0);
    public static final Outcome PENDING = new Outcome(-1, null, null, null, null, null, 0, 0);

    // ---- 进程内共享的线程池（按引用计数建立与关闭）/ process-wide shared pool, reference counted ----
    private static ThreadPoolExecutor sharedPool;
    private static int sharedRefs;

    private static synchronized ExecutorService acquire(int threads) {
        if (sharedPool == null) {
            AtomicInteger n = new AtomicInteger();
            sharedPool = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(), r -> {
                        Thread t = new Thread(r, "m3-train-" + n.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    });
            LOG.info("[M3] Training executor started: {} thread(s) in this TaskManager, OMP_NUM_THREADS={}",
                    threads, System.getenv("OMP_NUM_THREADS"));
        } else if (sharedPool.getCorePoolSize() != threads) {
            LOG.warn("[M3] Training executor already running with {} thread(s); requested {} ignored",
                    sharedPool.getCorePoolSize(), threads);
        }
        sharedRefs++;
        return sharedPool;
    }

    private static synchronized void release() {
        if (--sharedRefs == 0 && sharedPool != null) {
            sharedPool.shutdownNow();
            sharedPool = null;
            LOG.info("[M3] Training executor shut down");
        }
    }

    /**
     * 仅供测试：非空时，训练线程在开始训练之前等待它打开，使测试能确定地让一次训练停在「进行中」。
     * Test-only gate: when set, a training thread waits for it before training starts.
     */
    static volatile java.util.concurrent.CountDownLatch testGate;

    private final ExecutorService pool;
    private final Map<String, Future<Outcome>> slots = new ConcurrentHashMap<>();
    private boolean closed;

    public M3TrainExecutor(int threads) {
        if (threads < 1) {
            throw new IllegalArgumentException("--train-threads 必须至少为 1: " + threads);
        }
        this.pool = acquire(threads);
    }

    /** 提交一个训练任务（任务线程调用）/ submit a task (called on the task thread). */
    public void submit(TrainTask task, M3Training.EpochListener listener) {
        slots.put(task.key, pool.submit(() -> run(task, listener)));
    }

    /**
     * 查询结果槽（任务线程调用）：未提交返回 {@link #NOT_SUBMITTED}，未完成返回 {@link #PENDING}，
     * 完成则取出结果并清空该槽。
     * Poll a slot: NOT_SUBMITTED, PENDING, or the outcome (which empties the slot).
     */
    public Outcome poll(String key) {
        Future<Outcome> f = slots.get(key);
        if (f == null) {
            return NOT_SUBMITTED;
        }
        if (!f.isDone()) {
            return PENDING;
        }
        slots.remove(key);
        try {
            return f.get();
        } catch (CancellationException | ExecutionException | InterruptedException e) {
            return new Outcome(-1, e, null, null, null, null, 0, 0);
        }
    }

    /** 测试用：等待某个槽的训练完成（不取出结果）/ test hook: wait until a slot's training is done. */
    void awaitDone(String key, long timeoutMs) throws Exception {
        Future<Outcome> f = slots.get(key);
        if (f != null) {
            try {
                f.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (ExecutionException | CancellationException ignored) {
                // 失败也算完成 / a failure also counts as done
            }
        }
    }

    /** 关闭：中断本实例提交的全部训练，并释放共享线程池 / close: interrupt this instance's tasks, release the pool. */
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (Future<Outcome> f : slots.values()) {
            f.cancel(true);
        }
        slots.clear();
        release();
    }

    private static Outcome run(TrainTask t, M3Training.EpochListener listener) {
        java.util.concurrent.CountDownLatch gate = testGate;
        if (gate != null) {
            try {
                gate.await();
            } catch (InterruptedException e) {
                return new Outcome(t.taskId, e, null, null, null, null, 0, 0);
            }
        }
        long start = System.nanoTime();
        double waitSec = (start - t.submitNanos) / 1e9;
        LOG.info("[M3] Device {} training started on {} after {}s in the executor queue (task {}, {})",
                t.device, Thread.currentThread().getName(), String.format("%.1f", waitSec), t.taskId, t.purpose);
        try {
            M3Training.Result r = M3Training.train(t.config, t.trainData, t.trainMask, t.earlyStopData, listener);
            M3Function.logTrained(t.device, t.config, r);
            M3Scorer scorer = M3Function.calibrate(r.model, t.threshData, t.threshMask,
                    t.config.channelWeights, t.zThreshold);
            LOG.info("[M3] Device {} calibrated: median={}, IQR={}, threshold={}",
                    t.device, scorer.getMedian(), scorer.getIqr(), scorer.getThreshold());
            byte[] model = r.model.serializeModel();
            byte[] scorerBytes = M3Function.serializeScorer(scorer);
            return new Outcome(t.taskId, null, model, scorerBytes, r, scorer, waitSec,
                    (System.nanoTime() - start) / 1e9);
        } catch (InterruptedException e) {
            LOG.warn("[M3] Device {} training interrupted: {}", t.device, e.getMessage());
            return new Outcome(t.taskId, e, null, null, null, null, waitSec, (System.nanoTime() - start) / 1e9);
        } catch (Throwable e) {
            LOG.error("[M3] Device {} training failed (task {})", t.device, t.taskId, e);
            return new Outcome(t.taskId, e, null, null, null, null, waitSec, (System.nanoTime() - start) / 1e9);
        }
    }
}
