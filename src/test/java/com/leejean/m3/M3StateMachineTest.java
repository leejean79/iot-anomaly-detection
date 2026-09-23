package com.leejean.m3;

import com.leejean.m1.Channels;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * V-M3-2：状态机跃迁、早停、设备隔离、模型权重经 checkpoint 恢复后仍然有效。
 *
 * <p>本测试使用 Flink 提供的<b>算子测试夹具</b>（operator test harness，具体是
 * {@link KeyedOneInputStreamOperatorTestHarness} 这个类）直接驱动 {@link M3Function}，而不是启动
 * MiniCluster 运行整个作业。所谓测试夹具，是指可以在不启动完整作业的前提下单独实例化一个算子、
 * 逐条把记录喂给它、并对它的状态做快照与从快照恢复的工具类。
 *
 * <p>选用它而不是 MiniCluster 有两个原因。第一，只有在单个算子这一层才能做快照与恢复，而这正是
 * 「模型权重经 checkpoint 恢复后仍然有效」这一条验收要求的必要条件，MiniCluster 只能运行完整作业，
 * 做不到这一点。第二，测试夹具允许精确控制喂进去的轮数与顺序，从而可以逐条断言相位跃迁。
 *
 * <p>为了让验证一次相位跃迁不必喂入 8,640 条轮，测试通过<b>包级私有的构造函数</b>
 * （package-private constructor，即不带 public 修饰符、只对同一个 Java 包内的类可见的构造函数）
 * 把「每天折合多少轮」这个换算值调小。该值只决定「多少条轮算作一天」，不改变相位跃迁的判据，
 * 也不改变训练与标定的任何逻辑。
 *
 * <p>Uses Flink's operator test harness rather than a MiniCluster: only the harness can snapshot and
 * restore an operator, which the "weights survive a checkpoint" item requires, and it gives exact
 * control over how many rounds are fed. The package-private constructor shrinks rounds-per-day so a
 * transition does not need 8,640 rounds; that only rescales what counts as a day.
 */
class M3StateMachineTest {

    /** 每天折合的轮数（测试值）。取 40 使一次相位跃迁只需几十条轮。 */
    private static final int ROUNDS_PER_DAY = 40;
    private static final int TRAIN_DAYS = 2;        // 训练集 80 轮
    private static final int ES_DAYS = 1;           // 早停集 40 轮
    private static final int THRESH_DAYS = 1;       // 标定集 40 轮
    private static final int WINDOW_LENGTH = 8;     // 窗口长度（轮）
    private static final int MAX_EPOCHS = 30;
    private static final int PATIENCE = 3;
    /** 隐藏层宽度：全机队统一参数，不再由算子自行搜索（补遗三 §3）。 */
    private static final int HIDDEN_SIZE = 12;
    /** 小批量大小：取 1，与生产默认值及 2026-09-21 的参照口径一致。 */
    private static final int BATCH_SIZE = 1;
    private static final double Z_THRESHOLD = 3.0;
    /** 训练 + 早停 + 标定三段合计的轮数；喂满之后算子应处于 ONLINE 相位。 */
    private static final int ROUNDS_TO_ONLINE = (TRAIN_DAYS + ES_DAYS + THRESH_DAYS) * ROUNDS_PER_DAY;

    private static M3Function newFunction() {
        double[] weights = new double[Channels.N_DET];
        java.util.Arrays.fill(weights, 1.0);
        return new M3Function(TRAIN_DAYS, ES_DAYS, THRESH_DAYS, WINDOW_LENGTH, Z_THRESHOLD,
                weights, MAX_EPOCHS, PATIENCE, HIDDEN_SIZE, BATCH_SIZE, true, 0.001, 1.0, null, ROUNDS_PER_DAY);
    }

    private static OneInputStreamOperatorTestHarness<AnnotatedRound, M3ScoreRecord> newHarness()
            throws Exception {
        KeyedProcessOperator<String, AnnotatedRound, M3ScoreRecord> op =
                new KeyedProcessOperator<>(newFunction());
        KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        op, AnnotatedRound::getDevice, TypeInformation.of(String.class));
        h.open();
        return h;
    }

    /** 造一条正常轮：五通道取值围绕设备相关的常量做小幅正弦波动，无删失、无离群。 */
    private static AnnotatedRound round(String device, long ts, Random rnd) {
        double[] x = new double[Channels.N_DET];
        double phase = device.hashCode() % 7;
        for (int c = 0; c < x.length; c++) {
            x[c] = Math.sin((ts / 10.0 + phase + c) * 0.3) * 0.2 + rnd.nextGaussian() * 0.01;
        }
        return new AnnotatedRound(device, ts, x, false, new boolean[Channels.N_DET], false, ts);
    }

    private static void feed(OneInputStreamOperatorTestHarness<AnnotatedRound, M3ScoreRecord> h,
                            String device, int n, long startTs, Random rnd) throws Exception {
        for (int i = 0; i < n; i++) {
            long ts = startTs + i * 10L;
            h.processElement(round(device, ts, rnd), ts * 1000L);
        }
    }

    private static List<M3ScoreRecord> drain(
            OneInputStreamOperatorTestHarness<AnnotatedRound, M3ScoreRecord> h) {
        List<M3ScoreRecord> out = new ArrayList<>();
        h.getOutput().forEach(o -> {
            Object v = ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) o).getValue();
            if (v instanceof M3ScoreRecord) {
                out.add((M3ScoreRecord) v);
            }
        });
        return out;
    }

    @Test
    @DisplayName("相位跃迁：COLLECTING 期间不打分，喂满训练+早停+标定后进入 ONLINE 并开始打分")
    void everyPhaseTransitionHappensInOrder() throws Exception {
        Random rnd = new Random(42);
        try (OneInputStreamOperatorTestHarness<AnnotatedRound, M3ScoreRecord> h = newHarness()) {
            // 相位 0（COLLECTING）：喂到差一轮就满，期间不应有任何评分输出。
            feed(h, "E", ROUNDS_TO_ONLINE - 1, 1_000_000L, rnd);
            assertTrue(drain(h).isEmpty(),
                    "COLLECTING/TRAINING/CALIBRATING 三个相位都不应产出评分记录");

            // 再喂一轮触发 0→1（TRAINING）→2（CALIBRATING）→3（ONLINE）这条同步跃迁链。
            feed(h, "E", 1, 1_000_000L + (ROUNDS_TO_ONLINE - 1) * 10L, rnd);

            // 相位 3（ONLINE）：此后每一轮都应产出评分记录。
            int before = drain(h).size();
            feed(h, "E", WINDOW_LENGTH + 5, 2_000_000L, rnd);
            int after = drain(h).size();
            assertTrue(after > before,
                    "进入 ONLINE 之后应开始产出评分记录，实测 before=" + before + " after=" + after);
        }
    }

    @Test
    @DisplayName("设备隔离：一台设备喂满并进入 ONLINE，不会让另一台设备提前开始打分")
    void devicesAdvanceIndependently() throws Exception {
        Random rnd = new Random(7);
        try (OneInputStreamOperatorTestHarness<AnnotatedRound, M3ScoreRecord> h = newHarness()) {
            // E 喂满并越过跃迁点；G 只喂少量轮。
            feed(h, "E", ROUNDS_TO_ONLINE + WINDOW_LENGTH + 5, 1_000_000L, rnd);
            feed(h, "G", ROUNDS_PER_DAY, 1_000_000L, rnd);

            List<M3ScoreRecord> recs = drain(h);
            assertFalse(recs.isEmpty(), "E 应已进入 ONLINE 并产出评分");
            long gScores = recs.stream().filter(r -> "G".equals(r.getDevice())).count();
            assertEquals(0, gScores,
                    "G 的轮数远未喂满，不应产出任何评分——设备之间的状态必须彼此独立");
        }
    }

    @Test
    @DisplayName("早停：耐心用尽即停，训练不会跑满 maxEpochs")
    void earlyStoppingEndsTrainingBeforeMaxEpochs() throws Exception {
        Random rnd = new Random(11);
        // 用极大的 maxEpochs 与极小的耐心：若早停失效，本用例会明显变慢甚至超时。
        double[] weights = new double[Channels.N_DET];
        java.util.Arrays.fill(weights, 1.0);
        final int maxEpochs = 2000;
        M3Function fn = new M3Function(TRAIN_DAYS, ES_DAYS, THRESH_DAYS, WINDOW_LENGTH, Z_THRESHOLD,
                weights, maxEpochs, 1, HIDDEN_SIZE, BATCH_SIZE, true, 0.001, 1.0, null, ROUNDS_PER_DAY);
        KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(fn), AnnotatedRound::getDevice,
                        TypeInformation.of(String.class));
        h.open();
        try {
            feed(h, "E", ROUNDS_TO_ONLINE + WINDOW_LENGTH + 2, 1_000_000L, rnd);
            assertFalse(drain(h).isEmpty(), "应已进入 ONLINE");
            // 精确断言：实际跑完的 epoch 数必须小于 maxEpochs，否则早停没有生效。
            // 用 epoch 数而不是墙钟耗时，避免在慢机器上假失败。
            assertTrue(fn.lastTrainEpochs > 0, "应当确实训练过，实测 epoch 数 " + fn.lastTrainEpochs);
            assertTrue(fn.lastTrainEpochs < maxEpochs,
                    "patience=1 时训练应被早停截断在 maxEpochs=" + maxEpochs + " 之前，实测跑了 "
                            + fn.lastTrainEpochs + " 个 epoch");
        } finally {
            h.close();
        }
    }

    @Test
    @DisplayName("checkpoint 恢复：快照后重建算子，模型权重与阈值存活，恢复即可继续打分")
    void modelWeightsSurviveCheckpointRestore() throws Exception {
        Random rnd = new Random(3);
        org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
        List<M3ScoreRecord> beforeRestore;

        try (OneInputStreamOperatorTestHarness<AnnotatedRound, M3ScoreRecord> h = newHarness()) {
            feed(h, "E", ROUNDS_TO_ONLINE + WINDOW_LENGTH + 5, 1_000_000L, rnd);
            beforeRestore = drain(h);
            assertFalse(beforeRestore.isEmpty(), "快照之前应已进入 ONLINE 并产出评分");
            snapshot = h.snapshot(1L, 1L);
        }

        // 用一个全新的算子实例恢复：若权重与阈值没有随状态存活，恢复后的实例会退回 COLLECTING、
        // 需要重新喂满几百轮才会打分；断言「恢复后立刻能打分」即可把这一点区分开。
        KeyedProcessOperator<String, AnnotatedRound, M3ScoreRecord> op2 =
                new KeyedProcessOperator<>(newFunction());
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h2 =
                     new KeyedOneInputStreamOperatorTestHarness<>(
                             op2, AnnotatedRound::getDevice, TypeInformation.of(String.class))) {
            h2.setup();
            h2.initializeState(snapshot);
            h2.open();

            feed(h2, "E", WINDOW_LENGTH + 3, 3_000_000L, rnd);
            List<M3ScoreRecord> afterRestore = drain(h2);
            assertFalse(afterRestore.isEmpty(),
                    "恢复之后应立即继续打分；若为空，说明模型权重或相位没有随 checkpoint 存活");
        }

        // 对照组：同样的输入喂进一个**未恢复**的全新算子，必须一条分都打不出来。
        // 没有这个对照，上面那条断言无法区分「状态确实恢复了」与「这点输入本来就够打分」。
        // Control: the same input into a fresh, un-restored operator must score nothing. Without it
        // the assertion above cannot distinguish a real restore from "that input alone suffices".
        try (OneInputStreamOperatorTestHarness<AnnotatedRound, M3ScoreRecord> fresh = newHarness()) {
            feed(fresh, "E", WINDOW_LENGTH + 3, 3_000_000L, new Random(3));
            assertTrue(drain(fresh).isEmpty(),
                    "未恢复的新算子在同样输入下不应打分——否则恢复测试无判别力");
        }
    }

    @Test
    @DisplayName("冷启动只训练一个模型，隐藏层宽度取自参数而非算子自行搜索")
    void coldStartTrainsExactlyOneModel() throws Exception {
        double[] weights = new double[Channels.N_DET];
        java.util.Arrays.fill(weights, 1.0);
        // 这里刻意取 12——它不在被取消的那个硬编码网格 {40, 60, 90} 里。若搜索循环日后被重新引入，
        // 模型就会按 40、60、90 训练三次，下面两条断言都会失败。
        // 12 is deliberately outside the removed hard-coded grid, so a reintroduced search fails here.
        M3Function fn = new M3Function(TRAIN_DAYS, ES_DAYS, THRESH_DAYS, WINDOW_LENGTH, Z_THRESHOLD,
                weights, MAX_EPOCHS, PATIENCE, HIDDEN_SIZE, BATCH_SIZE, true, 0.001, 1.0, null, ROUNDS_PER_DAY);
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h =
                     new KeyedOneInputStreamOperatorTestHarness<>(
                             new KeyedProcessOperator<>(fn), AnnotatedRound::getDevice,
                             TypeInformation.of(String.class))) {
            h.open();
            feed(h, "E", ROUNDS_TO_ONLINE + WINDOW_LENGTH + 2, 1_000_000L, new Random(21));
            assertFalse(drain(h).isEmpty(), "应已进入 ONLINE 并产出评分");
            assertEquals(1, fn.lastTrainModels,
                    "冷启动应当只训练一个模型，实测训练了 " + fn.lastTrainModels
                            + " 个——算子自带的隐藏层搜索已按补遗三 §3 取消");
        }
    }

    @Test
    @DisplayName("参数错配：用与训练时不同的隐藏层宽度恢复，拒绝打分而不是静默出分")
    void restoringWithMismatchedHiddenSizeFailsLoudly() throws Exception {
        Random rnd = new Random(7);
        org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;

        try (OneInputStreamOperatorTestHarness<AnnotatedRound, M3ScoreRecord> h = newHarness()) {
            feed(h, "E", ROUNDS_TO_ONLINE + WINDOW_LENGTH + 5, 1_000_000L, rnd);
            assertFalse(drain(h).isEmpty(), "快照之前应已进入 ONLINE");
            snapshot = h.snapshot(1L, 1L);
        }

        // 换一个隐藏层宽度恢复。错配的模型算出来的分毫无意义，而且不会有任何外在症状，
        // 所以这里要求的是「响亮地失败」，不是「尽力打分」。
        // A mismatched model produces meaningless scores with no outward symptom; fail loudly.
        double[] weights = new double[Channels.N_DET];
        java.util.Arrays.fill(weights, 1.0);
        M3Function mismatched = new M3Function(TRAIN_DAYS, ES_DAYS, THRESH_DAYS, WINDOW_LENGTH,
                Z_THRESHOLD, weights, MAX_EPOCHS, PATIENCE, HIDDEN_SIZE + 4, BATCH_SIZE,
                true, 0.001, 1.0, null, ROUNDS_PER_DAY);
        try (KeyedOneInputStreamOperatorTestHarness<String, AnnotatedRound, M3ScoreRecord> h2 =
                     new KeyedOneInputStreamOperatorTestHarness<>(
                             new KeyedProcessOperator<>(mismatched), AnnotatedRound::getDevice,
                             TypeInformation.of(String.class))) {
            h2.setup();
            h2.initializeState(snapshot);
            h2.open();

            Exception thrown = assertThrows(Exception.class,
                    () -> feed(h2, "E", WINDOW_LENGTH + 3, 3_000_000L, new Random(7)),
                    "隐藏层宽度与模型不符时不得继续打分");
            assertTrue(messageChain(thrown).contains("拒绝用错配的模型打分"),
                    "异常应当明确指出是参数错配，实际信息：" + messageChain(thrown));
        }
    }

    /** 把异常链上的全部信息串起来，便于在 Flink 包装过异常之后仍能断言根因。 */
    private static String messageChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            sb.append(c.getMessage()).append(" | ");
        }
        return sb.toString();
    }
}
