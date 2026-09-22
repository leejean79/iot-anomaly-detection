package com.leejean.m3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.m1.Channels;
import com.leejean.m1.DeviceRound;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * V-M3-3 离线超参数网格：在三台代表设备上，对隐藏层大小与窗口长度的每一种组合训练一个
 * LSTM 自编码器，记录早停集误差、实际训练轮数（epoch）与耗时，供设计会话据此选型。
 *
 * <p>本程序是**离线**的：它读取 {@code synergia-m1-out} 的转储文件，在一个临时容器里以纯 Java 运行，
 * 不提交 Flink 作业、不占用集群的任务槽位（slot）。这与 {@code M2Probe} 在 M2 阶段扮演的角色相同。
 *
 * <p><b>口径必须与在线算子一致</b>，否则网格选出来的超参数与 {@link M3Function} 实际运行的不是
 * 同一件事。为此本程序复用了 {@link M3Function} 的两个包级可见方法：
 * {@link M3Function#zeroDeviceGLight} 负责把设备 G 的 Light 通道输入归零，
 * {@link M3Function#evaluateLoss} 负责计算早停集误差。其余口径也逐条对齐：跳过预热轮、跳过含缺失
 * 通道的轮、窗口不重叠（每满一个窗口即清空缓冲）、训练集剔除含离群轮的整窗而早停集不做剔除。
 *
 * <p><b>一处必须知情的口径限制：训练净化需要额外的输入。</b>在线算子从 {@link AnnotatedRound} 上直接
 * 读到 M2 给每一轮打的离群标记，据此把含离群轮的整窗从训练集剔除（训练净化）。而 {@code m1-out} 的
 * 转储里装的是 {@code DeviceRound}，**不带这个标记**——它是 M2 在作业内部产生的，没有写进 m1-out。
 * 因此本程序提供 {@code --scores-jsonl} 参数：传入 {@code synergia-scores} 的转储，程序据其中的
 * (设备, 轮时间戳) 名单还原离群标记，从而与在线口径一致。**不传则不做训练净化**，此时网格结果会
 * 偏乐观（含异常的窗口也参与了训练），程序会在标准输出与 CSV 的同一行里显式标注这一点。
 *
 * <p>Sanitization needs an extra input: the online operator reads M2's per-round outlier flag off the
 * AnnotatedRound, but the m1-out dump holds DeviceRound, which does not carry it. Pass
 * --scores-jsonl (a dump of synergia-scores) to restore the flags; without it no sanitization is
 * applied and every line says so.
 *
 * <p>Offline hyper-parameter grid for V-M3-3: for each device and each (hidden size, window length)
 * pair, train one autoencoder and record early-stopping-set error, epochs actually run and wall time.
 * It reads a dump of synergia-m1-out and runs as plain Java, submitting no Flink job. Its caliber
 * mirrors {@link M3Function} exactly, reusing that class's own preprocessing and loss methods.
 *
 * <p>调用示例 / Invocation:
 * <pre>
 * java -cp app.jar com.leejean.m3.M3Grid \
 *     --rounds-jsonl /work/m1out.jsonl --devices E,G,C \
 *     --hidden-grid 40,60,90 --window-grid 30,60,120 \
 *     --train-days 7 --early-stop-days 2 --max-epochs 200 --patience 10 \
 *     --out /work/m3_grid.csv
 * </pre>
 */
public final class M3Grid {

    private static final int N_FEATURES = Channels.N_DET;
    /** 每天折合的轮数的默认值：10 秒一轮 × 86,400 秒 = 8,640。与在线算子同值。 */
    private static final int DEFAULT_ROUNDS_PER_DAY = 8640;

    private M3Grid() {
    }

    /** 一台设备的一条轮记录，按事件时间升序保存。 */
    private static final class Row {
        final double[] xNorm;
        final boolean[] mask;      // true = 该通道参与损失 / true = channel counts toward the loss
        final boolean outlier;

        Row(double[] xNorm, boolean[] mask, boolean outlier) {
            this.xNorm = xNorm;
            this.mask = mask;
            this.outlier = outlier;
        }
    }

    /** 一种组合的结果。 */
    private static final class Result {
        String device;
        int hiddenSize;
        int windowLength;
        int trainWindows;
        int trainExcluded;
        int esWindows;
        int epochs;
        double esLoss;
        double trainSeconds;
        /** 早停集中**不含**离群轮的窗口的平均误差 / mean loss over clean early-stop windows. */
        double esLossClean;
        /** 早停集中**含**离群轮的窗口的平均误差 / mean loss over outlier-bearing early-stop windows. */
        double esLossOutlier;
        /** 两者之比。**只作诊断，不作判据**（裁决书第三节）。/ diagnostic only, never a criterion. */
        double separationRatio;
        /**
         * 上面两个平均值各自是由多少个窗口算出来的。没有这两个数，分离比无法判读：若含离群轮的窗口
         * 只有个位数，这个比值就由一两个窗口的个性决定，其波动不说明任何模型性质。
         * The window counts behind the two means; without them the ratio cannot be interpreted.
         */
        int esCleanWindows;
        int esOutlierWindows;
        /**
         * 平凡基线：在早停集上「一律输出 0」的加权均方误差。M1 的 RobustScaler 把每个通道减去中位数
         * 再除以四分位距，因此归一化之后 0 就是该通道的中位数——输出 0 等于「什么都不学，一律猜中位数」。
         * 没有这个标尺，一个 0.37 的误差究竟算好算坏无从判断。模型误差若不明显低于它，说明模型没有
         * 学到任何有用的东西。
         * Trivial baseline: predicting all zeros, which after the robust scaling is the per-channel
         * median. Without it there is no way to tell whether a given loss is good.
         */
        double esLossBaseline;
        int batchSize;
        String ompThreads;
        boolean sanitized;   // 是否做了训练净化 / whether sanitization was applied
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = parseArgs(args);
        String jsonl = a.getOrDefault("rounds-jsonl", "m1out.jsonl");
        announceRuntime();
        installTerminationHook();

        String outCsv = a.getOrDefault("out", "m3_grid.csv");
        String[] devices = a.getOrDefault("devices", "E,G,C").split(",");
        int[] hiddenGrid = parseInts(a.getOrDefault("hidden-grid", "40,60,90"));
        int[] windowGrid = parseInts(a.getOrDefault("window-grid", "30,60,120"));
        int trainDays = Integer.parseInt(a.getOrDefault("train-days", "7"));
        int esDays = Integer.parseInt(a.getOrDefault("early-stop-days", "2"));
        int maxEpochs = Integer.parseInt(a.getOrDefault("max-epochs", "200"));
        int patience = Integer.parseInt(a.getOrDefault("patience", "10"));
        // 小批量大小，与隐藏层、窗口长度一样是可以成组扫描的一个维度。默认 "1" 即 2026-09-21
        // 参照点的口径；步骤 A 传入 "1,16,32,64" 在一次运行里把整条扫描做完，好处是四个读数共用
        // 同一份转储、同一次切窗、同一个进程，除小批量大小外没有任何其他差别。
        // Mini-batch size is a sweepable dimension; step A passes "1,16,32,64" so all readings come
        // from one process and differ in nothing but the batch size.
        int[] batchGrid = parseInts(a.getOrDefault("batch-grid", "1"));
        // 参照早停集误差。给出时 CSV 的 relDeltaVsRef 列写出相对偏差，并在解读段按补遗三 §2 的
        // 5% 判据给出选型建议；不给则该列留空——没有参照就不该凭空算出一个相对值。
        // The reference early-stopping loss; without it relDeltaVsRef is left empty.
        double referenceLoss = Double.parseDouble(a.getOrDefault("reference-loss", "0"));
        // 重构目标是否取逆序。默认开启，与在线算子的默认值一致；两边不一致会让等值核验失效。
        // Reversed reconstruction target; must match the online operator's value.
        boolean reverseTarget = !"false".equalsIgnoreCase(a.getOrDefault("reverse-target", "true"));
        // OpenMP 线程数**不由本程序设置**，它由容器的 OMP_NUM_THREADS 环境变量决定。此处只是把它
        // 读出来写进 CSV，使每一行自带它是在什么并行度下测出来的。ND4J 启动日志里的
        // "Number of threads used for OpenMP BLAS" 才是权威确认。
        // The thread count is set by the container's OMP_NUM_THREADS; this only records it.
        String ompThreads = System.getenv("OMP_NUM_THREADS");
        if (ompThreads == null || ompThreads.isEmpty()) {
            ompThreads = "default";
        }
        // 每天折合多少轮。默认 8,640，与在线算子一致；只有当转储的轮密度确实不同于生产时才需要改动
        // （例如单元测试用几十条轮验证切分逻辑）。它只决定「多少条轮算作一天」，不改变任何其他语义。
        // Rounds per day; only change it when the dump's round density genuinely differs from
        // production, e.g. a unit test verifying the split with a few dozen rounds.
        int roundsPerDay = Integer.parseInt(
                a.getOrDefault("rounds-per-day", String.valueOf(DEFAULT_ROUNDS_PER_DAY)));
        String scoresJsonl = a.getOrDefault("scores-jsonl", "");
        java.util.Set<String> outlierKeys = scoresJsonl.isEmpty()
                ? java.util.Collections.emptySet() : readOutlierKeys(scoresJsonl);
        boolean sanitized = !outlierKeys.isEmpty();
        if (!sanitized) {
            System.out.println("[grid] **未做训练净化**：没有提供 --scores-jsonl，无法还原 M2 的逐轮离群标记，"
                    + "含异常的窗口也会参与训练，结果偏乐观。CSV 的 sanitized 列记为 false。");
        }

        double[] channelWeights = new double[N_FEATURES];
        Arrays.fill(channelWeights, 1.0);

        Map<String, List<Row>> byDevice = readRounds(jsonl, devices, outlierKeys);
        if (byDevice.isEmpty()) {
            System.err.println("ERROR: 转储里没有任何一台目标设备的可用轮，无法开展网格搜索。");
            System.err.println("       目标设备：" + String.join(",", devices));
            System.err.println("       最常见的成因是转储条数不足以覆盖标定期（预热轮会被整段跳过），");
            System.err.println("       请提高外层脚本的 --max-messages。");
            System.exit(3);
        }

        long trainRounds = (long) trainDays * roundsPerDay;
        long esRounds = (long) esDays * roundsPerDay;
        int totalCombos = byDevice.size() * hiddenGrid.length * windowGrid.length * batchGrid.length;
        System.out.printf("[grid] 开始：%d 设备 × %d 隐藏层 × %d 窗口长度 × %d 小批量大小 = %d 种组合"
                        + "（训练 %d 天 / 早停 %d 天，maxEpochs=%d，patience=%d，OpenMP 线程=%s）%n",
                byDevice.size(), hiddenGrid.length, windowGrid.length, batchGrid.length, totalCombos,
                trainDays, esDays, maxEpochs, patience, ompThreads);

        List<Result> results = new ArrayList<>();
        int done = 0;
        for (Map.Entry<String, List<Row>> e : byDevice.entrySet()) {
            String device = e.getKey();
            List<Row> rows = e.getValue();
            for (int win : windowGrid) {
                // 窗口划分只与窗口长度有关，同一窗口长度下各隐藏层大小共用同一份数据集。
                Split split = buildSplit(rows, win, trainRounds, esRounds);
                if (split.train.length == 0 || split.earlyStop.length == 0) {
                    System.out.printf("[grid] %s 窗口长度 %d：训练集 %d 窗、早停集 %d 窗，样本不足，跳过。%n",
                            device, win, split.train.length, split.earlyStop.length);
                    done += hiddenGrid.length * batchGrid.length;
                    continue;
                }
                // 切分结果在训练**之前**打印。训练一个 epoch 要一两分钟，若等到组合结束才输出，
                // 运行中的日志会长时间毫无动静，无法区分「正在训练」与「已经死掉」。
                // Print the split BEFORE training: otherwise the log is silent for minutes on end
                // and a live run is indistinguishable from a dead one.
                int esOutlierCount = 0;
                for (boolean b : split.earlyStopHasOutlier) {
                    if (b) {
                        esOutlierCount++;
                    }
                }
                System.out.printf("[grid] %s 窗口长度 %d：训练集 %d 窗（剔除 %d 窗）、早停集 %d 窗"
                                + "（其中含离群轮的 %d 窗，分离比即由这 %d 窗与其余 %d 窗相比而来），"
                                + "本窗口长度下将依次训练 %d 个隐藏层宽度。%s%n",
                        device, win, split.train.length, split.trainExcluded,
                        split.earlyStop.length, esOutlierCount, esOutlierCount,
                        split.earlyStop.length - esOutlierCount, hiddenGrid.length, memoryLine());

                for (int bs : batchGrid) {
                for (int hs : hiddenGrid) {
                    final int fhs = hs;
                    final int fwin = win;
                    final String fdev = device;
                    // 与在线算子共用同一份训练实现（M3Training），这是补遗三 §6 等值核验的前提。
                    // The same training core the online operator uses — the basis of the parity check.
                    M3Training.Config cfg = new M3Training.Config(
                            N_FEATURES, hs, win, bs, maxEpochs, patience, channelWeights,
                            reverseTarget);
                    M3Training.Result trained = M3Training.train(
                            cfg, split.train, split.trainMasks, split.earlyStop,
                            new M3Training.EpochListener() {
                                @Override
                                public void onEpoch(int epoch, double esLoss, double epochSeconds) {
                                    // 逐 epoch 输出：既是存活信号，也把耗时与内存占用的走向留在日志里，
                                    // 以便事后判断进程是被内核终止的还是自己退出的。
                                    // Per-epoch output: a liveness signal plus the cost and memory trend.
                                    System.out.printf("[epoch] %s hidden=%d window=%d batch=%d  第 %d/%d 轮  "
                                                    + "早停集误差 %.6f  本轮 %.1fs  %s%n",
                                            fdev, fhs, fwin, cfg.batchSize, epoch, cfg.maxEpochs,
                                            esLoss, epochSeconds, memoryLine());
                                }
                            });
                    if (trained.epochs < maxEpochs) {
                        System.out.printf("[epoch] %s hidden=%d window=%d  早停触发：连续 %d 轮无改善，"
                                        + "于第 %d 轮中止。%n",
                                device, hs, win, patience, trained.epochs);
                    }
                    Result r = new Result();
                    r.device = device;
                    r.hiddenSize = hs;
                    r.windowLength = win;
                    r.trainWindows = split.train.length;
                    r.trainExcluded = split.trainExcluded;
                    r.esWindows = split.earlyStop.length;
                    r.epochs = trained.epochs;
                    r.esLoss = trained.earlyStopLoss;
                    r.trainSeconds = trained.seconds;
                    r.sanitized = sanitized;
                    r.batchSize = bs;
                    // 分离比（裁决书第三节）：只作诊断。它接近 1 说明模型在无差别地抄写输入，
                    // 该行的其余读数存疑。它**不是**选型判据——M2 标的是点异常，M3 管的是上下文
                    // 异常，口径不同，不能用它替代 V-M3-4 的误报率与 V-M3-5 的注入召回。
                    // Diagnostic only: a ratio near 1 means the model copies indiscriminately.
                    double[][][] esClean = subset(split.earlyStop, split.earlyStopHasOutlier, false);
                    double[][][] esDirty = subset(split.earlyStop, split.earlyStopHasOutlier, true);
                    r.esLossClean = esClean.length > 0
                            ? M3Training.evaluateLoss(trained.model, esClean, channelWeights) : Double.NaN;
                    r.esLossOutlier = esDirty.length > 0
                            ? M3Training.evaluateLoss(trained.model, esDirty, channelWeights) : Double.NaN;
                    r.separationRatio = (esClean.length > 0 && esDirty.length > 0 && r.esLossClean > 0)
                            ? r.esLossOutlier / r.esLossClean : Double.NaN;
                    r.esCleanWindows = esClean.length;
                    r.esOutlierWindows = esDirty.length;
                    r.esLossBaseline = zeroBaselineLoss(split.earlyStop, channelWeights);
                    r.ompThreads = ompThreads;
                    results.add(r);
                    done++;
                    System.out.printf("[grid] (%d/%d) %s hidden=%d window=%d batch=%d → 早停集误差 %.6f，"
                                    + "训练 %d 个 epoch，用时 %.1fs（训练集 %d 窗，剔除 %d 窗，早停集 %d 窗；"
                            + "分离比 %.2f = 含离群窗 %.6f ÷ 干净窗 %.6f）%n",
                            done, totalCombos, device, hs, win, bs, r.esLoss, r.epochs, r.trainSeconds,
                            r.trainWindows, r.trainExcluded, r.esWindows,
                            r.separationRatio, r.esLossOutlier, r.esLossClean);
                    // 每完成一个组合就落盘一次。全量网格要连续跑十几个小时，若只在全部结束后才写
                    // CSV，中途任何中止都会让已完成的组合一并作废。
                    // Persist after every combination: the full grid runs for many hours, and writing
                    // the CSV only at the very end would discard all completed work on any abort.
                    writeCsv(results, outCsv, false, referenceLoss);
                }
                }
            }
        }

        writeCsv(results, outCsv, true, referenceLoss);
        interpret(results, outCsv, referenceLoss);
    }

    /**
     * 平凡基线的误差：把重构结果取为全 0，与真实窗口比较。归一化之后 0 即每通道的中位数，
     * 所以这相当于「一律猜中位数」这个不学习的模型。它只取决于数据，与训练无关。
     * The all-zeros (per-channel median) baseline loss; it depends on the data alone.
     */
    private static double zeroBaselineLoss(double[][][] windows, double[] channelWeights) {
        if (windows.length == 0) {
            return Double.NaN;
        }
        WeightedMseLoss lossCalc = new WeightedMseLoss(N_FEATURES, channelWeights);
        double total = 0.0;
        for (double[][] window : windows) {
            double[][] zeros = new double[window.length][N_FEATURES];
            total += lossCalc.compute(window, zeros, null, window.length).wmse;
        }
        return total / windows.length;
    }

    /** 取出早停集中含（或不含）离群轮的那一部分窗口。 */
    private static double[][][] subset(double[][][] windows, boolean[] hasOutlier, boolean wanted) {
        List<double[][]> out = new ArrayList<>();
        for (int i = 0; i < windows.length && i < hasOutlier.length; i++) {
            if (hasOutlier[i] == wanted) {
                out.add(windows[i]);
            }
        }
        return out.toArray(new double[0][][]);
    }

    /** 训练集与早停集，以及被净化剔除的窗口数。 */
    private static final class Split {
        double[][][] train;
        boolean[][][] trainMasks;
        double[][][] earlyStop;
        /** 早停集每个窗口是否含 M2 离群轮。早停集不做净化，因此两类窗口都在里面。/ per-window outlier flag. */
        boolean[] earlyStopHasOutlier;
        int trainExcluded;
    }

    /**
     * 按在线算子的口径切分窗口：窗口不重叠（攒满即结算并清空缓冲）；按**已见轮数**把窗口分到训练段
     * 与早停段；训练段剔除含离群轮的整窗，早停段不做剔除。
     */
    private static Split buildSplit(List<Row> rows, int windowLength, long trainRounds, long esRounds) {
        List<double[][]> train = new ArrayList<>();
        List<boolean[][]> trainMasks = new ArrayList<>();
        List<double[][]> es = new ArrayList<>();
        List<Boolean> esHasOutlier = new ArrayList<>();
        int excluded = 0;

        List<Row> buf = new ArrayList<>(windowLength);
        long count = 0;
        for (Row row : rows) {
            count++;
            buf.add(row);
            if (buf.size() < windowLength) {
                continue;
            }
            double[][] window = new double[windowLength][];
            boolean[][] mask = new boolean[windowLength][];
            boolean hasOutlier = false;
            for (int i = 0; i < windowLength; i++) {
                window[i] = buf.get(i).xNorm;
                mask[i] = buf.get(i).mask;
                hasOutlier |= buf.get(i).outlier;
            }
            buf.clear();                                   // 窗口不重叠 / windows do not overlap

            if (count <= trainRounds) {
                if (hasOutlier) {
                    excluded++;                            // 训练净化：整窗剔除 / sanitization drops the window
                } else {
                    train.add(window);
                    trainMasks.add(mask);
                }
            } else if (count <= trainRounds + esRounds) {
                es.add(window);                            // 早停集不做净化 / no sanitization here
                esHasOutlier.add(hasOutlier);              // 但记下它含不含离群轮，供分离比 / recorded for the ratio
            } else {
                break;                                     // 网格只用训练段与早停段 / the grid needs no more
            }
        }

        Split s = new Split();
        s.train = train.toArray(new double[0][][]);
        s.trainMasks = trainMasks.toArray(new boolean[0][][]);
        s.earlyStop = es.toArray(new double[0][][]);
        s.earlyStopHasOutlier = new boolean[esHasOutlier.size()];
        for (int i = 0; i < esHasOutlier.size(); i++) {
            s.earlyStopHasOutlier[i] = esHasOutlier.get(i);
        }
        s.trainExcluded = excluded;
        return s;
    }

    /** 读入转储，按设备分组；口径与在线算子一致：跳过预热轮、跳过含缺失通道的轮。 */
    /**
     * 从 {@code synergia-scores} 的转储里读出离群点名单，键为「设备@轮时间戳」。
     * scores 流只列出被判为离群的点（{@code outlier} 恒为 true），因此名单即离群集合。
     * Read the outlier roster from a synergia-scores dump; that stream lists only outliers.
     */
    private static java.util.Set<String> readOutlierKeys(String path) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        java.util.Set<String> keys = new java.util.HashSet<>();
        long bad = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                try {
                    com.leejean.m2.ScoreEvent se = mapper.readValue(line, com.leejean.m2.ScoreEvent.class);
                    keys.add(se.getDevice() + "@" + se.getRoundTs());
                } catch (Exception ex) {
                    bad++;
                }
            }
        }
        System.out.printf("[grid] 离群名单读入：%d 条 (设备, 轮时间戳)；解析失败 %d 行%n", keys.size(), bad);
        return keys;
    }

    private static Map<String, List<Row>> readRounds(String jsonl, String[] devices,
                                                     java.util.Set<String> outlierKeys) throws Exception {
        java.util.Set<String> want = new java.util.HashSet<>(Arrays.asList(devices));
        ObjectMapper mapper = new ObjectMapper();
        Map<String, List<Row>> byDevice = new TreeMap<>();
        long total = 0;
        long skippedWarmup = 0;
        long skippedMissing = 0;
        long parseErrors = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(jsonl))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                total++;
                DeviceRound r;
                try {
                    r = mapper.readValue(line, DeviceRound.class);
                } catch (Exception ex) {
                    parseErrors++;
                    continue;
                }
                if (!want.contains(r.getDevice())) {
                    continue;
                }
                if (r.isWarmup()) {
                    skippedWarmup++;
                    continue;
                }
                if (r.missingCount() > 0) {
                    skippedMissing++;
                    continue;
                }
                double[] x = r.getXNorm().clone();
                M3Function.zeroDeviceGLight(x, r.getDevice());   // 与在线算子同一份实现
                byDevice.computeIfAbsent(r.getDevice(), k -> new ArrayList<>())
                        .add(new Row(x, WeightedMseLoss.buildMask(r.getCensoredMask()),
                                outlierKeys.contains(r.getDevice() + "@" + r.getTs())));
            }
        }
        System.out.printf("[grid] 读入完成：%d 行；跳过预热 %d 行、缺失通道 %d 行、解析失败 %d 行；"
                        + "目标设备命中 %d 台%n",
                total, skippedWarmup, skippedMissing, parseErrors, byDevice.size());
        for (Map.Entry<String, List<Row>> e : byDevice.entrySet()) {
            System.out.printf("[grid]   设备 %s：可用轮 %d%n", e.getKey(), e.getValue().size());
        }
        return byDevice;
    }

    /**
     * 写出 CSV。{@code announce} 为 false 时不打印提示，供每完成一个组合后的增量落盘使用，
     * 以免全量网格刷出几十行重复提示。
     * Write the CSV; announce=false is the quiet incremental save after each combination.
     */
    private static void writeCsv(List<Result> results, String path, boolean announce,
                                 double referenceLoss) throws Exception {
        try (PrintWriter pw = new PrintWriter(path, "UTF-8")) {
            pw.println("device,hiddenSize,windowLength,batchSize,ompThreads,"
                    + "trainWindows,trainExcluded,esWindows,"
                    + "epochs,esLoss,relDeltaVsRef,"
                    + "esCleanWindows,esOutlierWindows,esLossBaseline,"
                    + "esLossClean,esLossOutlier,separationRatio,"
                    + "trainSeconds,secPerEpoch,sanitized");
            for (Result r : results) {
                // secPerEpoch 由程序算出并写入，避免事后手算出错 / computed here, not by hand afterwards
                double secPerEpoch = r.epochs > 0 ? r.trainSeconds / r.epochs : 0.0;
                // relDeltaVsRef 由程序算出而非事后手算，避免选型判据栽在一次心算上。
                // 未给参照值时留空：没有参照就不该凭空写出一个相对值。
                // Computed here, never by hand; left empty when no reference was given.
                String rel = referenceLoss > 0
                        ? String.format("%.6f", (r.esLoss - referenceLoss) / referenceLoss) : "";
                pw.printf("%s,%d,%d,%d,%s,%d,%d,%d,%d,%.8f,%s,%d,%d,%.8f,%.8f,%.8f,%.4f,%.1f,%.1f,%s%n",
                        r.device, r.hiddenSize, r.windowLength, r.batchSize, r.ompThreads,
                        r.trainWindows, r.trainExcluded, r.esWindows,
                        r.epochs, r.esLoss, rel,
                        r.esCleanWindows, r.esOutlierWindows, r.esLossBaseline,
                        r.esLossClean, r.esLossOutlier, r.separationRatio,
                        r.trainSeconds, secPerEpoch, r.sanitized);
            }
        }
        if (announce) {
            System.out.println("[grid] CSV → " + path);
        }
    }

    /** 逐设备给出早停集误差最小的组合，并列出与它同一量级的其他组合，供设计会话裁决选型。 */
    private static void interpret(List<Result> results, String outCsv, double referenceLoss) {
        System.out.println("==================== 网格结果解读 / interpretation ====================");
        if (results.isEmpty()) {
            System.out.println("无结果可解读。");
            return;
        }
        Map<String, Result> best = new HashMap<>();
        for (Result r : results) {
            Result b = best.get(r.device);
            if (b == null || r.esLoss < b.esLoss) {
                best.put(r.device, r);
            }
        }
        for (Map.Entry<String, Result> e : new TreeMap<>(best).entrySet()) {
            Result b = e.getValue();
            System.out.printf("设备 %s：早停集误差最小的是 hidden=%d、window=%d，误差 %.6f，"
                            + "训练 %d 个 epoch、用时 %.1fs%n",
                    e.getKey(), b.hiddenSize, b.windowLength, b.esLoss, b.epochs, b.trainSeconds);
            for (Result r : results) {
                if (!r.device.equals(e.getKey()) || r == b) {
                    continue;
                }
                if (r.esLoss <= b.esLoss * 1.05) {
                    System.out.printf("         与之相差不到 5%% 的还有 hidden=%d、window=%d（误差 %.6f，"
                                    + "用时 %.1fs）——若两者统计上难分高下，宜取更小的模型与更短的窗口%n",
                            r.hiddenSize, r.windowLength, r.esLoss, r.trainSeconds);
                }
            }
        }
        if (referenceLoss > 0) {
            interpretBatchSweep(results, referenceLoss);
        }
        System.out.println("提醒：本阶段**不定终值**——上表交设计会话裁决 (hidden, window)。");
        System.out.println("完整结果见 " + outCsv);
    }

    /**
     * 启动自述：把这次运行拿到的内存额度打印出来。上一次运行在建好网络之后无声消失，日志里没有
     * 任何可供判断的数字，本行就是为补上这个缺口而加的。
     * Announce the memory budget at startup; the previous run vanished silently with nothing logged.
     */
    private static void announceRuntime() {
        Runtime rt = Runtime.getRuntime();
        System.out.printf("[grid] 运行环境：可用处理器 %d 个，JVM 堆上限 %d MB，%s%n",
                rt.availableProcessors(), rt.maxMemory() / 1048576L, memoryLine());
        System.out.println("[grid] 提示：堆上限之外，ND4J 的张量分配走**堆外内存**，"
                + "因此判断内存是否吃紧应当看下面每一行末尾的「进程驻留」。");
    }

    /**
     * 注册终止钩子。它能区分两类死法：进程收到 SIGTERM 或正常退出时钩子会打印一行；被内核的内存
     * 杀手以 SIGKILL 终止时钩子**不会**执行，日志里也就不会有这一行。日志末尾有没有它，直接
     * 回答了「是自己退出的还是被杀死的」。
     * A shutdown hook distinguishes SIGTERM/normal exit (the line is printed) from SIGKILL (it is not).
     */
    private static void installTerminationHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.printf("[grid] 进程正在退出（此行说明进程是正常结束或收到 SIGTERM；"
                        + "若日志末尾没有此行，说明它是被 SIGKILL 直接终止的）。%s%n", memoryLine());
                System.out.flush();
            }
        }));
    }

    /**
     * 一行内存读数：JVM 堆的已用与上限，以及整个进程的驻留内存。后者包含 ND4J 的堆外分配，是判断
     * 内存杀手风险的依据；取不到时记为 n/a 而不是让监控本身把程序搞崩。
     * One line of memory readings; the process-resident figure includes ND4J's off-heap allocations.
     */
    private static String memoryLine() {
        Runtime rt = Runtime.getRuntime();
        long heapUsedMb = (rt.totalMemory() - rt.freeMemory()) / 1048576L;
        long heapMaxMb = rt.maxMemory() / 1048576L;
        String rss = "n/a";
        try {
            rss = (org.bytedeco.javacpp.Pointer.physicalBytes() / 1048576L) + " MB";
        } catch (Throwable ignored) {
            // 取不到进程驻留内存不影响训练，保持 n/a / failing to read RSS must not break training
        }
        return String.format("堆 %d/%d MB，进程驻留 %s", heapUsedMb, heapMaxMb, rss);
    }

    /**
     * 小批量大小的选型判据（补遗三 §2）：取早停集误差不超过参照值 1.05 倍的**最大**小批量大小。
     * 一个都不满足时如实报告并明说不得靠调高学习率或 epoch 数去凑——那会改变被比较的对象本身。
     * Selection rule: the largest batch size whose loss stays within 5% relative of the reference.
     */
    private static void interpretBatchSweep(List<Result> results, double referenceLoss) {
        double threshold = referenceLoss * 1.05;
        System.out.printf("%n---------- 小批量大小选型（判据：早停集误差 ≤ 参照值 %.6f × 1.05 = %.6f）----------%n",
                referenceLoss, threshold);
        Result chosen = null;
        for (Result r : results) {
            String verdict = r.esLoss <= threshold ? "通过" : "超出判据";
            System.out.printf("  小批量 %-3d  早停集误差 %.6f（相对参照 %+.2f%%）  %d 个 epoch  "
                            + "每 epoch %.1fs  → %s%n",
                    r.batchSize, r.esLoss, 100.0 * (r.esLoss - referenceLoss) / referenceLoss,
                    r.epochs, r.epochs > 0 ? r.trainSeconds / r.epochs : 0.0, verdict);
            if (r.esLoss <= threshold && (chosen == null || r.batchSize > chosen.batchSize)) {
                chosen = r;
            }
        }
        if (chosen == null) {
            System.out.println("  **无一满足判据**。按补遗三 §2，此时应如实交出本表并停止，");
            System.out.println("  不得通过调高学习率或 epoch 数去凑——那改变的是被比较的对象本身。");
            return;
        }
        System.out.printf("  建议取小批量 %d（满足判据的最大值）。它须同时成为离线网格与在线算子的"
                        + "默认值，否则补遗三 §6 的等值核验不成立。%n", chosen.batchSize);
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].startsWith("--")) {
                m.put(args[i].substring(2), args[i + 1]);
            }
        }
        return m;
    }

    private static int[] parseInts(String csv) {
        String[] parts = csv.split(",");
        int[] out = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = Integer.parseInt(parts[i].trim());
        }
        return out;
    }
}
