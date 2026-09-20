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
        boolean sanitized;   // 是否做了训练净化 / whether sanitization was applied
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = parseArgs(args);
        String jsonl = a.getOrDefault("rounds-jsonl", "m1out.jsonl");
        String outCsv = a.getOrDefault("out", "m3_grid.csv");
        String[] devices = a.getOrDefault("devices", "E,G,C").split(",");
        int[] hiddenGrid = parseInts(a.getOrDefault("hidden-grid", "40,60,90"));
        int[] windowGrid = parseInts(a.getOrDefault("window-grid", "30,60,120"));
        int trainDays = Integer.parseInt(a.getOrDefault("train-days", "7"));
        int esDays = Integer.parseInt(a.getOrDefault("early-stop-days", "2"));
        int maxEpochs = Integer.parseInt(a.getOrDefault("max-epochs", "200"));
        int patience = Integer.parseInt(a.getOrDefault("patience", "10"));
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
        int totalCombos = byDevice.size() * hiddenGrid.length * windowGrid.length;
        System.out.printf("[grid] 开始：%d 设备 × %d 隐藏层 × %d 窗口长度 = %d 种组合"
                        + "（训练 %d 天 / 早停 %d 天，maxEpochs=%d，patience=%d）%n",
                byDevice.size(), hiddenGrid.length, windowGrid.length, totalCombos,
                trainDays, esDays, maxEpochs, patience);

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
                    done += hiddenGrid.length;
                    continue;
                }
                for (int hs : hiddenGrid) {
                    long t0 = System.currentTimeMillis();
                    LstmAutoEncoder ae = new LstmAutoEncoder(N_FEATURES, hs);
                    double prevLoss = Double.MAX_VALUE;
                    int noImprove = 0;
                    int epochsRun = 0;
                    for (int epoch = 0; epoch < maxEpochs; epoch++) {
                        ae.trainEpoch(split.train, split.trainMasks);
                        epochsRun = epoch + 1;
                        double esLoss = M3Function.evaluateLoss(ae, split.earlyStop, channelWeights);
                        if (esLoss < prevLoss - 1e-6) {
                            prevLoss = esLoss;
                            noImprove = 0;
                        } else {
                            noImprove++;
                            if (noImprove >= patience) {
                                break;
                            }
                        }
                    }
                    Result r = new Result();
                    r.device = device;
                    r.hiddenSize = hs;
                    r.windowLength = win;
                    r.trainWindows = split.train.length;
                    r.trainExcluded = split.trainExcluded;
                    r.esWindows = split.earlyStop.length;
                    r.epochs = epochsRun;
                    r.esLoss = M3Function.evaluateLoss(ae, split.earlyStop, channelWeights);
                    r.trainSeconds = (System.currentTimeMillis() - t0) / 1000.0;
                    r.sanitized = sanitized;
                    results.add(r);
                    done++;
                    System.out.printf("[grid] (%d/%d) %s hidden=%d window=%d → 早停集误差 %.6f，"
                                    + "训练 %d 个 epoch，用时 %.1fs（训练集 %d 窗，剔除 %d 窗，早停集 %d 窗）%n",
                            done, totalCombos, device, hs, win, r.esLoss, r.epochs, r.trainSeconds,
                            r.trainWindows, r.trainExcluded, r.esWindows);
                }
            }
        }

        writeCsv(results, outCsv);
        interpret(results, outCsv);
    }

    /** 训练集与早停集，以及被净化剔除的窗口数。 */
    private static final class Split {
        double[][][] train;
        boolean[][][] trainMasks;
        double[][][] earlyStop;
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
            } else {
                break;                                     // 网格只用训练段与早停段 / the grid needs no more
            }
        }

        Split s = new Split();
        s.train = train.toArray(new double[0][][]);
        s.trainMasks = trainMasks.toArray(new boolean[0][][]);
        s.earlyStop = es.toArray(new double[0][][]);
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

    private static void writeCsv(List<Result> results, String path) throws Exception {
        try (PrintWriter pw = new PrintWriter(path, "UTF-8")) {
            pw.println("device,hiddenSize,windowLength,trainWindows,trainExcluded,esWindows,"
                    + "epochs,esLoss,trainSeconds,sanitized");
            for (Result r : results) {
                pw.printf("%s,%d,%d,%d,%d,%d,%d,%.8f,%.1f,%s%n",
                        r.device, r.hiddenSize, r.windowLength, r.trainWindows, r.trainExcluded,
                        r.esWindows, r.epochs, r.esLoss, r.trainSeconds, r.sanitized);
            }
        }
        System.out.println("[grid] CSV → " + path);
    }

    /** 逐设备给出早停集误差最小的组合，并列出与它同一量级的其他组合，供设计会话裁决选型。 */
    private static void interpret(List<Result> results, String outCsv) {
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
        System.out.println("提醒：本阶段**不定终值**——上表交设计会话裁决 (hidden, window)。");
        System.out.println("完整结果见 " + outCsv);
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
