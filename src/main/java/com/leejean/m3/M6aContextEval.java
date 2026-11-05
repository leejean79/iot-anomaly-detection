package com.leejean.m3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.m1.ChannelTransform;
import com.leejean.m1.Channels;
import com.leejean.m1.RobustScalerFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * 适应执行第一批离线模拟判读口径第五条（1.30 版裁决第四项，做法甲）：上下文通道在命令前后、新旧两组标准化参数下的主分分布。
 * 用上下文模块在线的同一套函数（G 光照输入归零、删失掩码、不重叠窗口、训练净化、{@link M3Training#train}、{@link M3Function#calibrate}、
 * {@link M3Scorer#score}），按全量运行的参数在旧参数（冻结参数）标准化的轮上训练并标定一个模型，再对四格评分：命令前一天与命令后七天，
 * 各用旧参数与新参数标准化。模型与全量运行的不是同一个（训练的随机性），种子由 --seed 指定并写进输出。
 * Context-channel main-score distributions before and after the command, under the old and the new normalization.
 *
 * <p>脚本交付五要素：
 * <ol>
 *   <li>执行环境：开发机，仓库根目录，JDK 11；先 mvn package（或用 target/classes 与依赖类路径）。</li>
 *   <li>调用命令：{@code java -cp <类路径> com.leejean.m3.M6aContextEval --rounds docs/m6a/extract/rounds_G.csv.gz --device G
 *       --params docs/m6a/sim/G_D1_py.json --outliers docs/m6a/sim/G_off_outliers.txt --day 2022-03-25 --seed 42
 *       --out docs/m6a/ctx/G_seed42.csv}；可选 --hidden 60、--window 60、--batch 64、--lr 0.001、--grad-clip 39072.0、
 *       --max-epochs 300、--patience 20、--z 2.22、--train-days 7、--es-days 2、--thresh-days 7、--warmup-rounds 60480。</li>
 *   <li>前置条件：轮表、零级参数 JSON（m6a/reestimate.py 的输出，含冻结参数与新参数）、无命令运行的离群轮表都已生成。</li>
 *   <li>期望产出：out 处一行表头加四行（两段 × 两组参数）的主分中位数、第九十五百分位、超阈率与窗口数，以及训练摘要；
 *       终端逐轮打印训练进度。训练在四核开发机上每次约数十分钟。</li>
 *   <li>失败兜底：轮表不足以冻结或训练三段不齐时退出码 2。</li>
 * </ol>
 */
public final class M6aContextEval {
    private M6aContextEval() {
    }

    private static final int N = Channels.N_DET;

    /** 一条入口轮 / one admitted round. */
    private static final class Pt {
        long ts;
        double[] xOld;
        double[] xNew;
        boolean[] mask;
        boolean outlier;
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            a.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        String dev = a.get("device");
        long eff = LocalDate.parse(a.get("day")).atStartOfDay(ZoneId.of("Europe/London")).toEpochSecond();
        long before = LocalDate.parse(a.get("day")).minusDays(1).atStartOfDay(ZoneId.of("Europe/London")).toEpochSecond();
        long after = LocalDate.parse(a.get("day")).plusDays(7).atStartOfDay(ZoneId.of("Europe/London")).toEpochSecond();
        long seed = Long.parseLong(a.getOrDefault("seed", "42"));
        int hidden = Integer.parseInt(a.getOrDefault("hidden", "60"));
        int window = Integer.parseInt(a.getOrDefault("window", "60"));
        int batch = Integer.parseInt(a.getOrDefault("batch", "64"));
        double lr = Double.parseDouble(a.getOrDefault("lr", "0.001"));
        double clip = Double.parseDouble(a.getOrDefault("grad-clip", "39072.0"));
        int maxEpochs = Integer.parseInt(a.getOrDefault("max-epochs", "300"));
        int patience = Integer.parseInt(a.getOrDefault("patience", "20"));
        double z = Double.parseDouble(a.getOrDefault("z", "2.22"));
        long perDay = 8640L;
        long trainR = Long.parseLong(a.getOrDefault("train-days", "7")) * perDay;
        long esR = Long.parseLong(a.getOrDefault("es-days", "2")) * perDay;
        long thR = Long.parseLong(a.getOrDefault("thresh-days", "7")) * perDay;
        int warmup = Integer.parseInt(a.getOrDefault("warmup-rounds", "60480"));

        JsonNode pj = new ObjectMapper().readTree(new File(a.get("params")));
        double[][] fp = params(pj.get("freeze"));
        double[][] np = params(pj.get("new"));
        boolean[] fb = bools(pj.get("freeze").get("bypass"));
        boolean[] nb = bools(pj.get("new").get("bypass"));
        Set<Long> outl = new HashSet<>();
        for (String l : Files.readAllLines(Paths.get(a.get("outliers")))) {
            if (!l.trim().isEmpty()) {
                outl.add(Long.parseLong(l.trim()));
            }
        }
        ChannelTransform[] tf = ChannelTransform.defaultTable();

        // ---- 入口轮：冻结之后、五个通道都有读数 / admitted rounds
        List<Pt> pts = new ArrayList<>();
        int idx = 0;
        try (BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(
                new FileInputStream(a.get("rounds"))), StandardCharsets.UTF_8))) {
            String line = r.readLine();
            while ((line = r.readLine()) != null) {
                idx++;
                if (idx <= warmup) {
                    continue;                              // 预热期（含触发冻结的那一轮）/ warm-up rounds
                }
                String[] f = line.split(",", -1);
                double[] x = new double[N];
                boolean[] miss = new boolean[N];
                boolean any = false;
                for (int c = 0; c < N; c++) {
                    miss[c] = f[1 + c].isEmpty();
                    any |= miss[c];
                    x[c] = miss[c] ? 0.0 : Double.parseDouble(f[1 + c]);
                }
                if (any) {
                    continue;                              // 点通道入口不接纳有缺失的轮 / not admitted
                }
                Pt p = new Pt();
                p.ts = Long.parseLong(f[0]);
                p.xOld = RobustScalerFunction.normalize(x, miss, fp[0], fp[1], fb, tf);
                p.xNew = RobustScalerFunction.normalize(x, miss, np[0], np[1], nb, tf);
                M3Function.zeroDeviceGLight(p.xOld, dev);
                M3Function.zeroDeviceGLight(p.xNew, dev);
                boolean[] cens = new boolean[N];
                cens[Channels.LIGHT_INDEX] = x[Channels.LIGHT_INDEX] == 65536.0;
                p.mask = WeightedMseLoss.buildMask(cens);
                p.outlier = outl.contains(p.ts);
                pts.add(p);
            }
        }

        // ---- 三段：按已见入口轮数分配不重叠窗口（与 M3Function.handleCollecting 相同）/ the three segments
        List<double[][]> trW = new ArrayList<>();
        List<boolean[][]> trM = new ArrayList<>();
        List<double[][]> esW = new ArrayList<>();
        List<double[][]> thW = new ArrayList<>();
        List<boolean[][]> thM = new ArrayList<>();
        int excluded = 0;
        long segEnd = 0;
        for (int i = 0; i + window <= pts.size(); i += window) {
            long count = i + window;
            if (count > trainR + esR + thR) {
                break;
            }
            double[][] w = new double[window][];
            boolean[][] m = new boolean[window][];
            boolean hasOutlier = false;
            for (int t = 0; t < window; t++) {
                w[t] = pts.get(i + t).xOld;
                m[t] = pts.get(i + t).mask;
                hasOutlier |= pts.get(i + t).outlier;
            }
            if (count <= trainR) {
                if (hasOutlier) {
                    excluded++;
                } else {
                    trW.add(w);
                    trM.add(m);
                }
            } else if (count <= trainR + esR) {
                esW.add(w);
            } else {
                thW.add(w);
                thM.add(m);
            }
            segEnd = pts.get(i + window - 1).ts;
        }
        if (trW.isEmpty() || esW.isEmpty() || thW.isEmpty()) {
            System.err.println("ERROR: 训练三段不齐 / segments incomplete");
            System.exit(2);
        }
        System.out.printf("%s 种子 %d：训练窗 %d（净化剔除 %d）、早停窗 %d、标定窗 %d；三段自 %d 至 %d%n", dev, seed, trW.size(),
                excluded, esW.size(), thW.size(), pts.get(0).ts, segEnd);

        M3Training.Config cfg = new M3Training.Config(N, hidden, window, batch, maxEpochs, patience, null, true, lr, clip);
        cfg.seed = seed;
        M3Training.Result res = M3Training.train(cfg, trW.toArray(new double[0][][]), trM.toArray(new boolean[0][][]),
                esW.toArray(new double[0][][]), (epoch, trainLoss, esLoss, sec) -> System.out.printf(
                        "  epoch %d：训练误差 %.6g，早停误差 %.6g，%.1f 秒%n", epoch, trainLoss, esLoss, sec));
        M3Scorer scorer = M3Function.calibrate(res.model, thW.toArray(new double[0][][]), thM.toArray(new boolean[0][][]), null, z);

        // ---- 四格评分 / the four cells
        WeightedMseLoss loss = new WeightedMseLoss(N, null);
        File out = new File(a.get("out"));
        if (out.getParentFile() != null) {
            out.getParentFile().mkdirs();
        }
        try (PrintWriter pw = new PrintWriter(out, "UTF-8")) {
            pw.println("device,seed,span,params,windows,median,p95,above_rate,train_windows,excluded,es_windows,th_windows,epochs,"
                    + "early_stop_loss,train_seconds,scorer_median,scorer_iqr,segment_end");
            for (String span : new String[]{"before", "after"}) {
                long t0 = "before".equals(span) ? before : eff;
                long t1 = "before".equals(span) ? eff : after;
                for (String which : new String[]{"old", "new"}) {
                    List<Double> s = new ArrayList<>();
                    int above = 0;
                    List<Pt> in = new ArrayList<>();
                    for (Pt p : pts) {
                        if (p.ts >= t0 && p.ts < t1) {
                            in.add(p);
                        }
                    }
                    for (int i = 0; i + window <= in.size(); i += window) {
                        double[][] w = new double[window][];
                        boolean[][] m = new boolean[window][];
                        for (int t = 0; t < window; t++) {
                            w[t] = "old".equals(which) ? in.get(i + t).xOld : in.get(i + t).xNew;
                            m[t] = in.get(i + t).mask;
                        }
                        WeightedMseLoss.LossResult lr0 = loss.compute(w, res.model.reconstruct(w), m, window);
                        M3Scorer.ScoreResult sr = scorer.score(lr0.wmse, lr0.perChannelMse);
                        s.add(sr.mainScore);
                        above += sr.aboveThreshold ? 1 : 0;
                    }
                    double[] v = s.stream().mapToDouble(Double::doubleValue).toArray();
                    Arrays.sort(v);
                    pw.println(String.join(",", dev, Long.toString(seed), span, which, Integer.toString(v.length),
                            Double.toString(pct(v, 0.5)), Double.toString(pct(v, 0.95)),
                            Double.toString(v.length == 0 ? Double.NaN : (double) above / v.length),
                            Integer.toString(trW.size()), Integer.toString(excluded), Integer.toString(esW.size()),
                            Integer.toString(thW.size()), Integer.toString(res.epochs), Double.toString(res.earlyStopLoss),
                            Double.toString(res.seconds), Double.toString(scorer.getMedian()), Double.toString(scorer.getIqr()),
                            Long.toString(segEnd)));
                    System.out.printf("  %s %s：窗口 %d，主分中位数 %.4g，P95 %.4g，超阈率 %.4f%n", span, which, v.length,
                            pct(v, 0.5), pct(v, 0.95), v.length == 0 ? Double.NaN : (double) above / v.length);
                }
            }
        }
    }

    private static double[][] params(JsonNode n) {
        double[][] p = new double[2][N];
        for (int c = 0; c < N; c++) {
            p[0][c] = n.get("median").get(c).asDouble();
            p[1][c] = n.get("scale").get(c).asDouble();
        }
        return p;
    }

    private static boolean[] bools(JsonNode n) {
        boolean[] b = new boolean[N];
        for (int c = 0; c < N; c++) {
            b[c] = n.get(c).asBoolean();
        }
        return b;
    }

    /** 线性插值分位数（与 numpy 默认相同的位置公式）/ linear percentile. */
    private static double pct(double[] v, double q) {
        if (v.length == 0) {
            return Double.NaN;
        }
        double pos = q * (v.length - 1);
        int lo = (int) Math.floor(pos);
        int hi = Math.min(lo + 1, v.length - 1);
        return v[lo] + (v[hi] - v[lo]) * (pos - lo);
    }
}
