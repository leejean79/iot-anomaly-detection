package com.leejean.m3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.m1.ChannelTransform;
import com.leejean.m1.Channels;
import com.leejean.m1.RobustScalerFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * 开放问题第十二条的离线实验（M6b 交接文档第五节）：一级重训的训练长度 N 与热冷启动。用例为三月事件后的设备 E：初始模型按全量运行的
 * 方式在冻结参数（版本一）标准化的入口轮上训练与标定（冻结后七天训练、两天早停、七天标定）；一级在 --at（默认 03-28 当地零点）生效，
 * 训练集取生效前 N 天的入口轮，按当前版本（--cur-params 的新参数）标准化，按七比二比七的比例划成训练、早停、标定三段，训练段剔除
 * 含点通道离群轮的窗口；热启动从初始模型的权重出发（优化器状态重新开始），冷启动从同一种子的新初始化出发。三项度量：重训耗时与轮数；
 * 重训后的平稳段超阈率；对四月五日六段注入（三段尖峰、三段阶跃）的召回。所用函数与上下文模块在线相同（训练、标定、评分）。
 * Open question 12: level-1 retraining length N and warm versus cold start, on device E after the March event.
 *
 * <p>评分窗口：每个评价时段从时段起点起按入口轮数划成不重叠的六十轮窗口。初始模型按训练版本（版本一）的参数评分，重训模型按重训
 * 时的当前版本评分（选项一：上下文模块按训练版本标准化）。
 *
 * <p>脚本交付五要素：
 * <ol>
 *   <li>执行环境：开发机，仓库根目录，JDK 11，先 mvn package；每个进程设 OMP_NUM_THREADS=2。</li>
 *   <li>调用命令：初始模型 {@code java -cp <jar> com.leejean.m3.M6bRetrainEval --phase init --seed 42 --model-dir docs/m6b/q12/models
 *       --out docs/m6b/q12/rows/init_42.csv}；重训 {@code ... --phase retrain --seed 42 --n 7 --start warm --model-dir ...
 *       --out docs/m6b/q12/rows/r_42_7_warm.csv}。其余选项有默认值：--rounds docs/m6b/extract/rounds_E_inj.csv.gz、--device E、
 *       --freeze-params docs/m6b/chain/D7/level0/E_1.json（取 freeze）、--cur-params docs/m6b/chain/D7/level0/E_2.json（取 new，版本三）、
 *       --outliers docs/m6b/chain/D7/outliers.csv、--inject docs/m3_a4/full_fix/inject-truth.csv、--at 2022-03-28、
 *       --stable-days 2022-03-29,2022-03-30,2022-04-17,2022-04-23,2022-04-24、--inject-day 2022-04-05，以及训练超参数（同 M6aContextEval）。</li>
 *   <li>前置条件：E 的注入轮表、链式实验乙档的零级 JSON 与离群轮表已生成；重训前须先跑同一种子的 init。</li>
 *   <li>期望产出：out 处一行表头加一行度量；init 另在 model-dir 写 init_&lt;种子&gt;.bin。终端逐轮打印训练进度。</li>
 *   <li>失败兜底：训练三段不齐时退出码 2；重训找不到初始模型时退出码 3。</li>
 * </ol>
 */
public final class M6bRetrainEval {
    private M6bRetrainEval() {
    }

    private static final int N = Channels.N_DET;
    private static final ZoneId ZONE = ZoneId.of("Europe/London");

    private static final class Pt {
        long ts;
        double[] x;
        boolean[] miss;
        boolean[] mask;
        boolean outlier;
    }

    private static final class Std {
        final double[] median;
        final double[] scale;
        final boolean[] bypass;

        Std(JsonNode n) {
            median = new double[N];
            scale = new double[N];
            bypass = new boolean[N];
            for (int c = 0; c < N; c++) {
                median[c] = n.get("median").get(c).asDouble();
                scale[c] = n.get("scale").get(c).asDouble();
                bypass[c] = n.get("bypass").get(c).asBoolean();
            }
        }
    }

    private static long day(String d) {
        return LocalDate.parse(d).atStartOfDay(ZONE).toEpochSecond();
    }

    private static long dayAfter(String d, int k) {
        return LocalDate.parse(d).plusDays(k).atStartOfDay(ZONE).toEpochSecond();
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            a.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        String phase = a.getOrDefault("phase", "init");
        String dev = a.getOrDefault("device", "E");
        long seed = Long.parseLong(a.getOrDefault("seed", "42"));
        int hidden = Integer.parseInt(a.getOrDefault("hidden", "60"));
        int window = Integer.parseInt(a.getOrDefault("window", "60"));
        int batch = Integer.parseInt(a.getOrDefault("batch", "64"));
        double lr = Double.parseDouble(a.getOrDefault("lr", "0.001"));
        double clip = Double.parseDouble(a.getOrDefault("grad-clip", "39072.0"));
        int maxEpochs = Integer.parseInt(a.getOrDefault("max-epochs", "300"));
        int patience = Integer.parseInt(a.getOrDefault("patience", "20"));
        double z = Double.parseDouble(a.getOrDefault("z", "2.22"));
        int warmup = Integer.parseInt(a.getOrDefault("warmup-rounds", "60480"));
        String atDay = a.getOrDefault("at", "2022-03-28");
        long at = day(atDay);
        int nDays = Integer.parseInt(a.getOrDefault("n", "7"));
        String start = a.getOrDefault("start", "warm");
        File modelDir = new File(a.getOrDefault("model-dir", "docs/m6b/q12/models"));
        ObjectMapper om = new ObjectMapper();
        Std v1 = new Std(om.readTree(new File(a.getOrDefault("freeze-params", "docs/m6b/chain/D7/level0/E_1.json"))).get("freeze"));
        JsonNode curJ = om.readTree(new File(a.getOrDefault("cur-params", "docs/m6b/chain/D7/level0/E_2.json")));
        Std cur = new Std(curJ.get("new"));
        int curVersion = curJ.get("version").asInt();
        Set<Long> outl = new HashSet<>();
        for (String l : Files.readAllLines(Paths.get(a.getOrDefault("outliers", "docs/m6b/chain/D7/outliers.csv")))) {
            String[] f = l.split(",");
            if (f[0].equals(dev)) {
                outl.add(Long.parseLong(f[1]));
            }
        }
        ChannelTransform[] tf = ChannelTransform.defaultTable();

        // ---- 入口轮：冻结之后、五个通道都有读数 / admitted rounds
        List<Pt> pts = new ArrayList<>();
        int idx = 0;
        try (BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(
                new FileInputStream(a.getOrDefault("rounds", "docs/m6b/extract/rounds_E_inj.csv.gz"))), StandardCharsets.UTF_8))) {
            String line = r.readLine();
            while ((line = r.readLine()) != null) {
                if (++idx <= warmup) {
                    continue;
                }
                String[] f = line.split(",", -1);
                Pt p = new Pt();
                p.x = new double[N];
                p.miss = new boolean[N];
                boolean any = false;
                for (int c = 0; c < N; c++) {
                    p.miss[c] = f[1 + c].isEmpty();
                    any |= p.miss[c];
                    p.x[c] = p.miss[c] ? 0.0 : Double.parseDouble(f[1 + c]);
                }
                if (any) {
                    continue;
                }
                p.ts = Long.parseLong(f[0]);
                boolean[] cens = new boolean[N];
                cens[Channels.LIGHT_INDEX] = p.x[Channels.LIGHT_INDEX] == 65536.0;
                p.mask = WeightedMseLoss.buildMask(cens);
                p.outlier = outl.contains(p.ts);
                pts.add(p);
            }
        }

        // ---- 三段 / the three segments
        Std trainStd;
        List<Pt> span = new ArrayList<>();
        long[] cut;                                            // 训练、早停、标定的窗口数 / windows per segment
        if ("init".equals(phase)) {
            trainStd = v1;
            long perDay = 8640L;
            long total = (Long.parseLong(a.getOrDefault("train-days", "7")) + Long.parseLong(a.getOrDefault("es-days", "2"))
                    + Long.parseLong(a.getOrDefault("thresh-days", "7"))) * perDay;
            for (int i = 0; i < pts.size() && i < total; i++) {
                span.add(pts.get(i));
            }
            long w = span.size() / window;
            cut = new long[]{7 * perDay / window, 2 * perDay / window, w - 9 * perDay / window};
        } else {
            trainStd = cur;
            long from = at - nDays * 86400L;
            for (Pt p : pts) {
                if (p.ts >= from && p.ts < at) {
                    span.add(p);
                }
            }
            long w = span.size() / window;
            long tr = w * 7 / 16;
            long es = w * 2 / 16;
            cut = new long[]{tr, es, w - tr - es};
        }
        List<double[][]> trW = new ArrayList<>();
        List<boolean[][]> trM = new ArrayList<>();
        List<double[][]> esW = new ArrayList<>();
        List<double[][]> thW = new ArrayList<>();
        List<boolean[][]> thM = new ArrayList<>();
        int excluded = 0;
        for (int k = 0; (k + 1) * window <= span.size(); k++) {
            double[][] w = new double[window][];
            boolean[][] m = new boolean[window][];
            boolean hasOutlier = false;
            for (int t = 0; t < window; t++) {
                Pt p = span.get(k * window + t);
                w[t] = norm(p, trainStd, tf, dev);
                m[t] = p.mask;
                hasOutlier |= p.outlier;
            }
            if (k < cut[0]) {
                if (hasOutlier) {
                    excluded++;
                } else {
                    trW.add(w);
                    trM.add(m);
                }
            } else if (k < cut[0] + cut[1]) {
                esW.add(w);
            } else {
                thW.add(w);
                thM.add(m);
            }
        }
        if (trW.isEmpty() || esW.isEmpty() || thW.isEmpty()) {
            System.err.println("ERROR: 训练三段不齐 / segments incomplete");
            System.exit(2);
        }
        byte[] init = null;
        File initFile = new File(modelDir, "init_" + seed + ".bin");
        if ("retrain".equals(phase) && "warm".equals(start)) {
            if (!initFile.exists()) {
                System.err.println("ERROR: 缺少初始模型 " + initFile + "；先跑 --phase init --seed " + seed);
                System.exit(3);
            }
            init = Files.readAllBytes(initFile.toPath());
        }
        System.out.printf("%s %s 种子 %d N=%d %s：训练窗 %d（净化剔除 %d）、早停窗 %d、标定窗 %d；三段自 %d 至 %d%n", dev, phase, seed,
                nDays, start, trW.size(), excluded, esW.size(), thW.size(), span.get(0).ts, span.get(span.size() - 1).ts);
        M3Training.Config cfg = new M3Training.Config(N, hidden, window, batch, maxEpochs, patience, null, true, lr, clip);
        cfg.seed = seed;
        M3Training.Result res = M3Training.train(cfg, init, trW.toArray(new double[0][][]), trM.toArray(new boolean[0][][]),
                esW.toArray(new double[0][][]), (epoch, trainLoss, esLoss, sec) -> System.out.printf(
                        "  epoch %d：训练误差 %.6g，早停误差 %.6g，%.1f 秒%n", epoch, trainLoss, esLoss, sec));
        M3Scorer scorer = M3Function.calibrate(res.model, thW.toArray(new double[0][][]), thM.toArray(new boolean[0][][]), null, z);
        if ("init".equals(phase)) {
            modelDir.mkdirs();
            try (FileOutputStream fo = new FileOutputStream(initFile)) {
                fo.write(res.model.serializeModel());
            }
        }

        // ---- 评价 / evaluation
        Eval ev = new Eval(res.model, scorer, trainStd, tf, dev, window, pts);
        double[] s1 = ev.span(at, dayAfter(atDay, 7));
        double[] s2 = new double[3];
        for (String d : a.getOrDefault("stable-days", "2022-03-29,2022-03-30,2022-04-17,2022-04-23,2022-04-24").split(",")) {
            double[] x = ev.span(day(d), dayAfter(d, 1));
            s2[0] += x[0];
            s2[1] += x[1];
        }
        String injDay = a.getOrDefault("inject-day", "2022-04-05");
        List<long[]> segs = new ArrayList<>();
        List<String> types = new ArrayList<>();
        for (String l : Files.readAllLines(Paths.get(a.getOrDefault("inject", "docs/m3_a4/full_fix/inject-truth.csv")))) {
            String[] f = l.split(",");
            if (!f[0].equals(dev) || f[0].equals("device")) {
                continue;
            }
            long s = Long.parseLong(f[2]);
            if (s >= day(injDay) && s < dayAfter(injDay, 1)) {
                segs.add(new long[]{s, Long.parseLong(f[3])});
                types.add(f[4]);
            }
        }
        double[] inj = ev.recall(day(injDay), dayAfter(injDay, 1), segs, types);
        File out = new File(a.get("out"));
        if (out.getParentFile() != null) {
            out.getParentFile().mkdirs();
        }
        try (PrintWriter pw = new PrintWriter(out, "UTF-8")) {
            pw.println("device,seed,phase,n_days,start,score_version,train_windows,excluded,es_windows,th_windows,epochs,best_epoch,"
                    + "train_seconds,early_stop_loss,scorer_median,scorer_iqr,s1_windows,s1_above,s1_above_rate,s2_windows,s2_above,"
                    + "s2_above_rate,inj_segments,inj_detected,spike_detected,step_detected,inj_windows,inj_above,other_windows,other_above");
            pw.println(String.join(",", dev, Long.toString(seed), phase, "init".equals(phase) ? "" : Integer.toString(nDays),
                    "init".equals(phase) ? "" : start, "init".equals(phase) ? "1" : Integer.toString(curVersion),
                    Integer.toString(trW.size()), Integer.toString(excluded), Integer.toString(esW.size()), Integer.toString(thW.size()),
                    Integer.toString(res.epochs), Integer.toString(res.bestEpoch), Double.toString(res.seconds),
                    Double.toString(res.earlyStopLoss), Double.toString(scorer.getMedian()), Double.toString(scorer.getIqr()),
                    i(s1[0]), i(s1[1]), Double.toString(s1[1] / s1[0]), i(s2[0]), i(s2[1]), Double.toString(s2[1] / s2[0]),
                    i(segs.size()), i(inj[0]), i(inj[1]), i(inj[2]), i(inj[3]), i(inj[4]), i(inj[5]), i(inj[6])));
        }
        System.out.printf("完成：耗时 %.0f 秒、%d 轮；平稳段一超阈率 %.4f；平稳段二超阈率 %.4f；注入段检出 %d/%d%n", res.seconds, res.epochs,
                s1[1] / s1[0], s2[1] / s2[0], (int) inj[0], segs.size());
    }

    private static String i(double v) {
        return Long.toString(Math.round(v));
    }

    private static double[] norm(Pt p, Std s, ChannelTransform[] tf, String dev) {
        double[] x = RobustScalerFunction.normalize(p.x, p.miss, s.median, s.scale, s.bypass, tf);
        M3Function.zeroDeviceGLight(x, dev);
        return x;
    }

    /** 按评价时段评分 / scoring over evaluation spans. */
    private static final class Eval {
        final LstmAutoEncoder model;
        final M3Scorer scorer;
        final Std std;
        final ChannelTransform[] tf;
        final String dev;
        final int window;
        final List<Pt> pts;
        final WeightedMseLoss loss = new WeightedMseLoss(N, null);

        Eval(LstmAutoEncoder model, M3Scorer scorer, Std std, ChannelTransform[] tf, String dev, int window, List<Pt> pts) {
            this.model = model;
            this.scorer = scorer;
            this.std = std;
            this.tf = tf;
            this.dev = dev;
            this.window = window;
            this.pts = pts;
        }

        /** 时段内的窗口：每个窗口为 {首轮时刻, 末轮时刻, 是否超阈} / windows of a span. */
        List<long[]> windows(long t0, long t1) {
            List<Pt> in = new ArrayList<>();
            for (Pt p : pts) {
                if (p.ts >= t0 && p.ts < t1) {
                    in.add(p);
                }
            }
            List<long[]> out = new ArrayList<>();
            for (int k = 0; (k + 1) * window <= in.size(); k++) {
                double[][] w = new double[window][];
                boolean[][] m = new boolean[window][];
                for (int t = 0; t < window; t++) {
                    w[t] = norm(in.get(k * window + t), std, tf, dev);
                    m[t] = in.get(k * window + t).mask;
                }
                WeightedMseLoss.LossResult r = loss.compute(w, model.reconstruct(w), m, window);
                boolean above = scorer.score(r.wmse, r.perChannelMse).aboveThreshold;
                out.add(new long[]{in.get(k * window).ts, in.get(k * window + window - 1).ts, above ? 1 : 0});
            }
            return out;
        }

        /** {窗口数, 超阈窗口数} / {windows, above}. */
        double[] span(long t0, long t1) {
            List<long[]> ws = windows(t0, t1);
            double above = 0;
            for (long[] w : ws) {
                above += w[2];
            }
            return new double[]{ws.size(), above};
        }

        /** {检出段数, 尖峰检出, 阶跃检出, 注入窗口数, 其中超阈, 其余窗口数, 其中超阈} / recall over injection segments. */
        double[] recall(long t0, long t1, List<long[]> segs, List<String> types) {
            List<long[]> ws = windows(t0, t1);
            double[] r = new double[7];
            boolean[] inInj = new boolean[ws.size()];
            for (int s = 0; s < segs.size(); s++) {
                boolean hit = false;
                for (int k = 0; k < ws.size(); k++) {
                    long[] w = ws.get(k);
                    if (w[1] >= segs.get(s)[0] && w[0] < segs.get(s)[1]) {
                        inInj[k] = true;
                        hit |= w[2] == 1;
                    }
                }
                if (hit) {
                    r[0]++;
                    r["spike".equals(types.get(s)) ? 1 : 2]++;
                }
            }
            for (int k = 0; k < ws.size(); k++) {
                if (inInj[k]) {
                    r[3]++;
                    r[4] += ws.get(k)[2];
                } else {
                    r[5]++;
                    r[6] += ws.get(k)[2];
                }
            }
            return r;
        }
    }
}
