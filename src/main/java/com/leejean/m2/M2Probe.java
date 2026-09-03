package com.leejean.m2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.m1.DeviceRound;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * (R, k) 校准探针（交接文档 §7）——**离线**网格扫描，复用 {@link McodCore}（不复制算法、忠实一致）。
 * The (R, k) calibration probe (handover §7): an OFFLINE grid sweep reusing {@link McodCore}
 * (no algorithm duplication), producing a per-device per-parameter outlier-rate table.
 *
 * <p>输入：{@code synergia-m1-out} 转储的标准化 DeviceRound JSONL（每行一轮）。探针按与 {@link M2Gate}
 * 相同的口径过滤（跳过 warmup、缺失掩码非空的轮），把每设备的轮序列（按 ts 升序）喂进 {@link McodCore}，
 * 对参数网格 R × k 逐组做事件时间滑动窗口模拟，统计逐设备逐参数组的离群率。
 * Input: normalized DeviceRound JSONL dumped from synergia-m1-out. Same admission as M2Gate; per device,
 * feed the ts-sorted sequence into McodCore over the R×k grid and tabulate the outlier rate.
 *
 * <p>本阶段**不定终值**——表格交回设计会话裁决（§7）。
 *
 * ---------------------------- 脚本交付五要素 -------------------------------
 * 1. 执行环境 / Environment: 任意有 JDK 的机器（master 或本地）；输入为 m1-out 转储 JSONL。
 * 2. 调用命令 / Invocation:
 *      java -cp <jar> com.leejean.m2.M2Probe --rounds-jsonl m1out.jsonl --out m2_probe.csv \
 *           --window-sec 3600 --slide-sec 60 --r-grid 0.5,1.0,1.5,2.0,2.5,3.0 --k-grid 5,10,20
 * 3. 前置条件 / Preconditions: m1out.jsonl 为标准化 DeviceRound JSON（含 device/ts/xNorm/masks/warmup）。
 * 4. 期望产出 / Expected output: CSV（device,R,k,slides,meanWindowPoints,meanOutlierRate,zeroRate）
 *      + stdout 一段通俗解读（哪些组合离群率过高、哪些恒为零）。
 * 5. 失败兜底 / Failure fallback: 无法解析的行跳过并计数；某设备无有效轮则该设备行标注 n/a。
 */
public final class M2Probe {

    private M2Probe() { }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = parseArgs(args);
        String jsonl = a.getOrDefault("rounds-jsonl", "m1out.jsonl");
        String outCsv = a.getOrDefault("out", "m2_probe.csv");
        // 可选：逐设备逐通道离散度诊断 CSV（收尾裁决 2b：P1/P99/峰度，判断发散集中于某通道还是普遍）。
        // Optional per-device per-channel dispersion CSV (closeout ruling 2b): P1/P99/kurtosis.
        String dispersionOut = a.getOrDefault("dispersion-out", "");
        long windowSec = Long.parseLong(a.getOrDefault("window-sec", "3600"));
        long slideSec = Long.parseLong(a.getOrDefault("slide-sec", "60"));
        double[] rGrid = parseDoubles(a.getOrDefault("r-grid", "0.5,1.0,1.5,2.0,2.5,3.0"));
        int[] kGrid = parseInts(a.getOrDefault("k-grid", "5,10,20"));
        // 离群率"过高"阈值，仅用于解读文字（不定终值）
        double highRate = Double.parseDouble(a.getOrDefault("high-rate", "0.2"));

        // 读入并按设备分组（跳过 warmup / 缺失掩码非空，口径同 M2Gate）
        ObjectMapper mapper = new ObjectMapper();
        Map<String, List<McodPoint>> byDevice = new TreeMap<>();
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
                } catch (Exception e) {
                    parseErrors++;
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
                double[] xNorm = r.getXNorm();
                double[] value = xNorm == null ? new double[0] : xNorm.clone();
                McodPoint p = new McodPoint(value, r.getTs() * 1000L, 0, r.getTs());
                byDevice.computeIfAbsent(r.getDevice(), d -> new ArrayList<>()).add(p);
            }
        }
        // 每设备按 arrival 升序
        for (List<McodPoint> pts : byDevice.values()) {
            pts.sort((x, y) -> Long.compare(x.arrival, y.arrival));
        }

        // 可选：逐设备逐通道离散度诊断（复用同一份标定段数据）/ optional per-channel dispersion diagnostic
        if (!dispersionOut.isEmpty()) {
            writeDispersion(byDevice, dispersionOut);
            System.out.println("[dispersion] 逐通道 P1/P99/峰度 → " + dispersionOut);
        }

        long windowMs = windowSec * 1000L;
        long slideMs = slideSec * 1000L;

        // 扫描网格并写 CSV
        List<String> highCombos = new ArrayList<>();
        List<String> zeroCombos = new ArrayList<>();
        try (PrintWriter pw = new PrintWriter(outCsv)) {
            pw.println("device,R,k,slides,meanWindowPoints,meanOutlierRate,fracZeroSlides");
            for (Map.Entry<String, List<McodPoint>> e : byDevice.entrySet()) {
                String device = e.getKey();
                List<McodPoint> pts = e.getValue();
                for (double rr : rGrid) {
                    for (int kk : kGrid) {
                        RateResult res = sweep(pts, rr, kk, windowMs, slideMs);
                        pw.printf("%s,%.2f,%d,%d,%.2f,%.6f,%.4f%n",
                                device, rr, kk, res.slides, res.meanWindowPoints,
                                res.meanOutlierRate, res.fracZeroSlides);
                        if (res.slides > 0 && res.meanOutlierRate >= highRate) {
                            highCombos.add(String.format("%s R=%.2f k=%d rate=%.3f",
                                    device, rr, kk, res.meanOutlierRate));
                        }
                        if (res.slides > 0 && res.meanOutlierRate == 0.0) {
                            zeroCombos.add(String.format("%s R=%.2f k=%d", device, rr, kk));
                        }
                    }
                }
            }
        }

        // 通俗解读（交回设计会话，不定终值）
        System.out.println("========== M2 (R,k) 校准探针 / calibration probe ==========");
        System.out.printf("输入行数 %d；跳过 warmup %d、缺失掩码 %d；解析失败 %d；有效设备 %d%n",
                total, skippedWarmup, skippedMissing, parseErrors, byDevice.size());
        System.out.println("网格：R=" + java.util.Arrays.toString(rGrid)
                + " × k=" + java.util.Arrays.toString(kGrid)
                + "；窗口 W=" + windowSec + "s / S=" + slideSec + "s。CSV → " + outCsv);
        System.out.println("--- 解读 / interpretation（阈值 highRate=" + highRate + "）---");
        System.out.println("· 离群率过高的组合（R 过小或 k 过大，几乎人人离群，无判别力）："
                + (highCombos.isEmpty() ? "无" : ""));
        for (String s : highCombos) {
            System.out.println("    - " + s);
        }
        System.out.println("· 离群率恒为零的组合（R 过大或 k 过小，无人离群，检不出异常）："
                + (zeroCombos.isEmpty() ? "无" : ""));
        for (String s : zeroCombos) {
            System.out.println("    - " + s);
        }
        System.out.println("· 结论：以上为选段实测离群率，**不在本阶段定终值**；请设计会话据此裁决 (R,k)。");
    }

    /**
     * 逐设备逐通道离散度诊断（收尾裁决 2b）：对每台设备每个标准化通道，输出第 1/第 99 百分位与超额峰度、
     * 以及分位间距 iqr99_1 = P99 − P1。目的是判断 C、D 的发散是集中在某一两个通道（标准化对该通道重尾的
     * 放大假象），还是五通道普遍（真实动态设备）——与带内设备 E 的同样统计量对照即可读出。
     * Per-device per-channel dispersion diagnostic: P1, P99, spread (P99−P1) and excess kurtosis of the
     * standardized values, so C/D's dispersion can be told apart as one-or-two-channel vs. all-channel.
     */
    private static void writeDispersion(Map<String, List<McodPoint>> byDevice, String outCsv)
            throws java.io.FileNotFoundException {
        try (PrintWriter pw = new PrintWriter(outCsv)) {
            pw.println("device,channel,n,p1,p99,spread_p99_p1,excess_kurtosis");
            for (Map.Entry<String, List<McodPoint>> e : byDevice.entrySet()) {
                String device = e.getKey();
                List<McodPoint> pts = e.getValue();
                if (pts.isEmpty()) {
                    continue;
                }
                int dims = pts.get(0).value.length;
                for (int c = 0; c < dims; c++) {
                    double[] col = new double[pts.size()];
                    for (int i = 0; i < pts.size(); i++) {
                        double[] v = pts.get(i).value;
                        col[i] = c < v.length ? v[c] : Double.NaN;
                    }
                    double p1 = nearestRankPercentile(col, 1.0);
                    double p99 = nearestRankPercentile(col, 99.0);
                    double kurt = excessKurtosis(col);
                    pw.printf("%s,%d,%d,%.6f,%.6f,%.6f,%.6f%n",
                            device, c, col.length, p1, p99, p99 - p1, kurt);
                }
            }
        }
    }

    /** 最近秩百分位（对升序数据取第 ceil(q/100×n) 个，1 基）。 */
    private static double nearestRankPercentile(double[] data, double q) {
        int n = data.length;
        if (n == 0) {
            return Double.NaN;
        }
        double[] s = data.clone();
        java.util.Arrays.sort(s);
        int rank = (int) Math.ceil(q / 100.0 * n);
        if (rank < 1) {
            rank = 1;
        }
        if (rank > n) {
            rank = n;
        }
        return s[rank - 1];
    }

    /** 超额峰度 excess kurtosis = m4/m2² − 3（m2、m4 为中心矩）；m2≈0 时返回 0。 */
    private static double excessKurtosis(double[] data) {
        int n = data.length;
        if (n < 2) {
            return 0.0;
        }
        double mean = 0;
        for (double x : data) {
            mean += x;
        }
        mean /= n;
        double m2 = 0;
        double m4 = 0;
        for (double x : data) {
            double d = x - mean;
            double d2 = d * d;
            m2 += d2;
            m4 += d2 * d2;
        }
        m2 /= n;
        m4 /= n;
        if (m2 <= 1e-12) {
            return 0.0;
        }
        return m4 / (m2 * m2) - 3.0;
    }

    /** 对一个设备的点序列跑一遍滑动窗口，返回离群率统计（复用 McodCore，忠实一致）。 */
    private static RateResult sweep(List<McodPoint> pts, double r, int k, long windowMs, long slideMs) {
        if (pts.isEmpty()) {
            return new RateResult(0, 0, 0, 0);
        }
        // 用新点副本：McodCore 会就地改写点状态，不能污染其他 (R,k) 组合
        List<McodPoint> copy = new ArrayList<>(pts.size());
        for (McodPoint p : pts) {
            copy.add(new McodPoint(p.value.clone(), p.arrival, 0, p.id));
        }
        McodCore core = new McodCore(r, k, slideMs, new McodState());
        long maxArrival = copy.get(copy.size() - 1).arrival;
        int cursor = 0;
        List<McodPoint> active = new ArrayList<>();
        long slides = 0;
        double sumRate = 0;
        long sumWindowPoints = 0;
        long zeroSlides = 0;
        for (long windowEnd = slideMs; windowEnd - windowMs <= maxArrival; windowEnd += slideMs) {
            long windowStart = windowEnd - windowMs;
            while (cursor < copy.size() && copy.get(cursor).arrival < windowEnd) {
                active.add(copy.get(cursor));
                cursor++;
            }
            List<McodPoint> window = new ArrayList<>();
            for (McodPoint p : active) {
                if (p.arrival >= windowStart && p.arrival < windowEnd) {
                    window.add(p);
                }
            }
            McodCore.McodResult res = core.processSlide(window, windowStart, windowEnd);
            if (res.windowPoints > 0) {
                double rate = (double) res.outlierIds.size() / res.windowPoints;
                sumRate += rate;
                sumWindowPoints += res.windowPoints;
                if (res.outlierIds.isEmpty()) {
                    zeroSlides++;
                }
                slides++;
            }
            active.removeIf(p -> p.arrival < windowStart + slideMs);
        }
        if (slides == 0) {
            return new RateResult(0, 0, 0, 0);
        }
        return new RateResult(slides, (double) sumWindowPoints / slides,
                sumRate / slides, (double) zeroSlides / slides);
    }

    private static final class RateResult {
        final long slides;
        final double meanWindowPoints;
        final double meanOutlierRate;
        final double fracZeroSlides;

        RateResult(long slides, double meanWindowPoints, double meanOutlierRate, double fracZeroSlides) {
            this.slides = slides;
            this.meanWindowPoints = meanWindowPoints;
            this.meanOutlierRate = meanOutlierRate;
            this.fracZeroSlides = fracZeroSlides;
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new java.util.HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            m.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        return m;
    }

    private static double[] parseDoubles(String csv) {
        String[] parts = csv.split(",");
        double[] out = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = Double.parseDouble(parts[i].trim());
        }
        return out;
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
