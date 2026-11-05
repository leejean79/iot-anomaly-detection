package com.leejean.m6;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.leejean.m1.AdaptiveScalerFunction;
import com.leejean.m1.ChannelTransform;
import com.leejean.m1.Channels;
import com.leejean.m1.DeviceRound;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m1.RobustScalerFunction;
import com.leejean.m2.DevicePoint;
import com.leejean.m2.McodPoint;
import com.leejean.m2.McodState;
import com.leejean.m2.PmcodFunction;
import com.leejean.m5.DriftEvent;
import com.leejean.m5a.M5aCore;
import com.leejean.m5a.RefHistogram;

import java.io.File;
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
import java.util.TreeMap;

/**
 * 适应执行第一批的离线模拟器（M6a 交接文档第五节）：对一台设备的轮表，逐轮重演在线的标准化（冻结、零级）、点通道（PmcodFunction 的同一个
 * 滑动步函数，含换版本时的清空与回填）与数据层（M5aCore），有命令与无命令各跑一次，输出逐日的数据层记录、逐小时的点通道离群率与零级的
 * 新参数，供 m6a/sim_report.py 写离线模拟报告。所用函数全部取自在线算子，模拟器只负责按事件时间顺序调用它们。
 * The M6a offline simulator: replays the scaler, the point channel and the data layer with the online functions.
 *
 * <p>与在线的对应：点通道的每个窗口恰好含到达时刻落在 [窗末 − 窗长, 窗末) 的点（允许迟到为零、迟到丢弃为零，断言六），按窗末升序处理，
 * 只处理非空窗口，最后一个窗末不超过最后一个点的到达时刻；离群记录取到达滑动步的点。数据层只喂轮（第一、二族），不涉及离群率爆发。
 *
 * <p>脚本交付五要素：
 * <ol>
 *   <li>执行环境：开发机或本地 Mac，仓库根目录，JDK 11，先 mvn package（或用 target/classes 与依赖类路径）。</li>
 *   <li>调用命令：{@code java -cp <类路径> com.leejean.m6.M6aSimulator --rounds docs/m6a/extract/rounds_G.csv.gz --device G
 *       --r 1.5 --out-dir docs/m6a/sim --tag G_off}（无命令）；加 {@code --day 2022-03-25 [--reestimate-days 1]} 为有命令；
 *       可选 --exclude-outliers <文件>（每行一个轮时间戳：重估时额外剔除这些轮，用于判读口径第四条）、--k 10、--window-sec 3600、
 *       --slide-sec 60、--warmup-rounds 60480、--seq 1、--start <秒>、--end <秒>（短重放：只用这一区间内的轮）。</li>
 *   <li>前置条件：轮表已由 eda/m6a_extract.py 生成。</li>
 *   <li>期望产出：out-dir 下 {tag}_days.csv（逐日逐通道的数据层记录）、{tag}_events.csv（偏离与回落）、{tag}_hours.csv（逐小时入口
 *       轮数与离群轮数）、{tag}_outliers.txt（到达滑动步的离群轮时间戳）、{tag}_level0.json（有命令时的零级结果）。</li>
 *   <li>失败兜底：轮表不足以冻结时退出码 2。</li>
 * </ol>
 */
public final class M6aSimulator {
    private M6aSimulator() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            a.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        String dev = a.get("device");
        ChannelTransform[] tf = ChannelTransform.defaultTable();
        Long eff = a.containsKey("effective") ? Long.valueOf(a.get("effective"))
                : a.containsKey("day") ? LocalDate.parse(a.get("day")).atStartOfDay(ZoneId.of("Europe/London")).toEpochSecond() : null;
        int seq = Integer.parseInt(a.getOrDefault("seq", "1"));
        long dSec = Long.parseLong(a.getOrDefault("reestimate-days", "1")) * 86400L;
        long windowSec = Long.parseLong(a.getOrDefault("window-sec", "3600"));
        long slideSec = Long.parseLong(a.getOrDefault("slide-sec", "60"));
        int warmup = Integer.parseInt(a.getOrDefault("warmup-rounds", "60480"));
        double eps = Double.parseDouble(a.getOrDefault("iqr-epsilon", "1e-9"));
        double r = Double.parseDouble(a.getOrDefault("r", "1.0"));
        int k = Integer.parseInt(a.getOrDefault("k", "10"));
        Set<Long> exclude = new HashSet<>();
        if (a.containsKey("exclude-outliers")) {
            for (String l : Files.readAllLines(Paths.get(a.get("exclude-outliers")))) {
                if (!l.trim().isEmpty()) {
                    exclude.add(Long.parseLong(l.trim()));
                }
            }
        }
        String outDir = a.getOrDefault("out-dir", "docs/m6a/sim");
        String tag = a.get("tag");
        new File(outDir).mkdirs();
        // --start：短重放从区间起点起算（轮表里更早的轮不进入）/ short replays start at the window start
        List<M6aOfflineRunner.Row> rows = M6aOfflineRunner.read(a.get("rounds"), a.containsKey("start") ? Long.valueOf(a.get("start")) : null);
        if (a.containsKey("start") && !rows.isEmpty()) {
            rows.get(0).cold = false;                       // 在线的第一轮前面没有更早的轮，不是冷启动 / no earlier round online
        }
        if (a.containsKey("end")) {                         // --end：只用不晚于此刻的轮（短重放的最后一轮）/ last round
            long end = Long.parseLong(a.get("end"));
            rows.removeIf(w -> w.ts > end);
        }
        if (rows.size() < warmup) {
            System.err.println("ERROR: 只有 " + rows.size() + " 轮，不足以冻结");
            System.exit(2);
        }
        M5aCore.Params m5p = new M5aCore.Params(new double[]{0.38, 0.73, 0.84, 0.43, 0.51}, 0.01, 0.2, 0.2, 10, (int) slideSec);

        // ---------------- 第一遍：标准化与数据层，按轮 / pass 1: scaler and data layer
        CompactCache cache = new CompactCache(7 * 86400L);
        List<List<Double>> res = new ArrayList<>();
        for (int c = 0; c < Channels.N_DET; c++) {
            res.add(new ArrayList<>());
        }
        RobustScalerFunction.Calibration cal = null;
        RefHistogram[] refsPending = null;
        Backfill bfPending = null;
        Integer version = null;
        boolean pending = eff != null;
        AdaptiveScalerFunction.Level0 l0 = null;
        M5aCore core = new M5aCore();
        List<DriftEvent> events = new ArrayList<>();
        List<MonitoringSnapshot> records = new ArrayList<>();
        List<DevicePoint> points = new ArrayList<>();
        int count = 0;
        // --dump-rounds：逐轮导出冻结之后的版本号与标准化值，供短重放与在线 m1-out 逐位比较 / per-round dump for the short replay
        PrintWriter dump = a.containsKey("dump-rounds") ? new PrintWriter(new File(a.get("dump-rounds")), "UTF-8") : null;
        if (dump != null) {
            dump.println("ts,version,x0,x1,x2,x3,x4,refs,backfill");
        }
        for (M6aOfflineRunner.Row w : rows) {
            boolean[] cens = new boolean[Channels.N_DET];
            cens[Channels.LIGHT_INDEX] = !w.missing[Channels.LIGHT_INDEX] && w.x[Channels.LIGHT_INDEX] == 65536.0;
            boolean[] stuck = new boolean[Channels.N_DET];
            stuck[0] = w.stuck;
            if (pending && cal != null && w.ts >= eff) {
                l0 = AdaptiveScalerFunction.level0(cache, eff, dSec, windowSec, eps, false, tf);
                cal = l0.cal;
                version = seq + 1;
                refsPending = l0.cal.hist;
                bfPending = l0.backfill;
                pending = false;
            }
            boolean[] guardForCache = stuck.clone();
            guardForCache[1] = exclude.contains(w.ts);          // 只影响重估取样（判读口径第四条）/ re-estimation only
            cache.add(w.ts, w.x, CompactCache.maskOf(w.missing, cens, guardForCache));
            DeviceRound d = new DeviceRound();
            d.setDevice(dev);
            d.setTs(w.ts);
            d.setX(w.x.clone());
            d.setMissingMask(w.missing.clone());
            d.setCensoredMask(cens);
            d.setStuckMask(stuck);
            boolean missingAny = d.missingCount() > 0;
            if (cal == null) {
                for (int c = 0; c < Channels.N_DET; c++) {
                    if (!w.missing[c] && !(c == Channels.LIGHT_INDEX && cens[c])) {
                        res.get(c).add(tf[c].apply(w.x[c]));
                    }
                }
                d.setWarmup(true);
                if (++count == warmup) {
                    cal = RobustScalerFunction.calibrate(res, eps, false);
                    refsPending = cal.hist;
                    res = null;
                    if (eff != null) {
                        version = 1;
                    }
                }
            } else {
                double[] xn = RobustScalerFunction.normalize(w.x, w.missing, cal.median, cal.scale, cal.bypass, tf);
                d.setXNorm(xn);
                d.setVersion(version);
                if (refsPending != null) {
                    d.setRefHistograms(refsPending);
                    refsPending = null;
                }
                if (bfPending != null && !missingAny) {
                    d.setBackfill(bfPending);
                    bfPending = null;
                }
                if (dump != null) {
                    StringBuilder line = new StringBuilder().append(w.ts).append(',').append(version == null ? "" : version.toString());
                    for (double v : xn) {
                        line.append(',').append(v);
                    }
                    dump.println(line.append(',').append(d.getRefHistograms() != null ? 1 : 0).append(',')
                            .append(d.getBackfill() != null ? d.getBackfill().size() : 0));
                }
                if (!missingAny) {
                    DevicePoint p = new DevicePoint(dev, new McodPoint(xn.clone(), w.ts * 1000L, 0, w.ts, w.cold), cens.clone());
                    p.setVersion(d.getVersion());
                    p.setBackfill(d.getBackfill());
                    points.add(p);
                }
            }
            core.onRound(d, m5p, events, records);
        }
        core.closeDay(dev, m5p, events, records);
        if (dump != null) {
            dump.close();
        }

        // ---------------- 第二遍：点通道，按窗末 / pass 2: the point channel, by window end
        long wMs = windowSec * 1000L;
        long sMs = slideSec * 1000L;
        McodState st = new McodState();
        TreeMap<Long, long[]> hours = new TreeMap<>();
        for (DevicePoint p : points) {
            hours.computeIfAbsent(Math.floorDiv(p.getPoint().id, 3600L) * 3600L, x -> new long[2])[0]++;
        }
        List<Long> outliers = new ArrayList<>();
        long last = points.get(points.size() - 1).getPoint().arrival;
        int lo = 0;
        int hi = 0;
        long firstEnd = (Math.floorDiv(points.get(0).getPoint().arrival, sMs) + 1) * sMs;
        for (long end = firstEnd; end <= last; end += sMs) {
            while (hi < points.size() && points.get(hi).getPoint().arrival < end) {
                hi++;
            }
            while (lo < hi && points.get(lo).getPoint().arrival < end - wMs) {
                lo++;
            }
            if (lo == hi) {
                continue;                                     // 空窗口不触发 / empty windows do not fire
            }
            PmcodFunction.Slide sl = PmcodFunction.slide(st, new ArrayList<>(points.subList(lo, hi)), end - wMs, end, sMs, r, k, false);
            long endSec = end / 1000L;
            for (long id : sl.emitted) {
                if (endSec - slideSec <= id && id < endSec) {
                    outliers.add(id);
                    hours.computeIfAbsent(Math.floorDiv(id, 3600L) * 3600L, x -> new long[2])[1]++;
                }
            }
        }

        // ---------------- 输出 / outputs
        try (PrintWriter pw = new PrintWriter(new File(outDir, tag + "_days.csv"), "UTF-8")) {
            pw.println("day,channel,rounds,expected,guard_rounds,evaluable,reason,n,d,p,p10,median,p90,shift,deviated,version");
            for (MonitoringSnapshot s : records) {
                pw.println(String.join(",", s.getM5aDay(), s.getM5aChannel(), Long.toString(s.getM5aRounds()),
                        Long.toString(s.getM5aExpected()), Long.toString(s.getM5aGuardRounds()), s.isM5aEvaluable() ? "1" : "0",
                        s.getM5aReason(), Long.toString(s.getM5aN()), Double.toString(s.getM5aD()), Double.toString(s.getM5aP()),
                        Double.toString(s.getM5aP10()), Double.toString(s.getM5aMedian()), Double.toString(s.getM5aP90()),
                        Double.toString(s.getM5aShift()), s.isM5aDeviated() ? "1" : "0",
                        s.getVersion() == null ? "" : Integer.toString(s.getVersion())));
            }
        }
        try (PrintWriter pw = new PrintWriter(new File(outDir, tag + "_events.csv"), "UTF-8")) {
            pw.println("day,channel,level,ts");
            for (DriftEvent e : events) {
                pw.println(e.dataDay + "," + e.channel + "," + e.level + "," + e.ts);
            }
        }
        try (PrintWriter pw = new PrintWriter(new File(outDir, tag + "_hours.csv"), "UTF-8")) {
            pw.println("hour_start,admitted,outliers");
            for (Map.Entry<Long, long[]> e : hours.entrySet()) {
                pw.println(e.getKey() + "," + e.getValue()[0] + "," + e.getValue()[1]);
            }
        }
        StringBuilder sb = new StringBuilder();
        for (long id : outliers) {
            sb.append(id).append('\n');
        }
        Files.write(Paths.get(outDir, tag + "_outliers.txt"), sb.toString().getBytes(StandardCharsets.UTF_8));
        if (l0 != null) {
            ObjectMapper om = new ObjectMapper();
            ObjectNode o = om.createObjectNode();
            o.put("device", dev);
            o.put("effectiveAt", eff);
            o.put("reestimateSec", dSec);
            o.put("windowRounds", l0.windowRounds);
            o.put("guardExcluded", l0.guardExcluded);
            o.put("backfillRounds", l0.backfill.size());
            o.putPOJO("median", l0.cal.median);
            o.putPOJO("scale", l0.cal.scale);
            o.putPOJO("bypass", l0.cal.bypass);
            om.writeValue(new File(outDir, tag + "_level0.json"), o);
        }
        System.out.println(tag + "：轮 " + rows.size() + "，点 " + points.size() + "，离群 " + outliers.size() + "，日记录 " + records.size()
                + "，数据层事件 " + events.size() + (l0 == null ? "" : "，零级窗口 " + l0.windowRounds + " 轮，剔除 " + l0.guardExcluded + " 轮"));
    }
}
