package com.leejean.m6;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.leejean.m1.AdaptiveScalerFunction;
import com.leejean.m1.ChannelTransform;
import com.leejean.m1.Channels;
import com.leejean.m1.RobustScalerFunction;
import com.leejean.m5a.RefHistogram;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * 零级重估的 Java 离线运行器（M6a 交接文档第五节）：读 eda/m6a_extract.py 的轮表，用在线算子的同一个标定函数复现冻结，再把生效时刻
 * 之前的轮放进紧凑缓存，调用在线算子的零级纯函数 {@link AdaptiveScalerFunction#level0}，结果写成与 m6a/reestimate.py 同结构的 JSON，
 * 由 m6a/compare.py 逐值比较。
 * Java offline runner of the level-0 re-estimation, sharing the online operator's functions.
 *
 * <p>脚本交付五要素：
 * <ol>
 *   <li>执行环境：开发机或本地 Mac，仓库根目录，JDK 11，先 mvn package。</li>
 *   <li>调用命令：{@code java -cp target/iot-anomaly-detection-1.0-SNAPSHOT.jar com.leejean.m6.M6aOfflineRunner --rounds
 *       docs/m6a/extract/rounds_G.csv.gz --device G --day 2022-03-25 --out docs/m6a/sim/G_java.json}；可选 --effective、--seq、
 *       --reestimate-days、--backfill-sec、--warmup-rounds、--iqr-epsilon、--start，含义与 Python 相同。</li>
 *   <li>前置条件：轮表已生成。</li>
 *   <li>期望产出：out 处的 JSON；终端打印窗口轮数与回填轮数。</li>
 *   <li>失败兜底：轮表不足以冻结或生效时刻不晚于冻结时，退出码 2。</li>
 * </ol>
 */
public final class M6aOfflineRunner {
    private M6aOfflineRunner() {
    }

    /** 轮表的一行 / one row of the round table. */
    static final class Row {
        long ts;
        double[] x = new double[Channels.N_DET];
        boolean[] missing = new boolean[Channels.N_DET];
        boolean stuck;
        boolean cold;
    }

    static List<Row> read(String path, Long start) throws IOException {
        List<Row> rows = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(path)),
                StandardCharsets.UTF_8))) {
            String line = r.readLine();                       // 表头 / header
            while ((line = r.readLine()) != null) {
                String[] f = line.split(",", -1);
                Row w = new Row();
                w.ts = Long.parseLong(f[0]);
                if (start != null && w.ts < start) {
                    continue;
                }
                for (int c = 0; c < Channels.N_DET; c++) {
                    w.missing[c] = f[1 + c].isEmpty();
                    w.x[c] = w.missing[c] ? 0.0 : Double.parseDouble(f[1 + c]);
                }
                w.stuck = "1".equals(f[6]);
                w.cold = f.length > 7 && "1".equals(f[7]);
                rows.add(w);
            }
        }
        return rows;
    }

    private static boolean usable(Row w, int c) {
        return !w.missing[c] && !(c == Channels.LIGHT_INDEX && w.x[c] == 65536.0);
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            a.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        ChannelTransform[] tf = ChannelTransform.defaultTable();
        long eff = a.containsKey("effective") ? Long.parseLong(a.get("effective"))
                : LocalDate.parse(a.get("day")).atStartOfDay(ZoneId.of("Europe/London")).toEpochSecond();
        int seq = Integer.parseInt(a.getOrDefault("seq", "1"));
        long dSec = Long.parseLong(a.getOrDefault("reestimate-days", "1")) * 86400L;
        long bfSec = Long.parseLong(a.getOrDefault("backfill-sec", "3600"));
        int warmup = Integer.parseInt(a.getOrDefault("warmup-rounds", "60480"));
        double eps = Double.parseDouble(a.getOrDefault("iqr-epsilon", "1e-9"));
        List<Row> rows = read(a.get("rounds"), a.containsKey("start") ? Long.parseLong(a.get("start")) : null);
        if (rows.size() < warmup) {
            System.err.println("ERROR: 只有 " + rows.size() + " 轮，不足以冻结");
            System.exit(2);
        }
        List<List<Double>> samples = new ArrayList<>();
        for (int c = 0; c < Channels.N_DET; c++) {
            samples.add(new ArrayList<>());
        }
        for (Row w : rows.subList(0, warmup)) {
            for (int c = 0; c < Channels.N_DET; c++) {
                if (usable(w, c)) {
                    samples.get(c).add(tf[c].apply(w.x[c]));
                }
            }
        }
        RobustScalerFunction.Calibration frz = RobustScalerFunction.calibrate(samples, eps, false);
        long tFreeze = rows.get(warmup - 1).ts;
        if (eff <= tFreeze) {
            System.err.println("ERROR: 生效时刻 " + eff + " 不晚于冻结时刻 " + tFreeze);
            System.exit(2);
        }
        CompactCache cache = new CompactCache(7 * 86400L);
        for (Row w : rows) {
            if (w.ts >= eff) {
                break;
            }
            boolean[] cens = new boolean[Channels.N_DET];
            cens[Channels.LIGHT_INDEX] = !w.missing[Channels.LIGHT_INDEX] && w.x[Channels.LIGHT_INDEX] == 65536.0;
            boolean[] guard = new boolean[Channels.N_DET];
            guard[0] = w.stuck;
            cache.add(w.ts, w.x, CompactCache.maskOf(w.missing, cens, guard));
        }
        AdaptiveScalerFunction.Level0 l0 = AdaptiveScalerFunction.level0(cache, eff, dSec, bfSec, eps, false, tf);

        ObjectMapper om = new ObjectMapper();
        ObjectNode o = om.createObjectNode();
        o.put("device", a.get("device"));
        o.put("effectiveAt", eff);
        o.put("seq", seq);
        o.put("version", seq + 1);
        o.put("freezeTs", tFreeze);
        o.put("reestimateSec", dSec);
        o.put("backfillSec", bfSec);
        o.put("windowRounds", l0.windowRounds);
        o.put("guardExcluded", l0.guardExcluded);
        o.set("freeze", params(om, frz));
        o.set("new", params(om, l0.cal));
        ArrayNode hist = o.putArray("hist");
        for (RefHistogram h : l0.cal.hist) {
            if (h == null) {
                hist.addNull();
                continue;
            }
            ObjectNode n = hist.addObject();
            ArrayNode e = n.putArray("edges");
            for (double v : h.getEdges()) {
                e.add(v);
            }
            ArrayNode cl = n.putArray("cells");
            for (long v : h.getCells()) {
                cl.add(v);
            }
            n.put("m", h.getM());
            n.put("p10", h.getP10());
            n.put("median", h.getMedian());
            n.put("p90", h.getP90());
        }
        ObjectNode bf = o.putObject("backfill");
        ArrayNode bts = bf.putArray("ts");
        ArrayNode bx = bf.putArray("xNorm");
        for (int i = 0; i < l0.backfill.size(); i++) {
            bts.add(l0.backfill.ts[i]);
            ArrayNode r = bx.addArray();
            for (double v : l0.backfill.xNorm[i]) {
                r.add(v);
            }
        }
        File out = new File(a.get("out"));
        if (out.getParentFile() != null) {
            out.getParentFile().mkdirs();
        }
        om.writeValue(out, o);
        System.out.println(a.get("device") + " 生效 " + eff + "：窗口 " + l0.windowRounds + " 轮，剔除守卫 " + l0.guardExcluded
                + " 轮，回填 " + l0.backfill.size() + " 轮");
    }

    private static ObjectNode params(ObjectMapper om, RobustScalerFunction.Calibration c) {
        ObjectNode n = om.createObjectNode();
        ArrayNode med = n.putArray("median");
        ArrayNode sc = n.putArray("scale");
        ArrayNode bp = n.putArray("bypass");
        for (int i = 0; i < Channels.N_DET; i++) {
            med.add(c.median[i]);
            sc.add(c.scale[i]);
            bp.add(c.bypass[i]);
        }
        return n;
    }
}
