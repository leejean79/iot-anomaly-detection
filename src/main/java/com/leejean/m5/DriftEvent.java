package com.leejean.m5;

import java.io.Serializable;
import java.util.Locale;

/**
 * 漂移事件（M5b 交接文档决定四）：候选、确认或撤销，带触发的检验与两个子窗的统计量；确认与撤销另带核对的参照、门限与
 * 最后一次滑动中位数。写到主题 {@code synergia-drift}。
 * Drift event: candidate, confirm or revoke, with the firing test and both sub-windows' statistics; gate outcomes also
 * carry the reference, threshold and last sliding median. Written to {@code synergia-drift}.
 *
 * <p>Flink POJO：public 字段加无参构造。/ Flink POJO: public fields and a no-arg constructor.
 */
public class DriftEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Level { CANDIDATE, CONFIRM, REVOKE }

    public String device;
    /** 事件时间：元素的时间戳（评分窗末，秒）/ event time: the element's timestamp (window end, seconds). */
    public long ts;
    public Level level;
    /** "F"（方差检验）或 "T"（均值检验）；确认与撤销沿用其候选的检验 / firing test. */
    public String test;
    public int windowLen, histLen, newLen;
    public double histMean, histSd, newMean, newSd, histMedian, magnitude;
    /** 候选：发出时是否有进行中的核对（有则不打断）/ candidate raised during a running check. */
    public boolean duringCheck;
    /** 确认与撤销：所核对的候选时刻、参照、门限、最后一次滑动中位数与已核对的元素数 / gate outcome fields. */
    public long candidateTs;
    public double reference, threshold, lastMedian;
    public int checkedElements;

    public DriftEvent() { }

    static DriftEvent candidate(String device, long ts, OptWinDetector.Alarm a, boolean duringCheck) {
        DriftEvent e = base(device, ts, Level.CANDIDATE, a);
        e.duringCheck = duringCheck;
        e.candidateTs = ts;
        return e;
    }

    static DriftEvent gate(String device, long ts, Level level, OptWinDetector.Alarm a, long candidateTs,
                           double reference, double threshold, double lastMedian, int checked) {
        DriftEvent e = base(device, ts, level, a);
        e.candidateTs = candidateTs;
        e.reference = reference;
        e.threshold = threshold;
        e.lastMedian = lastMedian;
        e.checkedElements = checked;
        return e;
    }

    private static DriftEvent base(String device, long ts, Level level, OptWinDetector.Alarm a) {
        DriftEvent e = new DriftEvent();
        e.device = device;
        e.ts = ts;
        e.level = level;
        e.test = a.test;
        e.windowLen = a.windowLen;
        e.histLen = a.histLen;
        e.newLen = a.newLen;
        e.histMean = a.histMean;
        e.histSd = a.histSd;
        e.newMean = a.newMean;
        e.newSd = a.newSd;
        e.histMedian = a.histMedian;
        e.magnitude = a.magnitude();
        return e;
    }

    /** 离线等值比较用的 CSV 表头 / CSV header for offline equality checks. */
    public static final String CSV_HEADER = "device,ts,level,test,window_len,hist_len,new_len,hist_mean,hist_sd,"
            + "new_mean,new_sd,hist_median,magnitude,during_check,candidate_ts,reference,threshold,last_median,checked";

    /** CSV 行：浮点数用 Java 的 Double.toString（可逐位还原）/ CSV row with round-trippable doubles. */
    public String toCsv() {
        return String.join(",", device, Long.toString(ts), level.name(), test, Integer.toString(windowLen),
                Integer.toString(histLen), Integer.toString(newLen), d(histMean), d(histSd), d(newMean), d(newSd),
                d(histMedian), d(magnitude), duringCheck ? "1" : "0", Long.toString(candidateTs), d(reference),
                d(threshold), d(lastMedian), Integer.toString(checkedElements));
    }

    private static String d(double v) {
        return Double.toString(v);
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "%s %d %s(%s) hist=%.3f±%.3f/%d new=%.3f±%.3f/%d",
                device, ts, level, test, histMean, histSd, histLen, newMean, newSd, newLen);
    }
}
