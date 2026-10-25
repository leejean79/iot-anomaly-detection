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

    /**
     * 性能层：CANDIDATE、CONFIRM、REVOKE。数据层（M5a，开发设计日志 1.09 版）：BURST（离群率爆发，候选即事件）、DEVIATE（长记忆
     * 进入偏离）、RECOVER（长记忆回落）。/ Performance-layer levels plus the data-layer BURST, DEVIATE and RECOVER.
     */
    public enum Level { CANDIDATE, CONFIRM, REVOKE, BURST, DEVIATE, RECOVER }

    /** 层：performance 或 data（M5a 交接文档决定六）/ layer marker. */
    public String layer = "performance";
    /** 通道：性能层为 main_score；数据层为检测通道名或 outlier_rate / channel. */
    public String channel = "main_score";

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
    /** 数据层爆发：报警元素起一小时内的离群轮数、离群率峰值、连续非零元素数 / data-layer burst fields. */
    public long burstSize;
    public double burstPeak;
    public int burstDuration;
    /** 数据层长记忆：当地日、分箱 D、渐近显著性、偏移、宽度倍数 / data-layer long-memory fields. */
    public String dataDay;
    public double dataD, dataP, dataShift, dataWidthX;

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

    /** 数据层离群率爆发 / data-layer outlier-rate burst. */
    public static DriftEvent burst(String device, long ts, OptWinDetector.Alarm a, long size, double peak, int duration) {
        DriftEvent e = base(device, ts, Level.BURST, a);
        e.layer = "data";
        e.channel = "outlier_rate";
        e.candidateTs = ts;
        e.burstSize = size;
        e.burstPeak = peak;
        e.burstDuration = duration;
        return e;
    }

    /** 数据层长记忆的状态转移（DEVIATE 或 RECOVER）；ts 为当地日结束的时刻 / data-layer long-memory transition. */
    public static DriftEvent dataState(String device, long ts, Level level, String channel, String day, double d, double p,
                                       double shift, double widthX) {
        DriftEvent e = new DriftEvent();
        e.device = device;
        e.ts = ts;
        e.level = level;
        e.layer = "data";
        e.channel = channel;
        e.test = "KS";
        e.dataDay = day;
        e.dataD = d;
        e.dataP = p;
        e.dataShift = shift;
        e.dataWidthX = widthX;
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

    /** 数据层事件的 CSV 表头 / CSV header of data-layer events. */
    public static final String DATA_CSV_HEADER = "device,ts,level,channel,test,magnitude,burst_size,burst_peak,burst_duration,"
            + "data_day,data_d,data_p,data_shift,data_width_x";

    /** 数据层事件的 CSV 行 / CSV row of a data-layer event. */
    public String toDataCsv() {
        return String.join(",", device, Long.toString(ts), level.name(), channel, test == null ? "" : test, d(magnitude),
                Long.toString(burstSize), d(burstPeak), Integer.toString(burstDuration), dataDay == null ? "" : dataDay,
                d(dataD), d(dataP), d(dataShift), d(dataWidthX));
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
