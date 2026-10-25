package com.leejean.m5a;

import com.leejean.m1.ChannelTransform;
import com.leejean.m1.Channels;
import com.leejean.m1.DeviceRound;
import com.leejean.m1.MonitoringSnapshot;
import com.leejean.m5.DriftEvent;
import com.leejean.m5.OptWinDetector;
import com.leejean.m5.OptWinTables;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 数据层漂移监测一台设备的全部状态与逻辑（M5a 交接文档决定一至六；离线操作口径第二、三节与第十节修订；开发设计日志 1.09 版）。
 * 在线算子 {@link M5aFunction} 与 Java 离线运行器 {@link M5aOfflineRunner} 共用这一份实现，两边因此逐位相同。
 * All per-device state and logic of the data-layer monitor, shared by the online operator and the offline runner.
 *
 * <p>第一、二族（按布里斯托当地日）：冻结后的第一条轮带来参照直方图；之后每个当地日收集五个通道的读数（变换域：光照取 log1p；
 * 缺失与光照删失不收），日界处算分箱 D、渐近显著性与 P10、中位数、P90、偏移、宽度倍数，写一条日记录；质量门控按缺轮比例与守卫
 * 打标比例；可判日上按状态转移发出 DEVIATE（p &lt; α/5 且 D &gt; D 零，原态在范围内）与 RECOVER（D ≤ D 零，原态已偏离）。参照所在的
 * 当地日记为标定日（不可判）。
 * Families 1 and 2 per local day: daily statistics, quality gating, and DEVIATE/RECOVER state transitions.
 *
 * <p>第三族（离群率爆发）：按世界协调时整十分钟计入点通道入口的轮数（非预热且五通道不缺，与 {@code M2Gate} 相同）与离群轮数
 * （只取到达滑动步的判定：窗口末 − 滑动步 ≤ 轮时刻 &lt; 窗口末）；十分钟关闭后（事件时间过了十分钟末加一个滑动步）把离群率送入
 * 最优窗口检测器，每次报警即一次爆发（不经持续性核对）。爆发在其后一小时的元素都到齐、且包含报警元素的连续非零段结束之后发出，
 * 带大小（报警元素起一小时内的离群轮数）、峰值与持续长度。
 * Family 3: outlier-rate bursts; every detector alarm is a burst, emitted once its one-hour window and run are complete.
 */
public class M5aCore implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 日记录的 windowEnd 标记（性能层快照 −2，训练态 −1）/ windowEnd marker of daily records. */
    public static final long DAY_RECORD_WINDOW_END = -3L;
    public static final ZoneId ZONE = ZoneId.of("Europe/London");
    private static final int BIN_SEC = 600;
    private static final int BURST_HORIZON_SEC = 3600;

    /** 参数（不可变）/ parameters. */
    public static final class Params implements Serializable {
        private static final long serialVersionUID = 1L;
        public final double[] d0;
        public final double alphaCorrected;
        public final double missingMax;
        public final double guardMax;
        public final int periodSec;
        public final int slideSec;

        public Params(double[] d0, double alpha, double missingMax, double guardMax, int periodSec, int slideSec) {
            this.d0 = d0.clone();
            this.alphaCorrected = alpha / Channels.N_DET;
            this.missingMax = missingMax;
            this.guardMax = guardMax;
            this.periodSec = periodSec;
            this.slideSec = slideSec;
        }
    }

    // ---- 第一、二族 / families 1 and 2
    private RefHistogram[] ref;
    private String calibDay;
    private String day;
    private long dayEnd;
    private int dayRounds;
    private int dayGuard;
    private double[][] buf = new double[Channels.N_DET][];
    private int[] bufN = new int[Channels.N_DET];
    private final boolean[] deviated = new boolean[Channels.N_DET];
    // ---- 第三族 / family 3
    private final TreeMap<Long, long[]> bins = new TreeMap<>();    // 十分钟末 → {入口轮数, 离群轮数}
    private OptWinDetector detector;
    private int lastRunLen;
    private final List<Pending> pending = new ArrayList<>();

    private static final class Pending implements Serializable {
        private static final long serialVersionUID = 1L;
        long ts;
        OptWinDetector.Alarm alarm;
        long size;
        double peak;
        int duration;
        boolean runOpen;
    }

    // ================================================================ 轮 / rounds
    /**
     * 处理一条标准化轮。若它属于新的当地日，先关闭前一日（写日记录与状态转移）。返回该轮所在当地日的结束时刻（秒），供在线算子登记定时器。
     * Process one round; closes the previous local day first if the round starts a new one. Returns the day end (s).
     */
    public long onRound(DeviceRound r, Params p, List<DriftEvent> events, List<MonitoringSnapshot> records) {
        String d = localDay(r.getTs());
        if (day != null && !d.equals(day)) {
            closeDay(r.getDevice(), p, events, records);
        }
        if (day == null) {
            day = d;
            dayEnd = dayEnd(d);
            dayRounds = 0;
            dayGuard = 0;
            Arrays.fill(bufN, 0);
        }
        if (r.getRefHistograms() != null && ref == null) {
            ref = r.getRefHistograms();
            calibDay = d;
        }
        dayRounds++;
        if (any(r.getStuckMask())) {
            dayGuard++;
        }
        if (ref != null && !r.isWarmup()) {
            boolean[] miss = r.getMissingMask();
            boolean[] cens = r.getCensoredMask();
            ChannelTransform[] tf = ChannelTransform.defaultTable();
            for (int c = 0; c < Channels.N_DET; c++) {
                if ((miss != null && miss[c]) || (c == Channels.LIGHT_INDEX && cens != null && cens[c])) {
                    continue;
                }
                append(c, tf[c].apply(r.getX()[c]));
            }
            if (r.missingCount() == 0) {                     // 点通道入口判据（M2Gate）/ the M2 entry gate
                long b = Math.floorDiv(r.getTs(), (long) BIN_SEC) * BIN_SEC + BIN_SEC;
                bins.computeIfAbsent(b, k -> new long[2])[0]++;
            }
        }
        return dayEnd;
    }

    /** 当前日的结束时刻（秒）；没有打开的日时为 0 / end of the open day, or 0. */
    public long openDayEnd() {
        return day == null ? 0L : dayEnd;
    }

    /** 关闭当前日（在线由日界定时器触发；离线在下一日的第一条轮或输入结束时）/ close the open day. */
    public void closeDay(String device, Params p, List<DriftEvent> events, List<MonitoringSnapshot> records) {
        if (day == null) {
            return;
        }
        if (calibDay != null) {
            long expected = (dayEnd - dayEnd(previousDay(day))) / p.periodSec;
            double missFrac = 1.0 - (double) dayRounds / expected;
            double guardFrac = (double) dayGuard / expected;
            String reason = day.equals(calibDay) ? "标定日"
                    : (missFrac > p.missingMax ? "缺轮" : (guardFrac > p.guardMax ? "守卫打标" : ""));
            boolean evaluable = reason.isEmpty();
            for (int c = 0; c < Channels.N_DET; c++) {
                MonitoringSnapshot s = new MonitoringSnapshot();
                s.setDevice(device);
                s.setTs(dayEnd);
                s.setWindowEnd(DAY_RECORD_WINDOW_END);
                s.setM5aDay(day);
                s.setM5aChannel(Channels.DETECTION[c]);
                s.setM5aRounds(dayRounds);
                s.setM5aExpected(expected);
                s.setM5aGuardRounds(dayGuard);
                s.setM5aEvaluable(evaluable);
                s.setM5aReason(reason);
                DayStats st = null;
                if (!day.equals(calibDay) && bufN[c] > 0 && ref[c] != null) {
                    st = DayStats.of(Arrays.copyOf(buf[c], bufN[c]), ref[c]);
                    s.setM5aN(st.n);
                    s.setM5aD(st.dBin);
                    s.setM5aP(st.p);
                    s.setM5aP10(st.p10);
                    s.setM5aMedian(st.median);
                    s.setM5aP90(st.p90);
                    s.setM5aShift(st.shift);
                    s.setM5aWidthX(st.widthX);
                    s.setM5aMaxCellMass(st.maxCellMass);
                }
                if (evaluable && st != null) {
                    if (!deviated[c] && st.p < p.alphaCorrected && st.dBin > p.d0[c]) {
                        deviated[c] = true;
                        events.add(DriftEvent.dataState(device, dayEnd, DriftEvent.Level.DEVIATE, Channels.DETECTION[c], day,
                                st.dBin, st.p, st.shift, st.widthX));
                    } else if (deviated[c] && st.dBin <= p.d0[c]) {
                        deviated[c] = false;
                        events.add(DriftEvent.dataState(device, dayEnd, DriftEvent.Level.RECOVER, Channels.DETECTION[c], day,
                                st.dBin, st.p, st.shift, st.widthX));
                    }
                }
                s.setM5aDeviated(deviated[c]);
                records.add(s);
            }
        }
        day = null;
        buf = new double[Channels.N_DET][];
        Arrays.fill(bufN, 0);
    }

    // ================================================================ 离群率 / outlier rate
    /** 一条点通道离群记录；只取到达滑动步的判定 / one point-channel outlier record (arrival slide only). */
    public void onOutlier(long roundTs, long windowEnd, Params p) {
        if (!(windowEnd - p.slideSec <= roundTs && roundTs < windowEnd)) {
            return;
        }
        long b = Math.floorDiv(roundTs, (long) BIN_SEC) * BIN_SEC + BIN_SEC;
        bins.computeIfAbsent(b, k -> new long[2])[1]++;
    }

    /** 十分钟末 b 的关闭时刻（秒）：过了 b 再加一个滑动步，离群记录都已到达 / when bin b may close. */
    public static long binCloseTime(long binEnd, Params p) {
        return binEnd + p.slideSec;
    }

    /** 关闭所有关闭时刻不晚于 t 的十分钟 / close every bin whose close time is ≤ t. */
    public void closeBinsUpTo(long t, String device, Params p, OptWinTables tables, List<DriftEvent> events) {
        Iterator<Map.Entry<Long, long[]>> it = bins.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, long[]> e = it.next();
            if (binCloseTime(e.getKey(), p) > t) {
                break;
            }
            it.remove();
            long admitted = e.getValue()[0];
            if (admitted > 0) {
                element(device, e.getKey(), e.getValue()[1], (double) e.getValue()[1] / admitted, tables, events);
            }
        }
    }

    /** 最早的未关闭十分钟末；没有时为 0 / earliest open bin end, or 0. */
    public long earliestOpenBin() {
        return bins.isEmpty() ? 0L : bins.firstKey();
    }

    private void element(String device, long b, long outliers, double rate, OptWinTables tables, List<DriftEvent> events) {
        // 先用这个元素更新尚未发出的爆发，再判断它们能否发出 / update pending bursts, then emit the complete ones
        Iterator<Pending> it = pending.iterator();
        while (it.hasNext()) {
            Pending q = it.next();
            if (b <= q.ts + BURST_HORIZON_SEC) {
                q.size += outliers;
                q.peak = Math.max(q.peak, rate);
            }
            if (q.runOpen) {
                if (rate > 0) {
                    q.duration++;
                } else {
                    q.runOpen = false;
                }
            }
            if (b > q.ts + BURST_HORIZON_SEC && !q.runOpen) {
                events.add(DriftEvent.burst(device, q.ts, q.alarm, q.size, q.peak, q.duration));
                it.remove();
            }
        }
        if (detector == null) {
            detector = new OptWinDetector(tables.wMax);
        }
        OptWinDetector.Alarm a = detector.update(rate, tables);
        if (a != null) {
            Pending q = new Pending();
            q.ts = b;
            q.alarm = a;
            q.size = outliers;
            q.peak = rate;
            if (rate > 0) {
                q.duration = lastRunLen + 1;
                q.runOpen = true;
            } else {
                q.duration = lastRunLen;
                q.runOpen = false;
            }
            pending.add(q);
        }
        lastRunLen = rate > 0 ? lastRunLen + 1 : 0;
    }

    // ================================================================ 工具 / helpers
    private void append(int c, double v) {
        if (buf[c] == null) {
            buf[c] = new double[1024];
        } else if (bufN[c] == buf[c].length) {
            buf[c] = Arrays.copyOf(buf[c], buf[c].length * 2);
        }
        buf[c][bufN[c]++] = v;
    }

    private static boolean any(boolean[] m) {
        if (m == null) {
            return false;
        }
        for (boolean b : m) {
            if (b) {
                return true;
            }
        }
        return false;
    }

    /** 时刻（秒）所在的布里斯托当地日 / local day (YYYY-MM-DD) of an epoch second. */
    public static String localDay(long ts) {
        return Instant.ofEpochSecond(ts).atZone(ZONE).toLocalDate().toString();
    }

    /** 当地日结束（次日零点）的时刻（秒）/ epoch second at which the local day ends. */
    public static long dayEnd(String day) {
        return LocalDate.parse(day).plusDays(1).atStartOfDay(ZONE).toEpochSecond();
    }

    private static String previousDay(String day) {
        return LocalDate.parse(day).minusDays(1).toString();
    }

    public boolean isDeviated(int c) {
        return deviated[c];
    }
}
