package com.leejean.m6;

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
import com.leejean.m5.OptWinTables;
import com.leejean.m5a.M5aCore;
import com.leejean.m5a.RefHistogram;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 链式离线实验中一台设备的逐段推进模拟器（M6b 交接文档第四节）：与 {@link M6aSimulator} 调用同一组在线函数（标准化的冻结与零级、
 * PmcodFunction 的滑动步、M5aCore 的日记录与离群率爆发），区别在于可以多次接受命令，并按「推进到某一时刻」分段运行，供驱动程序
 * {@link ChainDriver} 在每个日界之后读取新的输出、交给决策函数，再把新命令排进来。
 * Incremental per-device simulator for the chained offline experiment, built from the same online functions.
 *
 * <p>事件次序与在线相同：推进到 T 时，先处理时刻早于 T 的轮（标准化、缓存、数据层），再处理窗末不晚于 T 的点通道滑动步（离群记录
 * 送数据层），最后按十分钟的关闭时刻逐个关闭十分钟（爆发的发出时刻即关闭时刻），并在日界定时器到时关闭当日。命令在生效时刻之后的
 * 第一条轮上执行（与在线的定时器执行等价，第一批已核对）。
 */
final class ChainDeviceSim {

    /** 零级的三档（交接文档决定十五）与开环 / the three level-0 variants and the open loop. */
    enum Variant { OPEN, D1, D7, POS }

    /** 一次执行的零级 / one executed level-0. */
    static final class Exec {
        long eff;
        int seq;
        int version;
        AdaptiveScalerFunction.Level0 l0;            // 重估函数的原始结果 / raw result of the re-estimation
        RobustScalerFunction.Calibration used;       // 实际换上的参数（只重估位置一档的分母与旁路取冻结值）/ parameters installed
        Backfill backfill;                           // 实际附上的回填 / the backfill attached
    }

    /** 一次爆发：报警元素与发出时刻 / a burst: alarm element and emission time. */
    static final class Burst {
        long ts;
        long emittedAt;
        long size;
        double peak;
        int duration;
    }

    private static final long WINDOW_SEC = 3600L;
    private static final long SLIDE_SEC = 60L;
    private static final int K = 10;
    private static final int WARMUP = 60480;
    private static final double EPS = 1e-9;
    private static final long BIN_SEC = 600L;

    final String dev;
    private final Variant variant;
    private final double r;
    private boolean[] keep;                                             // 不随零级重估的通道（1.41 版）/ kept channels
    private final ChannelTransform[] tf = ChannelTransform.defaultTable();
    private final M5aCore.Params m5p = new M5aCore.Params(new double[]{0.38, 0.73, 0.84, 0.43, 0.51}, 0.01, 0.2, 0.2, 10,
            (int) SLIDE_SEC);
    private final OptWinTables tables;
    private final List<M6aOfflineRunner.Row> rows;
    private int idx;

    private final CompactCache cache = new CompactCache(7 * 86400L);
    private List<List<Double>> res = new ArrayList<>();
    private int count;
    private RobustScalerFunction.Calibration cal;
    private RobustScalerFunction.Calibration frozen;
    private long freezeTs;
    private RefHistogram[] refsPending;
    private Backfill bfPending;
    private int version = 1;
    private final TreeMap<Long, Integer> commands = new TreeMap<>();    // 生效时刻 → 序号 / effective time → seq

    private final M5aCore core = new M5aCore();
    private final McodState st = new McodState();
    private final List<DevicePoint> points = new ArrayList<>();
    private int lo;
    private long nextEnd = Long.MIN_VALUE;                              // 下一个滑动步的窗末（毫秒）/ next window end (ms)

    // ---------------- 输出（累积）/ outputs (accumulated)
    final List<MonitoringSnapshot> records = new ArrayList<>();
    final List<DriftEvent> transitions = new ArrayList<>();
    final List<Burst> bursts = new ArrayList<>();
    final List<Exec> execs = new ArrayList<>();
    final List<Long> outliers = new ArrayList<>();                     // 到达滑动步的离群轮 / arrival-slide outlier rounds
    final Map<Long, int[]> hours = new HashMap<>();                     // 整点 → {离群轮数, 最大版本, 入口轮数}

    ChainDeviceSim(String dev, String roundsPath, Variant variant, double r, OptWinTables tables) throws java.io.IOException {
        this.dev = dev;
        this.variant = variant;
        this.r = r;
        this.tables = tables;
        this.rows = M6aOfflineRunner.read(roundsPath, null);
        for (int c = 0; c < Channels.N_DET; c++) {
            res.add(new ArrayList<>());
        }
    }

    /** 设置不随零级重估的通道集合（1.41 版裁决第一节；全假为全部重估）/ set the channels that keep the freeze parameters. */
    void keepChannels(boolean[] k) {
        boolean any = false;
        for (boolean b : k) {
            any |= b;
        }
        keep = any ? k.clone() : null;
    }

    boolean[] keepChannels() {
        return keep;
    }

    /** 排入一条零级命令；开环时只记录不执行 / schedule a level-0 command (ignored in the open loop). */
    void schedule(long eff, int seq) {
        if (variant != Variant.OPEN) {
            commands.put(eff, seq);
        }
    }

    private int[] hour(long ts) {
        return hours.computeIfAbsent(Math.floorDiv(ts, 3600L) * 3600L, k -> new int[]{0, 1, 0});
    }

    /** 推进到时刻 t（秒）/ advance to time t (s). */
    void advanceTo(long t) {
        // 一、轮 / rounds
        while (idx < rows.size() && rows.get(idx).ts < t) {
            round(rows.get(idx++));
        }
        // 二、点通道滑动步 / point-channel slides
        long tMs = t * 1000L;
        long wMs = WINDOW_SEC * 1000L;
        long sMs = SLIDE_SEC * 1000L;
        if (nextEnd == Long.MIN_VALUE && !points.isEmpty()) {
            nextEnd = (Math.floorDiv(points.get(0).getPoint().arrival, sMs) + 1) * sMs;
        }
        int hi = lo;
        while (nextEnd != Long.MIN_VALUE && nextEnd <= tMs) {
            long end = nextEnd;
            nextEnd += sMs;
            while (lo < points.size() && points.get(lo).getPoint().arrival < end - wMs) {
                lo++;
            }
            hi = Math.max(hi, lo);
            while (hi < points.size() && points.get(hi).getPoint().arrival < end) {
                hi++;
            }
            if (lo == hi) {
                continue;                                     // 空窗口不触发 / empty windows do not fire
            }
            PmcodFunction.Slide sl = PmcodFunction.slide(st, new ArrayList<>(points.subList(lo, hi)), end - wMs, end, sMs, r, K, false);
            long endSec = end / 1000L;
            for (long id : sl.emitted) {
                if (endSec - SLIDE_SEC <= id && id < endSec) {
                    outliers.add(id);
                    hour(id)[0]++;
                }
                core.onOutlier(id, endSec, m5p);
            }
        }
        if (lo > 100000) {                                    // 丢掉已离开窗口的点 / drop points that left every window
            points.subList(0, lo).clear();
            lo = 0;
        }
        // 三、十分钟逐个关闭，再关日 / close bins one by one, then the day
        List<DriftEvent> ev = new ArrayList<>();
        List<MonitoringSnapshot> rec = new ArrayList<>();
        for (long b = core.earliestOpenBin(); b != 0 && M5aCore.binCloseTime(b, m5p) <= t; b = core.earliestOpenBin()) {
            long close = M5aCore.binCloseTime(b, m5p);
            ev.clear();
            core.closeBinsUpTo(close, dev, m5p, tables, ev);
            collect(ev, rec, close);
        }
        if (core.openDayEnd() > 0 && core.openDayEnd() <= t) {
            ev.clear();
            core.closeDay(dev, m5p, ev, rec);
            collect(ev, rec, core.openDayEnd());
        }
    }

    private void collect(List<DriftEvent> ev, List<MonitoringSnapshot> rec, long emittedAt) {
        for (DriftEvent e : ev) {
            if (e.level == DriftEvent.Level.BURST) {
                Burst b = new Burst();
                b.ts = e.ts;
                b.emittedAt = emittedAt;
                b.size = e.burstSize;
                b.peak = e.burstPeak;
                b.duration = e.burstDuration;
                bursts.add(b);
            } else {
                transitions.add(e);
            }
        }
        records.addAll(rec);
        rec.clear();
    }

    private void round(M6aOfflineRunner.Row w) {
        boolean[] cens = new boolean[Channels.N_DET];
        cens[Channels.LIGHT_INDEX] = !w.missing[Channels.LIGHT_INDEX] && w.x[Channels.LIGHT_INDEX] == 65536.0;
        boolean[] stuck = new boolean[Channels.N_DET];
        stuck[0] = w.stuck;
        if (cal != null && !commands.isEmpty() && w.ts >= commands.firstKey()) {
            execute(commands.firstKey(), commands.pollFirstEntry().getValue());
        }
        cache.add(w.ts, w.x, CompactCache.maskOf(w.missing, cens, stuck));
        DeviceRound d = new DeviceRound();
        d.setDevice(dev);
        d.setTs(w.ts);
        d.setX(w.x.clone());
        d.setMissingMask(w.missing.clone());
        d.setCensoredMask(cens);
        d.setStuckMask(stuck);
        boolean missingAny = d.missingCount() > 0;
        int[] h = hour(w.ts);
        if (cal == null) {
            for (int c = 0; c < Channels.N_DET; c++) {
                if (!w.missing[c] && !(c == Channels.LIGHT_INDEX && cens[c])) {
                    res.get(c).add(tf[c].apply(w.x[c]));
                }
            }
            d.setWarmup(true);
            if (++count == WARMUP) {
                cal = RobustScalerFunction.calibrate(res, EPS, false);
                frozen = cal;
                freezeTs = w.ts;
                refsPending = cal.hist;
                res = null;
            }
        } else {
            double[] xn = RobustScalerFunction.normalize(w.x, w.missing, cal.median, cal.scale, cal.bypass, tf);
            d.setXNorm(xn);
            d.setVersion(version);
            h[1] = Math.max(h[1], version);
            if (refsPending != null) {
                d.setRefHistograms(refsPending);
                refsPending = null;
            }
            if (bfPending != null && !missingAny) {
                d.setBackfill(bfPending);
                bfPending = null;
            }
            if (!missingAny) {
                h[2]++;
                DevicePoint p = new DevicePoint(dev, new McodPoint(xn.clone(), w.ts * 1000L, 0, w.ts, w.cold), cens.clone());
                p.setVersion(d.getVersion());
                p.setBackfill(d.getBackfill());
                points.add(p);
            }
        }
        List<DriftEvent> ev = new ArrayList<>();
        List<MonitoringSnapshot> rec = new ArrayList<>();
        core.onRound(d, m5p, ev, rec);
        collect(ev, rec, w.ts);
    }

    /** 执行零级：三档的差别只在重估窗口长度与分母是否冻结 / execute level-0 for the variant. */
    private void execute(long eff, int seq) {
        long dSec = variant == Variant.D1 ? 86400L : 7 * 86400L;
        Exec x = new Exec();
        x.eff = eff;
        x.seq = seq;
        x.version = seq + 1;
        x.l0 = AdaptiveScalerFunction.level0(cache, eff, dSec, WINDOW_SEC, EPS, false, ChannelTransform.defaultTable(), frozen, keep);
        if (variant == Variant.POS) {
            // 只重估位置：中位数取七天重估值，分母与旁路沿用首次冻结；参照直方图取七天窗口；回填按实际换上的参数重新标准化
            RobustScalerFunction.Calibration u = new RobustScalerFunction.Calibration();
            for (int c = 0; c < Channels.N_DET; c++) {
                u.median[c] = x.l0.cal.median[c];
                u.scale[c] = frozen.scale[c];
                u.bypass[c] = frozen.bypass[c];
                u.hist[c] = x.l0.cal.hist[c];
            }
            // 回填的取轮规则与 level0 相同：[生效 − 窗长, 生效) 内检测通道无缺失的轮 / same selection as level0
            List<Long> ts = new ArrayList<>();
            List<double[]> xn = new ArrayList<>();
            List<boolean[]> ce = new ArrayList<>();
            for (int k = 0; k < cache.size(); k++) {
                long t = cache.ts(k);
                int m = cache.mask(k);
                if (t < eff - WINDOW_SEC || t >= eff || (m & 0x1f) != 0) {
                    continue;
                }
                double[] raw = new double[Channels.N_DET];
                boolean[] cs = new boolean[Channels.N_DET];
                for (int c = 0; c < Channels.N_DET; c++) {
                    raw[c] = cache.x(k, c);
                    cs[c] = (m >>> (CompactCache.CENSORED_SHIFT + c) & 1) != 0;
                }
                ts.add(t);
                xn.add(RobustScalerFunction.normalize(raw, new boolean[Channels.N_DET], u.median, u.scale, u.bypass, tf));
                ce.add(cs);
            }
            x.backfill = new Backfill(ts.stream().mapToLong(Long::longValue).toArray(), xn.toArray(new double[0][]),
                    ce.toArray(new boolean[0][]));
            x.used = u;
        } else {
            x.used = x.l0.cal;
            x.backfill = x.l0.backfill;
        }
        cal = x.used;
        version = x.version;
        refsPending = x.used.hist;
        bfPending = x.backfill;
        execs.add(x);
    }

    RobustScalerFunction.Calibration frozen() {
        return frozen;
    }

    long freezeTs() {
        return freezeTs;
    }
}
