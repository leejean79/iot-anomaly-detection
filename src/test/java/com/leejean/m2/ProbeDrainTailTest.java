package com.leejean.m2;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 探针「排空尾巴」与运行中作业的口径差异 / the probe's drain tail versus a running job.
 *
 * <p>背景：三月干净基线跑完后，逐设备核验显示 A–G 七台设备的 meanOutlierRate 一律比 Java 8 探针
 * 参考值低约 0.0059 个百分点，且每台的滑窗数恰好比探针少 <b>60</b> 个。60 正是 W/S = 3600/60，
 * 即一个完整窗长折合的滑动步数。本测试把这个差异的机制固化下来。
 *
 * <p>机制：{@code M2Probe.sweep} 的循环条件是 {@code windowEnd - windowMs <= maxArrival}，也就是
 * {@code windowStart <= maxArrival}，因此它会一直滑到<b>窗口起点越过最后一个数据点</b>为止——在最后
 * 一个点之后还要多走 W/S 个滑动步，这段里窗口只出不进、逐步排空。而运行中的 Flink 作业只会触发
 * {@code windowEnd <= 当前水位线} 的窗口，流未结束时水位线停在最后一个事件上，这 W/S 个排空窗口
 * 因而<b>尚未触发</b>。两者相差的正是这一个窗长。
 *
 * <p>排空段里窗口点数持续下降，一旦降到半径 R 内邻居不足 k 个，窗内每个点都被判为离群，该滑窗的
 * 离群率达到 1.0。这类饱和滑窗数量虽少，却能把「逐滑窗比率的算术平均」整体抬高。
 *
 * <p>Mechanism: the probe's loop runs until the window START passes the last arrival, i.e. W/S slides
 * beyond the final point, draining the window; a running Flink job only fires windows whose END is at
 * or before the watermark, which stalls at the last event. The difference is exactly one window.
 */
class ProbeDrainTailTest {

    private static final long WINDOW_MS = 3600_000L;   // W = 3600s
    private static final long SLIDE_MS = 60_000L;      // S = 60s
    private static final int EXPECTED_EXTRA_SLIDES = (int) (WINDOW_MS / SLIDE_MS);   // = 60

    /** 一次扫描的统计量 / one sweep's statistics. */
    private static final class Sweep {
        int slides;
        double sumRate;
        int saturatedSlides;   // 离群率 == 1.0 的滑窗数 / slides whose outlier rate is exactly 1.0
    }

    /**
     * 复刻 M2Probe.sweep 的滑窗循环（sweep 是私有方法，无法直接调用），只有循环上界可切换：
     * drainTail=true 复刻探针（windowStart <= maxArrival），false 复刻运行中作业（windowEnd <= maxArrival）。
     * Replica of M2Probe.sweep's loop (that method is private); only the upper bound differs.
     */
    private static Sweep sweep(List<McodPoint> pts, double r, int k, boolean drainTail) {
        List<McodPoint> copy = new ArrayList<>(pts.size());
        for (McodPoint p : pts) {
            copy.add(new McodPoint(p.value.clone(), p.arrival, 0, p.id));
        }
        McodCore core = new McodCore(r, k, SLIDE_MS, new McodState());
        long maxArrival = copy.get(copy.size() - 1).arrival;
        int cursor = 0;
        List<McodPoint> active = new ArrayList<>();
        Sweep s = new Sweep();
        for (long windowEnd = SLIDE_MS;
             drainTail ? (windowEnd - WINDOW_MS <= maxArrival) : (windowEnd <= maxArrival);
             windowEnd += SLIDE_MS) {
            long windowStart = windowEnd - WINDOW_MS;
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
                s.sumRate += rate;
                if (rate == 1.0) {
                    s.saturatedSlides++;
                }
                s.slides++;
            }
            active.removeIf(p -> p.arrival < windowStart + SLIDE_MS);
        }
        return s;
    }

    /** 造一台设备：每 10 秒一轮，五通道取值挤在一处，正常滑窗离群率应为 0。 */
    private static List<McodPoint> denseDevice(int hours) {
        List<McodPoint> pts = new ArrayList<>();
        long periodMs = 10_000L;
        long n = hours * 3600_000L / periodMs;
        for (long i = 0; i < n; i++) {
            long arrival = (i + 1) * periodMs;
            double jitter = ((i % 7) - 3) * 0.001;   // 远小于 R 的抖动 / jitter far below R
            double[] v = {jitter, jitter, jitter, jitter, jitter};
            pts.add(new McodPoint(v, arrival, 0, arrival / 1000L));
        }
        return pts;
    }

    @Test
    @DisplayName("探针比运行中作业恰好多 W/S 个滑窗，且多出来的是排空尾巴")
    void probeCountsExactlyOneWindowOfDrainSlides() {
        List<McodPoint> pts = denseDevice(6);
        Sweep probe = sweep(pts, 1.0, 10, true);
        Sweep job = sweep(pts, 1.0, 10, false);

        assertEquals(EXPECTED_EXTRA_SLIDES, probe.slides - job.slides,
                "探针应恰好多出 W/S = " + EXPECTED_EXTRA_SLIDES + " 个滑窗（一个完整窗长的排空段）");
    }

    @Test
    @DisplayName("排空尾巴里出现离群率 1.0 的饱和滑窗，抬高逐滑窗比率的平均值")
    void drainTailSaturatesAndLiftsTheMean() {
        List<McodPoint> pts = denseDevice(6);
        Sweep probe = sweep(pts, 1.0, 10, true);
        Sweep job = sweep(pts, 1.0, 10, false);

        // 注意：开头的起步窗同样会饱和——第一个窗口只含最初几个点，不足 k 个邻居，于是全员离群。
        // 但探针与作业都从 windowEnd = S 开始扫，这段头部是**两边共有**的，在比较中互相抵消，
        // 因此它不是偏差的来源。真正只存在于探针一侧的，是末尾的排空段。
        // NOTE: the head ramp saturates too, but both sides include it, so it cancels; only the
        // drain tail is one-sided.
        assertTrue(job.saturatedSlides > 0, "头部起步窗在两边都会饱和（本断言记录这一事实）");
        assertTrue(probe.saturatedSlides > job.saturatedSlides,
                "排空段应额外产生离群率 1.0 的饱和滑窗，实测 探针 " + probe.saturatedSlides
                        + " 个 vs 作业 " + job.saturatedSlides + " 个");
        // 排空段贡献的比率之和是 O(1) 量级——与设备密度无关，因此在三万多个滑窗上表现为一个
        // 近似恒定的加性偏移，这正是 A–G 七台一致偏低约 0.0059 个百分点的形状。
        assertTrue(probe.sumRate - job.sumRate > 1.0,
                "排空段贡献的比率之和应大于 1，实测 " + (probe.sumRate - job.sumRate));
    }
}
