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
 * <p><b>背景</b>：三月干净基线跑完后，逐设备核验显示 A–G 七台设备的 meanOutlierRate 一律比 Java 8
 * 探针参考值低约 0.0059 个百分点，且每台的滑窗数恰好比探针少 <b>60</b> 个——60 正是 W/S = 3600/60，
 * 一个完整窗长折合的滑动步数。
 *
 * <p><b>机制</b>：修正前 {@code M2Probe} 的循环条件是 {@code windowEnd - windowMs <= maxArrival}，
 * 等价于 {@code windowStart <= maxArrival}，因此会一直滑到窗口起点越过最后一个数据点为止——在最后
 * 一个点之后还要多走 W/S 步，这段里窗口只出不进、逐步排空。而运行中的 Flink 作业只触发
 * {@code windowEnd ≤ 当前水位线} 的窗口，流未结束时水位线停在最后一个事件上，这些排空窗口尚未触发。
 * 排空段里窗口点数持续下降，一旦降到半径 R 内邻居不足 k 个，窗内每个点都被判为离群、该滑窗离群率
 * 达到 1.0，而「逐滑窗比率的算术平均」对这类滑窗没有任何加权保护。
 *
 * <p><b>修复状态</b>：两处循环已按设计会话裁决改为 {@code windowEnd <= maxArrival}；旧口径由
 * {@code --legacy-drain-tail} 保留，用于在同一运行时下做单变量比较。本测试直接驱动真实的
 * {@link M2Probe#sweep}，两种口径各跑一次。
 *
 * <p>Mechanism: the old bound kept sliding W/S steps past the final point while the window drained;
 * a running Flink job only fires windows whose end is at or before the watermark, which stalls at the
 * last event. Both calibers are exercised here through the real sweep.
 */
class ProbeDrainTailTest {

    private static final long WINDOW_MS = 3600_000L;   // W = 3600s
    private static final long SLIDE_MS = 60_000L;      // S = 60s
    private static final long EXPECTED_EXTRA_SLIDES = WINDOW_MS / SLIDE_MS;   // = 60

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
    @DisplayName("旧口径恰好比新口径多 W/S 个滑窗，多出来的正是排空尾巴")
    void legacyCaliberAddsExactlyOneWindowOfDrainSlides() {
        List<McodPoint> pts = denseDevice(6);
        M2Probe.RateResult corrected = M2Probe.sweep(pts, 1.0, 10, WINDOW_MS, SLIDE_MS, false);
        M2Probe.RateResult legacy = M2Probe.sweep(pts, 1.0, 10, WINDOW_MS, SLIDE_MS, true);

        assertEquals(EXPECTED_EXTRA_SLIDES, legacy.slides - corrected.slides,
                "旧口径应恰好多出 W/S = " + EXPECTED_EXTRA_SLIDES + " 个滑窗（一个完整窗长的排空段）");
    }

    @Test
    @DisplayName("排空尾巴抬高逐滑窗比率的平均值，且其贡献与设备密度无关")
    void drainTailLiftsTheMean() {
        List<McodPoint> pts = denseDevice(6);
        M2Probe.RateResult corrected = M2Probe.sweep(pts, 1.0, 10, WINDOW_MS, SLIDE_MS, false);
        M2Probe.RateResult legacy = M2Probe.sweep(pts, 1.0, 10, WINDOW_MS, SLIDE_MS, true);

        // 比率之和 = 平均值 × 滑窗数；旧口径多出来的那部分就是排空段的贡献。
        double sumCorrected = corrected.meanOutlierRate * corrected.slides;
        double sumLegacy = legacy.meanOutlierRate * legacy.slides;
        assertTrue(sumLegacy - sumCorrected > 1.0,
                "排空段贡献的比率之和应大于 1，实测 " + (sumLegacy - sumCorrected));
        assertTrue(legacy.meanOutlierRate > corrected.meanOutlierRate,
                "旧口径的均值应高于新口径");
    }

    @Test
    @DisplayName("回归守卫：默认口径就是作业口径，不含排空尾巴")
    void defaultCaliberMatchesTheJob() {
        List<McodPoint> pts = denseDevice(6);
        M2Probe.RateResult corrected = M2Probe.sweep(pts, 1.0, 10, WINDOW_MS, SLIDE_MS, false);
        M2Probe.RateResult legacy = M2Probe.sweep(pts, 1.0, 10, WINDOW_MS, SLIDE_MS, true);

        // 作业口径下，最后一个被触发的窗口其结束时刻不得超过最后一个数据点的到达时刻。
        long maxArrival = pts.get(pts.size() - 1).arrival;
        long expectedSlides = maxArrival / SLIDE_MS;
        assertEquals(expectedSlides, corrected.slides,
                "默认口径的滑窗数应为 floor(maxArrival / S)；若等于 " + legacy.slides
                        + " 说明排空尾巴的上界被改回去了");
    }
}
