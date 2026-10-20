package com.leejean.m5;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 最优窗口漂移检测器（M5b 交接文档决定二；论文 Tosi 第四章算法 2；裁决补充 0.93 版）。没有 Flink 依赖，供所有标量流复用；
 * 实例本身就是受检查点保护的状态（窗内容与增量统计量），常量表 {@link OptWinTables} 由调用方传入，不进状态。
 * The OPTWIN drift detector (Algorithm 2 of the thesis). No Flink dependency; the instance is the checkpointed state
 * (window contents and running sums); the constant tables are passed in by the caller and never stored.
 *
 * <p>每来一个元素：放进滑动窗（超过窗上限就丢掉最旧的）；窗长不足 30 不检验；否则按常量表的切点把窗分成历史子窗与新子窗，
 * 先做方差的 F 检验，再做均值的韦尔奇 t 检验（式 4.2，分母用方差）。两个检验都只在新子窗均值高于历史子窗时报警（论文
 * 4.5.3 节）；F 检验显著而均值未升高时不报警、不清空，只计数；两个子窗都为常数时不做 t 检验、视为不显著，也计数。报警后
 * 清空滑动窗，触发元素不放回。
 * Per element: append (dropping the oldest beyond wMax); no test below 30 elements; otherwise split at the tabulated cut,
 * F-test first, then the Welch t-test (Eq. 4.2, variances). Both alarm only when the new mean is higher (section 4.5.3);
 * a significant F with a non-increasing mean is only counted. After an alarm the window is emptied.
 *
 * <p>浮点运算的次序与 Python 参考实现 {@code m5b/optwin_offline.py} 逐步相同，两边的报警因此逐位一致：子窗的和与平方和
 * 增量维护；样本方差 = (平方和 − 和²/n)/(n−1)，负的舍入误差截为 0；F 检验的标准差各加 β = 1e−5（算法 2）。
 * Floating-point operations follow exactly the same order as the Python reference, so alarms agree bit for bit.
 */
public final class OptWinDetector implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 算法 2 的防除零常数，只加在 F 检验的标准差上 / Algorithm 2's β, added to the F-test only. */
    public static final double BETA = 1e-5;

    private final int wMax;
    private final double[] buf;      // 环形缓冲，容量 wMax+1（先放入再丢最旧）/ ring buffer
    private int head;
    private int n;                   // 窗长 / window length
    private int h;                   // 历史子窗长度（窗头的 h 个元素）/ historical sub-window length
    private double sumH, sqH, sumN, sqN;
    private long fWithoutRise;       // F 显著而均值未升高的次数 / significant F without a mean rise
    private long tSkippedConstant;   // 两个子窗都为常数、不做 t 检验的次数（0.94 版第八条）/ t-test skipped, both constant

    public OptWinDetector(int wMax) {
        if (wMax < OptWinTables.W_MIN) {
            throw new IllegalArgumentException("窗上限不得小于 " + OptWinTables.W_MIN);
        }
        this.wMax = wMax;
        this.buf = new double[wMax + 1];
    }

    /** 报警时两个子窗的统计量 / sub-window statistics at an alarm. */
    public static final class Alarm implements Serializable {
        private static final long serialVersionUID = 1L;
        /** "F" 或 "T"：先显著的检验 / the test that fired first. */
        public final String test;
        public final int windowLen, histLen, newLen;
        public final double histMean, histSd, newMean, newSd, histMedian, fValue, tValue;

        Alarm(String test, int windowLen, int histLen, int newLen, double histMean, double histSd,
              double newMean, double newSd, double histMedian, double fValue, double tValue) {
            this.test = test;
            this.windowLen = windowLen;
            this.histLen = histLen;
            this.newLen = newLen;
            this.histMean = histMean;
            this.histSd = histSd;
            this.newMean = newMean;
            this.newSd = newSd;
            this.histMedian = histMedian;
            this.fValue = fValue;
            this.tValue = tValue;
        }

        /** 漂移幅度：以历史标准差（加 β）为单位的均值差 / magnitude in historical standard deviations. */
        public double magnitude() {
            return (newMean - histMean) / (histSd + BETA);
        }
    }

    /**
     * 处理一个元素；报警时返回两个子窗的统计量并清空窗口，否则返回 null。
     * Process one element; on an alarm return the sub-window statistics (and empty the window), else null.
     */
    public Alarm update(double x, OptWinTables t) {
        if (t.wMax != wMax) {
            throw new IllegalArgumentException("常量表的窗上限与检测器不一致");
        }
        buf[(head + n) % buf.length] = x;
        n++;
        sumN += x;
        sqN += x * x;
        if (n > wMax) {                                   // 丢最旧的一个，它在历史子窗的头部 / drop the oldest
            double y = buf[head];
            head = (head + 1) % buf.length;
            n--;
            if (h > 0) {
                h--;
                sumH -= y;
                sqH -= y * y;
            } else {
                sumN -= y;
                sqN -= y * y;
            }
        }
        if (n < OptWinTables.W_MIN) {
            return null;
        }
        int c = t.cut[n];
        while (h < c) {                                   // 新子窗头部的元素并入历史子窗 / move into history
            double e = get(h);
            sumN -= e;
            sqN -= e * e;
            sumH += e;
            sqH += e * e;
            h++;
        }
        while (h > c) {                                   // 历史子窗尾部的元素退回新子窗 / move back to new
            h--;
            double e = get(h);
            sumH -= e;
            sqH -= e * e;
            sumN += e;
            sqN += e * e;
        }
        int m = n - h;
        double meanH = sumH / h;
        double meanN = sumN / m;
        double varH = variance(sumH, sqH, h);
        double varN = variance(sumN, sqN, m);
        double sdH = Math.sqrt(varH);
        double sdN = Math.sqrt(varN);
        double a = sdN + BETA;
        double b = sdH + BETA;
        double fValue = (a * a) / (b * b);
        double denom = Math.sqrt(varH / h + varN / m);
        double tValue = denom > 0 ? (meanN - meanH) / denom : 0.0;
        String fired = null;
        if (fValue > t.fCrit[n]) {
            if (meanN > meanH) {
                fired = "F";
            } else {
                fWithoutRise++;
            }
        }
        if (fired == null) {
            if (denom > 0) {
                if (tValue > t.tCrit[n]) {
                    fired = "T";
                }
            } else {
                tSkippedConstant++;
            }
        }
        if (fired == null) {
            return null;
        }
        Alarm alarm = new Alarm(fired, n, h, m, meanH, sdH, meanN, sdN, histMedian(), fValue, tValue);
        clear();
        return alarm;
    }

    private static double variance(double sum, double sq, int k) {
        if (k < 2) {
            return 0.0;
        }
        double v = (sq - sum * sum / k) / (k - 1);
        return v > 0 ? v : 0.0;
    }

    private double get(int i) {
        return buf[(head + i) % buf.length];
    }

    private double histMedian() {
        double[] s = new double[h];
        for (int i = 0; i < h; i++) {
            s[i] = get(i);
        }
        Arrays.sort(s);
        return h % 2 == 1 ? s[h / 2] : (s[h / 2 - 1] + s[h / 2]) / 2.0;
    }

    /** 清空窗口（报警后，或设备冷启动时）/ empty the window. */
    public void clear() {
        head = 0;
        n = 0;
        h = 0;
        sumH = 0;
        sqH = 0;
        sumN = 0;
        sqN = 0;
    }

    public int windowLength() {
        return n;
    }

    public long fWithoutRiseCount() {
        return fWithoutRise;
    }

    public long tSkippedConstantCount() {
        return tSkippedConstant;
    }
}
