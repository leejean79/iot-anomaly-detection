package com.leejean.m5;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 持续性核对（M5b 交接文档决定三，10 月 19 日补充）：把检测器的原始报警分成候选、确认与撤销。实例是受检查点保护的状态。
 * Persistence gate: turns raw detector alarms into candidate, confirmed and revoked drift. The instance is checkpointed state.
 *
 * <p>规则：检测器报警即发出候选。若此刻没有进行中的核对，就以报警时历史子窗的中位数为参照，门限 = 参照 + 鲁棒度 × 历史
 * 子窗样本标准差，开始核对。K 从候选之后的第一个元素起计数；只用候选之后的元素，凑满六个之后，每到一个元素就取候选之后
 * 最近六个元素的中位数，不高于门限即撤销，到第 K 个仍高于门限即确认（0.94 版第六条：候选之前的元素属于漂移前的水平，
 * 不得混入）。核对进行中出现的新候选照常发出，但不打断核对，参照与门限不变；确认或撤销之后状态清零。
 * Rule: every alarm is a candidate; if no check is running, start one with reference = historical median and
 * threshold = reference + robustness × historical sample sd. K counts from the first element after the candidate;
 * only post-candidate elements are used, and once six have arrived the median of the last six of them must stay
 * above the threshold (else revoke); at element K, confirm. Candidates during a running check are emitted but do not
 * restart it.
 *
 * <p>中位数与门限的浮点运算次序与 Python 参考实现逐步相同。/ Same floating-point order as the Python reference.
 */
public final class DriftGate implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 滑动中位数的元素数（一小时）/ elements in the sliding median (one hour). */
    public static final int MEDIAN_SPAN = 6;

    private final int k;
    private final double robustness;
    private final double[] last = new double[MEDIAN_SPAN];   // 候选之后最近六个元素 / last six post-candidate elements
    private int lastCount;
    private int lastHead;
    private boolean pending;
    private int seen;
    private double reference;
    private double threshold;
    private long candidateTs;
    private OptWinDetector.Alarm origin;

    public DriftGate(int k, double robustness) {
        if (k < MEDIAN_SPAN) {
            throw new IllegalArgumentException("持续性核对的 K 不得小于 " + MEDIAN_SPAN);
        }
        this.k = k;
        this.robustness = robustness;
    }

    /**
     * 处理一个元素及其上可能的报警，返回本元素上发出的事件（按发生顺序：先是进行中核对的确认或撤销，再是新候选）。
     * Process one element and its possible alarm; returns the events of this element (gate outcome first, then the candidate).
     */
    public List<DriftEvent> onElement(String device, long ts, double x, OptWinDetector.Alarm alarm) {
        List<DriftEvent> out = new ArrayList<>(2);
        if (pending) {
            seen++;
            last[(lastHead + lastCount) % MEDIAN_SPAN] = x;
            if (lastCount < MEDIAN_SPAN) {
                lastCount++;
            } else {
                lastHead = (lastHead + 1) % MEDIAN_SPAN;
            }
            if (lastCount == MEDIAN_SPAN) {               // 凑满六个之后才比较 / compare once six have arrived
                double med = median();
                if (!(med > threshold)) {
                    out.add(gateEvent(device, ts, DriftEvent.Level.REVOKE, med));
                    pending = false;
                } else if (seen == k) {
                    out.add(gateEvent(device, ts, DriftEvent.Level.CONFIRM, med));
                    pending = false;
                }
            }
        }
        if (alarm != null) {
            out.add(DriftEvent.candidate(device, ts, alarm, pending));
            if (!pending) {
                pending = true;
                seen = 0;
                lastCount = 0;
                lastHead = 0;
                reference = alarm.histMedian;
                threshold = reference + robustness * alarm.histSd;
                candidateTs = ts;
                origin = alarm;
            }
        }
        return out;
    }

    private DriftEvent gateEvent(String device, long ts, DriftEvent.Level level, double med) {
        return DriftEvent.gate(device, ts, level, origin, candidateTs, reference, threshold, med, seen);
    }

    private double median() {
        double[] s = new double[lastCount];
        for (int i = 0; i < lastCount; i++) {
            s[i] = last[(lastHead + i) % MEDIAN_SPAN];
        }
        Arrays.sort(s);
        return lastCount % 2 == 1 ? s[lastCount / 2] : (s[lastCount / 2 - 1] + s[lastCount / 2]) / 2.0;
    }

    /** 清空（设备冷启动时）/ reset on cold start. */
    public void clear() {
        lastCount = 0;
        lastHead = 0;
        pending = false;
        seen = 0;
        origin = null;
    }

    public boolean isPending() {
        return pending;
    }
}
