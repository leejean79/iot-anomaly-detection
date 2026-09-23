package com.leejean.m3;

import org.deeplearning4j.nn.api.Model;
import org.deeplearning4j.optimize.api.BaseTrainingListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 记录每一次参数更新时梯度的二范数（裁决书《梯度裁剪阈值的定法与步骤 A 选择规则的改写》第二节）。
 * Records the L2 norm of the gradient at every parameter update.
 *
 * <p><b>为什么挂在 onGradientCalculation 上。</b>DL4J 的梯度裁剪作用于**更新器之前**的原始梯度，
 * 而 {@code iterationDone} 是在更新器把梯度就地变换成「更新量」之后才触发的。挂错钩子记下来的
 * 会是 Adam 的更新量范数而不是原始梯度范数，两者量级完全不同，据此定出的裁剪阈值会是错的。
 * {@code onGradientCalculation} 恰好在梯度算出、更新器尚未作用时触发。
 * Clipping acts on the raw gradient before the updater, while iterationDone fires after the updater
 * has transformed the gradient in place — hooking the wrong one would measure the wrong quantity.
 *
 * <p>本类只读不改：它不修改梯度，因此开启记录与否不影响训练结果。
 * Read-only: enabling the recorder cannot change the training outcome.
 */
public class GradientNormRecorder extends BaseTrainingListener {

    /** 整个模型梯度的二范数，每次更新一条 / the whole-model gradient norm, one per update. */
    private final List<Double> norms = new ArrayList<>();
    /**
     * 每次更新中**逐层**二范数的最大值。
     *
     * <p><b>为什么要单独记它。</b>DL4J 的 ClipL2PerLayer 是**逐层**比较的：某一层的梯度范数超过阈值
     * 才裁那一层。而整模型范数是各层范数的平方和开方，必然大于任何单层的范数。若拿整模型范数的分位数
     * 去定阈值，得到的值会系统性偏大，裁剪几乎永远不触发——测的量与裁的量必须是同一个。
     * ClipL2PerLayer compares per-layer norms, which are always smaller than the whole-model norm;
     * setting the threshold from the latter would systematically overshoot.
     */
    private final List<Double> maxLayerNorms = new ArrayList<>();

    @Override
    public void onGradientCalculation(Model model) {
        if (model.gradient() == null || model.gradient().gradient() == null) {
            return;
        }
        norms.add(model.gradient().gradient().norm2Number().doubleValue());

        // 参数名形如 "0_W"、"0_RW"、"0_b"，下划线之前是层号；按层号归组后逐层求二范数。
        // Parameter keys look like "0_W"; the prefix before the underscore is the layer index.
        java.util.Map<String, Double> perLayerSq = new java.util.TreeMap<>();
        for (java.util.Map.Entry<String, org.nd4j.linalg.api.ndarray.INDArray> e
                : model.gradient().gradientForVariable().entrySet()) {
            if (e.getValue() == null) {
                continue;
            }
            String layer = e.getKey().contains("_")
                    ? e.getKey().substring(0, e.getKey().indexOf('_')) : e.getKey();
            double n = e.getValue().norm2Number().doubleValue();
            perLayerSq.merge(layer, n * n, Double::sum);
        }
        double worst = 0.0;
        for (double sq : perLayerSq.values()) {
            worst = Math.max(worst, Math.sqrt(sq));
        }
        maxLayerNorms.add(worst);
    }

    /** 逐次的「逐层范数最大值」，即裁剪实际比较的那个量 / the quantity clipping actually compares. */
    public List<Double> maxLayerNorms() {
        return maxLayerNorms;
    }

    /** 记录到的全部范数，按记录顺序 / all recorded norms, in order. */
    public List<Double> norms() {
        return norms;
    }

    public int count() {
        return norms.size();
    }

    /**
     * 分位数（最近邻取法，不插值）。样本量只有几百到几千，插值与否的差别远小于分布本身的抖动，
     * 取法简单反而便于复核。
     * Nearest-rank percentile; with a few hundred samples interpolation would be false precision.
     */
    public double percentile(double p) {
        return percentileOf(norms, p);
    }

    /** 对「逐层范数最大值」求分位数——定裁剪阈值须用它 / percentile of the per-layer maxima. */
    public double layerPercentile(double p) {
        return percentileOf(maxLayerNorms, p);
    }

    private static double percentileOf(List<Double> values, double p) {
        if (values.isEmpty()) {
            return Double.NaN;
        }
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(sorted.size() - 1, idx)));
    }

    public double layerMedian() {
        return layerPercentile(50.0);
    }

    public double layerMax() {
        double m = Double.NEGATIVE_INFINITY;
        for (double v : maxLayerNorms) {
            m = Math.max(m, v);
        }
        return maxLayerNorms.isEmpty() ? Double.NaN : m;
    }

    /** 给定阈值下会被裁到的更新占比，按**逐层**口径——与 ClipL2PerLayer 的行为一致。 */
    public double clippedFractionPerLayer(double threshold) {
        if (maxLayerNorms.isEmpty()) {
            return Double.NaN;
        }
        int n = 0;
        for (double v : maxLayerNorms) {
            if (v > threshold) {
                n++;
            }
        }
        return (double) n / maxLayerNorms.size();
    }

    public double median() {
        return percentile(50.0);
    }

    public double max() {
        double m = Double.NEGATIVE_INFINITY;
        for (double v : norms) {
            m = Math.max(m, v);
        }
        return norms.isEmpty() ? Double.NaN : m;
    }

    /**
     * 给定阈值下会被裁到的更新占比。裁决书第二节第 3 条要求报告它（预期千分之一以下）。
     * The fraction of updates that a given threshold would clip.
     */
    public double clippedFraction(double threshold) {
        if (norms.isEmpty()) {
            return Double.NaN;
        }
        int n = 0;
        for (double v : norms) {
            if (v > threshold) {
                n++;
            }
        }
        return (double) n / norms.size();
    }
}
