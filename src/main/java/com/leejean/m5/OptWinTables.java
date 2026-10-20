package com.leejean.m5;

import org.apache.commons.math3.distribution.FDistribution;
import org.apache.commons.math3.distribution.TDistribution;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/**
 * 最优窗口漂移检测器的常量表（M5b 交接文档第三节；论文 Tosi 第四章式 4.1、4.7、4.12；裁决补充 0.93 版）：对每一个窗长 W
 * 预先算出切点（历史子窗的长度）、F 检验与 t 检验的临界值。这些量只依赖窗长、置信度与鲁棒度，与数据无关，所以是常量，
 * 不进入受检查点保护的状态。
 * Constant tables of the OPTWIN detector: for every window length W, the cut (length of the historical
 * sub-window) and the critical values of the F- and t-tests. They depend only on W, the confidence and the
 * robustness, never on the data, so they are constants and never part of checkpointed state.
 *
 * <p>切点按式 4.1 取最大根：在整数网格 c = 2 … W−2 上取满足 g(c/W) ≥ 0 的最大 c，其中
 * g(ν) = ρ − t_ppf(δ′, df(ν))·√(1/(νW) + f_ppf(δ′, νW−1, (1−ν)W−1)/((1−ν)W))，δ′ = δ^(1/4)。最大根落在
 * [c/W, (c+1)/W) 内，所以 ⌊νW⌋ = c。无解时按算法 2 取 ν = 0.5，即切点 ⌊W/2⌋。搜索从上往下以 ⌈W/64⌉ 为步长粗扫，命中后
 * 在步长之内逐个细查；这一做法由 Python 参考在每一个窗长上用逐个穷举的结果核对（{@code m5b/check_tables.py}）。
 * The cut is the highest root of Eq. 4.1 on the integer grid; without a root it is ⌊W/2⌋ (ν = 0.5, Algorithm 2).
 *
 * <p>两种自由度公式：{@link Mode#THESIS} 按论文式 4.12（正式使用）；{@link Mode#AUTHOR_CODE} 按作者公开代码（方差比项用
 * 1/f_ppf²，无解时切点取 ⌊W/2⌋+1），只用来与作者预先算好的切点文件交叉核对求根与分位数的数值方法。
 * Two degrees-of-freedom formulas: THESIS (Eq. 4.12, used in production) and AUTHOR_CODE (only to cross-check the
 * numerics against the authors' precomputed cut files).
 */
public final class OptWinTables {

    /** 最小窗长（论文 w_min）/ minimum window length. */
    public static final int W_MIN = 30;

    /** 自由度公式的来源 / source of the degrees-of-freedom formula. */
    public enum Mode { THESIS, AUTHOR_CODE }

    private static final double INV_CUM_ACCURACY = 1e-12;

    public final double confidence;
    public final double robustness;
    public final int wMax;
    public final Mode mode;
    /** cut[W]：历史子窗长度；W < W_MIN 时为 0 / historical sub-window length. */
    public final int[] cut;
    /** fCrit[W] = f_ppf(δ′, cut−1, W−cut−1)（式 4.7 的公式写法）/ F critical value. */
    public final double[] fCrit;
    /** tCrit[W] = t_ppf(δ′, df(cut/W)) / t critical value. */
    public final double[] tCrit;
    /** rootFound[W]：式 4.1 是否有解 / whether Eq. 4.1 has a root. */
    public final boolean[] rootFound;

    private final double deltaPrime;

    private OptWinTables(double confidence, double robustness, int wMax, Mode mode) {
        if (confidence <= 0 || confidence >= 1 || robustness <= 0 || wMax < W_MIN) {
            throw new IllegalArgumentException("置信度须在 (0,1)、鲁棒度须为正、窗上限不得小于 " + W_MIN);
        }
        this.confidence = confidence;
        this.robustness = robustness;
        this.wMax = wMax;
        this.mode = mode;
        this.deltaPrime = Math.pow(confidence, 0.25);
        this.cut = new int[wMax + 1];
        this.fCrit = new double[wMax + 1];
        this.tCrit = new double[wMax + 1];
        this.rootFound = new boolean[wMax + 1];
    }

    /** 论文口径的常量表 / tables per the thesis. */
    public static OptWinTables build(double confidence, double robustness, int wMax) {
        return build(confidence, robustness, wMax, Mode.THESIS);
    }

    public static OptWinTables build(double confidence, double robustness, int wMax, Mode mode) {
        OptWinTables t = new OptWinTables(confidence, robustness, wMax, mode);
        for (int w = W_MIN; w <= wMax; w++) {
            int c = t.highestRootCut(w);
            t.rootFound[w] = c > 0;
            if (c <= 0) {
                c = mode == Mode.THESIS ? w / 2 : w / 2 + 1;
            }
            t.cut[w] = c;
            t.fCrit[w] = t.fPpf(c - 1.0, w - c - 1.0);
            t.tCrit[w] = t.tPpf(t.df((double) c / w, w));
        }
        return t;
    }

    /** 式 4.1 的最大根对应的切点；无解返回 0 / cut of the highest root, or 0 when none. */
    private int highestRootCut(int w) {
        int step = Math.max(1, (w + 63) / 64);
        for (int c = w - 2; c >= 2; c -= step) {
            if (g(c, w) >= 0) {
                int best = c;
                for (int d = Math.min(w - 2, c + step - 1); d > c; d--) {
                    if (g(d, w) >= 0) {
                        best = d;
                        break;
                    }
                }
                return best;
            }
        }
        return 0;
    }

    /** g(c/W)，式 4.1 移项后的形式 / Eq. 4.1 rearranged. */
    private double g(int c, int w) {
        double nu = (double) c / w;
        double f = fPpf(c - 1.0, w - c - 1.0);
        return robustness - tPpf(df(nu, w, f)) * Math.sqrt(1.0 / (nu * w) + f / ((1 - nu) * w));
    }

    private double df(double nu, int w) {
        return df(nu, w, fPpf(nu * w - 1, (1 - nu) * w - 1));
    }

    /** 自由度：论文式 4.12，或作者代码的写法 / degrees of freedom per Eq. 4.12 or the authors' code. */
    private double df(double nu, int w, double f) {
        double nh = nu * w;
        double nn = (1 - nu) * w;
        double r = mode == Mode.THESIS ? f : 1.0 / (f * f);
        double a = 1.0 / nh;
        double b = r / nn;
        return (a + b) * (a + b) / (a * a / (nh - 1) + b * b / (nn - 1));
    }

    private double fPpf(double dfn, double dfd) {
        return new FDistribution(null, dfn, dfd, INV_CUM_ACCURACY).inverseCumulativeProbability(deltaPrime);
    }

    private double tPpf(double df) {
        return new TDistribution(null, df, INV_CUM_ACCURACY).inverseCumulativeProbability(deltaPrime);
    }

    /** 把常量表写成 CSV，供 Python 参考实现读入（与 Java 用同一张表）/ write the tables for the Python reference. */
    public void writeCsv(Path out) throws IOException {
        Files.createDirectories(out.toAbsolutePath().getParent());
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8))) {
            pw.printf(Locale.ROOT, "# confidence=%s robustness=%s wmax=%d mode=%s%n", confidence, robustness, wMax, mode);
            pw.println("w,cut,root_found,f_crit,t_crit");
            for (int w = W_MIN; w <= wMax; w++) {
                pw.println(w + "," + cut[w] + "," + (rootFound[w] ? 1 : 0) + ","
                           + Double.toString(fCrit[w]) + "," + Double.toString(tCrit[w]));
            }
        }
    }

    /**
     * 命令行：生成常量表 CSV。参数依次为置信度、鲁棒度、窗上限、输出文件，可选第五个参数 AUTHOR_CODE。
     * CLI: confidence robustness wmax out.csv [AUTHOR_CODE].
     */
    public static void main(String[] args) throws IOException {
        Mode mode = args.length > 4 ? Mode.valueOf(args[4]) : Mode.THESIS;
        build(Double.parseDouble(args[0]), Double.parseDouble(args[1]), Integer.parseInt(args[2]), mode)
                .writeCsv(Paths.get(args[3]));
    }
}
