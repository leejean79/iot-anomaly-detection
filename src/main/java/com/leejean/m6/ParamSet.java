package com.leejean.m6;

import java.io.Serializable;

/**
 * 一个参数版本的标准化参数集（M6b 决定十三，选项一）：五个中位数、五个分母、旁路标志。数据接入模块在冻结时与每次零级之后，
 * 把它附在该版本第一条点通道会接纳的轮上（与零级回填同一机制）；上下文模块缓存全部版本，按训练版本自己从原始值标准化。
 * The standardization parameters of one version (decision 13, option 1), attached to the version's first admitted round.
 */
public class ParamSet implements Serializable {
    private static final long serialVersionUID = 1L;

    public int version;
    /** 变换域中位数 / medians in the transformed domain. */
    public double[] median;
    /** 分母（四分位距或替代分母）/ denominators. */
    public double[] scale;
    /** 旁路标志 / bypass flags. */
    public boolean[] bypass;

    public ParamSet() {
    }

    public ParamSet(int version, double[] median, double[] scale, boolean[] bypass) {
        this.version = version;
        this.median = median.clone();
        this.scale = scale.clone();
        this.bypass = bypass.clone();
    }
}
