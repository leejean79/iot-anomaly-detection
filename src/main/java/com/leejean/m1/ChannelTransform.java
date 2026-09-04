package com.leejean.m1;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 通道级预变换（收尾补充指令二，2026-09-04，设计会话裁决）：在 RobustScaler 估计中位数/四分位距**之前**
 * 对指定通道施加的单调预变换，用于把乘性/重尾通道拉回可标定的量级。
 * Per-channel pre-transform applied BEFORE the RobustScaler estimates median/IQR, to pull a
 * multiplicative / heavy-tailed channel back into a calibratable range.
 *
 * <p>当前只有 Light（ch4）取 {@link #LOG1P}，其余恒等；表结构为将来其他乘性通道预留（见 {@link #defaultTable()}）。
 * 判据来自 M2 收尾任务一的逐通道离散度诊断：C/D 的过度活跃几乎全部集中在 Light 通道的重尾（其标准化 P99
 * 达数十至数百个 IQR、超额峰度极高），而 Light 又是有已知右删失的通道（DF-11）；log1p 压掉该重尾后，
 * 删失顶格值 65536 变换为约 11.09，不再把距离与损失拽飞。
 *
 * <p><b>为何是 log1p（而非 log）</b>：加一使零照度映射到 0 而非 −∞（Light 原始值非负，最小为 0）。
 */
public enum ChannelTransform implements Serializable {

    /** 恒等（默认）/ identity. */
    IDENTITY {
        @Override
        public double apply(double v) {
            return v;
        }
    },

    /** 自然对数 log(1+v)：0→0，压缩乘性重尾 / natural log of (1+v). */
    LOG1P {
        @Override
        public double apply(double v) {
            return Math.log1p(v);
        }
    };

    /** 对原始检测值施加预变换 / apply the pre-transform to a raw detection value. */
    public abstract double apply(double v);

    /**
     * 默认通道预变换表：长度 = {@link Channels#N_DET}，仅 Light（{@link Channels#LIGHT_INDEX}）取 log1p，
     * 其余恒等。对**全部设备统一**施加（非逐设备）。
     * Default table: length N_DET, LOG1P only on the Light channel, IDENTITY elsewhere; applied uniformly
     * to all devices.
     */
    public static ChannelTransform[] defaultTable() {
        ChannelTransform[] t = new ChannelTransform[Channels.N_DET];
        Arrays.fill(t, IDENTITY);
        t[Channels.LIGHT_INDEX] = LOG1P;
        return t;
    }
}
