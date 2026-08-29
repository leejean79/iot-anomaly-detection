package com.leejean.m2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 微簇：忠实迁移 {@code Pmcod.scala:15} 的 {@code case class MicroCluster(center, points)}。
 * Micro-cluster: faithful port of {@code Pmcod.scala:15}.
 *
 * <p>center 为创建该微簇的点的 value（Pmcod.scala:156 {@code new MicroCluster(el.value, NC)}）；
 * points 为微簇成员（含创建点，createMC 时 {@code NC += el}）。所有成员两两在 R 内（各自距 center ≤ R/2）。
 * center is the creating point's value; points are the members (including the creator).
 */
public class MicroCluster implements Serializable {
    private static final long serialVersionUID = 1L;

    public final double[] center;              // 微簇中心 = 创建点的 value
    public final List<McodPoint> points;       // 成员点

    public MicroCluster(double[] center, List<McodPoint> points) {
        this.center = center;
        this.points = points != null ? points : new ArrayList<>();
    }
}
