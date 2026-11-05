package com.leejean.m6;

import java.io.Serializable;

/**
 * 零级的回填附件（M6a 交接文档第四节）：生效时刻之前一个窗长内、点通道入口会接纳的轮（检测通道无缺失），按新参数标准化。
 * 附在第一条点通道会接纳的新版本轮上，由点异常检测模块清空状态之后回填；回填的点只建立邻居关系，不发出判定。
 * The level-0 backfill: rounds of the last window before the effective time, normalized with the new parameters.
 */
public class Backfill implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 轮时间戳（秒），升序 / round timestamps (s), ascending. */
    public long[] ts;
    /** 新参数下的标准化值，每轮五个 / normalized values under the new parameters. */
    public double[][] xNorm;
    /** 删失掩码 / censored masks. */
    public boolean[][] censored;

    public Backfill() {
    }

    public Backfill(long[] ts, double[][] xNorm, boolean[][] censored) {
        this.ts = ts;
        this.xNorm = xNorm;
        this.censored = censored;
    }

    public int size() {
        return ts == null ? 0 : ts.length;
    }
}
