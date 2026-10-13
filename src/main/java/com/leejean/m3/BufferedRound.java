package com.leejean.m3;

import java.io.Serializable;

/**
 * 训练期间缓冲的一轮（补遗四第二节第 3 条）：标注轮本身加它进入算子时的记录时间戳（毫秒）。缓冲只存轮，
 * 不存窗口；窗口在排空时按原顺序重新装配，与同步训练时逐轮装配的行为一致。
 * A round buffered while training: the annotated round plus its record timestamp (ms). Windows are rebuilt in
 * order while draining, exactly as rounds were assembled under synchronous training.
 *
 * <p>Flink POJO：public 字段加无参构造。/ Flink POJO: public fields and a no-arg constructor.
 */
public class BufferedRound implements Serializable {
    private static final long serialVersionUID = 1L;

    public AnnotatedRound round;
    /** 记录时间戳（毫秒），排空时作为评分记录的时间戳，也是输出水位线保持的依据 / record timestamp (ms). */
    public long recordTs;

    public BufferedRound() { }

    public BufferedRound(AnnotatedRound round, long recordTs) {
        this.round = round;
        this.recordTs = recordTs;
    }
}
