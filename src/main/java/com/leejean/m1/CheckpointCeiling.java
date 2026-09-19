package com.leejean.m1;

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MemorySize;

/**
 * 有效 checkpoint 状态上限的解释与打印 / resolve and report the effective checkpoint state ceiling.
 *
 * <p>本集群没有共享文件系统，因此用 {@code JobManagerCheckpointStorage}：状态随**确认 RPC** 一起
 * 送回 JobManager。于是真正的上限是两个数的较小者——作业参数 {@code --checkpoint-max-state-mb}
 * 与集群参数 {@code akka.framesize}。历史上多次把注意力放在前者，而实际越限的一直是后者
 * （曾观测到一条 27,329,480 字节的确认 RPC 撞上默认的 10 MB framesize）。
 *
 * <p>With {@code JobManagerCheckpointStorage} the state travels inside the acknowledge RPC, so the
 * effective ceiling is {@code min(--checkpoint-max-state-mb, akka.framesize)}. Report both.
 */
public final class CheckpointCeiling {

    private CheckpointCeiling() {
    }

    /** {@code akka.framesize} 的默认值（Flink 1.13）/ Flink 1.13's default. */
    private static final String DEFAULT_FRAMESIZE = "10485760b";

    /**
     * 读取集群侧的 {@code akka.framesize}（字节）；读不到时返回 -1。
     * 作业在 jobmanager 容器内提交，因此 {@link GlobalConfiguration} 读到的就是该容器的
     * {@code flink-conf.yaml}，也就是 compose 里 {@code FLINK_PROPERTIES} 的落地结果。
     * Read akka.framesize in bytes, or -1 when it cannot be determined.
     */
    public static long frameSizeBytes() {
        try {
            String raw = GlobalConfiguration.loadConfiguration()
                    .getString("akka.framesize", DEFAULT_FRAMESIZE);
            return MemorySize.parse(raw).getBytes();
        } catch (Exception e) {
            return -1L;
        }
    }

    /**
     * 打印两个上限与生效值。{@code ckptMaxStateMb} 是作业参数 {@code --checkpoint-max-state-mb}。
     * Print both ceilings and which one binds.
     */
    public static void print(int ckptMaxStateMb) {
        long frame = frameSizeBytes();
        System.out.println("Ckpt max state:  " + ckptMaxStateMb + " MB/subtask (memory-backed)");
        if (frame < 0) {
            System.out.println("Ckpt akka.framesize: <读取失败/unavailable>  "
                    + "有效上限 = min(--checkpoint-max-state-mb, akka.framesize) 无法确定");
            return;
        }
        double frameMb = frame / 1048576.0;
        boolean frameBinds = frameMb < ckptMaxStateMb;
        System.out.printf("Ckpt akka.framesize: %.1f MB%n", frameMb);
        System.out.printf("Ckpt effective ceiling: %.1f MB  [由 %s 决定 / bound by %s]%n",
                Math.min(frameMb, (double) ckptMaxStateMb),
                frameBinds ? "akka.framesize" : "--checkpoint-max-state-mb",
                frameBinds ? "akka.framesize" : "--checkpoint-max-state-mb");
    }
}
