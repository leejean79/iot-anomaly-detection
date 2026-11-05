package com.leejean.m2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * MCOD 每设备状态：{PD 哈希表, MC 哈希表, mcCounter}（交接文档决策四）。
 * Per-device MCOD state: {PD map, MC map, mcCounter} (handover decision 4).
 *
 * <p>对应 {@code Pmcod.scala:13} 的 {@code case class McodState(PD, MC)}，外加把原文的普通成员变量
 * {@code mc_counter}（Pmcod.scala:22）**并入受 checkpoint 保护的算子状态**——修复理论文档风险 R8：
 * 原文把微簇计数器放在普通成员里，故障恢复后归零、导致微簇编号相撞。本类整体作为 Flink ValueState 存储，
 * 随 checkpoint 持久化，恢复后 mcCounter 延续、编号不撞。
 * Ports {@code McodState} and additionally folds the counter {@code mc_counter} (a plain member var in
 * the source) INTO the checkpointed state, fixing risk R8 (a counter that reset to zero on recovery and
 * caused micro-cluster id collisions). The whole object is stored as Flink ValueState.
 *
 * <p>PD 以点 id（long）为键（原文 {@code HashMap[Int, Data]}，id 拓宽后为 long）；MC 以微簇编号（int）为键。
 * PD is keyed by point id (long, widened); MC by micro-cluster id (int).
 */
public class McodState implements Serializable {
    private static final long serialVersionUID = 1L;

    public Map<Long, McodPoint> pd;         // Pmcod.scala:13 PD：非簇点集
    public Map<Integer, MicroCluster> mc;   // Pmcod.scala:13 MC：微簇集
    public int mcCounter;                   // Pmcod.scala:22 mc_counter（now in state，R8 修复）
    // 适应执行第一批（M6a）：该台当前的参数版本号（回路关闭时恒为空）与当前版本第一个点的轮时间戳（秒，之前的点只回填不判定）
    // M6a: the device's current parameter version (null when the loop is off) and the first point of that version.
    public Integer version;
    public long versionStart;

    public McodState() {
        this.pd = new HashMap<>();
        this.mc = new HashMap<>();
        this.mcCounter = 1;                 // 原文初值 mc_counter = 1（Pmcod.scala:22）
    }

    /** 冷启动清空：清 PD/MC 并复位计数器（该设备重新开始；此时无微簇，从 1 起编号安全）。 */
    public void clearForColdStart() {
        pd.clear();
        mc.clear();
        mcCounter = 1;
    }
}
