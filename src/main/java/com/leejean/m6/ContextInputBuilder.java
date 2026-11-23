package com.leejean.m6;

import com.leejean.m1.Channels;
import com.leejean.m3.AnnotatedRound;
import com.leejean.m5.OptWinTables;

import java.io.IOException;
import java.util.List;

/**
 * 按在线运行的命令序列重建一台设备的上下文模块输入（第三批交接文档第二节，一级切换的离线复算）：用链式模拟器（D7、湿度与气压不随零级
 * 重估）执行其中的零级、转发一级与二级，产出与在线 PmcodFunction 侧输出同构的标注轮。零级的新参数只取决于重估窗口里的原始读数与
 * 守卫（链式离线已与 Python 参考逐位对照、与在线逐条相同），所以版本与参数集与那次在线运行相同。
 * Rebuild one device's context-module input from an online run's command list, via the chain simulator.
 */
public final class ContextInputBuilder {
    private ContextInputBuilder() {
    }

    /**
     * @param dev        设备 / device
     * @param roundsPath 该台轮表（E 用注入轮表）/ the device's round table (E: the injected one)
     * @param cmds       那次在线运行中该台的全部命令（任意级别）/ all of the device's commands in that run
     * @param lastDay    推进到这一当地日结束 / advance to the end of this local day
     */
    public static List<AnnotatedRound> build(String dev, String roundsPath, List<AdaptCommand> cmds, String lastDay)
            throws IOException {
        ChainDeviceSim sim = new ChainDeviceSim(dev, roundsPath, ChainDeviceSim.Variant.D7, ChainDriver.radius(dev),
                OptWinTables.build(0.99, 0.5, 2016));
        sim.keepChannels(Channels.parseSet("Humidity,Pressure"));
        sim.annotate(true);
        for (AdaptCommand c : cmds) {
            if (c.level == 0) {
                sim.schedule(c.effectiveAt, c.seq);
            } else {
                sim.scheduleContext(c);
            }
        }
        for (String day = "2022-03-01"; day.compareTo(lastDay) <= 0; day = com.leejean.m7.FleetDecisionFunction.addDays(day, 1)) {
            sim.advanceTo(com.leejean.m7.FleetDecisionFunction.dayStart(com.leejean.m7.FleetDecisionFunction.addDays(day, 1)) + 60);
        }
        return sim.annotated;
    }
}
