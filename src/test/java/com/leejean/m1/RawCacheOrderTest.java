package com.leejean.m1;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 原始缓存算子移到标准化算子之前（适应执行第一批裁决第一项）不改变输出：同一批合成轮按旧顺序（标准化 → 原始缓存）与新顺序
 * （原始缓存 → 标准化）各走一遍，全部字段（含不写进 JSON 的游程长度与起点）逐位相同。合成轮覆盖预热与冻结、守卫游程与底值游程、
 * 冷启动缺口、缺失通道与光照删失。
 * Moving RawCache before RobustScaler does not change any output field (M6a ruling item 1).
 */
class RawCacheOrderTest {

    private static final int WARMUP = 300;

    private static List<DeviceRound> rounds() {
        Random rng = new Random(7);
        List<DeviceRound> out = new ArrayList<>();
        long ts = 1_646_092_800L;
        for (int i = 0; i < 1500; i++) {
            ts += (i == 900) ? 20_000 : 10;                   // 一次长缺席，触发冷启动 / one long absence: cold start
            DeviceRound r = new DeviceRound();
            r.setDevice("E");
            r.setTs(ts);
            double[] x = {
                    (i >= 400 && i < 470) ? 21.5 : Math.round((21 + rng.nextGaussian()) * 100) / 100.0,   // 温度卡死一段 / a stuck run
                    40 + rng.nextGaussian(),
                    101000 + Math.round(rng.nextGaussian() * 20),
                    700 + 30 * rng.nextGaussian(),
                    (i % 200 < 60) ? 0.0 : (rng.nextDouble() < 0.01 ? 65536.0 : Math.floor(rng.nextDouble() * 400))};
            boolean[] miss = new boolean[5];
            if (rng.nextDouble() < 0.01) {
                miss[rng.nextInt(5)] = true;
            }
            boolean[] cens = new boolean[5];
            cens[Channels.LIGHT_INDEX] = x[4] == 65536.0;
            r.setX(x);
            r.setMissingMask(miss);
            r.setCensoredMask(cens);
            out.add(r);
        }
        return out;
    }

    private static List<DeviceRound> through(KeyedProcessFunction<String, DeviceRound, DeviceRound> f, List<DeviceRound> in)
            throws Exception {
        List<DeviceRound> out = new ArrayList<>();
        try (KeyedOneInputStreamOperatorTestHarness<String, DeviceRound, DeviceRound> h =
                     new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(f), DeviceRound::getDevice,
                             TypeInformation.of(String.class))) {
            h.open();
            for (DeviceRound r : in) {
                h.processElement(new StreamRecord<>(r, r.getTs() * 1000L));
            }
            for (Object o : h.getOutput()) {
                if (o instanceof StreamRecord) {
                    out.add((DeviceRound) ((StreamRecord<?>) o).getValue());
                }
            }
        }
        return out;
    }

    private static RawCacheFunction cache() {
        return new RawCacheFunction(1000, 10, RepeatRunGuard.parse("E=30/0/0/0/40"),
                RepeatRunGuard.parseFloors("E.Light=0"));
    }

    private static RobustScalerFunction scaler() {
        return new RobustScalerFunction(WARMUP, 1e-9, ChannelTransform.defaultTable(), true);
    }

    private static String full(ObjectMapper om, DeviceRound r) throws Exception {
        return om.writeValueAsString(r) + Arrays.toString(r.getGuardRunLength()) + Arrays.toString(r.getGuardRunStartTs());
    }

    @Test
    @DisplayName("原始缓存移到标准化之前，全部输出字段逐位相同 / reordering keeps every output field identical")
    void reorderIsTransparent() throws Exception {
        List<DeviceRound> oldOrder = through(cache(), through(scaler(), rounds()));
        List<DeviceRound> newOrder = through(scaler(), through(cache(), rounds()));
        assertEquals(oldOrder.size(), newOrder.size());
        ObjectMapper om = new ObjectMapper();
        int stuck = 0;
        int cold = 0;
        int frozen = 0;
        for (int i = 0; i < oldOrder.size(); i++) {
            assertEquals(full(om, oldOrder.get(i)), full(om, newOrder.get(i)), "round " + i);
            stuck += oldOrder.get(i).stuckMaskBits() != 0 ? 1 : 0;
            cold += oldOrder.get(i).isColdStart() ? 1 : 0;
            frozen += oldOrder.get(i).isWarmup() ? 0 : 1;
        }
        assertTrue(stuck > 0 && cold > 0 && frozen > 0, "stuck " + stuck + ", cold " + cold + ", frozen " + frozen);
    }
}
