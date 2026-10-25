package com.leejean.m5a;

import com.leejean.m1.Channels;
import com.leejean.m1.DeviceRound;
import com.leejean.m1.RepeatRunGuard;
import com.leejean.source.Injector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M5a 离线实验移植的等值核对（操作口径第七节）：`eda/m5a_extract.py` 中 Injector 与 RepeatRunGuard 的 Python 移植，在同一段合成
 * 逐轮序列上与 Java 原类逐值相同。参照数据由 {@code m5a/make_port_fixtures.py} 生成。
 * The Python ports of Injector and RepeatRunGuard used by the M5a offline experiment match the Java classes value by value.
 */
class M5aPortTest {

    private static List<String> lines(String resource) throws Exception {
        List<String> out = new ArrayList<>();
        try (InputStream in = M5aPortTest.class.getResourceAsStream(resource);
             BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                out.add(line);
            }
        }
        return out;
    }

    @Test
    @DisplayName("注入与重复值游程守卫：Python 移植与 Java 原类逐值相同")
    void portsMatchJava() throws Exception {
        List<String> spec = lines("/m5a/port_spec.txt");
        Injector injector = new Injector(Injector.parse(spec.get(0)), null);
        int[] thr = RepeatRunGuard.parse(spec.get(1)).get("E");
        double[][] floors = RepeatRunGuard.parseFloors(spec.get(2)).get("E");
        RepeatRunGuard.RunState state = new RepeatRunGuard.RunState();
        List<String> rows = lines("/m5a/port_rounds.csv");
        int injected = 0;
        int stuckRounds = 0;
        for (String row : rows.subList(1, rows.size())) {
            String[] f = row.split(",");
            long ts = Long.parseLong(f[0]);
            double[] x = new double[Channels.N_DET];
            boolean[] missing = new boolean[Channels.N_DET];
            for (int c = 0; c < Channels.N_DET; c++) {
                if ("nan".equals(f[1 + c])) {
                    missing[c] = true;
                    continue;
                }
                String out = injector.apply(ts + ",E," + Channels.DETECTION[c] + "," + f[1 + c], ts, "E");
                x[c] = Double.parseDouble(out.split(",")[3]);
                assertEquals(Double.parseDouble(f[7 + c]), x[c], 0.0, "第 " + ts + " 轮通道 " + c + " 的注入值");
                if (x[c] != Double.parseDouble(f[1 + c])) {
                    injected++;
                }
            }
            DeviceRound r = new DeviceRound();
            r.setDevice("E");
            r.setTs(ts);
            r.setX(x);
            r.setMissingMask(missing);
            r.setColdStart("1".equals(f[6]));
            RepeatRunGuard.update(state, r, thr, floors);
            boolean anyStuck = false;
            for (boolean b : r.getStuckMask()) {
                anyStuck |= b;
            }
            assertEquals("1".equals(f[12]), anyStuck, "第 " + ts + " 轮的守卫标记");
            stuckRounds += anyStuck ? 1 : 0;
        }
        assertTrue(injected > 0 && stuckRounds > 0, "合成序列应同时含有注入与守卫打标");
    }
}
