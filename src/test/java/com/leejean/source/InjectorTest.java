package com.leejean.source;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Injector 单元测试（交接文档 §4 决策 8）：解析、四种注入类型、地面真值日志。
 * Injector unit tests (handover §4 decision 8): parsing, four injection types, ground-truth log.
 */
class InjectorTest {

    // ---- parse() 测试 / parse() tests ----

    @Test
    void parseSingleSpec() {
        List<Injector.Spec> specs = Injector.parse("E:Temperature:1711929600:300:spike:10");
        assertEquals(1, specs.size());
        Injector.Spec s = specs.get(0);
        assertEquals("E", s.device);
        assertEquals("Temperature", s.channel);
        assertEquals(1711929600L, s.startTs);
        assertEquals(1711929600L + 300, s.endTs);
        assertEquals("spike", s.type);
        assertEquals(10.0, s.magnitude, 1e-9);
    }

    @Test
    void parseMultipleSpecs() {
        List<Injector.Spec> specs = Injector.parse(
                "E:Temperature:1711929600:300:spike:10;F:Humidity:1711930000:600:step:5.5");
        assertEquals(2, specs.size());
        assertEquals("E", specs.get(0).device);
        assertEquals("F", specs.get(1).device);
        assertEquals("step", specs.get(1).type);
        assertEquals(5.5, specs.get(1).magnitude, 1e-9);
    }

    @Test
    void parseEmptyReturnsEmpty() {
        assertTrue(Injector.parse(null).isEmpty());
        assertTrue(Injector.parse("").isEmpty());
        assertTrue(Injector.parse("  ").isEmpty());
    }

    @Test
    void parseInvalidFieldCountThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Injector.parse("E:Temperature:1711929600:300:spike"));
    }

    @Test
    void parseUnknownTypeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Injector.parse("E:Temperature:1711929600:300:unknown:10"));
    }

    @Test
    void parseNumericErrorThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> Injector.parse("E:Temperature:abc:300:spike:10"));
    }

    // ---- apply() 测试 — spike / apply() tests — spike ----

    @Test
    void applySpike(@TempDir Path tmpDir) throws Exception {
        List<Injector.Spec> specs = Injector.parse("E:Temperature:100:10:spike:5.0");
        Injector inj = new Injector(specs, tmpDir.resolve("truth.csv"));
        try {
            // 在注入窗口内的行应被修改 / row within injection window should be modified
            String result = inj.apply("105,E,Temperature,20.0", 105, "E");
            assertEquals("105,E,Temperature,25.0", result);
            assertEquals(1, inj.getApplied());

            // 窗口外的行不受影响 / row outside window should not be affected
            String outside = inj.apply("111,E,Temperature,20.0", 111, "E");
            assertEquals("111,E,Temperature,20.0", outside);
            assertEquals(1, inj.getApplied());

            // 不同设备不受影响 / different device should not be affected
            String diffDev = inj.apply("105,F,Temperature,20.0", 105, "F");
            assertEquals("105,F,Temperature,20.0", diffDev);
        } finally {
            inj.close();
        }
    }

    // ---- apply() 测试 — step / apply() tests — step ----

    @Test
    void applyStep(@TempDir Path tmpDir) throws Exception {
        List<Injector.Spec> specs = Injector.parse("A:Humidity:200:50:step:3.0");
        Injector inj = new Injector(specs, tmpDir.resolve("truth.csv"));
        try {
            String result = inj.apply("210,A,Humidity,10.0", 210, "A");
            assertEquals("210,A,Humidity,13.0", result);
        } finally {
            inj.close();
        }
    }

    // ---- apply() 测试 — ramp / apply() tests — ramp ----

    @Test
    void applyRamp(@TempDir Path tmpDir) throws Exception {
        // ramp: magnitude * progress, progress = (ts - start) / (end - start)
        List<Injector.Spec> specs = Injector.parse("B:Temperature:1000:100:ramp:20.0");
        Injector inj = new Injector(specs, tmpDir.resolve("truth.csv"));
        try {
            // ts=1000: progress=0 → injected = original + 0 = 50.0
            String atStart = inj.apply("1000,B,Temperature,50.0", 1000, "B");
            assertEquals("1000,B,Temperature,50.0", atStart);

            // ts=1050: progress=0.5 → injected = 50 + 20*0.5 = 60.0
            String atMid = inj.apply("1050,B,Temperature,50.0", 1050, "B");
            assertEquals("1050,B,Temperature,60.0", atMid);

            // ts=1099: progress=0.99 → injected = 50 + 20*0.99 = 69.8
            String nearEnd = inj.apply("1099,B,Temperature,50.0", 1099, "B");
            String[] fields = nearEnd.split(",");
            double val = Double.parseDouble(fields[3]);
            assertEquals(69.8, val, 0.01);
        } finally {
            inj.close();
        }
    }

    // ---- apply() 测试 — stuck / apply() tests — stuck ----

    @Test
    void applyStuck(@TempDir Path tmpDir) throws Exception {
        // stuck: freeze at the first seen value
        List<Injector.Spec> specs = Injector.parse("C:RSSI:500:100:stuck:0");
        Injector inj = new Injector(specs, tmpDir.resolve("truth.csv"));
        try {
            // 第一行设定冻结值 / first row sets the frozen value
            String first = inj.apply("500,C,RSSI,42.5", 500, "C");
            assertEquals("500,C,RSSI,42.5", first);

            // 后续行始终返回冻结值 / subsequent rows always return the frozen value
            String second = inj.apply("510,C,RSSI,99.0", 510, "C");
            assertEquals("510,C,RSSI,42.5", second);

            String third = inj.apply("520,C,RSSI,0.5", 520, "C");
            assertEquals("520,C,RSSI,42.5", third);
        } finally {
            inj.close();
        }
    }

    // ---- 短行 / malformed lines ----

    @Test
    void applyShortLinePassthrough(@TempDir Path tmpDir) throws Exception {
        List<Injector.Spec> specs = Injector.parse("E:Temperature:100:10:spike:5.0");
        Injector inj = new Injector(specs, tmpDir.resolve("truth.csv"));
        try {
            String result = inj.apply("105,E,Temperature", 105, "E");
            assertEquals("105,E,Temperature", result);
        } finally {
            inj.close();
        }
    }

    // ---- 不同通道不匹配 / different channel does not match ----

    @Test
    void applyDifferentChannelNoMatch(@TempDir Path tmpDir) throws Exception {
        List<Injector.Spec> specs = Injector.parse("E:Temperature:100:10:spike:5.0");
        Injector inj = new Injector(specs, tmpDir.resolve("truth.csv"));
        try {
            String result = inj.apply("105,E,Humidity,20.0", 105, "E");
            assertEquals("105,E,Humidity,20.0", result);
            assertEquals(0, inj.getApplied());
        } finally {
            inj.close();
        }
    }

    // ---- 地面真值日志写入 / ground-truth log creation ----

    @Test
    void truthLogCreated(@TempDir Path tmpDir) throws Exception {
        Path logPath = tmpDir.resolve("truth.csv");
        List<Injector.Spec> specs = Injector.parse("E:Temperature:100:10:spike:5.0");
        Injector inj = new Injector(specs, logPath);
        inj.close();

        assertTrue(logPath.toFile().exists(), "Truth log file should be created");
        List<String> lines = java.nio.file.Files.readAllLines(logPath);
        assertEquals(2, lines.size());
        assertEquals("device,channel,start_ts,end_ts,type,magnitude", lines.get(0));
        assertTrue(lines.get(1).startsWith("E,Temperature,100,110,spike,"));
    }

    // ---- null truthLog 路径不崩溃 / null truth log path does not crash ----

    @Test
    void nullTruthLogDoesNotCrash() throws Exception {
        List<Injector.Spec> specs = Injector.parse("E:Temperature:100:10:spike:5.0");
        Injector inj = new Injector(specs, null);
        String result = inj.apply("105,E,Temperature,20.0", 105, "E");
        assertEquals("105,E,Temperature,25.0", result);
        inj.close();
    }
}
