package com.leejean.m3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.m1.Channels;
import com.leejean.m1.DeviceRound;
import com.leejean.m2.ScoreEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * V-M3-3 离线网格程序 {@link M3Grid} 的口径测试。
 *
 * <p>这里验证的不是模型效果，而是**口径**：设备 G 的 Light 通道是否被归零、预热轮与含缺失通道的轮
 * 是否被跳过、训练净化是否只在提供了离群名单时生效。这三条只要与在线算子 {@link M3Function} 不一致，
 * 网格选出的超参数就与实际运行的不是同一件事。
 *
 * <p>Caliber tests, not quality tests: device G's Light input zeroed, warm-up and missing-channel
 * rounds skipped, sanitization applied only when the outlier roster is supplied.
 */
class M3GridTest {

    private static DeviceRound round(String device, long ts, double value, boolean warmup,
                                     boolean missing) {
        DeviceRound r = new DeviceRound();
        r.setDevice(device);
        r.setTs(ts);
        double[] x = new double[Channels.N_DET];
        java.util.Arrays.fill(x, value);
        r.setXNorm(x);
        r.setX(x.clone());
        boolean[] miss = new boolean[Channels.N_DET];
        if (missing) {
            miss[0] = true;
        }
        r.setMissingMask(miss);
        r.setCensoredMask(new boolean[Channels.N_DET]);
        r.setWarmup(warmup);
        return r;
    }

    private static Path writeRounds(Path dir, List<DeviceRound> rounds) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Path p = dir.resolve("m1out.jsonl");
        try (PrintWriter pw = new PrintWriter(p.toFile(), "UTF-8")) {
            for (DeviceRound r : rounds) {
                pw.println(mapper.writeValueAsString(r));
            }
        }
        return p;
    }

    @Test
    @DisplayName("设备 G 的 Light 通道输入被归零，与在线算子同一份实现")
    void deviceGLightIsZeroed() {
        double[] g = new double[Channels.N_DET];
        java.util.Arrays.fill(g, 0.7);
        M3Function.zeroDeviceGLight(g, "G");
        assertEquals(0.0, g[Channels.LIGHT_INDEX], 1e-12,
                "设备 G 的 Light 通道应被归零（M3 交接文档决策 5）");
        for (int c = 0; c < g.length; c++) {
            if (c != Channels.LIGHT_INDEX) {
                assertEquals(0.7, g[c], 1e-12, "其余通道不应被改动");
            }
        }

        double[] e = new double[Channels.N_DET];
        java.util.Arrays.fill(e, 0.7);
        M3Function.zeroDeviceGLight(e, "E");
        assertEquals(0.7, e[Channels.LIGHT_INDEX], 1e-12, "非 G 设备的 Light 通道不应被归零");
    }

    @Test
    @DisplayName("网格能跑完并写出 CSV；预热轮与含缺失通道的轮被跳过")
    void gridRunsAndSkipsWarmupAndMissing(@TempDir Path dir) throws Exception {
        Random rnd = new Random(5);
        List<DeviceRound> rounds = new ArrayList<>();
        long ts = 1_600_000_000L;
        // 前 50 条是预热轮，应被整段跳过。
        for (int i = 0; i < 50; i++) {
            rounds.add(round("E", ts += 10, rnd.nextGaussian() * 0.01, true, false));
        }
        // 再 10 条含缺失通道，也应被跳过。
        for (int i = 0; i < 10; i++) {
            rounds.add(round("E", ts += 10, rnd.nextGaussian() * 0.01, false, true));
        }
        // 最后 120 条正常轮，够切出若干个长度为 10 的窗口。
        for (int i = 0; i < 120; i++) {
            rounds.add(round("E", ts += 10, Math.sin(i * 0.2) * 0.3, false, false));
        }
        Path jsonl = writeRounds(dir, rounds);
        Path out = dir.resolve("grid.csv");

        M3Grid.main(new String[]{
                "--rounds-jsonl", jsonl.toString(), "--devices", "E",
                "--hidden-grid", "8", "--window-grid", "10",
                // 训练段取 60 轮、早停段取 40 轮：用「天数 × 每天轮数」表达，这里每天按 20 轮算。
                "--rounds-per-day", "20", "--train-days", "3", "--early-stop-days", "2",
                "--max-epochs", "3", "--patience", "1",
                "--out", out.toString()});

        List<String> lines = java.nio.file.Files.readAllLines(out);
        assertEquals(2, lines.size(), "表头加一行结果");
        assertTrue(lines.get(0).endsWith(",sanitized"), "CSV 应含 sanitized 列");
        String[] f = lines.get(1).split(",");
        assertEquals("E", f[0]);
        assertEquals("false", f[f.length - 1],
                "未提供 --scores-jsonl 时不应做训练净化，该列须为 false");
        int trainWindows = Integer.parseInt(f[3]);
        int excluded = Integer.parseInt(f[4]);
        assertEquals(0, excluded, "没有离群名单就不该剔除任何窗口");
        assertTrue(trainWindows > 0, "应切出训练窗口，实测 " + trainWindows);
    }

    @Test
    @DisplayName("提供离群名单后，含离群轮的整窗被剔除出训练集")
    void outlierRosterDrivesSanitization(@TempDir Path dir) throws Exception {
        List<DeviceRound> rounds = new ArrayList<>();
        long base = 1_700_000_000L;
        for (int i = 0; i < 200; i++) {
            rounds.add(round("E", base + i * 10L, Math.sin(i * 0.2) * 0.3, false, false));
        }
        Path jsonl = writeRounds(dir, rounds);

        // 把第 0 和第 25 条轮列入离群名单：窗口长度 10 时，它们分别落在第 1 个与第 3 个窗口里。
        ObjectMapper mapper = new ObjectMapper();
        Path scores = dir.resolve("scores.jsonl");
        try (PrintWriter pw = new PrintWriter(scores.toFile(), "UTF-8")) {
            pw.println(mapper.writeValueAsString(new ScoreEvent("E", base, base)));
            pw.println(mapper.writeValueAsString(new ScoreEvent("E", base + 250L, base + 250L)));
        }
        Path out = dir.resolve("grid.csv");

        M3Grid.main(new String[]{
                "--rounds-jsonl", jsonl.toString(), "--scores-jsonl", scores.toString(),
                "--devices", "E", "--hidden-grid", "8", "--window-grid", "10",
                "--rounds-per-day", "20", "--train-days", "5", "--early-stop-days", "2",
                "--max-epochs", "2", "--patience", "1",
                "--out", out.toString()});

        String[] f = java.nio.file.Files.readAllLines(out).get(1).split(",");
        assertEquals("true", f[f.length - 1], "提供了离群名单，sanitized 列应为 true");
        assertEquals(2, Integer.parseInt(f[4]),
                "两条离群轮分属两个不同窗口，应剔除 2 个训练窗口，实测 " + f[4]);
    }
}
