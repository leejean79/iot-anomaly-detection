package com.leejean.m2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.m1.DeviceRound;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 重放完整性核验（补充指令五 step1）——每次标定/探针运行前的固定前置门槛。四条断言全部通过才允许下一步。
 * Replay-integrity verification (instruction 5 step1): a fixed prerequisite gate before any calibration
 * or probe run; all four assertions must pass to proceed.
 *
 * <p>四条断言 / four assertions（对 synergia-m1-out 转储的 DeviceRound JSONL 做**一遍**扫描得出）：
 * <ol>
 *   <li><b>轮数对账</b>：消费到的总轮数与探索性分析（EDA）记录的三月逐日轮数**合计**一致（相对误差 ≤ 容差）。
 *       EDA 权威值经 {@code --expected-total} 传入；未传入则判为"无参照"（退出码 3），不臆造参照。</li>
 *   <li><b>零重复</b>：同设备同时间戳（device+ts）的记录零重复——重发会在此暴露。</li>
 *   <li><b>边界对齐</b>：全局最早/最晚时间戳恰在时段边界（最早 = start；最晚 = end − 标称周期），
 *       允许 {@code --boundary-slack-sec} 的边界空档容差。</li>
 *   <li><b>冻结落第八天</b>：八台设备各自的标准化冻结时刻（首个 warmup=false 轮的事件时间）落在
 *       第八天区间 [start+calibDays 天, start+(calibDays+2) 天)。压缩/重发会使冻结提前到第四天左右，
 *       本断言即抓这种异常；逐台记入报告。</li>
 * </ol>
 *
 * <p>退出码 / exit codes：0 = 四条全过；1 = 有断言失败；3 = 断言一无 EDA 参照（需补 --expected-total）。
 *
 * ---------------------------- 脚本交付五要素 -------------------------------
 * 1. 执行环境 / Environment: 任意有 JDK 的机器（探针同款，master 临时 flink 容器即可）；输入为 m1-out 转储 JSONL。
 * 2. 调用命令 / Invocation:
 *      java -cp &lt;jar&gt; com.leejean.m2.ReplayVerify --rounds-jsonl m1out.jsonl \
 *          --start-utc 2022-03-01T00:00:00Z --end-utc 2022-04-01T00:00:00Z \
 *          --calib-days 7 --period-sec 10 --expected-total 2006400 --report-out verify.csv
 * 3. 前置条件 / Preconditions: 干净重放已完成、M1（--calib-days 7）已把整月消费进 synergia-m1-out。
 * 4. 期望产出 / Expected output: stdout 打印四条断言 PASS/FAIL + 逐台冻结日；report-out 写逐设备汇总 CSV；
 *      退出码见上（供 syn-replay-verify.sh 据以放行/拦截）。
 * 5. 失败兜底 / Failure fallback: 无法解析的行跳过并计数；任一断言失败即非零退出、拦住后续标定/探针。
 *
 * 缩写自查 / Abbreviations: EDA = 探索性数据分析；ts = 记录 epoch 秒；UTC = 协调世界时。
 */
public final class ReplayVerify {

    private ReplayVerify() { }

    /** 单轮的最小投影（核验只需三字段）/ minimal projection of a round for verification. */
    static final class Round {
        final String device;
        final long ts;
        final boolean warmup;
        Round(String device, long ts, boolean warmup) {
            this.device = device;
            this.ts = ts;
            this.warmup = warmup;
        }
    }

    /** 核验配置 / verification config. */
    static final class Config {
        final long startEpoch;      // 时段左界（含）/ period start (inclusive)
        final long endEpoch;        // 时段右界（不含）/ period end (exclusive)
        final int periodSec;        // 标称采样周期秒 / nominal sampling period
        final int calibDays;        // 标定窗口天数 / calibration window in days
        final long expectedTotal;   // EDA 权威总轮数；<0 表示未提供 / EDA authoritative total; <0 = absent
        final double tolPct;        // 轮数对账相对容差（%）/ count reconciliation tolerance
        final long boundarySlackSec;// 边界空档容差秒 / boundary slack
        Config(long startEpoch, long endEpoch, int periodSec, int calibDays,
               long expectedTotal, double tolPct, long boundarySlackSec) {
            this.startEpoch = startEpoch;
            this.endEpoch = endEpoch;
            this.periodSec = periodSec;
            this.calibDays = calibDays;
            this.expectedTotal = expectedTotal;
            this.tolPct = tolPct;
            this.boundarySlackSec = boundarySlackSec;
        }
    }

    /** 逐设备汇总 / per-device summary. */
    static final class DeviceStat {
        long total;
        long duplicates;
        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;
        long freezeTs = -1;         // 首个 warmup=false 轮的 ts / first frozen round's ts (-1 if none)
    }

    /** 四条断言的结果 / result of the four assertions. */
    static final class VerifyResult {
        boolean a1Count;
        boolean a1Inconclusive;     // 无 EDA 参照 / no EDA reference supplied
        boolean a2NoDup;
        boolean a3Boundary;
        boolean a4FreezeDay8;
        long total;
        long expectedTotal;
        long totalDuplicates;
        long globalMinTs = Long.MAX_VALUE;
        long globalMaxTs = Long.MIN_VALUE;
        final Map<String, DeviceStat> perDevice = new TreeMap<>();

        boolean allPass() {
            return a1Count && a2NoDup && a3Boundary && a4FreezeDay8;
        }
    }

    /**
     * 由已解析的轮序列与配置计算四条断言（纯函数，供单元测试）。
     * Compute the four assertions from parsed rounds and config (pure, unit-testable).
     */
    static VerifyResult verify(List<Round> rounds, Config cfg) {
        VerifyResult r = new VerifyResult();
        // 逐设备聚合，并用 (device→已见 ts 集合) 检测重复。
        Map<String, java.util.HashSet<Long>> seen = new java.util.HashMap<>();
        for (Round x : rounds) {
            DeviceStat st = r.perDevice.computeIfAbsent(x.device, d -> new DeviceStat());
            st.total++;
            r.total++;
            java.util.HashSet<Long> set = seen.computeIfAbsent(x.device, d -> new java.util.HashSet<>());
            if (!set.add(x.ts)) {
                st.duplicates++;
                r.totalDuplicates++;
            }
            if (x.ts < st.minTs) {
                st.minTs = x.ts;
            }
            if (x.ts > st.maxTs) {
                st.maxTs = x.ts;
            }
            if (x.ts < r.globalMinTs) {
                r.globalMinTs = x.ts;
            }
            if (x.ts > r.globalMaxTs) {
                r.globalMaxTs = x.ts;
            }
            // 首个已冻结（warmup=false）轮的 ts（按事件时间取最早）/ earliest frozen round ts
            if (!x.warmup && (st.freezeTs < 0 || x.ts < st.freezeTs)) {
                st.freezeTs = x.ts;
            }
        }

        // 断言一：轮数对账 / count reconciliation
        if (cfg.expectedTotal < 0) {
            r.a1Inconclusive = true;
            r.a1Count = false;
            r.expectedTotal = -1;
        } else {
            r.expectedTotal = cfg.expectedTotal;
            double rel = cfg.expectedTotal == 0 ? (r.total == 0 ? 0 : 1)
                    : Math.abs(r.total - cfg.expectedTotal) / (double) cfg.expectedTotal;
            r.a1Count = rel <= cfg.tolPct / 100.0;
        }

        // 断言二：零重复 / zero duplicates
        r.a2NoDup = r.totalDuplicates == 0;

        // 断言三：边界对齐 / boundary alignment
        long expectMax = cfg.endEpoch - cfg.periodSec;   // 时段内最后一轮 / last round in period
        boolean minOk = r.total > 0
                && Math.abs(r.globalMinTs - cfg.startEpoch) <= cfg.boundarySlackSec;
        boolean maxOk = r.total > 0
                && Math.abs(r.globalMaxTs - expectMax) <= cfg.boundarySlackSec;
        r.a3Boundary = minOk && maxOk;

        // 断言四：八台冻结落第八天区间 [start+calibDays 天, start+(calibDays+2) 天)
        long lo = cfg.startEpoch + (long) cfg.calibDays * 86400L;
        long hi = cfg.startEpoch + (long) (cfg.calibDays + 2) * 86400L;
        boolean allFreezeOk = !r.perDevice.isEmpty();
        for (DeviceStat st : r.perDevice.values()) {
            boolean ok = st.freezeTs >= lo && st.freezeTs < hi;
            if (!ok) {
                allFreezeOk = false;
            }
        }
        r.a4FreezeDay8 = allFreezeOk;
        return r;
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = parseArgs(args);
        String jsonl = a.getOrDefault("rounds-jsonl", "m1out.jsonl");
        String reportOut = a.getOrDefault("report-out", "");
        long start = parseUtc(a.getOrDefault("start-utc", "2022-03-01T00:00:00Z"));
        long end = parseUtc(a.getOrDefault("end-utc", "2022-04-01T00:00:00Z"));
        int periodSec = Integer.parseInt(a.getOrDefault("period-sec", "10"));
        int calibDays = Integer.parseInt(a.getOrDefault("calib-days", "7"));
        long expectedTotal = Long.parseLong(a.getOrDefault("expected-total", "-1"));
        double tolPct = Double.parseDouble(a.getOrDefault("tol-pct", "2.0"));
        long slack = Long.parseLong(a.getOrDefault("boundary-slack-sec", String.valueOf(periodSec)));
        Config cfg = new Config(start, end, periodSec, calibDays, expectedTotal, tolPct, slack);

        ObjectMapper mapper = new ObjectMapper();
        List<Round> rounds = new ArrayList<>();
        long parseErrors = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(jsonl))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                try {
                    DeviceRound dr = mapper.readValue(line, DeviceRound.class);
                    rounds.add(new Round(dr.getDevice(), dr.getTs(), dr.isWarmup()));
                } catch (Exception e) {
                    parseErrors++;
                }
            }
        }

        VerifyResult r = verify(rounds, cfg);
        printReport(r, cfg, parseErrors);
        if (!reportOut.isEmpty()) {
            writeReport(r, reportOut);
            System.out.println("[verify] 逐设备汇总 → " + reportOut);
        }

        if (r.a1Inconclusive) {
            System.out.println("退出码 3：断言一无 EDA 参照，请用 --expected-total <三月逐日轮数合计> 重跑。");
            System.exit(3);
        }
        if (!r.allPass()) {
            System.out.println("退出码 1：有断言未通过——**拦住后续标定/探针**，请先解决数据完整性问题。");
            System.exit(1);
        }
        System.out.println("退出码 0：四条断言全部通过，允许进入标定/探针。");
    }

    private static void printReport(VerifyResult r, Config cfg, long parseErrors) {
        System.out.println("========== 重放完整性核验 / replay-integrity verification ==========");
        System.out.printf("总轮数 %d；解析失败 %d；设备数 %d%n", r.total, parseErrors, r.perDevice.size());
        // 断言一
        if (r.a1Inconclusive) {
            System.out.printf("[断言一 轮数对账] 无参照 —— 实测总轮数 %d，请补 --expected-total（EDA 三月逐日合计）%n",
                    r.total);
        } else {
            System.out.printf("[断言一 轮数对账] %s —— 实测 %d vs 期望 %d（容差 %.1f%%）%n",
                    r.a1Count ? "PASS" : "FAIL", r.total, r.expectedTotal, cfg.tolPct);
        }
        // 断言二
        System.out.printf("[断言二 零重复]   %s —— 同设备同时间戳重复计数 %d%n",
                r.a2NoDup ? "PASS" : "FAIL", r.totalDuplicates);
        // 断言三
        System.out.printf("[断言三 边界对齐] %s —— 最早 %s（期望 %s）；最晚 %s（期望 %s）；容差 %ds%n",
                r.a3Boundary ? "PASS" : "FAIL",
                iso(r.globalMinTs), iso(cfg.startEpoch),
                iso(r.globalMaxTs), iso(cfg.endEpoch - cfg.periodSec), cfg.boundarySlackSec);
        // 断言四（逐台）
        long lo = cfg.startEpoch + (long) cfg.calibDays * 86400L;
        long hi = cfg.startEpoch + (long) (cfg.calibDays + 2) * 86400L;
        System.out.printf("[断言四 冻结落第八天] %s —— 期望冻结时刻 ∈ [%s, %s)；逐台：%n",
                r.a4FreezeDay8 ? "PASS" : "FAIL", iso(lo), iso(hi));
        for (Map.Entry<String, DeviceStat> e : r.perDevice.entrySet()) {
            DeviceStat st = e.getValue();
            boolean ok = st.freezeTs >= lo && st.freezeTs < hi;
            int dayIdx = st.freezeTs < 0 ? -1
                    : (int) ((st.freezeTs - cfg.startEpoch) / 86400L) + 1;   // 第几天（1 基）
            System.out.printf("    %s：冻结 %s（第 %d 天）%s%n",
                    e.getKey(), st.freezeTs < 0 ? "无(全程预热?)" : iso(st.freezeTs),
                    dayIdx, ok ? "" : "  ← 偏离第八天");
        }
    }

    private static void writeReport(VerifyResult r, String outCsv) throws java.io.FileNotFoundException {
        try (PrintWriter pw = new PrintWriter(outCsv)) {
            pw.println("device,total,duplicates,min_ts,max_ts,freeze_ts,freeze_iso");
            for (Map.Entry<String, DeviceStat> e : r.perDevice.entrySet()) {
                DeviceStat st = e.getValue();
                pw.printf("%s,%d,%d,%d,%d,%d,%s%n",
                        e.getKey(), st.total, st.duplicates, st.minTs, st.maxTs, st.freezeTs,
                        st.freezeTs < 0 ? "" : iso(st.freezeTs));
            }
        }
    }

    private static long parseUtc(String s) {
        return Instant.parse(s).getEpochSecond();
    }

    private static String iso(long epochSec) {
        if (epochSec == Long.MAX_VALUE || epochSec == Long.MIN_VALUE || epochSec < 0) {
            return "n/a";
        }
        return Instant.ofEpochSecond(epochSec).atOffset(ZoneOffset.UTC).toString();
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new java.util.HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            m.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        return m;
    }
}
