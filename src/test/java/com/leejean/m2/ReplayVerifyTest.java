package com.leejean.m2;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重放完整性核验单元测试（补充指令五 step1）：一份干净数据四条断言全过；再分别注入
 * 四种缺陷（轮数缺口、device+ts 重复、边界不齐、冻结提前=压缩窗口）各令对应断言失败。
 * Tests the four replay-integrity assertions: a clean fixture passes all four; four separate
 * defect injections each fail exactly the intended assertion.
 */
class ReplayVerifyTest {

    private static final long START = Instant.parse("2022-03-01T00:00:00Z").getEpochSecond();
    private static final long END = Instant.parse("2022-04-01T00:00:00Z").getEpochSecond();
    private static final int PERIOD = 10;
    private static final int CALIB_DAYS = 7;
    private static final String[] DEVS = {"A", "B"};

    /**
     * 造一份"干净"数据：两台设备，每 STEP 秒一轮从 START 到 END−PERIOD；前 7 天 warmup=true，
     * 之后 warmup=false（冻结落在第 8 天首轮）。为控制规模用较大步长 STEP，但仍覆盖到边界与第 8 天。
     */
    private static List<ReplayVerify.Round> cleanRounds(long stepSec) {
        List<ReplayVerify.Round> rounds = new ArrayList<>();
        long freezeBoundary = START + 7L * 86400L;   // 第 8 天起 warmup=false
        long lastTs = END - PERIOD;                  // 时段内最后一轮 / last round in period
        for (String d : DEVS) {
            long ts = START;
            for (; ts <= lastTs; ts += stepSec) {
                rounds.add(new ReplayVerify.Round(d, ts, ts < freezeBoundary));
            }
            // 确保恰好命中右边界（步长通常整除不到）/ ensure the exact right boundary is present
            if ((ts - stepSec) != lastTs) {
                rounds.add(new ReplayVerify.Round(d, lastTs, false));
            }
        }
        return rounds;
    }

    private static ReplayVerify.Config cfg(long expectedTotal) {
        return new ReplayVerify.Config(START, END, PERIOD, CALIB_DAYS, expectedTotal, 2.0, PERIOD);
    }

    @Test
    void cleanDataPassesAllFour() {
        List<ReplayVerify.Round> rounds = cleanRounds(600);   // 每 10 分钟一轮
        ReplayVerify.VerifyResult r = ReplayVerify.verify(rounds, cfg(rounds.size()));
        assertTrue(r.a1Count, "轮数对账应通过");
        assertTrue(r.a2NoDup, "应无重复");
        assertTrue(r.a3Boundary, "边界应对齐");
        assertTrue(r.a4FreezeDay8, "冻结应落第八天");
        assertTrue(r.allPass());
    }

    @Test
    void missingEdaReferenceIsInconclusive() {
        List<ReplayVerify.Round> rounds = cleanRounds(600);
        ReplayVerify.VerifyResult r = ReplayVerify.verify(rounds, cfg(-1));   // 未提供 expected-total
        assertTrue(r.a1Inconclusive, "无 EDA 参照应判为无法定论");
        assertFalse(r.a1Count);
        assertFalse(r.allPass());
    }

    @Test
    void countShortfallFailsAssertionOne() {
        List<ReplayVerify.Round> rounds = cleanRounds(600);
        // 期望值比实测高 10%（超 2% 容差）→ 断言一失败
        long inflated = Math.round(rounds.size() * 1.10);
        ReplayVerify.VerifyResult r = ReplayVerify.verify(rounds, cfg(inflated));
        assertFalse(r.a1Count, "轮数缺口应令断言一失败");
        assertTrue(r.a2NoDup);
        assertTrue(r.a3Boundary);
        assertTrue(r.a4FreezeDay8);
    }

    @Test
    void duplicateDeviceTsFailsAssertionTwo() {
        List<ReplayVerify.Round> rounds = cleanRounds(600);
        // 重发一条：复制某设备某时间戳 → device+ts 重复
        rounds.add(new ReplayVerify.Round("A", START + 600, false));
        ReplayVerify.VerifyResult r = ReplayVerify.verify(rounds, cfg(rounds.size()));
        assertFalse(r.a2NoDup, "device+ts 重复应令断言二失败");
        assertTrue(r.totalDuplicates >= 1);
    }

    @Test
    void offBoundaryFailsAssertionThree() {
        List<ReplayVerify.Round> rounds = new ArrayList<>();
        long freezeBoundary = START + 7L * 86400L;
        // 起点晚于 start 超过容差（迟到 1 小时）→ 最早时间戳不齐 → 断言三失败
        for (String d : DEVS) {
            for (long ts = START + 3600; ts <= END - PERIOD; ts += 600) {
                rounds.add(new ReplayVerify.Round(d, ts, ts < freezeBoundary));
            }
        }
        ReplayVerify.VerifyResult r = ReplayVerify.verify(rounds, cfg(rounds.size()));
        assertFalse(r.a3Boundary, "起点晚于边界应令断言三失败");
    }

    @Test
    void compressedWindowFreezesEarlyAndFailsAssertionFour() {
        // 模拟"重发压缩窗口"：冻结提前到第 4 天（warmup 边界设在 start+3.5 天）→ 断言四失败
        List<ReplayVerify.Round> rounds = new ArrayList<>();
        long earlyFreeze = START + (long) (3.5 * 86400L);
        long lastTs = END - PERIOD;
        for (String d : DEVS) {
            long ts = START;
            for (; ts <= lastTs; ts += 600) {
                rounds.add(new ReplayVerify.Round(d, ts, ts < earlyFreeze));
            }
            if ((ts - 600) != lastTs) {
                rounds.add(new ReplayVerify.Round(d, lastTs, false));   // 命中右边界
            }
        }
        ReplayVerify.VerifyResult r = ReplayVerify.verify(rounds, cfg(rounds.size()));
        assertFalse(r.a4FreezeDay8, "冻结提前（压缩窗口）应令断言四失败");
        // 其余三条仍应通过，证明断言四是**独立**抓到压缩窗口的
        assertTrue(r.a1Count);
        assertTrue(r.a2NoDup);
        assertTrue(r.a3Boundary);
    }
}
