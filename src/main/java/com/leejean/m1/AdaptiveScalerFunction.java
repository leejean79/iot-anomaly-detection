package com.leejean.m1;

import com.leejean.m6.AdaptCommand;
import com.leejean.m6.Backfill;
import com.leejean.m6.CompactCache;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * 命令回路打开时的标准化算子（适应执行第一批，M6a 交接文档第四节「标准化算子」）：按设备键连接命令主题，内部包着原来的
 * {@link RobustScalerFunction}（预热、冻结与逐轮标准化原样执行），在它外面加三件事：
 * <ol>
 *   <li>紧凑原始缓存 {@link CompactCache}：每轮五个原始读数、时间戳与缺失、删失、守卫掩码，深度默认七天；</li>
 *   <li>命令：到达时若生效时刻不晚于数据流的当前水位线、或已经处理过生效时刻及之后的轮，记为迟到并计数（不执行）；否则存入状态，
 *       并注册生效时刻的事件时间定时器；</li>
 *   <li>零级：在事件时间到达生效时刻时执行。触发点取「定时器」与「第一条时间戳不早于生效时刻的轮」二者中先到的一个，两者看到的
 *       缓存都恰好是生效时刻之前的全部轮，所以执行与命令到达的先后无关。</li>
 * </ol>
 * 零级的定义：取缓存中 [生效 − D, 生效) 的轮，剔除守卫打标的轮，用与冻结相同的函数 {@link RobustScalerFunction#calibrate} 重估五个
 * 通道的中位数与分母并建参照直方图；版本号换为命令序号加一；参照直方图附在第一条新版本轮上，回填附件（[生效 − 窗长, 生效) 内点通道
 * 会接纳的轮，按新参数标准化）附在第一条点通道会接纳的新版本轮上。命令回路关闭时作业不用本类，输出与改动前逐位相同。
 * The scaler with the command loop on: wraps RobustScalerFunction and adds the compact cache, commands and level-0 execution.
 */
public class AdaptiveScalerFunction extends KeyedCoProcessFunction<String, DeviceRound, AdaptCommand, DeviceRound> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveScalerFunction.class);

    private final RobustScalerFunction inner;
    private final long cacheDepthSec;
    private final long reestimateSec;
    private final long backfillSec;

    private transient ValueState<CompactCache> cache;
    private transient ListState<AdaptCommand> pending;
    private transient ValueState<Integer> version;
    private transient ValueState<Long> lastRoundTs;
    private transient ValueState<Backfill> pendingBackfill;
    private transient Counter lateCommands;
    private transient Counter executed;
    private transient Counter ignored;

    /**
     * @param inner         原来的标准化函数（参数与关闭时相同）/ the unchanged scaler
     * @param cacheDepthSec 缓存深度（秒，默认七天）/ cache depth in seconds
     * @param reestimateSec 重估窗口 D（秒，默认一天）/ re-estimation window D in seconds
     * @param backfillSec   回填窗长（秒，等于点通道窗长）/ backfill window = the point-channel window length
     */
    public AdaptiveScalerFunction(RobustScalerFunction inner, long cacheDepthSec, long reestimateSec, long backfillSec) {
        if (reestimateSec > cacheDepthSec || backfillSec > cacheDepthSec) {
            throw new IllegalArgumentException("重估窗口与回填窗长不得超过缓存深度 / D and the backfill window must fit in the cache");
        }
        this.inner = inner;
        this.cacheDepthSec = cacheDepthSec;
        this.reestimateSec = reestimateSec;
        this.backfillSec = backfillSec;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        inner.setRuntimeContext(getRuntimeContext());
        inner.open(parameters);
        cache = getRuntimeContext().getState(new ValueStateDescriptor<>("m6-cache", TypeInformation.of(CompactCache.class)));
        pending = getRuntimeContext().getListState(new ListStateDescriptor<>("m6-pending", TypeInformation.of(AdaptCommand.class)));
        version = getRuntimeContext().getState(new ValueStateDescriptor<>("m6-version", Types.INT));
        lastRoundTs = getRuntimeContext().getState(new ValueStateDescriptor<>("m6-last-round-ts", Types.LONG));
        pendingBackfill = getRuntimeContext().getState(new ValueStateDescriptor<>("m6-backfill", TypeInformation.of(Backfill.class)));
        lateCommands = getRuntimeContext().getMetricGroup().counter("m6_late_commands");
        executed = getRuntimeContext().getMetricGroup().counter("m6_executed_commands");
        ignored = getRuntimeContext().getMetricGroup().counter("m6_ignored_commands");
    }

    @Override
    public void processElement1(DeviceRound round, Context ctx, Collector<DeviceRound> out) throws Exception {
        String device = round.getDevice();
        executeDue(device, round.getTs(), "round");
        CompactCache c = cache.value();
        if (c == null) {
            c = new CompactCache(cacheDepthSec);
        }
        c.add(round.getTs(), round.getX(), CompactCache.maskOf(round.getMissingMask(), round.getCensoredMask(), round.getStuckMask()));
        cache.update(c);
        lastRoundTs.update(round.getTs());
        boolean wasFrozen = inner.isFrozen();
        inner.processElement(round, null, new Collector<DeviceRound>() {
            @Override
            public void collect(DeviceRound r) {
                try {
                    tag(r, wasFrozen);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                out.collect(r);
            }

            @Override
            public void close() {
            }
        });
    }

    /** 冻结之后的轮带版本号（冻结为一）；回填附件挂在第一条点通道会接纳的轮上 / version and backfill on frozen rounds. */
    private void tag(DeviceRound r, boolean wasFrozen) throws Exception {
        if (!wasFrozen) {
            return;                                   // 预热期（含触发冻结的那一轮）不带版本 / warm-up rounds carry no version
        }
        Integer v = version.value();
        if (v == null) {
            v = 1;
            version.update(v);
        }
        r.setVersion(v);
        Backfill bf = pendingBackfill.value();
        if (bf != null && r.missingCount() == 0) {
            r.setBackfill(bf);
            pendingBackfill.clear();
        }
    }

    @Override
    public void processElement2(AdaptCommand cmd, Context ctx, Collector<DeviceRound> out) throws Exception {
        long wm = ctx.timerService().currentWatermark();
        Long last = lastRoundTs.value();
        long wmSec = Math.floorDiv(wm, 1000L);
        if (cmd.level != 0) {
            ignored.inc();
            LOG.info("[M6a] Command ignored (level {} not in batch 1): device {} seq {} effective {} arrival watermark {}; {}",
                    cmd.level, cmd.device, cmd.seq, cmd.effectiveAt, wmSec, cmd.raw);
            return;
        }
        boolean late = (long) cmd.effectiveAt * 1000L <= wm || (last != null && last >= cmd.effectiveAt);
        if (late) {
            lateCommands.inc();
            LOG.warn("[M6a] Late command (not executed): device {} seq {} effective {} arrival watermark {} last round {}; {}",
                    cmd.device, cmd.seq, cmd.effectiveAt, wmSec, last, cmd.raw);
            return;
        }
        pending.add(cmd);
        ctx.timerService().registerEventTimeTimer(cmd.effectiveAt * 1000L);
        LOG.info("[M6a] Command stored: device {} seq {} effective {} decided {} arrival watermark {}",
                cmd.device, cmd.seq, cmd.effectiveAt, cmd.decidedAt, wmSec);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DeviceRound> out) throws Exception {
        executeDue(DeviceKeys.deviceOf(ctx.getCurrentKey()), timestamp / 1000L, "timer");
    }

    /** 执行全部生效时刻不晚于 t 的命令，按生效时刻与序号排序 / execute every pending command with effectiveAt ≤ t. */
    private void executeDue(String device, long t, String trigger) throws Exception {
        List<AdaptCommand> all = new ArrayList<>();
        for (AdaptCommand c : pending.get()) {
            all.add(c);
        }
        if (all.isEmpty()) {
            return;
        }
        all.sort(Comparator.<AdaptCommand>comparingLong(c -> c.effectiveAt).thenComparingInt(c -> c.seq));
        List<AdaptCommand> keep = new ArrayList<>();
        for (AdaptCommand c : all) {
            if (c.effectiveAt <= t) {
                execute(device, c, trigger);
            } else {
                keep.add(c);
            }
        }
        pending.update(keep);
    }

    private void execute(String device, AdaptCommand cmd, String trigger) throws Exception {
        if (!inner.isFrozen()) {
            ignored.inc();
            LOG.warn("[M6a] Command ignored (scaler not frozen yet): device {} seq {} effective {}", device, cmd.seq, cmd.effectiveAt);
            return;
        }
        long t0 = System.nanoTime();
        CompactCache c = cache.value();
        Level0 r = level0(c, cmd.effectiveAt, reestimateSec, backfillSec, inner.epsilon(), inner.relativeGuardEnabled(),
                inner.transforms());
        long t1 = System.nanoTime();
        RobustScalerFunction.Calibration old = inner.current();
        Integer oldVersion = version.value();
        inner.install(r.cal);
        int newVersion = cmd.seq + 1;
        version.update(newVersion);
        pendingBackfill.update(r.backfill);
        executed.inc();
        LOG.info("[M6a] Level 0 executed: device {} seq {} effective {} trigger {} version {} -> {}; window {} rounds, {} guard-excluded; "
                        + "backfill {} rounds; reestimate {} us, total {} us; cache {} rows {} bytes; old median {} scale {}; "
                        + "new median {} scale {} bypass {}; {}",
                device, cmd.seq, cmd.effectiveAt, trigger, oldVersion, newVersion, r.windowRounds, r.guardExcluded,
                r.backfill.size(), (t1 - t0) / 1000L, (System.nanoTime() - t0) / 1000L, c.size(), c.usedBytes(),
                Arrays.toString(old.median), Arrays.toString(old.scale), Arrays.toString(r.cal.median),
                Arrays.toString(r.cal.scale), Arrays.toString(r.cal.bypass), cmd.raw);
    }

    /** 一次零级的结果 / the outcome of one level-0 execution. */
    public static final class Level0 {
        public RobustScalerFunction.Calibration cal;
        public Backfill backfill;
        public int windowRounds;
        public int guardExcluded;
    }

    /**
     * 零级的纯函数（在线算子与离线运行器共用）：从缓存取 [eff − D, eff) 的轮，剔除守卫打标的轮，缺失与光照删失的读数不计入，按冻结
     * 的同一函数重估；再取 [eff − 回填窗长, eff) 内检测通道无缺失的轮，按新参数标准化为回填附件。
     * The level-0 pure function shared by the online operator and the offline runner.
     */
    public static Level0 level0(CompactCache c, long eff, long reestimateSec, long backfillSec, double epsilon,
                                boolean relativeGuard, ChannelTransform[] tf) {
        int n = Channels.N_DET;
        List<List<Double>> samples = new ArrayList<>(n);
        for (int ch = 0; ch < n; ch++) {
            samples.add(new ArrayList<>());
        }
        Level0 r = new Level0();
        int size = c == null ? 0 : c.size();
        for (int k = 0; k < size; k++) {
            long ts = c.ts(k);
            if (ts < eff - reestimateSec || ts >= eff) {
                continue;
            }
            r.windowRounds++;
            int m = c.mask(k);
            if ((m >>> CompactCache.GUARD_SHIFT & 0x1f) != 0) {
                r.guardExcluded++;
                continue;
            }
            for (int ch = 0; ch < n; ch++) {
                boolean missing = (m >>> ch & 1) != 0;
                boolean censored = (m >>> (CompactCache.CENSORED_SHIFT + ch) & 1) != 0;
                if (missing || (ch == Channels.LIGHT_INDEX && censored)) {
                    continue;
                }
                samples.get(ch).add(tf[ch].apply(c.x(k, ch)));
            }
        }
        r.cal = RobustScalerFunction.calibrate(samples, epsilon, relativeGuard);
        List<Long> ts = new ArrayList<>();
        List<double[]> xn = new ArrayList<>();
        List<boolean[]> cens = new ArrayList<>();
        for (int k = 0; k < size; k++) {
            long t = c.ts(k);
            int m = c.mask(k);
            if (t < eff - backfillSec || t >= eff || (m & 0x1f) != 0) {
                continue;
            }
            double[] x = new double[n];
            boolean[] miss = new boolean[n];
            boolean[] ce = new boolean[n];
            for (int ch = 0; ch < n; ch++) {
                x[ch] = c.x(k, ch);
                ce[ch] = (m >>> (CompactCache.CENSORED_SHIFT + ch) & 1) != 0;
            }
            ts.add(t);
            xn.add(RobustScalerFunction.normalize(x, miss, r.cal.median, r.cal.scale, r.cal.bypass, tf));
            cens.add(ce);
        }
        r.backfill = new Backfill(ts.stream().mapToLong(Long::longValue).toArray(), xn.toArray(new double[0][]),
                cens.toArray(new boolean[0][]));
        return r;
    }
}
