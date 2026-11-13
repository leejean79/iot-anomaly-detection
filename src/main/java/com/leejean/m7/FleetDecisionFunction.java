package com.leejean.m7;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 聚合模块的决策函数（交接文档 {@code handover_m7_decision_stage_zh.md} 1.24 版第四节）：小时节拍给出带范围与性质归因的告警，日节拍
 * 按时间形态与决策矩阵发出零级命令，日界复核补记当日才知道的事。在线算子与 Java 离线运行器共用本类，Python 参考实现
 * {@code m7/decide.py} 与本类逐条相同（验收项一）。本类只依赖调用者按事件时间送入的观测与事件，不依赖处理时间。
 * The M7 decision function shared by the online operator and the offline runner; mirrors m7/decide.py step by step.
 *
 * <p>调用顺序：先用 {@code add*} 方法送入事件（性能层事件与元素、爆发、数据层日结论），再对每个告警节拍调用 {@link #alertTick}，
 * 在每个决策节拍调用 {@link #cmdTick}（默认即每个当地日的最后一个整点之后调用 {@link #dayTick}）。交接文档未写死、此处选定的
 * 写法与 Python 实现相同（见 decide.py 文件头 a 至 d）；日内决策节拍的在场设备按前一日计（e，见 {@link #cmdTick}）。
 */
public class FleetDecisionFunction implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final ZoneId ZONE = ZoneId.of("Europe/London");
    public static final String[] CHANNELS = {"Temperature", "Humidity", "Pressure", "Gas", "Light"};
    public static final String[] ACTIVE = {"Temperature", "Gas", "Light"};      // 气压与湿度只记录 / record-only
    static final int SLIDE = 60;
    static final int NOMINAL_PER_HOUR = 360;

    public static final String QUALITY = "质量问题";
    public static final String SWITCH = "分布切换";
    public static final String DRIFT = "漂移";
    public static final String SITE = "现场条件";
    public static final String TRANSIENT = "瞬态事件（待日界判定）";
    public static final String ENVIRONMENT = "环境事件";
    public static final String ANOMALY = "异常";

    /** 参数 / parameters. */
    public static final class Params implements Serializable {
        private static final long serialVersionUID = 1L;
        public double pPoint;
        public double pCtx = 0.5;
        public int kSite = 3;
        public int siteWindowDays = 7;
        public int kSiteDrift = 7;
        public int siteDriftWindowDays = 14;
        public double syncFrac = 0.5;
        public int baselineDays = 10;
        public int commonNum = 3;
        public int commonDen = 4;
        public int minGlobal = 2;
        public double qualityMax = 0.2;
        public double pointMinFrac = 0.5;
        public boolean oncePerEpisode = true;
        public boolean useData = true;
        public boolean usePerf = true;
        public boolean useDetect = true;
        public long deltaSec;                                 // 生效提前量 Δ；0 表示一个决策节拍 / 0 means one τ_cmd
        public long alertSec = 3600;                          // 告警节拍 τ_alert：3600 或 600 / alert tick period
        public long cmdSec;                                   // 决策节拍 τ_cmd；0 表示一个当地日 / 0 means one local day
        public long waitSec;                                  // 等待时长 W；0 表示一个日界 / 0 means one day boundary
        // ---- 决策矩阵升级格（1.41 版裁决第三节；默认关闭，使离线运行器与收口基线逐字节相同）/ upgrade cells (off by default)
        public boolean upgradeCells;
        public int kUpDays = 7;                               // K_up（当地日）/ K_up in local days
        public int upCooldownDays = 7;                        // 一级与二级的冷却期：重训完成后七个当地日 / level-1/2 cooldown
        public int retrainDays = 3;                           // 一级的 N（与上下文模块同源；按秒计）/ level-1 N, in 86,400 s units
        public boolean retrainAfter = true;                   // 一级训练窗口在生效之后 / level-1 training window after the effect
        public int level2Days = 16;                           // 二级的收集长度（训练加早停加标定，按秒计）/ level-2 collection

        /** 一个告警节拍的名义轮数（每十秒一轮）/ nominal rounds per alert tick. */
        double nominal() {
            return NOMINAL_PER_HOUR * alertSec / 3600.0;
        }

        /** 共模门槛 max(最少台数, ⌈n·比例⌉) / common-mode threshold. */
        public int threshold(int n) {
            return Math.max(minGlobal, (n * commonNum + commonDen - 1) / commonDen);
        }

        boolean qualityBad(int rounds, int guard) {
            double missing = 1.0 - (double) rounds / nominal();
            return missing > qualityMax || (double) guard / nominal() > qualityMax;
        }
    }

    /** 一台设备一个告警节拍的观测 / one device observation over one alert tick. */
    public static final class HourObs implements Serializable {
        private static final long serialVersionUID = 1L;
        public int rounds;
        public int admitted;
        public int guardRounds;
        public int outlierRounds;
        public int ctxAbove;
        public String ctxMax = "";            // 原样输出 / emitted verbatim
        public final int[] n = new int[ACTIVE.length];
        public final double[] sum = new double[ACTIVE.length];
        // 该节拍内轮的参数版本号的最大值（M6b 决定十四；命令回路关闭时为一）/ max parameter version of the tick's rounds (1 when the loop is off)
        public int version = 1;
    }

    /** 告警、命令、复核三种输出 / the three outputs. */
    public static final class Alert implements Serializable {
        private static final long serialVersionUID = 1L;
        public String device;
        public long hourStart;
        public String localHour;
        public String label;
        public String scope;
        public String co;
        public Double pointRate;
        public String ctxMax;
        public Double perfMagnitude;
        public String dataChannels;
        public int burst;
        public int pointFlag;                 // 该节拍的检测标志，主标签被覆盖时仍可查 / detection flags kept in the payload
        public int ctxFlag;
        public String note;
        // 该节拍末上下文模块的模型代数（1.41 版裁决第二节第五条；从未换过模型时为空、不写进 JSON）/ model generation
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public Integer model;
    }

    public static final class Command implements Serializable {
        private static final long serialVersionUID = 1L;
        public String device;
        public String day;
        public long decidedAt;
        public long effectiveAt;
        public int seq;
        public int level;
        public String scope;
        public String label;
        public String triggers;
        public String forms;
        public int co;
        public String coDevices;
        public int episodeN;
        // 决定依据（1.41 版裁决第二、三节；升级格关闭且未换过模型时为空、不写进 JSON）/ basis of the decision
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public String cell;                   // 命中的决策矩阵格 / the decision-matrix cell
        @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        public Integer model;                 // 决定时刻上下文模块的模型代数 / model generation at the decision
    }

    public static final class Review implements Serializable {
        private static final long serialVersionUID = 1L;
        public String device;
        public String day;
        public String kind;
        public String hourStart;
        public String detail;
    }

    /** 性能层的确认与回落 / perf confirmation and fallback. */
    static final class PerfTracker implements Serializable {
        private static final long serialVersionUID = 1L;
        final List<double[]> items = new ArrayList<>();   // {ts, kind(0 事件,1 元素,2 模型代数改变), level(1=确认), value, magnitude}
        int i;
        boolean sorted = true;
        boolean confirmed;
        long confirmTs;
        // 因模型切换而结束的确认段的时刻（1.44 版裁决第七条；日界复核时写出后清空）/ confirmation segments ended by a switch
        final List<Long> switchEnded = new ArrayList<>();
        Long since;
        double threshold;
        Double magnitude;
        final List<Double> post = new ArrayList<>();

        void add(double[] it) {
            items.add(it);
            sorted = false;
        }

        void advance(long t) {
            if (!sorted) {
                List<double[]> rest = new ArrayList<>(items.subList(i, items.size()));
                rest.sort((a, b) -> a[0] != b[0] ? Double.compare(a[0], b[0]) : Double.compare(a[1], b[1]));
                items.subList(i, items.size()).clear();
                items.addAll(rest);
                sorted = true;
            }
            while (i < items.size() && items.get(i)[0] <= t) {
                double[] it = items.get(i++);
                long ts = (long) it[0];
                if (it[1] == 2) {
                    // 模型代数改变：旧门限与新评分器不在同一尺度，清空跟踪；进行中的确认段记为因模型切换而结束
                    // Model generation changed: clear the tracking; an open confirmation segment ends because of the switch.
                    if (confirmed) {
                        switchEnded.add(ts);
                    }
                    confirmed = false;
                    since = null;
                    magnitude = null;
                    post.clear();
                } else if (it[1] == 0) {
                    if (it[2] == 1) {
                        if (!confirmed) {
                            since = ts;
                        }
                        confirmed = true;
                        confirmTs = ts;
                        threshold = it[3];
                        magnitude = it[4];
                        post.clear();
                    }
                } else if (confirmed && ts > confirmTs) {
                    post.add(it[3]);
                    if (post.size() >= 6) {
                        double[] s = new double[6];
                        for (int k = 0; k < 6; k++) {
                            s[k] = post.get(post.size() - 6 + k);
                        }
                        Arrays.sort(s);
                        if ((s[2] + s[3]) / 2.0 <= threshold) {
                            confirmed = false;
                        }
                    }
                }
            }
            if (i > 4096) {                                   // 已处理的条目不再需要，在线状态不随时间增长 / prune processed items
                items.subList(0, i).clear();
                i = 0;
            }
            if (post.size() > 64) {
                post.subList(0, post.size() - 6).clear();
            }
        }
    }

    private static final class Book implements Serializable {
        private static final long serialVersionUID = 1L;
        String scope;
        List<String> triggers;
    }

    private final Params p;
    private final TreeSet<String> devices = new TreeSet<>();
    private final Map<String, PerfTracker> perf = new HashMap<>();
    private final Map<String, List<long[]>> bursts = new HashMap<>();              // {报警元素, 发出时刻}
    private final Map<String, String[]> dataDays = new HashMap<>();                // key dev|day|ch → {evaluable, deviated, transition}
    private final Map<String, Boolean> evaluable = new HashMap<>();
    private final Map<String, Integer> dayVersion = new HashMap<>();               // dev|day → 日结论的参数版本 / version of the day's conclusions
    private final Map<String, Integer> curVersion = new HashMap<>();               // dev → 当前参数版本 / current parameter version
    private final Map<String, double[]> refs = new HashMap<>();                    // dev|ch → {p10, p90}
    private final Map<String, String> freezeDay = new HashMap<>();
    private final Map<String, Set<String>> flagHist = new HashMap<>();             // dev|hod → days
    private final Map<String, Set<String>> siteHist = new HashMap<>();
    private final Map<String, double[]> base = new HashMap<>();                    // dev|ch|hod → {sum, n}
    private final TreeMap<String, Book> drift = new TreeMap<>();
    private final Map<String, Map<String, Integer>> episode = new HashMap<>();     // dev → {触发类: 第几次} / per trigger class
    private final Map<String, Long> cooldownUntil = new HashMap<>();
    // 升级格的状态（1.41 版裁决第三节）/ upgrade-cell state
    private final Map<String, Long> cooldown1Until = new HashMap<>();               // 一级的冷却截止 / level-1 cooldown end
    private final Map<String, Long> cooldown2Until = new HashMap<>();               // 二级的冷却截止 / level-2 cooldown end
    private final Map<String, Long> level1Done = new HashMap<>();                   // dev → 最近一条一级的重训完成时刻
    private final Map<String, Long> level0Eff = new HashMap<>();                    // dev|ch → 最近一条含该通道的零级的生效时刻
    private final Map<String, TreeMap<Long, Integer>> modelGen = new HashMap<>();   // dev → {时刻 → 模型代数}（只记变化）
    private final Map<String, Integer> seq = new HashMap<>();
    private final List<Alert> dayAlerts = new ArrayList<>();

    public FleetDecisionFunction(Params p) {
        this.p = p;
    }

    // ================================================================ 输入 / inputs
    public void addDevice(String dev) {
        devices.add(dev);
        perf.computeIfAbsent(dev, k -> new PerfTracker());
        bursts.computeIfAbsent(dev, k -> new ArrayList<>());
    }

    public void setFreeze(String dev, long freezeTs) {
        freezeDay.put(dev, localDay(freezeTs));
    }

    public void setRef(String dev, String ch, double p10, double p90) {
        refs.put(dev + "|" + ch, new double[]{p10, p90});
    }

    public void addPerfEvent(String dev, long ts, String level, double threshold, double magnitude) {
        addDevice(dev);
        if (p.usePerf) {
            perf.get(dev).add(new double[]{ts, 0, "CONFIRM".equals(level) ? 1 : 0, threshold, magnitude});
        }
    }

    public void addPerfElement(String dev, long ts, double mainScore) {
        addDevice(dev);
        perf.get(dev).add(new double[]{ts, 1, 0, mainScore, 0});
    }

    /**
     * 上下文评分所带的模型代数（1.41 版裁决第二节第五条；空或零表示从未换过模型）：只记变化，节拍按时刻查。
     * The model generation carried by a contextual score; only changes are kept, looked up by time.
     */
    public void setModel(String dev, long ts, int gen) {
        TreeMap<Long, Integer> m = modelGen.computeIfAbsent(dev, k -> new TreeMap<>());
        Map.Entry<Long, Integer> prev = m.floorEntry(ts);
        if (gen > 0 && (prev == null || prev.getValue() != gen)) {
            m.put(ts, gen);
            addDevice(dev);
            perf.get(dev).add(new double[]{ts, 2, 0, 0, 0});   // 性能层跟踪随模型代数清空（1.44 版）/ clear on a new generation
        }
    }

    /** 时刻 t 时的模型代数，从未换过时为空 / model generation at t, null if never changed. */
    Integer modelAt(String dev, long t) {
        TreeMap<Long, Integer> m = modelGen.get(dev);
        Map.Entry<Long, Integer> e = m == null ? null : m.floorEntry(t);
        return e == null ? null : e.getValue();
    }

    public void addBurst(String dev, long ts, long emittedAt) {
        addDevice(dev);
        if (p.useData) {
            bursts.get(dev).add(new long[]{ts, emittedAt});
        }
    }

    public void addDataDay(String dev, String day, String ch, boolean ev, boolean deviated, String transition) {
        addDataDay(dev, day, ch, ev, deviated, transition, 1);
    }

    /** 带参数版本号的数据层日结论（M6b 决定十四）/ a data-layer day conclusion with its parameter version. */
    public void addDataDay(String dev, String day, String ch, boolean ev, boolean deviated, String transition, int version) {
        dataDays.put(dev + "|" + day + "|" + ch, new String[]{ev ? "1" : "0", deviated ? "1" : "0", transition});
        evaluable.put(dev + "|" + day, ev);
        dayVersion.put(dev + "|" + day, version);
    }

    /**
     * 该日的数据层结论是否与该台当前版本相同（M6b 决定十四）：不同版本的日结论一律视为不存在。
     * Whether the day's data-layer conclusions carry the device's current version; others are treated as absent.
     */
    private boolean sameVersion(String dev, String day) {
        return dayVersion.getOrDefault(dev + "|" + day, 1).intValue() == curVersion.getOrDefault(dev, 1).intValue();
    }

    private boolean dataDev(String dev, String day, String ch) {
        if (!p.useData || !sameVersion(dev, day)) {
            return false;
        }
        String[] r = dataDays.get(dev + "|" + day + "|" + ch);
        return r != null && "1".equals(r[1]);
    }

    private String dataTrans(String dev, String day, String ch) {
        if (!p.useData || !sameVersion(dev, day)) {
            return "";
        }
        String[] r = dataDays.get(dev + "|" + day + "|" + ch);
        return r == null ? "" : r[2];
    }

    private boolean isEvaluable(String dev, String day) {
        return sameVersion(dev, day) && evaluable.getOrDefault(dev + "|" + day, false);
    }

    /**
     * 该台的参数版本变化（零级执行，数据层状态随之清零）：数据层通道的「同一段偏离」与漂移状态随之结束，性能层的段不受影响
     * （性能层的版本是训练版本，与零级无关）。/ A version change ends the data-channel episodes; the perf episode is unaffected.
     */
    private void onVersion(String dev, int v) {
        curVersion.put(dev, v);
        Book b = drift.get(dev);
        if (b != null) {
            b.triggers.removeIf(tr -> !"perf".equals(tr));
            if (b.triggers.isEmpty()) {
                drift.remove(dev);
            }
        }
        Map<String, Integer> ep = episode.get(dev);
        if (ep != null) {
            ep.keySet().removeIf(tr -> !"perf".equals(tr));
            if (ep.isEmpty()) {
                episode.remove(dev);
            }
        }
    }

    // ================================================================ 小时节拍 / hourly tick
    private static final class Info {
        boolean bad;
        Double point;
        boolean pflag;
        boolean cflag;
        boolean perf;
        final Map<String, Boolean> data = new HashMap<>();
        boolean burst;
        final Map<String, Double> means = new HashMap<>();
        String ctxMax;
    }

    /**
     * 从 hs 起、长 τ_alert 的告警节拍（默认为当地整点起的一小时）；obs 为该节拍内有读数的设备的观测。返回本节拍的告警（按设备排序）。
     * τ_alert 取十分钟时（交接文档 1.24 版第四点四）：点通道离群率按十分钟计（门槛由调用者按十分钟重新推导后放进 pPoint），上下文
     * 通道标志即该窗口超阈，性能层、数据层与爆发标志不变，现场条件仍按整点钟点累计、用该窗口的均值判定。
     * Alert tick of length τ_alert starting at hs (default: the local hour); obs holds the devices that have readings in it.
     */
    public List<Alert> alertTick(long hs, Map<String, HourObs> obs) {
        long he = hs + p.alertSec;
        String day = localDay(hs);
        int hod = localHour(hs);
        String d1 = addDays(day, -1);
        String d2 = addDays(day, -2);
        Map<String, Info> info = new TreeMap<>();
        for (String dev : devices) {
            HourObs r = obs.get(dev);
            if (r == null) {
                continue;
            }
            if (r.version != curVersion.getOrDefault(dev, 1)) {
                onVersion(dev, r.version);
            }
            Info x = new Info();
            x.bad = p.qualityBad(r.rounds, r.guardRounds);
            if (p.useDetect && r.admitted >= p.pointMinFrac * p.nominal()) {
                x.point = (double) r.outlierRounds / r.admitted;
            }
            x.pflag = x.point != null && x.point > p.pPoint;
            x.cflag = p.useDetect && r.ctxAbove / (p.alertSec / 600.0) >= p.pCtx;
            perf.get(dev).advance(he);
            x.perf = perf.get(dev).confirmed;
            for (String ch : CHANNELS) {
                x.data.put(ch, dataDev(dev, d1, ch) || dataDev(dev, d2, ch));
            }
            for (long[] b : bursts.get(dev)) {
                if (hs - 7200 < b[0] && b[0] <= hs && b[1] <= he) {
                    x.burst = true;
                    break;
                }
            }
            for (int k = 0; k < ACTIVE.length; k++) {
                if (r.n[k] > 0) {
                    x.means.put(ACTIVE[k], r.sum[k] / r.n[k]);
                }
            }
            x.ctxMax = r.ctxMax;
            info.put(dev, x);
            String fd = freezeDay.get(dev);
            if (fd != null && fd.compareTo(day) <= 0 && day.compareTo(addDays(fd, p.baselineDays - 1)) <= 0) {
                for (int k = 0; k < ACTIVE.length; k++) {
                    if (r.n[k] > 0) {
                        double[] b = base.computeIfAbsent(dev + "|" + ACTIVE[k] + "|" + hod, key -> new double[2]);
                        b[0] = b[0] + r.sum[k];
                        b[1] = b[1] + r.n[k];
                    }
                }
            }
        }
        List<String> present = new ArrayList<>();
        for (Map.Entry<String, Info> e : info.entrySet()) {
            if (!e.getValue().bad) {
                present.add(e.getKey());
            }
        }
        int t = p.threshold(present.size());
        Map<String, Integer> cnt = new HashMap<>();
        for (String d : present) {
            Info x = info.get(d);
            inc(cnt, "point", x.pflag);
            inc(cnt, "ctx", x.cflag);
            inc(cnt, "perf", x.perf);
            inc(cnt, "burst", x.burst);
            for (String ch : CHANNELS) {
                inc(cnt, "data_" + ch, x.data.get(ch));
            }
        }
        int tRows = p.threshold(info.size());
        int nBad = 0;
        for (Info x : info.values()) {
            nBad += x.bad ? 1 : 0;
        }
        List<Alert> out = new ArrayList<>();
        for (Map.Entry<String, Info> e : info.entrySet()) {
            String dev = e.getKey();
            Info x = e.getValue();
            String label = null;
            String scope = "局部";
            String note = "";
            List<String> co = new ArrayList<>();
            List<String> activeData = new ArrayList<>();
            for (String ch : ACTIVE) {
                if (x.data.get(ch)) {
                    activeData.add(ch);
                }
            }
            if (x.bad) {
                label = QUALITY;
                scope = nBad >= tRows ? "全局" : "局部";
                for (Map.Entry<String, Info> o : info.entrySet()) {
                    if (o.getValue().bad) {
                        co.add(o.getKey());
                    }
                }
            } else if (x.burst && enteredBefore(dev, d1, d2)) {
                label = SWITCH;
                scope = glob(cnt, "burst", t) ? "全局" : "局部";
                for (String d : present) {
                    if (info.get(d).burst) {
                        co.add(d);
                    }
                }
            } else if (drift.containsKey(dev)) {
                label = DRIFT;
                scope = drift.get(dev).scope;
                co.addAll(drift.keySet());
            } else if ((x.pflag || x.cflag) && siteCondition(dev, hod, day, x)) {
                label = SITE;
                String from = addDays(day, -(p.siteDriftWindowDays - 1));
                int k = 0;
                for (String dd : siteHist.getOrDefault(dev + "|" + hod, Collections.emptySet())) {
                    if (from.compareTo(dd) <= 0 && dd.compareTo(day) < 0) {
                        k++;
                    }
                }
                if (k + 1 >= p.kSiteDrift) {
                    note = "局部漂移候选，留待适应执行第二批";
                }
            } else if (x.perf || !activeData.isEmpty()) {
                label = TRANSIENT;
                boolean g = x.perf && glob(cnt, "perf", t);
                for (String ch : activeData) {
                    g = g || glob(cnt, "data_" + ch, t);
                }
                scope = g ? "全局" : "局部";
                for (String d : present) {
                    Info o = info.get(d);
                    boolean any = o.perf;
                    for (String ch : ACTIVE) {
                        any = any || o.data.get(ch);
                    }
                    if (any) {
                        co.add(d);
                    }
                }
            } else if ((x.pflag && glob(cnt, "point", t)) || (x.cflag && glob(cnt, "ctx", t))) {
                label = ENVIRONMENT;
                scope = "全局";
                for (String d : present) {
                    if (info.get(d).pflag || info.get(d).cflag) {
                        co.add(d);
                    }
                }
            } else if (x.pflag || x.cflag) {
                label = ANOMALY;
                co.add(dev);
            }
            if (SITE.equals(label)) {
                siteHist.computeIfAbsent(dev + "|" + hod, key -> new TreeSet<>()).add(day);
            }
            if (label == null) {
                continue;
            }
            Collections.sort(co);
            Alert a = new Alert();
            a.device = dev;
            a.hourStart = hs;
            a.localHour = day + String.format(" %02d", hod)
                    + (p.alertSec < 3600 ? String.format(":%02d", Instant.ofEpochSecond(hs).atZone(ZONE).getMinute()) : "");
            a.label = label;
            a.scope = scope;
            a.co = String.join("|", co);
            a.pointRate = x.point;
            a.ctxMax = x.ctxMax;
            a.perfMagnitude = x.perf ? perf.get(dev).magnitude : null;
            a.dataChannels = String.join("|", activeData);
            a.burst = x.burst ? 1 : 0;
            a.pointFlag = x.pflag ? 1 : 0;
            a.ctxFlag = x.cflag ? 1 : 0;
            a.note = note;
            a.model = modelAt(dev, hs + p.alertSec);
            out.add(a);
            dayAlerts.add(a);
        }
        for (Map.Entry<String, Info> e : info.entrySet()) {
            if (e.getValue().pflag || e.getValue().cflag) {
                flagHist.computeIfAbsent(e.getKey() + "|" + hod, key -> new TreeSet<>()).add(day);
            }
        }
        return out;
    }

    private boolean enteredBefore(String dev, String d1, String d2) {
        for (String dd : new String[]{d1, d2}) {
            for (String ch : ACTIVE) {
                if ("DEVIATE".equals(dataTrans(dev, dd, ch))) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean siteCondition(String dev, int hod, String day, Info x) {
        String from = addDays(day, -p.siteWindowDays);
        int past = 0;
        for (String dd : flagHist.getOrDefault(dev + "|" + hod, Collections.emptySet())) {
            if (from.compareTo(dd) <= 0 && dd.compareTo(day) < 0) {
                past++;
            }
        }
        if (past < p.kSite - 1) {
            return false;
        }
        String fd = freezeDay.get(dev);
        if (fd == null || day.compareTo(addDays(fd, p.baselineDays - 1)) <= 0) {
            return false;                                   // 基线累积中 / baseline still accumulating
        }
        for (String ch : ACTIVE) {
            Double m = x.means.get(ch);
            double[] b = base.get(dev + "|" + ch + "|" + hod);
            double[] r = refs.get(dev + "|" + ch);
            if (m == null || b == null || r == null) {
                continue;
            }
            if (Math.abs(m - b[0] / b[1]) > p.syncFrac * (r[1] - r[0])) {
                return true;
            }
        }
        return false;
    }

    // ================================================================ 决策矩阵升级格 / upgrade cells
    /**
     * 决策矩阵的格（1.41 版裁决第三节）。按次序判定，先命中者为准：
     * <ol>
     *   <li>二级：触发含性能层，且性能层当前确认段开始于最近一条一级的重训完成之后 K_up 个当地日内；</li>
     *   <li>一级：只有性能层持续；</li>
     *   <li>一级：两层同报，即某个数据层触发通道的形态为「两层同日同报」，或性能层持续与数据层触发同时出现；</li>
     *   <li>一级：只有数据层持续，且某个触发通道的当前持续段开始于该通道最近一条零级生效之后 K_up 个当地日内；</li>
     *   <li>零级：其余（只有数据层持续）。</li>
     * </ol>
     * 返回格的说明（以级别开头），供命令的决定依据使用。/ The decision-matrix cell, checked in order.
     */
    private String cell(String dev, String day, DevState s, List<String> triggers, List<String> forms) {
        boolean perfTrig = triggers.contains("perf");
        List<String> data = new ArrayList<>();
        for (String tr : triggers) {
            if (!"perf".equals(tr)) {
                data.add(tr);
            }
        }
        PerfTracker pf = perf.get(dev);
        Long done = level1Done.get(dev);
        if (perfTrig && done != null && pf.since != null && pf.since >= done && pf.since < addLocalDays(done, p.kUpDays)) {
            return "二级：一级之后 K_up 天内性能层新确认段";
        }
        if (perfTrig && data.isEmpty()) {
            return "一级：只有性能层持续";
        }
        boolean sameDay = false;
        for (String f : forms) {
            sameDay = sameDay || f.endsWith("两层同日同报");
        }
        if (sameDay || (perfTrig && !data.isEmpty())) {
            return "一级：两层同报";
        }
        for (String ch : data) {
            Long e0 = level0Eff.get(dev + "|" + ch);
            if (e0 == null) {
                continue;
            }
            // 当前持续段的起点：从当日向前连续偏离的第一天，不早于零级生效的当地日（换版本时数据层状态已清零）
            String first = day;
            String floor = localDay(e0);
            while (first.compareTo(floor) > 0 && dataDev(dev, addDays(first, -1), ch)) {
                first = addDays(first, -1);
            }
            long start = dayStart(first);
            if (start >= e0 && start < addLocalDays(e0, p.kUpDays)) {
                return "一级：零级之后 K_up 天内同一通道新持续段";
            }
        }
        return "零级：只有数据层持续";
    }

    // ================================================================ 日节拍与复核 / daily tick and review
    private static final class DevState {
        boolean perfAtT;
        boolean perfPersist;
        final Map<String, boolean[]> chans = new HashMap<>();   // ch → {dev, persistent}
    }

    /**
     * 当地日 day 结束（日界 t = 次日零点）时的日节拍与日界复核；返回本日的命令与复核两份清单。
     * Daily tick and review at the end of local day {@code day}.
     */
    public void dayTick(String day, List<Command> commands, List<Review> review) {
        cmdTick(dayStart(addDays(day, 1)), commands, review);
    }

    /**
     * 时刻 t 的决策节拍（交接文档 1.24 版第四点四）：决策节拍从当地零点起每 τ_cmd 触发一次。数据层的两条规则与日界复核只在当地日界
     * 那个节拍上判定；性能层的规则在每个节拍上判定：W 为一个日界时是「确认跨过当日零点且仍未回落」，W 小于一天时是「从确认起持续满
     * W 仍未回落」。日内节拍的在场设备按最近一个已有数据层结论的当地日（前一日）计。冷却期与同一段偏离只发一次照旧。
     * Decision tick at t; data rules and the review only at local day boundaries, the perf rule at every tick.
     */
    public void cmdTick(long t, List<Command> commands, List<Review> review) {
        String day = localDay(t - 1);
        boolean boundary = dayStart(addDays(day, 1)) == t;
        String prev = addDays(day, -1);
        long dayStart = dayStart(day);
        String evDay = boundary ? day : prev;
        List<String> present = new ArrayList<>();
        for (String d : devices) {
            if (isEvaluable(d, evDay)) {
                present.add(d);
            }
        }
        int thr = p.threshold(present.size());
        Map<String, DevState> st = new HashMap<>();
        for (String dev : devices) {
            PerfTracker pf = perf.get(dev);
            pf.advance(t);
            DevState s = new DevState();
            s.perfAtT = pf.confirmed;
            s.perfPersist = pf.confirmed && pf.since != null
                    && (p.waitSec == 0 ? pf.since < dayStart : t - pf.since >= p.waitSec);
            boolean ev = boundary && isEvaluable(dev, day);
            for (String ch : CHANNELS) {
                boolean dd = boundary && dataDev(dev, day, ch);
                boolean persistent = ev && dd && (dataDev(dev, prev, ch) || s.perfAtT);
                s.chans.put(ch, new boolean[]{ev && dd, persistent});
            }
            st.put(dev, s);
        }
        Map<String, Integer> cnt = new HashMap<>();
        for (String dev : present) {
            for (String ch : ACTIVE) {
                inc(cnt, ch, st.get(dev).chans.get(ch)[1]);
            }
            inc(cnt, "perf", st.get(dev).perfPersist);
        }
        // 回落按触发信号类分别结束（1.25 版裁决第六项）/ each trigger class ends on its own fallback
        for (String dev : devices) {
            Book b = drift.get(dev);
            if (b != null) {
                b.triggers.removeIf(tr -> fallen(st.get(dev), dev, day, tr, boundary));
                if (b.triggers.isEmpty()) {
                    drift.remove(dev);
                }
            }
            Map<String, Integer> ep = episode.get(dev);
            if (ep != null) {
                ep.keySet().removeIf(tr -> fallen(st.get(dev), dev, day, tr, boundary));
                if (ep.isEmpty()) {
                    episode.remove(dev);
                }
            }
        }
        for (String dev : devices) {
            DevState s = st.get(dev);
            List<String> triggers = new ArrayList<>();
            List<String> forms = new ArrayList<>();
            for (String ch : ACTIVE) {
                if (s.chans.get(ch)[1]) {
                    triggers.add(ch);
                    forms.add(ch + "：" + (s.perfAtT && !dataDev(dev, prev, ch) ? "两层同日同报" : "连续两日偏离"));
                }
            }
            if (s.perfPersist) {
                triggers.add("perf");
                forms.add(p.waitSec == 0 ? "性能层确认跨过日界" : "性能层确认持续满等待时长");
            }
            if (triggers.isEmpty()) {
                continue;
            }
            boolean global = false;
            for (String tr : triggers) {
                global = global || cnt.getOrDefault(tr, 0) >= thr;
            }
            String scope = global ? "全局" : "局部";
            List<String> co = new ArrayList<>();
            for (String d : present) {
                boolean any = false;
                for (String ch : ACTIVE) {
                    any = any || (triggers.contains(ch) && st.get(d).chans.get(ch)[1]);
                }
                if (any || (triggers.contains("perf") && st.get(d).perfPersist)) {
                    co.add(d);
                }
            }
            Collections.sort(co);
            Book db = drift.get(dev);
            if (db == null) {
                db = new Book();
                db.scope = scope;
                db.triggers = new ArrayList<>(triggers);
                drift.put(dev, db);
            } else {
                for (String tr : triggers) {
                    if (!db.triggers.contains(tr)) {
                        db.triggers.add(tr);
                    }
                }
            }
            // 决策矩阵的格（升级格关闭时一律零级）与按级别的冷却：各级只阻止同级（1.41 版裁决第三节第三问）
            // The decision-matrix cell (level 0 when the upgrade cells are off); each level's cooldown blocks only that level.
            String cell = p.upgradeCells ? cell(dev, day, s, triggers, forms) : null;
            int level = cell == null ? 0 : cell.startsWith("二级") ? 2 : cell.startsWith("一级") ? 1 : 0;
            Map<String, Long> cd = level == 0 ? cooldownUntil : level == 1 ? cooldown1Until : cooldown2Until;
            if (cd.getOrDefault(dev, 0L) > t) {
                continue;
            }
            Map<String, Integer> ep = episode.getOrDefault(dev, Collections.emptyMap());
            List<String> fresh = new ArrayList<>();
            for (String tr : triggers) {
                if (!ep.containsKey(tr)) {
                    fresh.add(tr);
                }
            }
            if (p.oncePerEpisode && fresh.isEmpty()) {
                continue;
            }
            ep = episode.computeIfAbsent(dev, k -> new LinkedHashMap<>());
            int episodeN = 0;
            for (String tr : p.oncePerEpisode ? fresh : triggers) {
                ep.merge(tr, 1, Integer::sum);
                episodeN = Math.max(episodeN, ep.get(tr));
            }
            int n = seq.getOrDefault(dev, 0) + 1;
            seq.put(dev, n);
            Command c = new Command();
            c.device = dev;
            c.day = day;
            c.decidedAt = t;
            // 生效 = t + Δ（Δ 为 0 时取一个决策节拍）；冷却期截止 = 生效之后一个当地日 / effective time and cooldown
            if (p.deltaSec != 0) {
                c.effectiveAt = t + p.deltaSec;
            } else {
                c.effectiveAt = p.cmdSec == 0 ? addLocalDays(t, 1) : t + p.cmdSec;
            }
            if (level == 0) {
                cooldownUntil.put(dev, addLocalDays(c.effectiveAt, 1));
                if (p.upgradeCells) {
                    for (String tr : triggers) {
                        if (!"perf".equals(tr)) {
                            level0Eff.put(dev + "|" + tr, c.effectiveAt);
                        }
                    }
                }
            } else {
                // 重训完成的时刻由命令自己算出（第三节第一问）：一级生效前侧为生效时刻，生效后侧为生效加 N 天；二级为生效加收集长度
                // Completion computed from the command itself; cooldown is seven local days after completion.
                long done = level == 1 ? c.effectiveAt + (p.retrainAfter ? p.retrainDays * 86400L : 0L)
                        : c.effectiveAt + p.level2Days * 86400L;
                if (level == 1) {
                    level1Done.put(dev, done);
                    cooldown1Until.put(dev, addLocalDays(done, p.upCooldownDays));
                } else {
                    cooldown2Until.put(dev, addLocalDays(done, p.upCooldownDays));
                }
            }
            c.seq = n;
            c.level = level;
            c.cell = cell;
            c.model = modelAt(dev, t);
            c.scope = scope;
            c.label = DRIFT;
            c.triggers = String.join("|", triggers);
            c.forms = String.join("；", forms);
            c.co = co.size();
            c.coDevices = String.join("|", co);
            c.episodeN = episodeN;
            commands.add(c);
        }
        if (!boundary) {
            return;
        }
        // 日界复核 / daily review
        for (String dev : devices) {
            PerfTracker pt = perf.get(dev);
            for (long e : pt.switchEnded) {
                Review r = new Review();
                r.device = dev;
                r.day = day;
                r.kind = "性能层确认段因模型切换而结束";
                r.hourStart = Long.toString(e);
                r.detail = "模型代数 " + modelAt(dev, e);
                review.add(r);
            }
            pt.switchEnded.clear();
        }
        for (String dev : devices) {
            Boolean ev = evaluable.get(dev + "|" + day);
            if (ev != null && !ev) {
                Review r = new Review();
                r.device = dev;
                r.day = day;
                r.kind = "当日不可判";
                r.hourStart = "";
                r.detail = "";
                review.add(r);
            }
            boolean entered = false;
            for (String ch : ACTIVE) {
                entered = entered || "DEVIATE".equals(dataTrans(dev, day, ch));
            }
            if (!entered) {
                continue;
            }
            for (Alert a : dayAlerts) {
                if (!a.device.equals(dev) || !ANOMALY.equals(a.label)) {
                    continue;
                }
                long hs = a.hourStart;
                for (long[] b : bursts.get(dev)) {
                    if (hs < b[0] && b[0] <= hs + p.alertSec && b[1] <= t + SLIDE) {
                        Review r = new Review();
                        r.device = dev;
                        r.day = day;
                        r.kind = "异常改记为分布切换";
                        r.hourStart = Long.toString(hs);
                        r.detail = a.localHour;
                        review.add(r);
                        break;
                    }
                }
            }
        }
        dayAlerts.clear();
    }

    private boolean fallen(DevState s, String dev, String day, String trig, boolean boundary) {
        if ("perf".equals(trig)) {
            return !s.perfAtT;
        }
        return boundary && isEvaluable(dev, day) && !dataDev(dev, day, trig);
    }

    private static void inc(Map<String, Integer> cnt, String k, boolean v) {
        if (v) {
            cnt.merge(k, 1, Integer::sum);
        }
    }

    private static boolean glob(Map<String, Integer> cnt, String k, int t) {
        return cnt.getOrDefault(k, 0) >= t;
    }

    // ================================================================ 时间 / time
    public static String localDay(long ts) {
        return Instant.ofEpochSecond(ts).atZone(ZONE).toLocalDate().toString();
    }

    public static int localHour(long ts) {
        return Instant.ofEpochSecond(ts).atZone(ZONE).getHour();
    }

    public static long dayStart(String day) {
        return LocalDate.parse(day).atStartOfDay(ZONE).toEpochSecond();
    }

    /** 按当地挂钟加 k 天（夏令时切换日为 23 或 25 小时）/ add k local calendar days, keeping the wall clock. */
    public static long addLocalDays(long ts, int k) {
        return Instant.ofEpochSecond(ts).atZone(ZONE).plusDays(k).toEpochSecond();
    }

    public static String addDays(String day, int k) {
        return LocalDate.parse(day).plusDays(k).toString();
    }

    /** 当地日 day 内的决策节拍时刻：从当地零点起每 cmdSec 一个，最后一个是次日零点 / decision ticks within a local day. */
    public static List<Long> cmdTicks(String day, long cmdSec) {
        List<Long> out = new ArrayList<>();
        long end = dayStart(addDays(day, 1));
        for (long t = cmdSec == 0 ? end : dayStart(day) + cmdSec; t < end; t += cmdSec) {
            out.add(t);
        }
        out.add(end);
        return out;
    }

    /** 当地日内各告警节拍的起点（夏令时开始那天少一个小时）/ alert tick starts within a local day. */
    public static List<Long> alertStarts(String day, long alertSec) {
        List<Long> out = new ArrayList<>();
        for (long t = dayStart(day); t < dayStart(addDays(day, 1)); t += alertSec) {
            out.add(t);
        }
        return out;
    }

    /** 当地日内各整点的起点（夏令时开始那天少一个小时）/ hour starts within a local day. */
    public static List<Long> localHours(String day) {
        List<Long> out = new ArrayList<>();
        for (long t = dayStart(day); t < dayStart(addDays(day, 1)); t += 3600) {
            out.add(t);
        }
        return out;
    }

    public Set<String> devices() {
        return Collections.unmodifiableSet(devices);
    }
}
