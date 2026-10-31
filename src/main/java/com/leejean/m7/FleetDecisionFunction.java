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
        final List<double[]> items = new ArrayList<>();   // {ts, kind(0 事件,1 元素), level(1=确认), value, magnitude}
        int i;
        boolean sorted = true;
        boolean confirmed;
        long confirmTs;
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
                if (it[1] == 0) {
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
    private final Map<String, double[]> refs = new HashMap<>();                    // dev|ch → {p10, p90}
    private final Map<String, String> freezeDay = new HashMap<>();
    private final Map<String, Set<String>> flagHist = new HashMap<>();             // dev|hod → days
    private final Map<String, Set<String>> siteHist = new HashMap<>();
    private final Map<String, double[]> base = new HashMap<>();                    // dev|ch|hod → {sum, n}
    private final TreeMap<String, Book> drift = new TreeMap<>();
    private final Map<String, Map<String, Integer>> episode = new HashMap<>();     // dev → {触发类: 第几次} / per trigger class
    private final Map<String, Long> cooldownUntil = new HashMap<>();
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

    public void addBurst(String dev, long ts, long emittedAt) {
        addDevice(dev);
        if (p.useData) {
            bursts.get(dev).add(new long[]{ts, emittedAt});
        }
    }

    public void addDataDay(String dev, String day, String ch, boolean ev, boolean deviated, String transition) {
        dataDays.put(dev + "|" + day + "|" + ch, new String[]{ev ? "1" : "0", deviated ? "1" : "0", transition});
        evaluable.put(dev + "|" + day, ev);
    }

    private boolean dataDev(String dev, String day, String ch) {
        if (!p.useData) {
            return false;
        }
        String[] r = dataDays.get(dev + "|" + day + "|" + ch);
        return r != null && "1".equals(r[1]);
    }

    private String dataTrans(String dev, String day, String ch) {
        if (!p.useData) {
            return "";
        }
        String[] r = dataDays.get(dev + "|" + day + "|" + ch);
        return r == null ? "" : r[2];
    }

    private boolean isEvaluable(String dev, String day) {
        return evaluable.getOrDefault(dev + "|" + day, false);
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
            if (cooldownUntil.getOrDefault(dev, 0L) > t) {
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
            cooldownUntil.put(dev, addLocalDays(c.effectiveAt, 1));
            c.seq = n;
            c.level = 0;
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
