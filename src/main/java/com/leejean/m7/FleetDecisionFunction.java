package com.leejean.m7;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 聚合模块的决策函数（交接文档 {@code handover_m7_decision_stage_zh.md} 1.23 版第四节）：小时节拍给出带范围与性质归因的告警，日节拍
 * 按时间形态与决策矩阵发出零级命令，日界复核补记当日才知道的事。在线算子与 Java 离线运行器共用本类，Python 参考实现
 * {@code m7/decide.py} 与本类逐条相同（验收项一）。本类只依赖调用者按事件时间送入的观测与事件，不依赖处理时间。
 * The M7 decision function shared by the online operator and the offline runner; mirrors m7/decide.py step by step.
 *
 * <p>调用顺序：先用 {@code add*} 方法送入事件（性能层事件与元素、爆发、数据层日结论），再对每个当地整点调用 {@link #hourTick}，
 * 每个当地日的最后一个整点之后调用 {@link #dayTick}。交接文档未写死、此处选定的写法与 Python 实现相同（见 decide.py 文件头 a 至 d）。
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

        /** 共模门槛 max(最少台数, ⌈n·比例⌉) / common-mode threshold. */
        public int threshold(int n) {
            return Math.max(minGlobal, (n * commonNum + commonDen - 1) / commonDen);
        }

        boolean qualityBad(int rounds, int guard) {
            double missing = 1.0 - (double) rounds / NOMINAL_PER_HOUR;
            return missing > qualityMax || (double) guard / NOMINAL_PER_HOUR > qualityMax;
        }
    }

    /** 一台设备一个小时的观测 / one device-hour observation. */
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
        int count;
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
    private final Map<String, Book> episode = new HashMap<>();
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
     * 当地整点 hs 起的小时节拍；obs 为该小时有读数的设备的观测。返回本小时的告警（按设备排序）。
     * Hourly tick for the local hour starting at hs; obs holds the devices that have readings in this hour.
     */
    public List<Alert> hourTick(long hs, Map<String, HourObs> obs) {
        long he = hs + 3600;
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
            if (p.useDetect && r.admitted >= p.pointMinFrac * NOMINAL_PER_HOUR) {
                x.point = (double) r.outlierRounds / r.admitted;
            }
            x.pflag = x.point != null && x.point > p.pPoint;
            x.cflag = p.useDetect && r.ctxAbove / 6.0 >= p.pCtx;
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
            a.localHour = day + String.format(" %02d", hod);
            a.label = label;
            a.scope = scope;
            a.co = String.join("|", co);
            a.pointRate = x.point;
            a.ctxMax = x.ctxMax;
            a.perfMagnitude = x.perf ? perf.get(dev).magnitude : null;
            a.dataChannels = String.join("|", activeData);
            a.burst = x.burst ? 1 : 0;
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
        boolean perfCross;
        final Map<String, boolean[]> chans = new HashMap<>();   // ch → {dev, persistent}
    }

    /**
     * 当地日 day 结束（日界 t = 次日零点）时的日节拍与日界复核；返回本日的命令与复核两份清单。
     * Daily tick and review at the end of local day {@code day}.
     */
    public void dayTick(String day, List<Command> commands, List<Review> review) {
        long t = dayStart(addDays(day, 1));
        String prev = addDays(day, -1);
        long dayStart = dayStart(day);
        List<String> present = new ArrayList<>();
        for (String d : devices) {
            if (isEvaluable(d, day)) {
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
            s.perfCross = pf.confirmed && pf.since != null && pf.since < dayStart;
            boolean ev = isEvaluable(dev, day);
            for (String ch : CHANNELS) {
                boolean dd = dataDev(dev, day, ch);
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
            inc(cnt, "perf", st.get(dev).perfCross);
        }
        for (String dev : devices) {
            for (Map<String, Book> book : Arrays.<Map<String, Book>>asList(drift, episode)) {
                Book b = book.get(dev);
                if (b == null) {
                    continue;
                }
                boolean fell = false;
                for (String trig : b.triggers) {
                    if ("perf".equals(trig)) {
                        fell = fell || !st.get(dev).perfAtT;
                    } else if (isEvaluable(dev, day) && !dataDev(dev, day, trig)) {
                        fell = true;
                    }
                }
                if (fell) {
                    book.remove(dev);
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
            if (s.perfCross) {
                triggers.add("perf");
                forms.add("性能层确认跨过日界");
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
                if (any || (triggers.contains("perf") && st.get(d).perfCross)) {
                    co.add(d);
                }
            }
            Collections.sort(co);
            if (!drift.containsKey(dev)) {
                Book b = new Book();
                b.scope = scope;
                b.triggers = triggers;
                drift.put(dev, b);
            }
            if (cooldownUntil.getOrDefault(dev, 0L) > t) {
                continue;
            }
            if (episode.containsKey(dev) && p.oncePerEpisode) {
                continue;
            }
            Book ep = episode.get(dev);
            if (ep == null) {
                ep = new Book();
                ep.triggers = triggers;
                episode.put(dev, ep);
            }
            ep.count++;
            int n = seq.getOrDefault(dev, 0) + 1;
            seq.put(dev, n);
            Command c = new Command();
            c.device = dev;
            c.day = day;
            c.decidedAt = t;
            c.effectiveAt = dayStart(addDays(day, 2));
            cooldownUntil.put(dev, dayStart(addDays(day, 3)));
            c.seq = n;
            c.level = 0;
            c.scope = scope;
            c.label = DRIFT;
            c.triggers = String.join("|", triggers);
            c.forms = String.join("；", forms);
            c.co = co.size();
            c.coDevices = String.join("|", co);
            c.episodeN = ep.count;
            commands.add(c);
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
                    if (hs < b[0] && b[0] <= hs + 3600 && b[1] <= t + SLIDE) {
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

    public static String addDays(String day, int k) {
        return LocalDate.parse(day).plusDays(k).toString();
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
