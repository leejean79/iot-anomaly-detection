package com.leejean.m6;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.leejean.m1.Channels;

import java.io.Serializable;

/**
 * 标准化算子里的紧凑原始缓存（M6a 交接文档第四节，开工核对裁决第一、四项）：每轮五个原始读数（光照为 log1p 之前的原始值）、
 * 事件时间戳与掩码（缺失、删失、守卫各五位），按时间保留最近 depthSec 秒（默认七天）。读数用双精度：提取脚本的统计里温度与
 * 湿度各有数万条读数不能被单精度精确表示（docs/m7/extract/meta.md），按裁决第四项的判据用双精度，每台约三兆。
 * 检查点里由自带的 Kryo 序列化器按时间顺序只写已用的行；它只写基本类型，不经 Java 序列化，不涉及类加载器。
 * The compact raw cache in the scaler: five raw readings, timestamp and masks per round, kept for depthSec seconds; double
 * precision because temperature and humidity contain readings float32 cannot represent exactly.
 */
@DefaultSerializer(CompactCache.KryoSer.class)
public class CompactCache implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int N = Channels.N_DET;
    /** 掩码位：缺失 0–4，删失 5–9，守卫 10–14 / mask bits: missing, censored, guard. */
    public static final int CENSORED_SHIFT = 5;
    public static final int GUARD_SHIFT = 10;

    private final long depthSec;
    private long[] ts;
    private double[] x;
    private int[] mask;
    private int head;      // 最早一行的位置 / index of the oldest row
    private int size;

    public CompactCache(long depthSec) {
        this(depthSec, 1024);
    }

    private CompactCache(long depthSec, int capacity) {
        if (depthSec <= 0) {
            throw new IllegalArgumentException("depthSec must be > 0, got " + depthSec);
        }
        this.depthSec = depthSec;
        this.ts = new long[capacity];
        this.x = new double[capacity * N];
        this.mask = new int[capacity];
    }

    public static int maskOf(boolean[] missing, boolean[] censored, boolean[] guard) {
        int m = 0;
        for (int c = 0; c < N; c++) {
            if (missing != null && missing[c]) {
                m |= 1 << c;
            }
            if (censored != null && censored[c]) {
                m |= 1 << (CENSORED_SHIFT + c);
            }
            if (guard != null && guard[c]) {
                m |= 1 << (GUARD_SHIFT + c);
            }
        }
        return m;
    }

    /** 追加一轮（时间戳须不减），并淘汰早于 t − depthSec 的行 / append one round and evict rows older than depth. */
    public void add(long t, double[] raw, int m) {
        if (size == ts.length) {
            grow();
        }
        int i = (head + size) % ts.length;
        ts[i] = t;
        System.arraycopy(raw, 0, x, i * N, N);
        mask[i] = m;
        size++;
        while (size > 0 && ts[head] <= t - depthSec) {
            head = (head + 1) % ts.length;
            size--;
        }
    }

    private void grow() {
        int cap = ts.length * 2;
        long[] t2 = new long[cap];
        double[] x2 = new double[cap * N];
        int[] m2 = new int[cap];
        for (int k = 0; k < size; k++) {
            int i = (head + k) % ts.length;
            t2[k] = ts[i];
            System.arraycopy(x, i * N, x2, k * N, N);
            m2[k] = mask[i];
        }
        ts = t2;
        x = x2;
        mask = m2;
        head = 0;
    }

    public int size() {
        return size;
    }

    public long depthSec() {
        return depthSec;
    }

    /** 第 k 行（0 为最早）的位置 / physical index of the k-th row (0 = oldest). */
    private int at(int k) {
        return (head + k) % ts.length;
    }

    public long ts(int k) {
        return ts[at(k)];
    }

    public int mask(int k) {
        return mask[at(k)];
    }

    public double x(int k, int c) {
        return x[at(k) * N + c];
    }

    /** 已用字节数（时间戳、读数、掩码）/ bytes in use. */
    public long usedBytes() {
        return (long) size * (8 + 8 * N + 4);
    }

    /** 只写已用的行，按时间顺序 / writes only the used rows, oldest first. */
    public static final class KryoSer extends Serializer<CompactCache> {
        @Override
        public void write(Kryo kryo, Output out, CompactCache c) {
            out.writeLong(c.depthSec);
            out.writeInt(c.size);
            for (int k = 0; k < c.size; k++) {
                int i = c.at(k);
                out.writeLong(c.ts[i]);
                for (int ch = 0; ch < N; ch++) {
                    out.writeDouble(c.x[i * N + ch]);
                }
                out.writeInt(c.mask[i]);
            }
        }

        @Override
        public CompactCache read(Kryo kryo, Input in, Class<CompactCache> type) {
            long depth = in.readLong();
            int n = in.readInt();
            CompactCache c = new CompactCache(depth, Math.max(1024, Integer.highestOneBit(Math.max(1, n)) * 2));
            for (int k = 0; k < n; k++) {
                c.ts[k] = in.readLong();
                for (int ch = 0; ch < N; ch++) {
                    c.x[k * N + ch] = in.readDouble();
                }
                c.mask[k] = in.readInt();
            }
            c.size = n;
            return c;
        }

        @Override
        public CompactCache copy(Kryo kryo, CompactCache c) {
            CompactCache d = new CompactCache(c.depthSec, c.ts.length);
            System.arraycopy(c.ts, 0, d.ts, 0, c.ts.length);
            System.arraycopy(c.x, 0, d.x, 0, c.x.length);
            System.arraycopy(c.mask, 0, d.mask, 0, c.mask.length);
            d.head = c.head;
            d.size = c.size;
            return d;
        }
    }
}
