package com.leejean.m5a;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

/**
 * 当日缓冲：每个检测通道当日的变换域读数，按倍数扩容。序列化时只写已用的部分（M5a 阶段报告第九节第 1 条，设计会话 2026-10-26 选乙）：检查点（Kryo，
 * {@link KryoSer}）与 Java 序列化（日志里的状态大小）都不写扩容留下的空位；恢复时数组长度等于已用长度，之后照常扩容。
 * Per-channel buffer of today's transformed readings; serialization writes only the used part (Kryo for checkpoints, Java for logs).
 */
@DefaultSerializer(DayBuffer.KryoSer.class)
public final class DayBuffer implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int INITIAL = 1024;

    private transient double[][] buf;
    private transient int[] n;

    public DayBuffer(int channels) {
        buf = new double[channels][];
        n = new int[channels];
    }

    public void append(int c, double v) {
        if (buf[c] == null) {
            buf[c] = new double[INITIAL];
        } else if (n[c] == buf[c].length) {
            buf[c] = Arrays.copyOf(buf[c], buf[c].length * 2);
        }
        buf[c][n[c]++] = v;
    }

    /** 已用读数数 / number of readings. */
    public int size(int c) {
        return n[c];
    }

    /** 已用部分的副本 / copy of the used part. */
    public double[] values(int c) {
        return Arrays.copyOf(buf[c], n[c]);
    }

    /** 已分配的容量（测试用）/ allocated capacity (for tests). */
    int capacity(int c) {
        return buf[c] == null ? 0 : buf[c].length;
    }

    private void write(DataSink out) throws IOException {
        out.writeInt(n.length);
        for (int c = 0; c < n.length; c++) {
            out.writeInt(n[c]);
            for (int i = 0; i < n[c]; i++) {
                out.writeDouble(buf[c][i]);
            }
        }
    }

    private static DayBuffer read(DataSource in) throws IOException {
        int channels = in.readInt();
        DayBuffer b = new DayBuffer(channels);
        for (int c = 0; c < channels; c++) {
            int k = in.readInt();
            b.n[c] = k;
            if (k > 0) {
                b.buf[c] = new double[k];
                for (int i = 0; i < k; i++) {
                    b.buf[c][i] = in.readDouble();
                }
            }
        }
        return b;
    }

    private interface DataSink {
        void writeInt(int v) throws IOException;

        void writeDouble(double v) throws IOException;
    }

    private interface DataSource {
        int readInt() throws IOException;

        double readDouble() throws IOException;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        write(new DataSink() {
            public void writeInt(int v) throws IOException { out.writeInt(v); }
            public void writeDouble(double v) throws IOException { out.writeDouble(v); }
        });
    }

    private void readObject(ObjectInputStream in) throws IOException {
        DayBuffer b = read(new DataSource() {
            public int readInt() throws IOException { return in.readInt(); }
            public double readDouble() throws IOException { return in.readDouble(); }
        });
        buf = b.buf;
        n = b.n;
    }

    /** 检查点用的 Kryo 序列化器：只写已用部分 / Kryo serializer for checkpoints: used part only. */
    public static final class KryoSer extends Serializer<DayBuffer> {
        @Override
        public void write(Kryo kryo, Output output, DayBuffer b) {
            try {
                b.write(new DataSink() {
                    public void writeInt(int v) { output.writeInt(v); }
                    public void writeDouble(double v) { output.writeDouble(v); }
                });
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public DayBuffer read(Kryo kryo, Input input, Class<DayBuffer> type) {
            try {
                return DayBuffer.read(new DataSource() {
                    public int readInt() { return input.readInt(); }
                    public double readDouble() { return input.readDouble(); }
                });
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public DayBuffer copy(Kryo kryo, DayBuffer b) {
            DayBuffer c = new DayBuffer(b.n.length);
            for (int i = 0; i < b.n.length; i++) {
                c.n[i] = b.n[i];
                c.buf[i] = b.buf[i] == null ? null : Arrays.copyOf(b.buf[i], b.n[i]);
            }
            return c;
        }
    }
}
