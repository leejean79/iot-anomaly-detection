package com.leejean.m1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于计数的滑动窗口环形缓冲（交接文档 §5，approved decision 4）。
 * A count-based sliding-window ring buffer over DeviceRounds (handover §5).
 *
 * <p>纯库类，**不依赖 Flink**，在 M1 包内单测；后续消费模块在各自算子中实例化。
 * **本阶段不接入 M1 作业**（交接文档 §5 明确要求）。
 * Pure library class with NO Flink dependency, unit-tested in the M1 package; consumer modules
 * instantiate it inside their own operators later. It is deliberately NOT wired into the M1 job.
 *
 * <p>语义 / semantics：容量 L。add(round) 追加，满则覆盖最旧（FIFO）。isFull() 为 size==L。
 * snapshot() 返回最近 L 个 round，**按到达先后顺序**（最旧在前、最新在后）。
 * Capacity L. add(round) appends, overwriting the oldest when full (FIFO). isFull() is size==L.
 * snapshot() returns the last L rounds in arrival order (oldest first, newest last).
 *
 * <p>环形缓冲模式仿 tree/RingBuffer，但 snapshot() 在此**保证时间顺序**（RingBuffer 的
 * snapshot 顺序未指定），因为滑动窗口消费方需要有序序列。
 * The ring-buffer pattern mirrors tree/RingBuffer, but snapshot() here guarantees arrival order
 * (RingBuffer left it unspecified) because sliding-window consumers need an ordered sequence.
 */
public class RoundWindow implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int capacity;
    private final DeviceRound[] buffer;
    private int head;   // 下一个写入位置 / next write position
    private int size;

    public RoundWindow(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, got " + capacity);
        }
        this.capacity = capacity;
        this.buffer = new DeviceRound[capacity];
        this.head = 0;
        this.size = 0;
    }

    /** 追加一个 round，满则覆盖最旧 / append a round, overwrite-oldest when full. */
    public void add(DeviceRound round) {
        buffer[head] = round;
        head = (head + 1) % capacity;
        if (size < capacity) {
            size++;
        }
    }

    /** 窗口是否已满 / whether the window has reached capacity. */
    public boolean isFull() {
        return size == capacity;
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return capacity;
    }

    /**
     * 返回最近 size 个 round，按到达先后顺序（最旧在前）。
     * Return the last `size` rounds in arrival order (oldest first).
     */
    public List<DeviceRound> snapshot() {
        List<DeviceRound> out = new ArrayList<>(size);
        // 最旧元素的位置：缓冲未满时为 0，满时为 head / oldest is at 0 when not full, else at head
        int start = isFull() ? head : 0;
        for (int i = 0; i < size; i++) {
            out.add(buffer[(start + i) % capacity]);
        }
        return out;
    }
}
