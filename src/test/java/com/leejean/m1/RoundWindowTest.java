package com.leejean.m1;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RoundWindow 语义单测（交接文档 §5，approved decision 4）。
 * Unit tests for RoundWindow semantics.
 */
class RoundWindowTest {

    private static DeviceRound roundAt(long ts) {
        DeviceRound r = new DeviceRound();
        r.setDevice("A");
        r.setTs(ts);
        return r;
    }

    @Test
    void capacityMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new RoundWindow(0));
        assertThrows(IllegalArgumentException.class, () -> new RoundWindow(-1));
    }

    @Test
    void fillsAndReportsFull() {
        RoundWindow w = new RoundWindow(3);
        assertFalse(w.isFull());
        w.add(roundAt(1));
        w.add(roundAt(2));
        assertFalse(w.isFull());
        assertEquals(2, w.size());
        w.add(roundAt(3));
        assertTrue(w.isFull());
        assertEquals(3, w.size());
    }

    @Test
    void snapshotIsOldestFirstBeforeFull() {
        RoundWindow w = new RoundWindow(5);
        w.add(roundAt(10));
        w.add(roundAt(20));
        w.add(roundAt(30));
        List<DeviceRound> s = w.snapshot();
        assertEquals(3, s.size());
        assertEquals(10, s.get(0).getTs());
        assertEquals(20, s.get(1).getTs());
        assertEquals(30, s.get(2).getTs());
    }

    @Test
    void slidesAndKeepsLastLInOrderWhenFull() {
        RoundWindow w = new RoundWindow(3);
        for (long ts = 1; ts <= 5; ts++) {
            w.add(roundAt(ts));   // 1,2,3,4,5 -> window keeps 3,4,5
        }
        assertTrue(w.isFull());
        List<DeviceRound> s = w.snapshot();
        assertEquals(3, s.size());
        assertEquals(3, s.get(0).getTs());
        assertEquals(4, s.get(1).getTs());
        assertEquals(5, s.get(2).getTs());
    }
}
