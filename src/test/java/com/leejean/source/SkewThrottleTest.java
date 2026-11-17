package com.leejean.source;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重放器按下游水位线限速（1.47 版裁决第一节）：用本地 HTTP 服务模拟 Flink REST 接口。
 * The replayer's watermark-skew throttle against a fake Flink REST endpoint.
 */
class SkewThrottleTest {
    private HttpServer server;
    private final AtomicLong stdWm = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong aggWm = new AtomicLong(Long.MIN_VALUE);
    private String rest;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        reply("/jobs/overview", () -> "{\"jobs\":[{\"jid\":\"old\",\"name\":\"M2Job x\",\"state\":\"CANCELED\"},"
                + "{\"jid\":\"j1\",\"name\":\"M2Job - M1 ...\",\"state\":\"RUNNING\"}]}");
        reply("/jobs/j1", () -> "{\"state\":\"RUNNING\",\"vertices\":[{\"id\":\"v1\",\"name\":\"RawCache -> RobustScaler\"},"
                + "{\"id\":\"v2\",\"name\":\"M7-Decision\"},{\"id\":\"v3\",\"name\":\"Pmcod\"}]}");
        reply("/jobs/j1/vertices/v1/watermarks", () -> "[{\"id\":\"0.currentInputWatermark\",\"value\":\"" + stdWm.get()
                + "\"},{\"id\":\"1.currentInputWatermark\",\"value\":\"" + (stdWm.get() + 5000) + "\"}]");
        reply("/jobs/j1/vertices/v2/watermarks", () -> "[{\"id\":\"0.currentInputWatermark\",\"value\":\"" + aggWm.get() + "\"}]");
        server.start();
        rest = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private void reply(String path, java.util.function.Supplier<String> body) {
        server.createContext(path, ex -> {
            if (!ex.getRequestURI().getPath().equals(path)) {
                ex.sendResponseHeaders(404, -1);
                ex.close();
                return;
            }
            byte[] b = body.get().getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            try (OutputStream o = ex.getResponseBody()) {
                o.write(b);
            }
        });
    }

    @Test
    void noThrottleBeforeWatermarksExist() {
        CsvKafkaReplayer.SkewThrottle t = new CsvKafkaReplayer.SkewThrottle(rest, 43_200L, 200L);
        assertEquals(0L, t.skewSec());
        assertFalse(t.await(1_000L));
    }

    @Test
    void skewIsScalerMinusAggregatorUsingSubtaskMinimum() {
        stdWm.set(100_000_000L);
        aggWm.set(100_000_000L - 3_600_000L);
        CsvKafkaReplayer.SkewThrottle t = new CsvKafkaReplayer.SkewThrottle(rest, 43_200L, 200L);
        assertEquals(3_600L, t.skewSec());
        assertFalse(t.await(1_000L));
        assertEquals(0L, t.pauses);
    }

    @Test
    void pausesWhileSkewedAndResumesWhenTheAggregatorCatchesUp() throws Exception {
        stdWm.set(200_000_000L);
        aggWm.set(200_000_000L - 50_000_000L);                // 50,000 s > 43,200 s
        CsvKafkaReplayer.SkewThrottle t = new CsvKafkaReplayer.SkewThrottle(rest, 43_200L, 200L);
        Thread catchUp = new Thread(() -> {
            try {
                Thread.sleep(700L);
            } catch (InterruptedException ignored) {
                return;
            }
            aggWm.set(200_000_000L - 1_000_000L);
        });
        catchUp.start();
        assertTrue(t.await(2_000L));
        catchUp.join();
        assertEquals(1L, t.pauses);
        assertTrue(t.pausedMs >= 500L);
        assertEquals(50_000L, t.maxSkewSeenSec);
    }

    @Test
    void keepaliveReleasesAboutTenSecondsWhileStillSkewed() {
        stdWm.set(300_000_000L);
        aggWm.set(300_000_000L - 50_000_000L);
        CsvKafkaReplayer.SkewThrottle t = new CsvKafkaReplayer.SkewThrottle(rest, 43_200L, 200L, 600L);
        long w0 = System.currentTimeMillis();
        assertTrue(t.await(5_000L));                       // 暂停约 600 毫秒后保活放行 / keepalive after ~600 ms
        assertTrue(System.currentTimeMillis() - w0 >= 500L);
        assertEquals(1L, t.keepalives);
        assertFalse(t.await(5_005L));                      // 十秒事件时间之内照常发送 / within the release
        assertFalse(t.await(5_009L));
        assertTrue(t.await(5_010L));                       // 仍偏差：再暂停、再放行，仍算同一次暂停 / same pause episode
        assertEquals(2L, t.keepalives);
        assertEquals(1L, t.pauses);
        aggWm.set(300_000_000L);                           // 追上之后恢复 / caught up
        assertFalse(t.await(5_020L));
        assertFalse(t.await(5_030L));
    }

    @Test
    void unreachableRestNeverBlocks() {
        CsvKafkaReplayer.SkewThrottle t = new CsvKafkaReplayer.SkewThrottle("http://127.0.0.1:1", 43_200L, 200L);
        assertFalse(t.await(1_000L));
        assertEquals(1L, t.pollErrors);
    }
}
