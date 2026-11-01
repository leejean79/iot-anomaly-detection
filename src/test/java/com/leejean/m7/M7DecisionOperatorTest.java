package com.leejean.m7;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 在线算子与离线驱动逐条相同（实现方案第三节核对一的单机版）：同一条合成到达序列（三月八日至三月三十一日，含三月二十七日夏令时
 * 切换与三月事件），一路经 {@link M7DecisionOperator}（Flink 测试工具，水位线在每条输入之前推进到它的到达时刻减一毫秒，中途做一次
 * 快照并在新算子上恢复），一路经 {@link M7DumpRunner#drive}，告警、复核与命令须逐条相同。
 * Online operator vs the offline driver on the same arrivals, with a snapshot and restore midway.
 */
class M7DecisionOperatorTest {

    private static final long FROM = FleetDecisionFunction.dayStart("2022-03-08");
    private static final long TO = FleetDecisionFunction.dayStart("2022-04-01");
    private static final long RESTORE_AT = FleetDecisionFunction.dayStart("2022-03-20") + 12 * 3600;

    @Test
    @DisplayName("在线算子（含快照与恢复）与离线驱动逐条相同 / operator with snapshot-restore equals the offline driver")
    void operatorEqualsDriver() throws Exception {
        FleetDecisionFunction.Params p = M7OfflineRunner.params(M7InputAssemblerTest.defaultArgs());
        List<M7DumpRunner.Arrival> xs = M7InputAssemblerTest.arrivals(FROM, TO);
        xs.sort(Comparator.comparingLong(x -> x.at));

        M7InputAssembler.Out expected = new M7InputAssembler.Out();
        M7DumpRunner.drive(new M7InputAssembler(p), new ArrayList<>(xs), TO + 60, expected);

        OutputTag<String> tag = new OutputTag<String>("m7-commands") { };
        List<String> main = new ArrayList<>();
        List<String> side = new ArrayList<>();
        int i = 0;
        OperatorSubtaskState snap;
        try (KeyedOneInputStreamOperatorTestHarness<Integer, M7Input, String> h = harness(p, tag)) {
            h.open();
            for (; i < xs.size() && xs.get(i).at < RESTORE_AT; i++) {
                feed(h, xs.get(i));
            }
            snap = h.snapshot(1L, 1L);
            collect(h, tag, main, side);
        }
        try (KeyedOneInputStreamOperatorTestHarness<Integer, M7Input, String> h = harness(p, tag)) {
            h.initializeState(snap);
            h.open();
            for (; i < xs.size(); i++) {
                feed(h, xs.get(i));
            }
            h.processWatermark((TO + 60) * 1000L);
            collect(h, tag, main, side);
        }

        ObjectMapper om = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        List<String> alerts = new ArrayList<>();
        List<String> review = new ArrayList<>();
        for (String line : main) {
            JsonNode n = om.readTree(line);
            if ("alert".equals(n.path("type").asText())) {
                alerts.add(om.writeValueAsString(om.treeToValue(n, FleetDecisionFunction.Alert.class)));
            } else {
                assertEquals("review", n.path("type").asText());
                review.add(om.writeValueAsString(om.treeToValue(n, FleetDecisionFunction.Review.class)));
            }
        }
        List<String> commands = new ArrayList<>();
        for (String line : side) {
            JsonNode n = om.readTree(line);
            assertEquals("command", n.path("type").asText());
            commands.add(om.writeValueAsString(om.treeToValue(n, FleetDecisionFunction.Command.class)));
        }
        assertTrue(expected.alerts.size() > 1000 && !expected.commands.isEmpty() && !expected.review.isEmpty(), "用例须有内容");
        assertEquals(asJson(om, expected.alerts), alerts);
        assertEquals(asJson(om, expected.review), review);
        assertEquals(asJson(om, expected.commands), commands);
    }

    private static KeyedOneInputStreamOperatorTestHarness<Integer, M7Input, String> harness(FleetDecisionFunction.Params p,
                                                                                            OutputTag<String> tag) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new M7DecisionOperator(p, tag)), x -> 0,
                Types.INT);
    }

    private static void feed(KeyedOneInputStreamOperatorTestHarness<Integer, M7Input, String> h, M7DumpRunner.Arrival x)
            throws Exception {
        h.processWatermark(x.at * 1000L - 1);
        h.processElement(new StreamRecord<>(x.in, x.at * 1000L));
    }

    private static void collect(KeyedOneInputStreamOperatorTestHarness<Integer, M7Input, String> h, OutputTag<String> tag,
                                List<String> main, List<String> side) {
        for (Object o : h.getOutput()) {
            if (o instanceof StreamRecord) {
                main.add((String) ((StreamRecord<?>) o).getValue());
            }
        }
        if (h.getSideOutput(tag) != null) {
            for (StreamRecord<String> r : h.getSideOutput(tag)) {
                side.add(r.getValue());
            }
        }
    }

    private static <T> List<String> asJson(ObjectMapper om, List<T> xs) throws Exception {
        List<String> out = new ArrayList<>();
        for (T x : xs) {
            out.add(om.writeValueAsString(x));
        }
        return out;
    }
}
