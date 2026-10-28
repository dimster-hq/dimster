package dev.dimster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.HdrHistogram.Histogram;
import org.junit.Test;

/**
 * The Flink latency split serializes flat, like the other latency fields,
 * so charting tools find e.g. producerToFlinkLatency99pct, and is absent
 * without a Flink stage, including after a resume reads a result back.
 */
public class TestResultFlinkLatencyJsonTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void flinkLatencyIsFlatUnderItsPrefix() throws Exception {
        Histogram h = new Histogram(60_000_000L, 3);
        h.recordValue(20_000);
        h.recordValue(40_000);

        var r = new TestResult.PerformanceResults();
        r.producerToFlinkLatency = new TestResult.LatencySeries();
        r.producerToFlinkLatency.add(h);
        r.aggregatedProducerToFlinkLatency = TestResult.LatencySummary.of(h);

        JsonNode json = mapper.valueToTree(r);
        assertEquals(40.0, json.get("producerToFlinkLatency99pct").get(0).asDouble(), 0.1);
        assertEquals(30.0, json.get("producerToFlinkLatencyAvg").get(0).asDouble(), 0.1);
        assertEquals(40.0, json.get("aggregatedProducerToFlinkLatency99pct").asDouble(), 0.1);
        assertTrue(json.get("aggregatedProducerToFlinkLatencyQuantiles").size() > 0);
        assertFalse(json.has("flinkToConsumerLatency99pct"));

        // Round trip, as a resumed run reads its partial results.
        var back = mapper.treeToValue(json, TestResult.PerformanceResults.class);
        assertEquals(40.0, back.aggregatedProducerToFlinkLatency.p99, 0.1);
        assertEquals(1, back.producerToFlinkLatency.p99.size());
    }

    @Test
    public void absentWithoutAFlinkStageEvenAfterARoundTrip() throws Exception {
        JsonNode json = mapper.valueToTree(new TestResult.PerformanceResults());
        var back = mapper.treeToValue(json, TestResult.PerformanceResults.class);
        String rewritten = mapper.writeValueAsString(back);
        assertFalse(rewritten, rewritten.contains("Flink"));
    }
}
