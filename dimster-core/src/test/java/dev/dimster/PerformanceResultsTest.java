package dev.dimster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dimster.worker.commands.PeriodStats;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Byte totals sit beside the per-period byte rates so figures over a window
 * (the resources report) use the raw counts instead of averaging rates.
 */
public class PerformanceResultsTest {

    private static PeriodStats period(long sent, long bytesSent, long received, long bytesReceived) {
        PeriodStats s = new PeriodStats();
        s.messagesSent = sent;
        s.bytesSent = bytesSent;
        s.messagesReceived = received;
        s.bytesReceived = bytesReceived;
        return s;
    }

    @Test
    public void totals_sum_every_recorded_period_and_serialize() throws Exception {
        var r = new TestResult.PerformanceResults();
        // A build period (sent only), then two drain periods of different lengths.
        r.addByteRates(period(1_000, 300_000, 0, 0), 200, 0);
        r.addByteRates(period(0, 0, 600, 180_000), 0, 120);
        r.addByteRates(period(0, 0, 400, 120_000), 0, 400);

        JsonNode json = new ObjectMapper().readTree(new ObjectMapper().writeValueAsString(r));

        assertEquals(300_000, json.get("bytesSent").asLong());
        assertEquals(300_000, json.get("bytesReceived").asLong());
        assertEquals(300.0, json.get("avgRecordBytes").asDouble(), 1e-9);
    }
}
