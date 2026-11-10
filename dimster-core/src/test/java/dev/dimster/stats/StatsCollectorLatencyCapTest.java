package dev.dimster.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import dev.dimster.MessageType;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.worker.commands.PeriodStats;
import org.HdrHistogram.Histogram;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Ensures reused topics cannot overflow consumer or Flink latency histograms. */
@RunWith(Parameterized.class)
public class StatsCollectorLatencyCapTest {
    private static final long PUBLISH_MILLIS = 1_700_000_000_000L;
    private static final byte[] NORMAL = {MessageType.NORMAL};

    private final long latencyMicros;
    private final long expectedMicros;

    @Parameterized.Parameters(name = "latency={0}, recorded={1}")
    public static Object[][] latencies() {
        long cap = StatsCollector.E2E_LATENCY_MAX_MICROS;
        return new Object[][] {
                {-1_000L, 0L},
                {0L, 0L},
                {12_345L, 12_345L},
                {cap, cap},
                {TimeUnit.HOURS.toMicros(13), cap},
                {71_951_082_736L, cap}, // The sample that crashed a consumer reading yesterday's topic.
                {TimeUnit.DAYS.toMicros(1), cap}
        };
    }

    public StatsCollectorLatencyCapTest(long latencyMicros, long expectedMicros) {
        this.latencyMicros = latencyMicros;
        this.expectedMicros = expectedMicros;
    }

    @Test
    public void consumerLatenciesAreBoundedWithoutLosingMessages() {
        StatsCollector stats = new StatsCollector();
        stats.setPerPartitionMetrics(true);
        stats.setRecordDeliveryLatency(true);
        Instant received = Instant.ofEpochMilli(PUBLISH_MILLIS)
                .plusNanos(TimeUnit.MICROSECONDS.toNanos(latencyMicros));

        stats.messageReceived(NORMAL, RecordFormat.RAW, "reused-topic", 0,
                PUBLISH_MILLIS, received, received);

        PeriodStats period = stats.snapshotAndReset();
        assertEquals(1, period.messagesReceived);
        assertEquals(1, period.totalMessagesReceived);
        assertEquals(NORMAL.length, period.bytesReceived);
        assertLatency(period.endToEndLatency);
        assertLatency(period.endToEndDeliveryLatency);
        assertLatency(period.perPartitionE2eLatency.get("reused-topic:0"));
        assertLatency(period.perPartitionE2eDeliveryLatency.get("reused-topic:0"));
    }

    @Test
    public void bothFlinkLatencySegmentsAreBounded() {
        StatsCollector stats = new StatsCollector();
        long emitMicros = TimeUnit.MILLISECONDS.toMicros(PUBLISH_MILLIS) + latencyMicros;
        Instant received = Instant.ofEpochMilli(PUBLISH_MILLIS)
                .plusNanos(TimeUnit.MICROSECONDS.toNanos(2 * latencyMicros));

        stats.flinkRecordReceived(NORMAL, RecordFormat.RAW, PUBLISH_MILLIS, emitMicros, received);

        PeriodStats period = stats.snapshotAndReset();
        assertLatency(period.producerToFlinkLatency);
        assertLatency(period.flinkToConsumerLatency);
    }

    private void assertLatency(Histogram histogram) {
        if (expectedMicros == 0) {
            // Non-positive latencies are dropped; per-partition recorders remain unallocated.
            if (histogram != null) {
                assertEquals(0, histogram.getTotalCount());
            }
            return;
        }
        assertNotNull(histogram);
        assertEquals(1, histogram.getTotalCount());
        // HDR reports the upper edge of the bucket containing the recorded value.
        assertEquals(histogram.highestEquivalentValue(expectedMicros), histogram.getMaxValue());
    }
}
