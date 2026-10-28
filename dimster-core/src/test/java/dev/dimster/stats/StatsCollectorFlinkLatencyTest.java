package dev.dimster.stats;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import dev.dimster.MessageType;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.worker.commands.PeriodStats;
import org.junit.Test;

/**
 * The Flink emit time splits end-to-end latency in two: producer send to
 * Flink emit, and Flink emit to consumer processing start.
 */
public class StatsCollectorFlinkLatencyTest {

    private static final long PUBLISH_MILLIS = 1_700_000_000_000L;
    private static final byte[] NORMAL = {MessageType.NORMAL};

    @Test
    public void splitsEndToEndLatencyAtTheFlinkEmitTime() {
        StatsCollector stats = new StatsCollector();
        long emitMicros = TimeUnit.MILLISECONDS.toMicros(PUBLISH_MILLIS) + 30_000;   // 30 ms later
        Instant start = Instant.ofEpochMilli(PUBLISH_MILLIS + 100);                  // 70 ms after that

        stats.flinkRecordReceived(NORMAL, RecordFormat.RAW, PUBLISH_MILLIS, emitMicros, start);

        PeriodStats period = stats.snapshotAndReset();
        assertEquals(1, period.producerToFlinkLatency.getTotalCount());
        assertEquals(30_000, period.producerToFlinkLatency.getMaxValue(), 5);
        assertEquals(1, period.flinkToConsumerLatency.getTotalCount());
        assertEquals(70_000, period.flinkToConsumerLatency.getMaxValue(), 10);

        // Interval histograms: the next period starts empty.
        assertEquals(0, stats.snapshotAndReset().producerToFlinkLatency.getTotalCount());
    }

    @Test
    public void probesAndSkewedClocksAreNotRecorded() {
        StatsCollector stats = new StatsCollector();
        long emitMicros = TimeUnit.MILLISECONDS.toMicros(PUBLISH_MILLIS) + 30_000;
        Instant start = Instant.ofEpochMilli(PUBLISH_MILLIS + 100);

        stats.flinkRecordReceived(new byte[]{MessageType.PROBE}, RecordFormat.RAW, PUBLISH_MILLIS, emitMicros, start);
        // A Flink clock behind the producer's: only the downstream half is positive.
        stats.flinkRecordReceived(NORMAL, RecordFormat.RAW, PUBLISH_MILLIS,
                TimeUnit.MILLISECONDS.toMicros(PUBLISH_MILLIS) - 1_000, start);

        PeriodStats period = stats.snapshotAndReset();
        assertEquals(0, period.producerToFlinkLatency.getTotalCount());
        assertEquals(1, period.flinkToConsumerLatency.getTotalCount());
    }
}
