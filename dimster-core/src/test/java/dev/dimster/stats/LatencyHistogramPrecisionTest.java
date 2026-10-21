package dev.dimster.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import dev.dimster.worker.commands.PeriodStats;
import org.HdrHistogram.Histogram;
import org.junit.Test;

/**
 * Pins the precision/cost trade-off of the latency histograms (see
 * {@link StatsCollector#LATENCY_SIG_DIGITS}): values must stay within 0.01%,
 * and a period's histograms must stay small enough to snapshot cheaply on
 * every sample. At 5 digits a PeriodStats held ~100 MB of histograms.
 */
public class LatencyHistogramPrecisionTest {

    @Test
    public void recordedLatenciesAreWithinOneHundredthOfAPercent() {
        Histogram h = new Histogram(StatsCollector.E2E_LATENCY_MAX_MICROS, StatsCollector.LATENCY_SIG_DIGITS);
        for (long micros : new long[]{1_234, 98_765, 1_234_567, 61_000_000}) {
            h.reset();
            h.recordValue(micros);
            long reported = h.getValueAtPercentile(50);
            assertEquals("value " + micros, micros, reported, micros * 0.0001);
        }
    }

    @Test
    public void periodStatsHistogramsStaySmall() {
        PeriodStats stats = new PeriodStats();
        long bytes = stats.publishLatency.getEstimatedFootprintInBytes()
                + stats.scheduleLatency.getEstimatedFootprintInBytes()
                + stats.publishDelayLatency.getEstimatedFootprintInBytes()
                + stats.endToEndLatency.getEstimatedFootprintInBytes()
                + stats.endToEndDeliveryLatency.getEstimatedFootprintInBytes()
                + stats.commitSyncLatency.getEstimatedFootprintInBytes()
                + stats.processingTime.getEstimatedFootprintInBytes()
                + stats.pollBatchSize.getEstimatedFootprintInBytes();
        assertTrue("PeriodStats histograms use " + bytes / 1_000_000 + " MB", bytes < 20_000_000);
    }
}
