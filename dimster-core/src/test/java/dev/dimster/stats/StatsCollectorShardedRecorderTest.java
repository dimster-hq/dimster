package dev.dimster.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import dev.dimster.MessageType;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.worker.commands.PeriodStats;
import org.HdrHistogram.Histogram;
import org.junit.Test;

/**
 * End-to-end multi-threaded and lifecycle tests for {@link StatsCollector}'s
 * {@link ShardedRecorder}-backed metrics, covering concurrent consumer/producer
 * recording, {@link StatsCollector#snapshotAndReset()}, transaction commit
 * histogram draining, {@link StatsCollector#resetRecorders()}, and
 * {@link StatsCollector#resetAll()}.
 */
public class StatsCollectorShardedRecorderTest {

    private static final byte[] NORMAL = {MessageType.NORMAL, 1, 2, 3, 4};

    @Test
    public void multiThreadedMessageReceivedAndSnapshotAndResetLoseZeroSamples() throws Exception {
        final int consumerThreads = 6;
        final int messagesPerThread = 20_000;
        final long publishMillis = 1_700_000_000_000L;

        StatsCollector stats = new StatsCollector();
        CountDownLatch startGate = new CountDownLatch(1);
        AtomicBoolean done = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Histogram totalE2e = new Histogram(StatsCollector.E2E_LATENCY_MAX_MICROS, StatsCollector.LATENCY_SIG_DIGITS);
        long[] totalMessages = {0L};
        long[] totalBytes = {0L};

        Thread reader = new Thread(() -> {
            try {
                while (!done.get()) {
                    PeriodStats period = stats.snapshotAndReset();
                    totalMessages[0] += period.messagesReceived;
                    totalBytes[0] += period.bytesReceived;
                    totalE2e.add(period.endToEndLatency);
                    Thread.yield();
                }
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, "stats-reader");

        List<Thread> consumers = new ArrayList<>();
        for (int t = 0; t < consumerThreads; t++) {
            final long latencyMicros = (t + 1) * 2_000L;
            final Instant delivery = Instant.ofEpochMilli(publishMillis)
                    .plusNanos(TimeUnit.MICROSECONDS.toNanos(latencyMicros));
            Thread consumer = new Thread(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < messagesPerThread; i++) {
                        stats.messageReceived(NORMAL, RecordFormat.RAW, "taxi", 0,
                                publishMillis, delivery, delivery);
                    }
                } catch (Throwable ex) {
                    failure.compareAndSet(null, ex);
                }
            }, "consumer-" + t);
            consumers.add(consumer);
            consumer.start();
        }

        reader.start();
        startGate.countDown();
        for (Thread c : consumers) {
            c.join(10_000);
        }
        done.set(true);
        reader.join(10_000);

        if (failure.get() != null) {
            throw new AssertionError("Concurrent test failed", failure.get());
        }

        PeriodStats tail = stats.snapshotAndReset();
        totalMessages[0] += tail.messagesReceived;
        totalBytes[0] += tail.bytesReceived;
        totalE2e.add(tail.endToEndLatency);

        long expectedMessages = (long) consumerThreads * messagesPerThread;
        assertEquals(expectedMessages, totalMessages[0]);
        assertEquals(expectedMessages, tail.totalMessagesReceived);
        assertEquals(expectedMessages * NORMAL.length, totalBytes[0]);
        assertEquals(expectedMessages, totalE2e.getTotalCount());
        assertEquals(2_000L, totalE2e.getMinValue());
        assertEquals(totalE2e.highestEquivalentValue(12_000L), totalE2e.getMaxValue());
        // Unused delivery & Flink recorders must remain empty.
        assertEquals(0, tail.endToEndDeliveryLatency.getTotalCount());
        assertEquals(0, tail.producerToFlinkLatency.getTotalCount());
        assertEquals(0, tail.flinkToConsumerLatency.getTotalCount());
    }

    @Test
    public void resetRecordersAndResetAllKeepLiveThreadsWorking() throws Exception {
        final long publishMillis = 1_700_000_000_000L;
        final Instant firstDelivery = Instant.ofEpochMilli(publishMillis + 50);
        final Instant secondDelivery = Instant.ofEpochMilli(publishMillis + 80);
        final Instant thirdDelivery = Instant.ofEpochMilli(publishMillis + 120);

        StatsCollector stats = new StatsCollector();
        CountDownLatch phase1Done = new CountDownLatch(1);
        CountDownLatch allowPhase2 = new CountDownLatch(1);
        CountDownLatch phase2Done = new CountDownLatch(1);
        CountDownLatch allowPhase3 = new CountDownLatch(1);
        CountDownLatch phase3Done = new CountDownLatch(1);

        Thread worker = new Thread(() -> {
            try {
                stats.messageReceived(NORMAL, RecordFormat.RAW, "t", 0, publishMillis, firstDelivery, firstDelivery);
                stats.recordTransactionCommit(4_000);
                phase1Done.countDown();

                allowPhase2.await();
                stats.messageReceived(NORMAL, RecordFormat.RAW, "t", 0, publishMillis, secondDelivery, secondDelivery);
                stats.recordTransactionCommit(7_000);
                phase2Done.countDown();

                allowPhase3.await();
                stats.messageReceived(NORMAL, RecordFormat.RAW, "t", 0, publishMillis, thirdDelivery, thirdDelivery);
                stats.recordTransactionCommit(11_000);
                phase3Done.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "live-worker");
        worker.start();

        assertTrue(phase1Done.await(5, TimeUnit.SECONDS));

        // resetRecorders clears latency histograms while preserving cumulative counters.
        stats.resetRecorders();
        PeriodStats afterResetRecorders = stats.snapshotAndReset();
        assertEquals(0, afterResetRecorders.endToEndLatency.getTotalCount());
        assertEquals(0, stats.getTransactionCommitLatencyIntervalHistogram().getTotalCount());
        assertEquals(1, afterResetRecorders.totalMessagesReceived);
        assertEquals(1, afterResetRecorders.totalTransactionsCommitted);

        // Live thread continues writing after resetRecorders().
        allowPhase2.countDown();
        assertTrue(phase2Done.await(5, TimeUnit.SECONDS));
        PeriodStats phase2 = stats.snapshotAndReset();
        assertEquals(1, phase2.endToEndLatency.getTotalCount());
        assertEquals(phase2.endToEndLatency.highestEquivalentValue(80_000), phase2.endToEndLatency.getMaxValue());
        Histogram txn2 = stats.getTransactionCommitLatencyIntervalHistogram();
        assertEquals(1, txn2.getTotalCount());
        assertEquals(txn2.highestEquivalentValue(7_000), txn2.getMaxValue());

        // resetAll clears both histograms and cumulative counters.
        stats.resetAll();
        PeriodStats afterResetAll = stats.snapshotAndReset();
        assertEquals(0, afterResetAll.endToEndLatency.getTotalCount());
        assertEquals(0, afterResetAll.totalMessagesReceived);
        assertEquals(0, afterResetAll.totalTransactionsCommitted);
        assertEquals(0, stats.getTransactionCommitLatencyIntervalHistogram().getTotalCount());

        // Live thread continues writing cleanly after resetAll().
        allowPhase3.countDown();
        assertTrue(phase3Done.await(5, TimeUnit.SECONDS));
        worker.join(5_000);

        PeriodStats phase3 = stats.snapshotAndReset();
        assertEquals(1, phase3.endToEndLatency.getTotalCount());
        assertEquals(phase3.endToEndLatency.highestEquivalentValue(120_000), phase3.endToEndLatency.getMaxValue());
        assertEquals(1, phase3.totalMessagesReceived);
        Histogram txn3 = stats.getTransactionCommitLatencyIntervalHistogram();
        assertEquals(1, txn3.getTotalCount());
        assertEquals(txn3.highestEquivalentValue(11_000), txn3.getMaxValue());
    }

    @Test
    public void producerAndConsumerAuxiliaryRecordersMergeAcrossExitedThreads() throws Exception {
        final int threads = 4;
        final int opsPerThread = 500;
        StatsCollector stats = new StatsCollector();

        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            Thread w = new Thread(() -> {
                for (int i = 0; i < opsPerThread; i++) {
                    long now = System.nanoTime();
                    stats.recordScheduleLatency(100L + id * 10L);
                    stats.recordPublishDelay(now + TimeUnit.MICROSECONDS.toNanos(200L), now);
                    stats.recordSendSuccess(now - TimeUnit.MICROSECONDS.toNanos(50L), 64);
                    stats.recordTransactionCommit(1_000L + id * 100L);
                    stats.pollCompleted(25 + id);
                    stats.commitSyncCompleted(500L + id * 50L);
                    stats.processingTimeCompleted(300L + id * 20L);
                }
            }, "aux-worker-" + id);
            workers.add(w);
            w.start();
        }
        for (Thread w : workers) {
            w.join(10_000);
        }

        long expected = (long) threads * opsPerThread;
        PeriodStats period = stats.snapshotAndReset();
        assertEquals(expected, period.messagesSent);
        assertEquals(expected, period.publishLatency.getTotalCount());
        assertEquals(expected, period.scheduleLatency.getTotalCount());
        assertEquals(expected, period.publishDelayLatency.getTotalCount());
        assertEquals(expected, period.pollBatchSize.getTotalCount());
        assertEquals(expected, period.commitSyncLatency.getTotalCount());
        assertEquals(expected, period.processingTime.getTotalCount());
        assertEquals(expected, stats.getTransactionCommitLatencyIntervalHistogram().getTotalCount());

        // All worker threads exited before drain, so a second snapshot must be empty.
        PeriodStats next = stats.snapshotAndReset();
        assertEquals(0, next.publishLatency.getTotalCount());
        assertEquals(0, next.scheduleLatency.getTotalCount());
        assertEquals(0, next.publishDelayLatency.getTotalCount());
        assertEquals(0, next.pollBatchSize.getTotalCount());
        assertEquals(0, next.commitSyncLatency.getTotalCount());
        assertEquals(0, next.processingTime.getTotalCount());
        assertEquals(0, stats.getTransactionCommitLatencyIntervalHistogram().getTotalCount());
    }

    @Test
    public void directSendSuccessAndErrorCallbacksHandlePerPartitionAndTimeoutExceptions() {
        StatsCollector stats = new StatsCollector();
        stats.setPerPartitionMetrics(true);

        long sendTime = System.nanoTime() - TimeUnit.MICROSECONDS.toNanos(500);
        stats.recordSendSuccess(sendTime, 128, "orders", 0);
        stats.recordSendSuccess(sendTime, 256, "orders", 1);

        // Both unwrapped Kafka TimeoutException (direct Callback) and CompletionException-wrapped
        // TimeoutException (CompletableFuture path) must increment timeoutErrors.
        stats.recordSendError(new org.apache.kafka.common.errors.TimeoutException("direct"), sendTime);
        stats.recordSendError(
                new java.util.concurrent.CompletionException(
                        new org.apache.kafka.common.errors.TimeoutException("wrapped")),
                sendTime);
        stats.recordSendError(new RuntimeException("non-timeout"), sendTime);

        PeriodStats period = stats.snapshotAndReset();
        assertEquals(2, period.messagesSent);
        assertEquals(384, period.bytesSent);
        assertEquals(3, period.errors);
        assertEquals(2, period.timeoutErrors);
        assertEquals(2, period.totalTimeoutErrors);
        assertEquals(5, period.publishLatency.getTotalCount());
        assertEquals(1, period.perPartitionPublishLatency.get("orders:0").getTotalCount());
        assertEquals(1, period.perPartitionPublishLatency.get("orders:1").getTotalCount());
    }
}
