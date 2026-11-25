package dev.dimster.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.HdrHistogram.Histogram;
import org.junit.Test;

/**
 * Verifies {@link ShardedRecorder}'s lazy shard allocation, lossless concurrent
 * draining, tail-sample preservation across thread exit, dead-thread pruning,
 * and reset semantics.
 */
public class ShardedRecorderTest {

    private static final long MAX_VALUE = TimeUnit.MINUTES.toMicros(5);
    private static final int SIG_DIGITS = 4;

    @Test
    public void unusedRecorderHasNoShardsAndProducesValidEmptyInterval() {
        long before = System.currentTimeMillis();
        ShardedRecorder recorder = new ShardedRecorder(MAX_VALUE, SIG_DIGITS);
        assertEquals(0, recorder.shardCount());

        Histogram h = recorder.getIntervalHistogram();
        long after = System.currentTimeMillis();

        assertEquals(0, h.getTotalCount());
        assertEquals(0, recorder.shardCount());
        assertTrue("start timestamp should be initialized", h.getStartTimeStamp() >= before);
        assertTrue("end timestamp should not precede start", h.getEndTimeStamp() >= h.getStartTimeStamp());
        assertTrue("end timestamp should be bounded by wall clock", h.getEndTimeStamp() <= after);
    }

    @Test
    public void constructorRejectsInvalidHistogramParametersEagerly() {
        try {
            new ShardedRecorder(1, 3);
            fail("Expected IllegalArgumentException for highestTrackableValue < 2");
        } catch (IllegalArgumentException expected) {
            // expected
        }
        try {
            new ShardedRecorder(1_000, 6);
            fail("Expected IllegalArgumentException for sigDigits > 5");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    @Test
    public void concurrentRecordingAndDrainingPreservesExactCountsAndValues() throws Exception {
        final int writerCount = 6;
        final int samplesPerWriter = 50_000;
        ShardedRecorder recorder = new ShardedRecorder(MAX_VALUE, SIG_DIGITS);

        CountDownLatch startGate = new CountDownLatch(1);
        AtomicBoolean writersDone = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Histogram merged = new Histogram(MAX_VALUE, SIG_DIGITS);
        Thread reader = new Thread(() -> {
            try {
                while (!writersDone.get()) {
                    merged.add(recorder.getIntervalHistogram());
                    Thread.yield();
                }
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, "sharded-reader");

        List<Thread> writers = new ArrayList<>();
        for (int t = 0; t < writerCount; t++) {
            final long baseValue = (t + 1) * 1_000L;
            Thread writer = new Thread(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < samplesPerWriter; i++) {
                        // Distinct value per writer plus deterministic spread across buckets.
                        recorder.recordValue(baseValue + (i % 500));
                    }
                } catch (Throwable ex) {
                    failure.compareAndSet(null, ex);
                }
            }, "sharded-writer-" + t);
            writers.add(writer);
            writer.start();
        }

        reader.start();
        startGate.countDown();

        for (Thread writer : writers) {
            writer.join(10_000);
        }
        writersDone.set(true);
        reader.join(10_000);

        if (failure.get() != null) {
            throw new AssertionError("Concurrent worker failed", failure.get());
        }

        // Final drain collects all remaining tail samples and prunes all exited writer shards.
        merged.add(recorder.getIntervalHistogram());

        long expectedTotal = (long) writerCount * samplesPerWriter;
        assertEquals(expectedTotal, merged.getTotalCount());
        assertEquals(0, recorder.shardCount());

        // Min value is 1_000, max value is 6_499; verify bucket accuracy within 4 sig digits.
        assertEquals(1_000L, merged.getMinValue());
        assertEquals(merged.highestEquivalentValue(6_499L), merged.getMaxValue());
        // Exact per-writer value range counts: each writer wrote samplesPerWriter values in [base, base+499].
        for (int t = 0; t < writerCount; t++) {
            long lo = (t + 1) * 1_000L;
            long hi = lo + 499L;
            assertEquals("writer " + t + " count", samplesPerWriter, merged.getCountBetweenValues(lo, hi));
        }

        // Next drain must be empty.
        assertEquals(0, recorder.getIntervalHistogram().getTotalCount());
    }

    @Test
    public void deadThreadTailSamplesArePreservedAndShardIsPruned() throws Exception {
        ShardedRecorder recorder = new ShardedRecorder(MAX_VALUE, SIG_DIGITS);

        Thread ephemeral = new Thread(() -> {
            recorder.recordValue(1_234);
            recorder.recordValue(56_789);
        }, "ephemeral-writer");
        ephemeral.start();
        ephemeral.join(5_000);

        // Shard stays registered until a drain or reset observes thread termination.
        assertEquals(1, recorder.shardCount());

        Histogram drained = recorder.getIntervalHistogram();
        assertEquals(2, drained.getTotalCount());
        assertEquals(1_234, drained.getMinValue());
        assertEquals(drained.highestEquivalentValue(56_789), drained.getMaxValue());
        assertEquals(0, recorder.shardCount());

        // Subsequent drain and reset stay empty.
        assertEquals(0, recorder.getIntervalHistogram().getTotalCount());
        recorder.reset();
        assertEquals(0, recorder.getIntervalHistogram().getTotalCount());
    }

    @Test
    public void concurrentShortLivedThreadsExitingDuringDrainsLoseZeroSamples() throws Exception {
        // Exercises the drain-vs-thread-exit boundary across many short-lived writers.
        final int rounds = 40;
        final int threadsPerRound = 4;
        final int samplesPerThread = 200;
        ShardedRecorder recorder = new ShardedRecorder(MAX_VALUE, SIG_DIGITS);

        AtomicBoolean done = new AtomicBoolean(false);
        Histogram merged = new Histogram(MAX_VALUE, SIG_DIGITS);
        Thread drainer = new Thread(() -> {
            while (!done.get()) {
                merged.add(recorder.getIntervalHistogram());
            }
        }, "continuous-drainer");
        drainer.start();

        for (int r = 0; r < rounds; r++) {
            List<Thread> batch = new ArrayList<>();
            for (int t = 0; t < threadsPerRound; t++) {
                Thread w = new Thread(() -> {
                    for (int i = 0; i < samplesPerThread; i++) {
                        recorder.recordValue(500 + i);
                    }
                });
                batch.add(w);
                w.start();
            }
            for (Thread w : batch) {
                w.join(5_000);
            }
        }

        done.set(true);
        drainer.join(5_000);
        merged.add(recorder.getIntervalHistogram());

        long expected = (long) rounds * threadsPerRound * samplesPerThread;
        assertEquals(expected, merged.getTotalCount());
        assertEquals(0, recorder.shardCount());
    }

    @Test
    public void resetClearsLiveShardsWithoutBreakingSubsequentWritesAndPrunesDeadShards() throws Exception {
        ShardedRecorder recorder = new ShardedRecorder(MAX_VALUE, SIG_DIGITS);

        // Record from an ephemeral thread that dies before reset().
        Thread deadWriter = new Thread(() -> recorder.recordValue(9_999), "dead-before-reset");
        deadWriter.start();
        deadWriter.join(5_000);

        // Keep a live thread active across reset() — both before and after an interval drain.
        CountDownLatch firstWriteDone = new CountDownLatch(1);
        CountDownLatch allowSecondWrite = new CountDownLatch(1);
        CountDownLatch secondWriteDone = new CountDownLatch(1);
        CountDownLatch allowThirdWrite = new CountDownLatch(1);
        CountDownLatch thirdWriteDone = new CountDownLatch(1);

        Thread liveWriter = new Thread(() -> {
            try {
                recorder.recordValue(1_111);
                firstWriteDone.countDown();

                allowSecondWrite.await();
                recorder.recordValue(2_222);
                secondWriteDone.countDown();

                allowThirdWrite.await();
                recorder.recordValue(3_333);
                thirdWriteDone.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "live-across-reset");
        liveWriter.start();

        assertTrue(firstWriteDone.await(5, TimeUnit.SECONDS));
        assertEquals(2, recorder.shardCount());

        // Reset before any drain (recycle == null on live shard) + dead shard present.
        recorder.reset();
        assertEquals("dead shard must be pruned by reset", 1, recorder.shardCount());
        assertEquals(0, recorder.getIntervalHistogram().getTotalCount());

        // Live thread writes again after reset, then drains cleanly.
        allowSecondWrite.countDown();
        assertTrue(secondWriteDone.await(5, TimeUnit.SECONDS));
        Histogram second = recorder.getIntervalHistogram();
        assertEquals(1, second.getTotalCount());
        assertEquals(second.highestEquivalentValue(2_222), second.getMaxValue());

        // Reset again when recycle != null, then verify subsequent write is still captured.
        recorder.reset();
        assertEquals(0, recorder.getIntervalHistogram().getTotalCount());

        allowThirdWrite.countDown();
        assertTrue(thirdWriteDone.await(5, TimeUnit.SECONDS));
        liveWriter.join(5_000);

        Histogram third = recorder.getIntervalHistogram();
        assertEquals(1, third.getTotalCount());
        assertEquals(third.highestEquivalentValue(3_333), third.getMaxValue());
        assertEquals(0, recorder.shardCount());
    }

    @Test
    public void concurrentBatchRecordingAndGetIntervalHistogramIntoAreLossless() throws Exception {
        final int writerCount = 6;
        final int batchesPerWriter = 100;
        final int batchSize = 500;
        ShardedRecorder recorder = new ShardedRecorder(MAX_VALUE, SIG_DIGITS);

        CountDownLatch startGate = new CountDownLatch(1);
        AtomicBoolean writersDone = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Histogram merged = new Histogram(MAX_VALUE, SIG_DIGITS);
        Histogram scratch = new Histogram(MAX_VALUE, SIG_DIGITS);
        // Seed scratch with stale data to verify getIntervalHistogramInto resets it first.
        scratch.recordValue(99_999);

        Thread reader = new Thread(() -> {
            try {
                while (!writersDone.get()) {
                    recorder.getIntervalHistogramInto(scratch);
                    merged.add(scratch);
                    Thread.yield();
                }
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, "batch-reader");

        List<Thread> writers = new ArrayList<>();
        for (int t = 0; t < writerCount; t++) {
            final long base = (t + 1) * 5_000L;
            Thread writer = new Thread(() -> {
                try {
                    startGate.await();
                    for (int b = 0; b < batchesPerWriter; b++) {
                        try (ShardedRecorder.Batch batch = recorder.openBatch()) {
                            for (int i = 0; i < batchSize; i++) {
                                batch.recordValue(base + i);
                            }
                        }
                    }
                } catch (Throwable ex) {
                    failure.compareAndSet(null, ex);
                }
            }, "batch-writer-" + t);
            writers.add(writer);
            writer.start();
        }

        reader.start();
        startGate.countDown();
        for (Thread writer : writers) {
            writer.join(10_000);
        }
        writersDone.set(true);
        reader.join(10_000);

        if (failure.get() != null) {
            throw new AssertionError("Concurrent batch worker failed", failure.get());
        }

        recorder.getIntervalHistogramInto(scratch);
        merged.add(scratch);

        long expectedTotal = (long) writerCount * batchesPerWriter * batchSize;
        assertEquals(expectedTotal, merged.getTotalCount());
        assertEquals(0, recorder.shardCount());
        assertEquals(5_000L, merged.getMinValue());
        assertEquals(merged.highestEquivalentValue(30_499L), merged.getMaxValue());
    }

    @Test
    public void openBatchReleasesPhaserOnException() {
        ShardedRecorder recorder = new ShardedRecorder(MAX_VALUE, SIG_DIGITS);
        try (ShardedRecorder.Batch batch = recorder.openBatch()) {
            batch.recordValue(1_000);
            throw new RuntimeException("boom");
        } catch (RuntimeException expected) {
            // expected
        }
        // Must not deadlock and must contain the sample recorded before the exception.
        Histogram h = recorder.getIntervalHistogram();
        assertEquals(1, h.getTotalCount());
        assertEquals(1_000L, h.getMinValue());
    }
}
