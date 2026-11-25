package dev.dimster.stats;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.WriterReaderPhaser;

/**
 * Thread-sharded latency recorder backed by per-thread double-buffered
 * {@link Histogram} shards coordinated via {@link WriterReaderPhaser}.
 *
 * <p>Replaces shared {@link org.HdrHistogram.Recorder} fields in {@link StatsCollector}
 * to eliminate multi-thread cache-line contention on {@code WriterReaderPhaser} and
 * {@code AtomicHistogram} counts on high-throughput consumer and producer hot paths.
 *
 * <p>Shards are allocated lazily on a thread's first write, so unused metrics retain
 * 0 bytes of histogram storage. Hot callers that process a batch of records at once
 * (e.g. a consumer poll batch) can also amortize the per-shard phaser barrier across
 * the entire batch via {@link #openBatch()}. Interval snapshots merge all registered
 * shards into a target {@link Histogram} while recycling per-shard inactive buffers,
 * and prune shards belonging to terminated threads without losing tail samples.
 */
public class ShardedRecorder {

    /**
     * Reusable per-shard batch recording handle returned by {@link #openBatch()}.
     * Holds a single {@link WriterReaderPhaser} critical section across a tight,
     * non-blocking batch loop so recording each sample performs plain array writes
     * with no atomic instructions or thread-local lookups.
     */
    public static final class Batch implements AutoCloseable {
        private final Shard shard;
        private Histogram active;
        private long criticalValue;

        private Batch(Shard shard) {
            this.shard = shard;
        }

        private Batch enter() {
            this.criticalValue = shard.phaser.writerCriticalSectionEnter();
            this.active = shard.active;
            return this;
        }

        /** Record one sample inside the open batch critical section. */
        public void recordValue(long value) {
            active.recordValue(value);
        }

        @Override
        public void close() {
            active = null;
            shard.phaser.writerCriticalSectionExit(criticalValue);
        }
    }

    /**
     * Per-thread recorder state. Static so {@code ThreadLocalMap -> Shard} never retains
     * an implicit back-reference to the enclosing {@link ShardedRecorder}.
     */
    private static final class Shard {
        final WriterReaderPhaser phaser = new WriterReaderPhaser();
        final Batch batch = new Batch(this);
        volatile Histogram active;
        // Secondary buffer swapped in during live drains; only touched under the
        // enclosing ShardedRecorder's monitor. Allocated lazily on first live drain.
        Histogram inactive;

        Shard(long highestTrackableValue, int numberOfSignificantValueDigits) {
            this.active = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
            this.active.setStartTimeStamp(System.currentTimeMillis());
        }

        void recordValue(long value) {
            long critical = phaser.writerCriticalSectionEnter();
            try {
                active.recordValue(value);
            } finally {
                phaser.writerCriticalSectionExit(critical);
            }
        }

        /**
         * Swap active and inactive buffers, flip the phaser to wait for any in-flight
         * write on the old active buffer, and return the drained inactive buffer.
         */
        Histogram drainLive(long highestTrackableValue, int numberOfSignificantValueDigits) {
            phaser.readerLock();
            try {
                if (inactive == null) {
                    inactive = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
                }
                inactive.reset();
                Histogram oldActive = active;
                active = inactive;
                inactive = oldActive;
                long now = System.currentTimeMillis();
                active.setStartTimeStamp(now);
                inactive.setEndTimeStamp(now);
                phaser.flipPhase(500_000L);
                return inactive;
            } finally {
                phaser.readerUnlock();
            }
        }

        /**
         * Drain a shard whose owner thread has already terminated without allocating
         * a replacement buffer. The phaser flip establishes happens-before with the
         * dead thread's final {@code writerCriticalSectionExit}.
         */
        Histogram drainDead() {
            phaser.readerLock();
            try {
                phaser.flipPhase(500_000L);
                Histogram drained = active;
                drained.setEndTimeStamp(System.currentTimeMillis());
                return drained;
            } finally {
                phaser.readerUnlock();
            }
        }
    }

    private final long highestTrackableValue;
    private final int numberOfSignificantValueDigits;
    // Live and not-yet-pruned exited writer threads; supports O(shards) drain and cleanup.
    private final ConcurrentHashMap<Thread, Shard> shards = new ConcurrentHashMap<>();
    // Lock-free O(1) shard lookup on the recordValue() hot path.
    private final ThreadLocal<Shard> localShard = ThreadLocal.withInitial(this::createShard);
    // Guarded by `this`; supplies interval start timestamps when no shards exist yet.
    private long lastIntervalTimeStampMs = System.currentTimeMillis();

    public ShardedRecorder(long highestTrackableValue, int numberOfSignificantValueDigits) {
        // Validate eagerly to match HdrHistogram Recorder's fail-fast constructor contract
        // while keeping shard histogram allocation lazy.
        if (highestTrackableValue < 2) {
            throw new IllegalArgumentException("highestTrackableValue must be >= 2");
        }
        if (numberOfSignificantValueDigits < 0 || numberOfSignificantValueDigits > 5) {
            throw new IllegalArgumentException("numberOfSignificantValueDigits must be between 0 and 5");
        }
        this.highestTrackableValue = highestTrackableValue;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
    }

    private Shard createShard() {
        Shard shard = new Shard(highestTrackableValue, numberOfSignificantValueDigits);
        // Register before returning so any subsequent recordValue() is visible to the reader.
        shards.put(Thread.currentThread(), shard);
        return shard;
    }

    /** Record a single sample into the calling thread's dedicated shard. */
    public void recordValue(long value) {
        localShard.get().recordValue(value);
    }

    /**
     * Open a reusable batch handle covering one non-blocking write burst on the
     * calling thread's shard. Must be closed promptly via try-with-resources.
     */
    public Batch openBatch() {
        return localShard.get().batch.enter();
    }

    /**
     * Drain all registered shards into a fresh merged interval {@link Histogram} and
     * prune shards owned by threads that have already terminated.
     */
    public synchronized Histogram getIntervalHistogram() {
        Histogram result = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
        getIntervalHistogramInto(result);
        return result;
    }

    /**
     * Reset {@code target} and drain all registered shards directly into it, avoiding
     * an intermediate {@link Histogram} allocation when the caller already holds one.
     */
    public synchronized void getIntervalHistogramInto(Histogram target) {
        target.reset();
        long minStart = Long.MAX_VALUE;
        long maxEnd = 0;

        for (Iterator<Map.Entry<Thread, Shard>> it = shards.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Thread, Shard> entry = it.next();
            // Check liveness BEFORE draining: a thread already dead cannot record any more
            // samples during or after the drain, whereas checking after would risk losing a
            // final write that raced between the drain and thread exit.
            boolean dead = !entry.getKey().isAlive();
            Shard shard = entry.getValue();
            Histogram interval;
            if (dead) {
                interval = shard.drainDead();
                it.remove();
            } else {
                interval = shard.drainLive(highestTrackableValue, numberOfSignificantValueDigits);
            }

            // Skip scanning empty shards (4-sig-digit E2E histograms span ~3 MB).
            if (interval.getTotalCount() > 0) {
                target.add(interval);
            }
            minStart = Math.min(minStart, interval.getStartTimeStamp());
            maxEnd = Math.max(maxEnd, interval.getEndTimeStamp());
        }

        long now = System.currentTimeMillis();
        if (minStart != Long.MAX_VALUE) {
            target.setStartTimeStamp(minStart);
            target.setEndTimeStamp(maxEnd);
        } else {
            target.setStartTimeStamp(lastIntervalTimeStampMs);
            target.setEndTimeStamp(now);
        }
        lastIntervalTimeStampMs = now;
    }

    /**
     * Clear all accumulated samples across live shards and prune dead threads.
     * Live threads retain their {@link ThreadLocal} shard and continue writing safely.
     */
    public synchronized void reset() {
        for (Iterator<Map.Entry<Thread, Shard>> it = shards.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Thread, Shard> entry = it.next();
            if (!entry.getKey().isAlive()) {
                it.remove();
                continue;
            }
            Shard shard = entry.getValue();
            Histogram drained = shard.drainLive(highestTrackableValue, numberOfSignificantValueDigits);
            drained.reset();
        }
        lastIntervalTimeStampMs = System.currentTimeMillis();
    }

    // Package-private for tests verifying lazy allocation and dead-thread pruning.
    int shardCount() {
        return shards.size();
    }
}
