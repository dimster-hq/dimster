package dev.dimster.kafka;

import dev.dimster.MessageType;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.utils.Sleeper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simulates processing time for consumer message batches across two operating modes:
 *
 * <p><b>Blocking mode</b> (asyncMode=false): Supports sequential (procParallelism=1),
 * constrained concurrency (procParallelism>1), and full parallelism (procParallelism=-1).
 * Uses aggregate sleep to reduce OS scheduling variance.
 *
 * <p><b>Async mode</b> (asyncMode=true): Uses {@link DelayQueue} for event-driven
 * deadline simulation. Records complete out-of-order based on simulated processing time.
 * Enables continuous polling without blocking the poll loop.
 */
public class ProcessingTimeSimulator {
    private static final Logger log = LoggerFactory.getLogger(ProcessingTimeSimulator.class);

    private volatile int procMsMin;
    private volatile int procMsMax;
    private final int procParallelism;
    private final boolean continuousMode;

    // Async mode state (null in blocking mode)
    private final DelayQueue<DelayedRecord> delayQueue;

    // Continuous mode parallelism tracking (accessed only from poll loop - no sync needed)
    private long lastDeadlineNanos = 0;  // for sequential mode (procParallelism == 1)
    private long[] workerLastDeadlineNanos;  // for constrained mode (procParallelism > 1)

    public ProcessingTimeSimulator(int procMsMin, int procMsMax, int procParallelism) {
        this(procMsMin, procMsMax, procParallelism, false);
    }

    public ProcessingTimeSimulator(int procMsMin, int procMsMax, int procParallelism, boolean continuousMode) {
        this.procMsMin = procMsMin;
        this.procMsMax = procMsMax;
        this.procParallelism = procParallelism;
        this.continuousMode = continuousMode;

        if (continuousMode) {
            this.delayQueue = new DelayQueue<>();
            // Initialize worker deadline tracking for constrained parallelism
            if (procParallelism > 1) {
                this.workerLastDeadlineNanos = new long[procParallelism];
            }
        } else {
            this.delayQueue = null;
        }
    }

    /**
     * Update processing time parameters at runtime without losing state.
     * Thread-safe (procMsMin/procMsMax are volatile).
     */
    public void updateProcessingTime(int minMs, int maxMs) {
        this.procMsMin = minMs;
        this.procMsMax = maxMs;
    }

    /**
     * Process a batch of messages with simulated processing time.
     * Calls the messageProcessor for each message with its simulated processing start time,
     * and sleeps appropriately based on the concurrency mode.
     *
     * @param batchSize number of messages in the batch
     * @param closing signal to check for early termination
     * @param callback callback for recording processing metrics
     * @param format layout of the record values, to find each one's message type
     * @param deliveryTime when consumer.poll() returned (base time for calculating processing start times)
     * @param messageProcessor callback to process each message with its simulated start time
     * @return total sleep time in milliseconds
     */
    public long processBatch(
            List<ConsumerRecord<String, byte[]>> records,
            AtomicBoolean closing,
            ConsumerCallback callback,
            RecordFormat format,
            Instant deliveryTime,
            MessageProcessor messageProcessor) {

        int batchSize = records.size();
        if (batchSize == 0 || procMsMax == 0) {
            // No processing time simulation - process messages with deliveryTime
            for (int i = 0; i < batchSize; i++) {
                messageProcessor.processMessage(i, deliveryTime);
            }
            return 0;
        }

        if (procParallelism == 1) {
            return processBatchSequential(records, closing, callback, format, deliveryTime, messageProcessor);
        } else if (procParallelism > 1) {
            return processBatchConstrained(records, closing, callback, format, deliveryTime, messageProcessor);
        } else { // procParallelism == -1
            return processBatchFullParallel(records, closing, callback, format, deliveryTime, messageProcessor);
        }
    }

    /**
     * Whether simulated processing time should be applied to this record.
     * Only NORMAL and CORRECTNESS payloads incur processing time; control-plane
     * types (AVAILABILITY, PROBE) and empty payloads complete instantly.
     * {@code format} locates the message type within the value.
     */
    private static boolean appliesProcessingTime(ConsumerRecord<String, byte[]> record, RecordFormat format) {
        int marker = format.typeOf(record.value());
        return marker == MessageType.NORMAL || marker == MessageType.CORRECTNESS;
    }

    private long randomProcMs() {
        return (procMsMin == procMsMax)
                ? procMsMin
                : ThreadLocalRandom.current().nextLong(procMsMin, procMsMax + 1);
    }

    /**
     * Sequential processing: process messages one by one.
     * Each message starts after all prior messages have finished.
     */
    private long processBatchSequential(
            List<ConsumerRecord<String, byte[]>> records,
            AtomicBoolean closing,
            ConsumerCallback callback,
            RecordFormat format,
            Instant deliveryTime,
            MessageProcessor messageProcessor) {

        long cumulativeMs = 0;
        int batchSize = records.size();

        for (int i = 0; i < batchSize; i++) {
            // This message starts after all prior messages finish
            Instant startTime = deliveryTime.plusMillis(cumulativeMs);

            // Process this message with its start time
            messageProcessor.processMessage(i, startTime);

            // Only NORMAL/CORRECTNESS records consume simulated processing time
            if (appliesProcessingTime(records.get(i), format)) {
                cumulativeMs += randomProcMs();
            }
        }

        // Sleep once for total time
        if (cumulativeMs > 0) {
            log.debug("SLEEP START - {} aggregate sleep ms for proc time simulation", cumulativeMs);
            long startNanos = System.nanoTime();
            if (Sleeper.sleepOrStop(closing, cumulativeMs, false)) {
                return 0;
            }
            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos);
            callback.processingTimeCompleted(latencyMicros);
            log.debug("SLEEP END - {} aggregate sleep ms for proc time simulation", cumulativeMs);
        }

        return cumulativeMs;
    }

    /**
     * Constrained concurrency: simulate N concurrent workers drawing from a shared queue.
     * Each record is assigned to the worker with the least cumulative work (so far),
     * simulating work-stealing behavior. Sleep once for the slowest worker's total time.
     */
    private long processBatchConstrained(
            List<ConsumerRecord<String, byte[]>> records,
            AtomicBoolean closing,
            ConsumerCallback callback,
            RecordFormat format,
            Instant deliveryTime,
            MessageProcessor messageProcessor) {

        int batchSize = records.size();

        // Generate processing times for all messages; non-NORMAL/CORRECTNESS get 0.
        long[] processingTimes = new long[batchSize];
        for (int i = 0; i < batchSize; i++) {
            processingTimes[i] = appliesProcessingTime(records.get(i), format) ? randomProcMs() : 0;
        }

        // Track cumulative work per worker (simulates work-stealing queue)
        long[] workerCumulativeTimes = new long[procParallelism];

        // Assign each record to worker with least cumulative work
        for (int i = 0; i < batchSize; i++) {
            // Find worker with minimum cumulative time
            int minWorker = 0;
            long minTime = workerCumulativeTimes[0];
            for (int w = 1; w < procParallelism; w++) {
                if (workerCumulativeTimes[w] < minTime) {
                    minTime = workerCumulativeTimes[w];
                    minWorker = w;
                }
            }

            // Message starts after all prior messages in this worker's queue finish
            Instant startTime = deliveryTime.plusMillis(workerCumulativeTimes[minWorker]);
            messageProcessor.processMessage(i, startTime);

            // Add this message's processing time to worker cumulative
            workerCumulativeTimes[minWorker] += processingTimes[i];
        }

        // Find max worker time (slowest worker determines total batch time)
        long maxWorkerTime = 0;
        for (int w = 0; w < procParallelism; w++) {
            maxWorkerTime = Math.max(maxWorkerTime, workerCumulativeTimes[w]);
        }

        // Sleep once for the slowest worker's time (simulating concurrent workers finishing)
        if (maxWorkerTime > 0) {
            log.debug("SLEEP START - {} aggregate sleep ms for proc time simulation", maxWorkerTime);
            long startNanos = System.nanoTime();
            if (Sleeper.sleepOrStop(closing, maxWorkerTime, false)) {
                return 0;
            }
            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos);
            callback.processingTimeCompleted(latencyMicros);
            log.debug("SLEEP END - {} aggregate sleep ms for proc time simulation", maxWorkerTime);
        }

        return maxWorkerTime;
    }

    /**
     * Full parallelism: all messages process concurrently (start at same time),
     * sleep once for the maximum processing time.
     */
    private long processBatchFullParallel(
            List<ConsumerRecord<String, byte[]>> records,
            AtomicBoolean closing,
            ConsumerCallback callback,
            RecordFormat format,
            Instant deliveryTime,
            MessageProcessor messageProcessor) {

        int batchSize = records.size();

        // Generate processing times and process all messages (all start at deliveryTime).
        // Only NORMAL/CORRECTNESS records contribute to maxTime.
        long maxTime = 0;
        for (int i = 0; i < batchSize; i++) {
            if (appliesProcessingTime(records.get(i), format)) {
                maxTime = Math.max(maxTime, randomProcMs());
            }

            // All messages start at deliveryTime (concurrent)
            messageProcessor.processMessage(i, deliveryTime);
        }

        // Sleep once for max duration
        if (maxTime > 0) {
            log.debug("SLEEP START - {} aggregate sleep ms for proc time simulation", maxTime);
            long startNanos = System.nanoTime();
            if (Sleeper.sleepOrStop(closing, maxTime, false)) {
                return 0;
            }
            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos);
            callback.processingTimeCompleted(latencyMicros);
            log.debug("SLEEP END - {} aggregate sleep ms for proc time simulation", maxTime);
        }

        return maxTime;
    }

    // ==================== Continuous Mode Methods ====================

    /**
     * Offer a record for async processing (continuous mode only).
     * Creates DelayedRecord with deadline based on parallelism mode and processing time.
     *
     * <p>If procMs==0 AND startOffset==0 AND queue is empty: return CompletedRecord immediately.
     * Otherwise: queue the record (even if procMs==0) to maintain ordering with inflight records.
     *
     * @param record the consumer record to process
     * @param deliveryTime when consumer.poll() returned
     * @param format the layout of the record value, to find its message type
     * @return CompletedRecord if processed immediately, null if queued
     */
    public CompletedRecord offerAsync(ConsumerRecord<String, byte[]> record,
                                      Instant deliveryTime,
                                      RecordFormat format) {
        if (!continuousMode) {
            throw new IllegalStateException("offerAsync() requires continuousMode=true");
        }

        // Only NORMAL/CORRECTNESS records incur simulated processing time.
        long procMs = appliesProcessingTime(record, format) ? randomProcMs() : 0;

        // Calculate when this record should start processing based on parallelism mode
        long startOffsetMs = calculateStartOffsetMs(procMs);
        Instant simulatedStartTime = deliveryTime.plusMillis(startOffsetMs);

        // Only process immediately if procMs==0 AND startOffset==0 AND queue is empty
        if (procMs == 0 && startOffsetMs == 0 && delayQueue.isEmpty()) {
            return new CompletedRecord(record, deliveryTime, deliveryTime);
        }

        // Queue the record with calculated start time
        delayQueue.put(new DelayedRecord(record, deliveryTime, simulatedStartTime, procMs, startOffsetMs));
        return null;
    }

    /**
     * Calculate when this record should start processing based on procParallelism mode.
     * Called from poll loop thread - no synchronization needed.
     *
     * @param procMs the processing time for this record
     * @return offset in milliseconds from deliveryTime when processing starts
     */
    private long calculateStartOffsetMs(long procMs) {
        if (procParallelism == -1) {
            // Full parallelism: all records start immediately
            return 0;
        } else if (procParallelism == 1) {
            // Sequential: next record starts after last one's deadline
            long currentNanos = System.nanoTime();
            long startOffsetNanos = Math.max(0, lastDeadlineNanos - currentNanos);
            long startOffsetMs = TimeUnit.NANOSECONDS.toMillis(startOffsetNanos);

            // Update lastDeadlineNanos for next record
            lastDeadlineNanos = currentNanos + startOffsetNanos + TimeUnit.MILLISECONDS.toNanos(procMs);

            return startOffsetMs;
        } else {
            // Constrained parallelism: pick worker with earliest deadline (load balancing)
            int workerIdx = 0;
            long minDeadline = workerLastDeadlineNanos[0];
            for (int i = 1; i < procParallelism; i++) {
                if (workerLastDeadlineNanos[i] < minDeadline) {
                    minDeadline = workerLastDeadlineNanos[i];
                    workerIdx = i;
                }
            }

            long currentNanos = System.nanoTime();
            long startOffsetNanos = Math.max(0, workerLastDeadlineNanos[workerIdx] - currentNanos);
            long startOffsetMs = TimeUnit.NANOSECONDS.toMillis(startOffsetNanos);

            // Update this worker's deadline
            workerLastDeadlineNanos[workerIdx] = currentNanos + startOffsetNanos + TimeUnit.MILLISECONDS.toNanos(procMs);

            return startOffsetMs;
        }
    }

    /**
     * Drain completed records (deadline expired) and return their metadata.
     * Does NOT invoke callbacks - the wrapper should process records using the returned timing info.
     *
     * @return list of completed records (topic, partition, offset, deliveryTime, simulatedStartTime)
     */
    public List<CompletedRecord> drainCompleted() {
        if (!continuousMode) {
            throw new IllegalStateException("drainCompleted() requires continuousMode=true");
        }

        List<DelayedRecord> expired = new ArrayList<>();
        delayQueue.drainTo(expired);  // only drains records past their deadline

        List<CompletedRecord> completed = new ArrayList<>(expired.size());
        for (DelayedRecord dr : expired) {
            // Return completed record with full record and timing information
            completed.add(new CompletedRecord(dr.record, dr.deliveryTime, dr.simulatedStartTime));
        }
        return completed;
    }

    public CompletedRecord takeCompleted(long timeout, TimeUnit unit) {
        try {
            DelayedRecord dr = delayQueue.poll(timeout, unit);
            if (dr != null) {
                return new CompletedRecord(dr.record, dr.deliveryTime, dr.simulatedStartTime);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return null;
    }

    /**
     * Get current queue size for capacity checks (async mode only).
     *
     * @return number of records currently in the async queue
     */
    public int getQueueSize() {
        return continuousMode ? delayQueue.size() : 0;
    }

    // ==================== Inner Classes ====================

    /**
     * Delayed record for continuous mode processing. Implements {@link Delayed} to work with
     * {@link DelayQueue}, which automatically orders by deadline and only returns
     * records past their deadline on {@code drainTo()}.
     */
    static class DelayedRecord implements Delayed {
        final ConsumerRecord<String, byte[]> record;
        final Instant deliveryTime;
        final Instant simulatedStartTime;
        final long deadlineNanos;

        DelayedRecord(ConsumerRecord<String, byte[]> record,
                      Instant deliveryTime,
                      Instant simulatedStartTime,
                      long processingMs,
                      long startOffsetMs) {
            this.record = record;
            this.deliveryTime = deliveryTime;
            this.simulatedStartTime = simulatedStartTime;
            // Deadline = now + time until record starts + processing time
            this.deadlineNanos = System.nanoTime() +
                TimeUnit.MILLISECONDS.toNanos(startOffsetMs + processingMs);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(deadlineNanos - System.nanoTime(),
                               TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(deadlineNanos, ((DelayedRecord) o).deadlineNanos);
        }
    }

    /**
     * Completed record with full ConsumerRecord and timing information for processing.
     * Returned by {@link #drainCompleted()} and {@link #offerAsync(ConsumerRecord, Instant, RecordFormat)}.
     */
    public record CompletedRecord(ConsumerRecord<String, byte[]> record,
                                   Instant deliveryTime, Instant simulatedStartTime) {}

    /**
     * Callback interface for processing a single message with its simulated processing start time.
     */
    @FunctionalInterface
    public interface MessageProcessor {
        /**
         * Process a single message with its simulated processing start time.
         *
         * @param messageIdx index of the message in the batch
         * @param simulatedProcessingStartTime when this message starts processing (for e2e latency calculation)
         */
        void processMessage(int messageIdx, Instant simulatedProcessingStartTime);
    }
}
