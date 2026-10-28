package dev.dimster.stats;

import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import dev.dimster.MessageType;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.kafka.ConsumerCallback;
import dev.dimster.worker.commands.CountersStats;
import dev.dimster.worker.commands.PerPartitionStats;
import dev.dimster.worker.commands.PeriodStats;

public class StatsCollector implements ConsumerCallback {

    public static final long NON_E2E_LATENCY_MAX_MICROS = TimeUnit.MINUTES.toMicros(5);
    public static final long E2E_LATENCY_MAX_MICROS = TimeUnit.HOURS.toMicros(12);

    /**
     * Precision of the main latency histograms: 4 significant digits, so a
     * recorded value is accurate to 1 part in 10,000 (0.01%), e.g. within
     * 4 us at 100 ms and 32 us at 1 s. Reports show milliseconds to two
     * decimals, so this is invisible below roughly 250 ms.
     */
    public static final int LATENCY_SIG_DIGITS = 4;

    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder errors = new LongAdder();
    private final LongAdder timeoutErrors = new LongAdder();
    private final LongAdder pollErrors = new LongAdder();
    private final LongAdder bytesSent = new LongAdder();

    private final LongAdder messagesReceived = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();

    private final LongAdder totalMessagesSent = new LongAdder();
    private final LongAdder totalBytesSent = new LongAdder();
    private final LongAdder totalErrors = new LongAdder();
    private final LongAdder totalTimeoutErrors = new LongAdder();
    private final LongAdder totalPollErrors = new LongAdder();
    private final LongAdder totalMessagesReceived = new LongAdder();
    private final LongAdder totalBytesReceived = new LongAdder();

    private final LongAdder probesReceived = new LongAdder();

    private final LongAdder totalPolls = new LongAdder();
    private final LongAdder totalCommitSync = new LongAdder();
    private final LongAdder totalCommitAsync = new LongAdder();
    // Aggregate group-managed partition ownership changes across all classic
    // consumers on this worker. Per-consumer detail remains available in logs.
    private final LongAdder totalPartitionsAssigned = new LongAdder();
    private final LongAdder totalPartitionsRevoked = new LongAdder();

    // Producer-transactions counters. Cumulative since worker start (or last
    // resetAll). Cumulative-only matches existing patterns like totalCommitSync;
    // PrometheusEndpoint exposes them as Prometheus counters.
    private final LongAdder totalTransactionsCommitted = new LongAdder();
    private final LongAdder totalTransactionsAborted = new LongAdder();

    private final Recorder publishLatencyRecorder = new Recorder(NON_E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    private final Recorder scheduleLatencyRecorder = new Recorder(NON_E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    private final Recorder publishDelayLatencyRecorder = new Recorder(NON_E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    private final Recorder endToEndLatencyRecorder = new Recorder(E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    // Delivery latency: publish timestamp → poll return time (before per-record processing).
    // Only meaningful when consumers simulate processing time — otherwise the value is
    // indistinguishable from endToEndLatency. Gated by recordDeliveryLatency so the
    // histogram + per-record CPU cost are skipped on the common no-processing workload.
    private final Recorder endToEndDeliveryLatencyRecorder = new Recorder(E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    private volatile boolean recordDeliveryLatency = false;
    // End-to-end latency split at the Flink job, from the emit time it stamps
    // on each output record (FlinkEmitTime): producer send → Flink emit, and
    // Flink emit → consumer processing start. Empty without a Flink stage.
    private final Recorder producerToFlinkLatencyRecorder = new Recorder(E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    private final Recorder flinkToConsumerLatencyRecorder = new Recorder(E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    private final Recorder pollBatchSizeRecorder = new Recorder(100_000, 3);
    private final Recorder commitSyncLatencyRecorder = new Recorder(NON_E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    private final Recorder processingTimeRecorder = new Recorder(NON_E2E_LATENCY_MAX_MICROS, 3);
    /**
     * Producer transaction commit latency. Read directly by PrometheusEndpoint
     * for /metrics scraping (we deliberately do not serialize this across the
     * worker→coordinator hop — local scraping is sufficient for benchmarking
     * EOS workloads, and adding cross-hop histogram transport would expand
     * scope beyond the v1 transactions feature).
     */
    private final Recorder transactionCommitLatencyRecorder = new Recorder(NON_E2E_LATENCY_MAX_MICROS, 3);

    // Per-partition tracking (3 significant digits to keep memory reasonable)
    private static final int PER_PARTITION_SIG_DIGITS = 3;
    private volatile boolean perPartitionEnabled = false;
    private final ConcurrentHashMap<String, Recorder> ppPublishLatencyRecorders = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Recorder> ppE2eLatencyRecorders = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Recorder> ppE2eDeliveryLatencyRecorders = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Histogram> ppPublishLatencyCumulative = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Histogram> ppE2eLatencyCumulative = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Histogram> ppE2eDeliveryLatencyCumulative = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> ppMessagesSent = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> ppMessagesReceived = new ConcurrentHashMap<>();

    // Per-consumer tracking
    private volatile boolean perConsumerEnabled = false;
    private final ConcurrentHashMap<String, LongAdder> pcMessagesReceived = new ConcurrentHashMap<>();

    public void setPerPartitionMetrics(boolean enabled) {
        this.perPartitionEnabled = enabled;
    }

    public void setPerConsumerMetrics(boolean enabled) {
        this.perConsumerEnabled = enabled;
    }

    // Toggled by LocalWorker whenever consumer processing-time config changes.
    // When false, messageReceived skips delivery-latency computation entirely.
    public void setRecordDeliveryLatency(boolean enabled) {
        this.recordDeliveryLatency = enabled;
    }

    public void recordScheduleLatency(long scheduleMicros) {
        long safe = Math.min(scheduleMicros, NON_E2E_LATENCY_MAX_MICROS);
        scheduleLatencyRecorder.recordValue(safe);
    }

    public void recordPublishDelay(long sendTimeNanos, long intendedSendTimeNanos) {
        long sendDelayMicros = TimeUnit.NANOSECONDS.toMicros(sendTimeNanos - intendedSendTimeNanos);
        long sendDelayMicrosSafe = Math.min(sendDelayMicros, NON_E2E_LATENCY_MAX_MICROS);
        publishDelayLatencyRecorder.recordValue(sendDelayMicrosSafe);
    }

    public void recordSendSuccess(long sendTimeNanos, int payloadSize) {
        messagesSent.increment();
        totalMessagesSent.increment();
        bytesSent.add(payloadSize);
        totalBytesSent.add(payloadSize);

        long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTimeNanos);
        long latencyMicrosSafe = Math.min(latencyMicros, NON_E2E_LATENCY_MAX_MICROS - 1);
        publishLatencyRecorder.recordValue(latencyMicrosSafe);
    }

    public void recordSendSuccess(long sendTimeNanos, int payloadSize, String topicPartition) {
        recordSendSuccess(sendTimeNanos, payloadSize);

        if (perPartitionEnabled) {
            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTimeNanos);
            long safe = Math.min(latencyMicros, NON_E2E_LATENCY_MAX_MICROS - 1);
            ppPublishLatencyRecorders
                    .computeIfAbsent(topicPartition, k -> new Recorder(NON_E2E_LATENCY_MAX_MICROS, PER_PARTITION_SIG_DIGITS))
                    .recordValue(safe);
            ppMessagesSent
                    .computeIfAbsent(topicPartition, k -> new LongAdder())
                    .increment();
        }
    }

    /**
     * Record a successful producer transaction commit. Increments the
     * cumulative committed counter and feeds the commit-latency histogram
     * used by the local /metrics endpoint.
     */
    public void recordTransactionCommit(long latencyMicros) {
        totalTransactionsCommitted.increment();
        long safe = Math.min(latencyMicros, NON_E2E_LATENCY_MAX_MICROS);
        transactionCommitLatencyRecorder.recordValue(safe);
    }

    /** Record a producer transaction abort (whatever the reason). */
    public void recordTransactionAbort() {
        totalTransactionsAborted.increment();
    }

    /**
     * Snapshot of the most-recent commit-latency interval. Read directly by
     * PrometheusEndpoint — see field comment for why this isn't surfaced via
     * PeriodStats.
     */
    public Histogram getTransactionCommitLatencyIntervalHistogram() {
        return transactionCommitLatencyRecorder.getIntervalHistogram();
    }

    public long getTotalTransactionsCommitted() {
        return totalTransactionsCommitted.sum();
    }

    public long getTotalTransactionsAborted() {
        return totalTransactionsAborted.sum();
    }

    public void recordSendError(Throwable ex, long sendTimeNanos) {
        if (ex.getCause() instanceof org.apache.kafka.common.errors.TimeoutException) {
            timeoutErrors.increment();
            totalTimeoutErrors.increment();
        }
        errors.increment();
        totalErrors.increment();

        // Record publish latency even for failed sends — shows how long the failed attempt took
        long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTimeNanos);
        long latencyMicrosSafe = Math.min(latencyMicros, NON_E2E_LATENCY_MAX_MICROS - 1);
        publishLatencyRecorder.recordValue(latencyMicrosSafe);
    }

    @Override
    public void messageReceived(byte[] payload, RecordFormat format, String topic, int partition,
                                long publishTimestampMillis, Instant deliveryTime, Instant processingStartTime) {
        if (format.typeOf(payload) == MessageType.PROBE) {
            probesReceived.increment();
            return;
        }
        messageReceived(payload.length, publishTimestampMillis, deliveryTime, processingStartTime);

        if (perPartitionEnabled) {
            String key = topic + ":" + partition;
            long publishTimeNanos = TimeUnit.MILLISECONDS.toNanos(publishTimestampMillis);

            // Per-partition e2e latency (measured at processing start time)
            long processingStartTimeNanos = TimeUnit.SECONDS.toNanos(processingStartTime.getEpochSecond()) + processingStartTime.getNano();
            long endToEndLatencyMicros = TimeUnit.NANOSECONDS.toMicros(processingStartTimeNanos - publishTimeNanos);
            if (endToEndLatencyMicros > 0) {
                ppE2eLatencyRecorders
                        .computeIfAbsent(key, k -> new Recorder(E2E_LATENCY_MAX_MICROS, PER_PARTITION_SIG_DIGITS))
                        .recordValue(endToEndLatencyMicros);
                ppMessagesReceived
                        .computeIfAbsent(key, k -> new LongAdder())
                        .increment();
            }

            // Per-partition delivery e2e latency (measured at poll return, before processing).
            // Gated so we don't allocate per-partition recorders for an uninformative metric
            // when no consumer simulates processing time.
            if (recordDeliveryLatency) {
                long deliveryTimeNanos = TimeUnit.SECONDS.toNanos(deliveryTime.getEpochSecond()) + deliveryTime.getNano();
                long deliveryLatencyMicros = TimeUnit.NANOSECONDS.toMicros(deliveryTimeNanos - publishTimeNanos);
                if (deliveryLatencyMicros > 0) {
                    ppE2eDeliveryLatencyRecorders
                            .computeIfAbsent(key, k -> new Recorder(E2E_LATENCY_MAX_MICROS, PER_PARTITION_SIG_DIGITS))
                            .recordValue(deliveryLatencyMicros);
                }
            }
        }
    }

    @Override
    public void messageReceived(int payloadSize, long publishTimestampMillis, Instant deliveryTime, Instant processingStartTime) {
        messagesReceived.increment();
        totalMessagesReceived.increment();
        bytesReceived.add(payloadSize);
        totalBytesReceived.add(payloadSize);

        long publishTimeNanos = TimeUnit.MILLISECONDS.toNanos(publishTimestampMillis);

        // Standard e2e latency: from publish to when processing starts
        long processingStartTimeNanos = TimeUnit.SECONDS.toNanos(processingStartTime.getEpochSecond()) + processingStartTime.getNano();
        long endToEndLatencyMicros = TimeUnit.NANOSECONDS.toMicros(processingStartTimeNanos - publishTimeNanos);
        if (endToEndLatencyMicros > 0) {
            endToEndLatencyRecorder.recordValue(endToEndLatencyMicros);
        }

        // Delivery e2e latency: from publish to when poll() returns
        // (only informative when consumers simulate processing time, otherwise equals endToEndLatency).
        if (recordDeliveryLatency) {
            long deliveryTimeNanos = TimeUnit.SECONDS.toNanos(deliveryTime.getEpochSecond()) + deliveryTime.getNano();
            long endToEndDeliveryLatencyMicros = TimeUnit.NANOSECONDS.toMicros(deliveryTimeNanos - publishTimeNanos);
            if (endToEndDeliveryLatencyMicros > 0) {
                endToEndDeliveryLatencyRecorder.recordValue(endToEndDeliveryLatencyMicros);
            }
        }
    }

    @Override
    public void flinkRecordReceived(byte[] payload, RecordFormat format, long publishTimestampMillis,
                                    long flinkEmitMicros, Instant processingStartTime) {
        if (format.typeOf(payload) == MessageType.PROBE) {
            return;
        }
        // Like e2e, non-positive values (clock skew between hosts) are dropped.
        long producerToFlinkMicros = flinkEmitMicros - TimeUnit.MILLISECONDS.toMicros(publishTimestampMillis);
        if (producerToFlinkMicros > 0) {
            producerToFlinkLatencyRecorder.recordValue(producerToFlinkMicros);
        }
        long processingStartMicros = TimeUnit.SECONDS.toMicros(processingStartTime.getEpochSecond())
                + processingStartTime.getNano() / 1_000;
        long flinkToConsumerMicros = processingStartMicros - flinkEmitMicros;
        if (flinkToConsumerMicros > 0) {
            flinkToConsumerLatencyRecorder.recordValue(flinkToConsumerMicros);
        }
    }

    @Override
    public void error() {
        pollErrors.increment();
        totalPollErrors.increment();
    }

    @Override
    public void pollCompleted(int messageCount) {
        totalPolls.increment();
        pollBatchSizeRecorder.recordValue(messageCount);
    }

    @Override
    public void commitSyncCompleted(long latencyMicros) {
        totalCommitSync.increment();
        long safe = Math.min(latencyMicros, NON_E2E_LATENCY_MAX_MICROS);
        commitSyncLatencyRecorder.recordValue(safe);
    }

    @Override
    public void processingTimeCompleted(long latencyMicros) {
        long safe = Math.min(latencyMicros, NON_E2E_LATENCY_MAX_MICROS);
        processingTimeRecorder.recordValue(safe);
    }

    @Override
    public void commitAsyncCompleted() {
        totalCommitAsync.increment();
    }

    @Override
    public void batchReceived(String consumerName, int messageCount) {
        if (perConsumerEnabled) {
            pcMessagesReceived
                    .computeIfAbsent(consumerName, k -> new LongAdder())
                    .add(messageCount);
        }
    }

    @Override
    public void partitionsAssigned(int partitionCount) {
        // Count partitions rather than callback invocations so eager and
        // cooperative assignors remain comparable when callbacks batch differently.
        totalPartitionsAssigned.add(partitionCount);
    }

    @Override
    public void partitionsRevoked(int partitionCount) {
        totalPartitionsRevoked.add(partitionCount);
    }

    /**
     * Snapshot all period counters/histograms and atomically reset them.
     * When per-partition metrics are enabled, the per-partition publish and
     * e2e interval histograms are always attached to the returned PeriodStats.
     * The cost is just Map references — the underlying Histograms have already
     * been drained from their Recorders to be accumulated into the cumulative
     * histograms. PrometheusEndpoint reads these in-process for interval-based
     * scrape output; the explore engine uses the e2e map for worst-partition
     * detection. Wire serialization of these histograms (when sending to a
     * remote coordinator) is gated separately at the WorkerHttpServer to avoid
     * expensive HTTP payloads on every period snapshot.
     */
    public PeriodStats snapshotAndReset() {
        PeriodStats stats = new PeriodStats();

        stats.messagesSent = messagesSent.sumThenReset();
        stats.bytesSent = bytesSent.sumThenReset();
        stats.timeoutErrors = timeoutErrors.sumThenReset();
        stats.errors = errors.sumThenReset();
        stats.pollErrors = pollErrors.sumThenReset();

        stats.messagesReceived = messagesReceived.sumThenReset();
        stats.bytesReceived = bytesReceived.sumThenReset();

        stats.totalMessagesSent = totalMessagesSent.sum();
        stats.totalBytesSent = totalBytesSent.sum();
        stats.totalErrors = totalErrors.sum();
        stats.totalTimeoutErrors = totalTimeoutErrors.sum();
        stats.totalPollErrors = totalPollErrors.sum();
        stats.totalMessagesReceived = totalMessagesReceived.sum();
        stats.totalBytesReceived = totalBytesReceived.sum();

        stats.totalPolls = totalPolls.sum();
        stats.totalCommitSync = totalCommitSync.sum();
        stats.totalCommitAsync = totalCommitAsync.sum();
        stats.totalTransactionsCommitted = totalTransactionsCommitted.sum();
        stats.totalTransactionsAborted = totalTransactionsAborted.sum();

        stats.publishLatency = publishLatencyRecorder.getIntervalHistogram();
        stats.scheduleLatency = scheduleLatencyRecorder.getIntervalHistogram();
        stats.publishDelayLatency = publishDelayLatencyRecorder.getIntervalHistogram();
        stats.endToEndLatency = endToEndLatencyRecorder.getIntervalHistogram();
        stats.endToEndDeliveryLatency = endToEndDeliveryLatencyRecorder.getIntervalHistogram();
        stats.producerToFlinkLatency = producerToFlinkLatencyRecorder.getIntervalHistogram();
        stats.flinkToConsumerLatency = flinkToConsumerLatencyRecorder.getIntervalHistogram();
        stats.pollBatchSize = pollBatchSizeRecorder.getIntervalHistogram();
        stats.commitSyncLatency = commitSyncLatencyRecorder.getIntervalHistogram();
        stats.processingTime = processingTimeRecorder.getIntervalHistogram();

        if (perPartitionEnabled) {
            PerPartitionIntervals intervals = drainPerPartitionRecorders();
            stats.perPartitionPublishLatency = intervals.publish;
            stats.perPartitionE2eLatency = intervals.e2e;
            stats.perPartitionE2eDeliveryLatency = intervals.e2eDelivery;
        }

        return stats;
    }

    public CountersStats counters() {
        return new CountersStats(totalMessagesSent.sum(), totalMessagesReceived.sum());
    }

    public void probeReceived() {
        probesReceived.increment();
    }

    public long getProbesReceived() {
        return probesReceived.sum();
    }

    public void resetAll() {
        perPartitionEnabled = false;
        perConsumerEnabled = false;

        publishLatencyRecorder.reset();
        scheduleLatencyRecorder.reset();
        publishDelayLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndDeliveryLatencyRecorder.reset();
        producerToFlinkLatencyRecorder.reset();
        flinkToConsumerLatencyRecorder.reset();
        pollBatchSizeRecorder.reset();
        commitSyncLatencyRecorder.reset();
        processingTimeRecorder.reset();
        transactionCommitLatencyRecorder.reset();

        messagesSent.reset();
        bytesSent.reset();
        errors.reset();
        timeoutErrors.reset();
        pollErrors.reset();
        messagesReceived.reset();
        bytesReceived.reset();
        totalMessagesSent.reset();
        totalBytesSent.reset();
        totalErrors.reset();
        totalTimeoutErrors.reset();
        totalPollErrors.reset();
        totalMessagesReceived.reset();
        totalBytesReceived.reset();
        totalPolls.reset();
        totalCommitSync.reset();
        totalCommitAsync.reset();
        totalPartitionsAssigned.reset();
        totalPartitionsRevoked.reset();
        totalTransactionsCommitted.reset();
        totalTransactionsAborted.reset();

        clearPerPartitionState();
    }

    public void resetRecorders() {
        publishLatencyRecorder.reset();
        scheduleLatencyRecorder.reset();
        publishDelayLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndDeliveryLatencyRecorder.reset();
        producerToFlinkLatencyRecorder.reset();
        flinkToConsumerLatencyRecorder.reset();
        pollBatchSizeRecorder.reset();
        commitSyncLatencyRecorder.reset();
        processingTimeRecorder.reset();
        transactionCommitLatencyRecorder.reset();

        clearPerPartitionState();
    }

    public PerPartitionStats getPerPartitionCumulativeStats() {
        if (!perPartitionEnabled) return null;

        PerPartitionStats result = new PerPartitionStats();

        Set<String> allKeys = new TreeSet<>();
        allKeys.addAll(ppPublishLatencyCumulative.keySet());
        allKeys.addAll(ppE2eLatencyCumulative.keySet());
        allKeys.addAll(ppMessagesSent.keySet());
        allKeys.addAll(ppMessagesReceived.keySet());

        for (String key : allKeys) {
            var m = new PerPartitionStats.PartitionMetrics();
            m.key = key;

            LongAdder sent = ppMessagesSent.get(key);
            if (sent != null) m.messagesSent = sent.sum();

            LongAdder received = ppMessagesReceived.get(key);
            if (received != null) m.messagesReceived = received.sum();

            Histogram pubHist = ppPublishLatencyCumulative.get(key);
            if (pubHist != null) {
                m.publishLatencyBytes = serializeHistogram(pubHist);
            }

            Histogram e2eHist = ppE2eLatencyCumulative.get(key);
            if (e2eHist != null) {
                m.endToEndLatencyBytes = serializeHistogram(e2eHist);
            }

            result.partitions.put(key, m);
        }

        return result;
    }

    /**
     * Holder for per-partition interval histograms drained from Recorders.
     * Both maps are keyed by "topic:partition".
     */
    private record PerPartitionIntervals(Map<String, Histogram> publish,
                                         Map<String, Histogram> e2e,
                                         Map<String, Histogram> e2eDelivery) {}

    /**
     * Drain per-partition recorders into cumulative histograms and return
     * the publish, e2e, and delivery e2e interval histograms for this period.
     * The cumulative histograms continue to grow across periods and are read
     * at end-of-test by getPerPartitionCumulativeStats().
     */
    private PerPartitionIntervals drainPerPartitionRecorders() {
        Map<String, Histogram> publishIntervals = new TreeMap<>();
        for (var entry : ppPublishLatencyRecorders.entrySet()) {
            Histogram interval = entry.getValue().getIntervalHistogram();
            ppPublishLatencyCumulative
                    .computeIfAbsent(entry.getKey(), k -> new Histogram(NON_E2E_LATENCY_MAX_MICROS, PER_PARTITION_SIG_DIGITS))
                    .add(interval);
            publishIntervals.put(entry.getKey(), interval);
        }

        Map<String, Histogram> e2eIntervals = new TreeMap<>();
        for (var entry : ppE2eLatencyRecorders.entrySet()) {
            Histogram interval = entry.getValue().getIntervalHistogram();
            ppE2eLatencyCumulative
                    .computeIfAbsent(entry.getKey(), k -> new Histogram(E2E_LATENCY_MAX_MICROS, PER_PARTITION_SIG_DIGITS))
                    .add(interval);
            e2eIntervals.put(entry.getKey(), interval);
        }

        Map<String, Histogram> e2eDeliveryIntervals = new TreeMap<>();
        for (var entry : ppE2eDeliveryLatencyRecorders.entrySet()) {
            Histogram interval = entry.getValue().getIntervalHistogram();
            ppE2eDeliveryLatencyCumulative
                    .computeIfAbsent(entry.getKey(), k -> new Histogram(E2E_LATENCY_MAX_MICROS, PER_PARTITION_SIG_DIGITS))
                    .add(interval);
            e2eDeliveryIntervals.put(entry.getKey(), interval);
        }

        return new PerPartitionIntervals(publishIntervals, e2eIntervals, e2eDeliveryIntervals);
    }

    private void clearPerPartitionState() {
        ppPublishLatencyRecorders.clear();
        ppE2eLatencyRecorders.clear();
        ppE2eDeliveryLatencyRecorders.clear();
        ppPublishLatencyCumulative.clear();
        ppE2eLatencyCumulative.clear();
        ppE2eDeliveryLatencyCumulative.clear();
        ppMessagesSent.clear();
        ppMessagesReceived.clear();
        pcMessagesReceived.clear();
    }

    /**
     * Emit per-partition cumulative message counters in Prometheus text format.
     * Counters are intentionally cumulative (Prometheus counter convention).
     * Per-partition publish/e2e latency percentiles are emitted by
     * PrometheusEndpoint from the most recent period's interval histograms,
     * so they reflect recent behavior rather than test-lifetime aggregates.
     */
    public void writePerPartitionCounters(StringWriter writer, String hostname) {
        if (!perPartitionEnabled) return;

        String hostLabel = "host=\"" + hostname + "\"";

        for (var entry : ppMessagesSent.entrySet()) {
            writer.write("client_pp_messages_sent_count{" + hostLabel + ",partition=\"" + entry.getKey() + "\"} " + entry.getValue().sum() + "\n");
        }
        for (var entry : ppMessagesReceived.entrySet()) {
            writer.write("client_pp_messages_received_count{" + hostLabel + ",partition=\"" + entry.getKey() + "\"} " + entry.getValue().sum() + "\n");
        }
    }

    public void writePerConsumerMetrics(StringWriter writer, String hostname) {
        if (!perConsumerEnabled) return;

        String hostLabel = "host=\"" + hostname + "\"";
        for (var entry : pcMessagesReceived.entrySet()) {
            writer.write("client_pc_messages_received_count{" + hostLabel + ",consumer=\"" + entry.getKey() + "\"} " + entry.getValue().sum() + "\n");
        }
    }

    /** Emit aggregate classic-consumer rebalance counters for this worker. */
    public void writeConsumerRebalanceMetrics(StringWriter writer, String hostname) {
        String hostLabel = "host=\"" + hostname + "\"";
        writer.write("client_consumer_partitions_assigned_count{" + hostLabel + "} "
                + totalPartitionsAssigned.sum() + "\n");
        writer.write("client_consumer_partitions_revoked_count{" + hostLabel + "} "
                + totalPartitionsRevoked.sum() + "\n");
    }

    private static byte[] serializeHistogram(Histogram histogram) {
        ByteBuffer buffer = ByteBuffer.allocate(histogram.getEstimatedFootprintInBytes());
        histogram.encodeIntoCompressedByteBuffer(buffer);
        byte[] result = new byte[buffer.position()];
        buffer.flip();
        buffer.get(result);
        return result;
    }
}
