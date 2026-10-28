/*
 * Originally derived from io.openmessaging.benchmark.worker.commands.PeriodStats
 * in OpenMessaging Benchmark (https://github.com/openmessaging/benchmark, Apache 2.0)
 * and substantially modified for Dimster. The current implementation is largely
 * Dimster-original; see NOTICE at the repo root for context.
 */
package dev.dimster.worker.commands;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.DataFormatException;

import org.HdrHistogram.Histogram;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import static dev.dimster.stats.StatsCollector.E2E_LATENCY_MAX_MICROS;
import static dev.dimster.stats.StatsCollector.LATENCY_SIG_DIGITS;
import static dev.dimster.stats.StatsCollector.NON_E2E_LATENCY_MAX_MICROS;

public class    PeriodStats {
    public long messagesSent = 0;
    public long bytesSent = 0;
    public long errors = 0;
    public long timeoutErrors = 0;
    public long pollErrors = 0;

    public long messagesReceived = 0;
    public long bytesReceived = 0;

    public long totalMessagesSent = 0;
    public long totalBytesSent = 0;
    public long totalErrors = 0;
    public long totalTimeoutErrors = 0;
    public long totalPollErrors = 0;
    public long totalMessagesReceived = 0;
    public long totalBytesReceived = 0;

    public long totalPolls = 0;
    public long totalCommitSync = 0;
    public long totalCommitAsync = 0;

    // Producer-transactions counters (cumulative). Populated by
    // StatsCollector.snapshotAndReset; non-zero only when at least one
    // producer is configured with dimster.transactions=true.
    public long totalTransactionsCommitted = 0;
    public long totalTransactionsAborted = 0;

    @JsonIgnore
    public Histogram publishLatency = new Histogram(NON_E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    public byte[] publishLatencyBytes;

    @JsonIgnore
    public Histogram publishDelayLatency = new Histogram(NON_E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    public byte[] publishDelayLatencyBytes;

    @JsonIgnore
    public Histogram endToEndLatency = new Histogram(E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    public byte[] endToEndLatencyBytes;

    // Delivery latency: publish timestamp → poll return time (before per-record processing)
    @JsonIgnore
    public Histogram endToEndDeliveryLatency = new Histogram(E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    public byte[] endToEndDeliveryLatencyBytes;

    // End-to-end latency split at the Flink job: producer → Flink emit and
    // Flink emit → consumer. Empty without a Flink stage.
    @JsonIgnore
    public Histogram producerToFlinkLatency = new Histogram(E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    public byte[] producerToFlinkLatencyBytes;

    @JsonIgnore
    public Histogram flinkToConsumerLatency = new Histogram(E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    public byte[] flinkToConsumerLatencyBytes;

    @JsonIgnore
    public Histogram pollBatchSize = new Histogram(100_000, 3);
    public byte[] pollBatchSizeBytes;

    @JsonIgnore
    public Histogram commitSyncLatency = new Histogram(NON_E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    public byte[] commitSyncLatencyBytes;

    @JsonIgnore
    public Histogram scheduleLatency = new Histogram(NON_E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    public byte[] scheduleLatencyBytes;

    @JsonIgnore
    public Histogram processingTime = new Histogram(NON_E2E_LATENCY_MAX_MICROS, 3);

    // Per-partition publish/e2e interval histograms. Always populated by
    // StatsCollector.snapshotAndReset when per-partition metrics are enabled
    // — the cost is just Map references to existing Histograms, so it's safe
    // to attach unconditionally. The PrometheusEndpoint reads these in-process
    // each scrape window; the explore engine reads them for worst-partition
    // detection. Wire serialization (perPartitionE2eLatencyBytes) is gated
    // separately at the WorkerHttpServer to avoid expensive HTTP payloads.
    @JsonIgnore
    public Map<String, Histogram> perPartitionPublishLatency;

    @JsonIgnore
    public Map<String, Histogram> perPartitionE2eLatency;

    @JsonIgnore
    public Map<String, Histogram> perPartitionE2eDeliveryLatency;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Map<String, byte[]> perPartitionE2eLatencyBytes;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Map<String, byte[]> perPartitionE2eDeliveryLatencyBytes;

    public int producerCount = 0;
    public int consumerCount = 0;
    public long consumerRecreations = 0;

    // Merges only period-specific data (counters that reset each period and
    // interval histograms) from source into this instance. Cumulative counters
    // and state fields are NOT touched — callers handle those differently
    // depending on whether they're aggregating across workers (sum) or across
    // time for the same worker (replace with latest).
    public void mergePeriodDataFrom(PeriodStats source) {
        messagesSent += source.messagesSent;
        bytesSent += source.bytesSent;
        errors += source.errors;
        timeoutErrors += source.timeoutErrors;
        pollErrors += source.pollErrors;
        messagesReceived += source.messagesReceived;
        bytesReceived += source.bytesReceived;

        publishLatency.add(source.publishLatency);
        publishDelayLatency.add(source.publishDelayLatency);
        endToEndLatency.add(source.endToEndLatency);
        endToEndDeliveryLatency.add(source.endToEndDeliveryLatency);
        producerToFlinkLatency.add(source.producerToFlinkLatency);
        flinkToConsumerLatency.add(source.flinkToConsumerLatency);
        scheduleLatency.add(source.scheduleLatency);
        pollBatchSize.add(source.pollBatchSize);
        commitSyncLatency.add(source.commitSyncLatency);
        processingTime.add(source.processingTime);

        this.perPartitionPublishLatency = mergePerPartitionMap(
                this.perPartitionPublishLatency, source.perPartitionPublishLatency);
        this.perPartitionE2eLatency = mergePerPartitionMap(
                this.perPartitionE2eLatency, source.perPartitionE2eLatency);
        this.perPartitionE2eDeliveryLatency = mergePerPartitionMap(
                this.perPartitionE2eDeliveryLatency, source.perPartitionE2eDeliveryLatency);
    }

    // Full cross-worker merge: period data plus cumulative counters and state.
    // Used by RemoteWorkerClient to aggregate stats from all workers.
    public void mergeFrom(PeriodStats source) {
        mergePeriodDataFrom(source);

        totalMessagesSent += source.totalMessagesSent;
        totalBytesSent += source.totalBytesSent;
        totalErrors += source.totalErrors;
        totalTimeoutErrors += source.totalTimeoutErrors;
        totalPollErrors += source.totalPollErrors;
        totalMessagesReceived += source.totalMessagesReceived;
        totalBytesReceived += source.totalBytesReceived;
        totalPolls += source.totalPolls;
        totalCommitSync += source.totalCommitSync;
        totalCommitAsync += source.totalCommitAsync;
        totalTransactionsCommitted += source.totalTransactionsCommitted;
        totalTransactionsAborted += source.totalTransactionsAborted;

        producerCount += source.producerCount;
        consumerCount += source.consumerCount;
        consumerRecreations += source.consumerRecreations;
    }

    // Replaces cumulative counters and state fields with the latest values
    // from source. Used by PrometheusEndpoint to track the same worker's
    // monotonically-increasing totals without summing across time periods.
    public void setCumulativeAndStateFrom(PeriodStats source) {
        totalMessagesSent = source.totalMessagesSent;
        totalBytesSent = source.totalBytesSent;
        totalErrors = source.totalErrors;
        totalTimeoutErrors = source.totalTimeoutErrors;
        totalPollErrors = source.totalPollErrors;
        totalMessagesReceived = source.totalMessagesReceived;
        totalBytesReceived = source.totalBytesReceived;
        totalPolls = source.totalPolls;
        totalCommitSync = source.totalCommitSync;
        totalCommitAsync = source.totalCommitAsync;
        totalTransactionsCommitted = source.totalTransactionsCommitted;
        totalTransactionsAborted = source.totalTransactionsAborted;

        producerCount = source.producerCount;
        consumerCount = source.consumerCount;
        consumerRecreations = source.consumerRecreations;
    }

    // Resets period-specific data (counters and histograms) to zero.
    // Called by PrometheusEndpoint after a scrape so the next scrape window
    // starts fresh, while cumulative counters remain unchanged.
    public void resetPeriodData() {
        messagesSent = 0;
        bytesSent = 0;
        errors = 0;
        timeoutErrors = 0;
        pollErrors = 0;
        messagesReceived = 0;
        bytesReceived = 0;

        publishLatency.reset();
        publishDelayLatency.reset();
        endToEndLatency.reset();
        endToEndDeliveryLatency.reset();
        producerToFlinkLatency.reset();
        flinkToConsumerLatency.reset();
        scheduleLatency.reset();
        pollBatchSize.reset();
        commitSyncLatency.reset();
        processingTime.reset();

        perPartitionPublishLatency = null;
        perPartitionE2eLatency = null;
    }

    // Populates in-memory histogram fields from wire-serialized byte arrays.
    // Only decodes the histograms that WorkerHttpServer actually serializes
    // (publishLatency, scheduleLatency, publishDelayLatency, the end-to-end
    // latencies, and per-partition e2e). pollBatchSize, commitSyncLatency, and
    // processingTime are never serialized and stay as default-empty histograms.
    public void decodeHistograms() {
        try {
            if (publishLatencyBytes != null) {
                publishLatency = Histogram.decodeFromCompressedByteBuffer(
                        ByteBuffer.wrap(publishLatencyBytes), NON_E2E_LATENCY_MAX_MICROS);
            }
            if (scheduleLatencyBytes != null) {
                scheduleLatency = Histogram.decodeFromCompressedByteBuffer(
                        ByteBuffer.wrap(scheduleLatencyBytes), NON_E2E_LATENCY_MAX_MICROS);
            }
            if (publishDelayLatencyBytes != null) {
                publishDelayLatency = Histogram.decodeFromCompressedByteBuffer(
                        ByteBuffer.wrap(publishDelayLatencyBytes), NON_E2E_LATENCY_MAX_MICROS);
            }
            if (endToEndLatencyBytes != null) {
                endToEndLatency = Histogram.decodeFromCompressedByteBuffer(
                        ByteBuffer.wrap(endToEndLatencyBytes), E2E_LATENCY_MAX_MICROS);
            }
            if (endToEndDeliveryLatencyBytes != null) {
                endToEndDeliveryLatency = Histogram.decodeFromCompressedByteBuffer(
                        ByteBuffer.wrap(endToEndDeliveryLatencyBytes), E2E_LATENCY_MAX_MICROS);
            }
            if (producerToFlinkLatencyBytes != null) {
                producerToFlinkLatency = Histogram.decodeFromCompressedByteBuffer(
                        ByteBuffer.wrap(producerToFlinkLatencyBytes), E2E_LATENCY_MAX_MICROS);
            }
            if (flinkToConsumerLatencyBytes != null) {
                flinkToConsumerLatency = Histogram.decodeFromCompressedByteBuffer(
                        ByteBuffer.wrap(flinkToConsumerLatencyBytes), E2E_LATENCY_MAX_MICROS);
            }
            if (perPartitionE2eLatencyBytes != null) {
                perPartitionE2eLatency = new TreeMap<>();
                for (var entry : perPartitionE2eLatencyBytes.entrySet()) {
                    perPartitionE2eLatency.put(entry.getKey(),
                            Histogram.decodeFromCompressedByteBuffer(
                                    ByteBuffer.wrap(entry.getValue()), E2E_LATENCY_MAX_MICROS));
                }
            }
            if (perPartitionE2eDeliveryLatencyBytes != null) {
                perPartitionE2eDeliveryLatency = new TreeMap<>();
                for (var entry : perPartitionE2eDeliveryLatencyBytes.entrySet()) {
                    perPartitionE2eDeliveryLatency.put(entry.getKey(),
                            Histogram.decodeFromCompressedByteBuffer(
                                    ByteBuffer.wrap(entry.getValue()), E2E_LATENCY_MAX_MICROS));
                }
            }
        } catch (DataFormatException e) {
            throw new RuntimeException("Failed to decode histogram from wire bytes", e);
        }
    }

    // Merges a source per-partition histogram map into a target map.
    // Creates new Histogram copies for first-seen partitions to avoid
    // sharing references with the source. Returns the (possibly newly
    // created) target map.
    private static Map<String, Histogram> mergePerPartitionMap(
            Map<String, Histogram> targetMap,
            Map<String, Histogram> sourceMap) {
        if (sourceMap == null) return targetMap;

        if (targetMap == null) {
            targetMap = new TreeMap<>();
        }

        for (var entry : sourceMap.entrySet()) {
            Histogram sourceHist = entry.getValue();
            targetMap.compute(entry.getKey(), (key, existing) -> {
                if (existing == null) {
                    Histogram copy = new Histogram(
                            sourceHist.getHighestTrackableValue(),
                            sourceHist.getNumberOfSignificantValueDigits());
                    copy.add(sourceHist);
                    return copy;
                }
                existing.add(sourceHist);
                return existing;
            });
        }
        return targetMap;
    }
}
