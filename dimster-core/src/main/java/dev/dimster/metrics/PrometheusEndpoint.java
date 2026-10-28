package dev.dimster.metrics;

import com.sun.net.httpserver.HttpServer;
import dev.dimster.kafka.KafkaMetricsSource;
import dev.dimster.stats.StatsCollector;
import dev.dimster.utils.HostUtil;
import dev.dimster.utils.PaddingDecimalFormat;
import dev.dimster.worker.commands.PeriodStats;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PrometheusEndpoint {

    String hostname;
    PeriodStats periodStats;
    Lock lock;
    private final StatsCollector statsCollector;
    private volatile ShareGroupLagMonitor lagMonitor;
    private volatile List<KafkaMetricsSource> kafkaMetricsSources = List.of();
    private static final DecimalFormat rateFormat = new PaddingDecimalFormat("0.000", 7);
    private static final DecimalFormat dec = new PaddingDecimalFormat("0.000", 4);

    public PrometheusEndpoint(StatsCollector statsCollector) {
        this.hostname = HostUtil.hostname();
        this.lock = new ReentrantLock();
        this.periodStats = new PeriodStats();
        this.statsCollector = statsCollector;
    }

    public void setLagMonitor(ShareGroupLagMonitor monitor) {
        this.lagMonitor = monitor;
    }

    /**
     * Sets the list of Kafka client metrics sources (producers and consumers).
     * An immutable copy is stored so that concurrent stopAll() cannot interfere
     * with an in-flight /metrics scrape.
     */
    public void setKafkaMetricsSources(List<KafkaMetricsSource> sources) {
        this.kafkaMetricsSources = sources == null ? List.of() : List.copyOf(sources);
    }

    public void reset() {
        lock.lock();
        try {
            this.periodStats = new PeriodStats();
        } finally {
            lock.unlock();
        }
    }

    // Merges incoming period data (histograms, period counters) into the
    // cached copy so nothing is lost between Prometheus scrapes, then
    // replaces cumulative counters with the latest values from the worker.
    public void updateStats(PeriodStats incoming) {
        lock.lock();
        try {
            this.periodStats.mergePeriodDataFrom(incoming);
            this.periodStats.setCumulativeAndStateFrom(incoming);
        } finally {
            lock.unlock();
        }
    }

    public void run() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(9090), 0);
            server.createContext("/metrics", httpExchange -> {
                StringWriter writer = new StringWriter();
                lock.lock();
                try {
                    writer.write("client_messages_received_count{host=\"" + hostname + "\"} " + periodStats.totalMessagesReceived + "\n");
                    writer.write("client_messages_sent_count{host=\"" + hostname + "\"} " + periodStats.totalMessagesSent + "\n");
                    writer.write("client_bytes_sent_count{host=\"" + hostname + "\"} " + periodStats.totalBytesSent + "\n");
                    writer.write("client_bytes_received_count{host=\"" + hostname + "\"} " + periodStats.totalBytesReceived + "\n");
                    writer.write("client_producer_errors_count{host=\"" + hostname + "\"} " + periodStats.totalErrors + "\n");
                    writer.write("client_producer_timeout_errors_count{host=\"" + hostname + "\"} " + periodStats.totalTimeoutErrors + "\n");
                    writer.write("client_consumer_errors_count{host=\"" + hostname + "\"} " + periodStats.totalPollErrors + "\n");
                    writer.write("client_consumer_polls_count{host=\"" + hostname + "\"} " + periodStats.totalPolls + "\n");
                    writer.write("client_consumer_commit_sync_count{host=\"" + hostname + "\"} " + periodStats.totalCommitSync + "\n");
                    writer.write("client_consumer_commit_async_count{host=\"" + hostname + "\"} " + periodStats.totalCommitAsync + "\n");
                    statsCollector.writeConsumerRebalanceMetrics(writer, hostname);
                    writer.write("client_producer_transactions_committed_count{host=\"" + hostname + "\"} " + periodStats.totalTransactionsCommitted + "\n");
                    writer.write("client_producer_transactions_aborted_count{host=\"" + hostname + "\"} " + periodStats.totalTransactionsAborted + "\n");

                    // Commit latency is read live from the StatsCollector recorder
                    // (not threaded through PeriodStats — see StatsCollector field comment).
                    var txnCommitHist = statsCollector.getTransactionCommitLatencyIntervalHistogram();
                    writer.write("client_producer_transaction_commit_latency{host=\"" + hostname + "\",percentile=\"50\"} " + dec.format(microsToMillis(txnCommitHist.getValueAtPercentile(50))) + "\n");
                    writer.write("client_producer_transaction_commit_latency{host=\"" + hostname + "\",percentile=\"95\"} " + dec.format(microsToMillis(txnCommitHist.getValueAtPercentile(95))) + "\n");
                    writer.write("client_producer_transaction_commit_latency{host=\"" + hostname + "\",percentile=\"99\"} " + dec.format(microsToMillis(txnCommitHist.getValueAtPercentile(99))) + "\n");
                    writer.write("client_producer_transaction_commit_latency{host=\"" + hostname + "\",percentile=\"99.9\"} " + dec.format(microsToMillis(txnCommitHist.getValueAtPercentile(99.9))) + "\n");
                    writer.write("client_producer_transaction_commit_latency{host=\"" + hostname + "\",percentile=\"100\"} " + dec.format(microsToMillis(txnCommitHist.getValueAtPercentile(100))) + "\n");
                    writer.write("client_e2e_latency{host=\"" + hostname + "\",percentile=\"50\"} " + dec.format(microsToMillis(periodStats.endToEndLatency.getValueAtPercentile(50))) + "\n");
                    writer.write("client_e2e_latency{host=\"" + hostname + "\",percentile=\"75\"} " + dec.format(microsToMillis(periodStats.endToEndLatency.getValueAtPercentile(75))) + "\n");
                    writer.write("client_e2e_latency{host=\"" + hostname + "\",percentile=\"90\"} " + dec.format(microsToMillis(periodStats.endToEndLatency.getValueAtPercentile(90))) + "\n");
                    writer.write("client_e2e_latency{host=\"" + hostname + "\",percentile=\"95\"} " + dec.format(microsToMillis(periodStats.endToEndLatency.getValueAtPercentile(95))) + "\n");
                    writer.write("client_e2e_latency{host=\"" + hostname + "\",percentile=\"99\"} " + dec.format(microsToMillis(periodStats.endToEndLatency.getValueAtPercentile(99))) + "\n");
                    writer.write("client_e2e_latency{host=\"" + hostname + "\",percentile=\"99.9\"} " + dec.format(microsToMillis(periodStats.endToEndLatency.getValueAtPercentile(99.9))) + "\n");
                    writer.write("client_e2e_latency{host=\"" + hostname + "\",percentile=\"99.99\"} " + dec.format(microsToMillis(periodStats.endToEndLatency.getValueAtPercentile(99.99))) + "\n");
                    writer.write("client_e2e_latency{host=\"" + hostname + "\",percentile=\"99.999\"} " + dec.format(microsToMillis(periodStats.endToEndLatency.getValueAtPercentile(99.999))) + "\n");
                    writer.write("client_e2e_latency{host=\"" + hostname + "\",percentile=\"100\"} " + dec.format(microsToMillis(periodStats.endToEndLatency.getValueAtPercentile(100))) + "\n");

                    writer.write("client_e2e_delivery_latency{host=\"" + hostname + "\",percentile=\"50\"} " + dec.format(microsToMillis(periodStats.endToEndDeliveryLatency.getValueAtPercentile(50))) + "\n");
                    writer.write("client_e2e_delivery_latency{host=\"" + hostname + "\",percentile=\"75\"} " + dec.format(microsToMillis(periodStats.endToEndDeliveryLatency.getValueAtPercentile(75))) + "\n");
                    writer.write("client_e2e_delivery_latency{host=\"" + hostname + "\",percentile=\"90\"} " + dec.format(microsToMillis(periodStats.endToEndDeliveryLatency.getValueAtPercentile(90))) + "\n");
                    writer.write("client_e2e_delivery_latency{host=\"" + hostname + "\",percentile=\"95\"} " + dec.format(microsToMillis(periodStats.endToEndDeliveryLatency.getValueAtPercentile(95))) + "\n");
                    writer.write("client_e2e_delivery_latency{host=\"" + hostname + "\",percentile=\"99\"} " + dec.format(microsToMillis(periodStats.endToEndDeliveryLatency.getValueAtPercentile(99))) + "\n");
                    writer.write("client_e2e_delivery_latency{host=\"" + hostname + "\",percentile=\"99.9\"} " + dec.format(microsToMillis(periodStats.endToEndDeliveryLatency.getValueAtPercentile(99.9))) + "\n");
                    writer.write("client_e2e_delivery_latency{host=\"" + hostname + "\",percentile=\"99.99\"} " + dec.format(microsToMillis(periodStats.endToEndDeliveryLatency.getValueAtPercentile(99.99))) + "\n");
                    writer.write("client_e2e_delivery_latency{host=\"" + hostname + "\",percentile=\"99.999\"} " + dec.format(microsToMillis(periodStats.endToEndDeliveryLatency.getValueAtPercentile(99.999))) + "\n");
                    writer.write("client_e2e_delivery_latency{host=\"" + hostname + "\",percentile=\"100\"} " + dec.format(microsToMillis(periodStats.endToEndDeliveryLatency.getValueAtPercentile(100))) + "\n");

                    // The e2e latency split at the Flink job: only with a Flink stage.
                    writeLatencyPercentiles(writer, "client_producer_to_flink_latency", periodStats.producerToFlinkLatency);
                    writeLatencyPercentiles(writer, "client_flink_to_consumer_latency", periodStats.flinkToConsumerLatency);

                    writer.write("client_publish_latency{host=\"" + hostname + "\",percentile=\"50\"} " + dec.format(microsToMillis(periodStats.publishLatency.getValueAtPercentile(50))) + "\n");
                    writer.write("client_publish_latency{host=\"" + hostname + "\",percentile=\"75\"} " + dec.format(microsToMillis(periodStats.publishLatency.getValueAtPercentile(75))) + "\n");
                    writer.write("client_publish_latency{host=\"" + hostname + "\",percentile=\"90\"} " + dec.format(microsToMillis(periodStats.publishLatency.getValueAtPercentile(90))) + "\n");
                    writer.write("client_publish_latency{host=\"" + hostname + "\",percentile=\"95\"} " + dec.format(microsToMillis(periodStats.publishLatency.getValueAtPercentile(95))) + "\n");
                    writer.write("client_publish_latency{host=\"" + hostname + "\",percentile=\"99\"} " + dec.format(microsToMillis(periodStats.publishLatency.getValueAtPercentile(99))) + "\n");
                    writer.write("client_publish_latency{host=\"" + hostname + "\",percentile=\"99.9\"} " + dec.format(microsToMillis(periodStats.publishLatency.getValueAtPercentile(99.9))) + "\n");
                    writer.write("client_publish_latency{host=\"" + hostname + "\",percentile=\"99.99\"} " + dec.format(microsToMillis(periodStats.publishLatency.getValueAtPercentile(99.99))) + "\n");
                    writer.write("client_publish_latency{host=\"" + hostname + "\",percentile=\"99.999\"} " + dec.format(microsToMillis(periodStats.publishLatency.getValueAtPercentile(99.999))) + "\n");
                    writer.write("client_publish_latency{host=\"" + hostname + "\",percentile=\"100\"} " + dec.format(microsToMillis(periodStats.publishLatency.getValueAtPercentile(100))) + "\n");

                    writer.write("client_publish_delay_latency{host=\"" + hostname + "\",percentile=\"50\"} " + dec.format(microsToMillis(periodStats.publishDelayLatency.getValueAtPercentile(50))) + "\n");
                    writer.write("client_publish_delay_latency{host=\"" + hostname + "\",percentile=\"75\"} " + dec.format(microsToMillis(periodStats.publishDelayLatency.getValueAtPercentile(75))) + "\n");
                    writer.write("client_publish_delay_latency{host=\"" + hostname + "\",percentile=\"90\"} " + dec.format(microsToMillis(periodStats.publishDelayLatency.getValueAtPercentile(90))) + "\n");
                    writer.write("client_publish_delay_latency{host=\"" + hostname + "\",percentile=\"95\"} " + dec.format(microsToMillis(periodStats.publishDelayLatency.getValueAtPercentile(95))) + "\n");
                    writer.write("client_publish_delay_latency{host=\"" + hostname + "\",percentile=\"99\"} " + dec.format(microsToMillis(periodStats.publishDelayLatency.getValueAtPercentile(99))) + "\n");
                    writer.write("client_publish_delay_latency{host=\"" + hostname + "\",percentile=\"99.9\"} " + dec.format(microsToMillis(periodStats.publishDelayLatency.getValueAtPercentile(99.9))) + "\n");
                    writer.write("client_publish_delay_latency{host=\"" + hostname + "\",percentile=\"99.99\"} " + dec.format(microsToMillis(periodStats.publishDelayLatency.getValueAtPercentile(99.99))) + "\n");
                    writer.write("client_publish_delay_latency{host=\"" + hostname + "\",percentile=\"99.999\"} " + dec.format(microsToMillis(periodStats.publishDelayLatency.getValueAtPercentile(99.999))) + "\n");
                    writer.write("client_publish_delay_latency{host=\"" + hostname + "\",percentile=\"100\"} " + dec.format(microsToMillis(periodStats.publishDelayLatency.getValueAtPercentile(100))) + "\n");

                    writer.write("client_schedule_latency{host=\"" + hostname + "\",percentile=\"50\"} " + dec.format(microsToMillis(periodStats.scheduleLatency.getValueAtPercentile(50))) + "\n");
                    writer.write("client_schedule_latency{host=\"" + hostname + "\",percentile=\"75\"} " + dec.format(microsToMillis(periodStats.scheduleLatency.getValueAtPercentile(75))) + "\n");
                    writer.write("client_schedule_latency{host=\"" + hostname + "\",percentile=\"90\"} " + dec.format(microsToMillis(periodStats.scheduleLatency.getValueAtPercentile(90))) + "\n");
                    writer.write("client_schedule_latency{host=\"" + hostname + "\",percentile=\"95\"} " + dec.format(microsToMillis(periodStats.scheduleLatency.getValueAtPercentile(95))) + "\n");
                    writer.write("client_schedule_latency{host=\"" + hostname + "\",percentile=\"99\"} " + dec.format(microsToMillis(periodStats.scheduleLatency.getValueAtPercentile(99))) + "\n");
                    writer.write("client_schedule_latency{host=\"" + hostname + "\",percentile=\"99.9\"} " + dec.format(microsToMillis(periodStats.scheduleLatency.getValueAtPercentile(99.9))) + "\n");
                    writer.write("client_schedule_latency{host=\"" + hostname + "\",percentile=\"99.99\"} " + dec.format(microsToMillis(periodStats.scheduleLatency.getValueAtPercentile(99.99))) + "\n");
                    writer.write("client_schedule_latency{host=\"" + hostname + "\",percentile=\"99.999\"} " + dec.format(microsToMillis(periodStats.scheduleLatency.getValueAtPercentile(99.999))) + "\n");
                    writer.write("client_schedule_latency{host=\"" + hostname + "\",percentile=\"100\"} " + dec.format(microsToMillis(periodStats.scheduleLatency.getValueAtPercentile(100))) + "\n");

                    writer.write("client_poll_batch_size{host=\"" + hostname + "\",percentile=\"50\"} " + periodStats.pollBatchSize.getValueAtPercentile(50) + "\n");
                    writer.write("client_poll_batch_size{host=\"" + hostname + "\",percentile=\"75\"} " + periodStats.pollBatchSize.getValueAtPercentile(75) + "\n");
                    writer.write("client_poll_batch_size{host=\"" + hostname + "\",percentile=\"99\"} " + periodStats.pollBatchSize.getValueAtPercentile(99) + "\n");
                    writer.write("client_poll_batch_size{host=\"" + hostname + "\",percentile=\"100\"} " + periodStats.pollBatchSize.getValueAtPercentile(100) + "\n");

                    writer.write("client_commit_sync_latency{host=\"" + hostname + "\",percentile=\"50\"} " + dec.format(microsToMillis(periodStats.commitSyncLatency.getValueAtPercentile(50))) + "\n");
                    writer.write("client_commit_sync_latency{host=\"" + hostname + "\",percentile=\"75\"} " + dec.format(microsToMillis(periodStats.commitSyncLatency.getValueAtPercentile(75))) + "\n");
                    writer.write("client_commit_sync_latency{host=\"" + hostname + "\",percentile=\"99\"} " + dec.format(microsToMillis(periodStats.commitSyncLatency.getValueAtPercentile(99))) + "\n");
                    writer.write("client_commit_sync_latency{host=\"" + hostname + "\",percentile=\"99.9\"} " + dec.format(microsToMillis(periodStats.commitSyncLatency.getValueAtPercentile(99.9))) + "\n");
                    writer.write("client_commit_sync_latency{host=\"" + hostname + "\",percentile=\"100\"} " + dec.format(microsToMillis(periodStats.commitSyncLatency.getValueAtPercentile(100))) + "\n");

                    writer.write("client_processing_time{host=\"" + hostname + "\",percentile=\"50\"} " + dec.format(microsToMillis(periodStats.processingTime.getValueAtPercentile(50))) + "\n");
                    writer.write("client_processing_time{host=\"" + hostname + "\",percentile=\"75\"} " + dec.format(microsToMillis(periodStats.processingTime.getValueAtPercentile(75))) + "\n");
                    writer.write("client_processing_time{host=\"" + hostname + "\",percentile=\"90\"} " + dec.format(microsToMillis(periodStats.processingTime.getValueAtPercentile(90))) + "\n");
                    writer.write("client_processing_time{host=\"" + hostname + "\",percentile=\"95\"} " + dec.format(microsToMillis(periodStats.processingTime.getValueAtPercentile(95))) + "\n");
                    writer.write("client_processing_time{host=\"" + hostname + "\",percentile=\"99\"} " + dec.format(microsToMillis(periodStats.processingTime.getValueAtPercentile(99))) + "\n");
                    writer.write("client_processing_time{host=\"" + hostname + "\",percentile=\"99.9\"} " + dec.format(microsToMillis(periodStats.processingTime.getValueAtPercentile(99.9))) + "\n");
                    writer.write("client_processing_time{host=\"" + hostname + "\",percentile=\"99.99\"} " + dec.format(microsToMillis(periodStats.processingTime.getValueAtPercentile(99.99))) + "\n");
                    writer.write("client_processing_time{host=\"" + hostname + "\",percentile=\"99.999\"} " + dec.format(microsToMillis(periodStats.processingTime.getValueAtPercentile(99.999))) + "\n");
                    writer.write("client_processing_time{host=\"" + hostname + "\",percentile=\"100\"} " + dec.format(microsToMillis(periodStats.processingTime.getValueAtPercentile(100))) + "\n");

                    writer.write("client_producer_count{host=\"" + hostname + "\"} " + periodStats.producerCount + "\n");
                    writer.write("client_consumer_count{host=\"" + hostname + "\"} " + periodStats.consumerCount + "\n");
                    writer.write("client_consumer_recreations_count{host=\"" + hostname + "\"} " + periodStats.consumerRecreations + "\n");

                    statsCollector.writePerPartitionCounters(writer, hostname);
                    writePerPartitionLatencies(writer, hostname);
                    statsCollector.writePerConsumerMetrics(writer, hostname);

                    var monitor = lagMonitor;
                    if (monitor != null) {
                        monitor.writeLagMetrics(writer, hostname);
                    }

                    // Reset period-specific data (histograms, period counters)
                    // so the next scrape window starts fresh. Cumulative counters
                    // are untouched — they're monotonically increasing.
                    this.periodStats.resetPeriodData();
                } finally {
                    lock.unlock();
                }

                // Kafka client metrics are read outside the lock because they
                // query the client's live metrics map (thread-safe) and don't
                // depend on PeriodStats.
                writeKafkaClientMetrics(writer, hostname);

                String response = writer.toString();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }

            });

            Thread t = new Thread(server::start);
            t.setDaemon(true);
            t.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Emits all numeric Kafka client metrics in Prometheus text format.
     * The Prometheus metric name is derived from the Kafka metric group and name
     * by replacing hyphens with underscores and prefixing with "kafka_", e.g.
     * group="producer-metrics" name="record-queue-time-avg" becomes
     * "kafka_producer_metrics_record_queue_time_avg".
     *
     * Each metric carries host, client, and group labels. Any additional Kafka
     * tags (client-id, node-id, topic, partition) are included as labels so
     * Prometheus queries can filter or aggregate as needed.
     */
    private void writeKafkaClientMetrics(StringWriter writer, String host) {
        var sources = kafkaMetricsSources;
        for (var source : sources) {
            Map<MetricName, ? extends Metric> metrics;
            try {
                metrics = source.kafkaMetrics();
            } catch (Exception e) {
                // Client may have been closed between the snapshot and the scrape
                continue;
            }

            String client = source.clientName();
            for (var entry : metrics.entrySet()) {
                MetricName mn = entry.getKey();

                Object value = entry.getValue().metricValue();
                if (!(value instanceof Number n)) continue;
                double d = n.doubleValue();
                if (Double.isNaN(d) || Double.isInfinite(d)) continue;

                // Build Prometheus metric name: kafka_{group}_{name} with hyphens → underscores
                String prometheusName = "kafka_"
                        + mn.group().replace('-', '_')
                        + "_" + mn.name().replace('-', '_');

                // Build labels: always include host, client, group; add any Kafka tags
                var sb = new StringBuilder();
                sb.append(prometheusName);
                sb.append("{host=\"").append(host);
                sb.append("\",client=\"").append(client);
                sb.append("\",group=\"").append(mn.group()).append('"');
                for (var tag : mn.tags().entrySet()) {
                    sb.append(",").append(tag.getKey().replace('-', '_'));
                    sb.append("=\"").append(tag.getValue()).append('"');
                }
                sb.append("} ").append(dec.format(d)).append('\n');
                writer.write(sb.toString());
            }
        }
    }

    /**
     * Emit per-partition publish/e2e latency percentiles as Prometheus gauges.
     * Reads from the most recent period's interval histograms (attached to the
     * cached PeriodStats by LocalWorker.getPeriodStats), so values reflect
     * recent behavior rather than test-lifetime aggregates. Partitions with
     * no samples in the current interval are skipped to keep the output small.
     *
     * Caller must hold {@link #lock} (we read the periodStats reference inside
     * the same critical section as the rest of /metrics).
     */
    private void writePerPartitionLatencies(StringWriter writer, String host) {
        String hostLabel = "host=\"" + host + "\"";

        Map<String, org.HdrHistogram.Histogram> publishIntervals = periodStats.perPartitionPublishLatency;
        if (publishIntervals != null) {
            for (var entry : publishIntervals.entrySet()) {
                var h = entry.getValue();
                if (h.getTotalCount() == 0) continue;
                String labels = hostLabel + ",partition=\"" + entry.getKey() + "\"";
                writer.write("client_pp_publish_latency{" + labels + ",percentile=\"50\"} " + dec.format(microsToMillis(h.getValueAtPercentile(50))) + "\n");
                writer.write("client_pp_publish_latency{" + labels + ",percentile=\"99\"} " + dec.format(microsToMillis(h.getValueAtPercentile(99))) + "\n");
                writer.write("client_pp_publish_latency{" + labels + ",percentile=\"99.9\"} " + dec.format(microsToMillis(h.getValueAtPercentile(99.9))) + "\n");
                writer.write("client_pp_publish_latency{" + labels + ",percentile=\"100\"} " + dec.format(microsToMillis(h.getValueAtPercentile(100))) + "\n");
            }
        }

        Map<String, org.HdrHistogram.Histogram> e2eIntervals = periodStats.perPartitionE2eLatency;
        if (e2eIntervals != null) {
            for (var entry : e2eIntervals.entrySet()) {
                var h = entry.getValue();
                if (h.getTotalCount() == 0) continue;
                String labels = hostLabel + ",partition=\"" + entry.getKey() + "\"";
                writer.write("client_pp_e2e_latency{" + labels + ",percentile=\"50\"} " + dec.format(microsToMillis(h.getValueAtPercentile(50))) + "\n");
                writer.write("client_pp_e2e_latency{" + labels + ",percentile=\"99\"} " + dec.format(microsToMillis(h.getValueAtPercentile(99))) + "\n");
                writer.write("client_pp_e2e_latency{" + labels + ",percentile=\"99.9\"} " + dec.format(microsToMillis(h.getValueAtPercentile(99.9))) + "\n");
                writer.write("client_pp_e2e_latency{" + labels + ",percentile=\"100\"} " + dec.format(microsToMillis(h.getValueAtPercentile(100))) + "\n");
            }
        }

        Map<String, org.HdrHistogram.Histogram> e2eDeliveryIntervals = periodStats.perPartitionE2eDeliveryLatency;
        if (e2eDeliveryIntervals != null) {
            for (var entry : e2eDeliveryIntervals.entrySet()) {
                var h = entry.getValue();
                if (h.getTotalCount() == 0) continue;
                String labels = hostLabel + ",partition=\"" + entry.getKey() + "\"";
                writer.write("client_pp_e2e_delivery_latency{" + labels + ",percentile=\"50\"} " + dec.format(microsToMillis(h.getValueAtPercentile(50))) + "\n");
                writer.write("client_pp_e2e_delivery_latency{" + labels + ",percentile=\"99\"} " + dec.format(microsToMillis(h.getValueAtPercentile(99))) + "\n");
                writer.write("client_pp_e2e_delivery_latency{" + labels + ",percentile=\"99.9\"} " + dec.format(microsToMillis(h.getValueAtPercentile(99.9))) + "\n");
                writer.write("client_pp_e2e_delivery_latency{" + labels + ",percentile=\"100\"} " + dec.format(microsToMillis(h.getValueAtPercentile(100))) + "\n");
            }
        }
    }

    /** Writes a latency gauge's percentiles in ms; nothing when the period recorded none. */
    private void writeLatencyPercentiles(StringWriter writer, String name, org.HdrHistogram.Histogram h) {
        if (h.getTotalCount() == 0) return;
        for (String p : new String[]{"50", "75", "90", "95", "99", "99.9", "99.99", "99.999", "100"}) {
            writer.write(name + "{host=\"" + hostname + "\",percentile=\"" + p + "\"} "
                    + dec.format(microsToMillis(h.getValueAtPercentile(Double.parseDouble(p)))) + "\n");
        }
    }

    private double microsToMillis(long microTime) {
        return microTime / (1000.0);
    }
}
