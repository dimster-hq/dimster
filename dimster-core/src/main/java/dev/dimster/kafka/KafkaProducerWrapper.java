package dev.dimster.kafka;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import dev.dimster.spi.kafka.RawProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-Kafka-producer wrapper. Owns one underlying {@link RawProducer} (the
 * SPI handle to a kafka-clients producer of whichever Kafka version this
 * dimster build is bound to), plus the producer's static partition pinning
 * (if any) and a per-instance round-robin cursor over those pinned partitions.
 *
 * Most workloads do not involve partition selection during send operations,
 * using the default partitioner based on the key.
 *
 * <h2>Partition selection model</h2>
 *
 * <p>Partition selection on each send goes through three resolution tiers:
 *
 * <ol>
 *   <li>If the caller passes a non-null {@code partitionOverride} (used by
 *       partition-targeting key distributors such as {@code PARTITION_ZIPF}
 *       and {@code PARTITION_ROUND_ROBIN}), that partition is selected for
 *       the send target.</li>
 *   <li>If partition pinning has been configured (correctness mode or
 *       {@code PINNED_PARTITIONS}, partitions are selected round-robin
 *       from the pinned partition list is decided once by {@code TopologyPlanner} and
 *       passed in at construction.</li>
 *   <li>Otherwise, the partition argument is left null and Kafka's configured
 *       partitioner picks based on the key.</li>
 * </ol>
 *
 * <p>The single-partition correctness case ({@code pinnedPartitions = [P]})
 * collapses naturally: every send produces partition {@code P}.
 */
public class KafkaProducerWrapper implements AutoCloseable, KafkaMetricsSource {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerWrapper.class);

    private final RawProducer<String, byte[]> producer;
    private final String topic;
    private final String name;
    private final int producerIdx;
    private final int[] pinnedPartitions;
    // Plain int — each wrapper is owned by exactly one thread for the lifetime of a test
    // point: the engine thread during probeProducers (sequential, no concurrency), then
    // the single runProducer executor task during startLoad. The executor.submit that
    // launches runProducer establishes happens-before with the prior probe writes.
    private int pinnedCursor = 0;
    private final boolean transactional;

    /**
     * @param producer          the underlying Kafka producer
     * @param topic             topic this producer publishes to
     * @param name              stable display name (used in logs and Prometheus labels)
     * @param producerIdx       the producer's index within its topic (VirtualProducer.producerIdx);
     *                          unique per topic across workers, used by generated datasets
     * @param pinnedPartitions  partitions this producer round-robins through;
     *                          empty array means no pinning
     * @param transactional     whether the producer was configured with a transactional.id
     */
    public KafkaProducerWrapper(RawProducer<String, byte[]> producer, String topic, String name,
                                int producerIdx, int[] pinnedPartitions, boolean transactional) {
        this.producer = producer;
        this.topic = topic;
        this.name = name;
        this.producerIdx = producerIdx;
        // Defensive copy so the caller can't mutate the wrapper's pinning post-construction.
        this.pinnedPartitions = pinnedPartitions == null
                ? new int[0]
                : Arrays.copyOf(pinnedPartitions, pinnedPartitions.length);
        this.transactional = transactional;
    }

    public String getTopic() {
        return topic;
    }

    /** The producer's index within its topic; see the constructor. */
    public int producerIdx() {
        return producerIdx;
    }

    /** Returns a copy of the pinned partitions array (empty if no pinning). */
    public int[] getPinnedPartitions() {
        return Arrays.copyOf(pinnedPartitions, pinnedPartitions.length);
    }

    public boolean isTransactional() {
        return transactional;
    }

    /**
     * Resolve which partition a send should go to. Public for unit testing the
     * resolution rules without standing up a real Kafka broker. Same three-tier
     * logic documented on the class:
     * {@code partitionOverride > pinned round-robin > null (Kafka partitioner)}.
     */
    Integer resolvePartition(Integer partitionOverride) {
        if (partitionOverride != null) return partitionOverride;
        if (pinnedPartitions.length == 0) return null;
        // floorMod handles negative cursor values cleanly if the int ever wraps past Integer.MAX_VALUE
        // (would take billions of sends on a single producer, but the cost is one extra branch).
        int idx = Math.floorMod(pinnedCursor++, pinnedPartitions.length);
        return pinnedPartitions[idx];
    }

    /**
     * Result handed to the success callback of {@link #sendAsync}. Carries the
     * "topic:partition" string used by stats keying plus the broker-acknowledged
     * record timestamp, both of which are needed at higher layers (the
     * timestamp is used for the optional per-message printMessages logging
     * in {@code LocalWorker.runProducer}).
     */
    public record SendResult(String topicPartition, int partition, long offset, long timestamp) {}

    public CompletableFuture<SendResult> sendAsync(String key, byte[] payload) {
        return sendAsync(key, null, payload);
    }

    /**
     * Send a record. Partition resolution: {@code partitionOverride} > pinned
     * round-robin > Kafka's partitioner. See class javadoc for the full
     * contract.
     */
    public CompletableFuture<SendResult> sendAsync(String key, Integer partitionOverride, byte[] payload) {
        Integer partition = resolvePartition(partitionOverride);
        var record = partition != null
                ? new ProducerRecord<>(topic, partition, key, payload)
                : new ProducerRecord<>(topic, key, payload);
        var future = new CompletableFuture<SendResult>();

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                future.completeExceptionally(exception);
            } else {
                future.complete(new SendResult(
                        metadata.topic() + ":" + metadata.partition(),
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp()));
            }
        });

        return future;
    }

    /**
     * Calls {@link RawProducer#initTransactions()} once if this producer is
     * transactional. Must be invoked before the first {@link #beginTransaction()}.
     * No-op for non-transactional producers, so {@code LocalWorker} can call
     * this unconditionally.
     */
    public void initTransactionsIfEnabled() {
        if (transactional) {
            producer.initTransactions();
        }
    }

    /** No-op for non-transactional producers. */
    public void beginTransaction() {
        if (transactional) {
            producer.beginTransaction();
        }
    }

    /** No-op for non-transactional producers. */
    public void commitTransaction() {
        if (transactional) {
            producer.commitTransaction();
        }
    }

    /**
     * No-op for non-transactional producers. Swallows secondary failures into
     * a warn log so the caller's loop can continue — the typical reason for
     * an abort to itself fail is the producer already being in a fenced or
     * uncommittable state, in which case the next {@code beginTransaction}
     * will surface the real problem.
     */
    public void abortTransaction() {
        if (!transactional) return;
        try {
            producer.abortTransaction();
        } catch (Exception e) {
            log.warn("abortTransaction failed for {} — continuing: {}", name, e.toString());
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> kafkaMetrics() {
        return producer.metrics();
    }

    @Override
    public String clientName() {
        return name;
    }

    @Override
    public void close() {
        // The 30-second close timeout that used to be passed here lives inside
        // RawProducer's per-version impls now; close() on the SPI is parameterless.
        producer.close();
    }
}
