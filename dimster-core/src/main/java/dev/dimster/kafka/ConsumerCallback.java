package dev.dimster.kafka;

import dev.dimster.datagen.RecordFormat;
import dev.dimster.flink.FlinkEmitTime;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;

public interface ConsumerCallback {

    void messageReceived(int payloadSize, long publishTimestampMillis,
                         Instant deliveryTime, Instant processingStartTime);

    /**
     * @param format layout of the value, which locates Dimster's header (and
     *     its message type) in it: RAW unless the test point produces a
     *     generated dataset. The consumer wrapper is given it by the worker.
     */
    default void messageReceived(byte[] payload, RecordFormat format, String topic,
                                 int partition, long publishTimestampMillis,
                                 Instant deliveryTime, Instant processingStartTime) {
        messageReceived(payload.length, publishTimestampMillis, deliveryTime, processingStartTime);
    }

    /**
     * Zero-copy {@link ByteBuffer} overload used by consumer wrappers backed by
     * {@code ByteBufferDeserializer}. Callers must not mutate buffer cursors.
     */
    default void messageReceived(ByteBuffer payload, RecordFormat format, String topic,
                                 int partition, long publishTimestampMillis,
                                 Instant deliveryTime, Instant processingStartTime) {
        messageReceived(payload != null ? payload.remaining() : 0,
                publishTimestampMillis, deliveryTime, processingStartTime);
    }

    /**
     * Batched fast-path notification for one partition's records from a single
     * poll when no per-record processing time is simulated ({@code processingStartTime == deliveryTime}).
     * Implementations like {@link dev.dimster.stats.StatsCollector} override this to
     * amortize counter updates, phaser barriers, and per-partition lookups per poll batch.
     */
    default void partitionBatchReceived(String topic, int partition,
                                        List<ConsumerRecord<String, ByteBuffer>> records,
                                        RecordFormat format, Instant deliveryTime,
                                        boolean checkFlinkEmitTime) {
        for (int i = 0, n = records.size(); i < n; i++) {
            ConsumerRecord<String, ByteBuffer> record = records.get(i);
            messageReceived(record.value(), format, topic, partition,
                    record.timestamp(), deliveryTime, deliveryTime);
            if (checkFlinkEmitTime) {
                FlinkEmitTime.reportBuffer(this, record, format, deliveryTime);
            }
        }
    }

    /**
     * A record that a Flink job stamped with the time it emitted it
     * ({@code FlinkEmitTime}), reported right after {@link #messageReceived}
     * for the same record. Only records of a Flink job's output have the stamp.
     *
     * @param flinkEmitMicros when the Flink job emitted the record, in epoch microseconds
     */
    default void flinkRecordReceived(byte[] payload, RecordFormat format, long publishTimestampMillis,
                                     long flinkEmitMicros, Instant processingStartTime) {}

    /** Zero-copy {@link ByteBuffer} overload of {@link #flinkRecordReceived}. */
    default void flinkRecordReceived(ByteBuffer payload, RecordFormat format, long publishTimestampMillis,
                                     long flinkEmitMicros, Instant processingStartTime) {}

    void error();

    default void pollCompleted(int messageCount) {}
    default void commitSyncCompleted(long latencyMicros) {}
    default void commitAsyncCompleted() {}
    default void processingTimeCompleted(long latencyMicros) {}
    default void batchReceived(String consumerName, int messageCount) {}

    /** Records how many dynamically managed partitions Kafka assigned. */
    default void partitionsAssigned(int partitionCount) {}

    /** Records how many dynamically managed partitions Kafka asked the consumer to release. */
    default void partitionsRevoked(int partitionCount) {}
}
