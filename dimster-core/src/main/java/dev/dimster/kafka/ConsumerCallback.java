package dev.dimster.kafka;

import dev.dimster.datagen.RecordFormat;

import java.time.Instant;

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
