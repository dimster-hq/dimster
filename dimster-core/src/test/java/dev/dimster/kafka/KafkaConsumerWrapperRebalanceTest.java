package dev.dimster.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import dev.dimster.ConsumerProcMode;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.spi.kafka.RawConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

/**
 * Tests the rebalance-listener role of {@link KafkaConsumerWrapper}. A small
 * RawConsumer fake keeps this focused on Dimster's version boundary without
 * requiring a broker or binding the test to one Kafka client implementation.
 */
public class KafkaConsumerWrapperRebalanceTest {

    @Test
    public void dynamicConsumerRegistersListenerAndReportsPartitionCounts() {
        FakeRawConsumer consumer = new FakeRawConsumer();
        TrackingCallback callback = new TrackingCallback();

        try (KafkaConsumerWrapper wrapper = wrapper(consumer, callback, null)) {
            assertSame(wrapper, consumer.rebalanceListener);

            consumer.rebalanceListener.onPartitionsAssigned(List.of(
                    new TopicPartition("topic", 0),
                    new TopicPartition("topic", 1),
                    new TopicPartition("topic", 2)));
            consumer.rebalanceListener.onPartitionsRevoked(List.of(
                    new TopicPartition("topic", 0),
                    new TopicPartition("topic", 1)));

            assertEquals(3, callback.assigned.get());
            assertEquals(2, callback.revoked.get());
        }
    }

    @Test
    public void staticConsumerUsesAssignmentWithoutRegisteringListener() {
        FakeRawConsumer consumer = new FakeRawConsumer();
        List<TopicPartition> assignment = List.of(new TopicPartition("topic", 4));

        try (KafkaConsumerWrapper ignored = wrapper(consumer, new TrackingCallback(), assignment)) {
            assertEquals(assignment, consumer.staticAssignment);
            assertNull(consumer.rebalanceListener);
        }
    }

    private static KafkaConsumerWrapper wrapper(FakeRawConsumer consumer,
                                                ConsumerCallback callback,
                                                List<TopicPartition> staticPartitions) {
        Properties properties = new Properties();
        properties.setProperty("dimster.poll.timeout.ms", "1");
        return new KafkaConsumerWrapper(
                consumer, properties, callback, "topic", "consumer-1",
                0, 0, 1, staticPartitions, false, null, RecordFormat.RAW,
                ConsumerProcMode.BLOCKING, 0);
    }

    /** Captures only the two callback values under test. */
    private static final class TrackingCallback implements ConsumerCallback {
        private final AtomicInteger assigned = new AtomicInteger();
        private final AtomicInteger revoked = new AtomicInteger();

        @Override
        public void messageReceived(int payloadSize, long publishTimestampMillis,
                                    Instant deliveryTime, Instant processingStartTime) {}

        @Override
        public void error() {}

        @Override
        public void partitionsAssigned(int partitionCount) {
            assigned.addAndGet(partitionCount);
        }

        @Override
        public void partitionsRevoked(int partitionCount) {
            revoked.addAndGet(partitionCount);
        }
    }

    /**
     * Captures subscription/assignment while returning empty polls until the
     * wrapper closes. It models only the stable SPI behavior this test needs.
     */
    private static final class FakeRawConsumer implements RawConsumer<String, byte[]> {
        private volatile ConsumerRebalanceListener rebalanceListener;
        private volatile List<TopicPartition> staticAssignment;

        @Override
        public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
            this.rebalanceListener = listener;
        }

        @Override
        public void assign(Collection<TopicPartition> partitions) {
            this.staticAssignment = List.copyOf(partitions);
        }

        @Override
        public ConsumerRecords<String, byte[]> poll(Duration timeout) {
            try {
                Thread.sleep(timeout.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return ConsumerRecords.empty();
        }

        @Override
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {}

        @Override
        public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,
                                OffsetCommitCallback callback) {}

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return Map.of();
        }

        @Override
        public void wakeup() {}

        @Override
        public void close() {}
    }
}
