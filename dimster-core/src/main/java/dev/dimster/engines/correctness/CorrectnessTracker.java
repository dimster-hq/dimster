package dev.dimster.engines.correctness;

import java.time.Instant;

import dev.dimster.datagen.RecordFormat;
import dev.dimster.kafka.ConsumerCallback;
import dev.dimster.stats.StatsCollector;

/**
 * ConsumerCallback wrapper that delegates performance stats to StatsCollector
 * and correctness tracking to ModelChecker.
 */
public class CorrectnessTracker implements ConsumerCallback {

    private final StatsCollector stats;
    private final ModelChecker checker;

    public CorrectnessTracker(StatsCollector stats, ModelChecker checker) {
        this.stats = stats;
        this.checker = checker;
    }

    @Override
    public void messageReceived(byte[] payload, RecordFormat format, String topic, int partition,
                                long publishTimestampMillis, Instant deliveryTime, Instant processingStartTime) {
        stats.messageReceived(payload, format, topic, partition, publishTimestampMillis, deliveryTime, processingStartTime);

        // The correctness fields live in Dimster's header, which the record
        // format locates (the leading bytes for RAW, the _dimster field otherwise).
        var msg = CorrectnessPayload.decode(format.header(payload));
        if (msg.producerId() >= 0) {
            checker.messageReceived(msg.producerId(), msg.sequence(), topic, partition, msg.crcValid());
        }
    }

    @Override
    public void messageReceived(int payloadSize, long publishTimestampMillis, Instant deliveryTime, Instant processingStartTime) {
        stats.messageReceived(payloadSize, publishTimestampMillis, deliveryTime, processingStartTime);
    }

    @Override
    public void error() {
        stats.error();
    }

    @Override
    public void pollCompleted(int messageCount) {
        stats.pollCompleted(messageCount);
    }

    @Override
    public void commitSyncCompleted(long latencyMicros) {
        stats.commitSyncCompleted(latencyMicros);
    }

    @Override
    public void commitAsyncCompleted() {
        stats.commitAsyncCompleted();
    }

    @Override
    public void processingTimeCompleted(long latencyMicros) {
        stats.processingTimeCompleted(latencyMicros);
    }

    @Override
    public void batchReceived(String consumerName, int messageCount) {
        stats.batchReceived(consumerName, messageCount);
    }

    @Override
    public void partitionsAssigned(int partitionCount) {
        stats.partitionsAssigned(partitionCount);
    }

    @Override
    public void partitionsRevoked(int partitionCount) {
        stats.partitionsRevoked(partitionCount);
    }

    public StatsCollector stats() {
        return stats;
    }

    public ModelChecker checker() {
        return checker;
    }
}
