package dev.dimster.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import dev.dimster.ConsumerProcMode;
import dev.dimster.MessageType;
import dev.dimster.datagen.DatasetGenerator;
import dev.dimster.datagen.DatasetResolver;
import dev.dimster.datagen.DatasetSpec;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.datagen.RowGenerator;
import dev.dimster.engines.correctness.CorrectnessPayload;
import dev.dimster.engines.correctness.CorrectnessTracker;
import dev.dimster.engines.correctness.ModelChecker;
import dev.dimster.flink.FlinkEmitTime;
import dev.dimster.spi.kafka.AckType;
import dev.dimster.spi.kafka.RawConsumer;
import dev.dimster.spi.kafka.RawShareConsumer;
import dev.dimster.spi.kafka.ShareAckListener;
import dev.dimster.stats.StatsCollector;
import dev.dimster.worker.commands.PeriodStats;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

/**
 * Broker-free unit tests covering the consumer data path optimizations in
 * {@link KafkaConsumerWrapper}, {@link KafkaShareConsumerWrapper},
 * {@link NoOpDeserializer}, {@link StatsCollector#partitionBatchReceived},
 * and {@link CorrectnessTracker}'s zero-copy {@link ByteBuffer} path.
 */
public class KafkaConsumerWrapperDataPathTest {

    @Test
    public void noOpDeserializerDiscardsNonNullKeysWithoutThrowing() {
        NoOpDeserializer deserializer = new NoOpDeserializer();
        assertNull(deserializer.deserialize("t", "some-key".getBytes(StandardCharsets.UTF_8)));
        assertNull(deserializer.deserialize("t", new RecordHeaders(),
                ByteBuffer.wrap("some-key".getBytes(StandardCharsets.UTF_8))));
        assertNull(deserializer.deserialize("t", (byte[]) null));
        assertNull(deserializer.deserialize("t", new RecordHeaders(), (ByteBuffer) null));
    }

    @Test
    public void blockingFastPathBatchesStatsPerPartitionMetricsAndManualCommitLinger() throws Exception {
        ScriptedRawConsumer raw = new ScriptedRawConsumer();
        StatsCollector stats = new StatsCollector();
        stats.setPerPartitionMetrics(true);

        TopicPartition tp0 = new TopicPartition("taxi", 0);
        TopicPartition tp1 = new TopicPartition("taxi", 1);

        long nowMs = System.currentTimeMillis();
        // Use non-zero position ByteBuffer slices (simulating Kafka fetch buffer slices)
        // to verify zero-copy reads never depend on position == 0 or mutate cursors.
        ByteBuffer p0Rec0 = slicedPayload(MessageType.NORMAL, 100);
        ByteBuffer p0Rec1 = slicedPayload(MessageType.PROBE, 40);
        ByteBuffer p0Rec2 = slicedPayload(MessageType.NORMAL, 120);
        ByteBuffer p1Rec0 = slicedPayload(MessageType.NORMAL, 80);
        // Future timestamp (clock skew) must be counted in messages/bytes but not allocate pp latency.
        ByteBuffer p1Rec1 = slicedPayload(MessageType.NORMAL, 60);

        Map<TopicPartition, List<ConsumerRecord<String, ByteBuffer>>> batch1 = new LinkedHashMap<>();
        batch1.put(tp0, List.of(
                record("taxi", 0, 10, nowMs - 50, p0Rec0, new RecordHeaders()),
                record("taxi", 0, 11, nowMs - 40, p0Rec1, new RecordHeaders()),
                record("taxi", 0, 12, nowMs - 30, p0Rec2, new RecordHeaders())));
        batch1.put(tp1, List.of(
                record("taxi", 1, 20, nowMs - 20, p1Rec0, new RecordHeaders()),
                record("taxi", 1, 25, nowMs + 60_000, p1Rec1, new RecordHeaders())));

        // Second poll arrives well inside the 60s manualCommitLingerMs -> must NOT commit again.
        Map<TopicPartition, List<ConsumerRecord<String, ByteBuffer>>> batch2 = Map.of(
                tp0, List.of(record("taxi", 0, 13, nowMs - 10, slicedPayload(MessageType.NORMAL, 50), new RecordHeaders())));

        raw.enqueue(new ConsumerRecords<>(batch1));
        raw.enqueue(new ConsumerRecords<>(batch2));

        Properties props = new Properties();
        props.setProperty("dimster.poll.timeout.ms", "5");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty("dimster.commit.mode", "CommitAsync");
        props.setProperty("dimster.commit.linger.ms", "60000");

        try (KafkaConsumerWrapper wrapper = new KafkaConsumerWrapper(
                raw, props, stats, "taxi", "c-fast",
                0, 0, 1, List.of(tp0, tp1), false, null, RecordFormat.RAW,
                ConsumerProcMode.BLOCKING, 0, false)) {
            assertTrue("both batches should be polled", raw.awaitDrained(5, TimeUnit.SECONDS));
            waitUntil(() -> stats.counters().messagesReceived() == 5, 5_000);
        }

        // ByteBuffer cursors must be untouched.
        assertEquals(100, p0Rec0.remaining());
        assertEquals(40, p0Rec1.remaining());

        // Only the first batch crossed manualCommitLingerMs; highest offset per partition committed.
        assertEquals(1, raw.asyncCommits.size());
        assertEquals(12L, raw.asyncCommits.get(0).get(tp0).offset());
        assertEquals(25L, raw.asyncCommits.get(0).get(tp1).offset());

        PeriodStats period = stats.snapshotAndReset();
        assertEquals(5, period.messagesReceived); // 6 total minus 1 probe
        assertEquals(1, stats.getProbesReceived());
        assertEquals(100 + 120 + 80 + 60 + 50, period.bytesReceived);
        assertEquals(4, period.endToEndLatency.getTotalCount()); // 1 future-timestamp dropped
        assertEquals(3, period.perPartitionE2eLatency.get("taxi:0").getTotalCount());
        assertEquals(1, period.perPartitionE2eLatency.get("taxi:1").getTotalCount());
        assertEquals(1, period.totalCommitAsync);
    }

    @Test
    public void flinkEmitTimeGatingSkipsOrRecordsHeadersAccurately() throws Exception {
        long publishMs = System.currentTimeMillis() - 100;
        long emitMicros = TimeUnit.MILLISECONDS.toMicros(publishMs + 40);
        RecordHeaders flinkHeaders = new RecordHeaders();
        flinkHeaders.add(FlinkEmitTime.HEADER, ByteBuffer.allocate(8).putLong(emitMicros).array());

        TopicPartition tp = new TopicPartition("flink-out", 0);
        ConsumerRecord<String, ByteBuffer> stamped = record(
                "flink-out", 0, 0, publishMs, slicedPayload(MessageType.NORMAL, 32), flinkHeaders);

        // With checkFlinkEmitTime = false, headers are skipped.
        ScriptedRawConsumer rawOff = new ScriptedRawConsumer();
        StatsCollector statsOff = new StatsCollector();
        rawOff.enqueue(new ConsumerRecords<>(Map.of(tp, List.of(stamped))));
        try (KafkaConsumerWrapper ignored = new KafkaConsumerWrapper(
                rawOff, fastProps(), statsOff, "flink-out", "c-no-flink",
                0, 0, 1, List.of(tp), false, null, RecordFormat.RAW,
                ConsumerProcMode.BLOCKING, 0, false)) {
            assertTrue(rawOff.awaitDrained(5, TimeUnit.SECONDS));
            waitUntil(() -> statsOff.counters().messagesReceived() == 1, 5_000);
        }
        PeriodStats offPeriod = statsOff.snapshotAndReset();
        assertEquals(0, offPeriod.producerToFlinkLatency.getTotalCount());
        assertEquals(0, offPeriod.flinkToConsumerLatency.getTotalCount());

        // With checkFlinkEmitTime = true, split latencies are recorded on the fast path.
        ScriptedRawConsumer rawOn = new ScriptedRawConsumer();
        StatsCollector statsOn = new StatsCollector();
        rawOn.enqueue(new ConsumerRecords<>(Map.of(tp, List.of(stamped))));
        try (KafkaConsumerWrapper ignored = new KafkaConsumerWrapper(
                rawOn, fastProps(), statsOn, "flink-out", "c-flink",
                0, 0, 1, List.of(tp), false, null, RecordFormat.RAW,
                ConsumerProcMode.BLOCKING, 0, true)) {
            assertTrue(rawOn.awaitDrained(5, TimeUnit.SECONDS));
            waitUntil(() -> statsOn.counters().messagesReceived() == 1, 5_000);
        }
        PeriodStats onPeriod = statsOn.snapshotAndReset();
        assertEquals(1, onPeriod.producerToFlinkLatency.getTotalCount());
        assertEquals(1, onPeriod.flinkToConsumerLatency.getTotalCount());
        assertEquals(40_000L, onPeriod.producerToFlinkLatency.getMaxValue(), 50);
    }

    @Test
    public void blockingSimulatedProcessingPathAndRuntimeToggleWorkCleanly() throws Exception {
        ScriptedRawConsumer raw = new ScriptedRawConsumer();
        StatsCollector stats = new StatsCollector();
        stats.setRecordDeliveryLatency(true);

        TopicPartition tp = new TopicPartition("proc-topic", 0);
        long publishMs = System.currentTimeMillis() - 100;
        raw.enqueue(new ConsumerRecords<>(Map.of(tp, List.of(
                record("proc-topic", 0, 0, publishMs, slicedPayload(MessageType.NORMAL, 64), new RecordHeaders()),
                record("proc-topic", 0, 1, publishMs, slicedPayload(MessageType.PROBE, 16), new RecordHeaders())))));

        try (KafkaConsumerWrapper wrapper = new KafkaConsumerWrapper(
                raw, fastProps(), stats, "proc-topic", "c-proc",
                5, 5, 1, List.of(tp), false, null, RecordFormat.RAW,
                ConsumerProcMode.BLOCKING, 0, false)) {
            assertTrue(raw.awaitDrained(5, TimeUnit.SECONDS));
            waitUntil(() -> stats.counters().messagesReceived() == 1 && stats.getProbesReceived() == 1, 5_000);

            // Toggle to fast path at runtime (as /modify does) and send another record.
            wrapper.setProcessingTime(0, 0);
            raw.enqueue(new ConsumerRecords<>(Map.of(tp, List.of(
                    record("proc-topic", 0, 2, publishMs, slicedPayload(MessageType.NORMAL, 64), new RecordHeaders())))));
            waitUntil(() -> stats.counters().messagesReceived() == 2, 5_000);
        }

        PeriodStats period = stats.snapshotAndReset();
        assertEquals(2, period.messagesReceived);
        assertEquals(2, period.endToEndLatency.getTotalCount());
        assertEquals(2, period.endToEndDeliveryLatency.getTotalCount());
        assertEquals(1, period.processingTime.getTotalCount());
    }

    @Test
    public void continuousModeTracksAndCommitsOffsetsPerPartition() throws Exception {
        ScriptedRawConsumer raw = new ScriptedRawConsumer();
        StatsCollector stats = new StatsCollector();
        TopicPartition tp0 = new TopicPartition("cont-topic", 0);
        TopicPartition tp1 = new TopicPartition("cont-topic", 1);
        long publishMs = System.currentTimeMillis() - 50;

        Map<TopicPartition, List<ConsumerRecord<String, ByteBuffer>>> batch = new LinkedHashMap<>();
        batch.put(tp0, List.of(
                record("cont-topic", 0, 0, publishMs, slicedPayload(MessageType.NORMAL, 32), new RecordHeaders()),
                record("cont-topic", 0, 2, publishMs, slicedPayload(MessageType.NORMAL, 32), new RecordHeaders())));
        batch.put(tp1, List.of(
                record("cont-topic", 1, 5, publishMs, slicedPayload(MessageType.NORMAL, 32), new RecordHeaders())));
        raw.enqueue(new ConsumerRecords<>(batch));

        try (KafkaConsumerWrapper ignored = new KafkaConsumerWrapper(
                raw, fastProps(), stats, "cont-topic", "c-cont",
                0, 0, 1, List.of(tp0, tp1), false, null, RecordFormat.RAW,
                ConsumerProcMode.CONTINUOUS, 100, false)) {
            waitUntil(() -> stats.counters().messagesReceived() == 3 && !raw.asyncCommits.isEmpty(), 5_000);
        }

        // Continuous mode commits highest + 1 per partition.
        Map<TopicPartition, OffsetAndMetadata> committed = raw.asyncCommits.get(raw.asyncCommits.size() - 1);
        assertEquals(3L, committed.get(tp0).offset());
        assertEquals(6L, committed.get(tp1).offset());
    }

    @Test
    public void correctnessTrackerWorksWithZeroCopyByteBuffersAcrossAllFormats() {
        for (RecordFormat format : RecordFormat.values()) {
            StatsCollector stats = new StatsCollector();
            ModelChecker checker = new ModelChecker(true, true, 0);
            CorrectnessTracker tracker = new CorrectnessTracker(stats, checker);
            RowGenerator rows = format == RecordFormat.RAW ? null : datasetRows(format);

            for (long seq = 0; seq < 20; seq++) {
                byte[] bytes;
                if (format == RecordFormat.RAW) {
                    bytes = new byte[64];
                    CorrectnessPayload.encode(bytes, 3, seq, System.currentTimeMillis(), true);
                } else {
                    byte[] header = CorrectnessPayload.header(3, seq, System.currentTimeMillis(), true);
                    bytes = rows.next(header);
                }
                // Embed in a larger array with non-zero position/offset to test slice handling.
                byte[] padded = new byte[bytes.length + 11];
                System.arraycopy(bytes, 0, padded, 7, bytes.length);
                ByteBuffer slice = ByteBuffer.wrap(padded, 7, bytes.length);

                checker.messageSent(3, seq, "corr-topic");
                tracker.messageReceived(slice, format, "corr-topic", 0,
                        System.currentTimeMillis() - 10, Instant.now(), Instant.now());

                assertEquals(format + " position unchanged", 7, slice.position());
                assertEquals(format + " limit unchanged", 7 + bytes.length, slice.limit());
            }

            assertEquals(format + " total received", 20, checker.getTotalReceived());
            assertEquals(format + " zero CRC failures", 0, checker.getCrcFailureCount());
            assertEquals(format + " zero loss", 0, checker.computeLoss().totalLost());
        }
    }

    @Test
    public void shareConsumerFastPathRecordsBatchAndExplicitlyAcksEveryRecord() throws Exception {
        ScriptedRawShareConsumer raw = new ScriptedRawShareConsumer();
        StatsCollector stats = new StatsCollector();
        TopicPartition tp = new TopicPartition("share-topic", 0);
        long publishMs = System.currentTimeMillis() - 25;

        raw.enqueue(new ConsumerRecords<>(Map.of(tp, List.of(
                record("share-topic", 0, 0, publishMs, slicedPayload(MessageType.NORMAL, 48), new RecordHeaders()),
                record("share-topic", 0, 1, publishMs, slicedPayload(MessageType.NORMAL, 52), new RecordHeaders())))));

        Properties props = new Properties();
        props.setProperty("dimster.poll.timeout.ms", "5");
        props.setProperty("share.acknowledgement.mode", "explicit");
        props.setProperty("dimster.commit.mode", "CommitAsync");

        try (KafkaShareConsumerWrapper ignored = new KafkaShareConsumerWrapper(
                raw, props, stats, "share-topic", "sc-fast",
                0, 0, 1, false, null, RecordFormat.RAW, false)) {
            waitUntil(() -> stats.counters().messagesReceived() == 2 && raw.asyncCommits.get() >= 1, 5_000);
        }

        assertEquals(2, raw.ackedOffsets.size());
        assertEquals(List.of(0L, 1L), raw.ackedOffsets);
        PeriodStats period = stats.snapshotAndReset();
        assertEquals(2, period.messagesReceived);
        assertEquals(100, period.bytesReceived);
        assertEquals(2, period.endToEndLatency.getTotalCount());
        assertEquals(1, period.totalCommitAsync);
    }

    // ─── Helpers ─────────────────────────────────────────────────────────

    private static Properties fastProps() {
        Properties props = new Properties();
        props.setProperty("dimster.poll.timeout.ms", "5");
        return props;
    }

    private static ByteBuffer slicedPayload(byte marker, int len) {
        byte[] backing = new byte[len + 9];
        backing[5] = marker;
        return ByteBuffer.wrap(backing, 5, len);
    }

    private static ConsumerRecord<String, ByteBuffer> record(
            String topic, int partition, long offset, long timestampMs,
            ByteBuffer value, RecordHeaders headers) {
        return new ConsumerRecord<>(topic, partition, offset, timestampMs,
                TimestampType.CREATE_TIME, 0, value.remaining(), null, value, headers, Optional.empty());
    }

    private static RowGenerator datasetRows(RecordFormat format) {
        DatasetSpec spec = new DatasetSpec();
        spec.preset = "orders";
        spec.format = format;
        return new DatasetGenerator(DatasetResolver.resolve(spec)).newRowGenerator("producer-0", 0);
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertTrue("Condition did not become true within " + timeoutMs + " ms", condition.getAsBoolean());
    }

    /** Fake classic consumer returning enqueued batches and recording commits. */
    private static final class ScriptedRawConsumer implements RawConsumer<String, ByteBuffer> {
        private final BlockingQueue<ConsumerRecords<String, ByteBuffer>> queue = new LinkedBlockingQueue<>();
        private final List<Map<TopicPartition, OffsetAndMetadata>> asyncCommits = new CopyOnWriteArrayList<>();
        private final AtomicInteger pending = new AtomicInteger();
        private volatile CountDownLatch drained = new CountDownLatch(0);

        void enqueue(ConsumerRecords<String, ByteBuffer> batch) {
            if (pending.getAndIncrement() == 0) {
                drained = new CountDownLatch(1);
            }
            queue.add(batch);
        }

        boolean awaitDrained(long timeout, TimeUnit unit) throws InterruptedException {
            return drained.await(timeout, unit);
        }

        @Override public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {}
        @Override public void assign(Collection<TopicPartition> partitions) {}

        @Override
        public ConsumerRecords<String, ByteBuffer> poll(Duration timeout) {
            try {
                ConsumerRecords<String, ByteBuffer> batch = queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
                if (batch != null) {
                    if (pending.decrementAndGet() == 0) {
                        drained.countDown();
                    }
                    return batch;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return ConsumerRecords.empty();
        }

        @Override public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {}

        @Override
        public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
            asyncCommits.add(Map.copyOf(offsets));
            if (callback != null) {
                callback.onComplete(offsets, null);
            }
        }

        @Override public Map<MetricName, ? extends Metric> metrics() { return Map.of(); }
        @Override public void wakeup() {}
        @Override public void close() {}
    }

    /** Fake share consumer returning enqueued batches and recording explicit acks + commits. */
    private static final class ScriptedRawShareConsumer implements RawShareConsumer<String, ByteBuffer> {
        private final BlockingQueue<ConsumerRecords<String, ByteBuffer>> queue = new LinkedBlockingQueue<>();
        private final List<Long> ackedOffsets = new CopyOnWriteArrayList<>();
        private final AtomicInteger asyncCommits = new AtomicInteger();

        void enqueue(ConsumerRecords<String, ByteBuffer> batch) {
            queue.add(batch);
        }

        @Override public void subscribe(Collection<String> topics) {}
        @Override public void setAckCallback(ShareAckListener listener) {}

        @Override
        public ConsumerRecords<String, ByteBuffer> poll(Duration timeout) {
            try {
                ConsumerRecords<String, ByteBuffer> batch = queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
                if (batch != null) {
                    return batch;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return ConsumerRecords.empty();
        }

        @Override
        public void acknowledge(ConsumerRecord<String, ByteBuffer> record, AckType type) {
            if (type == AckType.ACCEPT) {
                ackedOffsets.add(record.offset());
            }
        }

        @Override public void commitSync() {}
        @Override public void commitAsync() { asyncCommits.incrementAndGet(); }
        @Override public Map<MetricName, ? extends Metric> metrics() { return Map.of(); }
        @Override public void wakeup() {}
        @Override public void close() {}
    }
}
