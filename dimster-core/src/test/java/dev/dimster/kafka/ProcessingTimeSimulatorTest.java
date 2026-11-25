package dev.dimster.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import dev.dimster.MessageType;
import dev.dimster.datagen.DatasetGenerator;
import dev.dimster.datagen.DatasetResolver;
import dev.dimster.datagen.DatasetSpec;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.datagen.RowGenerator;
import dev.dimster.stats.StatsCollector;
import dev.dimster.worker.commands.PeriodStats;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

/**
 * Verifies {@link ProcessingTimeSimulator} accuracy across blocking (sequential,
 * constrained, full-parallel, random range) and continuous modes, including
 * zero-copy {@link ByteBuffer} slices across RAW/AVRO/JSON formats, probe
 * exclusion, delivery vs end-to-end latency split, and early stop wakeup.
 */
public class ProcessingTimeSimulatorTest {

    @Test
    public void blockingSequentialStaggersStartTimesSkipsProbesAndSplitsDeliveryVsE2eLatency() {
        ProcessingTimeSimulator sim = new ProcessingTimeSimulator(10, 10, 1, false);
        StatsCollector stats = new StatsCollector();
        stats.setRecordDeliveryLatency(true);

        long publishMs = 1_700_000_000_000L;
        Instant delivery = Instant.ofEpochMilli(publishMs + 50); // 50 ms delivery latency
        List<ConsumerRecord<String, ByteBuffer>> batch = List.of(
                rawRecord(0, publishMs, MessageType.NORMAL),
                rawRecord(1, publishMs, MessageType.PROBE),
                rawRecord(2, publishMs, MessageType.NORMAL),
                rawRecord(3, publishMs, MessageType.NORMAL),
                rawRecord(4, publishMs, MessageType.NORMAL));

        List<Long> startOffsetsMs = new ArrayList<>();
        long wallStart = System.nanoTime();
        long sleepMs = sim.processBatch(batch, new AtomicBoolean(false), stats, RecordFormat.RAW, delivery,
                (idx, startTime) -> {
                    startOffsetsMs.add(Duration.between(delivery, startTime).toMillis());
                    var r = batch.get(idx);
                    stats.messageReceived(r.value(), RecordFormat.RAW, "t", 0,
                            r.timestamp(), delivery, startTime);
                });
        long wallMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - wallStart);

        // 4 NORMAL records * 10 ms = 40 ms; PROBE does not consume processing time.
        assertEquals(40L, sleepMs);
        assertEquals(List.of(0L, 10L, 10L, 20L, 30L), startOffsetsMs);
        assertTrue("expected >= 35 ms wall sleep, got " + wallMs, wallMs >= 35);

        PeriodStats period = stats.snapshotAndReset();
        assertEquals(4, period.messagesReceived);
        assertEquals(1, stats.getProbesReceived());
        assertEquals(1, period.processingTime.getTotalCount());
        assertTrue("recorded processingTime should be >= 35 ms",
                period.processingTime.getMaxValue() >= 35_000);

        // Delivery latency is 50 ms for all 4 normal records; e2e ranges from 50 ms to 80 ms.
        assertEquals(50_000L, period.endToEndDeliveryLatency.getMinValue(), 100);
        assertEquals(50_000L, period.endToEndDeliveryLatency.getMaxValue(), 100);
        assertEquals(50_000L, period.endToEndLatency.getMinValue(), 100);
        assertEquals(80_000L, period.endToEndLatency.getMaxValue(), 100);
    }

    @Test
    public void blockingConstrainedParallelismWorkStealsAndSleepsForSlowestWorker() {
        ProcessingTimeSimulator sim = new ProcessingTimeSimulator(10, 10, 2, false);
        AtomicLong recordedProcMicros = new AtomicLong(-1);
        ConsumerCallback cb = procRecordingCallback(recordedProcMicros);

        Instant delivery = Instant.ofEpochMilli(1_700_000_000_000L);
        List<ConsumerRecord<String, ByteBuffer>> batch = List.of(
                rawRecord(0, 0, MessageType.NORMAL), // w0: 0->10
                rawRecord(1, 0, MessageType.NORMAL), // w1: 0->10
                rawRecord(2, 0, MessageType.PROBE),  // w0: 10->10 (0 ms)
                rawRecord(3, 0, MessageType.NORMAL), // w0: 10->20
                rawRecord(4, 0, MessageType.NORMAL), // w1: 10->20
                rawRecord(5, 0, MessageType.NORMAL)  // w0: 20->30
        );

        List<Long> startOffsetsMs = new ArrayList<>();
        long wallStart = System.nanoTime();
        long sleepMs = sim.processBatch(batch, new AtomicBoolean(false), cb, RecordFormat.RAW, delivery,
                (idx, startTime) -> startOffsetsMs.add(Duration.between(delivery, startTime).toMillis()));
        long wallMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - wallStart);

        assertEquals(30L, sleepMs);
        assertEquals(List.of(0L, 0L, 10L, 10L, 10L, 20L), startOffsetsMs);
        assertTrue("expected >= 25 ms wall sleep, got " + wallMs, wallMs >= 25);
        assertTrue("processingTimeCompleted should be >= 25 ms", recordedProcMicros.get() >= 25_000);
    }

    @Test
    public void blockingFullParallelismStartsAllAtDeliveryAndSleepsOnceForMax() {
        ProcessingTimeSimulator sim = new ProcessingTimeSimulator(15, 15, -1, false);
        AtomicLong recordedProcMicros = new AtomicLong(-1);
        ConsumerCallback cb = procRecordingCallback(recordedProcMicros);

        Instant delivery = Instant.ofEpochMilli(1_700_000_000_000L);
        List<ConsumerRecord<String, ByteBuffer>> batch = List.of(
                rawRecord(0, 0, MessageType.NORMAL),
                rawRecord(1, 0, MessageType.NORMAL),
                rawRecord(2, 0, MessageType.NORMAL),
                rawRecord(3, 0, MessageType.NORMAL));

        List<Long> startOffsetsMs = new ArrayList<>();
        long wallStart = System.nanoTime();
        long sleepMs = sim.processBatch(batch, new AtomicBoolean(false), cb, RecordFormat.RAW, delivery,
                (idx, startTime) -> startOffsetsMs.add(Duration.between(delivery, startTime).toMillis()));
        long wallMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - wallStart);

        assertEquals(15L, sleepMs);
        assertEquals(List.of(0L, 0L, 0L, 0L), startOffsetsMs);
        assertTrue("expected >= 12 ms wall sleep, got " + wallMs, wallMs >= 12);
        assertTrue("should sleep once for max (~15ms), not 4*15ms", wallMs < 50);
        assertTrue(recordedProcMicros.get() >= 12_000);
    }

    @Test
    public void blockingRandomRangeStaysWithinMinMaxAndWorksAcrossAvroAndJsonByteBufferSlices() {
        for (RecordFormat format : List.of(RecordFormat.AVRO, RecordFormat.JSON)) {
            ProcessingTimeSimulator sim = new ProcessingTimeSimulator(5, 10, 1, false);
            DatasetSpec spec = new DatasetSpec();
            spec.preset = "orders";
            spec.format = format;
            RowGenerator gen = new DatasetGenerator(DatasetResolver.resolve(spec)).newRowGenerator("p-0", 0);

            List<ConsumerRecord<String, ByteBuffer>> batch = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                byte[] bytes = gen.next(new byte[]{MessageType.NORMAL});
                batch.add(slicedRecord(i, 0, bytes));
            }
            // Add a probe record which must contribute 0 ms.
            batch.add(slicedRecord(4, 0, gen.next(new byte[]{MessageType.PROBE})));

            Instant delivery = Instant.ofEpochMilli(1_700_000_000_000L);
            List<Long> offsets = new ArrayList<>();
            long sleepMs = sim.processBatch(batch, new AtomicBoolean(false),
                    procRecordingCallback(new AtomicLong()), format, delivery,
                    (idx, startTime) -> offsets.add(Duration.between(delivery, startTime).toMillis()));

            assertEquals(0L, (long) offsets.get(0));
            for (int i = 1; i < 4; i++) {
                long step = offsets.get(i) - offsets.get(i - 1);
                assertTrue(format + " step " + step + " out of [5,10]", step >= 5 && step <= 10);
            }
            // Total sleep is 4 normal records * [5..10] ms; probe adds 0.
            long lastNormalStep = sleepMs - offsets.get(3);
            assertTrue(format + " last step " + lastNormalStep + " out of [5,10]",
                    lastNormalStep >= 5 && lastNormalStep <= 10);
            assertEquals(sleepMs, (long) offsets.get(4));
            // Verify ByteBuffer cursors were not mutated by appliesProcessingTime().
            for (var rec : batch) {
                assertEquals(5, rec.value().position());
            }
        }
    }

    @Test
    public void blockingEarlyStopCancelsSleepWithoutRecordingProcessingTime() throws Exception {
        ProcessingTimeSimulator sim = new ProcessingTimeSimulator(5_000, 5_000, 1, false);
        AtomicBoolean closing = new AtomicBoolean(false);
        AtomicLong recordedProcMicros = new AtomicLong(-1);

        Thread stopper = new Thread(() -> {
            try { Thread.sleep(30); } catch (InterruptedException ignored) {}
            closing.set(true);
        });
        stopper.start();

        long wallStart = System.nanoTime();
        long sleepMs = sim.processBatch(List.of(rawRecord(0, 0, MessageType.NORMAL)),
                closing, procRecordingCallback(recordedProcMicros), RecordFormat.RAW, Instant.now(),
                (idx, startTime) -> {});
        stopper.join(1_000);
        long wallMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - wallStart);

        assertEquals(0L, sleepMs);
        assertEquals(-1L, recordedProcMicros.get());
        // Sleeper polls closing in 1000 ms chunks, so a 5000 ms sleep aborts after the first chunk.
        assertTrue("should abort well before full 5000 ms sleep, took " + wallMs + " ms", wallMs < 2_500);
    }

    @Test
    public void continuousModeImmediateCompletionAndDelayedDrainSemantics() throws Exception {
        // procMs == 0 with empty queue completes immediately.
        ProcessingTimeSimulator zeroSim = new ProcessingTimeSimulator(0, 0, 1, true);
        Instant delivery = Instant.ofEpochMilli(1_700_000_000_000L);
        var immediate = zeroSim.offerAsync(rawRecord(0, 0, MessageType.NORMAL), delivery, RecordFormat.RAW);
        assertNotNull(immediate);
        assertEquals(delivery, immediate.simulatedStartTime());
        assertEquals(0, zeroSim.getQueueSize());

        // Sequential (parallelism=1), 15 ms per record: 3 records queue and complete sequentially.
        ProcessingTimeSimulator seqSim = new ProcessingTimeSimulator(15, 15, 1, true);
        assertNull(seqSim.offerAsync(rawRecord(0, 0, MessageType.NORMAL), delivery, RecordFormat.RAW));
        assertNull(seqSim.offerAsync(rawRecord(1, 0, MessageType.NORMAL), delivery, RecordFormat.RAW));
        assertNull(seqSim.offerAsync(rawRecord(2, 0, MessageType.NORMAL), delivery, RecordFormat.RAW));
        assertEquals(3, seqSim.getQueueSize());
        assertTrue("records should not drain immediately before deadline", seqSim.drainCompleted().isEmpty());

        List<ProcessingTimeSimulator.CompletedRecord> drained = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 2_000;
        while (drained.size() < 3 && System.currentTimeMillis() < deadline) {
            drained.addAll(seqSim.drainCompleted());
            if (drained.size() < 3) Thread.sleep(5);
        }
        assertEquals(3, drained.size());
        assertEquals(0L, drained.get(0).record().offset());
        assertEquals(1L, drained.get(1).record().offset());
        assertEquals(2L, drained.get(2).record().offset());
        long s0 = Duration.between(delivery, drained.get(0).simulatedStartTime()).toMillis();
        long s1 = Duration.between(delivery, drained.get(1).simulatedStartTime()).toMillis();
        long s2 = Duration.between(delivery, drained.get(2).simulatedStartTime()).toMillis();
        assertEquals(0L, s0);
        assertTrue("second record start offset ~15ms, got " + s1, s1 >= 10 && s1 <= 20);
        assertTrue("third record start offset ~30ms, got " + s2, s2 >= 25 && s2 <= 35);

        // Full parallel (parallelism=-1): all records start at deliveryTime and complete together.
        ProcessingTimeSimulator parSim = new ProcessingTimeSimulator(15, 15, -1, true);
        for (int i = 0; i < 4; i++) {
            assertNull(parSim.offerAsync(rawRecord(i, 0, MessageType.NORMAL), delivery, RecordFormat.RAW));
        }
        Thread.sleep(30);
        List<ProcessingTimeSimulator.CompletedRecord> parDrained = parSim.drainCompleted();
        assertEquals(4, parDrained.size());
        for (var cr : parDrained) {
            assertEquals(delivery, cr.simulatedStartTime());
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────

    private static ConsumerRecord<String, ByteBuffer> rawRecord(long offset, long timestampMs, byte marker) {
        byte[] payload = new byte[32];
        payload[0] = marker;
        return slicedRecord(offset, timestampMs, payload);
    }

    private static ConsumerRecord<String, ByteBuffer> slicedRecord(long offset, long timestampMs, byte[] payload) {
        byte[] padded = new byte[payload.length + 9];
        System.arraycopy(payload, 0, padded, 5, payload.length);
        ByteBuffer buf = ByteBuffer.wrap(padded, 5, payload.length);
        return new ConsumerRecord<>("t", 0, offset, timestampMs, TimestampType.CREATE_TIME,
                0, payload.length, null, buf, new RecordHeaders(), Optional.empty());
    }

    private static ConsumerCallback procRecordingCallback(AtomicLong recordedMicros) {
        return new ConsumerCallback() {
            @Override
            public void messageReceived(int payloadSize, long publishTimestampMillis,
                                        Instant deliveryTime, Instant processingStartTime) {}

            @Override public void error() {}

            @Override
            public void processingTimeCompleted(long latencyMicros) {
                recordedMicros.set(latencyMicros);
            }
        };
    }
}
