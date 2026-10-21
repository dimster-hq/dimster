package dev.dimster.engines.correctness;

import static org.junit.Assert.*;

import java.time.Instant;

import dev.dimster.MessageType;
import dev.dimster.datagen.DatasetGenerator;
import dev.dimster.datagen.DatasetResolver;
import dev.dimster.datagen.DatasetSpec;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.datagen.RowGenerator;
import dev.dimster.stats.StatsCollector;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for CorrectnessTracker — verifies the full pipeline from
 * raw payload bytes through decode to ModelChecker tracking.
 *
 * These tests catch bugs that unit tests on ModelChecker or CorrectnessPayload
 * alone would miss, such as probe message contamination and reset/sequence
 * coordination.
 */
public class CorrectnessTrackerTest {

    private StatsCollector stats;
    private ModelChecker checker;
    private CorrectnessTracker tracker;

    @Before
    public void setUp() {
        stats = new StatsCollector();
        checker = new ModelChecker(true, true, 0);
        tracker = new CorrectnessTracker(stats, checker);
    }

    /**
     * Probe messages (all zeros, MessageType.NORMAL) must not affect
     * correctness tracking. This was a real bug: probes were decoded as
     * producerId=0, sequence=0, contaminating the ModelChecker state.
     */
    @Test
    public void probeMessagesAreIgnoredByChecker() {
        byte[] probe = new byte[24];
        tracker.messageReceived(probe, RecordFormat.RAW, "test-topic", 0, System.currentTimeMillis(), Instant.now(), Instant.now());

        assertEquals(0, checker.getTotalReceived());
    }

    /**
     * Random payloads (normal benchmark mode) must not be tracked as
     * correctness messages, even if they happen to be large enough to decode.
     */
    @Test
    public void randomPayloadsAreIgnoredByChecker() {
        byte[] randomPayload = new byte[256];
        tracker.messageReceived(randomPayload, RecordFormat.RAW, "test-topic", 0, System.currentTimeMillis(), Instant.now(), Instant.now());

        assertEquals(0, checker.getTotalReceived());
    }

    /**
     * Correctness-encoded payloads must be tracked by the checker.
     */
    @Test
    public void correctnessPayloadsAreTracked() {
        byte[] payload = new byte[64];
        CorrectnessPayload.encode(payload, 0, 0, System.currentTimeMillis(), false);
        tracker.messageReceived(payload, RecordFormat.RAW, "test-topic", 0, System.currentTimeMillis(), Instant.now(), Instant.now());

        assertEquals(1, checker.getTotalReceived());
    }

    /**
     * A full send+receive cycle through the tracker should show no violations.
     */
    @Test
    public void fullCycleNoViolations() {
        int producerId = 0;
        for (long seq = 0; seq < 100; seq++) {
            byte[] payload = new byte[64];
            CorrectnessPayload.encode(payload, producerId, seq,
                    System.currentTimeMillis(), true);
            checker.messageSent(producerId, seq, "test-topic");
            tracker.messageReceived(payload, RecordFormat.RAW, "test-topic", 0, System.currentTimeMillis(), Instant.now(), Instant.now());
        }

        assertEquals(100, checker.getTotalSent());
        assertEquals(100, checker.getTotalReceived());
        assertEquals(0, checker.getDuplicateCount());

        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(0, loss.totalLost());
    }

    /**
     * Correctness tracking accumulates across warmup and test phases without
     * reset. Sequences are monotonically increasing throughout the entire run.
     * This avoids bugs caused by resetting sequences while messages are in-flight.
     */
    @Test
    public void accumulatesAcrossWarmupAndTestWithoutReset() {
        int producerId = 0;

        // Phase 1: warmup — send and receive 50 messages (seq 0-49)
        for (long seq = 0; seq < 50; seq++) {
            byte[] payload = new byte[64];
            CorrectnessPayload.encode(payload, producerId, seq,
                    System.currentTimeMillis(), false);
            checker.messageSent(producerId, seq, "test-topic");
            tracker.messageReceived(payload, RecordFormat.RAW, "test-topic", 0, System.currentTimeMillis(), Instant.now(), Instant.now());
        }

        // Phase 2: test — sequences continue from 50 (NO reset)
        for (long seq = 50; seq < 150; seq++) {
            byte[] payload = new byte[64];
            CorrectnessPayload.encode(payload, producerId, seq,
                    System.currentTimeMillis(), false);
            checker.messageSent(producerId, seq, "test-topic");
            tracker.messageReceived(payload, RecordFormat.RAW, "test-topic", 0, System.currentTimeMillis(), Instant.now(), Instant.now());
        }

        // Totals include both warmup and test
        assertEquals(150, checker.getTotalSent());
        assertEquals(150, checker.getTotalReceived());
        assertEquals(0, checker.getDuplicateCount());

        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(0, loss.totalLost());
    }

    /**
     * Interleaving probes with correctness messages: probes before and during
     * the test must not affect correctness results.
     */
    @Test
    public void probesInterleavedWithCorrectnessMessages() {
        int producerId = 0;
        byte[] probe = new byte[24]; // all zeros — MessageType.NORMAL

        // Probe before test
        tracker.messageReceived(probe, RecordFormat.RAW, "test-topic", 0, System.currentTimeMillis(), Instant.now(), Instant.now());

        // Real correctness messages
        for (long seq = 0; seq < 10; seq++) {
            byte[] payload = new byte[64];
            CorrectnessPayload.encode(payload, producerId, seq,
                    System.currentTimeMillis(), true);
            checker.messageSent(producerId, seq, "test-topic");
            tracker.messageReceived(payload, RecordFormat.RAW, "test-topic", 0, System.currentTimeMillis(), Instant.now(), Instant.now());
        }

        // More probes during test
        tracker.messageReceived(probe, RecordFormat.RAW, "test-topic", 0, System.currentTimeMillis(), Instant.now(), Instant.now());
        tracker.messageReceived(probe, RecordFormat.RAW, "test-topic", 1, System.currentTimeMillis(), Instant.now(), Instant.now());

        assertEquals(10, checker.getTotalSent());
        assertEquals(10, checker.getTotalReceived());
        assertEquals(0, checker.getDuplicateCount());

        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(0, loss.totalLost());
    }

    /**
     * CRC failures are detected through the full pipeline.
     */
    @Test
    public void crcFailureDetectedThroughTracker() {
        byte[] payload = new byte[64];
        CorrectnessPayload.encode(payload, 0, 0, System.currentTimeMillis(), true);
        checker.messageSent(0, 0, "test-topic");

        // Corrupt a data byte (after the type byte)
        payload[6] ^= 0xFF;

        tracker.messageReceived(payload, RecordFormat.RAW, "test-topic", 0, System.currentTimeMillis(), Instant.now(), Instant.now());

        assertEquals(1, checker.getCrcFailureCount());
    }

    /**
     * Multiple producers with probes — each producer's stream is independent.
     */
    @Test
    public void multipleProducersWithProbes() {
        byte[] probe = new byte[24];

        // Probes
        for (int i = 0; i < 3; i++) {
            tracker.messageReceived(probe, RecordFormat.RAW, "test-topic", i, System.currentTimeMillis(), Instant.now(), Instant.now());
        }

        // Producer 0: 10 messages
        for (long seq = 0; seq < 10; seq++) {
            byte[] payload = new byte[64];
            CorrectnessPayload.encode(payload, 0, seq,
                    System.currentTimeMillis(), false);
            checker.messageSent(0, seq, "test-topic");
            tracker.messageReceived(payload, RecordFormat.RAW, "test-topic", 0, System.currentTimeMillis(), Instant.now(), Instant.now());
        }

        // Producer 1: 10 messages, lose seq 5
        for (long seq = 0; seq < 10; seq++) {
            byte[] payload = new byte[64];
            CorrectnessPayload.encode(payload, 1, seq,
                    System.currentTimeMillis(), false);
            checker.messageSent(1, seq, "test-topic");
            if (seq != 5) {
                tracker.messageReceived(payload, RecordFormat.RAW, "test-topic", 1, System.currentTimeMillis(), Instant.now(), Instant.now());
            }
        }

        assertEquals(20, checker.getTotalSent());
        assertEquals(19, checker.getTotalReceived());

        ModelChecker.LossReport loss = checker.computeLoss();
        assertEquals(1, loss.totalLost());
        assertEquals(1, loss.ranges().get(0).producerId());
        assertEquals(5, loss.ranges().get(0).fromSequence());
    }

    /**
     * With a generated dataset the correctness fields are the record's
     * _dimster header; the tracker must find them through the record format
     * and still detect a corrupted header.
     */
    @Test
    public void datasetRecordsAreTrackedThroughTheirHeader() {
        for (RecordFormat format : new RecordFormat[]{RecordFormat.AVRO, RecordFormat.JSON}) {
            setUp();
            RowGenerator rows = datasetRows(format);
            for (long seq = 0; seq < 50; seq++) {
                byte[] header = CorrectnessPayload.header(7, seq, System.currentTimeMillis(), true);
                checker.messageSent(7, seq, "test-topic");
                tracker.messageReceived(rows.next(header), format, "test-topic", 0,
                        System.currentTimeMillis(), Instant.now(), Instant.now());
            }
            assertEquals(format + " received", 50, checker.getTotalReceived());
            assertEquals(0, checker.computeLoss().totalLost());

            // Normal and probe records of the same topic are not correctness messages.
            tracker.messageReceived(rows.next(new byte[]{MessageType.NORMAL}), format, "test-topic", 0,
                    System.currentTimeMillis(), Instant.now(), Instant.now());
            tracker.messageReceived(rows.next(new byte[]{MessageType.PROBE}), format, "test-topic", 0,
                    System.currentTimeMillis(), Instant.now(), Instant.now());
            assertEquals(50, checker.getTotalReceived());
            assertEquals(1, stats.getProbesReceived());
        }
    }

    @Test
    public void corruptedDatasetHeaderFailsTheCrc() {
        byte[] header = CorrectnessPayload.header(1, 0, System.currentTimeMillis(), true);
        byte[] record = datasetRows(RecordFormat.AVRO).next(header);
        // Avro: byte 0 is the header length, the header starts at byte 1;
        // flip a bit of the sequence field.
        record[1 + 6] ^= 0x01;
        assertFalse(CorrectnessPayload.decode(RecordFormat.AVRO.header(record)).crcValid());
    }

    private static RowGenerator datasetRows(RecordFormat format) {
        DatasetSpec spec = new DatasetSpec();
        spec.preset = "orders";
        spec.format = format;
        return new DatasetGenerator(DatasetResolver.resolve(spec)).newRowGenerator("producer-0", 0);
    }
}
