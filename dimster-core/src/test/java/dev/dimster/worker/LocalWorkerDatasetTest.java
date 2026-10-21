package dev.dimster.worker;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dimster.MessageType;
import dev.dimster.datagen.DatasetResolver;
import dev.dimster.datagen.DatasetSpec;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.datagen.ResolvedDataset;
import dev.dimster.engines.correctness.CorrectnessConfig;
import dev.dimster.engines.correctness.CorrectnessPayload;
import dev.dimster.kafka.KafkaProducerWrapper;
import dev.dimster.spi.kafka.RawProducer;
import dev.dimster.utils.distributor.MessageDistributorType;
import dev.dimster.worker.commands.ProducerWorkAssignment;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.After;
import org.junit.Test;

/**
 * Drives LocalWorker's real producer loop against a recording fake producer
 * to check what reaches Kafka: generated records with the right header, keys
 * from the key column, correctness headers, probes, and the payload dataset
 * that workloads without a dataset use.
 */
public class LocalWorkerDatasetTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PRODUCER = "producer-topic-i3";

    private final LocalWorker worker = new LocalWorker(false);
    private final RecordingProducer raw = new RecordingProducer(200);

    @After
    public void stop() throws Exception {
        worker.removeProducers(List.of(PRODUCER));
        worker.close();
    }

    @Test
    public void datasetRecordsCarryANormalHeaderAndTheKeyColumnAsKey() throws Exception {
        worker.setDataset(dataset(RecordFormat.JSON, "user_id"));
        addProducer();
        worker.startLoad(new ProducerWorkAssignment(20_000,
                MessageDistributorType.KEY_COLUMN, 1, 1.0));
        assertTrue(raw.enough.await(5, TimeUnit.SECONDS));

        for (ProducerRecord<String, byte[]> record : raw.snapshot()) {
            JsonNode row = JSON.readTree(record.value());
            assertEquals("00", row.get("_dimster").asText());
            assertEquals(row.get("user_id").asText(), record.key());
        }
        // SEQUENCE values encode the producer's index within its topic (3).
        long eventId = JSON.readTree(raw.snapshot().get(0).value()).get("event_id").asLong();
        assertEquals(3L, eventId >>> 40);
    }

    @Test
    public void correctnessFieldsBecomeTheRecordHeader() throws Exception {
        CorrectnessConfig config = new CorrectnessConfig();
        config.enableCrc = true;
        worker.enableCorrectness(config);
        worker.setDataset(dataset(RecordFormat.AVRO, null));
        addProducer();
        worker.startLoad(new ProducerWorkAssignment(20_000,
                MessageDistributorType.NO_KEY, 1, 1.0));
        assertTrue(raw.enough.await(5, TimeUnit.SECONDS));

        long expected = 0;
        for (ProducerRecord<String, byte[]> record : raw.snapshot()) {
            assertEquals(MessageType.CORRECTNESS, RecordFormat.AVRO.typeOf(record.value()));
            var decoded = CorrectnessPayload.decode(RecordFormat.AVRO.header(record.value()));
            assertEquals(expected++, decoded.sequence());
            assertTrue(decoded.crcValid());
        }
    }

    @Test
    public void probesAreGeneratedRowsWithAProbeHeader() throws Exception {
        worker.setDataset(dataset(RecordFormat.AVRO, null));
        addProducer();
        worker.probeProducers();
        byte[] probe = raw.snapshot().get(0).value();
        assertEquals(MessageType.PROBE, RecordFormat.AVRO.typeOf(probe));
        assertTrue("a full row follows the header", probe.length > 50);
    }

    @Test
    public void payloadDatasetRecordsAreTheHeaderThenARandomPrefixAndZeros() throws Exception {
        // messageSize 100: a 1-byte header and 99 payload bytes, half random.
        worker.setDataset(DatasetResolver.payloadDataset(99, 0.5, 10, null));
        addProducer();
        worker.startLoad(new ProducerWorkAssignment(20_000, MessageDistributorType.NO_KEY, 1, 1.0));
        assertTrue(raw.enough.await(5, TimeUnit.SECONDS));

        Set<String> distinct = new HashSet<>();
        for (ProducerRecord<String, byte[]> record : raw.snapshot()) {
            byte[] value = record.value();
            assertEquals(100, value.length);
            assertEquals(MessageType.NORMAL, value[0]);
            // round(99 * 0.5) = 50 random bytes, then 49 zeros.
            assertArrayEquals(new byte[49], Arrays.copyOfRange(value, 51, 100));
            distinct.add(Arrays.toString(value));
        }
        assertTrue("at most the 10 pooled payloads: " + distinct.size(), distinct.size() <= 10);

        worker.probeProducers();
        byte[] probe = raw.snapshot().get(raw.snapshot().size() - 1).value();
        assertEquals(MessageType.PROBE, RecordFormat.RAW.typeOf(probe));
        assertEquals(100, probe.length);
    }

    @Test
    public void payloadFileContentFollowsTheHeader() throws Exception {
        byte[] file = "the payload file".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        CorrectnessConfig config = new CorrectnessConfig();
        config.enableCrc = false;
        worker.enableCorrectness(config);
        worker.setDataset(DatasetResolver.payloadDataset(file.length, 0.5, 100, file));
        addProducer();
        worker.startLoad(new ProducerWorkAssignment(20_000, MessageDistributorType.NO_KEY, 1, 1.0));
        assertTrue(raw.enough.await(5, TimeUnit.SECONDS));

        for (ProducerRecord<String, byte[]> record : raw.snapshot()) {
            byte[] value = record.value();
            // The correctness header precedes the payload rather than overwriting it.
            assertEquals(CorrectnessPayload.HEADER_SIZE + file.length, value.length);
            assertTrue(CorrectnessPayload.decode(RecordFormat.RAW.header(value)).producerId() >= 0);
            assertArrayEquals(file, Arrays.copyOfRange(value, CorrectnessPayload.HEADER_SIZE, value.length));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void aDatasetIsRequired() {
        worker.setDataset(null);
    }

    private void addProducer() throws Exception {
        KafkaProducerWrapper wrapper = new KafkaProducerWrapper(raw, "topic", PRODUCER, 3, new int[0], false);
        Field f = LocalWorker.class.getDeclaredField("producers");
        f.setAccessible(true);
        f.set(worker, new ArrayList<>(List.of(wrapper)));
    }

    private static ResolvedDataset dataset(RecordFormat format, String keyColumn) {
        DatasetSpec spec = new DatasetSpec();
        spec.preset = "clicks";
        spec.format = format;
        spec.keyColumn = keyColumn;
        return DatasetResolver.resolve(spec);
    }

    /** Records every sent record; never completes sends (no broker). */
    private static class RecordingProducer implements RawProducer<String, byte[]> {
        private final List<ProducerRecord<String, byte[]>> records = new CopyOnWriteArrayList<>();
        final CountDownLatch enough;

        RecordingProducer(int wanted) {
            this.enough = new CountDownLatch(wanted);
        }

        List<ProducerRecord<String, byte[]>> snapshot() {
            return new ArrayList<>(records);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<String, byte[]> record, Callback callback) {
            records.add(record);
            enough.countDown();
            return new CompletableFuture<>();
        }

        @Override public void initTransactions() {}
        @Override public void beginTransaction() {}
        @Override public void commitTransaction() {}
        @Override public void abortTransaction() {}
        @Override public Map<MetricName, ? extends Metric> metrics() { return Map.of(); }
        @Override public void close() {}
    }
}
