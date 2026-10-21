package dev.dimster.datagen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static dev.dimster.datagen.RecordFormatTest.col;
import static dev.dimster.datagen.RecordFormatTest.columns;
import static dev.dimster.datagen.RecordFormatTest.spec;
import static org.junit.Assert.*;

/**
 * Checks the statistical and determinism properties that the data generator
 * promises: global cardinality, distributions, null rates, run lengths,
 * sequences and keys. Rows are generated as JSON and parsed back, which keeps
 * the assertions about values rather than about bytes.
 */
public class RowGeneratorTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final byte[] HEADER = {0};

    @Test
    public void normalPayloadRecordsArePrebuiltAndShared() {
        DatasetGenerator generator = new DatasetGenerator(DatasetResolver.payloadDataset(99, 0.5, 4, null));
        byte[] normal = {dev.dimster.MessageType.NORMAL};
        Map<String, byte[]> byContent = new HashMap<>();
        for (String producer : List.of("producer-1", "producer-2")) {
            RowGenerator gen = generator.newRowGenerator(producer, 0);
            for (int i = 0; i < 100; i++) {
                byte[] record = gen.next(normal);
                byte[] first = byContent.computeIfAbsent(java.util.Arrays.toString(record), k -> record);
                // No per-record allocation: equal records are the same array, across producers too.
                assertSame(first, record);
            }
        }
        assertEquals(4, byContent.size());

        // Probe and correctness records are built per row: the header, then
        // one of the same payloads.
        byte[] probe = generator.newRowGenerator("producer-1", 0).next(new byte[]{dev.dimster.MessageType.PROBE});
        assertEquals(dev.dimster.MessageType.PROBE, probe[0]);
        assertEquals(100, probe.length);
        Set<String> payloads = new HashSet<>();
        for (byte[] record : byContent.values()) {
            payloads.add(java.util.Arrays.toString(java.util.Arrays.copyOfRange(record, 1, record.length)));
        }
        byte[] header = dev.dimster.engines.correctness.CorrectnessPayload.header(1, 7, 0, false);
        RowGenerator gen = generator.newRowGenerator("producer-1", 0);
        byte[] a = gen.next(header);
        byte[] b = gen.next(header);
        assertNotSame(a, b);
        assertEquals(header.length + 99, a.length);
        assertArrayEquals(header, java.util.Arrays.copyOf(a, header.length));
        assertTrue(payloads.contains(java.util.Arrays.toString(
                java.util.Arrays.copyOfRange(a, header.length, a.length))));
    }

    @Test
    public void sameSeedAndProducerGiveTheSameRows() {
        // TIMESTAMP columns read the wall clock, so this dataset has none.
        ResolvedDataset dataset = DatasetResolver.resolve(spec(RecordFormat.AVRO, columns(
                "name", col(ColumnSpec.Type.STRING, c -> { c.kind = ColumnSpec.Kind.FULL_NAME; c.cardinality = 1000L; }),
                "text", col(ColumnSpec.Type.STRING, c -> c.kind = ColumnSpec.Kind.SENTENCE),
                "n", col(ColumnSpec.Type.LONG, c -> c.cardinality = 50L),
                "x", col(ColumnSpec.Type.DOUBLE, c -> c.nullRate = 0.2))));
        RowGenerator a = new DatasetGenerator(dataset).newRowGenerator("producer-7", 7);
        RowGenerator b = new DatasetGenerator(dataset).newRowGenerator("producer-7", 7);
        RowGenerator otherProducer = new DatasetGenerator(dataset).newRowGenerator("producer-8", 8);
        int differences = 0;
        for (int i = 0; i < 500; i++) {
            byte[] row = a.next(HEADER);
            assertArrayEquals(row, b.next(HEADER));
            if (!java.util.Arrays.equals(row, otherProducer.next(HEADER))) {
                differences++;
            }
        }
        assertTrue("different producers draw different streams", differences > 450);
    }

    @Test
    public void cardinalityIsGlobalAcrossProducers() throws Exception {
        ResolvedDataset dataset = DatasetResolver.resolve(spec(RecordFormat.JSON, columns(
                "name", col(ColumnSpec.Type.STRING, c -> { c.kind = ColumnSpec.Kind.FULL_NAME; c.cardinality = 300L; }),
                "id", col(ColumnSpec.Type.LONG, c -> c.cardinality = 300L))));
        DatasetGenerator generator = new DatasetGenerator(dataset);
        Set<String> names = new HashSet<>();
        Set<Long> ids = new HashSet<>();
        for (int producer = 0; producer < 4; producer++) {
            RowGenerator gen = generator.newRowGenerator("producer-" + producer, producer);
            for (int i = 0; i < 5000; i++) {
                JsonNode row = JSON.readTree(gen.next(HEADER));
                names.add(row.get("name").asText());
                ids.add(row.get("id").asLong());
            }
        }
        // 20k draws over 300 ranks reach every rank; names may collide, ids may not.
        assertEquals(300, ids.size());
        assertTrue("distinct names " + names.size(), names.size() <= 300 && names.size() >= 290);
    }

    @Test
    public void scrambledIntegersArePermutationsOfTheDomain() {
        ResolvedDataset dataset = DatasetResolver.resolve(spec(RecordFormat.JSON, columns(
                "zone", col(ColumnSpec.Type.INT, c -> { c.min = 1.0; c.max = 265.0; c.cardinality = 265L; }))));
        ColumnGenerator.CategoricalIntegral zone = new ColumnGenerator.CategoricalIntegral(
                dataset.columns().get(0), Hashing.columnSeed(0, "zone"));
        Set<Long> values = new HashSet<>();
        for (long rank = 0; rank < 265; rank++) {
            long v = zone.valueFor(rank);
            assertTrue(v >= 1 && v <= 265);
            values.add(v);
        }
        assertEquals(265, values.size());
        assertNotEquals("scrambled", 1L, zone.valueFor(0));
    }

    @Test
    public void zipfConcentratesOnTheFirstRank() throws Exception {
        ResolvedDataset dataset = DatasetResolver.resolve(spec(RecordFormat.JSON, columns(
                "last", col(ColumnSpec.Type.STRING, c -> {
                    c.kind = ColumnSpec.Kind.LAST_NAME;
                    c.cardinality = 1000L;
                    c.distribution = ColumnSpec.Distribution.ZIPF;
                    c.skew = 1.0;
                }))));
        Map<String, Integer> counts = count(dataset, "last", 50_000);
        // P(rank 0) = 1 / H(1000) ≈ 0.134 for skew 1; rank 0 of the
        // frequency-ordered dictionary is "Smith".
        double share = counts.get("Smith") / 50_000.0;
        assertEquals(0.134, share, 0.01);
    }

    @Test
    public void weightedValuesFollowTheirWeights() throws Exception {
        ResolvedDataset dataset = DatasetResolver.resolve(spec(RecordFormat.JSON, columns(
                "device", col(ColumnSpec.Type.STRING, c -> {
                    c.values = List.of("mobile", "desktop", "tablet");
                    c.weights = List.of(6.0, 3.0, 1.0);
                }))));
        Map<String, Integer> counts = count(dataset, "device", 50_000);
        assertEquals(0.6, counts.get("mobile") / 50_000.0, 0.01);
        assertEquals(0.3, counts.get("desktop") / 50_000.0, 0.01);
        assertEquals(0.1, counts.get("tablet") / 50_000.0, 0.01);
    }

    @Test
    public void nullRateIsRespected() throws Exception {
        ResolvedDataset dataset = DatasetResolver.resolve(spec(RecordFormat.JSON, columns(
                "ref", col(ColumnSpec.Type.STRING, c -> { c.kind = ColumnSpec.Kind.URL; c.nullRate = 0.3; }))));
        Map<String, Integer> counts = count(dataset, "ref", 20_000);
        assertEquals(0.3, counts.getOrDefault("null", 0) / 20_000.0, 0.02);
    }

    @Test
    public void runLengthHoldsEachValue() throws Exception {
        ResolvedDataset dataset = DatasetResolver.resolve(spec(RecordFormat.JSON, columns(
                "bucket", col(ColumnSpec.Type.LONG, c -> { c.cardinality = 1_000_000L; c.runLength = 50; }))));
        RowGenerator gen = new DatasetGenerator(dataset).newRowGenerator("producer-1", 1);
        List<Long> values = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            values.add(JSON.readTree(gen.next(HEADER)).get("bucket").asLong());
        }
        for (int run = 0; run < 20; run++) {
            for (int i = 1; i < 50; i++) {
                assertEquals(values.get(run * 50), values.get(run * 50 + i));
            }
        }
        assertEquals(20, new HashSet<>(values).size(), 1);
    }

    @Test
    public void sequencesAreMonotonicAndUniqueAcrossProducers() throws Exception {
        ResolvedDataset dataset = DatasetResolver.resolve(spec(RecordFormat.JSON, columns(
                "id", col(ColumnSpec.Type.SEQUENCE, c -> {}))));
        DatasetGenerator generator = new DatasetGenerator(dataset);
        Set<Long> all = new HashSet<>();
        for (int producer = 0; producer < 3; producer++) {
            RowGenerator gen = generator.newRowGenerator("producer-" + producer, producer);
            long previous = -1;
            for (int i = 0; i < 1000; i++) {
                long id = JSON.readTree(gen.next(HEADER)).get("id").asLong();
                assertTrue(id > previous);
                previous = id;
                all.add(id);
            }
        }
        assertEquals(3000, all.size());
    }

    @Test
    public void rangesStayWithinBoundsAndScaleRounds() throws Exception {
        ResolvedDataset dataset = DatasetResolver.resolve(spec(RecordFormat.JSON, columns(
                "price", col(ColumnSpec.Type.DOUBLE, c -> { c.min = 1.0; c.max = 500.0; c.scale = 2; }),
                "dist", col(ColumnSpec.Type.DOUBLE, c -> { c.min = 0.0; c.max = 12.0; c.distribution = ColumnSpec.Distribution.NORMAL; }),
                "age", col(ColumnSpec.Type.INT, c -> { c.min = 18.0; c.max = 90.0; }))));
        RowGenerator gen = new DatasetGenerator(dataset).newRowGenerator("producer-1", 1);
        double distSum = 0;
        for (int i = 0; i < 5000; i++) {
            JsonNode row = JSON.readTree(gen.next(HEADER));
            double price = row.get("price").asDouble();
            assertTrue(price >= 1 && price <= 500);
            assertEquals(price, Math.round(price * 100) / 100.0, 0);
            double dist = row.get("dist").asDouble();
            assertTrue(dist >= 0 && dist <= 12);
            distSum += dist;
            int age = row.get("age").asInt();
            assertTrue(age >= 18 && age <= 90);
        }
        assertEquals(6.0, distSum / 5000, 0.2);
    }

    @Test
    public void keyIsTheKeyColumnValueOfTheLastRow() throws Exception {
        DatasetSpec spec = new DatasetSpec();
        spec.preset = "clicks";
        spec.format = RecordFormat.JSON;
        spec.keyColumn = "user_id";
        RowGenerator gen = new DatasetGenerator(DatasetResolver.resolve(spec)).newRowGenerator("producer-1", 1);
        for (int i = 0; i < 100; i++) {
            JsonNode row = JSON.readTree(gen.next(HEADER));
            assertEquals(row.get("user_id").asText(), gen.key());
        }
        RowGenerator noKey = new DatasetGenerator(RecordFormatTest.resolve("clicks", RecordFormat.JSON))
                .newRowGenerator("producer-1", 1);
        noKey.next(HEADER);
        assertNull(noKey.key());
    }

    private static Map<String, Integer> count(ResolvedDataset dataset, String column, int rows) throws Exception {
        RowGenerator gen = new DatasetGenerator(dataset).newRowGenerator("producer-1", 1);
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < rows; i++) {
            JsonNode v = JSON.readTree(gen.next(HEADER)).get(column);
            counts.merge(v.isNull() ? "null" : v.asText(), 1, Integer::sum);
        }
        return counts;
    }
}
