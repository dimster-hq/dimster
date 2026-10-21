package dev.dimster.datagen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;

import java.util.List;
import java.util.function.Consumer;

import static dev.dimster.datagen.RecordFormatTest.col;
import static dev.dimster.datagen.RecordFormatTest.columns;
import static dev.dimster.datagen.RecordFormatTest.spec;
import static org.junit.Assert.*;

/** Checks preset loading, merging, defaults and validation errors. */
public class DatasetResolverTest {

    @Test
    public void presetsResolveWithDefaults() {
        for (String preset : List.of("clicks", "orders", "taxi")) {
            DatasetSpec spec = new DatasetSpec();
            spec.preset = preset;
            ResolvedDataset d = DatasetResolver.resolve(spec);
            assertEquals(RecordFormat.AVRO, d.format());
            assertEquals(0L, d.seed());
            assertFalse(d.columns().isEmpty());
        }
    }

    @Test
    public void inlineColumnsOverridePresetFieldsByNameAndAppendNewColumns() {
        DatasetSpec spec = new DatasetSpec();
        spec.preset = "clicks";
        spec.columns = columns(
                "country", col(null, c -> c.cardinality = 10L),
                "extra", col(ColumnSpec.Type.BOOLEAN, c -> {}));
        ResolvedDataset d = DatasetResolver.resolve(spec);
        ResolvedDataset.Column country = find(d, "country");
        assertEquals(10L, country.cardinality());
        // Untouched fields keep the preset's values.
        assertEquals(ColumnSpec.Kind.COUNTRY, country.kind());
        assertEquals(ColumnSpec.Distribution.ZIPF, country.distribution());
        assertEquals("extra", d.columns().get(d.columns().size() - 1).name());
        assertEquals("event_id", d.columns().get(0).name());
    }

    @Test
    public void dictionaryKindsDefaultToTheWholeDictionaryAndOthersToUnbounded() {
        ResolvedDataset d = DatasetResolver.resolve(spec(RecordFormat.AVRO, columns(
                "city", col(ColumnSpec.Type.STRING, c -> c.kind = ColumnSpec.Kind.CITY),
                "text", col(ColumnSpec.Type.STRING, c -> c.kind = ColumnSpec.Kind.SENTENCE),
                "code", col(ColumnSpec.Type.STRING, c -> {}))));
        assertEquals(Dictionary.CITIES.size(), find(d, "city").cardinality());
        assertEquals(0L, find(d, "text").cardinality());
        assertEquals(ColumnSpec.Kind.ALPHANUMERIC, find(d, "code").kind());
    }

    @Test
    public void resolvedDatasetsSurviveJsonRoundTrip() throws Exception {
        // Workers receive the resolved dataset as JSON.
        DatasetSpec spec = new DatasetSpec();
        spec.preset = "orders";
        spec.keyColumn = "customer_id";
        ResolvedDataset d = DatasetResolver.resolve(spec);
        ObjectMapper mapper = new ObjectMapper();
        ResolvedDataset copy = mapper.readValue(mapper.writeValueAsBytes(d), ResolvedDataset.class);
        assertEquals(d, copy);
    }

    @Test
    public void workloadYamlUsesUppercaseEnumsAndRejectsUnknownFields() throws Exception {
        ObjectMapper yaml = new ObjectMapper(new YAMLFactory())
                .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        DatasetSpec spec = yaml.readValue("""
                preset: clicks
                format: JSON
                columns:
                  country: {cardinality: 5}
                """, DatasetSpec.class);
        assertEquals(RecordFormat.JSON, spec.format);
        assertEquals(5L, (long) spec.columns.get("country").cardinality);
        try {
            yaml.readValue("columns: {x: {type: LONG, cardinalty: 5}}", DatasetSpec.class);
            fail("typo should be rejected");
        } catch (Exception expected) {
            // FAIL_ON_UNKNOWN_PROPERTIES, as in BenchmarkRunner.
        }
    }

    @Test
    public void invalidDatasetsAreRejectedWithTheColumnName() {
        assertRejected("type is required", "x", null, c -> {});
        assertRejected("reserved", "_dimster_x", ColumnSpec.Type.LONG, c -> {});
        assertRejected("must match", "bad-name", ColumnSpec.Type.LONG, c -> {});
        assertRejected("ZIPF requires a cardinality", "x", ColumnSpec.Type.LONG,
                c -> c.distribution = ColumnSpec.Distribution.ZIPF);
        assertRejected("ZIPF requires a cardinality", "x", ColumnSpec.Type.STRING,
                c -> { c.kind = ColumnSpec.Kind.UUID; c.distribution = ColumnSpec.Distribution.ZIPF; });
        assertRejected("exceeds", "x", ColumnSpec.Type.STRING,
                c -> { c.kind = ColumnSpec.Kind.CITY; c.cardinality = 10_000L; });
        assertRejected("exceeds", "x", ColumnSpec.Type.INT,
                c -> { c.min = 1.0; c.max = 6.0; c.cardinality = 7L; });
        assertRejected("does not apply", "x", ColumnSpec.Type.LONG, c -> c.kind = ColumnSpec.Kind.CITY);
        assertRejected("does not apply", "x", ColumnSpec.Type.STRING, c -> c.min = 1.0);
        assertRejected("weights must have one entry per value", "x", ColumnSpec.Type.STRING,
                c -> { c.values = List.of("a", "b"); c.weights = List.of(1.0); });
        assertRejected("can't be combined", "x", ColumnSpec.Type.STRING,
                c -> { c.values = List.of("a"); c.kind = ColumnSpec.Kind.CITY; });
        assertRejected("nullRate", "x", ColumnSpec.Type.LONG, c -> c.nullRate = 1.0);
        assertRejected("runLength", "x", ColumnSpec.Type.LONG, c -> c.runLength = 0);
        assertRejected("length must be", "x", ColumnSpec.Type.STRING, c -> c.length = List.of(5, 2));
        assertRejected("NORMAL", "x", ColumnSpec.Type.LONG,
                c -> { c.cardinality = 5L; c.distribution = ColumnSpec.Distribution.NORMAL; });
        assertRejected("whole numbers", "x", ColumnSpec.Type.LONG, c -> c.min = 1.5);
        assertRejected("randomRatio must be", "x", ColumnSpec.Type.BYTES, c -> c.randomRatio = 1.5);
        assertRejected("does not apply", "x", ColumnSpec.Type.STRING, c -> c.randomRatio = 0.5);
        assertRejected("ZIPF requires a cardinality", "x", ColumnSpec.Type.BYTES,
                c -> c.distribution = ColumnSpec.Distribution.ZIPF);
        assertRejected("length must be", "x", ColumnSpec.Type.BYTES, c -> c.length = List.of(0, 4));
    }

    @Test
    public void thePayloadDatasetIsOneRawBytesColumn() {
        ResolvedDataset payload = DatasetResolver.payloadDataset(999, 0.25, 50, null);
        assertTrue(payload.isPayloadDataset());
        assertEquals(RecordFormat.RAW, payload.format());
        ResolvedDataset.Column c = payload.columns().get(0);
        assertEquals(DatasetResolver.PAYLOAD_COLUMN, c.name());
        assertEquals(ColumnSpec.Type.BYTES, c.type());
        assertEquals(999, c.minLength());
        assertEquals(999, c.maxLength());
        assertEquals(50, c.cardinality());
        assertEquals(0.25, c.randomRatio(), 0);

        // A payload file is the only value.
        byte[] file = new byte[999];
        assertEquals(1, DatasetResolver.payloadDataset(999, 0.25, 50, file).columns().get(0).cardinality());
        assertThrows(IllegalArgumentException.class, () -> DatasetResolver.payloadDataset(10, 0.25, 50, file));
        assertThrows(IllegalArgumentException.class, () -> DatasetResolver.payloadDataset(999, 2.0, 50, null));
        assertThrows(IllegalArgumentException.class, () -> DatasetResolver.payloadDataset(999, 0.5, 0, null));
    }

    @Test
    public void invalidDatasetLevelSettingsAreRejected() {
        DatasetSpec spec = new DatasetSpec();
        spec.preset = "clicks";
        spec.keyColumn = "nope";
        assertThrows(IllegalArgumentException.class, () -> DatasetResolver.resolve(spec));

        DatasetSpec raw = new DatasetSpec();
        raw.preset = "clicks";
        raw.format = RecordFormat.RAW;
        assertThrows(IllegalArgumentException.class, () -> DatasetResolver.resolve(raw));

        DatasetSpec unknown = new DatasetSpec();
        unknown.preset = "nope";
        assertThrows(IllegalArgumentException.class, () -> DatasetResolver.resolve(unknown));

        DatasetSpec traversal = new DatasetSpec();
        traversal.preset = "../dict/words";
        assertThrows(IllegalArgumentException.class, () -> DatasetResolver.resolve(traversal));

        assertThrows(IllegalArgumentException.class, () -> DatasetResolver.resolve(new DatasetSpec()));
    }

    private static void assertRejected(String message, String name, ColumnSpec.Type type, Consumer<ColumnSpec> config) {
        try {
            DatasetResolver.resolve(spec(RecordFormat.AVRO, columns(name, col(type, config))));
            fail("expected rejection containing '" + message + "'");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains(message));
            assertTrue(e.getMessage(), e.getMessage().contains("'" + name + "'"));
        }
    }

    private static ResolvedDataset.Column find(ResolvedDataset d, String name) {
        return d.columns().stream().filter(c -> c.name().equals(name)).findFirst().orElseThrow();
    }
}
