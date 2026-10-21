package dev.dimster.datagen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dimster.MessageType;
import dev.dimster.engines.correctness.CorrectnessPayload;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Checks that dataset records are valid in their format and that Dimster can
 * find its header in them again. Avro records are decoded with the Apache
 * Avro reference implementation against {@link ResolvedDataset#avroSchema()},
 * so the hand-written encoder is held to the specification rather than to
 * itself; JSON records are parsed with Jackson.
 */
public class RecordFormatTest {

    private static final List<String> PRESETS = List.of("clicks", "orders", "taxi");
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    public void avroRecordsDecodeWithTheReferenceImplementation() throws Exception {
        for (String preset : PRESETS) {
            ResolvedDataset dataset = resolve(preset, RecordFormat.AVRO);
            Schema schema = new Schema.Parser().parse(dataset.avroSchema());
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            RowGenerator gen = new DatasetGenerator(dataset).newRowGenerator("producer-1", 1);
            byte[] header = {MessageType.NORMAL};
            for (int i = 0; i < 2000; i++) {
                byte[] value = gen.next(header);
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(value, null);
                GenericRecord record = reader.read(null, decoder);
                assertTrue(preset + ": trailing bytes after record " + i, decoder.isEnd());
                assertArrayEquals(header, bytes((ByteBuffer) record.get(ResolvedDataset.HEADER_FIELD)));
                assertEquals(schema.getFields().size(), dataset.columns().size() + 1);
            }
        }
    }

    @Test
    public void avroEncodesEachTypeAsTheSchemaSays() throws Exception {
        ResolvedDataset dataset = DatasetResolver.resolve(spec(RecordFormat.AVRO, columns(
                "l", col(ColumnSpec.Type.LONG, c -> { c.min = -5.0; c.max = -5.0; }),
                "i", col(ColumnSpec.Type.INT, c -> { c.min = 300.0; c.max = 300.0; }),
                "d", col(ColumnSpec.Type.DOUBLE, c -> { c.min = 2.5; c.max = 2.5; }),
                "b", col(ColumnSpec.Type.BOOLEAN, c -> c.trueRate = 1.0),
                "s", col(ColumnSpec.Type.STRING, c -> c.values = List.of("héllo")),
                "n", col(ColumnSpec.Type.LONG, c -> c.nullRate = 0.5))));
        Schema schema = new Schema.Parser().parse(dataset.avroSchema());
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        RowGenerator gen = new DatasetGenerator(dataset).newRowGenerator("producer-1", 1);
        boolean sawNull = false;
        boolean sawValue = false;
        for (int i = 0; i < 200; i++) {
            GenericRecord r = reader.read(null, DecoderFactory.get().binaryDecoder(gen.next(new byte[]{0}), null));
            assertEquals(-5L, r.get("l"));
            assertEquals(300, r.get("i"));
            assertEquals(2.5, (double) r.get("d"), 0);
            assertEquals(true, r.get("b"));
            assertEquals("héllo", r.get("s").toString());
            sawNull |= r.get("n") == null;
            sawValue |= r.get("n") != null;
        }
        assertTrue("nullable column produced both nulls and values", sawNull && sawValue);
    }

    @Test
    public void bytesColumnsAreAvroBytesAndJsonHex() throws Exception {
        LinkedHashMap<String, ColumnSpec> cols = columns(
                "y", col(ColumnSpec.Type.BYTES, c -> { c.length = List.of(8, 8); c.randomRatio = 0.5; c.cardinality = 3L; }),
                "ny", col(ColumnSpec.Type.BYTES, c -> { c.length = List.of(2, 6); c.nullRate = 0.5; }));

        ResolvedDataset avro = DatasetResolver.resolve(spec(RecordFormat.AVRO, cols));
        GenericDatumReader<GenericRecord> reader =
                new GenericDatumReader<>(new Schema.Parser().parse(avro.avroSchema()));
        RowGenerator gen = new DatasetGenerator(avro).newRowGenerator("producer-1", 1);
        java.util.Set<String> distinct = new java.util.HashSet<>();
        boolean sawNull = false;
        for (int i = 0; i < 500; i++) {
            GenericRecord r = reader.read(null, DecoderFactory.get().binaryDecoder(gen.next(new byte[]{0}), null));
            byte[] y = bytes((ByteBuffer) r.get("y"));
            assertEquals(8, y.length);
            // round(8 * 0.5) = 4 random bytes, then zeros.
            assertArrayEquals(new byte[4], Arrays.copyOfRange(y, 4, 8));
            distinct.add(Arrays.toString(y));
            Object ny = r.get("ny");
            sawNull |= ny == null;
            if (ny != null) {
                int length = bytes((ByteBuffer) ny).length;
                assertTrue("length " + length, length >= 2 && length <= 6);
            }
        }
        assertTrue("cardinality 3: " + distinct.size(), distinct.size() <= 3);
        assertTrue(sawNull);

        ResolvedDataset json = DatasetResolver.resolve(spec(RecordFormat.JSON, cols));
        JsonNode node = JSON.readTree(new DatasetGenerator(json).newRowGenerator("producer-1", 1)
                .next(new byte[]{0}));
        String hex = node.get("y").asText();
        assertTrue(hex, hex.matches("[0-9a-f]{8}00000000"));
    }

    @Test
    public void payloadRecordsAreTheHeaderThenPayloadsSharedByAllProducers() {
        ResolvedDataset payload = DatasetResolver.payloadDataset(99, 0.5, 5, null);
        DatasetGenerator generator = new DatasetGenerator(payload);
        java.util.Set<String> distinct = new java.util.HashSet<>();
        for (String producer : List.of("producer-1", "producer-2")) {
            RowGenerator gen = generator.newRowGenerator(producer, 0);
            for (int i = 0; i < 200; i++) {
                byte[] value = gen.next(new byte[]{MessageType.NORMAL});
                assertEquals(100, value.length);
                assertEquals(MessageType.NORMAL, RecordFormat.RAW.typeOf(value));
                distinct.add(Arrays.toString(value));
            }
        }
        // Payloads are a function of their rank, so both producers draw from the same 5.
        assertEquals(5, distinct.size());
    }

    @Test
    public void jsonRecordsParseWithHeaderFirst() throws Exception {
        for (String preset : PRESETS) {
            ResolvedDataset dataset = resolve(preset, RecordFormat.JSON);
            RowGenerator gen = new DatasetGenerator(dataset).newRowGenerator("producer-1", 1);
            for (int i = 0; i < 2000; i++) {
                JsonNode node = JSON.readTree(gen.next(new byte[]{MessageType.NORMAL}));
                Iterator<String> names = node.fieldNames();
                assertEquals(ResolvedDataset.HEADER_FIELD, names.next());
                assertEquals("00", node.get(ResolvedDataset.HEADER_FIELD).asText());
                for (ResolvedDataset.Column c : dataset.columns()) {
                    assertEquals(preset + " column order", c.name(), names.next());
                    JsonNode v = node.get(c.name());
                    if (v.isNull()) {
                        assertTrue(c.name() + " is null but not nullable", c.nullable());
                    } else if (c.type() == ColumnSpec.Type.STRING) {
                        assertTrue(v.isTextual());
                    } else if (c.type() == ColumnSpec.Type.BOOLEAN) {
                        assertTrue(v.isBoolean());
                    } else {
                        assertTrue(c.name() + " should be numeric: " + v, v.isNumber());
                    }
                }
                assertFalse(names.hasNext());
            }
        }
    }

    @Test
    public void jsonEscapesQuotesBackslashesAndControlCharacters() throws Exception {
        String tricky = "a\"b\\c\nd\u0001é";
        ResolvedDataset dataset = DatasetResolver.resolve(spec(RecordFormat.JSON, columns(
                "s", col(ColumnSpec.Type.STRING, c -> c.values = List.of(tricky)))));
        JsonNode node = JSON.readTree(new DatasetGenerator(dataset).newRowGenerator("producer-1", 1).next(new byte[]{0}));
        assertEquals(tricky, node.get("s").asText());
    }

    @Test
    public void typeAndHeaderAreFoundInEveryFormat() {
        byte[] correctness = new byte[CorrectnessPayload.HEADER_WITH_CRC_SIZE];
        CorrectnessPayload.encode(correctness, 9, 1234L, 5678L, true);
        byte[] correctnessNoCrc = new byte[CorrectnessPayload.HEADER_SIZE];
        CorrectnessPayload.encode(correctnessNoCrc, 9, 1234L, 5678L, false);
        List<byte[]> headers = List.of(new byte[]{MessageType.NORMAL}, new byte[]{MessageType.PROBE},
                correctness, correctnessNoCrc);

        for (RecordFormat format : List.of(RecordFormat.AVRO, RecordFormat.JSON)) {
            RowGenerator gen = new DatasetGenerator(resolve("orders", format)).newRowGenerator("producer-3", 3);
            for (byte[] header : headers) {
                byte[] value = gen.next(header);
                assertEquals(format + " type", header[0], format.typeOf(value));
                assertArrayEquals(format + " header", header, format.header(value));
            }
            var decoded = CorrectnessPayload.decode(format.header(gen.next(correctness)));
            assertEquals(9, decoded.producerId());
            assertEquals(1234L, decoded.sequence());
            assertTrue(decoded.crcValid());
        }

        // RAW is today's pool layout: header bytes, then payload.
        byte[] raw = Arrays.copyOf(correctness, 100);
        assertEquals(MessageType.CORRECTNESS, RecordFormat.RAW.typeOf(raw));
        assertEquals(1234L, CorrectnessPayload.decode(RecordFormat.RAW.header(raw)).sequence());
    }

    @Test
    public void malformedValuesHaveNoType() {
        for (RecordFormat format : RecordFormat.values()) {
            assertEquals(-1, format.typeOf(new byte[0]));
            assertEquals(-1, format.typeOf(null));
        }
        assertEquals(-1, RecordFormat.AVRO.typeOf(new byte[]{0, 0}));
        assertEquals(-1, RecordFormat.JSON.typeOf("{\"x\":1}".getBytes()));
        assertEquals(0, RecordFormat.AVRO.header(new byte[]{40, 1}).length);
    }

    // ─── Helpers ─────────────────────────────────────────────────────────

    static ResolvedDataset resolve(String preset, RecordFormat format) {
        DatasetSpec spec = new DatasetSpec();
        spec.preset = preset;
        spec.format = format;
        return DatasetResolver.resolve(spec);
    }

    static DatasetSpec spec(RecordFormat format, LinkedHashMap<String, ColumnSpec> columns) {
        DatasetSpec spec = new DatasetSpec();
        spec.format = format;
        spec.columns = columns;
        return spec;
    }

    static LinkedHashMap<String, ColumnSpec> columns(Object... nameAndSpec) {
        LinkedHashMap<String, ColumnSpec> map = new LinkedHashMap<>();
        for (int i = 0; i < nameAndSpec.length; i += 2) {
            map.put((String) nameAndSpec[i], (ColumnSpec) nameAndSpec[i + 1]);
        }
        return map;
    }

    static ColumnSpec col(ColumnSpec.Type type, java.util.function.Consumer<ColumnSpec> config) {
        ColumnSpec c = new ColumnSpec();
        c.type = type;
        config.accept(c);
        return c;
    }

    private static byte[] bytes(ByteBuffer buffer) {
        byte[] out = new byte[buffer.remaining()];
        buffer.duplicate().get(out);
        return out;
    }
}
