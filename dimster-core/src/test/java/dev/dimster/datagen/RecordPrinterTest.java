package dev.dimster.datagen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dimster.MessageType;
import dev.dimster.engines.correctness.CorrectnessPayload;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static dev.dimster.datagen.RecordFormatTest.col;
import static dev.dimster.datagen.RecordFormatTest.columns;
import static dev.dimster.datagen.RecordFormatTest.spec;
import static org.junit.Assert.*;

/** Checks how printContents renders records in each format. */
public class RecordPrinterTest {

    private static final byte[] HEADER = {MessageType.NORMAL};
    private static final ResolvedDataset PAYLOAD = DatasetResolver.payloadDataset(99, 0.5, 1, null);

    /** Every column type, nullable ones included, and no wall-clock columns. */
    private static DatasetSpec allTypes(RecordFormat format) {
        return spec(format, columns(
                "id", col(ColumnSpec.Type.SEQUENCE, c -> {}),
                "user", col(ColumnSpec.Type.LONG, c -> { c.cardinality = 1000L; c.nullRate = 0.3; }),
                "zone", col(ColumnSpec.Type.INT, c -> { c.min = -5.0; c.max = 300.0; }),
                "price", col(ColumnSpec.Type.DOUBLE, c -> { c.min = 0.0; c.max = 1e6; c.scale = 2; c.nullRate = 0.2; }),
                "ratio", col(ColumnSpec.Type.DOUBLE, c -> {}),
                "gift", col(ColumnSpec.Type.BOOLEAN, c -> {}),
                "name", col(ColumnSpec.Type.STRING, c -> { c.kind = ColumnSpec.Kind.FULL_NAME; c.nullRate = 0.1; }),
                "quote", col(ColumnSpec.Type.STRING, c -> c.values = List.of("say \"hi\"", "a\\b", "é"))));
    }

    @Test
    public void avroRecordsPrintExactlyAsTheirJsonEncoding() {
        // The same seed and producer give the same rows in both formats.
        ResolvedDataset avro = DatasetResolver.resolve(allTypes(RecordFormat.AVRO));
        ResolvedDataset json = DatasetResolver.resolve(allTypes(RecordFormat.JSON));
        RowGenerator avroRows = new DatasetGenerator(avro).newRowGenerator("producer-0", 0);
        RowGenerator jsonRows = new DatasetGenerator(json).newRowGenerator("producer-0", 0);
        RecordPrinter printer = new RecordPrinter(avro);
        byte[] correctness = CorrectnessPayload.header(3, 42, 1000L, true);
        for (int i = 0; i < 2000; i++) {
            byte[] header = i % 2 == 0 ? HEADER : correctness;
            String expected = new String(jsonRows.next(header), StandardCharsets.UTF_8);
            assertEquals(expected, printer.render(avroRows.next(header)));
        }
    }

    @Test
    public void presetRecordsPrintAsValidJsonInBothFormats() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        for (String preset : List.of("clicks", "orders", "taxi")) {
            for (RecordFormat format : List.of(RecordFormat.AVRO, RecordFormat.JSON)) {
                ResolvedDataset dataset = RecordFormatTest.resolve(preset, format);
                RecordPrinter printer = new RecordPrinter(dataset);
                RowGenerator rows = new DatasetGenerator(dataset).newRowGenerator("producer-0", 0);
                for (int i = 0; i < 200; i++) {
                    JsonNode node = mapper.readTree(printer.render(rows.next(HEADER)));
                    assertEquals(preset + "/" + format, dataset.columns().size() + 1, node.size());
                }
            }
        }
    }

    @Test
    public void payloadRecordsShowTypeSizeAndFirstBytes() {
        RecordPrinter printer = new RecordPrinter(PAYLOAD);
        byte[] pooled = new byte[100];
        pooled[0] = MessageType.NORMAL;
        pooled[1] = (byte) 0xab;
        assertEquals("<normal, 100 bytes: 00ab0000000000000000000000000000...>", printer.render(pooled));
        byte[] probe = {MessageType.PROBE, 1};
        assertEquals("<probe, 2 bytes: 0301>", printer.render(probe));
        assertEquals("key=k1 value=<probe, 2 bytes: 0301>", printer.describe("k1", probe));
    }

    @Test
    public void undecodableAndOversizedValuesAreHandled() {
        ResolvedDataset avro = RecordFormatTest.resolve("clicks", RecordFormat.AVRO);
        RecordPrinter printer = new RecordPrinter(avro);
        assertEquals("<undecodable 3 bytes>", printer.render(new byte[]{2, 0, 5}));
        // A valid record with trailing garbage is not a record of this dataset.
        byte[] record = new DatasetGenerator(avro).newRowGenerator("producer-0", 0).next(HEADER);
        byte[] padded = java.util.Arrays.copyOf(record, record.length + 4);
        assertTrue(printer.render(padded).startsWith("<undecodable"));
        assertEquals("null", printer.render(null));

        ResolvedDataset longText = DatasetResolver.resolve(spec(RecordFormat.JSON, columns(
                "text", col(ColumnSpec.Type.STRING, c -> { c.kind = ColumnSpec.Kind.ALPHANUMERIC; c.length = List.of(5000, 5000); }))));
        String rendered = new RecordPrinter(longText)
                .render(new DatasetGenerator(longText).newRowGenerator("producer-0", 0).next(HEADER));
        assertEquals(RecordPrinter.MAX_CHARS + "...(truncated)".length(), rendered.length());
    }

    @Test
    public void logSuffixAddsContentsOnlyWithAPrinter() {
        byte[] probe = {MessageType.PROBE};
        assertEquals(" key=k value=<probe, 1 bytes: 03>",
                RecordPrinter.logSuffix(new RecordPrinter(PAYLOAD), "k", probe));
        assertEquals("", RecordPrinter.logSuffix(null, "k", probe));
    }
}
