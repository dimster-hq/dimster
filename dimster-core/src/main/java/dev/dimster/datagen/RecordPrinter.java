package dev.dimster.datagen;

import dev.dimster.MessageType;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

/**
 * Renders record values as readable text for {@code engineConfig.printContents}.
 *
 * <ul>
 *   <li>JSON dataset records are printed as they are.</li>
 *   <li>Avro dataset records are decoded with the dataset's schema and
 *       printed in the same JSON form, so both formats read alike.</li>
 *   <li>RAW records (the payload dataset) are mostly random bytes, so only
 *       their message type, size and first bytes are shown.</li>
 * </ul>
 * Printing is only allowed at low rates (the engines force it off above
 * 50 msg/s), so rendering allocates freely and is thread-safe by building
 * everything per call.
 */
public final class RecordPrinter {

    /** Longer renderings are cut, so a single log line stays readable. */
    static final int MAX_CHARS = 2000;
    private static final int RAW_PREFIX_BYTES = 16;

    private final ResolvedDataset dataset;

    /** @param dataset the test point's dataset */
    public RecordPrinter(ResolvedDataset dataset) {
        this.dataset = dataset;
    }

    /** {@code key=<key> value=<rendered value>}, for appending to a log line. */
    public String describe(String key, byte[] value) {
        return "key=" + key + " value=" + render(value);
    }

    /**
     * The suffix producers and consumers append to their printMessages lines:
     * a space and {@link #describe}, or an empty string when printer is null
     * (engineConfig.printContents is off).
     */
    public static String logSuffix(RecordPrinter printer, String key, byte[] value) {
        return printer == null ? "" : " " + printer.describe(key, value);
    }

    /** The value as text; see the class comment for the per-format forms. */
    public String render(byte[] value) {
        if (value == null) {
            return "null";
        }
        String text;
        if (dataset.format() == RecordFormat.RAW) {
            text = renderRaw(value);
        } else if (dataset.format() == RecordFormat.JSON) {
            text = new String(value, StandardCharsets.UTF_8);
        } else {
            text = renderAvro(value);
        }
        return text.length() > MAX_CHARS ? text.substring(0, MAX_CHARS) + "...(truncated)" : text;
    }

    private String renderAvro(byte[] value) {
        JsonRecordWriter json = new JsonRecordWriter(dataset);
        try {
            AvroRecordReader.decode(value, dataset, json);
        } catch (IllegalArgumentException e) {
            return "<undecodable " + value.length + " bytes>";
        }
        return new String(json.finish(), StandardCharsets.UTF_8);
    }

    private static String renderRaw(byte[] value) {
        int n = Math.min(RAW_PREFIX_BYTES, value.length);
        return "<" + MessageType.nameOf(RecordFormat.RAW.typeOf(value)) + ", " + value.length + " bytes: "
                + HexFormat.of().formatHex(value, 0, n) + (value.length > n ? "...>" : ">");
    }
}
