package dev.dimster.datagen;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * A validated dataset with every default filled in: the contract between the
 * coordinator, which resolves a workload's {@link DatasetSpec} once per test
 * point, and the workers, which generate rows from it.
 *
 * <p>It is immutable and JSON-serializable so that it can be shipped to remote
 * workers as is. It also defines the record schema: the {@code _dimster}
 * header field followed by {@link #columns} in order.
 *
 * <p>Every test point has one. A workload without a {@code dataset} gets the
 * {@link #PAYLOAD_PRESET} dataset, a single BYTES column of messageSize - 1
 * bytes encoded as RAW, so its records are the header followed by opaque
 * payload bytes.
 *
 * @param preset    the preset it was built from, if any (for reporting only)
 * @param format    the record encoding: AVRO or JSON, or RAW for the payload dataset
 * @param seed      seed for rank-to-value mappings; identical across producers
 * @param keyColumn column whose value is the record key, or null
 * @param columns   the columns in schema order
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ResolvedDataset(String preset, RecordFormat format, long seed, String keyColumn,
                              List<Column> columns) {

    /** Name of the header field that every dataset record starts with. */
    public static final String HEADER_FIELD = "_dimster";

    /** Preset name of the default dataset, {@code {_dimster: bytes, payload: bytes}}. */
    public static final String PAYLOAD_PRESET = "payload";

    public ResolvedDataset {
        columns = List.copyOf(columns);
    }

    /**
     * True for the default payload dataset, whose size follows the
     * workload's messageSize rather than generated data.
     */
    @JsonIgnore
    public boolean isPayloadDataset() {
        return PAYLOAD_PRESET.equals(preset);
    }

    /** Index of {@link #keyColumn} in {@link #columns}, or -1 when there is none. */
    @JsonIgnore
    public int keyColumnIndex() {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(keyColumn)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The Avro schema of the records, as JSON. AVRO-encoded records follow it
     * exactly; it is also the schema a schema registry or the Kafka columnar
     * PoC would be given. Nullable columns are {@code ["null", T]} unions.
     */
    @JsonIgnore
    public String avroSchema() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"record\",\"name\":\"")
                .append(preset != null ? recordName(preset) : "DimsterRecord")
                .append("\",\"namespace\":\"dev.dimster.datagen\",\"fields\":[")
                .append("{\"name\":\"").append(HEADER_FIELD).append("\",\"type\":\"bytes\"}");
        for (Column c : columns) {
            String type = switch (c.type()) {
                case LONG, SEQUENCE -> "\"long\"";
                case TIMESTAMP -> "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}";
                case INT -> "\"int\"";
                case DOUBLE -> "\"double\"";
                case BOOLEAN -> "\"boolean\"";
                case STRING -> "\"string\"";
                case BYTES -> "\"bytes\"";
            };
            sb.append(",{\"name\":\"").append(c.name()).append("\",\"type\":");
            if (c.nullable()) {
                sb.append("[\"null\",").append(type).append("],\"default\":null}");
            } else {
                sb.append(type).append('}');
            }
        }
        return sb.append("]}").toString();
    }

    /** "clicks" -> "Clicks": Avro record names are conventionally capitalised. */
    private static String recordName(String preset) {
        return Character.toUpperCase(preset.charAt(0)) + preset.substring(1);
    }

    /**
     * One fully specified column. See {@link ColumnSpec} for what each field
     * means; here nothing is null except fields that don't apply to the type.
     *
     * @param cardinality number of distinct values; 0 means unbounded
     * @param scale       DOUBLE decimal places; -1 means no rounding
     * @param weights     STRING enum weights, normalised to sum to 1
     * @param randomRatio BYTES: fraction of each value that is random
     * @param fixedValue  BYTES: the column's only value, or null when values
     *                    are generated (the payload dataset's payloadFile)
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Column(String name,
                         ColumnSpec.Type type,
                         ColumnSpec.Kind kind,
                         List<String> values,
                         List<Double> weights,
                         long cardinality,
                         ColumnSpec.Distribution distribution,
                         double skew,
                         boolean scramble,
                         double min,
                         double max,
                         int scale,
                         int minLength,
                         int maxLength,
                         int minWords,
                         int maxWords,
                         double trueRate,
                         long jitterMs,
                         double nullRate,
                         int runLength,
                         double randomRatio,
                         byte[] fixedValue) {

        /** Nullable columns are encoded as unions (Avro) and may hold JSON nulls. */
        @JsonIgnore
        public boolean nullable() {
            return nullRate > 0;
        }

        /** True when values come from sampling a rank in [0, cardinality). */
        @JsonIgnore
        public boolean categorical() {
            return cardinality > 0;
        }
    }
}
