package dev.dimster.flink.runner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Builds the Flink SQL that wraps the workload's SELECT into a Kafka-to-Kafka
 * pipeline that Dimster consumers can read:
 *
 * <ul>
 *   <li>the {@code source} table mirrors the dataset's schema byte for byte
 *       (Dimster's Avro is schema-less binary, the same encoding as Flink's
 *       {@code avro} format), and adds the record timestamp and key as
 *       {@code _dimster_ts} and {@code _dimster_key};</li>
 *   <li>the {@code sink} table is derived from the SELECT's result: the
 *       {@code _dimster} header goes first (consumers read the message type
 *       from the first field), {@code _dimster_ts} is written back as the record
 *       timestamp (end-to-end latency is measured from it), and
 *       {@code _dimster_key}, when selected, becomes the record key, and
 *       {@code _dimster_headers} carries the Flink emit time as a Kafka header
 *       ({@link EmitTimeHeaders}).</li>
 * </ul>
 *
 * Pure string building, so it can be tested without a Flink cluster.
 */
final class DdlBuilder {

    static final String HEADER = "_dimster";
    static final String TIMESTAMP = "_dimster_ts";
    static final String KEY = "_dimster_key";
    /** The sink's headers metadata column; always last, computed by {@link DimsterSqlJob}. */
    static final String HEADERS = "_dimster_headers";

    private DdlBuilder() {}

    /** A column of a resolved schema: name and serializable Flink SQL type. */
    record SchemaColumn(String name, String type) {}

    /**
     * The sink's columns in write order, derived from the query result:
     * header, data columns, optional key, then the timestamp.
     */
    record SinkLayout(SchemaColumn header, List<SchemaColumn> dataColumns,
                      Optional<SchemaColumn> key, SchemaColumn timestamp) {

        /** Column names in sink order, for the projection that feeds the sink. */
        List<String> columnNames() {
            List<String> names = new ArrayList<>();
            names.add(header.name());
            dataColumns.forEach(c -> names.add(c.name()));
            key.ifPresent(k -> names.add(k.name()));
            names.add(timestamp.name());
            return names;
        }
    }

    static String headerType(String format) {
        return switch (format) {
            case "AVRO" -> "BYTES NOT NULL";
            // Dimster's JSON writes the header as lowercase hex, while Flink's
            // JSON format reads BYTES as base64, so it is a string here.
            case "JSON" -> "STRING NOT NULL";
            default -> throw new IllegalArgumentException("Unsupported format for Flink: " + format);
        };
    }

    private static String valueFormat(String format) {
        headerType(format); // validates
        return format.toLowerCase();
    }

    /** DDL of the {@code source} table reading one input topic. */
    static String sourceDdl(FlinkJobSpec spec, FlinkJobSpec.Pipeline pipeline) {
        List<String> columns = new ArrayList<>();
        columns.add(quote(HEADER) + " " + headerType(spec.format()));
        for (FlinkJobSpec.Column c : spec.columns()) {
            columns.add(quote(c.name()) + " " + c.type());
        }
        columns.add(quote(KEY) + " STRING");
        columns.add(quote(TIMESTAMP) + " TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'");

        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", "kafka");
        options.put("topic", pipeline.sourceTopic());
        kafkaProperties(spec, options);
        options.put("properties.group.id", spec.sourceGroupId());
        // Fresh topics per test point: read everything, including a backlog
        // built before the job started (drain-backlog mode).
        options.put("scan.startup.mode", "earliest-offset");
        options.put("key.format", "raw");
        options.put("key.fields", KEY);
        options.put("value.format", valueFormat(spec.format()));
        options.put("value.fields-include", "EXCEPT_KEY");
        options.putAll(spec.sourceOptions());
        return createTable("source", columns, options);
    }

    /**
     * Orders and validates the query's result columns for the sink. The query
     * must keep {@code _dimster} and {@code _dimster_ts} with their source
     * types; {@code _dimster_key} is optional.
     */
    static SinkLayout sinkLayout(List<SchemaColumn> queryColumns, List<SchemaColumn> sourceColumns) {
        Map<String, SchemaColumn> source = byName(sourceColumns);
        Map<String, SchemaColumn> query = byName(queryColumns);
        if (query.size() != queryColumns.size()) {
            throw new IllegalArgumentException("The Flink SQL query returns duplicate column names: "
                    + queryColumns.stream().map(SchemaColumn::name).collect(Collectors.joining(", ")));
        }
        if (query.containsKey(HEADERS)) {
            throw new IllegalArgumentException("The Flink SQL query must not return a column named " + HEADERS
                    + ": Dimster writes it as the output records' headers");
        }
        SchemaColumn header = required(query, source, HEADER);
        SchemaColumn timestamp = required(query, source, TIMESTAMP);
        Optional<SchemaColumn> key = Optional.ofNullable(query.get(KEY));
        key.ifPresent(k -> checkType(k, source.get(KEY)));

        List<SchemaColumn> data = queryColumns.stream()
                .filter(c -> !c.name().equals(HEADER) && !c.name().equals(TIMESTAMP) && !c.name().equals(KEY))
                .toList();
        return new SinkLayout(header, data, key, timestamp);
    }

    /** DDL of the {@code sink} table writing pipeline {@code index}'s output topic. */
    static String sinkDdl(FlinkJobSpec spec, int index, SinkLayout layout) {
        List<String> columns = new ArrayList<>();
        columns.add(quote(HEADER) + " " + layout.header().type());
        for (SchemaColumn c : layout.dataColumns()) {
            columns.add(quote(c.name()) + " " + c.type());
        }
        layout.key().ifPresent(k -> columns.add(quote(KEY) + " " + k.type()));
        columns.add(quote(TIMESTAMP) + " TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'");
        columns.add(quote(HEADERS) + " MAP<STRING, BYTES> METADATA FROM 'headers'");

        FlinkJobSpec.Pipeline pipeline = spec.pipelines().get(index);
        Map<String, String> options = new LinkedHashMap<>();
        options.put("connector", "kafka");
        options.put("topic", pipeline.sinkTopic());
        kafkaProperties(spec, options);
        if (layout.key().isPresent()) {
            options.put("key.format", "raw");
            options.put("key.fields", KEY);
            options.put("value.fields-include", "EXCEPT_KEY");
        }
        options.put("value.format", valueFormat(spec.format()));
        options.put("sink.delivery-guarantee", deliveryGuarantee(spec.deliveryGuarantee()));
        if ("EXACTLY_ONCE".equals(spec.deliveryGuarantee())) {
            options.put("sink.transactional-id-prefix", spec.jobName() + "-" + index);
            // Flink's default transaction timeout (1 h) exceeds the broker's
            // default transaction.max.timeout.ms (15 min), which fails the job.
            options.put("properties.transaction.timeout.ms", "900000");
        }
        options.putAll(spec.sinkOptions());
        return createTable("sink", columns, options);
    }

    private static String deliveryGuarantee(String guarantee) {
        return switch (guarantee) {
            case "AT_LEAST_ONCE" -> "at-least-once";
            case "EXACTLY_ONCE" -> "exactly-once";
            case "NONE" -> "none";
            default -> throw new IllegalArgumentException("Unknown delivery guarantee: " + guarantee);
        };
    }

    private static void kafkaProperties(FlinkJobSpec spec, Map<String, String> options) {
        spec.kafkaProperties().forEach((k, v) -> options.put("properties." + k, v));
    }

    private static SchemaColumn required(Map<String, SchemaColumn> query, Map<String, SchemaColumn> source,
                                         String name) {
        SchemaColumn c = query.get(name);
        if (c == null) {
            throw new IllegalArgumentException("The Flink SQL query must select " + HEADER + " and " + TIMESTAMP
                    + " (missing " + name + "). Dimster consumers read the message type from " + HEADER
                    + " and measure end-to-end latency from " + TIMESTAMP
                    + ", the producer's record timestamp. SELECT * keeps both.");
        }
        checkType(c, source.get(name));
        return c;
    }

    private static void checkType(SchemaColumn queryColumn, SchemaColumn sourceColumn) {
        if (!queryColumn.type().equals(sourceColumn.type())) {
            throw new IllegalArgumentException("Column " + queryColumn.name() + " must be passed through unchanged:"
                    + " the query returns it as " + queryColumn.type() + " but the source type is "
                    + sourceColumn.type());
        }
    }

    private static Map<String, SchemaColumn> byName(List<SchemaColumn> columns) {
        Map<String, SchemaColumn> m = new LinkedHashMap<>();
        columns.forEach(c -> m.put(c.name(), c));
        return m;
    }

    private static String createTable(String name, List<String> columns, Map<String, String> options) {
        String cols = columns.stream().map(c -> "  " + c).collect(Collectors.joining(",\n"));
        String opts = options.entrySet().stream()
                .map(e -> "  " + literal(e.getKey()) + " = " + literal(e.getValue()))
                .collect(Collectors.joining(",\n"));
        return "CREATE TABLE " + quote(name) + " (\n" + cols + "\n) WITH (\n" + opts + "\n)";
    }

    static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
