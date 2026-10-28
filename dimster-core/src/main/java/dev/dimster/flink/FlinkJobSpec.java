package dev.dimster.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.dimster.datagen.RecordFormat;
import dev.dimster.datagen.ResolvedDataset;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The job spec the coordinator hands to the Flink runner jar
 * ({@code dev.dimster.flink.runner.DimsterSqlJob} in the dimster-flink module)
 * as its single, base64-encoded JSON argument.
 *
 * <p>The runner has its own copy of this record: it targets Java 17 and is
 * shaded, so the modules share no classes and the JSON field names are the
 * contract. Keep the two in step.
 *
 * <p>The source table's columns mirror the dataset's record schema
 * ({@link ResolvedDataset#avroSchema()}) so Flink decodes Dimster's records as
 * they are: Dimster's Avro is schema-less binary, the same encoding as Flink's
 * {@code avro} format.
 */
public record FlinkJobSpec(
        String jobName,
        String sql,
        String format,
        List<Column> columns,
        List<Pipeline> pipelines,
        String sourceGroupId,
        Map<String, String> kafkaProperties,
        int parallelism,
        String deliveryGuarantee,
        long checkpointIntervalMs,
        Map<String, String> config,
        Map<String, String> sourceOptions,
        Map<String, String> sinkOptions) {

    /** A source column: name and Flink SQL type, including nullability. */
    public record Column(String name, String type) {}

    /** One input topic and the output topic its query results go to. */
    public record Pipeline(String sourceTopic, String sinkTopic) {}

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Builds the spec of one test point's job.
     *
     * @param jobName         Flink job name, {@code dimster-} prefixed
     * @param flink           the test point's resolved Flink stage
     * @param dataset         the dataset producers generate
     * @param pipelineTopics  input topic to output topic
     * @param sourceGroupId   Kafka group id for the Flink source
     * @param kafkaCommon     the Kafka client properties shared by every
     *                        client (bootstrap and security)
     */
    public static FlinkJobSpec build(String jobName, FlinkSpec flink, ResolvedDataset dataset,
                                     Map<String, String> pipelineTopics, String sourceGroupId,
                                     Properties kafkaCommon) {
        List<Pipeline> pipelines = new ArrayList<>();
        pipelineTopics.forEach((in, out) -> pipelines.add(new Pipeline(in, out)));
        Map<String, String> kafka = new LinkedHashMap<>();
        kafkaCommon.stringPropertyNames().stream().sorted()
                // Flink's connector sets its own (de)serializers.
                .filter(k -> !k.endsWith(".serializer") && !k.endsWith(".deserializer"))
                .forEach(k -> kafka.put(k, kafkaCommon.getProperty(k)));
        return new FlinkJobSpec(jobName, flink.sql, dataset.format().name(), columns(dataset), pipelines,
                sourceGroupId, kafka, flink.parallelism, flink.deliveryGuarantee.name(),
                flink.checkpointIntervalMs, flink.config,
                withParallelism(flink.sourceOptions, FlinkSpec.SCAN_PARALLELISM, flink.resolved(flink.sourceParallelism)),
                withParallelism(flink.sinkOptions, FlinkSpec.SINK_PARALLELISM, flink.resolved(flink.sinkParallelism)));
    }

    /**
     * The table options plus the operator's parallelism. Source and sink
     * parallelism travel as the connector's own table options, so the runner
     * needs no field of its own for them.
     */
    private static Map<String, String> withParallelism(Map<String, String> options, String key, int parallelism) {
        Map<String, String> m = options == null ? new LinkedHashMap<>() : new LinkedHashMap<>(options);
        m.put(key, Integer.toString(parallelism));
        return m;
    }

    /**
     * The dataset's columns as Flink SQL columns, after the {@code _dimster}
     * header (which the runner adds). Types follow the record encoding:
     * {@code TIMESTAMP} and {@code SEQUENCE} are epoch-millis / counter longs,
     * and in JSON, where Dimster writes bytes as lowercase hex, {@code BYTES}
     * columns are strings (Flink's JSON format would read them as base64).
     * Nullability follows the column's nullRate, as in the Avro schema.
     */
    public static List<Column> columns(ResolvedDataset dataset) {
        if (dataset.format() != RecordFormat.AVRO && dataset.format() != RecordFormat.JSON) {
            throw new IllegalArgumentException("Flink needs an AVRO or JSON dataset, not " + dataset.format());
        }
        List<Column> columns = new ArrayList<>();
        for (ResolvedDataset.Column c : dataset.columns()) {
            String type = switch (c.type()) {
                case LONG, SEQUENCE, TIMESTAMP -> "BIGINT";
                case INT -> "INT";
                case DOUBLE -> "DOUBLE";
                case BOOLEAN -> "BOOLEAN";
                case STRING -> "STRING";
                case BYTES -> dataset.format() == RecordFormat.JSON ? "STRING" : "BYTES";
            };
            columns.add(new Column(c.name(), c.nullable() ? type : type + " NOT NULL"));
        }
        return columns;
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize the Flink job spec", e);
        }
    }

    /** The runner jar's program argument. */
    public String toBase64() {
        return Base64.getEncoder().encodeToString(toJson().getBytes(StandardCharsets.UTF_8));
    }
}
