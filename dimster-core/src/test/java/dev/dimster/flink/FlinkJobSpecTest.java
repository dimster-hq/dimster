package dev.dimster.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.dimster.datagen.DatasetResolver;
import dev.dimster.datagen.DatasetSpec;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.datagen.ResolvedDataset;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.*;

public class FlinkJobSpecTest {

    private static ResolvedDataset dataset(String preset, RecordFormat format) {
        var spec = new DatasetSpec();
        spec.preset = preset;
        spec.format = format;
        return DatasetResolver.resolve(spec);
    }

    /**
     * The Flink columns must decode the records byte for byte, so they follow
     * the Avro schema Dimster writes: same names, order and nullability.
     */
    @Test
    public void columnsMirrorTheAvroSchemaOfEveryPreset() throws Exception {
        var mapper = new ObjectMapper();
        for (String preset : List.of("clicks", "orders", "taxi")) {
            ResolvedDataset ds = dataset(preset, RecordFormat.AVRO);
            JsonNode fields = mapper.readTree(ds.avroSchema()).path("fields");
            List<FlinkJobSpec.Column> columns = FlinkJobSpec.columns(ds);
            assertEquals(preset, fields.size() - 1, columns.size()); // minus _dimster
            for (int i = 0; i < columns.size(); i++) {
                JsonNode field = fields.get(i + 1);
                FlinkJobSpec.Column column = columns.get(i);
                assertEquals(field.path("name").asText(), column.name());
                boolean avroNullable = field.path("type").isArray();
                assertEquals(preset + "." + column.name(), avroNullable, !column.type().endsWith("NOT NULL"));
                String avroType = avroNullable ? field.path("type").get(1).toString() : field.path("type").toString();
                String flinkType = column.type().replace(" NOT NULL", "");
                String expected = switch (avroType.replace("\"", "")) {
                    case "long", "{type:long,logicalType:timestamp-millis}" -> "BIGINT";
                    case "int" -> "INT";
                    case "double" -> "DOUBLE";
                    case "boolean" -> "BOOLEAN";
                    case "string" -> "STRING";
                    case "bytes" -> "BYTES";
                    default -> throw new AssertionError("unexpected Avro type " + avroType);
                };
                assertEquals(preset + "." + column.name(), expected, flinkType);
            }
        }
    }

    @Test
    public void jsonBytesColumnsAreHexStrings() {
        var spec = new DatasetSpec();
        spec.format = RecordFormat.JSON;
        var col = new dev.dimster.datagen.ColumnSpec();
        col.type = dev.dimster.datagen.ColumnSpec.Type.BYTES;
        spec.columns = new LinkedHashMap<>(Map.of("blob", col));
        assertEquals(List.of(new FlinkJobSpec.Column("blob", "STRING NOT NULL")),
                FlinkJobSpec.columns(DatasetResolver.resolve(spec)));
        spec.format = RecordFormat.AVRO;
        assertEquals(List.of(new FlinkJobSpec.Column("blob", "BYTES NOT NULL")),
                FlinkJobSpec.columns(DatasetResolver.resolve(spec)));
    }

    @Test
    public void payloadDatasetIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> FlinkJobSpec.columns(DatasetResolver.payloadDataset(99, 0.5, 1, null)));
    }

    /** The JSON field names are the contract with the runner jar's FlinkJobSpec. */
    @Test
    public void serializesWithTheRunnersFieldNamesAndDropsSerializers() throws Exception {
        var flink = new FlinkSpec();
        flink.sql = "SELECT * FROM source";
        flink.parallelism = 3;
        var kafka = new Properties();
        kafka.put("bootstrap.servers", "kafka:9092");
        kafka.put("security.protocol", "SSL");
        kafka.put("key.serializer", "x");
        kafka.put("value.deserializer", "y");
        var pipelines = new LinkedHashMap<String, String>();
        pipelines.put("dimster-abc-0000", "dimster-abc-0000-out");
        FlinkJobSpec spec = FlinkJobSpec.build("dimster-abc", flink, dataset("clicks", RecordFormat.JSON),
                pipelines, "dimster-grp-flink-abc", kafka);

        JsonNode json = new ObjectMapper().readTree(
                new String(Base64.getDecoder().decode(spec.toBase64()), StandardCharsets.UTF_8));
        Set<String> names = new TreeSet<>();
        json.fieldNames().forEachRemaining(names::add);
        assertEquals(new TreeSet<>(Set.of("jobName", "sql", "format", "columns", "pipelines", "sourceGroupId",
                "kafkaProperties", "parallelism", "deliveryGuarantee", "checkpointIntervalMs", "config",
                "sourceOptions", "sinkOptions")), names);
        assertEquals("JSON", json.path("format").asText());
        assertEquals("AT_LEAST_ONCE", json.path("deliveryGuarantee").asText());
        assertEquals("dimster-abc-0000-out", json.path("pipelines").get(0).path("sinkTopic").asText());
        assertEquals(Set.of("bootstrap.servers", "security.protocol"),
                Set.copyOf(spec.kafkaProperties().keySet()));
    }

    /** Source and sink parallelism reach the runner as the connector's own table options. */
    @Test
    public void operatorParallelismBecomesTableOptions() {
        var flink = new FlinkSpec();
        flink.sql = "SELECT * FROM source";
        flink.parallelism = 2;
        flink.sinkParallelism = 4;
        flink.sinkOptions.put("properties.linger.ms", "5");
        FlinkJobSpec spec = FlinkJobSpec.build("dimster-abc", flink, dataset("clicks", RecordFormat.AVRO),
                new LinkedHashMap<>(java.util.Map.of("t", "t-out")), "g", new Properties());

        assertEquals("2", spec.sourceOptions().get("scan.parallelism"));
        assertEquals("4", spec.sinkOptions().get("sink.parallelism"));
        assertEquals("5", spec.sinkOptions().get("properties.linger.ms"));
        // The workload's own options are left as they were.
        assertFalse(flink.sinkOptions.containsKey("sink.parallelism"));
    }
}
