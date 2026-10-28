package dev.dimster.flink.runner;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Plans whole pipelines with the real planner, connector and formats. Planning
 * (explain) resolves the Kafka tables and the Avro/JSON formats but never
 * connects to Kafka, so these tests need no cluster.
 */
public class DimsterSqlJobTest {

    private static FlinkJobSpec spec(String format, String sql, int pipelines) {
        var columns = List.of(
                new FlinkJobSpec.Column("event_id", "BIGINT NOT NULL"),
                new FlinkJobSpec.Column("event_type", "STRING NOT NULL"),
                new FlinkJobSpec.Column("referrer", "STRING"),
                new FlinkJobSpec.Column("duration_ms", "INT NOT NULL"));
        var pairs = new java.util.ArrayList<FlinkJobSpec.Pipeline>();
        for (int i = 0; i < pipelines; i++) {
            pairs.add(new FlinkJobSpec.Pipeline("dimster-abc-000" + i, "dimster-abc-000" + i + "-out"));
        }
        return new FlinkJobSpec("dimster-abc", sql, format, columns, pairs, "dimster-grp-flink-abc",
                Map.of("bootstrap.servers", "localhost:9092"), 2, "AT_LEAST_ONCE", 10000,
                Map.of(), Map.of(), Map.of());
    }

    private static String plan(FlinkJobSpec spec) {
        TableEnvironment tEnv = TableEnvironment.create(DimsterSqlJob.settings(spec));
        return DimsterSqlJob.buildStatementSet(tEnv, spec).explain();
    }

    @Test
    public void filterOverAvroPlansAsOneSourceToSinkPipeline() {
        String plan = plan(spec("AVRO",
                "SELECT * FROM source WHERE event_type = 'purchase'", 1));
        assertTrue(plan, plan.contains("Sink(table=[default_catalog.p0.sink], fields=[_dimster, event_id"));
        assertTrue(plan, plan.contains("where=[(event_type = 'purchase')]"));
    }

    @Test
    public void everyOutputRecordIsStampedWithItsEmitTime() {
        String plan = plan(spec("JSON", "SELECT * FROM source WHERE event_type = 'purchase'", 1));
        // Planned per row in the Calc that feeds the sink's headers metadata.
        assertTrue(plan, plan.contains("Calc(select=[_dimster, event_id"));
        assertTrue(plan, plan.contains("EmitTimeHeaders*() AS headers"));
    }

    @Test
    public void projectionAndTransformationOverJsonPlans() {
        String plan = plan(spec("JSON",
                "SELECT _dimster_ts, UPPER(event_type) AS event_type, duration_ms * 2 AS d, _dimster FROM source", 1));
        assertTrue(plan, plan.contains("UPPER"));
    }

    @Test
    public void eachTopicGetsItsOwnPipelineInOneJob() {
        String plan = plan(spec("AVRO", "SELECT _dimster, _dimster_key, _dimster_ts FROM source", 3));
        for (int i = 0; i < 3; i++) {
            assertTrue(plan, plan.contains("p" + i));
        }
    }

    @Test
    public void queryWithoutDimsterFieldsFailsWithExplanation() {
        var e = assertThrows(IllegalArgumentException.class,
                () -> plan(spec("AVRO", "SELECT event_id FROM source", 1)));
        assertTrue(e.getMessage().contains("must select _dimster and _dimster_ts"));
    }

    @Test
    public void aggregationIsRejectedByTheAppendOnlySink() {
        assertThrows(org.apache.flink.table.api.TableException.class,
                () -> plan(spec("AVRO",
                        "SELECT _dimster, MAX(_dimster_ts) AS _dimster_ts FROM source GROUP BY _dimster", 1)));
    }

    @Test
    public void unknownColumnIsASqlError() {
        assertThrows(ValidationException.class,
                () -> plan(spec("AVRO", "SELECT _dimster, _dimster_ts, nope FROM source", 1)));
    }

    @Test
    public void specRoundTripsThroughBase64Json() throws Exception {
        String json = """
                {"jobName":"dimster-x","sql":"SELECT * FROM source","format":"AVRO",
                 "columns":[{"name":"a","type":"INT NOT NULL"}],
                 "pipelines":[{"sourceTopic":"t","sinkTopic":"t-out"}],
                 "sourceGroupId":"g","kafkaProperties":{"bootstrap.servers":"k:9092"},
                 "parallelism":3,"deliveryGuarantee":"NONE","checkpointIntervalMs":0,
                 "config":{},"sourceOptions":{},"sinkOptions":{}}""";
        var spec = FlinkJobSpec.fromBase64(
                Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8)));
        assertEquals(3, spec.parallelism());
        assertEquals("t-out", spec.pipelines().get(0).sinkTopic());
        assertEquals("INT NOT NULL", spec.columns().get(0).type());
    }
}
