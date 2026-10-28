package dev.dimster.flink;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import dev.dimster.ScenarioExpander;
import dev.dimster.ScenarioExpander.TestPoint;
import dev.dimster.engines.run.RunConfig;
import dev.dimster.engines.run.Workload;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * The {@code flink} workload block as written in YAML: parsing (with the same
 * mapper settings as BenchmarkRunner), scenario sweeps, defaults and
 * validation at expansion time.
 */
public class FlinkWorkloadTest {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

    private static final String BASE = """
            name: flink-test
            runConfig:
              testDurationMinutes: 1
            baseWorkload:
              topics: 1
              partitionsPerTopic: 3
              dataset: {preset: clicks, format: AVRO}
            """;

    private static List<TestPoint> expand(String yaml) throws Exception {
        Workload w = YAML.readValue(yaml, Workload.class);
        return ScenarioExpander.expand(w.baseWorkload, w.scenarios, null, null, null, new RunConfig());
    }

    private static void assertRejected(String yaml, String messagePart) {
        var e = assertThrows(IllegalArgumentException.class, () -> expand(yaml));
        assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
    }

    @Test
    public void workloadsWithoutFlinkHaveNone() throws Exception {
        assertNull(expand(BASE).getFirst().flink());
    }

    @Test
    public void baseFlinkGetsDefaultsAndTheInputPartitionCount() throws Exception {
        TestPoint point = expand(BASE + """
                  flink:
                    sql: SELECT * FROM source WHERE event_type = 'purchase'
                    sinkOptions:
                      properties.linger.ms: 5
                """).getFirst();
        FlinkSpec flink = point.flink();
        assertEquals("SELECT * FROM source WHERE event_type = 'purchase'", flink.sql);
        assertEquals(1, flink.parallelism);
        assertEquals(Integer.valueOf(1), flink.sourceParallelism);
        assertEquals(Integer.valueOf(1), flink.sinkParallelism);
        assertEquals(10_000, flink.checkpointIntervalMs);
        assertEquals(FlinkSpec.DeliveryGuarantee.AT_LEAST_ONCE, flink.deliveryGuarantee);
        assertEquals(Integer.valueOf(3), flink.outputPartitionsPerTopic);
        // YAML numbers become option strings.
        assertEquals("5", flink.sinkOptions.get("properties.linger.ms"));
    }

    @Test
    public void scenariosSweepFlinkFieldsAlongsideOtherDimensions() throws Exception {
        List<TestPoint> points = expand(BASE + """
                  flink:
                    sql: SELECT * FROM source
                scenarios:
                  - name: parallelism
                    producerRate: [100, 200, 300]
                    flink:
                      parallelism: [1, 2, 4]
                      outputPartitionsPerTopic: 6
                """);
        assertEquals(3, points.size());
        assertEquals(List.of(1, 2, 4), points.stream().map(p -> p.flink().parallelism).toList());
        assertTrue(points.stream().allMatch(p -> p.flink().outputPartitionsPerTopic == 6));
        assertTrue(points.stream().allMatch(p -> p.flink().sql.equals("SELECT * FROM source")));
    }

    @Test
    public void sourceAndSinkParallelismDefaultToTheSweptParallelism() throws Exception {
        List<TestPoint> points = expand(BASE + """
                  flink:
                    sql: SELECT * FROM source
                    sourceParallelism: 1
                scenarios:
                  - name: fan-out
                    flink:
                      parallelism: [1, 2, 4]
                      sinkParallelism: [4, 4, 8]
                  - name: defaults
                    flink:
                      parallelism: 3
                      sourceParallelism: 2
                """);
        assertEquals(4, points.size());
        assertEquals(List.of(1, 1, 1, 2), points.stream().map(p -> p.flink().sourceParallelism).toList());
        assertEquals(List.of(4, 4, 8, 3), points.stream().map(p -> p.flink().sinkParallelism).toList());
        assertEquals(8, points.get(2).flink().slotsNeeded());
    }

    @Test
    public void operatorParallelismIsValidatedAndOnlySetByItsFields() {
        assertRejected(BASE + """
                  flink: {sql: SELECT * FROM source, sinkParallelism: 0}
                """, "flink.sinkParallelism must be at least 1");
        assertRejected(BASE + """
                  flink:
                    sql: SELECT * FROM source
                    sourceOptions: {scan.parallelism: 2}
                """, "use flink.sourceParallelism or flink.sinkParallelism");
    }

    @Test
    public void scenarioCanAddFlinkToABaseWithout() throws Exception {
        List<TestPoint> points = expand(BASE + """
                scenarios:
                  - name: with-flink
                    flink: {sql: "SELECT * FROM source"}
                """);
        assertNotNull(points.getFirst().flink());
    }

    @Test
    public void mismatchedFlinkListsAreRejected() {
        assertRejected(BASE + """
                  flink: {sql: SELECT * FROM source}
                scenarios:
                  - name: bad
                    flink:
                      parallelism: [1, 2]
                      checkpointIntervalMs: [0, 1000, 2000]
                """, "flink.checkpointIntervalMs has 3 elements");
    }

    @Test
    public void flinkNeedsADataset() {
        assertRejected("""
                name: no-dataset
                baseWorkload:
                  flink: {sql: SELECT * FROM source}
                """, "flink requires a dataset");
    }

    @Test
    public void sqlIsRequired() {
        assertRejected(BASE + """
                  flink: {parallelism: 2}
                """, "flink.sql is required");
    }

    @Test
    public void exactlyOnceNeedsCheckpointing() {
        assertRejected(BASE + """
                  flink: {sql: SELECT * FROM source, deliveryGuarantee: EXACTLY_ONCE, checkpointIntervalMs: 0}
                """, "requires checkpointing");
    }

    @Test
    public void dimsterOwnedTableOptionsAreRejected() {
        assertRejected(BASE + """
                  flink:
                    sql: SELECT * FROM source
                    sourceOptions: {value.format: csv}
                """, "cannot set 'value.format'");
        assertRejected(BASE + """
                  flink:
                    sql: SELECT * FROM source
                    sinkOptions: {topic: elsewhere}
                """, "cannot set 'topic'");
    }

    @Test
    public void exactlyOnceDefaultsConsumersToReadCommitted() throws Exception {
        TestPoint eos = expand(BASE + """
                  flink: {sql: SELECT * FROM source, deliveryGuarantee: EXACTLY_ONCE}
                """).getFirst();
        assertEquals("read_committed", eos.kafkaConfig().classicConsumerProperties().get("isolation.level"));

        TestPoint explicit = expand(BASE + """
                  kafkaConfig:
                    classicConsumerConfig: {isolation.level: read_uncommitted}
                  flink: {sql: SELECT * FROM source, deliveryGuarantee: EXACTLY_ONCE}
                """).getFirst();
        assertEquals("read_uncommitted", explicit.kafkaConfig().classicConsumerProperties().get("isolation.level"));

        TestPoint atLeastOnce = expand(BASE + """
                  flink: {sql: SELECT * FROM source}
                """).getFirst();
        assertTrue(atLeastOnce.kafkaConfig() == null
                || atLeastOnce.kafkaConfig().classicConsumerProperties().get("isolation.level") == null);
    }

    @Test
    public void transactionalProducersDefaultTheFlinkSourceToReadCommitted() throws Exception {
        String transactional = """
                  kafkaConfig:
                    producerConfig: {dimster.transactions: true}
                """;
        TestPoint txn = expand(BASE + transactional + """
                  flink: {sql: SELECT * FROM source}
                """).getFirst();
        assertEquals("read_committed", txn.flink().sourceOptions.get("properties.isolation.level"));

        TestPoint explicit = expand(BASE + transactional + """
                  flink:
                    sql: SELECT * FROM source
                    sourceOptions: {properties.isolation.level: read_uncommitted}
                """).getFirst();
        assertEquals("read_uncommitted", explicit.flink().sourceOptions.get("properties.isolation.level"));

        TestPoint plain = expand(BASE + """
                  flink: {sql: SELECT * FROM source}
                """).getFirst();
        assertNull(plain.flink().sourceOptions.get("properties.isolation.level"));
    }

    @Test
    public void transactionsFromTheKafkaConfigFileAlsoApply() throws Exception {
        Workload w = YAML.readValue(BASE + """
                  flink: {sql: SELECT * FROM source}
                """, Workload.class);
        var file = new dev.dimster.kafka.KafkaConfig();
        file.producerConfig = new java.util.LinkedHashMap<>(java.util.Map.of("dimster.transactions", true));
        TestPoint point = ScenarioExpander.expand(w.baseWorkload, w.scenarios, file, null, null,
                new RunConfig()).getFirst();
        assertEquals("read_committed", point.flink().sourceOptions.get("properties.isolation.level"));
    }

    /** The documented examples must parse with the coordinator's strict settings and expand. */
    @Test
    public void exampleWorkloadsExpand() throws Exception {
        var dir = new java.io.File("../run/workloads/flink-test");
        for (String name : List.of("quick-flink-filter-avro.yaml", "quick-flink-projection-json.yaml",
                "flink-parallelism-sweep.yaml", "quick-flink-sink-fanout.yaml")) {
            Workload w = YAML.readValue(new java.io.File(dir, name), Workload.class);
            List<TestPoint> points = ScenarioExpander.expand(w.baseWorkload, w.scenarios, null, null, null,
                    w.runConfig);
            assertTrue(name, points.stream().allMatch(p -> p.flink() != null));
        }
        Workload fanOut = YAML.readValue(new java.io.File(dir, "quick-flink-sink-fanout.yaml"), Workload.class);
        FlinkSpec fanOutFlink = ScenarioExpander.expand(fanOut.baseWorkload, fanOut.scenarios, null, null, null,
                fanOut.runConfig).getFirst().flink();
        assertEquals(List.of(1, 1, 4, 4), List.of(fanOutFlink.parallelism, fanOutFlink.sourceParallelism,
                fanOutFlink.sinkParallelism, fanOutFlink.outputPartitionsPerTopic));
        Workload sweep = YAML.readValue(new java.io.File(dir, "flink-parallelism-sweep.yaml"), Workload.class);
        assertEquals(List.of(1, 2, 4), ScenarioExpander.expand(sweep.baseWorkload, sweep.scenarios, null, null,
                null, sweep.runConfig).stream().map(p -> p.flink().parallelism).toList());

        var drain = YAML.readValue(new java.io.File(dir, "quick-flink-drain-backlog.yaml"),
                dev.dimster.engines.drainbacklog.DrainBacklogWorkload.class);
        TestPoint point = ScenarioExpander.expand(drain.baseWorkload, drain.scenarios, null,
                drain.drainBacklogConfig).getFirst();
        assertEquals(2, point.flink().parallelism);
    }
}
