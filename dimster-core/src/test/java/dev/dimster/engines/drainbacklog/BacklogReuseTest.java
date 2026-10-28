package dev.dimster.engines.drainbacklog;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.dimster.ScenarioExpander;
import dev.dimster.ScenarioExpander.TestPoint;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Which drain-backlog test points may share a backlog, what makes a workload
 * unfit for reuse, and when a recorded backlog is no longer intact. Test
 * points come from real workload YAML through the scenario expander.
 */
public class BacklogReuseTest {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

    private static final String BASE = """
            name: reuse
            drainBacklogConfig:
              backlogSizeGB: 0.1
              reuseBacklog: true
            baseWorkload:
              topics: 1
              partitionsPerTopic: 6
              messageSize: 1000
              producersPerTopic: 1
              consumerGroupsPerTopic: 1
              consumersPerGroup: 1
              kafkaConfig:
                name: base
                replicationFactor: 1
                commonConfig:
                  bootstrap.servers: kafka:9092
                producerConfig:
                  batch.size: 16384
                classicConsumerConfig:
                  auto.offset.reset: earliest
                topicConfig:
                  segment.bytes: 1073741824
            """;

    private static List<TestPoint> points(String scenariosYaml) throws Exception {
        DrainBacklogWorkload wl = YAML.readValue(BASE + scenariosYaml, DrainBacklogWorkload.class);
        return ScenarioExpander.expand(wl.baseWorkload, wl.scenarios, null, wl.drainBacklogConfig);
    }

    private static List<String> identities(List<TestPoint> points) {
        return points.stream().map(BacklogReuse::identity).toList();
    }

    @Test
    public void drain_side_sweeps_share_one_identity() throws Exception {
        var ids = identities(points("""
                scenarios:
                  - name: consumers
                    label: [c1, c2, c4]
                    consumersPerGroup: [1, 2, 4]
                    consumerProcMsMax: [0, 1, 2]
                    drainBacklogConfig:
                      drainTimeoutMinutes: [5, 10, 20]
                """));
        assertEquals(3, ids.size());
        assertEquals(1, Set.copyOf(ids).size());
    }

    @Test
    public void consumer_config_and_type_are_drain_side() throws Exception {
        var ids = identities(points("""
                scenarios:
                  - name: a
                    consumerType: [CONSUMER_GROUP]
                    kafkaConfig:
                      classicConsumerConfig:
                        max.poll.records: [500, 2000]
                """));
        assertEquals(ids.get(0), ids.get(1));
    }

    @Test
    public void repeats_share_one_identity() throws Exception {
        var ids = identities(points("""
                scenarios:
                  - name: r
                    repeat: 3
                """));
        assertEquals(3, ids.size());
        assertEquals(1, Set.copyOf(ids).size());
    }

    @Test
    public void build_side_sweeps_force_a_rebuild() throws Exception {
        for (String sweep : List.of(
                "partitionsPerTopic: [6, 12]",
                "messageSize: [1000, 2000]",
                "producersPerTopic: [1, 2]",
                "consumerGroupsPerTopic: [1, 2]",
                "drainBacklogConfig:\n      backlogSizeGB: [0.1, 0.2]",
                "drainBacklogConfig:\n      buildRate: [1000, 2000]",
                "kafkaConfig:\n      producerConfig:\n        batch.size: [16384, 65536]",
                "kafkaConfig:\n      topicConfig:\n        segment.bytes: [1073741824, 104857600]")) {
            var ids = identities(points("scenarios:\n  - name: s\n    " + sweep + "\n"));
            assertNotEquals(sweep, ids.get(0), ids.get(1));
        }
    }

    @Test
    public void identity_ignores_map_order() throws Exception {
        var a = points("""
                scenarios:
                  - name: a
                    kafkaConfig:
                      producerConfig:
                        linger.ms: 5
                        acks: all
                """);
        var b = points("""
                scenarios:
                  - name: b
                    kafkaConfig:
                      producerConfig:
                        acks: all
                        linger.ms: 5
                """);
        assertEquals(BacklogReuse.identity(a.get(0)), BacklogReuse.identity(b.get(0)));
    }

    @Test
    public void rejects_production_during_the_drain() throws Exception {
        var pts = points("""
                scenarios:
                  - name: s
                    drainBacklogConfig:
                      backlogProducerRate: [1000]
                """);
        var e = assertThrows(IllegalArgumentException.class, () -> BacklogReuse.validate(pts));
        assertTrue(e.getMessage().contains("backlogProducerRate"));
    }

    @Test
    public void rejects_consumers_that_would_skip_the_backlog() throws Exception {
        var pts = points("""
                scenarios:
                  - name: s
                    kafkaConfig:
                      classicConsumerConfig:
                        auto.offset.reset: latest
                """);
        var e = assertThrows(IllegalArgumentException.class, () -> BacklogReuse.validate(pts));
        assertTrue(e.getMessage().contains("auto.offset.reset"));
    }

    @Test
    public void accepts_a_reusable_workload() throws Exception {
        BacklogReuse.validate(points("scenarios:\n  - name: s\n    consumersPerGroup: [1, 2]\n"));
    }

    // ─── Intactness ────────────────────────────────────────────────────

    private static ReusableBacklog backlog() {
        var b = new ReusableBacklog();
        b.topics = List.of("dimster-x-0000");
        b.endOffsets = Map.of("dimster-x-0000-0", 100L, "dimster-x-0000-1", 50L);
        return b;
    }

    private static final TopicPartition P0 = new TopicPartition("dimster-x-0000", 0);
    private static final TopicPartition P1 = new TopicPartition("dimster-x-0000", 1);

    @Test
    public void intact_backlog_has_no_problem() {
        assertTrue(BacklogReuse.whyNotIntact(backlog(), Set.of("dimster-x-0000"),
                Map.of(P0, 0L, P1, 0L), Map.of(P0, 100L, P1, 50L)).isEmpty());
    }

    @Test
    public void missing_topic_is_not_intact() {
        assertTrue(BacklogReuse.whyNotIntact(backlog(), Set.of(), Map.of(), Map.of()).isPresent());
    }

    @Test
    public void retention_deleting_the_head_is_not_intact() {
        var why = BacklogReuse.whyNotIntact(backlog(), Set.of("dimster-x-0000"),
                Map.of(P0, 10L, P1, 0L), Map.of(P0, 100L, P1, 50L));
        assertTrue(why.orElseThrow().contains("retention"));
    }

    @Test
    public void appended_records_are_not_intact() {
        assertTrue(BacklogReuse.whyNotIntact(backlog(), Set.of("dimster-x-0000"),
                Map.of(P0, 0L, P1, 0L), Map.of(P0, 100L, P1, 51L)).isPresent());
    }
}
