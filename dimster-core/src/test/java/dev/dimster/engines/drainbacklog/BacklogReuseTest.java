package dev.dimster.engines.drainbacklog;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.dimster.ScenarioExpander;
import dev.dimster.ScenarioExpander.TestPoint;
import dev.dimster.kafka.KafkaCluster;
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
    public void identity_ignores_how_clients_reach_the_cluster() throws Exception {
        var plain = points("scenarios:\n  - name: s\n");
        var tls = points("""
                scenarios:
                  - name: s
                    kafkaConfig:
                      commonConfig:
                        bootstrap.servers: kafka-tls:9093
                        security.protocol: SSL
                        ssl.keystore.password: secret
                        sasl.mechanism: PLAIN
                """);
        assertEquals(BacklogReuse.identity(plain.get(0)), BacklogReuse.identity(tls.get(0)));
        // Other common settings reach the producers, so they still count.
        var tuned = points("""
                scenarios:
                  - name: s
                    kafkaConfig:
                      commonConfig:
                        linger.ms: 50
                """);
        assertNotEquals(BacklogReuse.identity(plain.get(0)), BacklogReuse.identity(tuned.get(0)));
    }

    // ─── Persistent backlog names (reuseTopic) ─────────────────────────

    private static String persistentPrefix(TestPoint p) {
        return BacklogReuse.persistentTopicPrefix(BacklogReuse.identity(p));
    }

    @Test
    public void persistent_name_is_outside_test_topic_cleanup() throws Exception {
        String prefix = persistentPrefix(points("scenarios:\n  - name: s\n").get(0));
        assertTrue(prefix, prefix.startsWith(KafkaCluster.PERSISTENT_TOPIC_PREFIX + "-"));
        assertFalse(prefix, prefix.startsWith(KafkaCluster.TOPIC_PREFIX));
    }

    @Test
    public void workloads_with_equal_build_settings_share_a_persistent_name() throws Exception {
        TestPoint a = points("scenarios:\n  - name: first\n    label: [x]\n    consumersPerGroup: [4]\n").get(0);
        DrainBacklogWorkload other = YAML.readValue(BASE.replace("name: reuse", "name: another workload")
                .replace("reuseBacklog: true", "reuseTopic: true")
                + "scenarios:\n  - name: second\n", DrainBacklogWorkload.class);
        TestPoint b = ScenarioExpander.expand(other.baseWorkload, other.scenarios, null, other.drainBacklogConfig)
                .get(0);
        assertEquals(persistentPrefix(a), persistentPrefix(b));
    }

    @Test
    public void build_settings_change_the_persistent_name() throws Exception {
        var pts = points("""
                scenarios:
                  - name: none
                    kafkaConfig:
                      producerConfig:
                        compression.type: none
                  - name: zstd
                    kafkaConfig:
                      producerConfig:
                        compression.type: zstd
                """);
        assertNotEquals(persistentPrefix(pts.get(0)), persistentPrefix(pts.get(1)));
    }

    @Test
    public void reuse_topic_rejects_transactional_producers() throws Exception {
        var pts = points("""
                scenarios:
                  - name: s
                    kafkaConfig:
                      producerConfig:
                        dimster.transactions: true
                """);
        var e = assertThrows(IllegalArgumentException.class, () -> BacklogReuse.validate(pts, true));
        assertTrue(e.getMessage().contains("transactional"));
        // Without kept topics, the record count comes from the producers, not the offsets.
        BacklogReuse.validate(pts, false);
    }

    @Test
    public void rejects_production_during_the_drain() throws Exception {
        var pts = points("""
                scenarios:
                  - name: s
                    drainBacklogConfig:
                      backlogProducerRate: [1000]
                """);
        var e = assertThrows(IllegalArgumentException.class, () -> BacklogReuse.validate(pts, false));
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
        var e = assertThrows(IllegalArgumentException.class, () -> BacklogReuse.validate(pts, false));
        assertTrue(e.getMessage().contains("auto.offset.reset"));
    }

    @Test
    public void accepts_a_reusable_workload() throws Exception {
        BacklogReuse.validate(points("scenarios:\n  - name: s\n    consumersPerGroup: [1, 2]\n"), false);
    }

    // ─── Intactness ────────────────────────────────────────────────────

    /** 1,000,000 records over two partitions: up to 500 may be appended (0.05%). */
    private static ReusableBacklog backlog() {
        var b = new ReusableBacklog();
        b.topics = List.of("dimster-x-0000");
        b.endOffsets = Map.of("dimster-x-0000-0", 600_000L, "dimster-x-0000-1", 400_000L);
        b.records = 1_000_000;
        return b;
    }

    private static final TopicPartition P0 = new TopicPartition("dimster-x-0000", 0);
    private static final TopicPartition P1 = new TopicPartition("dimster-x-0000", 1);
    private static final Map<TopicPartition, Long> AT_START = Map.of(P0, 0L, P1, 0L);

    private static BacklogReuse.Integrity check(Map<TopicPartition, Long> startOffsets,
                                                Map<TopicPartition, Long> endOffsets) {
        return BacklogReuse.checkIntact(backlog(), Set.of("dimster-x-0000"), startOffsets, endOffsets);
    }

    private static <T> T assertInstanceOf(Class<T> type, Object actual) {
        assertTrue("expected " + type.getSimpleName() + ", got " + actual, type.isInstance(actual));
        return type.cast(actual);
    }

    private static String reason(BacklogReuse.Integrity integrity) {
        return assertInstanceOf(BacklogReuse.NotIntact.class, integrity).reason();
    }

    @Test
    public void unchanged_backlog_is_intact() {
        var intact = assertInstanceOf(BacklogReuse.Intact.class,
                check(AT_START, Map.of(P0, 600_000L, P1, 400_000L)));
        assertEquals(0, intact.appended());
    }

    @Test
    public void missing_topic_is_not_intact() {
        var integrity = BacklogReuse.checkIntact(backlog(), Set.of(), Map.of(), Map.of());
        assertTrue(reason(integrity).contains("no longer exists"));
    }

    @Test
    public void retention_deleting_the_head_is_not_intact() {
        var integrity = check(Map.of(P0, 10L, P1, 0L), Map.of(P0, 600_000L, P1, 400_000L));
        assertTrue(reason(integrity).contains("retention"));
    }

    @Test
    public void records_appended_within_the_margin_are_intact() {
        // e.g. sends still in flight when the build's producers paused
        var intact = assertInstanceOf(BacklogReuse.Intact.class,
                check(AT_START, Map.of(P0, 600_200L, P1, 400_300L)));
        assertEquals(500, intact.appended());
        assertEquals(Map.of(P0, 600_200L, P1, 400_300L), intact.endOffsets());
    }

    @Test
    public void records_appended_beyond_the_margin_are_not_intact() {
        var integrity = check(AT_START, Map.of(P0, 600_200L, P1, 400_301L));
        assertTrue(reason(integrity).contains("501 records were appended"));
    }

    @Test
    public void partition_ending_before_its_recorded_end_is_not_intact() {
        // Fewer records than recorded, even if another partition grew to make up for it.
        var integrity = check(AT_START, Map.of(P0, 600_010L, P1, 399_990L));
        assertTrue(reason(integrity).contains("before the recorded"));
    }
}
