package dev.dimster.engines.drainbacklog;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.dimster.ConsumerType;
import dev.dimster.ScenarioExpander.TestPoint;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The rules for draining one built backlog across several test points
 * ({@code drainBacklogConfig.reuseBacklog}). Pure functions, so the engine
 * keeps only the sequencing.
 *
 * Test points may share a backlog when they differ only in drain-side
 * settings: the {@link #DRAIN_SIDE_FIELDS} allowlist. Everything else
 * (topics, partitions, message size, dataset, producer config, topic config,
 * backlog size, build rate...) shapes what is on disk, so any difference
 * there means a rebuild. An allowlist rather than a list of build fields so
 * that a field added later defaults to forcing a rebuild.
 */
public final class BacklogReuse {

    /** Topic configs forced on a reusable backlog, so retention cannot shrink it between drains. */
    public static final Map<String, String> RETENTION_OVERRIDES = Map.of(
            "retention.ms", "-1",
            "retention.bytes", "-1");

    /** TestPoint fields that only affect the drain (consumer side), or only label the point. */
    static final Set<String> DRAIN_SIDE_FIELDS = Set.of(
            "scenarioName", "scenarioIndex", "totalScenarios", "label", "dimensionIndex", "totalDimensions",
            "consumersPerGroup", "consumerType",
            "consumerProcMsMin", "consumerProcMsMax", "consumerProcParallelism", "consumerProcMode",
            "consumerProcCapacity",
            // The Flink job runs only in the drain and writes fresh output topics.
            "flink",
            // Settings of other modes, unused in drain-backlog.
            "exploreConfig", "runConfig");

    /** KafkaConfig fields that only affect the drain, or only label the config. */
    static final Set<String> DRAIN_SIDE_KAFKA_FIELDS = Set.of(
            "name", "classicConsumerConfig", "shareConsumerConfig");

    /** DrainBacklogConfig fields that only affect the drain. */
    static final Set<String> DRAIN_SIDE_DRAIN_FIELDS = Set.of(
            "sampleRateMillis", "drainTimeoutMinutes", "backlogProducerRate",
            // Workload-level settings, not properties of the backlog.
            "warmup", "reuseBacklog", "settleTimeoutMinutes");

    private static final ObjectMapper CANONICAL = JsonMapper.builder()
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .build();

    private BacklogReuse() {}

    /** Canonical form of a test point's build-side settings. Equal identities may share a backlog. */
    public static String identity(TestPoint point) {
        ObjectNode tree = CANONICAL.valueToTree(point);
        tree.remove(DRAIN_SIDE_FIELDS);
        if (tree.get("kafkaConfig") instanceof ObjectNode kafka) {
            kafka.remove(DRAIN_SIDE_KAFKA_FIELDS);
        }
        if (tree.get("drainBacklogConfig") instanceof ObjectNode drain) {
            drain.remove(DRAIN_SIDE_DRAIN_FIELDS);
        }
        try {
            // Via plain maps, so map-valued settings (e.g. topicConfig) are key-sorted too.
            return CANONICAL.writeValueAsString(CANONICAL.treeToValue(tree, Object.class));
        } catch (Exception e) {
            throw new IllegalStateException("Cannot compute the backlog identity of test point " + point.label(), e);
        }
    }

    /**
     * Rejects workloads that cannot reuse a backlog correctly. Producing
     * during the drain would grow the backlog from one test point to the next,
     * and classic consumers must start from the beginning of the topic.
     */
    public static void validate(List<TestPoint> points) {
        for (TestPoint p : points) {
            if (p.drainBacklogConfig().backlogProducerRate > 0) {
                throw new IllegalArgumentException(("Test point '%s': reuseBacklog requires backlogProducerRate 0, "
                        + "since producing during the drain would grow the backlog for later test points")
                        .formatted(p.label()));
            }
            if (p.consumerType() == ConsumerType.CONSUMER_GROUP && p.flink() == null) {
                Object reset = p.kafkaConfig().classicConsumerConfig != null
                        ? p.kafkaConfig().classicConsumerConfig.get("auto.offset.reset") : null;
                if (reset == null || !"earliest".equals(reset.toString())) {
                    throw new IllegalArgumentException(("Test point '%s': reuseBacklog requires "
                            + "classicConsumerConfig auto.offset.reset: earliest (got %s), "
                            + "or consumers would skip the reused backlog").formatted(p.label(), reset));
                }
            }
        }
    }

    /**
     * Why a recorded backlog can no longer be drained as-is, or empty when it
     * is intact: its topics exist, nothing was deleted from their heads
     * (start offsets 0) and nothing was appended (end offsets unchanged).
     */
    public static Optional<String> whyNotIntact(ReusableBacklog backlog, Set<String> existingTopics,
                                                Map<TopicPartition, Long> startOffsets,
                                                Map<TopicPartition, Long> endOffsets) {
        for (String topic : backlog.topics) {
            if (!existingTopics.contains(topic)) {
                return Optional.of("topic " + topic + " no longer exists");
            }
        }
        for (var e : startOffsets.entrySet()) {
            if (e.getValue() != 0) {
                return Optional.of("partition " + e.getKey() + " starts at offset " + e.getValue()
                        + " (records were deleted, e.g. by retention)");
            }
        }
        if (endOffsets.size() != backlog.endOffsets.size()) {
            return Optional.of("partition count changed");
        }
        for (var e : endOffsets.entrySet()) {
            Long recorded = backlog.endOffsets.get(e.getKey().toString());
            if (recorded == null || !recorded.equals(e.getValue())) {
                return Optional.of("partition " + e.getKey() + " ends at offset " + e.getValue()
                        + ", recorded " + recorded);
            }
        }
        return Optional.empty();
    }
}
