package dev.dimster.kafka;

import java.util.*;
import java.util.regex.Pattern;

import dev.dimster.spi.kafka.GroupKind;
import dev.dimster.spi.kafka.RawAdmin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dimster's facade over Kafka cluster admin operations - topic management, test
 * group cleanup, lag queries.
 *
 * <p>The actual admin RPCs go through {@link RawAdmin}, the dimster Kafka SPI's
 * admin interface. The per-Kafka-version impl module bound at startup
 * (dimster-kafka-3.x or dimster-kafka-4.2.0) supplies the concrete RawAdmin via
 * {@code KafkaClientFactory.newAdmin}; KafkaCluster just calls it.
 *
 * <p>Why a wrapper at all (rather than letting callers use RawAdmin directly):
 * KafkaCluster owns the dimster-side conventions - the names of test topics and
 * groups and which of them cleanup may delete, replication factor coming from
 * KafkaConfig, fail-soft logging when group cleanup fails. Those policies don't
 * belong in the SPI.
 *
 * <p>Ownership: several coordinators (one per Dimster env) may share one Kafka
 * cluster, so each names everything it creates under its env's topic tag and
 * cleans up only those names. With tag {@code T} the prefix is
 * {@code dimster-T}: topics {@code dimster-T-...}, groups {@code dimster-T-grp-...},
 * the availability probe {@code dimster-T-avail-probe} and kept backlogs
 * {@code dimst-no-delete-T-...}. A tag has no {@code -}, so one tag's prefix
 * never matches another's names. Without a tag (embedded mode) the names are
 * the untagged {@code dimster-...} ones and cleanup takes every {@code dimster*}
 * topic, so an untagged coordinator must not share its cluster.
 */
public class KafkaCluster implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaCluster.class);
    // Public so other tools (e.g. InspectMain) can apply the same dimster-resource
    // detection rules without duplicating the prefix strings. Every tagged and
    // untagged name starts with these, so they detect any env's resources.
    public static final String TOPIC_PREFIX = "dimster";
    public static final String TEST_GROUP_PREFIX = "dimster-grp-";
    public static final String AVAIL_PROBE_TOPIC = "dimster-avail-probe";
    /**
     * Prefix of drain-backlog backlogs kept across workloads and runs
     * ({@code drainBacklogConfig.reuseTopic}). Deliberately not under
     * {@link #TOPIC_PREFIX}, so test-topic cleanup never deletes them.
     */
    public static final String PERSISTENT_TOPIC_PREFIX = "dimst-no-delete";

    /** A topic tag: what Kafka allows in a topic name, minus the {@code -} that ends the tag. */
    private static final Pattern TAG = Pattern.compile("[A-Za-z0-9._]+");

    private final RawAdmin admin;
    private final KafkaConfig config;
    private final String tag;

    /**
     * @param admin  SPI admin handle - typically built by the caller via
     *               {@code factory.newAdmin(config.commonProperties())} so the
     *               KafkaConfig and the admin's bootstrap properties stay in sync.
     * @param config dimster's Kafka config (replication factor, topic properties, ...).
     * @param tag    the topic tag owning this cluster's test topics and groups, or
     *               null for the untagged names (see the class comment).
     */
    public KafkaCluster(RawAdmin admin, KafkaConfig config, String tag) {
        validateTag(tag);
        this.admin = admin;
        this.config = config;
        this.tag = tag;
    }

    /** Rejects a tag that could not keep its names apart from other tags'. Null is valid. */
    public static void validateTag(String tag) {
        if (tag != null && !TAG.matcher(tag).matches()) {
            throw new IllegalArgumentException("Invalid topic tag '" + tag
                    + "': use only letters, digits, '.' and '_' (no '-', which ends the tag in topic names)");
        }
    }

    /** {@code dimster-<tag>}, or {@code dimster} untagged: test topics are {@code <prefix>-...}. */
    public static String topicPrefix(String tag) {
        return tag == null ? TOPIC_PREFIX : TOPIC_PREFIX + "-" + tag;
    }

    /** Prefix of the tag's consumer and share groups. */
    public static String groupPrefix(String tag) {
        return groupPrefixOf(topicPrefix(tag));
    }

    /** Groups live under their topic prefix, so a topic prefix alone names them. */
    public static String groupPrefixOf(String topicPrefix) {
        return topicPrefix + "-grp-";
    }

    /** Whether test-topic cleanup under {@code tag} may delete {@code topic}. */
    public static boolean ownsTopic(String tag, String topic) {
        return tag == null ? topic.startsWith(TOPIC_PREFIX) : topic.startsWith(topicPrefix(tag) + "-");
    }

    /** Whether test-group cleanup under {@code tag} may delete {@code groupId}. */
    public static boolean ownsGroup(String tag, String groupId) {
        return groupId.startsWith(groupPrefix(tag));
    }

    public String topicPrefix() {
        return topicPrefix(tag);
    }

    public String groupPrefix() {
        return groupPrefix(tag);
    }

    /** The availability probe topic, under the topic prefix so cleanup removes it. */
    public String availProbeTopic() {
        return topicPrefix() + "-avail-probe";
    }

    /**
     * Prefix of this tag's kept backlogs. Outside the topic prefix, so test-topic
     * cleanup never deletes them, and tagged so envs sharing the cluster never
     * adopt (or delete) each other's.
     */
    public String persistentTopicPrefix() {
        return tag == null ? PERSISTENT_TOPIC_PREFIX : PERSISTENT_TOPIC_PREFIX + "-" + tag;
    }

    public void deleteTestTopics() {
        deleteTestTopics(Set.of());
    }

    /**
     * Deletes every test topic this cluster's tag owns ({@link #ownsTopic})
     * except those in {@code keep}, e.g. a drain-backlog backlog that the next
     * test point reuses. Other tags' topics on a shared cluster are untouched.
     */
    public void deleteTestTopics(Set<String> keep) {
        try {
            Set<String> topics = new HashSet<>();
            for (String name : admin.listTopicNames()) {
                if (keep.contains(name)) continue;
                if (ownsTopic(tag, name)) {
                    topics.add(name);
                }
            }
            if (!topics.isEmpty()) {
                log.info("Deleting {} existing test topics", topics.size());
                admin.deleteTopics(topics);
                // Allow metadata propagation before creating new topics
                Thread.sleep(2000);
            } else {
                log.info("No test topics to delete");
            }
        } catch (Throwable t) {
            throw new RuntimeException("Failed to delete test topics", t);
        }
    }

    /** Deletes the given topics, e.g. a persistent backlog that cannot be drained as-is. */
    public void deleteTopics(Collection<String> topics) {
        if (topics.isEmpty()) return;
        try {
            log.info("Deleting {} topic(s): {}", topics.size(), topics);
            admin.deleteTopics(new HashSet<>(topics));
            // Allow metadata propagation before creating topics of the same name
            Thread.sleep(2000);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to delete topics " + topics, t);
        }
    }

    public void createTopic(String topic, int partitions) {
        createTopic(topic, partitions, Map.of());
    }

    /** Creates a topic with the configured topic properties plus {@code overrides}, which win. */
    public void createTopic(String topic, int partitions, Map<String, String> overrides) {
        try {
            var newTopic = new NewTopic(topic, partitions, config.replicationFactor);
            Map<String, String> configs = new HashMap<>(config.topicProperties());
            configs.putAll(overrides);
            newTopic.configs(configs);
            admin.createTopic(newTopic);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create topic: " + topic, t);
        }
    }

    public void createShareGroup(String groupName, String topic, int partitions) {
        try {
            Map<TopicPartition, Long> tps = new HashMap<>();
            for(int p=0; p<partitions; p++) {
                TopicPartition tp = new TopicPartition(topic, p);
                tps.put(tp, 0L);
            }

            // Fire-and-forget at the SPI layer (matches pre-SPI behavior); throws
            // UnsupportedOperationException on builds without share-group support.
            admin.alterShareGroupOffsets(groupName, tps);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create group: " + groupName, t);
        }
    }

    /** Deletes the consumer and share groups this cluster's tag owns ({@link #ownsGroup}). */
    public void deleteTestGroups() {
        deleteTestConsumerGroups();
        deleteTestShareGroups();
    }

    private void deleteTestConsumerGroups() {
        try {
            List<String> groupIds = new ArrayList<>();
            for (var groupId : admin.listGroupIds(GroupKind.CLASSIC_OR_CONSUMER)) {
                if (ownsGroup(tag, groupId)) {
                    groupIds.add(groupId);
                }
            }
            if (!groupIds.isEmpty()) {
                log.info("Deleting {} test consumer group(s)", groupIds.size());
                admin.deleteConsumerGroups(groupIds);
            }
        } catch (Exception e) {
            log.warn("Failed to delete test consumer groups: {}", e.getMessage());
        }
    }

    private void deleteTestShareGroups() {
        try {
            // On builds without share-group support, listGroupIds(SHARE) returns
            // an empty set, so this whole method is a no-op without throwing.
            List<String> groupIds = new ArrayList<>();
            for (var groupId : admin.listGroupIds(GroupKind.SHARE)) {
                if (ownsGroup(tag, groupId)) {
                    groupIds.add(groupId);
                }
            }
            if (!groupIds.isEmpty()) {
                log.info("Deleting {} test share group(s)", groupIds.size());
                admin.deleteShareGroups(groupIds);
            }
        } catch (Exception e) {
            log.warn("Failed to delete test share groups: {}", e.getMessage());
        }
    }

    /**
     * Queries the broker for consumer group lag by comparing committed offsets
     * against log end offsets. Returns a map of TopicPartition to lag, or an
     * empty map on error. Uses the SPI's narrow committedOffsets/endOffsets
     * primitives instead of the raw admin client.
     */
    public Map<TopicPartition, Long> getConsumerGroupLag(String groupName) {
        try {
            var committedOffsets = admin.committedOffsets(groupName);
            if (committedOffsets == null || committedOffsets.isEmpty()) {
                return Map.of();
            }

            Map<TopicPartition, Long> endOffsets = admin.endOffsets(committedOffsets.keySet());

            // Compute lag = logEndOffset - committedOffset
            var lag = new LinkedHashMap<TopicPartition, Long>();
            for (var entry : committedOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                long committed = entry.getValue() != null ? entry.getValue().offset() : 0;
                long endOffset = endOffsets.containsKey(tp) ? endOffsets.get(tp) : 0;
                lag.put(tp, Math.max(0, endOffset - committed));
            }
            return lag;
        } catch (Exception e) {
            log.warn("Failed to query consumer group lag for {}: {}", groupName, e.getMessage());
            return Map.of();
        }
    }

    /**
     * Queries the broker for share group lag. Returns a map of TopicPartition
     * to lag (messages behind), or an empty map on error. Throws
     * UnsupportedOperationException on builds without share-group support;
     * callers should gate on {@code factory.capabilities().shareGroupsSupported()}.
     */
    public Map<TopicPartition, Long> getShareGroupLag(String groupName) {
        try {
            return admin.shareGroupLag(groupName);
        } catch (Exception e) {
            log.warn("Failed to query share group lag for {}: {}", groupName, e.getMessage());
            return Map.of();
        }
    }

    /** Names of every topic on the cluster. */
    public Set<String> listTopicNames() {
        return admin.listTopicNames();
    }

    /** Log start offsets of every partition of the given topics. */
    public Map<TopicPartition, Long> startOffsets(Collection<String> topics) {
        return admin.startOffsets(partitionsOf(topics));
    }

    /** Log end offsets of every partition of the given topics. */
    public Map<TopicPartition, Long> endOffsets(Collection<String> topics) {
        return admin.endOffsets(partitionsOf(topics));
    }

    /** Partition count of each of the given topics, which must exist. */
    public Map<String, Integer> partitionCounts(Collection<String> topics) {
        Map<String, Integer> counts = new HashMap<>();
        for (TopicPartition tp : partitionsOf(topics)) {
            counts.merge(tp.topic(), 1, Integer::sum);
        }
        return counts;
    }

    private Set<TopicPartition> partitionsOf(Collection<String> topics) {
        return admin.describePartitions(new HashSet<>(topics)).keySet();
    }

    /**
     * Waits until every partition of the given topics has a leader and a full
     * ISR, moving leadership back to preferred replicas along the way. Used
     * before a measurement that a hook may have disturbed (e.g. by restarting
     * brokers). Throws if the cluster has not settled within the timeout.
     */
    public void awaitSettled(Collection<String> topics, java.time.Duration timeout,
                             java.util.concurrent.atomic.AtomicBoolean stopRequested) {
        long deadline = System.nanoTime() + timeout.toNanos();
        Set<String> topicSet = new HashSet<>(topics);
        PartitionHealth health = PartitionHealth.assess(admin.describePartitions(topicSet));
        if (health.settled()) {
            log.info("Cluster settled: {}", health);
            return;
        }
        log.info("Waiting for the cluster to settle: {}", health);
        while (true) {
            if (!health.notPreferred().isEmpty()) {
                log.info("Electing preferred leaders for {} partition(s)", health.notPreferred().size());
                admin.electPreferredLeaders(health.notPreferred());
            }
            dev.dimster.utils.Sleeper.sleepOrStop(stopRequested, 2000, true);
            health = PartitionHealth.assess(admin.describePartitions(topicSet));
            if (health.settled()) {
                log.info("Cluster settled: {}", health);
                return;
            }
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Cluster did not settle within " + timeout + ": " + health);
            }
            log.info("    Still settling: {}", health);
        }
    }

    @Override
    public void close() {
        admin.close();
    }
}
