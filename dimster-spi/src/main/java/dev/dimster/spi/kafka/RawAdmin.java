package dev.dimster.spi.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * SPI for Kafka cluster administration.
 *
 * <p>Narrowed to exactly the operations dimster-core performs today via
 * {@code KafkaCluster} and {@code ShareGroupLagMonitor}. Adding new admin
 * operations later is cheap - add the method here, implement in each impl module.
 *
 * <p>{@link NewTopic}, {@link OffsetAndMetadata}, and {@link TopicPartition} are
 * stable Kafka types reused directly. The share-group methods at the bottom of the
 * interface throw {@link UnsupportedOperationException} on impls whose
 * {@link Capabilities#shareGroupsSupported} is false - dimster-core's capability
 * check is the primary guard but the impl-side throw is a backstop in case
 * capability checks are bypassed.
 */
public interface RawAdmin extends AutoCloseable {

    // --- Topic management ----------------------------------------------------

    /** Lists every topic name visible to this admin client. */
    Set<String> listTopicNames();

    /** Creates one topic. Blocks until creation completes (or throws on failure). */
    void createTopic(NewTopic topic);

    /** Deletes the named topics. Blocks until deletion completes. */
    void deleteTopics(Set<String> topicNames);

    // --- Classic / consumer group management --------------------------------

    /**
     * Returns the committed offsets for a classic (or KIP-848 consumer) group.
     * Empty map if the group has no committed offsets yet.
     */
    Map<TopicPartition, OffsetAndMetadata> committedOffsets(String groupId);

    /** Returns the latest (high-watermark) offsets for the given partitions. */
    Map<TopicPartition, Long> endOffsets(Set<TopicPartition> partitions);

    /**
     * Returns the log start offsets for the given partitions. Above 0 once
     * retention has deleted the head of a partition.
     */
    Map<TopicPartition, Long> startOffsets(Set<TopicPartition> partitions);

    // --- Cluster health ------------------------------------------------------

    /** Returns the leader, replicas and ISR of every partition of the named topics. */
    Map<TopicPartition, PartitionState> describePartitions(Set<String> topicNames);

    /**
     * Moves leadership of the given partitions to their preferred replicas.
     * Partitions already led by their preferred replica are skipped without
     * error. Blocks until the elections complete.
     */
    void electPreferredLeaders(Set<TopicPartition> partitions);

    /**
     * Lists group ids of the given kind. {@link GroupKind#CLASSIC_OR_CONSUMER}
     * matches both classic and KIP-848 consumer groups in a single call;
     * {@link GroupKind#SHARE} matches share groups (impls without share-group
     * support return an empty set rather than throw - listing is harmless).
     */
    Set<String> listGroupIds(GroupKind kind);

    /** Deletes the named consumer groups. */
    void deleteConsumerGroups(Collection<String> groupIds);

    // --- Share group management (4.2+ only) ---------------------------------

    /**
     * Returns per-partition lag (records behind the share-group's processed
     * position) for a share group. Implementations on pre-4.2 builds throw
     * {@link UnsupportedOperationException}.
     *
     * <p>Lean shape used by the metric-polling hot path and engines that
     * just want lag; {@link #shareGroupOffsets} returns the same data plus
     * the share-partition start offset for diagnostic / inspect callers.
     */
    Map<TopicPartition, Long> shareGroupLag(String groupId);

    /**
     * Returns per-partition share-group offset state — both the share-partition
     * start offset (SPSO) and the lag — in a single RPC. Used by
     * {@code dimster inspect} to surface both columns in the SHARE GROUPS
     * section.
     *
     * <p>Available on 4.1+. Pre-4.1 impls throw
     * {@link UnsupportedOperationException}. Same gating as
     * {@link #shareGroupLag} and {@link #describeShareGroup}.
     */
    Map<TopicPartition, ShareGroupOffsetInfo> shareGroupOffsets(String groupId);

    /**
     * Returns a snapshot of the share group's state, coordinator, and member
     * list (with each member's current partition assignment). Used by
     * {@code dimster inspect} to surface share-group membership alongside
     * lag, mirroring how the consumer-group section shows members + offsets.
     *
     * <p>Available on impls whose {@link Capabilities#shareGroupsSupported()}
     * is true (4.1+ — share-group describe APIs landed in 4.1, not 4.2).
     * Pre-4.1 impls throw {@link UnsupportedOperationException}.
     */
    ShareGroupInfo describeShareGroup(String groupId);

    /**
     * Initialises a share group's per-partition starting offsets. Used by
     * dimster's KafkaCluster.createShareGroup() to seed a new share group at offset
     * 0 on each partition. Pre-4.2 builds throw.
     */
    void alterShareGroupOffsets(String groupId, Map<TopicPartition, Long> startOffsets);

    /** Deletes the named share groups. Pre-4.2 builds throw. */
    void deleteShareGroups(Collection<String> groupIds);

    @Override
    void close();
}
