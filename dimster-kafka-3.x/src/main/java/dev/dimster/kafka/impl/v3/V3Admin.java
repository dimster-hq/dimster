package dev.dimster.kafka.impl.v3;

import dev.dimster.spi.kafka.GroupKind;
import dev.dimster.spi.kafka.PartitionState;
import dev.dimster.spi.kafka.RawAdmin;
import dev.dimster.spi.kafka.ShareGroupInfo;
import dev.dimster.spi.kafka.ShareGroupOffsetInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.ElectionNotNeededException;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Kafka 3.x implementation of {@link RawAdmin}.
 *
 * <p>Differences from the 4.2.0 impl:
 * <ul>
 *   <li>{@link #listGroupIds} uses the older {@code admin.listConsumerGroups()} for
 *       classic/consumer groups - 3.x predates the {@code listGroups(...).withTypes(...)}
 *       form, and the older API returns exactly what we want.
 *       {@link GroupKind#SHARE} short-circuits to an empty set since share groups
 *       don't exist anywhere in the 3.x line.</li>
 *   <li>The three share-group methods at the bottom throw
 *       {@link UnsupportedOperationException}. dimster-core's capability check
 *       should keep them from ever being called - this is a backstop.</li>
 * </ul>
 *
 * <p>Everything else is the same shape as the 4.2.0 impl. Kept as a separate class
 * (not a shared base) so that a future point-version drift in the admin API can
 * be absorbed in exactly one impl rather than rippled through a hierarchy.
 */
public class V3Admin implements RawAdmin {

    private static final long ADMIN_OP_TIMEOUT_SECONDS = 30;
    private static final String SHARE_NOT_SUPPORTED =
            "Share groups are not supported by the Kafka 3.x client. Build dimster against 4.2+.";

    private final AdminClient admin;

    public V3Admin(AdminClient admin) {
        this.admin = admin;
    }

    @Override
    public Set<String> listTopicNames() {
        return unwrap(() -> admin.listTopics().names().get());
    }

    @Override
    public void createTopic(NewTopic topic) {
        unwrap(() -> {
            admin.createTopics(List.of(topic)).all().get();
            return null;
        });
    }

    @Override
    public void deleteTopics(Set<String> topicNames) {
        unwrap(() -> {
            admin.deleteTopics(topicNames).all().get();
            return null;
        });
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committedOffsets(String groupId) {
        return unwrap(() -> admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(ADMIN_OP_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Set<TopicPartition> partitions) {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>(partitions.size());
        for (TopicPartition tp : partitions) {
            request.put(tp, OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> raw = unwrap(() ->
                admin.listOffsets(request).all().get(ADMIN_OP_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Map<TopicPartition, Long> out = new LinkedHashMap<>(raw.size());
        for (var e : raw.entrySet()) {
            out.put(e.getKey(), e.getValue().offset());
        }
        return out;
    }

    @Override
    public Map<TopicPartition, Long> startOffsets(Set<TopicPartition> partitions) {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>(partitions.size());
        for (TopicPartition tp : partitions) {
            request.put(tp, OffsetSpec.earliest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> raw = unwrap(() ->
                admin.listOffsets(request).all().get(ADMIN_OP_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Map<TopicPartition, Long> out = new LinkedHashMap<>(raw.size());
        for (var e : raw.entrySet()) {
            out.put(e.getKey(), e.getValue().offset());
        }
        return out;
    }

    @Override
    public Map<TopicPartition, PartitionState> describePartitions(Set<String> topicNames) {
        Map<String, TopicDescription> topics = unwrap(() -> admin.describeTopics(topicNames)
                .allTopicNames().get(ADMIN_OP_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        Map<TopicPartition, PartitionState> out = new LinkedHashMap<>();
        for (TopicDescription topic : topics.values()) {
            for (TopicPartitionInfo p : topic.partitions()) {
                Node leader = p.leader();
                out.put(new TopicPartition(topic.name(), p.partition()), new PartitionState(
                        leader == null || leader.isEmpty() ? -1 : leader.id(),
                        p.replicas().stream().map(Node::id).toList(),
                        p.isr().stream().map(Node::id).toList()));
            }
        }
        return out;
    }

    @Override
    public void electPreferredLeaders(Set<TopicPartition> partitions) {
        Map<TopicPartition, Optional<Throwable>> results = unwrap(() -> admin
                .electLeaders(ElectionType.PREFERRED, partitions)
                .partitions().get(ADMIN_OP_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        for (var e : results.entrySet()) {
            // ElectionNotNeeded: the preferred replica already leads.
            if (e.getValue().isPresent() && !(e.getValue().get() instanceof ElectionNotNeededException)) {
                throw new RuntimeException("Preferred leader election failed for " + e.getKey()
                        + ": " + e.getValue().get().getMessage(), e.getValue().get());
            }
        }
    }

    @Override
    public Set<String> listGroupIds(GroupKind kind) {
        if (kind == GroupKind.SHARE) {
            // Listing share groups on 3.x is well-defined as "none exist".
            // Returning empty (rather than throwing) lets the caller treat the lookup
            // as a no-op without needing to inspect Capabilities first.
            return Set.of();
        }
        return unwrap(() -> {
            var listings = admin.listConsumerGroups().all().get();
            var ids = new LinkedHashSet<String>(listings.size());
            for (var listing : listings) {
                ids.add(listing.groupId());
            }
            return ids;
        });
    }

    @Override
    public void deleteConsumerGroups(Collection<String> groupIds) {
        unwrap(() -> {
            admin.deleteConsumerGroups(groupIds).all().get();
            return null;
        });
    }

    // --- Share group operations: not supported on 3.x -----------------------

    @Override
    public Map<TopicPartition, Long> shareGroupLag(String groupId) {
        throw new UnsupportedOperationException(SHARE_NOT_SUPPORTED);
    }

    @Override
    public Map<TopicPartition, ShareGroupOffsetInfo> shareGroupOffsets(String groupId) {
        throw new UnsupportedOperationException(SHARE_NOT_SUPPORTED);
    }

    @Override
    public ShareGroupInfo describeShareGroup(String groupId) {
        throw new UnsupportedOperationException(SHARE_NOT_SUPPORTED);
    }

    @Override
    public void alterShareGroupOffsets(String groupId, Map<TopicPartition, Long> startOffsets) {
        throw new UnsupportedOperationException(SHARE_NOT_SUPPORTED);
    }

    @Override
    public void deleteShareGroups(Collection<String> groupIds) {
        throw new UnsupportedOperationException(SHARE_NOT_SUPPORTED);
    }

    @Override
    public void close() {
        admin.close();
    }

    /**
     * See {@code V4_2_0Admin.unwrap} for the rationale; same pattern repeated
     * here intentionally rather than pulled into a base class, since the
     * per-version impls are otherwise self-contained.
     */
    private static <T> T unwrap(AdminCall<T> call) {
        try {
            return call.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for admin operation", e);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new RuntimeException("Admin operation failed: " + cause.getMessage(), cause);
        } catch (Exception e) {
            throw new RuntimeException("Admin operation failed: " + e.getMessage(), e);
        }
    }

    @FunctionalInterface
    private interface AdminCall<T> {
        T run() throws Exception;
    }
}
