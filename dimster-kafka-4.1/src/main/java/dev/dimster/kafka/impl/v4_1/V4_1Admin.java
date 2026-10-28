package dev.dimster.kafka.impl.v4_1;

import dev.dimster.spi.kafka.GroupKind;
import dev.dimster.spi.kafka.PartitionState;
import dev.dimster.spi.kafka.RawAdmin;
import dev.dimster.spi.kafka.ShareGroupInfo;
import dev.dimster.spi.kafka.ShareGroupMember;
import dev.dimster.spi.kafka.ShareGroupOffsetInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.ElectionNotNeededException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Kafka 4.1 implementation of {@link RawAdmin}.
 *
 * <p>All blocking admin calls go through {@link #unwrap}, which converts checked
 * concurrency exceptions into {@link RuntimeException}. The dimster-side wrapper
 * ({@code KafkaCluster}) catches those at its own boundary and converts them to
 * domain errors. This matches the pre-SPI behaviour - the original
 * {@code KafkaCluster} caught {@code Throwable} on every admin call.
 *
 * <p>The 30-second timeouts on the offset-related calls match what
 * {@code KafkaCluster.getConsumerGroupLag} and {@code ShareGroupLagMonitor.poll}
 * used pre-refactor, and serve as a safety net against an unresponsive broker
 * locking up a poll thread.
 *
 * <p><b>Difference from V4_2_0Admin:</b> {@link #shareGroupLag} computes lag
 * client-side (committed-offset minus end-offset) because 4.1's
 * {@code listShareGroupOffsets} returns plain {@link OffsetAndMetadata} - the
 * broker-side lag field on {@code SharePartitionOffsetInfo} only landed in 4.2.
 * Two admin RPCs per poll instead of one. The "committed offset" of a share
 * group is the SPSO (share-partition start offset), so the lag value here
 * approximates rather than exactly matches what 4.2's broker-side computation
 * returns - good enough for trend / alerting use cases.
 */
public class V4_1Admin implements RawAdmin {

    private static final long ADMIN_OP_TIMEOUT_SECONDS = 30;

    private final AdminClient admin;

    public V4_1Admin(AdminClient admin) {
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
        // GroupType.CLASSIC + GroupType.CONSUMER together cover both the legacy
        // and KIP-848 protocols - dimster's KafkaCluster.deleteTestConsumerGroups
        // queried both originally. SHARE is the 4.2 share-group type.
        Set<GroupType> types = switch (kind) {
            case CLASSIC_OR_CONSUMER -> Set.of(GroupType.CLASSIC, GroupType.CONSUMER);
            case SHARE -> Set.of(GroupType.SHARE);
        };
        var options = new ListGroupsOptions().withTypes(types);
        return unwrap(() -> {
            var listings = admin.listGroups(options).all().get();
            var ids = new java.util.LinkedHashSet<String>(listings.size());
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

    @Override
    public Map<TopicPartition, Long> shareGroupLag(String groupId) {
        // 4.1's listShareGroupOffsets returns Map<TopicPartition, OffsetAndMetadata> -
        // the SPSO (share-partition start offset) per partition, no broker-side lag
        // field. We approximate lag client-side: lag = endOffset - committedOffset.
        // Same shape as KafkaCluster.getConsumerGroupLag for classic groups; two
        // admin RPCs per poll instead of one.
        var spec = Map.of(groupId, new ListShareGroupOffsetsSpec());
        var perGroup = unwrap(() -> admin.listShareGroupOffsets(spec)
                .all()
                .get(ADMIN_OP_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        var committed = perGroup.get(groupId);
        if (committed == null || committed.isEmpty()) {
            return Map.of();
        }

        Map<TopicPartition, OffsetSpec> request = new HashMap<>(committed.size());
        for (TopicPartition tp : committed.keySet()) {
            request.put(tp, OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = unwrap(() ->
                admin.listOffsets(request).all().get(ADMIN_OP_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        var out = new LinkedHashMap<TopicPartition, Long>(committed.size());
        for (var e : committed.entrySet()) {
            TopicPartition tp = e.getKey();
            long c = e.getValue() != null ? e.getValue().offset() : 0;
            long endOff = endOffsets.containsKey(tp) ? endOffsets.get(tp).offset() : 0;
            out.put(tp, Math.max(0, endOff - c));
        }
        return out;
    }

    @Override
    public Map<TopicPartition, ShareGroupOffsetInfo> shareGroupOffsets(String groupId) {
        // Same shape as shareGroupLag above but keeps both startOffset and lag.
        // 4.1's listShareGroupOffsets returns Map<TopicPartition, OffsetAndMetadata>
        // where offset() is the SPSO. Lag is computed client-side as
        // endOffset - SPSO (broker-side lag landed in 4.2).
        var spec = Map.of(groupId, new ListShareGroupOffsetsSpec());
        var perGroup = unwrap(() -> admin.listShareGroupOffsets(spec)
                .all()
                .get(ADMIN_OP_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        var committed = perGroup.get(groupId);
        if (committed == null || committed.isEmpty()) {
            return Map.of();
        }

        Map<TopicPartition, OffsetSpec> request = new HashMap<>(committed.size());
        for (TopicPartition tp : committed.keySet()) {
            request.put(tp, OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = unwrap(() ->
                admin.listOffsets(request).all().get(ADMIN_OP_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        var out = new LinkedHashMap<TopicPartition, ShareGroupOffsetInfo>(committed.size());
        for (var e : committed.entrySet()) {
            TopicPartition tp = e.getKey();
            long startOffset = e.getValue() != null ? e.getValue().offset() : 0;
            long endOff = endOffsets.containsKey(tp) ? endOffsets.get(tp).offset() : 0;
            long lag = Math.max(0, endOff - startOffset);
            out.put(tp, new ShareGroupOffsetInfo(startOffset, lag));
        }
        return out;
    }

    @Override
    public ShareGroupInfo describeShareGroup(String groupId) {
        ShareGroupDescription desc = unwrap(() -> admin.describeShareGroups(List.of(groupId))
                .describedGroups()
                .get(groupId)
                .get(ADMIN_OP_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        List<ShareGroupMember> members = new ArrayList<>(desc.members().size());
        for (var m : desc.members()) {
            // ShareMemberAssignment.topicPartitions() can be empty for members
            // that haven't received an assignment yet; we forward as-is.
            Set<TopicPartition> assignment = m.assignment() != null
                    ? m.assignment().topicPartitions()
                    : Set.of();
            members.add(new ShareGroupMember(m.consumerId(), m.clientId(), m.host(), assignment));
        }
        // groupState() exists on ShareGroupDescription since 4.1; toString()
        // gives "STABLE", "EMPTY", "DEAD", etc. — the textual form keeps the
        // SPI free of Kafka's GroupState enum (which has shifted across
        // releases).
        String state = desc.groupState() != null ? desc.groupState().toString() : "UNKNOWN";
        int coordinatorId = desc.coordinator() != null ? desc.coordinator().id() : -1;
        return new ShareGroupInfo(desc.groupId(), state, coordinatorId, members);
    }

    @Override
    public void alterShareGroupOffsets(String groupId, Map<TopicPartition, Long> startOffsets) {
        // Fire-and-forget on purpose: matches pre-SPI behaviour in
        // KafkaCluster.createShareGroup, which similarly never awaited the result.
        // If we ever want to surface failures here we can switch to .all().get();
        // doing so is a behaviour change so it's left to a follow-up.
        admin.alterShareGroupOffsets(groupId, startOffsets);
    }

    @Override
    public void deleteShareGroups(Collection<String> groupIds) {
        unwrap(() -> {
            admin.deleteShareGroups(groupIds).all().get();
            return null;
        });
    }

    @Override
    public void close() {
        admin.close();
    }

    /**
     * Runs a blocking admin call, converting checked concurrency exceptions into
     * {@link RuntimeException}. {@code Future.get()} throws InterruptedException
     * (re-interrupts the thread) and ExecutionException (unwraps the cause); both
     * are pointless to bubble up as checked exceptions through the SPI when every
     * caller wraps them anyway.
     */
    private static <T> T unwrap(AdminCall<T> call) {
        try {
            return call.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for admin operation", e);
        } catch (java.util.concurrent.ExecutionException e) {
            // Surface the underlying Kafka exception, not the ExecutionException wrapper.
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
