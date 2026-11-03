package dev.dimster.engines.drainbacklog;

import dev.dimster.ScenarioExpander.TestPoint;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The rules for persistent backlogs ({@code drainBacklogConfig.reuseTopic}):
 * backlogs kept on the cluster, never deleted by Dimster, in topics named
 * from their build identity ({@link BacklogReuse#persistentTopicPrefix}).
 * A test point whose topics already exist drains them instead of building,
 * whichever workload or run built them. Pure functions, so the engine keeps
 * only the cluster calls.
 *
 * The name guarantees the build settings match, but not that the build
 * finished: a run stopped mid-build leaves a partial backlog behind. So
 * existing topics are only drained once their shape and record count show
 * a complete build; anything else is rebuilt.
 */
public final class TopicReuse {

    /** What the cluster holds under a persistent backlog's topic names. */
    public sealed interface Assessment permits Absent, Usable, Unusable {}

    /** None of the topics exist: build the backlog into them. */
    public record Absent() implements Assessment {}

    /** A complete backlog: drain it. {@code records} is the sum of its end offsets. */
    public record Usable(long records, Map<TopicPartition, Long> endOffsets) implements Assessment {}

    /** The topics exist but cannot be drained as-is: delete and rebuild them. */
    public record Unusable(String reason) implements Assessment {}

    private TopicReuse() {}

    /** The topics of the point's persistent backlog, numbered like any test topic. */
    public static List<String> topicNames(TestPoint point, String identity) {
        String prefix = BacklogReuse.persistentTopicPrefix(identity);
        List<String> names = new ArrayList<>(point.topics());
        for (int i = 0; i < point.topics(); i++) {
            names.add(String.format("%s-%04d", prefix, i));
        }
        return names;
    }

    /**
     * The fewest records a complete build of the point's backlog holds: the
     * build stops once records x messageSize x consumer groups reaches
     * backlogSizeGB.
     */
    public static long minRecords(TestPoint point) {
        long target = (long) (point.drainBacklogConfig().backlogSizeGB * 1_000_000_000.0);
        long bytesPerRecord = (long) point.messageSize() * Math.max(1, point.consumerGroupsPerTopic());
        return (target + bytesPerRecord - 1) / bytesPerRecord;
    }

    /**
     * Whether the topics on the cluster hold a complete backlog.
     *
     * @param partitionCounts partitions of each of {@code names} that exists
     * @param startOffsets    start offsets of every partition, once all topics exist
     * @param endOffsets      end offsets of every partition, once all topics exist
     */
    public static Assessment assess(List<String> names, int partitionsPerTopic, long minRecords,
                                    Map<String, Integer> partitionCounts,
                                    Map<TopicPartition, Long> startOffsets,
                                    Map<TopicPartition, Long> endOffsets) {
        if (partitionCounts.isEmpty()) {
            return new Absent();
        }
        for (String name : names) {
            Integer partitions = partitionCounts.get(name);
            if (partitions == null) {
                return new Unusable("topic " + name + " is missing");
            }
            if (partitions != partitionsPerTopic) {
                return new Unusable("topic " + name + " has " + partitions + " partitions, expected "
                        + partitionsPerTopic);
            }
        }
        Optional<String> head = BacklogReuse.whyHeadDeleted(startOffsets);
        if (head.isPresent()) {
            return new Unusable(head.get());
        }
        long records = endOffsets.values().stream().mapToLong(Long::longValue).sum();
        if (records < minRecords) {
            return new Unusable("it holds " + records + " records, a complete build at least " + minRecords
                    + ": likely an interrupted build");
        }
        return new Usable(records, endOffsets);
    }
}
