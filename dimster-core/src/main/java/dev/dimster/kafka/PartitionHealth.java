package dev.dimster.kafka;

import dev.dimster.spi.kafka.PartitionState;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Comparator;

/**
 * Whether a set of partitions is settled enough to measure against: every
 * partition has a leader, a full ISR, and is led by its preferred replica.
 * After broker restarts (typically from a hook) leadership is skewed and
 * followers are catching up, so measuring straight away would compare an
 * unbalanced cluster with a balanced one. Pure, so the rules are testable;
 * {@link KafkaCluster#awaitSettled} does the waiting.
 *
 * @param notPreferred partitions with a full ISR led by a non-preferred replica,
 *                     which a preferred leader election can fix
 */
public record PartitionHealth(int partitions, int leaderless, int underReplicated, Set<TopicPartition> notPreferred) {

    private static final Comparator<TopicPartition> ORDER =
            Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition);

    public static PartitionHealth assess(Map<TopicPartition, PartitionState> states) {
        int leaderless = 0;
        int underReplicated = 0;
        Set<TopicPartition> notPreferred = new TreeSet<>(ORDER);
        for (var e : states.entrySet()) {
            PartitionState s = e.getValue();
            if (s.leader() < 0) {
                leaderless++;
            } else if (s.isr().size() < s.replicas().size()) {
                underReplicated++;
            } else if (!s.replicas().isEmpty() && s.leader() != s.replicas().get(0)) {
                notPreferred.add(e.getKey());
            }
        }
        return new PartitionHealth(states.size(), leaderless, underReplicated, notPreferred);
    }

    public boolean settled() {
        return leaderless == 0 && underReplicated == 0 && notPreferred.isEmpty();
    }

    @Override
    public String toString() {
        return "%d partitions: %d leaderless, %d under-replicated, %d not on their preferred leader"
                .formatted(partitions, leaderless, underReplicated, notPreferred.size());
    }
}
