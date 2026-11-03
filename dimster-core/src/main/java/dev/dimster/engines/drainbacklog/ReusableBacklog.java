package dev.dimster.engines.drainbacklog;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A built backlog that test points drain, one after another, instead of each
 * building their own (see {@link BacklogReuse}). With reuse, every backlog is
 * built outside the test points, before the first one that needs it, so a
 * test point only ever drains. Kept in the in-progress result so a resumed
 * run can pick it up, after checking it is still intact.
 */
public class ReusableBacklog {
    /** Canonical form of the build-side settings; reuse requires an exact match. */
    public String identity;
    /** Index of the test point it was built before (for the warm-up, the point warmed up). */
    public int builtAtTestPoint;
    public String builtAtLabel;
    /** Built for the warm-up, before the workload's first test point. */
    public boolean builtForWarmUp;
    /**
     * Found complete on the cluster (reuseTopic) rather than built: by an
     * earlier workload or run, or earlier in this workload. Then
     * {@code builtAt*} is the test point it was found for.
     */
    public boolean adopted;
    /** How long the build took; reported on the backlog's first recorded drain. */
    public long buildDurationMs;
    /** The topics holding the backlog (Flink input topics when there is a Flink stage). */
    public List<String> topics = new ArrayList<>();
    /** Records produced into the backlog, per consumer group. */
    public long records;
    /** End offset of every partition once built, keyed {@code topic-partition}. */
    public Map<String, Long> endOffsets = new LinkedHashMap<>();
    /** Index of the recorded test point that drained it first; null until one has. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Integer firstDrainedAt;
    /** The warm-up drained it (the warm-up is never recorded). */
    public boolean drainedByWarmUp;

    /** A backlog in {@code topics} whose partitions end at {@code endOffsets}, for the given test point. */
    public static ReusableBacklog of(String identity, int testPoint, String label, List<String> topics,
                                     Map<TopicPartition, Long> endOffsets) {
        var b = new ReusableBacklog();
        b.identity = identity;
        b.builtAtTestPoint = testPoint;
        b.builtAtLabel = label;
        b.topics = new ArrayList<>(topics);
        endOffsets.forEach((tp, offset) -> b.endOffsets.put(tp.toString(), offset));
        return b;
    }

    /**
     * What a recorded drain reports about its backlog: the earlier recorded
     * test point that drained it ({@code reusedFrom}, null on its first
     * recorded drain), the build time (on the first recorded drain only, so
     * each build appears once in the results), and whether the warm-up
     * drained it before (on the first recorded drain only).
     */
    public record Provenance(Integer reusedFrom, long buildDurationMs, boolean drainedByWarmUp) {}

    /**
     * Notes a drain of this backlog and returns what a recorded drain
     * reports, or null for the warm-up's drain, which is not recorded.
     */
    public Provenance recordDrain(int testPoint, boolean warmUp) {
        if (warmUp) {
            drainedByWarmUp = true;
            return null;
        }
        if (firstDrainedAt == null) {
            firstDrainedAt = testPoint;
            return new Provenance(null, buildDurationMs, drainedByWarmUp);
        }
        return new Provenance(firstDrainedAt, 0, false);
    }
}
