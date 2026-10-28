package dev.dimster.flink;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Dimension-aware version of {@link FlinkSpec} for use in scenarios, like
 * {@code RunDimensions} for {@code runConfig}. Each field is a nullable list:
 * null inherits from {@code baseWorkload.flink}, and multi-element lists
 * co-vary with the scenario's other dimensions. The maps of {@link FlinkSpec}
 * (config and table options) are not sweepable and always come from the base.
 *
 * Jackson's ACCEPT_SINGLE_VALUE_AS_ARRAY coerces scalar YAML values into
 * single-element lists.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FlinkDimensions {
    public List<String> sql;
    public List<Integer> parallelism;
    public List<Integer> sourceParallelism;
    public List<Integer> sinkParallelism;
    public List<Long> checkpointIntervalMs;
    public List<FlinkSpec.DeliveryGuarantee> deliveryGuarantee;
    public List<Integer> outputPartitionsPerTopic;

    /** Every list, in field order, for length checks. */
    public List<List<?>> lists() {
        return java.util.Arrays.asList(sql, parallelism, sourceParallelism, sinkParallelism,
                checkpointIntervalMs, deliveryGuarantee, outputPartitionsPerTopic);
    }

    /**
     * Validate that all multi-element lists have the same length, before they
     * are resolved against the base (which would promote missing fields to
     * singletons and mask a mismatch).
     */
    public void validate(String scenarioName) {
        String[] names = {"sql", "parallelism", "sourceParallelism", "sinkParallelism",
                "checkpointIntervalMs", "deliveryGuarantee", "outputPartitionsPerTopic"};
        List<List<?>> lists = lists();
        int n = -1;
        String first = null;
        for (int i = 0; i < lists.size(); i++) {
            List<?> list = lists.get(i);
            if (list == null || list.size() <= 1) continue;
            if (n > 1 && list.size() != n) {
                throw new IllegalArgumentException("Scenario '%s': flink.%s has %d elements but flink.%s has %d"
                        .formatted(scenarioName, names[i], list.size(), first, n));
            }
            n = list.size();
            first = names[i];
        }
    }

    /** Applies this scenario's values at test point {@code index} onto a copy of {@code base}. */
    public FlinkSpec applyAt(FlinkSpec base, int index) {
        FlinkSpec point = base != null ? base.copy() : new FlinkSpec();
        if (sql != null && !sql.isEmpty()) point.sql = at(sql, index);
        if (parallelism != null && !parallelism.isEmpty()) point.parallelism = at(parallelism, index);
        if (sourceParallelism != null && !sourceParallelism.isEmpty()) {
            point.sourceParallelism = at(sourceParallelism, index);
        }
        if (sinkParallelism != null && !sinkParallelism.isEmpty()) {
            point.sinkParallelism = at(sinkParallelism, index);
        }
        if (checkpointIntervalMs != null && !checkpointIntervalMs.isEmpty()) {
            point.checkpointIntervalMs = at(checkpointIntervalMs, index);
        }
        if (deliveryGuarantee != null && !deliveryGuarantee.isEmpty()) {
            point.deliveryGuarantee = at(deliveryGuarantee, index);
        }
        if (outputPartitionsPerTopic != null && !outputPartitionsPerTopic.isEmpty()) {
            point.outputPartitionsPerTopic = at(outputPartitionsPerTopic, index);
        }
        return point;
    }

    private static <T> T at(List<T> list, int index) {
        return list.size() == 1 ? list.getFirst() : list.get(index);
    }
}
