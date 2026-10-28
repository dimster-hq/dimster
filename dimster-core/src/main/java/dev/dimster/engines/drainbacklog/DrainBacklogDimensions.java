package dev.dimster.engines.drainbacklog;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Dimension-aware version of {@link DrainBacklogConfig} for use in scenarios.
 * Each field is a nullable list — null means "inherit from the workload-level
 * {@code drainBacklogConfig}." Lists with multiple elements define co-varying
 * dimensions alongside other scenario fields (e.g. consumersPerGroup).
 *
 * Jackson's ACCEPT_SINGLE_VALUE_AS_ARRAY coerces scalar YAML values
 * (e.g. {@code backlogSizeGB: 10}) into single-element lists automatically.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DrainBacklogDimensions {
    /** Size of backlog to build before draining (in GB). Supports fractions, e.g. 0.05 = 50MB. */
    public List<Double> backlogSizeGB;

    /** Producer rate during drain phase. -1 means producers are paused. */
    public List<Integer> backlogProducerRate;

    /** Producer rate while building backlog. -1 means use the baseWorkload producerRate. */
    public List<Integer> buildRate;

    /** Stats sampling interval in milliseconds. */
    public List<Integer> sampleRateMillis;

    /** Maximum time to wait for drain to complete (minutes). 0 means no limit. */
    public List<Integer> drainTimeoutMinutes;

    /** Warm up before this scenario (see {@link DrainBacklogConfig#warmup}). One value, not a sweep; null inherits. */
    public Boolean warmup;

    /**
     * Validate that all non-null, multi-element lists have the same length.
     * Call this before resolving against defaults so that mismatches between
     * user-specified fields are caught early (before resolve() promotes
     * missing fields to singletons, masking the mismatch).
     *
     * @throws IllegalArgumentException if any two multi-element lists differ in size
     */
    public void validate(String scenarioName) {
        int n = -1;
        String firstField = null;
        n = checkField(scenarioName, "backlogSizeGB", backlogSizeGB, n, firstField);
        if (n > 1 && firstField == null) firstField = "backlogSizeGB";
        n = checkField(scenarioName, "backlogProducerRate", backlogProducerRate, n, firstField);
        if (n > 1 && firstField == null) firstField = "backlogProducerRate";
        n = checkField(scenarioName, "buildRate", buildRate, n, firstField);
        if (n > 1 && firstField == null) firstField = "buildRate";
        n = checkField(scenarioName, "sampleRateMillis", sampleRateMillis, n, firstField);
        if (n > 1 && firstField == null) firstField = "sampleRateMillis";
        n = checkField(scenarioName, "drainTimeoutMinutes", drainTimeoutMinutes, n, firstField);
    }

    private static int checkField(String scenarioName, String fieldName, List<?> list,
                                   int currentMax, String firstField) {
        if (list == null || list.size() <= 1) {
            return currentMax;
        }
        if (currentMax > 1 && list.size() != currentMax) {
            throw new IllegalArgumentException(
                    "Scenario '%s': drainBacklogConfig.%s has %d elements but %s has %d"
                            .formatted(scenarioName, fieldName, list.size(), firstField, currentMax));
        }
        return Math.max(currentMax, list.size());
    }
}
