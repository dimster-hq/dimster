package dev.dimster.hooks;

import java.util.List;

/**
 * Per-invocation facts the engine hands to a hook: which test point is
 * running and what Kafka resources it uses. The hook receives them as
 * {@code DIMSTER_*} environment variables (see {@link Hooks}), so a script
 * can target, say, only the topics that hold the backlog.
 *
 * @param testPointIndex 0-based index of the test point, or -1 for workload-level hooks
 * @param testPointCount number of test points in the workload, or 0 if unknown
 * @param topics         topics holding data at this point (empty when none or unknown)
 */
public record HookContext(int testPointIndex, int testPointCount, String testPointLabel,
                          String scenarioName, String bootstrapServers, List<String> topics) {

    public HookContext {
        topics = topics != null ? List.copyOf(topics) : List.of();
    }

    /** Context for hooks that run outside any test point. */
    public static HookContext workload() {
        return new HookContext(-1, 0, null, null, null, List.of());
    }
}
