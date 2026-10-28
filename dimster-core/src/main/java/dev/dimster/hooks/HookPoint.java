package dev.dimster.hooks;

/**
 * The points in a workload's lifecycle at which user hooks run. Hooks let a
 * benchmark act on its environment (restart brokers, evict caches, call a
 * cloud API) without the coordinator knowing anything about that
 * environment: the coordinator only runs the configured script at each point.
 */
public enum HookPoint {
    /** Once, before the first test point of the workload (again on resume). */
    BEFORE_WORKLOAD("beforeWorkload"),
    /** Once, after the last test point of a workload that completed. */
    AFTER_WORKLOAD("afterWorkload"),
    /** Before each test point, ahead of its topic and client setup. */
    BEFORE_TEST_POINT("beforeTestPoint"),
    /** After each test point, once its clients are stopped and its topics deleted. */
    AFTER_TEST_POINT("afterTestPoint"),
    /** Drain-backlog only: after the backlog exists and before consumers start. */
    BEFORE_DRAIN("beforeDrain");

    private final String yamlName;

    HookPoint(String yamlName) {
        this.yamlName = yamlName;
    }

    /** The name used in workload YAML and in hook environments. */
    public String yamlName() {
        return yamlName;
    }
}
