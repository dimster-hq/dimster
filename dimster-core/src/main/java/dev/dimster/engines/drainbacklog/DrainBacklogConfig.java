package dev.dimster.engines.drainbacklog;

/**
 * Configuration for drain-backlog tests.
 */
public class DrainBacklogConfig {
    /** Size of backlog to build before draining (in GB). Supports fractions, e.g. 0.05 = 50MB. */
    public double backlogSizeGB = 1;

    /** Producer rate during drain phase. 0 means producers are paused (pure consumer drain). */
    public int backlogProducerRate = 0;

    /** Producer rate while building backlog. -1 means use the baseWorkload producerRate. */
    public int buildRate = -1;

    /** Stats sampling interval in milliseconds. */
    public int sampleRateMillis = 10000;

    /** Maximum time to wait for drain to complete (minutes). 0 means no limit. */
    public int drainTimeoutMinutes = 0;

    /**
     * Run the workload's first test point once before it, unrecorded (a test
     * point 0 that is in no result), so the first recorded drain does not run
     * on cold client and broker JVMs. Workload-level only: not settable in
     * scenarios.
     */
    public boolean warmup = false;

    /**
     * Build a backlog once and drain it again in each following test point
     * that differs only in drain-side settings (see {@link BacklogReuse}).
     * Workload-level only: not sweepable in scenarios.
     */
    public boolean reuseBacklog = false;

    /**
     * Before a drain that a beforeDrain hook may have disturbed, or of a
     * reused backlog, how long to wait for every partition to have a full ISR
     * and its preferred leader. Workload-level only.
     */
    public int settleTimeoutMinutes = 10;
}
