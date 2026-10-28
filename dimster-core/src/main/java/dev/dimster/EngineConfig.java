package dev.dimster;

import com.fasterxml.jackson.annotation.JsonInclude;
import dev.dimster.hooks.HooksConfig;

/**
 * Operational settings shared across all engine modes.
 * Parsed from the top-level "engineConfig:" section of workload YAML files.
 */
public class EngineConfig {
    public int maxConsumerRecreations = 0;
    public boolean perPartitionMetrics = false;
    public boolean perConsumerMetrics = false;

    /** Minutes to wait between test points, giving Kafka time to stabilize
     *  and metric scrapers time to collect all stats before topics/groups are deleted. */
    public int cooldownMinutes = 0;

    /** When true, every successful produce and every consumed record is logged
     *  with its partition and timestamp. Intended for debugging and correctness
     *  inspection at low rates. The coordinator forces this off if any test
     *  point has a producer rate > 50 msg/s, and always off for the explore
     *  engine (where rate is dynamic). */
    public boolean printMessages = false;

    /** When true, the printed lines also show each record's key and contents
     *  (dataset records as JSON; payload dataset records as type, size and
     *  first bytes). Turns printing on by itself, under the same rate
     *  limit as {@link #printMessages}. */
    public boolean printContents = false;

    /** Whether either printing option is set. */
    public boolean printingRequested() {
        return printMessages || printContents;
    }

    /** When true, the planned VirtualTopology for each test point is dumped to
     *  the coordinator log at the start of that test point — workers and the
     *  producers/consumers assigned to them, with topic and pinned-partition
     *  details. Independent of the topology JSON file (always written to the
     *  results directory regardless of this flag); this only controls log
     *  visibility. */
    public boolean printTopology = false;

    /** Scripts to run at points in the workload's lifecycle (see {@link dev.dimster.hooks.Hooks}).
     *  Null when the workload configures none, keeping result JSON unchanged. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public HooksConfig hooks;
}
