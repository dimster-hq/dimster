package dev.dimster.engines.drainbacklog;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.dimster.ScenarioExpander;
import dev.dimster.ScenarioExpander.TestPoint;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * The drain-backlog warm-up is a workload-level setting: the engine runs the
 * workload's first test point once before it, unrecorded. Scenarios cannot
 * set it, and it is not part of what a reused backlog must match.
 */
public class DrainWarmupExpansionTest {

    // As BenchmarkRunner parses workloads.
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

    private static DrainBacklogWorkload workload(String drainConfig, String scenarios) throws Exception {
        return YAML.readValue("""
                name: w
                drainBacklogConfig:
                %s
                baseWorkload:
                  topics: 1
                  partitionsPerTopic: 1
                %s
                """.formatted(drainConfig, scenarios), DrainBacklogWorkload.class);
    }

    @Test
    public void off_by_default() throws Exception {
        assertFalse(workload("  backlogSizeGB: 1", "").drainBacklogConfig.warmup);
    }

    @Test
    public void scenarios_cannot_set_it() {
        assertThrows(UnrecognizedPropertyException.class, () -> workload("  warmup: true", """
                scenarios:
                  - name: a
                    drainBacklogConfig:
                      warmup: false
                """));
    }

    @Test
    public void warmup_does_not_change_the_backlog_identity() throws Exception {
        DrainBacklogWorkload wl = workload("  warmup: true\n  reuseBacklog: true", """
                scenarios:
                  - name: a
                    consumersPerGroup: [1, 2]
                """);
        List<TestPoint> points = ScenarioExpander.expand(wl.baseWorkload, wl.scenarios, null, wl.drainBacklogConfig);
        assertEquals(BacklogReuse.identity(points.get(0)), BacklogReuse.identity(points.get(1)));
    }
}
