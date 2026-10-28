package dev.dimster.engines.drainbacklog;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.dimster.ScenarioExpander;
import dev.dimster.ScenarioExpander.TestPoint;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Which test points get a drain-backlog warm-up: the first test point of each
 * scenario with warmup on (set for the workload or per scenario), once per
 * scenario rather than once per repeat.
 */
public class DrainWarmupExpansionTest {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

    private static List<Boolean> warmups(String drainConfig, String scenarios) throws Exception {
        DrainBacklogWorkload wl = YAML.readValue("""
                name: w
                drainBacklogConfig:
                %s
                baseWorkload:
                  topics: 1
                  partitionsPerTopic: 1
                %s
                """.formatted(drainConfig, scenarios), DrainBacklogWorkload.class);
        return ScenarioExpander.expand(wl.baseWorkload, wl.scenarios, null, wl.drainBacklogConfig)
                .stream().map(p -> p.drainBacklogConfig().warmup).toList();
    }

    @Test
    public void off_by_default() throws Exception {
        assertEquals(List.of(false, false), warmups("  backlogSizeGB: 1", """
                scenarios:
                  - name: s
                    consumersPerGroup: [1, 2]
                """));
    }

    @Test
    public void workload_setting_warms_up_each_scenario_once() throws Exception {
        assertEquals(List.of(true, false, false, true, false), warmups("  warmup: true", """
                scenarios:
                  - name: a
                    consumersPerGroup: [1, 2, 3]
                  - name: b
                    consumersPerGroup: [1, 2]
                """));
    }

    @Test
    public void scenario_setting_overrides_the_workload() throws Exception {
        assertEquals(List.of(true, false, false), warmups("  warmup: true", """
                scenarios:
                  - name: a
                  - name: b
                    drainBacklogConfig:
                      warmup: false
                  - name: c
                    drainBacklogConfig:
                      warmup: false
                """));
        assertEquals(List.of(false, true), warmups("  backlogSizeGB: 1", """
                scenarios:
                  - name: a
                  - name: b
                    drainBacklogConfig:
                      warmup: true
                """));
    }

    @Test
    public void repeats_warm_up_only_before_the_first_run() throws Exception {
        assertEquals(List.of(true, false, false), warmups("  warmup: true", """
                scenarios:
                  - name: r
                    repeat: 3
                """));
    }

    @Test
    public void workload_without_scenarios_warms_up_its_one_test_point() throws Exception {
        assertEquals(List.of(true), warmups("  warmup: true", ""));
    }

    @Test
    public void warmup_cannot_be_swept() {
        assertThrows(Exception.class, () -> warmups("  backlogSizeGB: 1", """
                scenarios:
                  - name: a
                    drainBacklogConfig:
                      warmup: [true, false]
                """));
    }

    @Test
    public void warmup_does_not_change_the_backlog_identity() throws Exception {
        DrainBacklogWorkload wl = YAML.readValue("""
                name: w
                drainBacklogConfig:
                  warmup: true
                  reuseBacklog: true
                baseWorkload:
                  topics: 1
                  partitionsPerTopic: 1
                scenarios:
                  - name: a
                    consumersPerGroup: [1, 2]
                """, DrainBacklogWorkload.class);
        List<TestPoint> points = ScenarioExpander.expand(wl.baseWorkload, wl.scenarios, null, wl.drainBacklogConfig);
        assertEquals(BacklogReuse.identity(points.get(0)), BacklogReuse.identity(points.get(1)));
    }
}
