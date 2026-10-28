package dev.dimster.hooks;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.dimster.EngineConfig;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/** Hook sequencing, failure policies, validation and the hook environment. */
public class HooksTest {

    /** Records invocations and returns scripted exit codes (by script name). */
    private static class FakeExecutor implements HookExecutor {
        final Map<String, Integer> exitCodes;
        final List<String> ran = new ArrayList<>();
        final List<Map<String, String>> envs = new ArrayList<>();

        FakeExecutor(Map<String, Integer> exitCodes) {
            this.exitCodes = exitCodes;
        }

        @Override
        public boolean hasScript(String script) {
            return exitCodes.containsKey(script);
        }

        @Override
        public Outcome execute(HookConfig hook, Map<String, String> env, AtomicBoolean stopRequested) {
            ran.add(hook.script);
            envs.add(env);
            return new Outcome(exitCodes.get(hook.script), false, List.of("out"));
        }
    }

    private static HookConfig hook(String script, HookConfig.OnFailure onFailure) {
        var h = new HookConfig();
        h.script = script;
        h.onFailure = onFailure;
        return h;
    }

    private static Hooks hooks(HooksConfig config, HookExecutor executor, String mode) {
        return new Hooks(config, executor, mode, "wl", new AtomicBoolean(), List.of());
    }

    private static final HookContext CONTEXT = new HookContext(2, 5, "tp-3", "scn", "kafka:9092", List.of("t1", "t2"));

    @Test
    public void runs_hooks_in_order_and_records_each() {
        var config = new HooksConfig();
        config.beforeTestPoint = List.of(hook("a", HookConfig.OnFailure.FAIL), hook("b", HookConfig.OnFailure.FAIL));
        var executor = new FakeExecutor(Map.of("a", 0, "b", 0));
        var hooks = hooks(config, executor, "run");

        hooks.run(HookPoint.BEFORE_TEST_POINT, CONTEXT);
        hooks.run(HookPoint.AFTER_TEST_POINT, CONTEXT);

        assertEquals(List.of("a", "b"), executor.ran);
        assertEquals(2, hooks.executions().size());
        HookExecution first = hooks.executions().get(0);
        assertEquals("beforeTestPoint", first.hookPoint);
        assertEquals(2, first.testPointIndex);
        assertEquals("tp-3", first.testPointLabel);
        assertTrue(first.succeeded());
    }

    @Test
    public void failing_hook_with_fail_policy_stops_the_sequence() {
        var config = new HooksConfig();
        config.beforeTestPoint = List.of(hook("bad", HookConfig.OnFailure.FAIL), hook("after", HookConfig.OnFailure.FAIL));
        var executor = new FakeExecutor(Map.of("bad", 1, "after", 0));
        var hooks = hooks(config, executor, "run");

        assertThrows(HookFailedException.class, () -> hooks.run(HookPoint.BEFORE_TEST_POINT, CONTEXT));
        assertEquals(List.of("bad"), executor.ran);
        // The failure is still recorded for the result JSON.
        assertEquals(1, hooks.executions().size());
        assertFalse(hooks.executions().get(0).succeeded());
    }

    @Test
    public void failing_hook_with_continue_policy_carries_on() {
        var config = new HooksConfig();
        config.beforeTestPoint = List.of(hook("bad", HookConfig.OnFailure.CONTINUE), hook("after", HookConfig.OnFailure.FAIL));
        var executor = new FakeExecutor(Map.of("bad", 1, "after", 0));
        var hooks = hooks(config, executor, "run");

        hooks.run(HookPoint.BEFORE_TEST_POINT, CONTEXT);

        assertEquals(List.of("bad", "after"), executor.ran);
    }

    @Test
    public void environment_describes_the_test_point() {
        var config = new HooksConfig();
        config.beforeDrain = List.of(hook("a", HookConfig.OnFailure.FAIL));
        var executor = new FakeExecutor(Map.of("a", 0));

        hooks(config, executor, "drain-backlog").run(HookPoint.BEFORE_DRAIN, CONTEXT);

        Map<String, String> env = executor.envs.get(0);
        assertEquals("beforeDrain", env.get("DIMSTER_HOOK_POINT"));
        assertEquals("drain-backlog", env.get("DIMSTER_MODE"));
        assertEquals("wl", env.get("DIMSTER_WORKLOAD"));
        assertEquals("2", env.get("DIMSTER_TEST_POINT_INDEX"));
        assertEquals("5", env.get("DIMSTER_TEST_POINT_COUNT"));
        assertEquals("tp-3", env.get("DIMSTER_TEST_POINT_LABEL"));
        assertEquals("scn", env.get("DIMSTER_SCENARIO"));
        assertEquals("kafka:9092", env.get("DIMSTER_BOOTSTRAP_SERVERS"));
        assertEquals("t1,t2", env.get("DIMSTER_TOPICS"));
    }

    @Test
    public void workload_level_environment_omits_unknowns() {
        var config = new HooksConfig();
        config.beforeWorkload = List.of(hook("a", HookConfig.OnFailure.FAIL));
        var executor = new FakeExecutor(Map.of("a", 0));

        hooks(config, executor, "run").run(HookPoint.BEFORE_WORKLOAD, HookContext.workload());

        Map<String, String> env = executor.envs.get(0);
        assertEquals("-1", env.get("DIMSTER_TEST_POINT_INDEX"));
        assertFalse(env.containsKey("DIMSTER_TEST_POINT_COUNT"));
        assertFalse(env.containsKey("DIMSTER_TOPICS"));
        assertFalse(env.containsKey("DIMSTER_TEST_POINT_LABEL"));
    }

    @Test
    public void previous_executions_are_kept_on_resume() {
        var previous = new HookExecution();
        previous.script = "old";
        var config = new HooksConfig();
        config.beforeTestPoint = List.of(hook("a", HookConfig.OnFailure.FAIL));
        var hooks = new Hooks(config, new FakeExecutor(Map.of("a", 0)), "run", "wl", new AtomicBoolean(), List.of(previous));

        hooks.run(HookPoint.BEFORE_TEST_POINT, CONTEXT);

        assertEquals(List.of("old", "a"), hooks.executions().stream().map(e -> e.script).toList());
    }

    @Test
    public void validation_rejects_missing_scripts() {
        var config = new HooksConfig();
        config.afterWorkload = List.of(hook("missing", HookConfig.OnFailure.FAIL));

        var e = assertThrows(IllegalArgumentException.class,
                () -> hooks(config, new FakeExecutor(Map.of()), "run").validate());
        assertTrue(e.getMessage().contains("missing"));
    }

    @Test
    public void validation_rejects_before_drain_outside_drain_mode() {
        var config = new HooksConfig();
        config.beforeDrain = List.of(hook("a", HookConfig.OnFailure.FAIL));

        assertThrows(IllegalArgumentException.class,
                () -> hooks(config, new FakeExecutor(Map.of("a", 0)), "run").validate());
        hooks(config, new FakeExecutor(Map.of("a", 0)), "drain-backlog").validate();
    }

    @Test
    public void validation_rejects_non_positive_timeout() {
        var config = new HooksConfig();
        var h = hook("a", HookConfig.OnFailure.FAIL);
        h.timeoutSeconds = 0;
        config.beforeTestPoint = List.of(h);

        assertThrows(IllegalArgumentException.class,
                () -> hooks(config, new FakeExecutor(Map.of("a", 0)), "run").validate());
    }

    @Test
    public void none_runs_nothing() {
        Hooks.NONE.validate();
        Hooks.NONE.run(HookPoint.BEFORE_TEST_POINT, CONTEXT);
        assertTrue(Hooks.NONE.executions().isEmpty());
    }

    // ─── YAML shape ────────────────────────────────────────────────────

    private static final ObjectMapper STRICT_YAML = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

    @Test
    public void parses_hooks_from_engine_config_yaml() throws Exception {
        String yaml = """
                hooks:
                  beforeDrain:
                    - script: evict-topic-cache.sh
                      timeoutSeconds: 900
                  afterTestPoint:
                    - script: notify.sh
                      args: [a, b]
                      onFailure: CONTINUE
                """;
        EngineConfig ec = STRICT_YAML.readValue(yaml, EngineConfig.class);

        assertEquals("evict-topic-cache.sh", ec.hooks.beforeDrain.get(0).script);
        assertEquals(900, ec.hooks.beforeDrain.get(0).timeoutSeconds);
        assertEquals(HookConfig.OnFailure.FAIL, ec.hooks.beforeDrain.get(0).onFailure);
        assertEquals(List.of("a", "b"), ec.hooks.afterTestPoint.get(0).args);
        assertEquals(HookConfig.OnFailure.CONTINUE, ec.hooks.afterTestPoint.get(0).onFailure);
        assertTrue(ec.hooks.beforeTestPoint.isEmpty());
    }

    @Test
    public void rejects_misspelt_hook_point() {
        String yaml = """
                hooks:
                  beforeDrian:
                    - script: x.sh
                """;
        assertThrows(Exception.class, () -> STRICT_YAML.readValue(yaml, EngineConfig.class));
    }

    @Test
    public void engine_config_without_hooks_serializes_without_them() throws Exception {
        String json = new ObjectMapper().writeValueAsString(new EngineConfig());
        assertFalse(json.contains("hooks"));
    }
}
