package dev.dimster.hooks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs a workload's configured hooks at each {@link HookPoint} and keeps the
 * record of every execution. One instance per workload run: built by
 * {@code BenchmarkRunner}, handed to the engine (which decides when each
 * point is reached), and read back by the runner to stamp the executions
 * into the result metadata.
 *
 * Hooks receive their context as environment variables:
 * {@code DIMSTER_HOOK_POINT}, {@code DIMSTER_MODE}, {@code DIMSTER_WORKLOAD},
 * {@code DIMSTER_TEST_POINT_INDEX} (0-based, -1 outside a test point),
 * and when known {@code DIMSTER_TEST_POINT_COUNT}, {@code DIMSTER_TEST_POINT_LABEL},
 * {@code DIMSTER_SCENARIO}, {@code DIMSTER_BOOTSTRAP_SERVERS} and
 * {@code DIMSTER_TOPICS} (comma-separated).
 */
public class Hooks {

    private static final Logger log = LoggerFactory.getLogger(Hooks.class);

    /** No hooks: every point is a no-op. The default for engines run without a runner. */
    public static final Hooks NONE = new Hooks(new HooksConfig(), null, "", "", new AtomicBoolean(), List.of());

    private final HooksConfig config;
    private final HookExecutor executor;
    private final String mode;
    private final String workloadName;
    private final AtomicBoolean stopRequested;
    private final List<HookExecution> executions;

    /**
     * @param previous executions recorded before a resume, kept so the final
     *                 result lists every execution of the workload
     */
    public Hooks(HooksConfig config, HookExecutor executor, String mode, String workloadName,
                 AtomicBoolean stopRequested, List<HookExecution> previous) {
        this.config = config != null ? config : new HooksConfig();
        this.executor = executor;
        this.mode = mode;
        this.workloadName = workloadName;
        this.stopRequested = stopRequested;
        this.executions = new ArrayList<>(previous != null ? previous : List.of());
    }

    /**
     * Fails fast, before any test point runs, on hooks that could never run:
     * a missing script, a non-positive timeout, or a point the mode never reaches.
     */
    public void validate() {
        if (!config.beforeDrain.isEmpty() && !"drain-backlog".equals(mode)) {
            throw new IllegalArgumentException("engineConfig.hooks.beforeDrain is only supported in drain-backlog mode, not " + mode);
        }
        for (HookConfig hook : config.all()) {
            if (hook.script == null || hook.script.isBlank()) {
                throw new IllegalArgumentException("engineConfig.hooks: every hook needs a script");
            }
            if (hook.timeoutSeconds <= 0) {
                throw new IllegalArgumentException("engineConfig.hooks: timeoutSeconds must be positive for '" + hook.script + "'");
            }
            if (executor == null || !executor.hasScript(hook.script)) {
                throw new IllegalArgumentException("engineConfig.hooks: script '" + hook.script
                        + "' was not sent with the run request (the CLI ships every script a workload references)");
            }
        }
    }

    /** Whether any hook is configured for the point, so engines can skip gathering context. */
    public boolean has(HookPoint point) {
        return !config.forPoint(point).isEmpty();
    }

    /**
     * Runs the point's hooks in order. A failing hook with
     * {@link HookConfig.OnFailure#FAIL} stops the sequence and throws
     * {@link HookFailedException}; with CONTINUE it is recorded and skipped.
     */
    public void run(HookPoint point, HookContext context) {
        for (HookConfig hook : config.forPoint(point)) {
            log.info("Running {} hook '{}'{}", point.yamlName(), hook.script,
                    context.testPointIndex() >= 0 ? " for test point " + (context.testPointIndex() + 1) : "");
            HookExecution execution = execute(point, hook, context);
            synchronized (executions) {
                executions.add(execution);
            }
            if (execution.succeeded()) {
                log.info("{} hook '{}' completed in {}ms", point.yamlName(), hook.script, execution.durationMs);
            } else if (hook.onFailure == HookConfig.OnFailure.CONTINUE) {
                log.warn("{} hook '{}' failed ({}), continuing as configured",
                        point.yamlName(), hook.script, describeFailure(execution));
            } else {
                throw new HookFailedException("%s hook '%s' failed (%s)".formatted(
                        point.yamlName(), hook.script, describeFailure(execution)));
            }
        }
    }

    /** Every execution so far, in order, including those before a resume. */
    public List<HookExecution> executions() {
        synchronized (executions) {
            return List.copyOf(executions);
        }
    }

    private HookExecution execute(HookPoint point, HookConfig hook, HookContext context) {
        var execution = new HookExecution();
        execution.hookPoint = point.yamlName();
        execution.script = hook.script;
        execution.args = new ArrayList<>(hook.args);
        execution.testPointIndex = context.testPointIndex();
        execution.testPointLabel = context.testPointLabel();
        execution.startTime = Instant.now().toString();

        long start = System.nanoTime();
        HookExecutor.Outcome outcome = executor.execute(hook, environment(point, context), stopRequested);
        execution.durationMs = (System.nanoTime() - start) / 1_000_000;
        execution.exitCode = outcome.exitCode();
        execution.timedOut = outcome.timedOut();
        execution.outputTail = new ArrayList<>(outcome.outputTail());
        return execution;
    }

    /** The {@code DIMSTER_*} variables a hook sees; unknown values are left unset. */
    Map<String, String> environment(HookPoint point, HookContext context) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("DIMSTER_HOOK_POINT", point.yamlName());
        env.put("DIMSTER_MODE", mode);
        env.put("DIMSTER_WORKLOAD", workloadName);
        env.put("DIMSTER_TEST_POINT_INDEX", String.valueOf(context.testPointIndex()));
        if (context.testPointCount() > 0) {
            env.put("DIMSTER_TEST_POINT_COUNT", String.valueOf(context.testPointCount()));
        }
        putIfPresent(env, "DIMSTER_TEST_POINT_LABEL", context.testPointLabel());
        putIfPresent(env, "DIMSTER_SCENARIO", context.scenarioName());
        putIfPresent(env, "DIMSTER_BOOTSTRAP_SERVERS", context.bootstrapServers());
        if (!context.topics().isEmpty()) {
            env.put("DIMSTER_TOPICS", String.join(",", context.topics()));
        }
        return env;
    }

    private static void putIfPresent(Map<String, String> env, String key, String value) {
        if (value != null && !value.isBlank()) env.put(key, value);
    }

    private static String describeFailure(HookExecution execution) {
        return execution.timedOut ? "timed out" : "exit code " + execution.exitCode;
    }
}
