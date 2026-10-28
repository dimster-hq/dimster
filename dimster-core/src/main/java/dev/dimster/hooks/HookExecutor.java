package dev.dimster.hooks;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs one hook. The seam between {@link Hooks} (when and with what context
 * to run) and the transport (how). {@link ScriptHookExecutor} runs a local
 * script; tests substitute a fake.
 */
public interface HookExecutor {

    /** What a finished (or killed) hook produced. A null exit code means it was killed. */
    record Outcome(Integer exitCode, boolean timedOut, List<String> outputTail) {}

    /** Whether this executor can run the named script. Used to fail fast at workload start. */
    boolean hasScript(String script);

    /**
     * Run the hook to completion, killing it after its timeout. Throws
     * {@link dev.dimster.utils.StopRequestedException} (after killing it) if
     * a stop is requested meanwhile.
     */
    Outcome execute(HookConfig hook, Map<String, String> env, AtomicBoolean stopRequested);
}
