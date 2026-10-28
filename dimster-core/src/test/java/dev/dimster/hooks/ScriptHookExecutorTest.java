package dev.dimster.hooks;

import dev.dimster.utils.StopRequestedException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/** Runs real shell scripts through the executor: exit codes, env, timeouts and stop. */
public class ScriptHookExecutorTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private ScriptHookExecutor install(String name, String body) throws Exception {
        File dir = tmp.newFolder("hooks");
        return ScriptHookExecutor.install(Map.of(name, "#!/bin/sh\n" + body + "\n"), dir);
    }

    private static HookConfig hook(String script, int timeoutSeconds, String... args) {
        var h = new HookConfig();
        h.script = script;
        h.timeoutSeconds = timeoutSeconds;
        h.args = List.of(args);
        return h;
    }

    @Test
    public void runs_script_with_args_and_environment() throws Exception {
        var executor = install("hooks/echo.sh", "echo \"arg=$1 point=$DIMSTER_HOOK_POINT\"");

        var outcome = executor.execute(hook("hooks/echo.sh", 10, "x"),
                Map.of("DIMSTER_HOOK_POINT", "beforeDrain"), new AtomicBoolean());

        assertEquals(Integer.valueOf(0), outcome.exitCode());
        assertFalse(outcome.timedOut());
        assertEquals(List.of("arg=x point=beforeDrain"), outcome.outputTail());
    }

    @Test
    public void reports_non_zero_exit_and_stderr() throws Exception {
        var executor = install("fail.sh", "echo boom >&2; exit 3");

        var outcome = executor.execute(hook("fail.sh", 10), Map.of(), new AtomicBoolean());

        assertEquals(Integer.valueOf(3), outcome.exitCode());
        assertEquals(List.of("boom"), outcome.outputTail());
    }

    @Test
    public void keeps_only_the_last_output_lines() throws Exception {
        var executor = install("many.sh", "i=0; while [ $i -lt 120 ]; do echo line$i; i=$((i+1)); done");

        var outcome = executor.execute(hook("many.sh", 10), Map.of(), new AtomicBoolean());

        assertEquals(ScriptHookExecutor.OUTPUT_TAIL_LINES, outcome.outputTail().size());
        assertEquals("line119", outcome.outputTail().get(outcome.outputTail().size() - 1));
    }

    @Test
    public void kills_script_on_timeout() throws Exception {
        var executor = install("slow.sh", "sleep 30");

        long start = System.nanoTime();
        var outcome = executor.execute(hook("slow.sh", 1), Map.of(), new AtomicBoolean());

        assertTrue(outcome.timedOut());
        assertNull(outcome.exitCode());
        assertTrue("killed promptly", System.nanoTime() - start < 15_000_000_000L);
    }

    @Test
    public void stop_request_kills_script_and_propagates() throws Exception {
        var executor = install("slow.sh", "sleep 30");
        var stop = new AtomicBoolean(true);

        assertThrows(StopRequestedException.class,
                () -> executor.execute(hook("slow.sh", 60), Map.of(), stop));
    }

    @Test
    public void knows_only_installed_scripts() throws Exception {
        var executor = install("a.sh", "true");

        assertTrue(executor.hasScript("a.sh"));
        assertFalse(executor.hasScript("b.sh"));
    }
}
