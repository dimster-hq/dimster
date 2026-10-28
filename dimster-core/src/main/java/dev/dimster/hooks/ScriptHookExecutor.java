package dev.dimster.hooks;

import dev.dimster.utils.StopRequestedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs hooks as local processes. The scripts arrive inside the run request
 * (the CLI reads them from the user's machine) and are installed into a
 * directory, so the coordinator only executes code it was explicitly sent.
 * What a script does (kubectl, docker, a cloud CLI) is opaque to the
 * coordinator, which is what keeps it free of infrastructure knowledge.
 */
public class ScriptHookExecutor implements HookExecutor {

    private static final Logger log = LoggerFactory.getLogger(ScriptHookExecutor.class);

    /** Lines of output kept for the result JSON; the full output goes to the log. */
    static final int OUTPUT_TAIL_LINES = 50;

    private final Map<String, Path> scripts;

    public ScriptHookExecutor(Map<String, Path> scripts) {
        this.scripts = Map.copyOf(scripts);
    }

    /**
     * Writes each script (keyed by the name workloads reference it by) into
     * {@code dir} as an executable file and returns an executor over them.
     * File names are sanitised and numbered, since keys may be paths.
     */
    public static ScriptHookExecutor install(Map<String, String> scriptContents, File dir) {
        Map<String, Path> installed = new LinkedHashMap<>();
        if (scriptContents == null || scriptContents.isEmpty()) {
            return new ScriptHookExecutor(installed);
        }
        try {
            Files.createDirectories(dir.toPath());
            int n = 0;
            for (var e : new TreeMap<>(scriptContents).entrySet()) {
                String fileName = String.format("%02d-%s", n++, e.getKey().replaceAll("[^A-Za-z0-9._-]", "_"));
                Path path = dir.toPath().resolve(fileName);
                Files.writeString(path, e.getValue(), StandardCharsets.UTF_8);
                if (!path.toFile().setExecutable(true)) {
                    throw new IOException("Cannot make hook script executable: " + path);
                }
                installed.put(e.getKey(), path);
                log.info("Installed hook script '{}' as {}", e.getKey(), path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to install hook scripts into " + dir, e);
        }
        return new ScriptHookExecutor(installed);
    }

    @Override
    public boolean hasScript(String script) {
        return scripts.containsKey(script);
    }

    @Override
    public Outcome execute(HookConfig hook, Map<String, String> env, AtomicBoolean stopRequested) {
        Path script = scripts.get(hook.script);
        if (script == null) {
            throw new IllegalArgumentException("Hook script '" + hook.script + "' was not sent with the run request");
        }
        List<String> command = new ArrayList<>();
        command.add(script.toString());
        command.addAll(hook.args);

        ProcessBuilder pb = new ProcessBuilder(command)
                .directory(script.getParent().toFile())
                .redirectErrorStream(true);
        pb.environment().putAll(env);

        Process process;
        try {
            process = pb.start();
        } catch (IOException e) {
            return new Outcome(null, false, List.of("Failed to start: " + e.getMessage()));
        }

        Deque<String> tail = new ArrayDeque<>();
        Thread reader = startOutputReader(process, hook.script, tail);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(hook.timeoutSeconds);
        boolean timedOut = false;
        try {
            while (!process.waitFor(1, TimeUnit.SECONDS)) {
                if (stopRequested.get()) {
                    log.warn("[hook {}] stop requested, killing the hook", hook.script);
                    kill(process);
                    throw new StopRequestedException();
                }
                if (System.nanoTime() >= deadline) {
                    log.warn("[hook {}] timed out after {}s, killing it", hook.script, hook.timeoutSeconds);
                    kill(process);
                    timedOut = true;
                    break;
                }
            }
            reader.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            kill(process);
            throw new StopRequestedException();
        }

        List<String> outputTail;
        synchronized (tail) {
            outputTail = new ArrayList<>(tail);
        }
        return new Outcome(timedOut ? null : process.exitValue(), timedOut, outputTail);
    }

    /** Streams the hook's output into the coordinator log, keeping the last lines. */
    private static Thread startOutputReader(Process process, String name, Deque<String> tail) {
        Thread t = new Thread(() -> {
            try (var in = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    log.info("[hook {}] {}", name, line);
                    synchronized (tail) {
                        tail.addLast(line);
                        if (tail.size() > OUTPUT_TAIL_LINES) tail.removeFirst();
                    }
                }
            } catch (IOException e) {
                // The stream closes when the process is killed.
            }
        }, "hook-output-" + name);
        t.setDaemon(true);
        t.start();
        return t;
    }

    /** Kills the hook and anything it started, so no half-run script lingers. */
    private static void kill(Process process) {
        process.descendants().forEach(ProcessHandle::destroyForcibly);
        process.destroyForcibly();
        try {
            process.waitFor(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
