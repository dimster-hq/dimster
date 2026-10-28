package dev.dimster.hooks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Finds and loads the hook scripts a workload references, for embedded
 * (single-process) mode, where there is no CLI to ship them in a run
 * request. Mirrors the CLI's resolution: absolute, then relative to the
 * workload file, then relative to the working directory.
 */
public final class HookScripts {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private HookScripts() {}

    /** The {@code script} values under {@code engineConfig.hooks} in a workload YAML. */
    public static Set<String> referencedIn(String workloadYaml) {
        Set<String> scripts = new LinkedHashSet<>();
        JsonNode hooks;
        try {
            hooks = YAML.readTree(workloadYaml).path("engineConfig").path("hooks");
        } catch (IOException e) {
            return scripts; // Unparseable YAML is reported when the workload is loaded.
        }
        for (HookPoint point : HookPoint.values()) {
            for (JsonNode hook : hooks.path(point.yamlName())) {
                String script = hook.path("script").asText("");
                if (!script.isBlank()) scripts.add(script);
            }
        }
        return scripts;
    }

    /** Reads each referenced script, keyed by its reference, resolving it from {@code workloadDir}. */
    public static Map<String, String> load(String workloadYaml, Path workloadDir) {
        Map<String, String> contents = new LinkedHashMap<>();
        for (String script : referencedIn(workloadYaml)) {
            Path path = resolve(script, workloadDir);
            try {
                contents.put(script, Files.readString(path));
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot read hook script '" + script + "' (resolved to " + path + ")", e);
            }
        }
        return contents;
    }

    private static Path resolve(String script, Path workloadDir) {
        Path p = Path.of(script);
        if (p.isAbsolute()) return p;
        Path besideWorkload = workloadDir.resolve(p);
        return Files.exists(besideWorkload) ? besideWorkload : p.toAbsolutePath();
    }
}
