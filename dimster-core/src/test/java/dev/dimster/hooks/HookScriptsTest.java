package dev.dimster.hooks;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/** Embedded mode's discovery and loading of the scripts a workload references. */
public class HookScriptsTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private static final String WORKLOAD = """
            name: x
            engineConfig:
              hooks:
                beforeDrain:
                  - script: hooks/evict.sh
                afterTestPoint:
                  - script: notify.sh
                  - script: hooks/evict.sh
            """;

    @Test
    public void finds_each_referenced_script_once() {
        assertEquals(Set.of("hooks/evict.sh", "notify.sh"), HookScripts.referencedIn(WORKLOAD));
    }

    @Test
    public void workload_without_hooks_references_nothing() {
        assertTrue(HookScripts.referencedIn("name: x\nengineConfig:\n  cooldownMinutes: 1\n").isEmpty());
    }

    @Test
    public void loads_scripts_relative_to_the_workload() throws Exception {
        File dir = tmp.newFolder("workloads");
        new File(dir, "hooks").mkdirs();
        Files.writeString(new File(dir, "hooks/evict.sh").toPath(), "echo evict");
        Files.writeString(new File(dir, "notify.sh").toPath(), "echo notify");

        Map<String, String> scripts = HookScripts.load(WORKLOAD, dir.toPath());

        assertEquals("echo evict", scripts.get("hooks/evict.sh"));
        assertEquals("echo notify", scripts.get("notify.sh"));
    }

    @Test
    public void missing_script_fails_with_its_name() {
        var e = assertThrows(RuntimeException.class, () -> HookScripts.load(WORKLOAD, tmp.getRoot().toPath()));
        // Hook points are scanned in lifecycle order, so afterTestPoint comes first.
        assertTrue(e.getMessage().contains("notify.sh"));
    }
}
