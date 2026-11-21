package dev.dimster.coordinator;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

/**
 * The results subdirectory names the coordinator accepts from the CLI: the
 * run's start time plus a random suffix (run_configs::new_run_dir_name), and
 * nothing that could escape the results directory.
 */
public class ResultsSubdirTest {

    @Test
    public void accepts_the_cli_run_dir_name() {
        assertTrue(CoordinatorServer.isSafeResultsSubdir("2026-11-21-17-15-06-k3x9"));
        assertTrue(CoordinatorServer.isSafeResultsSubdir("2026-11-21-17-15-06-0000"));
    }

    @Test
    public void rejects_other_shapes_and_path_escapes() {
        for (String bad : List.of(
                "2026-11-21-17-15-06",          // no suffix
                "2026-11-21-17-15-06-K3X9",     // uppercase
                "2026-11-21-17-15-06-k3x",      // short suffix
                "2026-11-21-17-15-06-k3x9a",    // long suffix
                "2026-11-21-17-15-06-../x",
                "../2026-11-21-17-15-06-k3x9",
                "/etc/passwd",
                "")) {
            assertFalse(bad, CoordinatorServer.isSafeResultsSubdir(bad));
        }
    }
}
