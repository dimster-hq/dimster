package dev.dimster.hooks;

import java.util.List;

/**
 * One hook as written in the workload YAML. The script is named exactly as
 * the CLI shipped it in the run request; the coordinator never reads a
 * script from anywhere else.
 */
public class HookConfig {

    /** What happens to the run when a hook exits non-zero or times out. */
    public enum OnFailure {
        /** Fail the workload. Its checkpoint is kept, so it can be resumed. */
        FAIL,
        /** Record the failure and carry on. */
        CONTINUE
    }

    /** The script to run, as referenced in the workload (resolved by the CLI). */
    public String script;

    /** Arguments passed to the script. */
    public List<String> args = List.of();

    /** Time limit for one execution, after which the script is killed. */
    public int timeoutSeconds = 600;

    public OnFailure onFailure = OnFailure.FAIL;
}
