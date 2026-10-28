package dev.dimster.hooks;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.ArrayList;
import java.util.List;

/**
 * The record of one hook execution, written to the result JSON under
 * {@code metadata.hooks} so a result shows exactly what was done to the
 * environment, when, and with what outcome. Read back from the partial
 * result on resume, hence the mutable bean shape.
 */
@JsonPropertyOrder({"hookPoint", "script", "args", "testPointIndex", "testPointLabel",
        "startTime", "durationMs", "exitCode", "timedOut", "outputTail"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HookExecution {
    public String hookPoint;
    public String script;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> args = new ArrayList<>();
    /** -1 for workload-level hooks. */
    public int testPointIndex;
    public String testPointLabel;
    /** ISO-8601 wall-clock start time, for correlating with Prometheus. */
    public String startTime;
    public long durationMs;
    /** The script's exit code; null when it was killed on timeout. */
    public Integer exitCode;
    public boolean timedOut;
    /** The last lines of combined stdout/stderr. */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> outputTail = new ArrayList<>();

    public boolean succeeded() {
        return !timedOut && exitCode != null && exitCode == 0;
    }
}
