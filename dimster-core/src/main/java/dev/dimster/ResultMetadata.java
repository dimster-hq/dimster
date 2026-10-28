package dev.dimster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import dev.dimster.hooks.HookExecution;

/**
 * Top-level "what produced this result" block, serialized as the
 * {@code metadata} field on every engine result JSON
 * (RunResult, ExploreResult, CorrectnessResult, DrainBacklogResult).
 *
 * Lives as a shared class — rather than as four identical inner classes
 * on each result type — because all four result types stamp the same
 * provenance fields, and a future fifth result type must not be able to
 * silently omit any of them. In particular, {@link #dimsterVersion} is
 * populated in the no-arg constructor from {@link Version#get()}, so
 * any caller that does {@code new ResultMetadata()} cannot accidentally
 * ship a result with no version string. This was the whole point of
 * adding the field — to make every result self-describing for
 * reproducibility once we start cutting git tags.
 *
 * The {@link JsonPropertyOrder} puts {@code dimsterVersion} first so
 * it's the first thing a human sees when opening a result file, and so
 * it's adjacent to the other identifying fields rather than buried
 * after the timestamps.
 */
@JsonPropertyOrder({
        "dimsterVersion",
        "name",
        "kafkaConfigName",
        "environment",
        "system",
        "hardware",
        "beginTime",
        "endTime",
        "extraDimensions",
        "hooks",
})
public class ResultMetadata {

    /**
     * Dimster version that produced this result, e.g. {@code "0.1.0"} or
     * {@code "0.1.0-SNAPSHOT"}. Pulled from {@link Version#get()} at
     * construction time. Will be {@link Version#UNKNOWN} if the build
     * resource was missing — see the Version class for diagnosis hints.
     */
    public String dimsterVersion;

    public String name;
    public String kafkaConfigName;
    public String environment;
    public String system;
    public String hardware;
    public String beginTime;
    public String endTime;

    /**
     * User-defined infrastructure dimensions, such as broker count or TLS
     * mode, that are not part of Dimster's workload schema. Kept separate
     * from free-form metadata so analysis tools can treat these fields as
     * comparison dimensions.
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> extraDimensions = new TreeMap<>();

    /**
     * Every hook execution of the workload, in order: what was done to the
     * environment around the test points, and whether it worked. Empty (and
     * omitted) when the workload configures no hooks.
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<HookExecution> hooks = new ArrayList<>();

    private final Map<String, Object> userMetadata = new TreeMap<>();

    public ResultMetadata() {
        this.dimsterVersion = Version.get();
    }

    @JsonAnyGetter
    public Map<String, Object> userMetadata() {
        return userMetadata;
    }

    @JsonAnySetter
    public void putUserMetadata(String key, Object value) {
        userMetadata.put(key, value);
    }

    public void putUserMetadata(Map<String, Object> metadata) {
        if (metadata != null) {
            userMetadata.putAll(metadata);
        }
    }

    public void putExtraDimensions(Map<String, Object> dimensions) {
        if (dimensions != null) {
            extraDimensions.putAll(dimensions);
        }
    }
}
