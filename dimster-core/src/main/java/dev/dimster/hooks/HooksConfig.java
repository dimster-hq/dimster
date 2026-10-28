package dev.dimster.hooks;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@code engineConfig.hooks} block of a workload: the hooks to run at
 * each {@link HookPoint}, in order. Explicit fields (rather than a map keyed
 * by point) let strict YAML parsing reject a misspelt hook point.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class HooksConfig {
    public List<HookConfig> beforeWorkload = List.of();
    public List<HookConfig> afterWorkload = List.of();
    public List<HookConfig> beforeTestPoint = List.of();
    public List<HookConfig> afterTestPoint = List.of();
    public List<HookConfig> beforeDrain = List.of();

    /** The hooks configured for a point, in execution order. */
    public List<HookConfig> forPoint(HookPoint point) {
        return switch (point) {
            case BEFORE_WORKLOAD -> beforeWorkload;
            case AFTER_WORKLOAD -> afterWorkload;
            case BEFORE_TEST_POINT -> beforeTestPoint;
            case AFTER_TEST_POINT -> afterTestPoint;
            case BEFORE_DRAIN -> beforeDrain;
        };
    }

    /** Every configured hook, across all points. */
    @JsonIgnore
    public List<HookConfig> all() {
        List<HookConfig> all = new ArrayList<>();
        for (HookPoint point : HookPoint.values()) {
            all.addAll(forPoint(point));
        }
        return all;
    }
}
