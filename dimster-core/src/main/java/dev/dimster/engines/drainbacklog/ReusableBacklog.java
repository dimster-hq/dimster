package dev.dimster.engines.drainbacklog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A built backlog that later test points may drain again instead of
 * rebuilding it (see {@link BacklogReuse}). Kept in the in-progress result
 * so a resumed run can pick it up, after checking it is still intact.
 */
public class ReusableBacklog {
    /** Canonical form of the build-side settings; reuse requires an exact match. */
    public String identity;
    /** Index of the test point that built it. */
    public int builtAtTestPoint;
    public String builtAtLabel;
    /** The topics holding the backlog (Flink input topics when there is a Flink stage). */
    public List<String> topics = new ArrayList<>();
    /** Records produced into the backlog, per consumer group. */
    public long records;
    /** End offset of every partition once built, keyed {@code topic-partition}. */
    public Map<String, Long> endOffsets = new LinkedHashMap<>();
}
