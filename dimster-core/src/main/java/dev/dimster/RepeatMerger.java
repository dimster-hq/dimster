package dev.dimster;

import dev.dimster.engines.correctness.CorrectnessResult;
import dev.dimster.engines.drainbacklog.DrainBacklogResult;
import dev.dimster.engines.explore.ExploreResult;
import dev.dimster.engines.run.RunResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.ToDoubleFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Merges repeated scenario results into a single averaged ScenarioResult.
 * Detects repeats by the " (run N)" suffix appended by ScenarioExpander.
 */
public class RepeatMerger {

    private static final Pattern RUN_SUFFIX = Pattern.compile(" \\(run \\d+\\)$");

    static String baseName(String scenarioName) {
        Matcher m = RUN_SUFFIX.matcher(scenarioName);
        return m.find() ? scenarioName.substring(0, m.start()) : scenarioName;
    }

    // --- Run mode ---

    public static void mergeRunRepeats(RunResult result) {
        var groups = groupByBaseName(result.scenarioResults, sr -> sr.name);
        for (var entry : groups.entrySet()) {
            if (entry.getValue().size() > 1) {
                result.scenarioResults.add(mergeRunGroup(entry.getKey(), entry.getValue()));
            }
        }
    }

    private static RunResult.ScenarioResult mergeRunGroup(String baseName, List<RunResult.ScenarioResult> runs) {
        var merged = new RunResult.ScenarioResult();
        merged.name = baseName + " (merged)";

        int testCount = runs.getFirst().tests.size();
        for (int t = 0; t < testCount; t++) {
            List<RunResult.TestEntry> entries = collectAtIndex(runs, sr -> sr.tests, t);
            var mergedEntry = new RunResult.TestEntry();
            mergedEntry.label = entries.getFirst().label;
            mergedEntry.workloadConfig = entries.getFirst().workloadConfig;
            mergedEntry.results = mergePerformance(entries.stream().map(e -> e.results).toList());
            merged.tests.add(mergedEntry);
        }
        return merged;
    }

    // --- Explore mode ---

    public static void mergeExploreRepeats(ExploreResult result) {
        var groups = groupByBaseName(result.scenarioResults, sr -> sr.name);
        for (var entry : groups.entrySet()) {
            if (entry.getValue().size() > 1) {
                result.scenarioResults.add(mergeExploreGroup(entry.getKey(), entry.getValue()));
            }
        }
    }

    private static ExploreResult.ScenarioResult mergeExploreGroup(String baseName,
                                                                    List<ExploreResult.ScenarioResult> runs) {
        var merged = new ExploreResult.ScenarioResult();
        merged.name = baseName + " (merged)";

        int testCount = runs.getFirst().tests.size();
        for (int t = 0; t < testCount; t++) {
            List<ExploreResult.TestOutcome> outcomes = collectAtIndex(runs, sr -> sr.tests, t);
            var m = new ExploreResult.TestOutcome();

            // Copy config from first
            var first = outcomes.getFirst();
            m.label = first.label;
            m.topics = first.topics;
            m.partitionsPerTopic = first.partitionsPerTopic;
            m.producersPerTopic = first.producersPerTopic;
            m.consumerGroupsPerTopic = first.consumerGroupsPerTopic;
            m.consumersPerGroup = first.consumersPerGroup;
            m.messageSize = first.messageSize;
            m.messageDistributor = first.messageDistributor;
            m.consumerType = first.consumerType;
            m.datasetPreset = first.datasetPreset;
            m.datasetFormat = first.datasetFormat;

            // Average performance metrics
            m.peakSustainableRate = avg(outcomes, o -> o.peakSustainableRate);
            m.peakSustainableThroughputMBs = avg(outcomes, o -> o.peakSustainableThroughputMBs);
            m.foundRateUpper = avg(outcomes, o -> o.foundRateUpper);
            m.converged = outcomes.stream().allMatch(o -> o.converged);
            m.reason = "merged from " + outcomes.size() + " runs";

            m.publishLatency = mergeLatencyBreakdown(outcomes.stream().map(o -> o.publishLatency).toList());
            m.endToEndLatency = mergeLatencyBreakdown(outcomes.stream().map(o -> o.endToEndLatency).toList());
            m.endToEndDeliveryLatency = mergeLatencyBreakdown(outcomes.stream().map(o -> o.endToEndDeliveryLatency).toList());

            merged.tests.add(m);
        }
        return merged;
    }

    // --- Drain-backlog mode ---

    public static void mergeDrainBacklogRepeats(DrainBacklogResult result) {
        var groups = groupByBaseName(result.scenarioResults, sr -> sr.name);
        for (var entry : groups.entrySet()) {
            if (entry.getValue().size() > 1) {
                result.scenarioResults.add(mergeDrainBacklogGroup(entry.getKey(), entry.getValue()));
            }
        }
    }

    private static DrainBacklogResult.ScenarioResult mergeDrainBacklogGroup(
            String baseName, List<DrainBacklogResult.ScenarioResult> runs) {
        var merged = new DrainBacklogResult.ScenarioResult();
        merged.name = baseName + " (merged)";

        int testCount = runs.getFirst().tests.size();
        for (int t = 0; t < testCount; t++) {
            List<DrainBacklogResult.TestOutcome> outcomes = collectAtIndex(runs, sr -> sr.tests, t);
            var m = new DrainBacklogResult.TestOutcome();
            m.label = outcomes.getFirst().label;
            m.workloadConfig = outcomes.getFirst().workloadConfig;

            // Average drain scalars
            m.drain.backlogSizeBytes = avgLong(outcomes, o -> o.drain.backlogSizeBytes);
            m.drain.backlogMessages = avgLong(outcomes, o -> o.drain.backlogMessages);
            // Repeats that drained a reused backlog had no build: average the builds only.
            var built = outcomes.stream().filter(o -> o.drain.reusedBacklogFrom == null).toList();
            m.drain.buildDurationMs = built.isEmpty() ? 0 : avgLong(built, o -> o.drain.buildDurationMs);
            m.drain.drainDurationMs = avgLong(outcomes, o -> o.drain.drainDurationMs);
            m.drain.backlogProducerRate = outcomes.getFirst().drain.backlogProducerRate;
            // Time series left empty in merged result

            m.performance = mergePerformance(outcomes.stream().map(o -> o.performance).toList());
            merged.tests.add(m);
        }
        return merged;
    }

    // --- Correctness mode ---

    public static void mergeCorrectnessRepeats(CorrectnessResult result) {
        var groups = groupByBaseName(result.scenarioResults, sr -> sr.name);
        for (var entry : groups.entrySet()) {
            if (entry.getValue().size() > 1) {
                result.scenarioResults.add(mergeCorrectnessGroup(entry.getKey(), entry.getValue()));
            }
        }
    }

    private static CorrectnessResult.ScenarioResult mergeCorrectnessGroup(
            String baseName, List<CorrectnessResult.ScenarioResult> runs) {
        var merged = new CorrectnessResult.ScenarioResult();
        merged.name = baseName + " (merged)";

        int testCount = runs.getFirst().tests.size();
        for (int t = 0; t < testCount; t++) {
            List<CorrectnessResult.TestOutcome> outcomes = collectAtIndex(runs, sr -> sr.tests, t);
            var m = new CorrectnessResult.TestOutcome();
            m.label = outcomes.getFirst().label;
            m.workloadConfig = outcomes.getFirst().workloadConfig;
            m.performance = mergePerformance(outcomes.stream().map(o -> o.performance).toList());

            // Sum correctness counters
            m.correctness.passed = outcomes.stream().allMatch(o -> o.correctness.passed);
            m.correctness.totalSent = outcomes.stream().mapToLong(o -> o.correctness.totalSent).sum();
            m.correctness.totalReceived = outcomes.stream().mapToLong(o -> o.correctness.totalReceived).sum();
            m.correctness.lostMessages = outcomes.stream().mapToLong(o -> o.correctness.lostMessages).sum();
            m.correctness.duplicateCount = outcomes.stream().mapToLong(o -> o.correctness.duplicateCount).sum();
            m.correctness.outOfOrderCount = outcomes.stream().mapToLong(o -> o.correctness.outOfOrderCount).sum();
            m.correctness.crcFailureCount = outcomes.stream().mapToLong(o -> o.correctness.crcFailureCount).sum();
            // Per-sample correctness arrays, lostMessageRanges, and violations
            // are omitted for the same reason performance time series are:
            // samples from separate runs do not share a wall-clock timeline.

            merged.tests.add(m);
        }
        return merged;
    }

    // --- Shared helpers ---

    private static TestResult.PerformanceResults mergePerformance(List<TestResult.PerformanceResults> results) {
        var m = new TestResult.PerformanceResults();

        // Average scalar aggregates
        m.aggregatedPublishLatencyAvg = avg(results, r -> r.aggregatedPublishLatencyAvg);
        m.avgRecordBytes = avg(results, r -> r.avgRecordBytes);
        m.aggregatedPublishLatency50pct = avg(results, r -> r.aggregatedPublishLatency50pct);
        m.aggregatedPublishLatency75pct = avg(results, r -> r.aggregatedPublishLatency75pct);
        m.aggregatedPublishLatency95pct = avg(results, r -> r.aggregatedPublishLatency95pct);
        m.aggregatedPublishLatency99pct = avg(results, r -> r.aggregatedPublishLatency99pct);
        m.aggregatedPublishLatency999pct = avg(results, r -> r.aggregatedPublishLatency999pct);
        m.aggregatedPublishLatency9999pct = avg(results, r -> r.aggregatedPublishLatency9999pct);
        m.aggregatedPublishLatencyMax = avg(results, r -> r.aggregatedPublishLatencyMax);

        m.aggregatedPublishDelayLatencyAvg = avg(results, r -> r.aggregatedPublishDelayLatencyAvg);
        m.aggregatedPublishDelayLatency50pct = avg(results, r -> r.aggregatedPublishDelayLatency50pct);
        m.aggregatedPublishDelayLatency75pct = avg(results, r -> r.aggregatedPublishDelayLatency75pct);
        m.aggregatedPublishDelayLatency95pct = avg(results, r -> r.aggregatedPublishDelayLatency95pct);
        m.aggregatedPublishDelayLatency99pct = avg(results, r -> r.aggregatedPublishDelayLatency99pct);
        m.aggregatedPublishDelayLatency999pct = avg(results, r -> r.aggregatedPublishDelayLatency999pct);
        m.aggregatedPublishDelayLatency9999pct = avg(results, r -> r.aggregatedPublishDelayLatency9999pct);
        m.aggregatedPublishDelayLatencyMax = avg(results, r -> r.aggregatedPublishDelayLatencyMax);

        m.aggregatedEndToEndLatencyAvg = avg(results, r -> r.aggregatedEndToEndLatencyAvg);
        m.aggregatedEndToEndLatency50pct = avg(results, r -> r.aggregatedEndToEndLatency50pct);
        m.aggregatedEndToEndLatency75pct = avg(results, r -> r.aggregatedEndToEndLatency75pct);
        m.aggregatedEndToEndLatency95pct = avg(results, r -> r.aggregatedEndToEndLatency95pct);
        m.aggregatedEndToEndLatency99pct = avg(results, r -> r.aggregatedEndToEndLatency99pct);
        m.aggregatedEndToEndLatency999pct = avg(results, r -> r.aggregatedEndToEndLatency999pct);
        m.aggregatedEndToEndLatency9999pct = avg(results, r -> r.aggregatedEndToEndLatency9999pct);
        m.aggregatedEndToEndLatencyMax = avg(results, r -> r.aggregatedEndToEndLatencyMax);

        // Average quantile maps
        m.aggregatedScheduleLatencyQuantiles = mergeQuantileMaps(
                results.stream().map(r -> r.aggregatedScheduleLatencyQuantiles).toList());
        m.aggregatedPublishLatencyQuantiles = mergeQuantileMaps(
                results.stream().map(r -> r.aggregatedPublishLatencyQuantiles).toList());
        m.aggregatedPublishDelayLatencyQuantiles = mergeQuantileMaps(
                results.stream().map(r -> r.aggregatedPublishDelayLatencyQuantiles).toList());
        m.aggregatedEndToEndLatencyQuantiles = mergeQuantileMaps(
                results.stream().map(r -> r.aggregatedEndToEndLatencyQuantiles).toList());

        // Per-period time series (publishRate, consumeRate, latency-over-time, etc.) are left
        // empty in merged results — collapsing per-period rates to a single run-wide average per
        // run produces a degenerate "series" that isn't a true time series and, for drain-backlog,
        // makes publish and consume charts coincide (since totalSent ≈ totalReceived per run).
        // Users should inspect individual runs for time-series detail.
        // perPartitionResults omitted
        return m;
    }

    private static ExploreResult.LatencyBreakdown mergeLatencyBreakdown(
            List<ExploreResult.LatencyBreakdown> breakdowns) {
        var m = new ExploreResult.LatencyBreakdown();
        m.p50 = avg(breakdowns, b -> b.p50);
        m.p75 = avg(breakdowns, b -> b.p75);
        m.p99 = avg(breakdowns, b -> b.p99);
        m.p999 = avg(breakdowns, b -> b.p999);
        m.p9999 = avg(breakdowns, b -> b.p9999);
        return m;
    }

    private static Map<Double, Double> mergeQuantileMaps(List<Map<Double, Double>> maps) {
        if (maps.isEmpty()) return new TreeMap<>();
        Map<Double, Double> merged = new TreeMap<>();
        // Use keys from first map as reference
        Map<Double, Double> first = maps.getFirst();
        for (Double key : first.keySet()) {
            double sum = 0;
            int count = 0;
            for (Map<Double, Double> map : maps) {
                Double val = map.get(key);
                if (val != null) {
                    sum += val;
                    count++;
                }
            }
            merged.put(key, count > 0 ? sum / count : 0);
        }
        return merged;
    }

    private static <T> double avg(List<T> items, ToDoubleFunction<T> extractor) {
        return items.stream().mapToDouble(extractor).average().orElse(0);
    }

    private static <T> long avgLong(List<T> items, ToDoubleFunction<T> extractor) {
        return Math.round(items.stream().mapToDouble(extractor).average().orElse(0));
    }

    /**
     * Group scenario results by their base name (without " (run N)" suffix).
     * Only returns groups — entries without the suffix are excluded.
     */
    private static <S> Map<String, List<S>> groupByBaseName(List<S> scenarios,
                                                             java.util.function.Function<S, String> nameExtractor) {
        Map<String, List<S>> groups = new LinkedHashMap<>();
        for (S scenario : scenarios) {
            String name = nameExtractor.apply(scenario);
            if (RUN_SUFFIX.matcher(name).find()) {
                groups.computeIfAbsent(baseName(name), k -> new ArrayList<>()).add(scenario);
            }
        }
        return groups;
    }

    /**
     * Collect the test entry at index {@code i} from each scenario result's test list.
     */
    private static <S, T> List<T> collectAtIndex(List<S> scenarios,
                                                   java.util.function.Function<S, List<T>> testsExtractor,
                                                   int index) {
        List<T> collected = new ArrayList<>();
        for (S scenario : scenarios) {
            List<T> tests = testsExtractor.apply(scenario);
            if (index < tests.size()) {
                collected.add(tests.get(index));
            }
        }
        return collected;
    }
}
