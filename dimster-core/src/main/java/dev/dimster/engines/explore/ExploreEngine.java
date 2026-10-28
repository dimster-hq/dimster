package dev.dimster.engines.explore;

import dev.dimster.BaseEngine;
import dev.dimster.BaseWorkload;
import dev.dimster.utils.Sleeper;
import dev.dimster.BenchmarkRunner;
import dev.dimster.RepeatMerger;
import dev.dimster.ScenarioExpander;
import dev.dimster.ScenarioExpander.TestPoint;
import dev.dimster.ConsumerType;
import dev.dimster.engines.explore.ExploreWorkload.ExploreConfig;
import dev.dimster.kafka.KafkaCluster;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.kafka.KafkaConsumerWrapper;
import dev.dimster.utils.StopRequestedException;
import dev.dimster.worker.Worker;
import dev.dimster.worker.topology.TopologyPlanner;
import dev.dimster.worker.commands.CountersStats;
import dev.dimster.worker.commands.PeriodStats;
import dev.dimster.worker.commands.ProducerWorkAssignment;
import dev.dimster.hooks.HookPoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.HdrHistogram.Histogram;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;

/**
 * Explores the performance limits of a Kafka cluster by running a series
 * of tests across different configurations (scenarios). For each test,
 * it finds the maximum sustainable throughput while keeping a chosen
 * latency percentile under a threshold.
 *
 * Algorithm for finding peak sustainable rate:
 * 1. RAMP: Start at initialRate, double every short interval until
 *    latency exceeds threshold or producers saturate. Establishes
 *    the search range [initialRate, saturatedRate].
 * 2. SEARCH: Binary search between low and high with long measurement
 *    windows (default 2 minutes). During each window, check every N
 *    seconds for saturation. Early exit if saturation detected
 *    (3 consecutive bad checks). Converges when range is within 5%.
 * 3. SUSTAIN: Hold the found rate for a sustained period (default 5 min).
 *    If latency stays under threshold, report as peak.
 *
 * If sustain fails, reduce the upper bound and retry the search (up to
 * searchMaxRetries times).
 */
public class ExploreEngine extends BaseEngine {

    private static final Logger log = LoggerFactory.getLogger(ExploreEngine.class);
    private static final DecimalFormat rateFormat = new DecimalFormat("#,##0.##");
    private static final DecimalFormat latFormat = new DecimalFormat("0.0");
    private static final DecimalFormat throughputFormat = new DecimalFormat("0.00");

    /** Number of consecutive bad checks before declaring saturation during search */
    private static final int SATURATION_THRESHOLD = 3;

    /**
     * If a single check sees e2e latency above this multiple of the target,
     * abort the window immediately rather than waiting for the usual saturation
     * pattern. Used during both binary search and sustain to cut the loss when
     * the system is clearly far past its limit.
     */
    private static final int CATASTROPHIC_E2E_MULTIPLIER = 100;

    /** Number of linear steps used when ramping into a fast-path sustain attempt. */
    private static final int FAST_PATH_RAMP_STEPS = 6;

    /** Duration of each fast-path ramp step in seconds (6 steps x 10s = 60s total ramp). */
    private static final int FAST_PATH_RAMP_STEP_SECONDS = 10;

    private boolean useDeliveryLatency;

    private final ExploreWorkload exploreWorkload;
    private final BaseWorkload base;
    private final KafkaConfig fileKafkaConfig;

    public ExploreEngine(ExploreWorkload exploreWorkload, Worker worker, TopologyPlanner topologyPlanner,
                         KafkaConfig fileKafkaConfig, AtomicBoolean stopRequested) {
        super(worker, topologyPlanner, stopRequested);
        this.exploreWorkload = exploreWorkload;
        this.base = exploreWorkload.baseWorkload;
        this.fileKafkaConfig = fileKafkaConfig;
    }

    public ExploreResult run() throws Exception {
        return run(0, null, null);
    }

    public ExploreResult run(int startTestPoint, ExploreResult partialResult,
                              BenchmarkRunner.CheckpointCallback checkpointCallback) throws Exception {
        ExploreResult result;
        // partial result from checkpoint
        if (partialResult != null) {
            result = partialResult;
        } else {
            result = new ExploreResult();
            result.metadata.name = exploreWorkload.name;
            result.config = exploreWorkload;
        }

        List<TestPoint> allPoints = ScenarioExpander.expand(base, exploreWorkload.scenarios,
                fileKafkaConfig, exploreWorkload.exploreConfig);
        rejectFlink("explore", allPoints);
        int totalTests = allPoints.size();

        // The explore engine drives rate dynamically via ExploreConfig (initialRate
        // doubled during ramp, then binary-searched) so per-message printing would
        // inevitably saturate logs and skew the search itself — force it off regardless
        // of the user setting.
        if (exploreWorkload.engineConfig.printingRequested()) {
            log.warn("engineConfig.printMessages/printContents is not supported by the explore " +
                     "engine (rates are dynamic) and will be ignored");
        }
        worker.setPrintMessages(false);
        worker.setPrintContents(false);

        try {
            for (int i = startTestPoint; i < allPoints.size(); i++) {
                throwOnStopRequested(stopRequested);

                TestPoint point = allPoints.get(i);
                ExploreConfig config = point.exploreConfig();

                ExploreResult.ScenarioResult scenarioResult = findOrCreateScenario(
                        result.scenarioResults, point.scenarioName(),
                        sr -> sr.name, name -> {
                            var sr = new ExploreResult.ScenarioResult();
                            sr.name = name;
                            log.info("===== SCENARIO: {} =====", name);
                            return sr;
                        });

                log.info("--- Test {}/{}: {} ---", i + 1, totalTests, point.label());
                log.info("Merged workload:\n{}", point.toLogString());
                this.useDeliveryLatency = config.useDeliveryLatencyForThreshold || point.consumerProcMsMax() > 0;
                String thresholdLabel = this.useDeliveryLatency ? "e2e-dlv" : "e2e";
                log.info("    Thresholds: p{}-{} <= {} ms (sustain <= {} ms), publish delay <= {} ms (sustain <= {} ms)",
                        (int) config.e2eLatencyPercentile,
                        thresholdLabel,
                        latFormat.format(config.e2eLatencyThresholdMs),
                        latFormat.format(lenientThreshold(config.e2eLatencyThresholdMs, config.sustainLeniency)),
                        latFormat.format(config.publishDelayThresholdMs),
                        latFormat.format(lenientThreshold(config.publishDelayThresholdMs, config.sustainLeniency)));

                // Stamp the scenario boundary the first time we touch it, then
                // refresh the end after every test so the exporter knows the
                // exact time range this scenario occupies in Prometheus.
                if (scenarioResult.beginTime == null) {
                    scenarioResult.beginTime = currentScenarioTimestamp();
                }
                runHooks(HookPoint.BEFORE_TEST_POINT, point, i, totalTests, List.of());
                ExploreResult.TestOutcome outcome = runSingleTest(point);
                scenarioResult.endTime = currentScenarioTimestamp();
                scenarioResult.tests.add(outcome);

                log.info("    Result: {} msgs/s ({} MB/s), publish p99={} ms, e2e p99={} ms [{}]",
                        rateFormat.format(outcome.peakSustainableRate),
                        throughputFormat.format(outcome.peakSustainableThroughputMBs),
                        latFormat.format(outcome.publishLatency.p99),
                        latFormat.format(outcome.endToEndLatency.p99),
                        outcome.reason);

                if (checkpointCallback != null) {
                    checkpointCallback.onTestPointCompleted(i, result);
                }

                cooldownBetweenTests(exploreWorkload.engineConfig.cooldownMinutes, point, i, totalTests);
            }
        } catch (StopRequestedException e) {
            // Surface the stop to BenchmarkRunner so it does not treat the
            // partial result as a completed workload (which would advance the
            // checkpoint and write the final JSON, breaking resume).
            log.warn("Benchmark terminated early due to stop requested");
            worker.stopAll();
            throw e;
        }

        RepeatMerger.mergeExploreRepeats(result);
        return result;
    }

    private ExploreResult.TestOutcome runSingleTest(TestPoint point) throws Exception {
        ExploreConfig exploreConfig = point.exploreConfig();
        ExploreResult.TestOutcome outcome = new ExploreResult.TestOutcome();
        outcome.label = point.label();
        outcome.topics = point.topics();
        outcome.partitionsPerTopic = point.partitionsPerTopic();
        outcome.producersPerTopic = point.producersPerTopic();
        outcome.consumerGroupsPerTopic = point.consumerGroupsPerTopic();
        outcome.consumersPerGroup = point.consumersPerGroup();
        outcome.messageSize = point.messageSize();
        outcome.datasetPreset = point.dataset().preset() != null ? point.dataset().preset() : "custom";
        outcome.datasetFormat = point.dataset().format().name();
        outcome.messageDistributor = point.messageDistributor().toString();
        outcome.consumerType = point.consumerType().toString();

        // Clean up from previous test
        worker.stopAll();
        worker.awaitStopComplete(java.time.Duration.ofMinutes(10));

        // Set kafka config for this test point
        KafkaConfig pointConfig = point.kafkaConfig();
        worker.setKafkaConfig(pointConfig);
        cluster = new KafkaCluster(
                dev.dimster.kafka.KafkaSpi.factory().newAdmin(pointConfig.commonProperties()),
                pointConfig);

        cluster.deleteTestTopics();
        cluster.deleteTestGroups();

        // Plan topology first so the planner owns topic + group naming, then create
        // the actual Kafka topics from the planned names.
        planTopology(point.topics(), point.partitionsPerTopic(),
                point.producersPerTopic(), point.consumerGroupsPerTopic(), point.consumersPerGroup(),
                point.consumerType(), point.consumerProcMsMin(), point.consumerProcMsMax(), point.consumerProcParallelism(),
                point.consumerProcMode(), point.consumerProcCapacity(),
                point.messageDistributor(), point.messageDistributorSkew(),
                false /* correctness off */,
                false /* dynamic consumer assignment */);
        recordPlannedTopology(point.label(), exploreWorkload.engineConfig.printTopology);
        List<String> topics = createPlannedTopics();
        configureDataset(point);
        createConsumers();
        worker.setMaxConsumerRecreations(exploreWorkload.engineConfig.maxConsumerRecreations);
        // Enable per-partition metrics when needed for worst-partition e2e checking
        // or when the user explicitly requested them (e.g. for Prometheus monitoring)
        if (exploreConfig.perPartitionE2eMode || exploreWorkload.engineConfig.perPartitionMetrics) {
            worker.enablePerPartitionMetrics();
        }
        if (exploreWorkload.engineConfig.perConsumerMetrics) {
            worker.enablePerConsumerMetrics();
        }
        createProducers();

        // Verify connectivity
        ensureReady(topics, point.consumerGroupsPerTopic(), 30);

        throwOnStopRequested(stopRequested);

        // Start at initial rate (resolved per-test-point from explore config)
        worker.startLoad(new ProducerWorkAssignment(
                exploreConfig.initialRate, point.messageDistributor(),
                point.partitionsPerTopic(), point.messageDistributorSkew()));

        // Find peak sustainable rate
        findPeakRate(exploreConfig, outcome, point);

        // stop producing as the test point is over
        worker.pauseProducers();

        return outcome;
    }

    // ==================== Core Algorithm ====================

    /**
     * Three-phase algorithm to find peak sustainable rate.
     */
    private void findPeakRate(ExploreConfig config, ExploreResult.TestOutcome outcome,
                              TestPoint point) throws Exception {
        if (this.useDeliveryLatency) {
            String reason = point.consumerProcMsMax() > 0 ? "Consumer processing time configured"
                    : "useDeliveryLatencyForThreshold configured";
            log.info("Using batch delivery end-to-end latency instead of per-message. Reason: {}", reason);
        }

        // Phase 1: Find saturation point.
        // When consumer processing time is configured AND processing is sequential,
        // the theoretical max throughput is calculable — skip the ramp and use it
        // as the upper bound for binary search instead. With concurrent processing
        // the max depends on poll batch sizes which aren't known ahead of time.
        RampResult ramp;
        if (point.consumerProcMsMin() > 0 && point.consumerProcParallelism() == 1) {
            int totalConsumers = point.topics() * point.consumerGroupsPerTopic() * point.consumersPerGroup();
            double theoreticalMax = theoreticalMaxRate(
                    point.consumerProcMsMin(), point.consumerProcMsMax(),
                    totalConsumers);
            log.info("    Consumer processing time detected ({}–{} ms). "
                            + "Theoretical max: {} msgs/s ({} consumers x {} msgs/s/consumer). "
                            + "Skipping ramp, using [0 - {}] as search range.",
                    point.consumerProcMsMin(), point.consumerProcMsMax(),
                    rateFormat.format(theoreticalMax),
                    totalConsumers,
                    rateFormat.format(1000.0 / ((point.consumerProcMsMin() + point.consumerProcMsMax()) / 2.0)),
                    rateFormat.format(theoreticalMax));
            ramp = new RampResult(0, theoreticalMax, null);
        } else {
            // Warmup
            if (config.rampWarmupSeconds > 0) {
                log.info("    Warming up for {}s at {} msg/s...", config.rampWarmupSeconds, config.initialRate);
                Sleeper.sleepOrStop(stopRequested, config.rampWarmupSeconds * 1000L, true);
                worker.getPeriodStats(); // discard — resets all counters
            }
            ramp = findSaturationPoint(config);
        }

        if (ramp.saturatedRate < 0) {
            outcome.converged = false;
            outcome.reason = ramp.reason;
            return;
        }

        double low = 0;
        double high = ramp.saturatedRate;
        FoundRate lastFound = null;

        // Fast path: when the theoretical max is known (proc time configured AND
        // sequential processing), drain, linearly ramp to the limit over 60s in
        // 6 steps, then sustain. Repeat at 95% if the first attempt fails. Most
        // well-tuned scenarios hit the consumer-side ceiling, so this skips the
        // binary search entirely. The linear ramp replaces a sudden rate jump that
        // produced spurious sustain failures from initial-burst transients. If both
        // attempts fail we fall through with a tighter upper bound for the standard
        // search. Concurrent processing disables this path because the max depends
        // on unknown poll batch sizes.
        if (point.consumerProcMsMin() > 0 && point.consumerProcParallelism() == 1) {
            double theoreticalMax = high;
            double[] fastCandidates = {theoreticalMax, theoreticalMax * 0.95};
            boolean firstCandidate = true;
            for (double candidate : fastCandidates) {
                throwOnStopRequested(stopRequested);
                log.info("    Fast-path: ramping to {} msgs/s ({}% of theoretical max) then sustaining",
                        rateFormat.format(candidate),
                        (int) Math.round(candidate / theoreticalMax * 100));

                // Drain backlog from prior state (initialRate on the first
                // iteration, the previous failed candidate on the second) so
                // the ramp starts from a clean baseline.
                if (!firstCandidate)
                    stabilize(config, point);

                firstCandidate = false;

                // Linear ramp into the candidate rate. If latency is
                // catastrophic mid-ramp the rate is clearly unsustainable —
                // skip the sustain, tighten the upper bound, move on.
                if (!linearRampToTarget(candidate, config, point)) {
                    log.info("    Fast-path: sustain skipped at {} msgs/s (ramp aborted)",
                            rateFormat.format(candidate));
                    high = candidate;
                    continue;
                }

                // Producers are now running at candidate rate with the system
                // warmed up — skip sustain's stabilize step (running it again
                // would drain everything the ramp just built up). Sustain's
                // own first-interval discard still runs as a final warm-up
                // before measurement begins.
                SustainResult sustain = sustain(config, candidate, point, true);
                if (sustain.passed) {
                    String reason = firstCandidate ? "converged at theoretical limit"
                            : "converged at 95% theoretical limit";
                    outcome.peakSustainableRate = candidate;
                    outcome.peakSustainableThroughputMBs = candidate * point.messageSize() / 1_000_000.0;
                    outcome.foundRateUpper = candidate;
                    outcome.converged = true;
                    outcome.reason = reason;
                    populateLatencyBreakdown(outcome.publishLatency, sustain.aggPublishLatency);
                    populateLatencyBreakdown(outcome.endToEndLatency, sustain.aggE2eLatency);
                    populateLatencyBreakdown(outcome.endToEndDeliveryLatency, sustain.aggE2eDeliveryLatency);
                    return;
                }
                log.info("    Fast-path: sustain failed at {} msgs/s (pass rate {}/{})",
                        rateFormat.format(candidate),
                        sustain.goodIntervals, sustain.totalIntervals);
                // Anything at or above this rate has been shown to fail —
                // tighten the upper bound for any subsequent attempt.
                high = candidate;
            }
            log.info("    Fast-path exhausted, falling back to binary search [{} - {}]",
                    rateFormat.format(low), rateFormat.format(high));
        }

        // Phase 2 + 3: Binary search with retries
        for (int attempt = 1; attempt <= config.searchMaxRetries; attempt++) {
            throwOnStopRequested(stopRequested);

            try {
                checkForFailedClients(worker.getHealth());
            } catch (RuntimeException e) {
                // Clients failed
                outcome.converged = false;
                outcome.reason = "Client failures: " + e.getMessage();
                return;
            }

            if (attempt == 1) {
                log.info("    === Attempt {}/{} ===", attempt, config.searchMaxRetries);
            } else {
                log.info("    === Attempt {}/{} (narrowed range [{} - {}]) ===",
                        attempt, config.searchMaxRetries, rateFormat.format(low), rateFormat.format(high));
            }

            // Phase 2: Binary search with long measurements
            FoundRate found = binarySearch(config, low, high, point);

            if (found == null) {
                // Search returned null due to min rate floor, not stop requested
                outcome.converged = false;
                outcome.reason = "rate below minimum viable threshold";
                return;
            }

            lastFound = found;

            // Phase 3: Sustain. Skip the pre-sustain drain when the binary
            // search converged on a successful window — producers are already
            // running at found.rate() with no backlog. When the last window
            // failed (e.g. converged via high = mid step-down), the drain is
            // required to clear the saturation backlog before measuring at
            // the lower rate.
            SustainResult sustain = sustain(config, found.rate(), point, found.lastWindowSucceeded());

            if (sustain.passed) {
                outcome.peakSustainableRate = found.rate();
                outcome.peakSustainableThroughputMBs = found.rate() * point.messageSize() / 1_000_000.0;
                outcome.foundRateUpper = found.high();
                outcome.converged = true;
                outcome.reason = "converged";
                populateLatencyBreakdown(outcome.publishLatency, sustain.aggPublishLatency);
                populateLatencyBreakdown(outcome.endToEndLatency, sustain.aggE2eLatency);
                populateLatencyBreakdown(outcome.endToEndDeliveryLatency, sustain.aggE2eDeliveryLatency);
                return;
            }

            // Sustain failed — reduce upper bound and retry search
            log.info("    Sustain failed at {} msgs/s (pass rate {}/{}), retrying with reduced range [{} - {}]",
                    rateFormat.format(found.rate()), sustain.goodIntervals, sustain.totalIntervals,
                    rateFormat.format(low), rateFormat.format(found.rate()));
            high = found.rate();
        }

        // All retries exhausted — leave metrics at zero
        outcome.converged = false;
        if (lastFound != null) {
            outcome.reason = "sustain failed after " + config.searchMaxRetries
                    + " retries (last attempt: " + rateFormat.format(lastFound.rate()) + " msgs/s)";
        } else {
            outcome.reason = "search did not converge";
        }
    }

    // ==================== Phase 1: Ramp ====================

    record RampResult(double lastGoodRate, double saturatedRate, String reason) {}

    /**
     * Phase 1: Double rate until saturation. Short measurements (rampIntervalSeconds).
     * Returns [initialRate, saturatedRate] as the search range.
     */
    private RampResult findSaturationPoint(ExploreConfig config) throws Exception {
        int intervalMs = config.rampIntervalSeconds * 1000;
        double threshold = config.e2eLatencyThresholdMs;
        double percentile = config.e2eLatencyPercentile;

        double currentRate = config.initialRate;
        double lastGoodRate = 0;

        log.info("    Phase 1: Ramp-up from {} msgs/s ({}s intervals)",
                rateFormat.format(currentRate), config.rampIntervalSeconds);

        while (!stopRequested.get()) {
            MeasuredRate measured = measureAtRateWithRetry(currentRate, intervalMs, percentile,
                    config.perPartitionE2eMode, this.useDeliveryLatency);

            double delayThreshold = config.publishDelayThresholdMs;

            String e2eLabel = this.useDeliveryLatency ? "e2e-dlv" : "e2e";
            log.info("    Ramp: target={} actual={} p{}-{}={} ms{} (limit {}) delay={} ms (limit {})",
                    rateFormat.format(currentRate),
                    rateFormat.format(measured.actualPublishRate),
                    (int) percentile, e2eLabel, latFormat.format(measured.e2eLatencyAtPercentile),
                    formatWorstPartition(measured.worstPartition),
                    latFormat.format(threshold),
                    latFormat.format(measured.publishDelayAtPercentile),
                    latFormat.format(delayThreshold));

            boolean delayExceeded = measured.publishDelayAtPercentile > delayThreshold;
            boolean e2eExceeded = measured.e2eLatencyAtPercentile > threshold;

            if (delayExceeded || e2eExceeded) {
                String reason = (delayExceeded && e2eExceeded) ? "e2e+delay exceeded"
                        : e2eExceeded ? "e2e exceeded" : "delay exceeded";
                log.info("    {} at target {} (actual {}, delay={} ms, e2e={} ms), search range [{} - {}]",
                        reason, rateFormat.format(currentRate),
                        rateFormat.format(measured.actualPublishRate),
                        latFormat.format(measured.publishDelayAtPercentile),
                        latFormat.format(measured.e2eLatencyAtPercentile),
                        rateFormat.format(lastGoodRate > 0 ? lastGoodRate : currentRate / 4),
                        rateFormat.format(currentRate));

                if (lastGoodRate == 0) {
                    lastGoodRate = currentRate / 4;
                }
                return new RampResult(lastGoodRate, currentRate, null);
            }

            lastGoodRate = currentRate;
            currentRate *= 2;
        }

        throwOnStopRequested(stopRequested);

        return new RampResult(lastGoodRate, -1,
                "did not reach saturation");
    }

    // ==================== Fast-path ramp ====================

    /**
     * Linearly ramp the producer rate from 0 up to targetRate in
     * FAST_PATH_RAMP_STEPS equal steps of FAST_PATH_RAMP_STEP_SECONDS each.
     * Each step holds the new rate for the full step duration and measures
     * latency; if catastrophic latency is observed at any step the ramp
     * aborts early.
     *
     * Used only by the fast path (consumer processing time configured) so the
     * system warms up smoothly into the theoretical max instead of jumping
     * from the stabilize-drain rate straight to the full target. Sudden jumps
     * cause initial-burst transients (producer queues filling, consumer poll
     * latency spiking) that produced spurious sustain failures when the rate
     * was actually achievable. Walking the rate up in equal steps lets the
     * system reach steady-state before measurement starts.
     *
     * Producers are assumed to already exist (started via startLoad in run())
     * and to have been drained by a prior stabilize() call. This method only
     * changes the rate, it does not start or pause producers.
     *
     * Returns true if all steps completed without catastrophic latency, false
     * if the ramp was aborted. On true, producers are running at targetRate
     * when this returns and the caller can hand off directly to sustain
     * without another stabilize/warmup step.
     */
    private boolean linearRampToTarget(double targetRate, ExploreConfig config,
                                       TestPoint point) throws Exception {
        // Use the same lenient threshold sustain uses, so the catastrophic
        // check is consistent between ramp and sustain phases.
        double threshold = lenientThreshold(config.e2eLatencyThresholdMs, config.sustainLeniency);
        double percentile = config.e2eLatencyPercentile;
        int stepMs = FAST_PATH_RAMP_STEP_SECONDS * 1000;

        log.info("    Fast-path ramp: 0 -> {} msgs/s in {} steps of {}s",
                rateFormat.format(targetRate), FAST_PATH_RAMP_STEPS, FAST_PATH_RAMP_STEP_SECONDS);

        for (int step = 1; step <= FAST_PATH_RAMP_STEPS && !stopRequested.get(); step++) {
            double stepRate = targetRate * step / FAST_PATH_RAMP_STEPS;

            worker.adjustPublishRate(stepRate);
            // Discard any stats accumulated from the prior step / stabilize
            // so this step's measurement reflects only the new rate.
            worker.getPeriodStats();

            long startNanos = System.nanoTime();
            Sleeper.sleepOrStop(stopRequested, stepMs, true);

            PeriodStats stats = worker.getPeriodStats(config.perPartitionE2eMode);
            double elapsedSecs = (System.nanoTime() - startNanos) / 1_000_000_000.0;
            MeasuredRate measured = measureRate(stats, elapsedSecs, percentile,
                    config.perPartitionE2eMode, this.useDeliveryLatency);

            String e2eLabel = this.useDeliveryLatency ? "e2e-dlv" : "e2e";
            log.info("    Ramp[{}/{}]: target={} actual={} p{}-{}={} ms{} delay={} ms",
                    step, FAST_PATH_RAMP_STEPS,
                    rateFormat.format(stepRate),
                    rateFormat.format(measured.actualPublishRate),
                    (int) percentile, e2eLabel,
                    latFormat.format(measured.e2eLatencyAtPercentile),
                    formatWorstPartition(measured.worstPartition),
                    latFormat.format(measured.publishDelayAtPercentile));

            if (measured.e2eLatencyAtPercentile > threshold * CATASTROPHIC_E2E_MULTIPLIER) {
                log.info("    Ramp aborted: catastrophic e2e latency ({} ms > {}x threshold {} ms) at step {}/{}",
                        latFormat.format(measured.e2eLatencyAtPercentile),
                        CATASTROPHIC_E2E_MULTIPLIER,
                        latFormat.format(threshold),
                        step, FAST_PATH_RAMP_STEPS);
                return false;
            }
        }

        throwOnStopRequested(stopRequested);
        return true;
    }

    // ==================== Phase 2: Binary Search ====================

    /**
     * Result of binary search: the chosen rate, the final convergence range
     * bounds, and whether the very last measurement window in the search
     * succeeded.
     *
     * `lastWindowSucceeded` lets the caller decide whether to drain (stabilize)
     * before sustain. When true, producers are already running at `rate` with
     * no backlog and sustain can begin immediately. When false, producers are
     * at a higher saturated rate with likely backlog and sustain must drain
     * first before stepping down to `rate`.
     */
    record FoundRate(double rate, double high, boolean lastWindowSucceeded) {}

    /**
     * Phase 2: Binary search with long measurement windows.
     * Each measurement runs for searchDurationSeconds, checking every
     * searchIntervalSeconds for saturation.
     *
     * Returns the found rate with convergence bounds, or null if interrupted/deadline.
     */
    private FoundRate binarySearch(ExploreConfig config, double low, double high,
                                    TestPoint point) throws Exception {
        double bestRate = low;
        // Tracks whether the most recent window measurement was sustainable.
        // - false on entry: the first round must always stabilize since
        //   producers arrive here at the ramp's saturated rate (or a previously
        //   failed sustain rate), which is higher than the first mid.
        // - true after a successful round: producers are running at mid with
        //   no backlog, and the next round's mid is HIGHER (low moved up), so
        //   we can skip the inter-window drain and just raise the rate.
        // - false after a failed round: producers are at a saturated mid with
        //   likely backlog, and the next round's mid is LOWER (high moved
        //   down), so we must drain before measuring.
        boolean lastWindowSucceeded = false;

        log.info("    Phase 2: Binary search [{} - {}] ({}s windows, {}s checks)",
                rateFormat.format(low), rateFormat.format(high),
                config.searchDurationSeconds, config.searchIntervalSeconds);

        for (int i = 0; i < 20 && !stopRequested.get(); i++) {
            double range = high - low;
            double mid = (low + high) / 2;

            if (mid < config.minViableRate) {
                log.info("    Search range collapsed below {} msgs/s — system cannot sustain load for this test point",
                        rateFormat.format(config.minViableRate));
                return null;
            }

            if (range < mid * 0.05) {
                log.info("    Converged: range {} < 5% of midpoint {} → found rate {} [{} - {}]",
                        rateFormat.format(range), rateFormat.format(mid),
                        rateFormat.format(bestRate),
                        rateFormat.format(low), rateFormat.format(high));
                return new FoundRate(bestRate, high, lastWindowSucceeded);
            }

            // Stabilize (when needed) then measure at midpoint for a long window
            log.info("    --- Search round {}: testing {} msgs/s (range [{} - {}]) ---",
                    i + 1, rateFormat.format(mid),
                    rateFormat.format(low), rateFormat.format(high));
            if (lastWindowSucceeded) {
                log.info("    Skipping stabilize — previous window succeeded");
            } else {
                stabilize(config, point);
            }
            boolean sustainable = measureLongWindow(config, mid, point);

            if (sustainable) {
                low = mid;
                bestRate = mid;
                lastWindowSucceeded = true;
                log.info("    --- Round {} result: GOOD → new range [{} - {}] ---",
                        i + 1, rateFormat.format(low), rateFormat.format(high));
            } else {
                high = mid;
                lastWindowSucceeded = false;
                log.info("    --- Round {} result: BAD → new range [{} - {}] ---",
                        i + 1, rateFormat.format(low), rateFormat.format(high));
            }
        }

        throwOnStopRequested(stopRequested);

        // Didn't fully converge but return best effort
        return new FoundRate(bestRate, high, lastWindowSucceeded);
    }

    /**
     * Run at the given rate for searchDurationSeconds, checking every
     * searchIntervalSeconds for saturation.
     *
     * Returns true if the rate is sustainable (no saturation detected).
     */
    private boolean measureLongWindow(ExploreConfig config, double rate,
                                       TestPoint point) throws Exception {
        double threshold = config.e2eLatencyThresholdMs;
        double delayThreshold = config.publishDelayThresholdMs;
        double percentile = config.e2eLatencyPercentile;

        int checkIntervalMs = config.searchIntervalSeconds * 1000;
        int totalChecks = config.searchDurationSeconds / config.searchIntervalSeconds;

        worker.adjustPublishRate(rate);
        // Give producers time to pick up new rate
        int warmupTimeMs = Math.min(30000, checkIntervalMs);
        log.info("    Adjusted rate to {}, warm-up time {} ms", rateFormat.format(rate), warmupTimeMs);
        Sleeper.sleepOrStop(stopRequested, warmupTimeMs, true);
        worker.getPeriodStats(); // discard — resets all counters

        int consecutiveBad = 0;
        int goodChecks = 0;
        int totalChecked = 0;

        for (int i = 0; i < totalChecks && !stopRequested.get(); i++) {
            long startNanos = System.nanoTime();
            Sleeper.sleepOrStop(stopRequested, checkIntervalMs, true);

            PeriodStats stats = worker.getPeriodStats(config.perPartitionE2eMode);
            double elapsedSecs = (System.nanoTime() - startNanos) / 1_000_000_000.0;
            MeasuredRate measured = measureRate(stats, elapsedSecs, percentile,
                    config.perPartitionE2eMode, this.useDeliveryLatency);
            totalChecked++;

            boolean delayExceeded = measured.publishDelayAtPercentile > delayThreshold;
            boolean e2eExceeded = measured.e2eLatencyAtPercentile > threshold;
            boolean bad = delayExceeded || e2eExceeded;

            String badReason = bad
                    ? buildBadReason(delayExceeded, e2eExceeded)
                    : "[OK]";

            String e2eLabel = this.useDeliveryLatency ? "e2e-dlv" : "e2e";
            log.info("    Window[{}/{}]: actual={} p{}-{}={} ms{} (limit {}) behind-sched={} ms (limit {}) {}",
                    i + 1, totalChecks,
                    rateFormat.format(measured.actualPublishRate),
                    (int) percentile, e2eLabel, latFormat.format(measured.e2eLatencyAtPercentile),
                    formatWorstPartition(measured.worstPartition),
                    latFormat.format(threshold),
                    latFormat.format(measured.publishDelayAtPercentile),
                    latFormat.format(delayThreshold),
                    badReason);

            if (measured.e2eLatencyAtPercentile > threshold * CATASTROPHIC_E2E_MULTIPLIER) {
                log.info("    Catastrophic e2e latency ({} ms > {}x threshold {} ms) — aborting window",
                        latFormat.format(measured.e2eLatencyAtPercentile),
                        CATASTROPHIC_E2E_MULTIPLIER,
                        latFormat.format(threshold));
                return false;
            }

            if (bad) {
                consecutiveBad++;
                if (consecutiveBad >= SATURATION_THRESHOLD) {
                    log.info("    Saturated: {} consecutive bad checks", consecutiveBad);
                    return false;
                }
            } else {
                consecutiveBad = 0;
                goodChecks++;
            }

            // Early exit if it's mathematically impossible to reach 80% good
            int remaining = totalChecks - totalChecked;
            int bestPossibleGood = goodChecks + remaining;
            if (bestPossibleGood < totalChecks * 0.8) {
                int badChecks = totalChecked - goodChecks;
                log.info("    Early exit: too many bad checks ({}/{}), cannot reach 80% good",
                        badChecks, totalChecked);
                return false;
            }
        }

        // Survived the full window — check overall health
        double goodRatio = totalChecked > 0 ? (double) goodChecks / totalChecked : 0;
        if (goodRatio < 0.8) {
            log.info("    Marginal: only {}/{} good checks ({} %)",
                    goodChecks, totalChecked, (int) (goodRatio * 100));
            return false;
        }

        return true;
    }

    // ==================== Phase 3: Sustain ====================

    record SustainResult(boolean passed, int goodIntervals, int totalIntervals,
                         Histogram aggPublishLatency, Histogram aggE2eLatency,
                         Histogram aggE2eDeliveryLatency) {}

    /**
     * Phase 3: Hold rate for sustainDurationSeconds and verify stability.
     *
     * `skipStabilize` lets the caller bypass the pre-sustain drain when the
     * producers are already running at `rate` with no backlog — typically
     * because the binary search just converged on a successful window at the
     * same rate. Skipping avoids 10–120s of wasted drain time per converged
     * test point. The caller MUST only pass true when the producer state is
     * known to be healthy at `rate`; otherwise pass false to force a drain.
     */
    private SustainResult sustain(ExploreConfig config, double rate,
                                   TestPoint point, boolean skipStabilize) throws Exception {
        double threshold = lenientThreshold(config.e2eLatencyThresholdMs, config.sustainLeniency);
        double delayThreshold = lenientThreshold(config.publishDelayThresholdMs, config.sustainLeniency);
        double percentile = config.e2eLatencyPercentile;

        int checkIntervalMs = config.searchIntervalSeconds * 1000;
        int sustainIntervals = Math.max(1,
                config.sustainDurationSeconds / config.searchIntervalSeconds);

        log.info("    Phase 3: Sustaining {} msgs/s for {}s ({} intervals)",
                rateFormat.format(rate), config.sustainDurationSeconds, sustainIntervals);

        Histogram aggPublishLatency = new Histogram(dev.dimster.stats.StatsCollector.NON_E2E_LATENCY_MAX_MICROS,
                dev.dimster.stats.StatsCollector.LATENCY_SIG_DIGITS);
        Histogram aggE2eLatency = new Histogram(dev.dimster.stats.StatsCollector.E2E_LATENCY_MAX_MICROS,
                dev.dimster.stats.StatsCollector.LATENCY_SIG_DIGITS);
        Histogram aggE2eDeliveryLatency = new Histogram(dev.dimster.stats.StatsCollector.E2E_LATENCY_MAX_MICROS,
                dev.dimster.stats.StatsCollector.LATENCY_SIG_DIGITS);

        if (skipStabilize) {
            log.info("    Skipping stabilize — last window at sustainable rate");
        } else {
            stabilize(config, point);
        }
        worker.adjustPublishRate(rate);
        // Discard first interval (transition period)
        Sleeper.sleepOrStop(stopRequested, checkIntervalMs, true);
        worker.getPeriodStats();

        int goodIntervals = 0;
        int totalChecked = 0;

        for (int i = 0; i < sustainIntervals && !stopRequested.get(); i++) {
            long startNanos = System.nanoTime();
            Sleeper.sleepOrStop(stopRequested, checkIntervalMs, true);

            PeriodStats stats = worker.getPeriodStats(config.perPartitionE2eMode);
            double elapsedSecs = (System.nanoTime() - startNanos) / 1_000_000_000.0;
            MeasuredRate measured = measureRate(stats, elapsedSecs, percentile,
                    config.perPartitionE2eMode, this.useDeliveryLatency);
            totalChecked++;

            aggPublishLatency.add(stats.publishLatency);
            aggE2eLatency.add(stats.endToEndLatency);
            aggE2eDeliveryLatency.add(stats.endToEndDeliveryLatency);

            boolean delayExceeded = measured.publishDelayAtPercentile > delayThreshold;
            boolean e2eExceeded = measured.e2eLatencyAtPercentile > threshold;
            boolean bad = delayExceeded || e2eExceeded;

            String badReason = bad
                    ? buildBadReason(delayExceeded, e2eExceeded)
                    : "[OK]";

            String e2eLabel = this.useDeliveryLatency ? "e2e-dlv" : "e2e";
            log.info("    Sustain[{}/{}]: actual={} p{}-{}={} ms{} (limit {}) behind-sched={} ms (limit {}) {}",
                    i + 1, sustainIntervals,
                    rateFormat.format(measured.actualPublishRate),
                    (int) percentile, e2eLabel, latFormat.format(measured.e2eLatencyAtPercentile),
                    formatWorstPartition(measured.worstPartition),
                    latFormat.format(threshold),
                    latFormat.format(measured.publishDelayAtPercentile),
                    latFormat.format(delayThreshold),
                    badReason);

            if (!bad) {
                goodIntervals++;
            }

            if (measured.e2eLatencyAtPercentile > threshold * CATASTROPHIC_E2E_MULTIPLIER) {
                log.info("    Catastrophic e2e latency ({} ms > {}x threshold {} ms) — aborting sustain",
                        latFormat.format(measured.e2eLatencyAtPercentile),
                        CATASTROPHIC_E2E_MULTIPLIER,
                        latFormat.format(threshold));
                return new SustainResult(false, goodIntervals, totalChecked,
                        aggPublishLatency, aggE2eLatency, aggE2eDeliveryLatency);
            }

            // Early exit if it's mathematically impossible to reach 80% good
            int remaining = sustainIntervals - totalChecked;
            int bestPossibleGood = goodIntervals + remaining;
            if (bestPossibleGood < sustainIntervals * 0.8) {
                int badIntervals = totalChecked - goodIntervals;
                log.info("    Early exit: too many bad intervals ({}/{}), cannot reach 80% good",
                        badIntervals, totalChecked);
                break;
            }
        }

        double passRate = totalChecked > 0 ? (double) goodIntervals / totalChecked : 0;
        boolean passed = passRate >= 0.8;

        log.info("    Sustain result: {}/{} good intervals ({} %) → {}",
                goodIntervals, totalChecked, (int) (passRate * 100),
                passed ? "PASS" : "FAIL");

        return new SustainResult(passed, goodIntervals, totalChecked,
                aggPublishLatency, aggE2eLatency, aggE2eDeliveryLatency);
    }

    // ==================== Measurement Helpers ====================

    /**
     * Measure at a rate with retry on transient HTTP errors.
     */
    private MeasuredRate measureAtRateWithRetry(double rate, int intervalMs,
                                                double percentile, boolean perPartitionE2e) throws Exception {
        return measureAtRateWithRetry(rate, intervalMs, percentile, perPartitionE2e, false);
    }

    private MeasuredRate measureAtRateWithRetry(double rate, int intervalMs,
                                                double percentile, boolean perPartitionE2e,
                                                boolean useDeliveryLatency) throws Exception {
        try {
            return measureAtRate(rate, intervalMs, percentile, perPartitionE2e, useDeliveryLatency);
        } catch (Exception e) {
            if (isTransientError(e)) {
                log.warn("    Transient error during measurement, retrying: {}", e.getMessage());
                Sleeper.sleepOrStop(stopRequested, 2000, true);
                return measureAtRate(rate, intervalMs, percentile, perPartitionE2e, useDeliveryLatency);
            }
            throw e;
        }
    }

    private boolean isTransientError(Throwable e) {
        String msg = e.getMessage();
        if (msg != null && (msg.contains("Connection reset") || msg.contains("header parser received no bytes"))) {
            return true;
        }
        return e.getCause() != null && isTransientError(e.getCause());
    }

    /**
     * Short single-interval measurement (used during ramp phase).
     */
    private MeasuredRate measureAtRate(double rate, int intervalMs,
                                       double percentile, boolean perPartitionE2e,
                                       boolean useDeliveryLatency) throws Exception {
        worker.adjustPublishRate(rate);
        // warm-up period
        Sleeper.sleepOrStop(stopRequested, Math.min(intervalMs, 2000), true);
        worker.getPeriodStats(); // discard — atomically resets ALL counters

        // measurement period
        long startNanos = System.nanoTime();
        Sleeper.sleepOrStop(stopRequested, intervalMs, true);
        PeriodStats stats = worker.getPeriodStats(perPartitionE2e);
        double elapsedSecs = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        return measureRate(stats, elapsedSecs, percentile, perPartitionE2e, useDeliveryLatency);
    }

    /**
     * Stabilize the system by draining any in-flight messages.
     * Reduces producers to a low trickle rate (config.stabilizeRate),
     * then waits for consumers to reach quiescence (two consecutive
     * matching totalMessagesReceived values).
     * Uses a trickle instead of a full pause because Kafka 4.2 share
     * groups have a bug that stalls drain when producers are stopped.
     * Uses a configurable timeout (stabilizeTimeoutSeconds, default 120s)
     * because failed high-rate attempts can leave millions of stale messages
     * whose e2e latency would contaminate subsequent measurements.
     */
    private void stabilize(ExploreConfig config, TestPoint point) throws Exception {
        worker.adjustPublishRate(config.stabilizeRate);

        // Temporarily remove consumer processing time so consumers can drain
        // quickly. A high per-record processing time would prevent catch-up
        // because the trickle rate alone is not enough to offset the delay.
        boolean hadProcessingTime = point.consumerProcMsMin() > 0;
        if (hadProcessingTime) {
            log.info("    Stabilize: Temporarily removing consumer processing time for drain");
            worker.setConsumerProcessingTime(0, 0);
        }

        // Extract unique group names for broker lag check — when local counters
        // show a phantom backlog but broker lag is 0, we can exit early instead
        // of waiting out the full stabilize timeout.
        Set<String> groupNames = plannedTopology.consumers().stream()
                .map(vc -> vc.consumerGroup())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        boolean isShareGroup = point.consumerType() == ConsumerType.SHARE_GROUP;

        int timeoutSeconds = config.stabilizeTimeoutSeconds;
        log.info("    Stabilize: Producers reduced to {} msgs/s. Waiting for consumers to drain (up to {}s)...",
                config.stabilizeRate, timeoutSeconds);
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        boolean caughtUp = false;
        long lastRecv = 0;

        while (System.currentTimeMillis() < deadline) {
            Sleeper.sleepOrStop(stopRequested, 10_000, true);
            CountersStats counterStats = worker.getCountersStats();
            long sent = counterStats.messagesSent();
            long received = counterStats.messagesReceived();
            if (sent - received <= config.stabilizeRate) {
                log.info("    Stabilize: Consumers caught up");
                caughtUp = true;
                break;
            } else if (received == lastRecv) {
                // Check broker-reported lag — if 0, remaining backlog is phantom
                // messages that will never arrive (e.g. lost in-flight acks)
                long totalLag = 0;
                for (String group : groupNames) {
                    Map<TopicPartition, Long> lagByPartition = isShareGroup
                            ? cluster.getShareGroupLag(group)
                            : cluster.getConsumerGroupLag(group);
                    totalLag += lagByPartition.values().stream().mapToLong(Long::longValue).sum();
                }
                if (totalLag == 0) {
                    log.info("    Stabilize: Broker lag is 0 but local counters show backlog (sent={}, received={}). "
                            + "Either lost messages or bad Dimster counters  — breaking out early.", sent, received);
                    caughtUp = true;
                    break;
                }
                log.info("    Stabilize: Messages sent total: {}, messages received total: {}, broker lag: {}",
                        sent, received, totalLag);
            } else {
                log.info("    Stabilize: Messages sent total: {}, messages received total: {}",
                        sent, received);
            }

            lastRecv = received;
        }
        if (!caughtUp) {
            log.warn("    Stabilize: Consumer did not catch up after {}s timeout", timeoutSeconds);
        }

        // Restore original processing time before resuming measurements
        if (hadProcessingTime) {
            log.info("    Stabilize: Restoring consumer processing time: min={} ms, max={} ms",
                    point.consumerProcMsMin(), point.consumerProcMsMax());
            worker.setConsumerProcessingTime(point.consumerProcMsMin(), point.consumerProcMsMax());
        }

        // Flush any stale latency data accumulated during drain
        worker.getPeriodStats(); // discard — resets period counters
        Sleeper.sleepOrStop(stopRequested, 2000, true);
    }

    private MeasuredRate measureRate(PeriodStats stats, double elapsedSecs,
                                     double percentile, boolean perPartitionE2e,
                                     boolean useDeliveryLatency) {
        double pubRate = stats.messagesSent / elapsedSecs;
        double conRate = stats.messagesReceived / elapsedSecs;
        double latAtPercentile = microsToMillis(stats.publishLatency.getValueAtPercentile(percentile));
        double delayAtPercentile = microsToMillis(stats.publishDelayLatency.getValueAtPercentile(percentile));

        // Choose aggregate and per-partition histograms based on whether
        // delivery latency is selected for threshold checks
        Histogram aggregateE2e = useDeliveryLatency
                ? stats.endToEndDeliveryLatency
                : stats.endToEndLatency;
        Map<String, Histogram> ppE2eMap = useDeliveryLatency
                ? stats.perPartitionE2eDeliveryLatency
                : stats.perPartitionE2eLatency;

        // Use worst per-partition e2e latency when enabled and per-partition
        // interval data is available, otherwise fall back to the aggregate histogram
        double e2eAtPercentile;
        String worstPartition = null;
        if (perPartitionE2e && ppE2eMap != null && !ppE2eMap.isEmpty()) {
            WorstPartitionResult worst = worstPartitionE2e(ppE2eMap, percentile);
            if (worst.partition != null) {
                e2eAtPercentile = worst.latencyMs;
                worstPartition = worst.partition;
            } else {
                e2eAtPercentile = microsToMillis(aggregateE2e.getValueAtPercentile(percentile));
            }
        } else {
            e2eAtPercentile = microsToMillis(aggregateE2e.getValueAtPercentile(percentile));
        }

        return new MeasuredRate(pubRate, conRate, latAtPercentile, delayAtPercentile, e2eAtPercentile, worstPartition);
    }

    private void populateLatencyBreakdown(ExploreResult.LatencyBreakdown breakdown, Histogram histogram) {
        breakdown.p50 = microsToMillis(histogram.getValueAtPercentile(50));
        breakdown.p75 = microsToMillis(histogram.getValueAtPercentile(75));
        breakdown.p99 = microsToMillis(histogram.getValueAtPercentile(99));
        breakdown.p999 = microsToMillis(histogram.getValueAtPercentile(99.9));
        breakdown.p9999 = microsToMillis(histogram.getValueAtPercentile(99.99));
    }

    record MeasuredRate(double actualPublishRate, double consumeRate,
                       double latencyAtPercentile, double publishDelayAtPercentile,
                       double e2eLatencyAtPercentile, String worstPartition) {}

    record WorstPartitionResult(String partition, double latencyMs) {}

    // Package-private for testing
    static WorstPartitionResult worstPartitionE2e(Map<String, Histogram> perPartitionE2e, double percentile) {
        String worstPartition = null;
        double worst = 0;
        for (var entry : perPartitionE2e.entrySet()) {
            if (entry.getValue().getTotalCount() == 0) continue;
            double latency = microsToMillis(entry.getValue().getValueAtPercentile(percentile));
            if (latency > worst) {
                worst = latency;
                worstPartition = entry.getKey();
            }
        }
        return new WorstPartitionResult(worstPartition, worst);
    }

    // Package-private for testing
    static double lenientThreshold(double threshold, double leniency) {
        return threshold * (1 + leniency);
    }

    private static String formatWorstPartition(String worstPartition) {
        return worstPartition != null ? " [" + worstPartition + "]" : "";
    }

    static String buildBadReason(boolean delay, boolean e2e) {
        if (delay && e2e) return "[BAD:delay+e2e]";
        if (delay) return "[BAD:delay]";
        return "[BAD:e2e]";
    }

    /**
     * Compute the theoretical maximum consume rate (msgs/sec) when each consumer
     * has a fixed processing time per message. With N total consumers each taking
     * avgProcMs milliseconds per message, all consumers can process at most
     * N * (1000 / avgProcMs) messages per second.
     */
    static double theoreticalMaxRate(int procMsMin, int procMsMax, int totalConsumers) {
        double avgProcMs = (procMsMin + procMsMax) / 2.0;
        double perConsumer = 1000.0 / avgProcMs;
        return perConsumer * totalConsumers;
    }

    // ==================== Setup Helpers ====================


    @Override
    public void close() throws Exception {
        try {
            worker.stopAll();
            worker.awaitStopComplete(java.time.Duration.ofMinutes(5));
        } catch (Exception e) { /* ignore */ }
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
    }

    private static double microsToMillis(long microTime) {
        return microTime / 1000.0;
    }
}
