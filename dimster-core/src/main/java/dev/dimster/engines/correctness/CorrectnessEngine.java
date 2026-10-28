package dev.dimster.engines.correctness;

import dev.dimster.BaseEngine;
import dev.dimster.BaseWorkload;
import dev.dimster.BenchmarkRunner;
import dev.dimster.ConsumerType;
import dev.dimster.RepeatMerger;
import dev.dimster.ScenarioExpander;
import dev.dimster.ScenarioExpander.TestPoint;
import dev.dimster.TestResult;
import dev.dimster.kafka.KafkaCluster;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.kafka.KafkaConsumerWrapper;
import dev.dimster.coordinator.ModificationTarget;
import dev.dimster.coordinator.ModifyRequest;
import dev.dimster.coordinator.ModifyResult;
import dev.dimster.utils.StopRequestedException;
import dev.dimster.worker.Worker;
import dev.dimster.worker.topology.TopologyPlanner;
import dev.dimster.worker.commands.CountersStats;
import dev.dimster.worker.commands.CumulativeLatencies;
import dev.dimster.worker.commands.PeriodStats;
import dev.dimster.worker.commands.ProducerWorkAssignment;
import dev.dimster.utils.Sleeper;
import dev.dimster.hooks.HookPoint;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Orchestrates correctness tests.
 */
public class CorrectnessEngine extends BaseEngine implements ModificationTarget {

    private static final Logger log = LoggerFactory.getLogger(CorrectnessEngine.class);

    private final CorrectnessWorkload workload;
    private final CorrectnessConfig config;
    private final BaseWorkload base;
    private final KafkaConfig fileKafkaConfig;
    private final CorrectnessConsumerRestartController consumerRestartController;

    // Campaign progress tracking — set per test point in the run() loop
    private int currentTestIndex;
    private int totalTestPoints;
    private long testPointStartNanos;

    public CorrectnessEngine(CorrectnessWorkload workload, Worker worker, TopologyPlanner topologyPlanner,
                             KafkaConfig fileKafkaConfig, AtomicBoolean stopRequested) {
        super(worker, topologyPlanner, stopRequested);
        this.workload = workload;
        this.config = workload.correctnessConfig;
        this.base = workload.baseWorkload;
        this.fileKafkaConfig = fileKafkaConfig;
        this.consumerRestartController = new CorrectnessConsumerRestartController(worker);
    }

    public CorrectnessResult run() throws Exception {
        return run(0, null, null);
    }

    public CorrectnessResult run(int startTestPoint, CorrectnessResult partialResult,
                                  BenchmarkRunner.CheckpointCallback checkpointCallback) throws Exception {
        CorrectnessResult result;
        // partial result from checkpoint
        if (partialResult != null) {
            result = partialResult;
        } else {
            result = new CorrectnessResult();
            result.metadata.name = workload.name;
            result.config = workload;
        }

        List<TestPoint> allPoints = ScenarioExpander.expand(base, workload.scenarios, fileKafkaConfig);
        rejectFlink("correctness", allPoints);
        int totalTests = allPoints.size();

        applyPrintMessages(allPoints);

        try {
            for (int i = startTestPoint; i < allPoints.size(); i++) {
                if (stopRequested.get()) break;

                TestPoint point = allPoints.get(i);

                CorrectnessResult.ScenarioResult scenarioResult = findOrCreateScenario(
                        result.scenarioResults,
                        point.scenarioName() != null ? point.scenarioName() : "default",
                        sr -> sr.name, name -> {
                            var sr = new CorrectnessResult.ScenarioResult();
                            sr.name = name;
                            return sr;
                        });

                log.info("--- Correctness test {}/{}: {} ---", i + 1, totalTests, point.label());
                log.info("Merged workload:\n{}", point.toLogString());

                this.currentTestIndex = i;
                this.totalTestPoints = totalTests;
                this.testPointStartNanos = System.nanoTime();

                // Stamp the scenario boundary the first time we touch it, then
                // refresh the end after every test so the exporter knows the
                // exact time range this scenario occupies in Prometheus.
                if (scenarioResult.beginTime == null) {
                    scenarioResult.beginTime = currentScenarioTimestamp();
                }
                runHooks(HookPoint.BEFORE_TEST_POINT, point, i, totalTests, List.of());
                CorrectnessResult.TestOutcome outcome = runSingleTest(point);
                scenarioResult.endTime = currentScenarioTimestamp();
                scenarioResult.tests.add(outcome);

                String status = outcome.correctness.passed ? "PASSED" : "FAILED";
                log.info("    Result: {} — sent={}, received={}, lost={}, duplicates={}, outOfOrder={}, crc={}",
                        status, outcome.correctness.totalSent, outcome.correctness.totalReceived,
                        outcome.correctness.lostMessages, outcome.correctness.duplicateCount,
                        outcome.correctness.outOfOrderCount, outcome.correctness.crcFailureCount);

                if (checkpointCallback != null) {
                    checkpointCallback.onTestPointCompleted(i, result);
                }

                cooldownBetweenTests(workload.engineConfig.cooldownMinutes, point, i, totalTests);
            }
        } catch (StopRequestedException e) {
            // Surface the stop to BenchmarkRunner so it does not treat the
            // partial result as a completed workload (which would advance the
            // checkpoint and write the final JSON, breaking resume).
            log.warn("Benchmark terminated early due to stop requested");
            worker.stopAll();
            throw e;
        }

        RepeatMerger.mergeCorrectnessRepeats(result);

        return result;
    }

    private CorrectnessResult.TestOutcome runSingleTest(TestPoint point) throws Exception {
        if (point.consumerGroupsPerTopic() != 1) {
            throw new IllegalArgumentException(
                    "Correctness mode requires consumerGroupsPerTopic == 1 (got "
                            + point.consumerGroupsPerTopic() + "). Multiple consumer groups would "
                            + "cause each message to be consumed multiple times, triggering false "
                            + "duplicate violations.");
        }

        worker.stopAll();
        worker.awaitStopComplete(java.time.Duration.ofMinutes(10));
        KafkaConfig pointConfig = point.kafkaConfig();
        worker.setKafkaConfig(pointConfig);
        cluster = new KafkaCluster(
                dev.dimster.kafka.KafkaSpi.factory().newAdmin(pointConfig.commonProperties()),
                pointConfig);

        cluster.deleteTestTopics();
        cluster.deleteTestGroups();

        // Enable correctness mode on the worker (payload encoding, tracker setup).
        // The topology-side correctness behavior (one-partition-per-producer pinning,
        // topic affinity, static consumer assignment) is requested via TopologySpec.
        worker.enableCorrectness(config);

        // Plan upfront — in correctness mode the topology pins each producer to a single
        // partition and (when staticConsumerAssignment is on for classic consumers)
        // assigns each consumer a stable subset of partitions.
        boolean staticConsumerPartitions = config.staticConsumerAssignment
                && point.consumerType() != ConsumerType.SHARE_GROUP;
        planTopology(point.topics(), point.partitionsPerTopic(),
                point.producersPerTopic(), point.consumerGroupsPerTopic(), point.consumersPerGroup(),
                point.consumerType(), point.consumerProcMsMin(), point.consumerProcMsMax(), point.consumerProcParallelism(),
                point.consumerProcMode(), point.consumerProcCapacity(),
                point.messageDistributor(), point.messageDistributorSkew(),
                true /* correctness on */,
                staticConsumerPartitions);
        recordPlannedTopology(point.label(), workload.engineConfig.printTopology);
        // Reset the seeded selection sequence for every test point. The
        // action window remains closed until measured CONCURRENT work begins.
        consumerRestartController.prepareTest(plannedTopology, config.consumerRestartSeed);

        // Create the actual Kafka topics from the planned names.
        List<String> topics = createPlannedTopics();

        // Set before producers: in DRAIN mode consumers are created later and
        // read the same dataset's record format.
        configureDataset(point);
        createProducers();

        if (config.testMode == CorrectnessConfig.TestMode.CONCURRENT) {
            // CONCURRENT: create consumers now, verify full readiness
            createConsumers();
            worker.setMaxConsumerRecreations(workload.engineConfig.maxConsumerRecreations);
            if (workload.engineConfig.perPartitionMetrics) {
                worker.enablePerPartitionMetrics();
            }
            if (workload.engineConfig.perConsumerMetrics) {
                worker.enablePerConsumerMetrics();
            }
            ensureReady(topics, point.consumerGroupsPerTopic(), 60);
        } else {
            // DRAIN: defer consumer creation until after backlog is built
            waitForClientsReady("PRODUCER", 60);
        }

        // The correctness fields form each record's _dimster header, ahead of
        // the dataset's columns, so records are larger than messageSize by
        // the header's extra bytes.
        var workAssignment = new ProducerWorkAssignment(
                point.producerRate(), point.messageDistributor(),
                point.partitionsPerTopic(), point.messageDistributorSkew());

        throwOnStopRequested(stopRequested);

        // Run test phases — record per-period performance data
        TestResult.PerformanceResults perf = new TestResult.PerformanceResults();
        CorrectnessTimeSeriesRecorder correctnessSeries = new CorrectnessTimeSeriesRecorder();
        CumulativeLatencies agg = switch (config.testMode) {
            case CONCURRENT -> runConcurrentPhases(workAssignment, point, perf, correctnessSeries);
            case DRAIN -> runBacklogBuildDrainPhases(workAssignment, point, perf, correctnessSeries);
        };

        // Populate aggregated performance results
        recordAggregateResults(perf, agg);

        // Collect correctness results
        CorrectnessResult.TestOutcome outcome = worker.getCorrectnessResult();
        if (outcome == null) {
            outcome = new CorrectnessResult.TestOutcome();
        }
        outcome.label = point.label();
        populateWorkloadConfig(outcome.workloadConfig, point);
        outcome.performance = perf;
        correctnessSeries.copyTo(outcome.correctness);
        consumerRestartController.populateOutcome(outcome);

        return outcome;
    }

    /** Delegate live-action validation to the synchronized lifecycle controller. */
    @Override
    public ModifyResult applyModification(ModifyRequest request) {
        return consumerRestartController.applyModification(request);
    }



    /**
     * Collects periodic stats during a test phase.
     *
     * @param testMode        test mode — controls logging format (CONCURRENT: verbose with progress,
     *                        DRAIN: compact like DrainBacklogEngine)
     * @param duration        maximum duration to collect stats (Duration.ZERO means no time limit)
     * @param targetBacklogBytes  target backlog size in bytes; when > 0 the loop exits once the
     *                            backlog reaches this size (used for size-based backlog building
     *                            in DRAIN mode). 0 means size-based exit is disabled.
     * @param messageSize     message size in bytes, used to estimate backlog bytes from message count.
     *                        Only relevant when targetBacklogBytes > 0.
     * @param perf            performance results to record period data into
     */
    private CumulativeLatencies collectStats(CorrectnessConfig.TestMode testMode,
                                             Duration duration, long targetBacklogBytes,
                                             int messageSize,
                                             TestResult.PerformanceResults perf,
                                             CorrectnessTimeSeriesRecorder correctnessSeries)
            throws IOException {
        long startTime = startMeasurementWindow();
        long oldTime = startTime;
        long endTime = duration.toNanos() > 0 ? startTime + duration.toNanos() : Long.MAX_VALUE;

        CumulativeLatencies agg = new CumulativeLatencies();

        while (!stopRequested.get()) {
            Sleeper.sleepOrStop(stopRequested, config.sampleRateMillis, true);

            PeriodStats stats = worker.getPeriodStats();
            agg.endToEndLatency.add(stats.endToEndLatency);
            agg.endToEndDeliveryLatency.add(stats.endToEndDeliveryLatency);
            agg.publishDelayLatency.add(stats.publishDelayLatency);
            agg.scheduleLatency.add(stats.scheduleLatency);
            agg.publishLatency.add(stats.publishLatency);

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;
            double publishRate = stats.messagesSent / elapsed;
            double consumeRate = stats.messagesReceived / elapsed;

            recordPeriodResults(perf, stats, publishRate, consumeRate);
            CorrectnessResult.TestOutcome currentOutcome = correctnessSeries.record(
                    worker.getCorrectnessResult());

            if (testMode == CorrectnessConfig.TestMode.DRAIN) {
                // Compact build-phase logging matching DrainBacklogEngine format
                CountersStats counters = worker.getCountersStats();
                long sentMessages = counters.messagesSent();
                long sentBytes = sentMessages * messageSize;

                if (targetBacklogBytes > 0) {
                    long pct = (sentBytes * 100) / targetBacklogBytes;
                    log.info("    Building: pub={} msg/s, {} msgs, {}MB / {}MB ({}%)",
                            (long) publishRate, sentMessages, sentBytes / 1_000_000,
                            targetBacklogBytes / 1_000_000, pct);
                } else {
                    double elapsedMinutes = (now - startTime) / 1e9 / 60.0;
                    int totalMin = (int) duration.toMinutes();
                    int pct = totalMin > 0 ? Math.min((int) (elapsedMinutes / totalMin * 100), 100) : 100;
                    log.info("    Building: pub={} msg/s, {} msgs, {}MB, {}/{}m ({}%)",
                            (long) publishRate, sentMessages, sentBytes / 1_000_000,
                            (int) elapsedMinutes, totalMin, pct);
                }

                // Size-based exit
                if (targetBacklogBytes > 0 && sentBytes >= targetBacklogBytes) {
                    log.info("    Target backlog size reached: {}MB", sentBytes / 1_000_000);
                    break;
                }
            } else {
                // CONCURRENT: verbose logging with progress and correctness status
                double elapsedMinutes = (now - testPointStartNanos) / 1e9 / 60.0;
                int elapsedMin = (int) elapsedMinutes;
                String progressLine;
                if (duration.toNanos() > 0) {
                    int totalMin = (int) duration.toMinutes();
                    int pct = totalMin > 0 ? Math.min((int) (elapsedMinutes / totalMin * 100), 100) : 100;
                    progressLine = String.format("Test point progress: %d/%d minutes, %d%%", elapsedMin, totalMin, pct);
                } else {
                    progressLine = String.format("Test point progress: %d minutes elapsed", elapsedMin);
                }
                progressLine += String.format("\nCampaign progress: test %d/%d (%d%%)",
                        currentTestIndex + 1, totalTestPoints,
                        totalTestPoints > 0 ? (currentTestIndex * 100) / totalTestPoints : 0);

                if (currentOutcome != null) {
                    long backlog = currentOutcome.correctness.totalSent
                            - currentOutcome.correctness.totalReceived;
                    log.info("\n── Correctness ────────────────────────────────────────────\n"
                            + progressLine + "\n"
                            + String.format("  Pub: %,7d msg/s   Cons: %,7d msg/s%n", (long) publishRate, (long) consumeRate)
                            + String.format("  Sent: %,d   Received: %,d   Backlog: %,d   Dup: %,d   OOO: %,d   CRC: %,d%n",
                                    currentOutcome.correctness.totalSent,
                                    currentOutcome.correctness.totalReceived,
                                    backlog,
                                    currentOutcome.correctness.duplicateCount,
                                    currentOutcome.correctness.outOfOrderCount,
                                    currentOutcome.correctness.crcFailureCount));
                } else {
                    log.info("  Pub: {} msg/s  Cons: {} msg/s", (long) publishRate, (long) consumeRate);
                }
            }

            if (now >= endTime) break;
            oldTime = now;
        }
        throwOnStopRequested(stopRequested);

        return agg;
    }

    /**
     * CONCURRENT mode: producers and consumers run together for the test duration,
     * then producers pause and consumers drain in-flight messages during the grace period.
     */
    private CumulativeLatencies runConcurrentPhases(ProducerWorkAssignment workAssignment,
                                                    TestPoint point,
                                                    TestResult.PerformanceResults perf,
                                                    CorrectnessTimeSeriesRecorder correctnessSeries)
            throws IOException {
        // Allow consumer group assignment to stabilize before starting the workload
        // to avoid false duplicates from rebalancing
        log.info("Waiting 10s for consumer group assignment to stabilize...");
        Sleeper.sleepOrStop(stopRequested, 10_000, true);

        worker.startLoad(workAssignment);
        log.info("----- Correctness test [CONCURRENT] ({}m) -----", config.testDurationMinutes);
        worker.resetStats();
        CumulativeLatencies agg;
        consumerRestartController.openWindow();
        try {
            agg = collectStats(CorrectnessConfig.TestMode.CONCURRENT,
                    Duration.ofMinutes(config.testDurationMinutes), 0, 0, perf,
                    correctnessSeries);
        } finally {
            // Closing on the same monitor as applyModification waits for an
            // accepted restart before producers pause and grace handling starts.
            consumerRestartController.closeWindow();
        }

        worker.pauseProducers();
        log.info("----- Grace period ({}s) — waiting for in-flight messages -----",
                config.gracePeriodSeconds);
        waitForDrain(config.gracePeriodSeconds, point.messageSize(), correctnessSeries);

        return agg;
    }

    /**
     * DRAIN mode: producers build a backlog (consumers not yet created),
     * then producers pause and consumers are created to drain it.
     * The grace period is the maximum time allowed for consumers to drain.
     */
    private CumulativeLatencies runBacklogBuildDrainPhases(ProducerWorkAssignment workAssignment,
                                                           TestPoint point,
                                                           TestResult.PerformanceResults perf,
                                                           CorrectnessTimeSeriesRecorder correctnessSeries)
            throws IOException {
        // Enable per-partition/per-consumer metrics before producers start sending,
        // so build-phase sends populate the per-partition counters for Prometheus.
        if (workload.engineConfig.perPartitionMetrics) {
            worker.enablePerPartitionMetrics();
        }
        if (workload.engineConfig.perConsumerMetrics) {
            worker.enablePerConsumerMetrics();
        }

        worker.startLoad(workAssignment);

        // Determine backlog build strategy: size-based or time-based
        long targetBacklogBytes = config.backlogSizeGB > 0
                ? (long) (config.backlogSizeGB * 1_000_000_000.0) : 0;
        Duration buildDuration = targetBacklogBytes > 0
                ? Duration.ZERO : Duration.ofMinutes(config.testDurationMinutes);

        if (targetBacklogBytes > 0) {
            log.info("----- Producing backlog [DRAIN] (target: {} GB) -----", config.backlogSizeGB);
        } else {
            log.info("----- Producing backlog [DRAIN] ({}m) -----", config.testDurationMinutes);
        }

        long buildStart = System.currentTimeMillis();
        worker.resetStats();
        CumulativeLatencies agg = collectStats(CorrectnessConfig.TestMode.DRAIN,
                buildDuration, targetBacklogBytes, point.messageSize(), perf,
                correctnessSeries);

        worker.pauseProducers();
        CountersStats buildCounters = worker.getCountersStats();
        long backlogMessages = buildCounters.messagesSent();
        long backlogSizeBytes = backlogMessages * point.messageSize();
        long buildDurationMs = System.currentTimeMillis() - buildStart;
        log.info("    Backlog built: {} messages, {}MB in {}s",
                backlogMessages, backlogSizeBytes / 1_000_000, buildDurationMs / 1000);

        // === DRAIN PHASE ===
        log.info("----- Draining backlog (producers paused) -----");

        // Create consumers now that backlog is built — they start consuming immediately.
        // The distribution was already planned upfront in runSingleTest.
        log.info("Creating consumers. Initial consumer rebalancing will likely create duplicates.");
        createConsumers();
        worker.setMaxConsumerRecreations(workload.engineConfig.maxConsumerRecreations);
        waitForClientsReady("CONSUMER", 60);

        waitForDrain(config.gracePeriodSeconds, point.messageSize(), correctnessSeries);

        return agg;
    }

    /**
     * Wait for consumers to drain remaining messages.
     *
     * @param timeoutSeconds maximum time to wait
     * @param messageSize    message size in bytes for backlog MB calculation;
     *                       when > 0 uses compact tabular format (DRAIN mode),
     *                       when 0 uses simple format (CONCURRENT grace period)
     */
    private void waitForDrain(int timeoutSeconds, int messageSize,
                              CorrectnessTimeSeriesRecorder correctnessSeries)
            throws IOException {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(timeoutSeconds));
        long drainStart = System.currentTimeMillis();
        Instant lastLossCheck = Instant.now().minusSeconds(20);

        // Broker lag tracking — only used in tabular mode (messageSize > 0).
        // Lag is queried via the admin API which is relatively expensive,
        // so it's refreshed every ~30s rather than every sample interval.
        Set<String> groupNames = null;
        boolean isShareGroup = false;
        long lagRefreshIntervalMs = 30_000;
        long lastLagRefresh = 0;
        long brokerLag = -1; // -1 means not yet fetched

        // Print tabular header and lag description for DRAIN mode
        if (messageSize > 0) {
            groupNames = plannedTopology.consumers().stream()
                    .map(vc -> vc.consumerGroup())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            isShareGroup = plannedTopology.consumerType() == ConsumerType.SHARE_GROUP;

            String lagDescription = isShareGroup
                    ? "broker-reported share group lag (LogEndOffset - StartOffset)"
                    : "broker-reported consumer group lag (LogEndOffset - CommittedOffset)";
            log.info("    'lag' = {}, refreshed every {}s via admin API", lagDescription, lagRefreshIntervalMs / 1000);
            log.info(String.format("    %12s %14s %14s %12s %10s %14s",
                    "cons msg/s", "total sent", "total recv", "backlog", "backlog MB", "lag"));
        }

        long oldTime = System.nanoTime();
        int stuckBacklogCtr = 0;
        long lastReceived = 0;
        long lastBacklog = 0;
        while (Instant.now().isBefore(deadline) && !stopRequested.get()) {
            Sleeper.sleepOrStop(stopRequested, config.sampleRateMillis, true);
            worker.getPeriodStats(); // ensures metrics endpoint of each worker is updated for scraping
            CountersStats counters = worker.getCountersStats();
            CorrectnessResult.TestOutcome currentOutcome = correctnessSeries.record(
                    worker.getCorrectnessResult());
            long received = counters.messagesReceived();
            long backlog = counters.messagesSent() - counters.messagesReceived();
            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;
            double rate = lastReceived == 0d ? 0 : (received - lastReceived) / elapsed;

            if (messageSize > 0) {
                // Refresh broker lag periodically
                long nowMs = System.currentTimeMillis();
                if (nowMs - lastLagRefresh >= lagRefreshIntervalMs) {
                    long totalLag = 0;
                    for (String group : groupNames) {
                        Map<TopicPartition, Long> lagByPartition = isShareGroup
                                ? cluster.getShareGroupLag(group)
                                : cluster.getConsumerGroupLag(group);
                        totalLag += lagByPartition.values().stream().mapToLong(Long::longValue).sum();
                    }
                    brokerLag = totalLag;
                    lastLagRefresh = nowMs;
                }

                // Compact tabular format with lag column
                long backlogBytes = backlog * messageSize;
                String lagStr = brokerLag >= 0 ? String.format("%,14d", brokerLag) : String.format("%14s", "---");
                log.info(String.format("    %,12d %,14d %,14d %,12d %,10d %s",
                        (long) rate, counters.messagesSent(), counters.messagesReceived(),
                        backlog, backlogBytes / 1_000_000, lagStr));
            } else {
                // Simple format for CONCURRENT grace period
                log.info("  Draining: backlog: {} (Sent: {}, Received: {}, Rate: {})",
                        backlog, counters.messagesSent(), counters.messagesReceived(), (int) rate);
            }

            if (backlog == lastBacklog)
                stuckBacklogCtr++;
            else
                stuckBacklogCtr = 0;

            lastBacklog = backlog;
            lastReceived = received;

            if ((backlog <=0 || stuckBacklogCtr >= 3)
                    && Duration.between(lastLossCheck, Instant.now()).toSeconds() >= 10) {
                // fallback to check the correctness tester which is not based on counters.
                lastLossCheck = Instant.now();
                if (currentOutcome != null && currentOutcome.correctness.lostMessages == 0) {
                    long drainDurationMs = System.currentTimeMillis() - drainStart;
                    log.info("    Backlog drained in {}s — no loss detected", drainDurationMs / 1000);
                    return;
                } else if (currentOutcome != null) {
                    log.info("  Fallen back to correctness checker for drain status - {} messages provisionally lost",
                            currentOutcome.correctness.lostMessages);
                }
            }

            oldTime = now;
        }
        throwOnStopRequested(stopRequested);
        log.warn("  Drain timed out after {}s", timeoutSeconds);
    }

    private void populateWorkloadConfig(TestResult.WorkloadConfig wc, TestPoint point) {
        wc.topics = point.topics();
        wc.partitionsPerTopic = point.partitionsPerTopic();
        wc.messageSize = point.messageSize();
        wc.setDataset(point.dataset());
        wc.messageDistributor = point.messageDistributor().name();
        wc.producersPerTopic = point.producersPerTopic();
        wc.producerRate = point.producerRate();
        wc.consumerGroupsPerTopic = point.consumerGroupsPerTopic();
        wc.consumersPerGroup = point.consumersPerGroup();
        wc.testDurationMinutes = config.testDurationMinutes;
        wc.warmupDurationMinutes = 0;
        wc.sampleRateMillis = config.sampleRateMillis;
    }

    private void recordPeriodResults(TestResult.PerformanceResults r, PeriodStats stats,
                                     double publishRate, double consumeRate) {
        r.sent.add(stats.messagesSent);
        r.consumed.add(stats.messagesReceived);
        r.publishFailed.add(stats.errors);
        r.consumeFailed.add(stats.pollErrors);

        r.publishRate.add(publishRate);
        r.consumeRate.add(consumeRate);
        r.addByteRates(stats, publishRate, consumeRate);

        r.publishLatencyAvg.add(microsToMillis(stats.publishLatency.getMean()));
        r.publishLatencyMin.add(microsToMillis(stats.publishLatency.getMinValue()));
        r.publishLatency50pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(50)));
        r.publishLatency75pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(75)));
        r.publishLatency95pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(95)));
        r.publishLatency99pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(99)));
        r.publishLatency999pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(99.9)));
        r.publishLatency9999pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(99.99)));
        r.publishLatencyMax.add(microsToMillis(stats.publishLatency.getMaxValue()));

        r.scheduleLatencyMin.add(microsToMillis(stats.scheduleLatency.getMinValue()));
        r.scheduleLatency50pct.add(microsToMillis(stats.scheduleLatency.getValueAtPercentile(50)));
        r.scheduleLatency75pct.add(microsToMillis(stats.scheduleLatency.getValueAtPercentile(75)));
        r.scheduleLatency99pct.add(microsToMillis(stats.scheduleLatency.getValueAtPercentile(99)));
        r.scheduleLatencyMax.add(microsToMillis(stats.scheduleLatency.getMaxValue()));

        r.publishDelayLatencyAvg.add(microsToMillis(stats.publishDelayLatency.getMean()));
        r.publishDelayLatency50pct.add(microsToMillis(stats.publishDelayLatency.getValueAtPercentile(50)));
        r.publishDelayLatency75pct.add(microsToMillis(stats.publishDelayLatency.getValueAtPercentile(75)));
        r.publishDelayLatency95pct.add(microsToMillis(stats.publishDelayLatency.getValueAtPercentile(95)));
        r.publishDelayLatency99pct.add(microsToMillis(stats.publishDelayLatency.getValueAtPercentile(99)));
        r.publishDelayLatency999pct.add(microsToMillis(stats.publishDelayLatency.getValueAtPercentile(99.9)));
        r.publishDelayLatency9999pct.add(microsToMillis(stats.publishDelayLatency.getValueAtPercentile(99.99)));
        r.publishDelayLatencyMax.add(microsToMillis(stats.publishDelayLatency.getMaxValue()));

        r.endToEndLatencyAvg.add(microsToMillis(stats.endToEndLatency.getMean()));
        r.endToEndLatencyMin.add(microsToMillis(stats.endToEndLatency.getMinValue()));
        r.endToEndLatency50pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(50)));
        r.endToEndLatency75pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(75)));
        r.endToEndLatency95pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(95)));
        r.endToEndLatency99pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99)));
        r.endToEndLatency999pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99.9)));
        r.endToEndLatency9999pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99.99)));
        r.endToEndLatencyMax.add(microsToMillis(stats.endToEndLatency.getMaxValue()));

        r.endToEndDeliveryLatencyAvg.add(microsToMillis(stats.endToEndDeliveryLatency.getMean()));
        r.endToEndDeliveryLatencyMin.add(microsToMillis(stats.endToEndDeliveryLatency.getMinValue()));
        r.endToEndDeliveryLatency50pct.add(microsToMillis(stats.endToEndDeliveryLatency.getValueAtPercentile(50)));
        r.endToEndDeliveryLatency75pct.add(microsToMillis(stats.endToEndDeliveryLatency.getValueAtPercentile(75)));
        r.endToEndDeliveryLatency95pct.add(microsToMillis(stats.endToEndDeliveryLatency.getValueAtPercentile(95)));
        r.endToEndDeliveryLatency99pct.add(microsToMillis(stats.endToEndDeliveryLatency.getValueAtPercentile(99)));
        r.endToEndDeliveryLatency999pct.add(microsToMillis(stats.endToEndDeliveryLatency.getValueAtPercentile(99.9)));
        r.endToEndDeliveryLatency9999pct.add(microsToMillis(stats.endToEndDeliveryLatency.getValueAtPercentile(99.99)));
        r.endToEndDeliveryLatencyMax.add(microsToMillis(stats.endToEndDeliveryLatency.getMaxValue()));
    }

    private void recordAggregateResults(TestResult.PerformanceResults r, CumulativeLatencies agg) {
        log.info("\n══ Aggregated Results ═════════════════════════════════════\n"
                + formatAggLatencyHeader()
                + formatAggLatencyRow("Publish Latency (ms)",
                        microsToMillis(agg.publishLatency.getMean()),
                        microsToMillis(agg.publishLatency.getValueAtPercentile(50)),
                        microsToMillis(agg.publishLatency.getValueAtPercentile(95)),
                        microsToMillis(agg.publishLatency.getValueAtPercentile(99)),
                        microsToMillis(agg.publishLatency.getValueAtPercentile(99.9)),
                        microsToMillis(agg.publishLatency.getValueAtPercentile(99.99)),
                        microsToMillis(agg.publishLatency.getMaxValue()))
                + formatAggLatencyRow("E2E Latency (ms)",
                        microsToMillis(agg.endToEndLatency.getMean()),
                        microsToMillis(agg.endToEndLatency.getValueAtPercentile(50)),
                        microsToMillis(agg.endToEndLatency.getValueAtPercentile(95)),
                        microsToMillis(agg.endToEndLatency.getValueAtPercentile(99)),
                        microsToMillis(agg.endToEndLatency.getValueAtPercentile(99.9)),
                        microsToMillis(agg.endToEndLatency.getValueAtPercentile(99.99)),
                        microsToMillis(agg.endToEndLatency.getMaxValue()))
                + formatAggLatencyRow("Pub Delay (ms)",
                        microsToMillis(agg.publishDelayLatency.getMean()),
                        microsToMillis(agg.publishDelayLatency.getValueAtPercentile(50)),
                        microsToMillis(agg.publishDelayLatency.getValueAtPercentile(95)),
                        microsToMillis(agg.publishDelayLatency.getValueAtPercentile(99)),
                        microsToMillis(agg.publishDelayLatency.getValueAtPercentile(99.9)),
                        microsToMillis(agg.publishDelayLatency.getValueAtPercentile(99.99)),
                        microsToMillis(agg.publishDelayLatency.getMaxValue())));

        r.aggregatedPublishLatencyAvg = microsToMillis(agg.publishLatency.getMean());
        r.aggregatedPublishLatency50pct = microsToMillis(agg.publishLatency.getValueAtPercentile(50));
        r.aggregatedPublishLatency75pct = microsToMillis(agg.publishLatency.getValueAtPercentile(75));
        r.aggregatedPublishLatency95pct = microsToMillis(agg.publishLatency.getValueAtPercentile(95));
        r.aggregatedPublishLatency99pct = microsToMillis(agg.publishLatency.getValueAtPercentile(99));
        r.aggregatedPublishLatency999pct = microsToMillis(agg.publishLatency.getValueAtPercentile(99.9));
        r.aggregatedPublishLatency9999pct = microsToMillis(agg.publishLatency.getValueAtPercentile(99.99));
        r.aggregatedPublishLatencyMax = microsToMillis(agg.publishLatency.getMaxValue());

        r.aggregatedEndToEndLatencyAvg = microsToMillis(agg.endToEndLatency.getMean());
        r.aggregatedEndToEndLatency50pct = microsToMillis(agg.endToEndLatency.getValueAtPercentile(50));
        r.aggregatedEndToEndLatency75pct = microsToMillis(agg.endToEndLatency.getValueAtPercentile(75));
        r.aggregatedEndToEndLatency95pct = microsToMillis(agg.endToEndLatency.getValueAtPercentile(95));
        r.aggregatedEndToEndLatency99pct = microsToMillis(agg.endToEndLatency.getValueAtPercentile(99));
        r.aggregatedEndToEndLatency999pct = microsToMillis(agg.endToEndLatency.getValueAtPercentile(99.9));
        r.aggregatedEndToEndLatency9999pct = microsToMillis(agg.endToEndLatency.getValueAtPercentile(99.99));
        r.aggregatedEndToEndLatencyMax = microsToMillis(agg.endToEndLatency.getMaxValue());

        r.aggregatedEndToEndDeliveryLatencyAvg = microsToMillis(agg.endToEndDeliveryLatency.getMean());
        r.aggregatedEndToEndDeliveryLatency50pct = microsToMillis(agg.endToEndDeliveryLatency.getValueAtPercentile(50));
        r.aggregatedEndToEndDeliveryLatency75pct = microsToMillis(agg.endToEndDeliveryLatency.getValueAtPercentile(75));
        r.aggregatedEndToEndDeliveryLatency95pct = microsToMillis(agg.endToEndDeliveryLatency.getValueAtPercentile(95));
        r.aggregatedEndToEndDeliveryLatency99pct = microsToMillis(agg.endToEndDeliveryLatency.getValueAtPercentile(99));
        r.aggregatedEndToEndDeliveryLatency999pct = microsToMillis(agg.endToEndDeliveryLatency.getValueAtPercentile(99.9));
        r.aggregatedEndToEndDeliveryLatency9999pct = microsToMillis(agg.endToEndDeliveryLatency.getValueAtPercentile(99.99));
        r.aggregatedEndToEndDeliveryLatencyMax = microsToMillis(agg.endToEndDeliveryLatency.getMaxValue());

        r.aggregatedPublishDelayLatencyAvg = microsToMillis(agg.publishDelayLatency.getMean());
        r.aggregatedPublishDelayLatency50pct = microsToMillis(agg.publishDelayLatency.getValueAtPercentile(50));
        r.aggregatedPublishDelayLatency75pct = microsToMillis(agg.publishDelayLatency.getValueAtPercentile(75));
        r.aggregatedPublishDelayLatency95pct = microsToMillis(agg.publishDelayLatency.getValueAtPercentile(95));
        r.aggregatedPublishDelayLatency99pct = microsToMillis(agg.publishDelayLatency.getValueAtPercentile(99));
        r.aggregatedPublishDelayLatency999pct = microsToMillis(agg.publishDelayLatency.getValueAtPercentile(99.9));
        r.aggregatedPublishDelayLatency9999pct = microsToMillis(agg.publishDelayLatency.getValueAtPercentile(99.99));
        r.aggregatedPublishDelayLatencyMax = microsToMillis(agg.publishDelayLatency.getMaxValue());

        agg.publishLatency.percentiles(100).forEach(value -> {
            r.aggregatedPublishLatencyQuantiles.put(value.getPercentile(),
                    microsToMillis(value.getValueIteratedTo()));
        });

        agg.scheduleLatency.percentiles(100).forEach(value -> {
            r.aggregatedScheduleLatencyQuantiles.put(value.getPercentile(),
                    microsToMillis(value.getValueIteratedTo()));
        });

        agg.publishDelayLatency.percentiles(100).forEach(value -> {
            r.aggregatedPublishDelayLatencyQuantiles.put(value.getPercentile(),
                    microsToMillis(value.getValueIteratedTo()));
        });

        agg.endToEndLatency.percentiles(100).forEach(value -> {
            r.aggregatedEndToEndLatencyQuantiles.put(value.getPercentile(),
                    microsToMillis(value.getValueIteratedTo()));
        });

        agg.endToEndDeliveryLatency.percentiles(100).forEach(value -> {
            r.aggregatedEndToEndDeliveryLatencyQuantiles.put(value.getPercentile(),
                    microsToMillis(value.getValueIteratedTo()));
        });
    }

    private static String formatAggLatencyHeader() {
        return String.format("  %-22s %10s %10s %10s %10s %10s %10s %10s%n",
                "", "avg", "p50", "p95", "p99", "p99.9", "p99.99", "max");
    }

    private static String formatAggLatencyRow(String label, double avg, double p50, double p95,
                                               double p99, double p999, double p9999, double max) {
        return String.format("  %-22s %10.2f %10.2f %10.2f %10.2f %10.2f %10.2f %10.2f%n",
                label, avg, p50, p95, p99, p999, p9999, max);
    }

    private static double microsToMillis(double microTime) {
        return microTime / 1000;
    }

    private static double microsToMillis(long microTime) {
        return microTime / 1000.0;
    }

    @Override
    public void close() throws Exception {
        consumerRestartController.closeWindow();
        try {
            worker.stopAll();
            worker.awaitStopComplete(java.time.Duration.ofMinutes(5));
        } catch (Exception e) {
            log.error("Unable to stop workload - {}", e.toString());
        }
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
    }

    /** Push effective {@code engineConfig.printMessages} to the worker once.
     *  Disabled if any test point has producer rate > 50 msg/s, to avoid log
     *  flooding and self-inflicted slowdown. */
    private void applyPrintMessages(List<TestPoint> allPoints) {
        boolean configured = workload.engineConfig.printingRequested();
        boolean effective = configured
                && allPoints.stream().allMatch(p -> p.producerRate() <= 50);
        if (configured && !effective) {
            log.warn("engineConfig.printMessages/printContents disabled: at least one test " +
                     "point has producerRate > 50 msg/s");
        }
        worker.setPrintMessages(effective);
        worker.setPrintContents(effective && workload.engineConfig.printContents);
    }
}
