package dev.dimster.engines.drainbacklog;

import dev.dimster.BaseEngine;
import dev.dimster.BaseWorkload;
import dev.dimster.utils.Sleeper;
import dev.dimster.BenchmarkRunner;
import dev.dimster.RepeatMerger;
import dev.dimster.ScenarioExpander;
import dev.dimster.ScenarioExpander.TestPoint;
import dev.dimster.TestResult;
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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;

/**
 * Orchestrates drain-backlog tests: builds a backlog with no consumers running,
 * then measures the time to drain it, with optional producer load.
 *
 * With {@code drainBacklogConfig.reuseBacklog}, consecutive test points that
 * differ only in drain-side settings drain the same backlog instead of each
 * building their own (rules in {@link BacklogReuse}). Isolation between those
 * drains (cache eviction, broker restarts) is left to beforeDrain hooks; the
 * engine only waits for the cluster to settle afterwards.
 */
public class DrainBacklogEngine extends BaseEngine {

    private static final Logger log = LoggerFactory.getLogger(DrainBacklogEngine.class);

    /** How often the drain phase polls the counters for completion. */
    private static final long DRAIN_CHECK_INTERVAL_MS = 500;
    /** How often the drain phase polls for the first consumed message. */
    private static final long FIRST_RECEIVE_CHECK_INTERVAL_MS = 100;
    /** Backlog (messages) at or below which the drain is considered complete. */
    private static final long DRAINED_THRESHOLD_MSGS = 1000;
    /**
     * Quiet period before consumers start, after the build or reuse check,
     * the beforeDrain hooks and the settle wait. Lets restarted brokers and
     * just-elected leaders finish settling down, so each drain starts from a
     * comparable state. Outside the measured drain, which starts at the first
     * consumed message.
     */
    private static final long PRE_DRAIN_PAUSE_MS = 10_000;

    private final DrainBacklogWorkload workload;
    private final BaseWorkload base;
    private final KafkaConfig fileKafkaConfig;

    /** With reuseBacklog: the backlog on the cluster that a test point may drain again, or null. */
    private ReusableBacklog backlog;

    public DrainBacklogEngine(DrainBacklogWorkload workload, Worker worker, TopologyPlanner topologyPlanner,
                              KafkaConfig fileKafkaConfig, AtomicBoolean stopRequested) {
        super(worker, topologyPlanner, stopRequested);
        this.workload = workload;
        this.base = workload.baseWorkload;
        this.fileKafkaConfig = fileKafkaConfig;
    }

    public DrainBacklogResult run() throws Exception {
        return run(0, null, null);
    }

    public DrainBacklogResult run(int startTestPoint, DrainBacklogResult partialResult,
                                   BenchmarkRunner.CheckpointCallback checkpointCallback) throws Exception {
        DrainBacklogResult result;
        // partial result from checkpoint
        if (partialResult != null) {
            result = partialResult;
        } else {
            result = new DrainBacklogResult();
            result.metadata.name = workload.name;
            result.config = workload;
        }

        List<TestPoint> allPoints = ScenarioExpander.expand(base, workload.scenarios,
                fileKafkaConfig, workload.drainBacklogConfig);
        int totalTests = allPoints.size();

        applyPrintMessages(allPoints);

        // With reuse, each point's build identity decides whether it can drain
        // the previous point's backlog. A resumed run picks the backlog up from
        // the partial result (and re-checks it is intact before using it).
        boolean reuse = workload.drainBacklogConfig.reuseBacklog;
        List<String> identities = null;
        if (reuse) {
            BacklogReuse.validate(allPoints);
            identities = allPoints.stream().map(BacklogReuse::identity).toList();
            backlog = result.reusableBacklog;
        }

        try {
            for (int i = startTestPoint; i < allPoints.size(); i++) {
                if (stopRequested.get()) break;

                TestPoint point = allPoints.get(i);

                DrainBacklogResult.ScenarioResult scenarioResult = findOrCreateScenario(
                        result.scenarioResults,
                        point.scenarioName() != null ? point.scenarioName() : "default",
                        sr -> sr.name, name -> {
                            var sr = new DrainBacklogResult.ScenarioResult();
                            sr.name = name;
                            return sr;
                        });

                log.info("--- Drain-backlog test {}/{}: {} ---", i + 1, totalTests, point.label());
                log.info("Merged workload:\n{}", point.toLogString());

                // Unrecorded, and outside the scenario's time window.
                if (point.drainBacklogConfig().warmup) {
                    warmUp(point, i, totalTests, reuse ? identities.get(i) : null);
                }

                // Stamp the scenario boundary the first time we touch it, then
                // refresh the end after every test so the exporter knows the
                // exact time range this scenario occupies in Prometheus.
                if (scenarioResult.beginTime == null) {
                    scenarioResult.beginTime = currentScenarioTimestamp();
                }
                runHooks(HookPoint.BEFORE_TEST_POINT, point, i, totalTests, List.of());
                DrainBacklogResult.TestOutcome outcome = runSingleTest(point, i, totalTests,
                        reuse ? identities.get(i) : null, false);
                scenarioResult.endTime = currentScenarioTimestamp();
                scenarioResult.tests.add(outcome);

                log.info("    Result: {} {}MB{}, drained in {}s (producer rate during drain: {})",
                        outcome.drain.reusedBacklogFrom != null ? "reused" : "built",
                        outcome.drain.backlogSizeBytes / 1_000_000,
                        outcome.drain.reusedBacklogFrom != null
                                ? " from test point " + (outcome.drain.reusedBacklogFrom + 1)
                                : " in " + outcome.drain.buildDurationMs / 1000 + "s",
                        outcome.drain.drainDurationMs / 1000,
                        outcome.drain.backlogProducerRate <= 0 ? "paused" : outcome.drain.backlogProducerRate + " msg/s");

                // Keep the backlog only if the next point can drain it again.
                Set<String> retain = Set.of();
                if (backlog != null && i + 1 < totalTests && identities.get(i + 1).equals(backlog.identity)) {
                    retain = Set.copyOf(backlog.topics);
                } else {
                    backlog = null;
                }
                result.reusableBacklog = backlog;

                if (checkpointCallback != null) {
                    checkpointCallback.onTestPointCompleted(i, result);
                }

                cooldownBetweenTests(workload.engineConfig.cooldownMinutes, point, i, totalTests, retain);
            }
        } catch (StopRequestedException e) {
            // Surface the stop to BenchmarkRunner so it does not treat the
            // partial result as a completed workload (which would advance the
            // checkpoint and write the final JSON, breaking resume).
            log.warn("Benchmark terminated early due to stop requested");
            worker.stopAll();
            throw e;
        }

        result.reusableBacklog = null;
        RepeatMerger.mergeDrainBacklogRepeats(result);

        return result;
    }

    /**
     * Runs the test point once without recording it, so the scenario's first
     * recorded drain runs on warm worker and broker JVMs (e.g. right after a
     * deploy). No hooks run: they prepare measured drains. With reuse, the
     * backlog it builds is kept for the test point itself, so a warm-up costs
     * one drain rather than a build.
     */
    private void warmUp(TestPoint point, int index, int totalTests, String identity) throws Exception {
        log.info("--- Warm-up for drain-backlog test {}/{}: {} (not recorded) ---", index + 1, totalTests, point.label());
        DrainBacklogResult.TestOutcome outcome = runSingleTest(point, index, totalTests, identity, true);
        log.info("    Warm-up drained in {}s", outcome.drain.drainDurationMs / 1000);
        releaseTestPointResources(backlog != null ? Set.copyOf(backlog.topics) : Set.of());
        log.info("----- WARM-UP DRAIN COMPLETE -----");
    }

    /**
     * @param identity the point's build identity when reuseBacklog is on, else null
     * @param warmUp   an unrecorded warm-up run: no hooks
     */
    private DrainBacklogResult.TestOutcome runSingleTest(TestPoint point, int index, int totalTests,
                                                         String identity, boolean warmUp) throws Exception {
        DrainBacklogConfig dbc = point.drainBacklogConfig();
        String label = warmUp ? point.label() + " (warm-up)" : point.label();

        worker.stopAll();
        worker.awaitStopComplete(java.time.Duration.ofMinutes(10));
        KafkaConfig pointConfig = point.kafkaConfig();
        worker.setKafkaConfig(pointConfig);
        cluster = new KafkaCluster(
                dev.dimster.kafka.KafkaSpi.factory().newAdmin(pointConfig.commonProperties()),
                pointConfig);

        ReusableBacklog reused = identity != null ? reusableBacklogFor(identity) : null;
        if (reused == null) {
            backlog = null;
        }
        Set<String> existing = reused != null ? Set.copyOf(reused.topics) : Set.of();
        cluster.deleteTestTopics(existing);
        cluster.deleteTestGroups();
        cancelLeftoverFlinkJobs(point);

        throwOnStopRequested(stopRequested);

        // Plan first, then create the actual Kafka topics from the planned topic
        // names. A reused backlog's topics already exist; the rest (Flink output
        // topics) are created fresh. Reusable backlogs get unlimited retention.
        planTopology(point.topics(), point.partitionsPerTopic(),
                point.producersPerTopic(), point.consumerGroupsPerTopic(), point.consumersPerGroup(),
                point.consumerType(), point.consumerProcMsMin(), point.consumerProcMsMax(), point.consumerProcParallelism(),
                point.consumerProcMode(), point.consumerProcCapacity(),
                point.messageDistributor(), point.messageDistributorSkew(),
                false /* correctness off */,
                false /* dynamic consumer assignment */,
                outputPartitionsPerTopic(point),
                reused != null ? reused.topics : null);
        recordPlannedTopology(label, workload.engineConfig.printTopology);
        List<String> allTopics = createPlannedTopics(existing,
                identity != null ? BacklogReuse.RETENTION_OVERRIDES : Map.of());
        List<String> backlogTopics = point.flink() != null
                ? new ArrayList<>(plannedTopology.pipelineTopics().keySet())
                : allTopics;
        // Set before producers: consumers, created after the backlog is
        // built, read the same dataset's record format.
        configureDataset(point);

        var perf = new TestResult.PerformanceResults();
        var drainData = new DrainBacklogResult.DrainData();
        double buildRate = dbc.buildRate > 0 ? dbc.buildRate : point.producerRate();
        long buildDuration = 0;
        long backlogMessages;
        long priorRecords; // records already in the backlog before this point's producers sent any

        if (reused != null) {
            enableClientMetrics();
            log.info("----- Reusing the backlog built by test point {} ({}): {} messages -----",
                    reused.builtAtTestPoint + 1, reused.builtAtLabel, reused.records);
            priorRecords = reused.records;
            backlogMessages = point.consumerGroupsPerTopic() * reused.records;
            drainData.reusedBacklogFrom = reused.builtAtTestPoint;
        } else {
            priorRecords = 0;
            backlogMessages = buildBacklog(point, dbc, buildRate, perf, drainData);
            buildDuration = drainData.buildDurationMs;
        }
        long backlogSizeBytes = backlogMessages * point.messageSize();

        // === DRAIN PHASE ===
        // A warm-up's drain is marked as such, ending with WARM-UP DRAIN COMPLETE (see warmUp).
        if (dbc.backlogProducerRate > 0) {
            worker.adjustPublishRate(dbc.backlogProducerRate);
            if (warmUp) {
                log.info("----- WARM-UP DRAIN START (producer rate: {} msg/s) -----", dbc.backlogProducerRate);
            } else {
                log.info("----- Draining backlog (producer rate: {} msg/s) -----", dbc.backlogProducerRate);
            }
        } else {
            if (reused == null) worker.pauseProducers();
            log.info(warmUp ? "----- WARM-UP DRAIN START -----" : "----- Draining backlog (producers paused) -----");
        }

        // Hooks get the cluster to the state each drain should start from
        // (e.g. a cold page cache), then the cluster must settle before
        // consumers start: a hook may have restarted brokers.
        if (!warmUp) {
            runHooks(HookPoint.BEFORE_DRAIN, point, index, totalTests, backlogTopics);
        }
        if (identity != null || hasHooks(HookPoint.BEFORE_DRAIN)) {
            cluster.awaitSettled(allTopics,
                    java.time.Duration.ofMinutes(workload.drainBacklogConfig.settleTimeoutMinutes), stopRequested);
        }

        log.info("Pausing {}s before starting consumers", PRE_DRAIN_PAUSE_MS / 1000);
        Sleeper.sleepOrStop(stopRequested, PRE_DRAIN_PAUSE_MS, true);

        // Create consumers now that backlog is built — topology was already planned upfront
        createConsumers();
        worker.setMaxConsumerRecreations(workload.engineConfig.maxConsumerRecreations);
        waitForClientsReady("CONSUMER", 60);

        long flinkRunningAt = 0;
        if (point.flink() != null) {
            // With a Flink stage the backlog sits on the input topics. The
            // consumers are already waiting on the (empty) output topics, so
            // the drain clock starts when the job runs and covers Flink
            // processing the backlog plus consumers reading its output.
            // Completion counts assume one output row per input row: a
            // filtering query never reaches the expected count and drains
            // until drainTimeoutMinutes.
            log.info("Starting the Flink job to drain the backlog (the drain assumes a non-filtering query)");
            startFlink(point);
            flinkRunningAt = System.currentTimeMillis();
        }

        // Extract unique group names for broker lag reporting
        Set<String> groupNames = plannedTopology.consumers().stream()
                .map(vc -> vc.consumerGroup())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        boolean isShareGroup = point.consumerType() == dev.dimster.ConsumerType.SHARE_GROUP;

        long lagRefreshIntervalMs = dbc.sampleRateMillis;
        long lastLagRefresh = 0;
        long brokerLag = -1; // -1 means not yet fetched

        // Start the drain clock at the first consumed message so group join
        // and initial fetch latency are excluded from the drain duration.
        // With Flink, it starts when the job runs (see above): records only
        // reach the consumers once Flink has processed them, which is part
        // of what the drain measures.
        long drainStart;
        if (flinkRunningAt > 0) {
            drainStart = flinkRunningAt;
        } else {
            long consumersReady = System.currentTimeMillis();
            drainStart = awaitFirstReceive(dbc.drainTimeoutMinutes);
            log.info("    First message received {}ms after consumers ready", drainStart - consumersReady);
        }
        // Restart the sample clock so the first drain sample excludes the join wait.
        long oldTime = resetSampleClock();

        String lagDescription = isShareGroup
                ? "broker-reported share group lag"
                : "broker-reported consumer group lag (LogEndOffset - CommittedOffset)";
        log.info("    'expect recv' = total sent x {} consumer group(s){}", point.consumerGroupsPerTopic(),
                priorRecords > 0 ? " (total sent includes the " + priorRecords + " reused backlog records)" : "");
        log.info("    'lag' = {}, refreshed every {}s via admin API", lagDescription, lagRefreshIntervalMs / 1000);
        log.info(String.format("    %12s %12s %14s %14s %14s %12s %10s %14s",
                "pub msg/s", "cons msg/s", "total sent", "expect recv", "total recv", "backlog", "backlog MB", "lag"));

        long drainEnd = 0;
        while (!stopRequested.get()) {
            // Poll the cumulative counters within the sample period so drain
            // completion is detected promptly without resetting period stats.
            // On completion, end the period early so the final partial sample
            // is still recorded below.
            boolean drained = false;
            long periodEndNanos = System.nanoTime() + dbc.sampleRateMillis * 1_000_000L;
            long remainingMs;
            while ((remainingMs = (periodEndNanos - System.nanoTime()) / 1_000_000) > 0) {
                Sleeper.sleepOrStop(stopRequested, Math.min(DRAIN_CHECK_INTERVAL_MS, remainingMs), true);
                CountersStats counters = worker.getCountersStats();
                long backlog = (priorRecords + counters.messagesSent()) * point.consumerGroupsPerTopic()
                        - counters.messagesReceived();
                if (backlog <= DRAINED_THRESHOLD_MSGS) {
                    drained = true;
                    break;
                }
            }
            // Stamp completion before the sample work (lag refresh is an admin call).
            long periodEndMs = System.currentTimeMillis();

            // Stamp before the call: stats are reset near the start of a
            // (possibly slow) remote call, not when it returns.
            long now = System.nanoTime();
            PeriodStats stats = worker.getPeriodStats();
            double elapsed = (now - oldTime) / 1e9;
            double publishRate = stats.messagesSent / elapsed;
            double consumeRate = stats.messagesReceived / elapsed;
            oldTime = now;

            CountersStats counters = worker.getCountersStats();
            long totalSent = priorRecords + counters.messagesSent();
            long expectedRecv = totalSent * point.consumerGroupsPerTopic();
            long totalRecv = counters.messagesReceived();
            long currentBacklog = Math.max(0, expectedRecv - totalRecv);
            long currentBacklogBytes = currentBacklog * point.messageSize();

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

            String lagStr = brokerLag >= 0 ? String.format("%,14d", brokerLag) : String.format("%14s", "---");
            log.info(String.format("    %,12d %,12d %,14d %,14d %,14d %,12d %,10d %s",
                    (long) publishRate, (long) consumeRate,
                    totalSent, expectedRecv, totalRecv,
                    currentBacklog, currentBacklogBytes / 1_000_000, lagStr));

            recordPeriodResults(perf, stats, publishRate, consumeRate);
            drainData.backlogMessagesOverTime.add(currentBacklog);
            drainData.backlogBytesOverTime.add(currentBacklogBytes);

            // Honour the sub-loop's detection even if producer load has since
            // pushed the backlog back above the threshold.
            if (drained || currentBacklog <= DRAINED_THRESHOLD_MSGS) {
                drainEnd = periodEndMs;
                break;
            }

            if (dbc.drainTimeoutMinutes > 0) {
                long elapsedMs = System.currentTimeMillis() - drainStart;
                if (elapsedMs >= dbc.drainTimeoutMinutes * 60_000L) {
                    log.warn("    Drain timeout reached ({}m), aborting with {} messages remaining",
                            dbc.drainTimeoutMinutes, currentBacklog);
                    break;
                }
            }
        }

        throwOnStopRequested(stopRequested);

        if (drainEnd == 0) drainEnd = System.currentTimeMillis();
        long drainDuration = drainEnd - drainStart;
        log.info("    Backlog drained in {}s", drainDuration / 1000);

        // A freshly built backlog becomes reusable once drained: producers
        // were paused throughout, so the sent count is final.
        if (identity != null && reused == null) {
            backlog = recordBacklog(identity, index, label, backlogTopics);
            // Report what was drained (producers send briefly after the last
            // build sample), so it matches the test points that reuse it.
            backlogMessages = point.consumerGroupsPerTopic() * backlog.records;
            backlogSizeBytes = backlogMessages * point.messageSize();
        }

        // Build outcome
        DrainBacklogResult.TestOutcome outcome = new DrainBacklogResult.TestOutcome();
        outcome.label = point.label();
        populateWorkloadConfig(outcome.workloadConfig, point);
        drainData.backlogSizeGB = dbc.backlogSizeGB;
        drainData.backlogSizeBytes = backlogSizeBytes;
        drainData.backlogMessages = backlogMessages;
        drainData.buildRate = (int) buildRate;
        drainData.buildDurationMs = buildDuration;
        drainData.drainStartTime = java.time.Instant.ofEpochMilli(drainStart).toString();
        drainData.drainEndTime = java.time.Instant.ofEpochMilli(drainEnd).toString();
        drainData.drainDurationMs = drainDuration;
        drainData.drainTimeoutMinutes = dbc.drainTimeoutMinutes;
        drainData.backlogProducerRate = dbc.backlogProducerRate;
        drainData.sampleRateMillis = dbc.sampleRateMillis;
        outcome.drain = drainData;
        outcome.performance = perf;
        outcome.flink = flinkResult(point);

        return outcome;
    }

    /**
     * Produces at the build rate until the backlog reaches backlogSizeGB,
     * recording build samples into {@code perf} and {@code drainData}.
     * Returns the backlog in messages (summed across consumer groups);
     * producers keep running until the caller pauses them.
     */
    private long buildBacklog(TestPoint point, DrainBacklogConfig dbc, double buildRate,
                              TestResult.PerformanceResults perf, DrainBacklogResult.DrainData drainData)
            throws IOException {
        createProducers();
        waitForClientsReady("PRODUCER", 60);

        // Enable per-partition/per-consumer metrics before producers start sending,
        // so build-phase sends populate the per-partition counters for Prometheus.
        enableClientMetrics();

        log.info("Starting producers at build rate: {} msg/s", buildRate);
        worker.startLoad(new ProducerWorkAssignment(
                buildRate, point.messageDistributor(),
                point.partitionsPerTopic(), point.messageDistributorSkew()));

        long oldTime = resetSampleClock();

        throwOnStopRequested(stopRequested);
        log.info("----- Building backlog (target: {} GB) -----", dbc.backlogSizeGB);
        long buildStart = System.currentTimeMillis();

        long requestedBacklogSize = (long) (dbc.backlogSizeGB * 1_000_000_000.0);
        long backlogMessages = 0;

        while (!stopRequested.get()) {
            Sleeper.sleepOrStop(stopRequested, dbc.sampleRateMillis, true);

            // Stamp before the call: stats are reset near the start of a
            // (possibly slow) remote call, not when it returns.
            long now = System.nanoTime();
            PeriodStats stats = worker.getPeriodStats();
            double elapsed = (now - oldTime) / 1e9;
            double publishRate = stats.messagesSent / elapsed;
            double consumeRate = stats.messagesReceived / elapsed;
            oldTime = now;

            CountersStats counters = worker.getCountersStats();
            backlogMessages = Math.max(0,
                    point.consumerGroupsPerTopic() * counters.messagesSent() - counters.messagesReceived());
            long currentBacklogBytes = backlogMessages * point.messageSize();

            long pct = requestedBacklogSize > 0 ? (currentBacklogBytes * 100) / requestedBacklogSize : 100;
            log.info("    Building: pub={} msg/s, {} msgs, {}MB / {}MB ({}%)",
                    (long) publishRate, backlogMessages, currentBacklogBytes / 1_000_000,
                    requestedBacklogSize / 1_000_000, pct);

            recordPeriodResults(perf, stats, publishRate, consumeRate);
            drainData.backlogMessagesOverTime.add(backlogMessages);
            drainData.backlogBytesOverTime.add(currentBacklogBytes);

            if (currentBacklogBytes >= requestedBacklogSize) {
                break;
            }
        }

        throwOnStopRequested(stopRequested);

        drainData.buildDurationMs = System.currentTimeMillis() - buildStart;
        log.info("    Backlog built: {} messages, {}MB in {}s",
                backlogMessages, backlogMessages * point.messageSize() / 1_000_000,
                drainData.buildDurationMs / 1000);
        return backlogMessages;
    }

    private void enableClientMetrics() throws IOException {
        if (workload.engineConfig.perPartitionMetrics) {
            worker.enablePerPartitionMetrics();
        }
        if (workload.engineConfig.perConsumerMetrics) {
            worker.enablePerConsumerMetrics();
        }
    }

    /**
     * The backlog on the cluster if this point can drain it again: built for
     * the same identity and still intact. Otherwise null, logging why, and
     * the point builds its own.
     */
    private ReusableBacklog reusableBacklogFor(String identity) {
        if (backlog == null) return null;
        if (!backlog.identity.equals(identity)) {
            log.info("Build settings differ from the backlog built by test point {}: building a new backlog",
                    backlog.builtAtTestPoint + 1);
            return null;
        }
        Set<String> existingTopics = cluster.listTopicNames();
        Optional<String> problem = existingTopics.containsAll(backlog.topics)
                ? BacklogReuse.whyNotIntact(backlog, existingTopics,
                        cluster.startOffsets(backlog.topics), cluster.endOffsets(backlog.topics))
                : BacklogReuse.whyNotIntact(backlog, existingTopics, Map.of(), Map.of());
        if (problem.isPresent()) {
            log.warn("Cannot reuse the backlog built by test point {} ({}): building a new backlog",
                    backlog.builtAtTestPoint + 1, problem.get());
            return null;
        }
        return backlog;
    }

    /** Captures a just-built backlog so following test points can drain it again. */
    private ReusableBacklog recordBacklog(String identity, int index, String label, List<String> topics)
            throws IOException {
        var b = new ReusableBacklog();
        b.identity = identity;
        b.builtAtTestPoint = index;
        b.builtAtLabel = label;
        b.topics = new ArrayList<>(topics);
        b.records = worker.getCountersStats().messagesSent();
        long offsetTotal = 0;
        for (var e : cluster.endOffsets(topics).entrySet()) {
            b.endOffsets.put(e.getKey().toString(), e.getValue());
            offsetTotal += e.getValue();
        }
        // Equal unless producers are transactional (commit markers take offsets).
        log.info("    Backlog kept for reuse: {} records sent, end offsets total {}", b.records, offsetTotal);
        return b;
    }

    /** Discards period stats and returns the new sample start time. Unlike
     *  {@code startMeasurementWindow()}, stamps before the call, matching the
     *  sampling loops. */
    private long resetSampleClock() throws IOException {
        long now = System.nanoTime();
        worker.getPeriodStats();
        return now;
    }

    /** Polls the cumulative counters until consumers receive their first
     *  message and returns that time. Gives up after the drain timeout (if
     *  set), returning the time of giving up so the drain loop can time out. */
    private long awaitFirstReceive(int drainTimeoutMinutes) throws IOException {
        long baselineRecv = worker.getCountersStats().messagesReceived();
        long deadline = drainTimeoutMinutes > 0
                ? System.currentTimeMillis() + drainTimeoutMinutes * 60_000L
                : Long.MAX_VALUE;
        while (System.currentTimeMillis() < deadline) {
            Sleeper.sleepOrStop(stopRequested, FIRST_RECEIVE_CHECK_INTERVAL_MS, true);
            if (worker.getCountersStats().messagesReceived() > baselineRecv) {
                break;
            }
        }
        return System.currentTimeMillis();
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
    }



    private void populateWorkloadConfig(TestResult.WorkloadConfig wc, TestPoint point) {
        wc.topics = point.topics();
        wc.partitionsPerTopic = point.partitionsPerTopic();
        wc.messageSize = point.messageSize();
        wc.setDataset(point.dataset());
        wc.setFlink(point.flink());
        wc.messageDistributor = point.messageDistributor().name();
        wc.producersPerTopic = point.producersPerTopic();
        wc.producerRate = point.producerRate();
        wc.consumerGroupsPerTopic = point.consumerGroupsPerTopic();
        wc.consumersPerGroup = point.consumersPerGroup();
        wc.sampleRateMillis = point.drainBacklogConfig().sampleRateMillis;
    }

    @Override
    public void close() throws Exception {
        stopFlink();
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

    private static double microsToMillis(double microTime) {
        return microTime / 1000;
    }

    private static double microsToMillis(long microTime) {
        return microTime / 1000.0;
    }

    /** Push effective {@code engineConfig.printMessages} to the worker once.
     *  For drain-backlog the effective rate is the max of base producerRate,
     *  buildRate (build phase) and backlogProducerRate (drain phase). If any
     *  of those exceeds 50 msg/s on any test point, printing is disabled. */
    private void applyPrintMessages(List<TestPoint> allPoints) {
        boolean configured = workload.engineConfig.printingRequested();
        boolean effective = configured && allPoints.stream().allMatch(p -> {
            double max = p.producerRate();
            if (p.drainBacklogConfig() != null) {
                max = Math.max(max, p.drainBacklogConfig().buildRate);
                max = Math.max(max, p.drainBacklogConfig().backlogProducerRate);
            }
            return max <= 50;
        });
        if (configured && !effective) {
            log.warn("engineConfig.printMessages/printContents disabled: at least one test " +
                     "point has producer rate > 50 msg/s (build/drain/base)");
        }
        worker.setPrintMessages(effective);
        worker.setPrintContents(effective && workload.engineConfig.printContents);
    }
}
