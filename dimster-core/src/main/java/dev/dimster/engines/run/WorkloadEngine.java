package dev.dimster.engines.run;

import dev.dimster.BaseEngine;
import dev.dimster.BaseWorkload;
import dev.dimster.utils.Sleeper;
import dev.dimster.BenchmarkRunner;
import dev.dimster.RepeatMerger;
import dev.dimster.ScenarioExpander;
import dev.dimster.ScenarioExpander.TestPoint;
import dev.dimster.TestResult;
import dev.dimster.coordinator.ModifyRequest;
import dev.dimster.coordinator.ModifyResult;
import dev.dimster.coordinator.ModificationTarget;
import dev.dimster.kafka.KafkaCluster;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.kafka.KafkaConsumerWrapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.dimster.utils.StopRequestedException;
import dev.dimster.worker.commands.*;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.dimster.stats.StatsCollector;
import dev.dimster.availability.AvailabilityResult;
import dev.dimster.availability.ProbeAssignment;
import dev.dimster.utils.PaddingDecimalFormat;
import dev.dimster.worker.Worker;
import dev.dimster.worker.topology.TopologyPlanner;
import dev.dimster.worker.topology.VirtualTopology;

public class WorkloadEngine extends BaseEngine implements ModificationTarget {

    /**
     * Overall run progress context — how far through the entire multi-test run we are.
     * @param totalRunMinutes total minutes across all test points (warmup + test each)
     * @param completedMinutesBefore minutes completed by prior test points
     * @param testPointIndex 0-based index of current test point across all scenarios
     * @param totalTestPoints total test points across all scenarios
     */
    public record RunProgress(int totalRunMinutes, int completedMinutesBefore,
                               int testPointIndex, int totalTestPoints) {}

    private final Workload workload;
    private final BaseWorkload base;
    private final KafkaConfig fileKafkaConfig;

    // Per-test-point mutable state
    private TestPoint testPoint;
    private RunProgress runProgress;
    private List<String> topics;

    // private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private volatile double targetPublishRate;

    /** Start time of the current test point (set when run() begins). */
    private long testPointStartNanos;
    /** Shared wall-clock start time of the active producer-rate schedule. */
    private long scheduleStartEpochMillis;

    // Set true while a test point is actively driving load (warmup + test
    // phase). Read by applyModification() to gate the side-channel /modify
    // endpoint: modifications outside this window have nothing to act on
    // (producers/consumers are stopped or being torn down). Written only
    // from the engine thread inside runSingleTest(); read from the
    // coordinator's HTTP handler thread, hence volatile.
    private volatile boolean testPointActive;

    // Modifications applied during the current test point. Appended to
    // under applyModification's synchronized lock; drained under the same
    // lock by consumeModifications() at the end of runSingleTest. Cleared
    // at the start of each test point as defence-in-depth in case some
    // exception path skipped the drain.
    private final List<RunResult.Modification> currentModifications = new ArrayList<>();

    // Set true by /modify --terminate to end the current test point early
    // (record aggregates, advance to next test point). Read in collectStats
    // alongside the existing stopRequested check. Reset at the start of
    // every test point so it never bleeds across them. Volatile because
    // the engine thread reads it on every sample tick while the HTTP
    // handler thread sets it from inside applyModification.
    private volatile boolean terminateRequested;

    // True only while collectStats is running with collectAgg=true (the
    // test phase). Used by applyModification to gate /modify
    // --discard-stats and --run-for-minutes — both are test-phase-only
    // operations. Set/cleared inside collectStats in a try/finally.
    private volatile boolean testPhaseActive;

    // True only while the ramp-up phase is running. Used by applyModification
    // to reject /modify during ramp because the ramp logic itself is mutating
    // the publish rate, and we don't want a user-issued rate change to fight
    // the next ramp tick. The exception is `--skip-phase`, which is the one
    // action accepted during ramp. Set/cleared in a try/finally around the
    // ramp loop.
    private volatile boolean rampActive;

    // Set true by /modify --skip-phase. Consumed by collectStatsInner's
    // sample tick (breaks the loop without recording aggregates) and by the
    // ramp for-loop (breaks out of the remaining intervals). Cleared after
    // the ramp loop and before the test phase so a skip fired during one
    // phase doesn't leak into the next phase. Rejected during the test
    // phase (use --terminate to end the test point early).
    private volatile boolean skipPhaseRequested;

    // Set true by /modify --discard-stats. Consumed once on the next
    // collectStats sample tick: per-period stats list and cumulative
    // histograms are wiped and worker recorders are reset. Cleared on
    // consumption. Independent of runForMinutesRequested — either flag can
    // be set alone or both can be set together.
    private volatile boolean discardStatsRequested;

    // Pending /modify --run-for-minutes value. Consumed on the next
    // collectStats sample tick: the test phase end time is reset to N
    // minutes from that moment. Cleared on consumption. Independent of
    // discardStatsRequested — either flag can be set alone or both can be
    // set together. Null means no pending change.
    private volatile Integer runForMinutesRequested;

    // After /modify --run-for-minutes resets the test phase timeline, the
    // values used by the periodic progress display (logPeriodStats) need
    // to reflect the new totals — otherwise users see stale "elapsed/X
    // minutes" against the original yaml-defined duration. Both default
    // to 0 ("no override, use the derived value") and are reset to 0 at
    // the start of each test point. Only updated when run-for-minutes
    // fires; discard-stats alone leaves the timeline untouched.
    private volatile int testPointMinutesOverride;
    private volatile int totalRunMinutesOverride;

    public WorkloadEngine(Workload workload, Worker worker, TopologyPlanner topologyPlanner,
                          KafkaConfig fileKafkaConfig, AtomicBoolean stopRequested) {
        super(worker, topologyPlanner, stopRequested);
        this.workload = workload;
        this.base = workload.baseWorkload;
        this.fileKafkaConfig = fileKafkaConfig;
    }

    public RunResult run() throws Exception {
        return run(0, null, null);
    }

    public RunResult run(int startTestPoint, RunResult partialResult,
                         BenchmarkRunner.CheckpointCallback checkpointCallback) throws Exception {
        RunResult result;
        // partial result from checkpoint
        if (partialResult != null) {
            result = partialResult;
        } else {
            result = new RunResult();
            result.metadata.name = workload.name;
            result.runConfig = workload.runConfig;
            result.availabilityConfig = workload.availabilityConfig;
        }

        // Pass workload.runConfig as runDefaults so the expander can resolve any
        // per-scenario `runConfig:` overrides into per-test-point RunConfig snapshots.
        // Each TestPoint then carries its own RunConfig (never null in run mode).
        RunConfig runDefaults = workload.runConfig != null ? workload.runConfig : new RunConfig();
        List<TestPoint> allPoints = ScenarioExpander.expand(base, workload.scenarios, fileKafkaConfig,
                null, null, runDefaults);
        int totalTests = allPoints.size();

        log.info("Workload '{}' has {} test point(s), starting from {}", workload.name,
                totalTests, startTestPoint);

        applyPrintMessages(allPoints);

        // Sum per-test-point durations — when scenarios sweep testDurationMinutes /
        // warmupDurationMinutes / ramp settings, each point may contribute a
        // different amount of wall-clock time.
        int totalRunMinutes = 0;
        for (TestPoint p : allPoints) {
            totalRunMinutes += testPointTotalMinutes(p.runConfig());
        }

        try {
            for (int i = startTestPoint; i < allPoints.size(); i++) {
                throwOnStopRequested(stopRequested);

                TestPoint point = allPoints.get(i);

                RunResult.ScenarioResult scenarioResult = findOrCreateScenario(
                        result.scenarioResults,
                        point.scenarioName() != null ? point.scenarioName() : "default",
                        sr -> sr.name, name -> {
                            var sr = new RunResult.ScenarioResult();
                            sr.name = name;
                            return sr;
                        });

                if (totalTests > 1) {
                    log.info("──────── Test point {}/{}: {} ────────", i + 1, totalTests, point.label());
                }
                log.info("Merged workload:\n{}", point.toLogString());

                // Sum durations of points already finished (each may have its own
                // ramp/warmup/test durations when scenarios sweep them).
                int completedMinutes = 0;
                for (int j = 0; j < i; j++) {
                    completedMinutes += testPointTotalMinutes(allPoints.get(j).runConfig());
                }
                this.testPoint = point;
                this.runProgress = new RunProgress(totalRunMinutes, completedMinutes, i, totalTests);

                // Stamp the scenario boundary the first time we touch it, then
                // refresh the end after every test so the exporter knows the
                // exact time range this scenario occupies in Prometheus.
                if (scenarioResult.beginTime == null) {
                    scenarioResult.beginTime = currentScenarioTimestamp();
                }
                RunResult.TestEntry entry = runSingleTest(point);
                scenarioResult.endTime = currentScenarioTimestamp();
                scenarioResult.tests.add(entry);

                if (checkpointCallback != null) {
                    checkpointCallback.onTestPointCompleted(i, result);
                }

                cooldownBetweenTests(workload.engineConfig.cooldownMinutes, i, totalTests);
            }
        } catch (StopRequestedException e) {
            // Surface the stop to BenchmarkRunner so it does not treat the
            // partial result as a completed workload (which would advance the
            // checkpoint and write the final JSON, breaking resume).
            log.warn("Benchmark terminated early due to stop requested");
            worker.stopAll();
            throw e;
        }

        RepeatMerger.mergeRunRepeats(result);

        return result;
    }

    private RunResult.TestEntry runSingleTest(TestPoint point) throws Exception {
        worker.stopAll();
        worker.awaitStopComplete(Duration.ofMinutes(10));
        log.info("Stopped any running workers");

        KafkaConfig pointConfig = point.kafkaConfig();
        worker.setKafkaConfig(pointConfig);
        log.info("Workers configured with Kafka config");

        cluster = new KafkaCluster(
                dev.dimster.kafka.KafkaSpi.factory().newAdmin(pointConfig.commonProperties()),
                pointConfig);

        cluster.deleteTestTopics();
        cluster.deleteTestGroups();

        // Plan the virtual topology first — it owns topic and consumer-group naming —
        // then create the actual Kafka topics from the planned names.
        planTopology(point.topics(), point.partitionsPerTopic(),
                point.producersPerTopic(), point.consumerGroupsPerTopic(), point.consumersPerGroup(),
                point.consumerType(), point.consumerProcMsMin(), point.consumerProcMsMax(), point.consumerProcParallelism(),
                point.consumerProcMode(), point.consumerProcCapacity(),
                point.messageDistributor(), point.messageDistributorSkew(),
                false /* correctness off in WorkloadEngine */,
                false /* dynamic consumer assignment outside correctness mode */);
        recordPlannedTopology(point.label(), workload.engineConfig.printTopology);
        topics = createPlannedTopics();

        if (workload.availabilityConfig != null) {
            cluster.createTopic(KafkaCluster.AVAIL_PROBE_TOPIC, workload.availabilityConfig.probePartitions);
            log.info("Created availability probe topic ({} partitions)", workload.availabilityConfig.probePartitions);
            worker.enableAvailability(workload.availabilityConfig);
            log.info("Availability probes enabled (threshold={}ms, timeout={}s)",
                    workload.availabilityConfig.unavailabilityThresholdMs,
                    workload.availabilityConfig.probeTimeoutSeconds);
        }

        if (workload.engineConfig.perPartitionMetrics) {
            worker.enablePerPartitionMetrics();
            log.info("Per-partition metrics enabled");
        }
        if (workload.engineConfig.perConsumerMetrics) {
            worker.enablePerConsumerMetrics();
            log.info("Per-consumer metrics enabled");
        }

        configureDataset(point);
        createConsumers();
        setConsumerRecreation();
        createProducers();
        ensureReady(topics, point.consumerGroupsPerTopic(), 60);

        log.info("--------------- WORKLOAD : {} --- CONFIG : {} ---------------",
                workload.name, pointConfig.name != null ? pointConfig.name : "embedded");

        // --- benchmark execution ---
        testPointStartNanos = System.nanoTime();

        targetPublishRate = effectiveTargetRate(point);
        // Defence-in-depth: drain any leftover modifications so a previous
        // test point that exited via an unexpected path can't bleed entries
        // into this one. Normal flow drains them via consumeModifications()
        // below, leaving the list empty here.
        synchronized (this) {
            currentModifications.clear();
            terminateRequested = false;
            discardStatsRequested = false;
            runForMinutesRequested = null;
            skipPhaseRequested = false;
        }
        testPointMinutesOverride = 0;
        totalRunMinutesOverride = 0;

        // Per-test-point runConfig. When the workload's `scenarios:` block sweeps
        // any runConfig field (e.g. rampIntervals: [3, 5, 10]), each test point
        // carries its own RunConfig snapshot. Defaults to the workload-level config
        // for points with no scenario override. Always non-null in run mode.
        RunConfig pointRunConfig = point.runConfig();

        // Ramp-up is opt-in. Fixed-rate workloads ramp toward targetPublishRate.
        // Scheduled workloads ramp toward the first scheduled rate, then start
        // the schedule from elapsed zero after the ramp completes.
        double rampTargetRate = point.producerRateSchedule() == null
                ? targetPublishRate
                : point.producerRateSchedule().firstRate();
        boolean rampEnabled = pointRunConfig.rampIntervals > 0
                && pointRunConfig.rampIntervalSeconds > 0
                && rampTargetRate > 0.0;
        double initialRate = rampEnabled
                ? rampTargetRate / pointRunConfig.rampIntervals
                : rampTargetRate;
        boolean scheduledWorkload = point.producerRateSchedule() != null;

        ProducerWorkAssignment producerWorkAssignment = new ProducerWorkAssignment(
                initialRate, null, point.messageDistributor(),
                point.partitionsPerTopic(), point.messageDistributorSkew());

        worker.startLoad(producerWorkAssignment);

        TestResult testResult;
        // testPointActive is the gate for /modify — set it true once load is
        // running and clear it in finally so an exception during warmup,
        // probes, or test never leaves the flag stuck on (which would let
        // /modify try to act on torn-down workers).
        testPointActive = true;
        try {
            // maybe do progressive ramp-up -----------
            // Step the publish rate up in equal increments before warm-up
            // begins, holding each step for rampIntervalSeconds. Using
            // collectStats with collectAgg=false keeps per-sample logs
            // flowing during the ramp (so the operator can see producers
            // keeping up) while ensuring no stats are aggregated into
            // the recorded results — same treatment as warm-up.
            if (rampEnabled) {
                rampActive = true;
                try {
                    log.info("----- Starting ramp-up ({} intervals × {}s, target {} msg/s) ------",
                            pointRunConfig.rampIntervals, pointRunConfig.rampIntervalSeconds, rampTargetRate);
                    for (int i = 1; i <= pointRunConfig.rampIntervals; i++) {
                        throwOnStopRequested(stopRequested);
                        double stepRate = rampTargetRate * i / pointRunConfig.rampIntervals;
                        // i == 1 is already in effect via startLoad's initialRate.
                        if (i > 1) {
                            worker.adjustPublishRate(stepRate);
                        }
                        log.info("Ramp interval {}/{}: target {} msg/s",
                                i, pointRunConfig.rampIntervals, stepRate);
                        collectStats(Duration.ofSeconds(pointRunConfig.rampIntervalSeconds), false);
                        // collectStats returned early if --skip-phase fired.
                        // Break the ramp for-loop too so we stop stepping
                        // the rate up and proceed straight to warm-up at
                        // the full target rate (set below).
                        if (skipPhaseRequested) {
                            log.info("Skip-phase: ending ramp-up early");
                            break;
                        }
                        // --terminate during ramp also short-circuits the
                        // remaining intervals — no point stepping the rate
                        // up when the test point is about to end. Warm-up
                        // and the test phase will both see the flag and
                        // break immediately too.
                        if (terminateRequested) {
                            log.info("Terminate: ending ramp-up early");
                            break;
                        }
                    }
                } finally {
                    rampActive = false;
                }
                // The ramp drives a temporary fixed-rate limiter. Hold the
                // first scheduled rate through warm-up; scheduled workloads
                // start their variable timeline only when the measured test
                // phase begins.
                worker.adjustPublishRate(scheduledWorkload ? rampTargetRate : targetPublishRate);
                // Don't let a ramp-time skip-phase bleed into warm-up.
                synchronized (this) { skipPhaseRequested = false; }
            }

            // maybe do warm-up period -----------
            if (pointRunConfig.warmupDurationMinutes > 0) {
                throwOnStopRequested(stopRequested);

                log.info("----- Starting warm-up traffic ({}m) ------", pointRunConfig.warmupDurationMinutes);
                collectStats(Duration.ofMinutes(pointRunConfig.warmupDurationMinutes), false);
            }
            // Defensive clear before the test phase begins (handles a
            // warm-up-time --skip-phase). No-op if the flag wasn't set.
            synchronized (this) { skipPhaseRequested = false; }

            if (scheduledWorkload) {
                long scheduleStartEpochMillis = System.currentTimeMillis();
                worker.adjustPublishRateSchedule(point.producerRateSchedule(), scheduleStartEpochMillis);
                this.scheduleStartEpochMillis = scheduleStartEpochMillis;
            }

            worker.resetStats();

            // if availability tracking is configured, start it up -----------
            if (workload.availabilityConfig != null) {
                List<ProbeAssignment> assignments = new ArrayList<>();
                for (int p = 0; p < workload.availabilityConfig.probePartitions; p++) {
                    assignments.add(new ProbeAssignment(KafkaCluster.AVAIL_PROBE_TOPIC, p));
                }
                worker.startAvailabilityProbes(assignments);
                log.info("Availability probes started ({} partitions, 1 probe/partition/sec)",
                        workload.availabilityConfig.probePartitions);
            }

            // start the recorded benchmark  -----------
            log.info("----- Starting benchmark traffic ({}m) ------", pointRunConfig.testDurationMinutes);

            testResult = collectStats(Duration.ofMinutes(pointRunConfig.testDurationMinutes), true);
        } finally {
            testPointActive = false;
        }

        // stop producing as the test point is over
        worker.pauseProducers();

        // benchmark completed, gather and record metrics -----------
        if (workload.engineConfig.perPartitionMetrics) {
            PerPartitionStats perPartitionStats = worker.getPerPartitionStats();
            if (perPartitionStats != null) {
                recordPerPartitionResults(testResult, perPartitionStats);
            }
        }

        if (workload.availabilityConfig != null) {
            AvailabilityResult.TestOutcome outcome = worker.getAvailabilityResult();
            if (outcome != null) {
                testResult.availability = outcome.availability;
                var a = outcome.availability;
                log.info("\n══ Availability Results ({} partitions, {}s) ══════════════\n"
                        + formatAvailabilityTable(a),
                        workload.availabilityConfig.probePartitions, a.wallClockMs / 1000);
            }
        }

        RunResult.TestEntry entry = new RunResult.TestEntry();
        entry.label = point.label();
        entry.kafkaConfig = testResult.kafkaConfig;
        entry.workloadConfig = testResult.workloadConfig;
        entry.results = testResult.results;
        entry.availability = testResult.availability;
        entry.modifications = consumeModifications();
        return entry;
    }

    @Override
    public void close() throws Exception {
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
        executor.shutdownNow();
    }

    /**
     * Apply a side-channel modification to the in-progress test point.
     *
     * Three distinct kinds of modifications flow through here:
     *  1. Engine-internal action flags (terminate / discardStats /
     *     runForMinutes) — these flip a flag on the engine that
     *     {@link #collectStats} consumes on its next sample tick. They
     *     don't touch the workers.
     *  2. Explicit client restart actions — these select concrete producer
     *     or consumer entries from the current topology and recreate them on
     *     their owning workers without changing the topology.
     *  3. Workload / Kafka-config field changes (producerRate, batch.size,
     *     producersPerTopic, ...) — these go through {@link
     *     TestPointMutator} which builds a diff and {@link DiffApplier}
     *     which executes it against the workers. The mutator+applier pair
     *     is the centralized analogue of {@code TopologyPlanner} for
     *     in-flight changes.
     *
     * The two kinds are mutually exclusive: an action flag means "no
     * field change"; a field change means "no action flag".
     *
     * Synchronized to serialize concurrent /modify calls — modifications
     * are rare and we don't want two of them racing through the worker
     * RPC fan-out.
     */
    public synchronized ModifyResult applyModification(ModifyRequest req) {
        if (req == null) {
            return ModifyResult.rejected("empty modify request");
        }
        if (!testPointActive) {
            return ModifyResult.rejected("no test point is active (modifications are " +
                    "only accepted while a test point is in progress)");
        }
        boolean skipPhase = Boolean.TRUE.equals(req.skipPhase());
        boolean terminate = Boolean.TRUE.equals(req.terminate());
        // Ramp-up rejects everything except --skip-phase and --terminate.
        // The ramp itself is mutating the publish rate at each interval, so
        // anything that races with the next ramp tick is unsafe — but
        // --skip-phase ends the ramp cleanly to start warm-up, and
        // --terminate ends the whole test point, neither of which races.
        if (rampActive && !skipPhase && !terminate) {
            return ModifyResult.rejected("ramp-up phase is in progress; only --skip-phase " +
                    "and --terminate are accepted during ramp-up " +
                    "(other modifications are accepted once warm-up begins)");
        }
        boolean discardStats = Boolean.TRUE.equals(req.discardStats());
        Integer runForMinutes = req.runForMinutes();
        Integer restartConsumersCount = req.restartConsumersCount();
        Integer restartProducersCount = req.restartProducersCount();
        boolean hasWorkloadOverrides = req.workloadOverrides() != null
                && !req.workloadOverrides().isEmpty();
        boolean hasKafkaConfigOverrides = req.kafkaConfigOverrides() != null
                && !req.kafkaConfigOverrides().isEmpty();
        boolean hasFieldOverrides = hasWorkloadOverrides || hasKafkaConfigOverrides;

        // Cross-kind validation up front.
        if (!hasFieldOverrides && !terminate && !discardStats && !skipPhase && runForMinutes == null
                && restartConsumersCount == null && restartProducersCount == null) {
            return ModifyResult.rejected("at least one workload/kafka override or " +
                    "action flag (terminate, discardStats, runForMinutes, skipPhase, " +
                    "restartConsumersCount, restartProducersCount) must be supplied");
        }
        if (terminate && (hasFieldOverrides || discardStats || skipPhase || runForMinutes != null)) {
            return ModifyResult.rejected("terminate must be sent alone " +
                    "(no other modify fields)");
        }
        if (skipPhase && (hasFieldOverrides || discardStats || runForMinutes != null)) {
            // terminate+skipPhase is already caught by the terminate rule above.
            return ModifyResult.rejected("skipPhase must be sent alone " +
                    "(no other modify fields)");
        }
        if (discardStats && hasFieldOverrides) {
            return ModifyResult.rejected("discardStats must be sent alone " +
                    "(use a separate --modify call to change throughput first)");
        }
        if (runForMinutes != null && runForMinutes <= 0) {
            return ModifyResult.rejected("runForMinutes must be > 0, got " + runForMinutes);
        }
        if (discardStats && !testPhaseActive) {
            return ModifyResult.rejected("discardStats is only valid during the test " +
                    "phase (warmup is in progress or no test point is running)");
        }
        if (runForMinutes != null && !testPhaseActive && !hasFieldOverrides) {
            return ModifyResult.rejected("runForMinutes is only valid during the test " +
                    "phase (warmup is in progress or no test point is running)");
        }
        if (skipPhase && testPhaseActive) {
            return ModifyResult.rejected("skipPhase has no effect during the test phase; " +
                    "use --terminate to end the current test point early");
        }
        if (restartConsumersCount != null && restartProducersCount != null) {
            return ModifyResult.rejected("restartConsumersCount and restartProducersCount " +
                    "must be sent separately");
        }
        Integer restartCount = restartConsumersCount != null
                ? restartConsumersCount : restartProducersCount;
        String restartField = restartConsumersCount != null
                ? "restartConsumersCount" : "restartProducersCount";
        if (restartCount != null) {
            if (restartCount < 0) {
                return ModifyResult.rejected(restartField + " must be >= 0, got "
                        + restartCount);
            }
            if (hasFieldOverrides || discardStats || terminate || skipPhase || runForMinutes != null) {
                return ModifyResult.rejected(restartField + " must be sent alone " +
                        "(no other modify fields)");
            }
            if (!testPhaseActive) {
                return ModifyResult.rejected(restartField + " is only valid during the test " +
                        "phase (warmup is in progress or no test point is running)");
            }
        }

        // Build the per-call modifications log entry; populated as we go.
        RunResult.Modification logEntry = new RunResult.Modification();
        logEntry.timestamp = currentScenarioTimestamp();
        logEntry.workloadOverrides = hasWorkloadOverrides ? req.workloadOverrides() : null;
        logEntry.kafkaConfigOverrides = hasKafkaConfigOverrides ? req.kafkaConfigOverrides() : null;
        logEntry.terminate = terminate ? Boolean.TRUE : null;
        logEntry.skipPhase = skipPhase ? Boolean.TRUE : null;
        logEntry.discardStats = discardStats ? Boolean.TRUE : null;
        logEntry.runForMinutes = runForMinutes;
        StringBuilder summary = new StringBuilder("MODIFY applied:");

        // ─── Engine-internal action flags ──────────────────────────────
        // These flip volatile flags that collectStats consumes on its
        // next sample tick. No worker action.
        if (terminate) {
            terminateRequested = true;
            summary.append(" terminate=true");
            currentModifications.add(logEntry);
            log.info(summary.toString());
            return ModifyResult.ok(summary.toString());
        }
        if (skipPhase) {
            skipPhaseRequested = true;
            summary.append(" skipPhase=true");
            currentModifications.add(logEntry);
            log.info(summary.toString());
            return ModifyResult.ok(summary.toString());
        }
        if (restartConsumersCount != null) {
            TestPointMutator.MutationPlan plan;
            try {
                plan = new TestPointMutator(topologyPlanner,
                        size -> ScenarioExpander.payloadDataset(base, size))
                        .planConsumerRestart(testPoint, plannedTopology, restartConsumersCount);
            } catch (IllegalArgumentException e) {
                log.warn("MODIFY rejected: {}", e.getMessage());
                return ModifyResult.rejected(e.getMessage());
            }

            // The plan contains concrete topology entries. Capture their
            // stable names before dispatch so both successful and partially
            // failed distributed restarts remain auditable.
            List<String> restartedNames = plan.actions().isEmpty()
                    ? List.of()
                    : ((DiffAction.RestartConsumers) plan.actions().get(0))
                            .consumers().consumers().stream()
                            .map(dev.dimster.worker.topology.VirtualConsumer::name)
                            .toList();
            log.info("MODIFY selected consumers for restart: {}", restartedNames);

            DiffApplier.Result applyResult = new DiffApplier(worker).apply(plan);
            logEntry.actions = applyResult.appliedActions().isEmpty()
                    ? null
                    : new java.util.ArrayList<>(applyResult.appliedActions());

            String countDesc = restartConsumersCount >= plannedTopology.consumers().size()
                    ? "all " + plannedTopology.consumers().size()
                    : String.valueOf(restartConsumersCount);
            summary.append(" restartConsumersCount=").append(countDesc);

            logEntry.restartConsumersCount = restartConsumersCount;
            logEntry.restartedConsumerNames = restartedNames;
            currentModifications.add(logEntry);
            log.info(summary.toString());
            return applyResult.modifyResult();
        }
        if (restartProducersCount != null) {
            TestPointMutator.MutationPlan plan;
            try {
                plan = new TestPointMutator(topologyPlanner,
                        size -> ScenarioExpander.payloadDataset(base, size))
                        .planProducerRestart(testPoint, plannedTopology, restartProducersCount);
            } catch (IllegalArgumentException e) {
                log.warn("MODIFY rejected: {}", e.getMessage());
                return ModifyResult.rejected(e.getMessage());
            }

            List<String> restartedNames = plan.actions().isEmpty()
                    ? List.of()
                    : ((DiffAction.RestartProducers) plan.actions().get(0))
                            .producers().stream()
                            .map(dev.dimster.worker.topology.VirtualProducer::name)
                            .toList();
            log.info("MODIFY selected producers for restart: {}", restartedNames);

            DiffApplier.Result applyResult = new DiffApplier(worker).apply(plan);
            logEntry.actions = applyResult.appliedActions().isEmpty()
                    ? null
                    : new java.util.ArrayList<>(applyResult.appliedActions());

            String countDesc = restartProducersCount >= plannedTopology.producers().size()
                    ? "all " + plannedTopology.producers().size()
                    : String.valueOf(restartProducersCount);
            summary.append(" restartProducersCount=").append(countDesc);

            logEntry.restartProducersCount = restartProducersCount;
            logEntry.restartedProducerNames = restartedNames;
            currentModifications.add(logEntry);
            log.info(summary.toString());
            return applyResult.modifyResult();
        }
        if (discardStats) {
            discardStatsRequested = true;
            summary.append(" discardStats=true");
        }
        if (runForMinutes != null) {
            runForMinutesRequested = runForMinutes;
            summary.append(" runForMinutes=").append(runForMinutes);
        }

        // ─── Workload / Kafka-config field overrides ───────────────────
        // Routed through TestPointMutator + DiffApplier. The mutator
        // computes the new TestPoint + topology + ordered action list;
        // the applier runs each action and tells us which succeeded.
        if (hasFieldOverrides) {
            TestPointMutator.MutationPlan plan;
            try {
                plan = new TestPointMutator(topologyPlanner,
                        // Builds the payload dataset at the requested
                        // messageSize, exactly as at test-point start, so
                        // BaseWorkload's payload settings (randomBytesRatio,
                        // payloadFile, ...) hold across the live change.
                        size -> ScenarioExpander.payloadDataset(base, size))
                        .plan(testPoint, plannedTopology, req);
            } catch (IllegalArgumentException e) {
                log.warn("MODIFY rejected: {}", e.getMessage());
                return ModifyResult.rejected(e.getMessage());
            }

            // Snapshot the pre-modify state for potential recovery. These
            // references stay stable as long as we don't reassign the
            // engine's testPoint / plannedTopology fields below — which
            // we only do on full success.
            TestPoint preModifyTestPoint = this.testPoint;
            VirtualTopology preModifyTopology = this.plannedTopology;

            DiffApplier.Result applyResult = new DiffApplier(worker).apply(plan);
            logEntry.actions = applyResult.appliedActions().isEmpty()
                    ? null
                    : new java.util.ArrayList<>(applyResult.appliedActions());

            if (applyResult.modifyResult().accepted()) {
                // Full success — commit the new state and we're done.
                currentModifications.add(logEntry);
                this.testPoint = plan.newTestPoint();
                this.plannedTopology = plan.newTopology();
                this.targetPublishRate = effectiveTargetRate(plan.newTestPoint());
                long scheduleEpoch = scheduleEpochFromActions(plan.actions());
                if (scheduleEpoch > 0L) {
                    this.scheduleStartEpochMillis = scheduleEpoch;
                }
                return applyResult.modifyResult();
            }

            // Partial failure (some actions applied, then one failed) or
            // first action failed. In both cases the worker may now be
            // out of sync with the pre-modify state, so we issue a force
            // reset that recreates the workers from the previous
            // TestPoint + topology. Engine state stays at the pre-modify
            // view (we never reassigned testPoint / plannedTopology).
            String recoveryNote = forceResetToPreviousState(
                    preModifyTestPoint, preModifyTopology);
            // Append a recovery entry to the log so the result JSON
            // reflects what we did to undo the failed modify.
            if (logEntry.actions == null) {
                logEntry.actions = new java.util.ArrayList<>();
            }
            logEntry.actions.add(recoveryNote);
            currentModifications.add(logEntry);

            return ModifyResult.rejected(applyResult.modifyResult().message()
                    + " — " + recoveryNote);
        }

        // No field overrides — only action flags fired. Record the entry
        // (covers discardStats / runForMinutes which we deferred above).
        currentModifications.add(logEntry);
        log.info(summary.toString());
        return ModifyResult.ok(summary.toString());
    }

    /**
     * Drain modifications recorded for the just-finished test point.
     * Called once at the tail of runSingleTest to attach the list to the
     * TestEntry. Returns null when nothing was modified so the field
     * disappears from the JSON entirely (NON_NULL on TestEntry.modifications).
     *
     * Synchronized on the same monitor as applyModification so we never
     * snapshot a half-appended list.
     */
    private synchronized List<RunResult.Modification> consumeModifications() {
        if (currentModifications.isEmpty()) return null;
        List<RunResult.Modification> snapshot = new ArrayList<>(currentModifications);
        currentModifications.clear();
        return snapshot;
    }

    /**
     * Recovery path for /modify partial failures: stop everything on the
     * workers, push the previous Kafka config, and recreate the producers
     * and consumers from the previous topology. Engine state stays at the
     * pre-modify view (caller never reassigned testPoint /
     * plannedTopology), so workers re-converge on what the engine
     * already believes.
     *
     * Note: stopAll resets accumulated stats on the workers, so the
     * sample-tick stats immediately after recovery will dip — this is
     * the legitimate cost of unwinding a bad change. The test phase
     * clock keeps running, so the modifications log captures the
     * recovery and the timeline reflects the disruption.
     */
    private String forceResetToPreviousState(TestPoint prev, VirtualTopology prevTopology) {
        log.warn("MODIFY recovery: force-reset to previous state " +
                "(pre-modify TestPoint and topology)");
        try {
            worker.stopAll();
            worker.awaitStopComplete(Duration.ofMinutes(5));
            worker.setKafkaConfig(prev.kafkaConfig());
            // The engine's plannedTopology field equals prevTopology
            // here (we never reassigned it on partial failure), so the
            // existing createConsumers/createProducers helpers re-create
            // from the pre-modify topology. We pass prev fields
            // explicitly for clarity rather than relying on the field.
            var consumerSet = new dev.dimster.worker.topology.ConsumerSet(
                    prevTopology.consumers(),
                    prev.consumerType(),
                    prev.consumerProcMsMin(),
                    prev.consumerProcMsMax(),
                    prev.consumerProcParallelism(),
                    prev.consumerProcMode(),
                    prev.consumerProcCapacity());
            configureDataset(prev);
            worker.createConsumers(consumerSet);
            setConsumerRecreation();
            worker.createProducers(prevTopology.producers());
            ProducerWorkAssignment assignment = new ProducerWorkAssignment(
                    effectiveTargetRate(prev), prev.producerRateSchedule(),
                    prev.producerRateSchedule() == null ? null : System.currentTimeMillis(),
                    prev.messageDistributor(),
                    prev.partitionsPerTopic(), prev.messageDistributorSkew());
            worker.startLoad(assignment);
            this.targetPublishRate = effectiveTargetRate(prev);
            log.info("MODIFY recovery complete");
            return "force-reset to previous state succeeded";
        } catch (Exception e) {
            log.error("MODIFY recovery FAILED — workers may be in " +
                    "inconsistent state; consider `dimster stop` and restart", e);
            return "force-reset FAILED: " + e.getMessage();
        }
    }

    private void setConsumerRecreation() {
        worker.setMaxConsumerRecreations(workload.engineConfig.maxConsumerRecreations);
        log.info("Set max consumer recreations to {}", workload.engineConfig.maxConsumerRecreations);
    }

    private static double effectiveTargetRate(TestPoint point) {
        return point.producerRateSchedule() == null
                ? point.producerRate()
                : point.producerRateSchedule().maxConfiguredRate();
    }

    private static long scheduleEpochFromActions(List<DiffAction> actions) {
        for (DiffAction action : actions) {
            if (action instanceof DiffAction.AdjustPublishRateSchedule adjust) {
                return adjust.scheduleStartEpochMillis();
            }
            if (action instanceof DiffAction.StartLoad start
                    && start.assignment().scheduleStartEpochMillis() != null) {
                return start.assignment().scheduleStartEpochMillis();
            }
        }
        return 0L;
    }

    /** Push effective {@code engineConfig.printMessages} to the worker once.
     *  Disabled if any test point has producer rate > 50 msg/s, to avoid log
     *  flooding and self-inflicted slowdown. */
    private void applyPrintMessages(List<TestPoint> allPoints) {
        boolean configured = workload.engineConfig.printingRequested();
        boolean effective = configured
                && allPoints.stream().allMatch(p -> effectiveTargetRate(p) <= 50);
        if (configured && !effective) {
            log.warn("engineConfig.printMessages/printContents disabled: at least one test " +
                     "point has producerRate > 50 msg/s");
        }
        worker.setPrintMessages(effective);
        worker.setPrintContents(effective && workload.engineConfig.printContents);
    }

    private TestResult collectStats(Duration testDuration, boolean collectAgg) throws IOException {
        // collectAgg distinguishes the test phase (true) from warmup (false).
        // testPhaseActive is the gate /modify --discard-stats checks against,
        // so it's only true during the test phase.
        if (collectAgg) testPhaseActive = true;
        try {
            return collectStatsInner(testDuration, collectAgg);
        } finally {
            if (collectAgg) testPhaseActive = false;
        }
    }

    private TestResult collectStatsInner(Duration testDuration, boolean collectAgg) throws IOException {
        long startTime = startMeasurementWindow();
        long oldTime = startTime;
        long testEndTime = testDuration.toNanos() > 0 ? startTime + testDuration.toNanos() : Long.MAX_VALUE;

        TestResult result = new TestResult();

        // Metadata
        result.metadata.workload = workload.name;
        result.metadata.kafkaConfigName = testPoint.kafkaConfig().name;
        result.metadata.label = testPoint.label();
        result.metadata.scenarioName = testPoint.scenarioName();
        result.metadata.dimensionIndex = testPoint.dimensionIndex();
        result.metadata.totalDimensions = testPoint.totalDimensions();

        // Kafka config summary
        KafkaConfig pointConfig = testPoint.kafkaConfig();
        result.kafkaConfig.replicationFactor = pointConfig.replicationFactor;
        result.kafkaConfig.topicConfig = pointConfig.topicConfig;
        result.kafkaConfig.producerConfig = pointConfig.producerConfig;
        result.kafkaConfig.classicConsumerConfig = pointConfig.classicConsumerConfig;
        result.kafkaConfig.shareConsumerConfig = pointConfig.shareConsumerConfig;

        // Workload config
        result.workloadConfig.topics = testPoint.topics();
        result.workloadConfig.partitionsPerTopic = testPoint.partitionsPerTopic();
        result.workloadConfig.messageSize = testPoint.messageSize();
        result.workloadConfig.setDataset(testPoint.dataset());
        result.workloadConfig.messageDistributor = testPoint.messageDistributor().name();
        result.workloadConfig.messageDistributorSkew = testPoint.messageDistributorSkew();
        result.workloadConfig.consumerType = testPoint.consumerType().name();
        result.workloadConfig.producersPerTopic = testPoint.producersPerTopic();
        result.workloadConfig.producerRate = testPoint.producerRate();
        result.workloadConfig.producerRateSchedule = testPoint.producerRateSchedule();
        result.workloadConfig.consumerGroupsPerTopic = testPoint.consumerGroupsPerTopic();
        result.workloadConfig.consumersPerGroup = testPoint.consumersPerGroup();
        result.workloadConfig.consumerProcMsMin = testPoint.consumerProcMsMin();
        result.workloadConfig.consumerProcMsMax = testPoint.consumerProcMsMax();
        // Per-test-point runConfig — picks up any scenarios `runConfig:` sweep.
        RunConfig effectiveRunConfig = testPoint.runConfig();
        result.workloadConfig.testDurationMinutes = effectiveRunConfig.testDurationMinutes;
        result.workloadConfig.warmupDurationMinutes = effectiveRunConfig.warmupDurationMinutes;
        result.workloadConfig.rampIntervals = effectiveRunConfig.rampIntervals;
        result.workloadConfig.rampIntervalSeconds = effectiveRunConfig.rampIntervalSeconds;
        result.workloadConfig.sampleRateMillis = effectiveRunConfig.sampleRateMillis;

        CumulativeLatencies agg = new CumulativeLatencies();

        while (true) {
            Sleeper.sleepOrStop(stopRequested, testPoint.runConfig().sampleRateMillis, true);

            CountersStats totals = worker.getCountersStats();
            PeriodStats stats = worker.getPeriodStats();
            agg.endToEndLatency.add(stats.endToEndLatency);
            agg.endToEndDeliveryLatency.add(stats.endToEndDeliveryLatency);
            agg.publishDelayLatency.add(stats.publishDelayLatency);
            agg.scheduleLatency.add(stats.scheduleLatency);
            agg.publishLatency.add(stats.publishLatency);

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;

            double publishRate = stats.messagesSent / elapsed;
            double publishThroughput = stats.bytesSent / elapsed / 1_000_000;
            double consumeRate = stats.messagesReceived / elapsed;
            double consumeThroughput = stats.bytesReceived / elapsed / 1_000_000;

            logPeriodStats(stats, publishRate, publishThroughput, consumeRate, consumeThroughput,
                    oldTime, now,
                    totals.messagesSent(), totals.messagesReceived());
            logAvailabilityStatus();
            recordPeriodResults(result, stats, publishRate, consumeRate);

            if (stopRequested.get()) {
                log.info("Stop requested — finishing current workload");
                recordAggregateResults(result, agg);
                break;
            }

            if (terminateRequested) {
                log.info("Terminate requested — ending current test point");
                recordAggregateResults(result, agg);
                break;
            }

            if (skipPhaseRequested) {
                log.info("Skip-phase requested — ending current phase early");
                // Don't clear here. The caller (ramp for-loop or main flow)
                // inspects the flag to decide whether to break out of *its*
                // surrounding loop, then clears after the current phase.
                // Reachable only when collectAgg=false (ramp or warm-up) —
                // applyModification rejects skipPhase during the test phase.
                break;
            }

            // Discard-stats / run-for-minutes handling. Placed BEFORE the
            // time-end check so a fresh testEndTime via run-for-minutes is
            // honored on the same tick. The two flags are independent:
            //   - discardStats alone: wipe accumulated stats, keep the
            //     original testEndTime
            //   - runForMinutes alone: extend (or shorten) the test phase
            //     to N minutes from now, keep the accumulated stats
            //   - both together: wipe stats AND reset the remaining
            //     duration — the typical "lock in this throughput, give me
            //     a fresh N-minute measurement window" workflow
            // Snapshot+clear under the engine lock to serialize with
            // applyModification.
            boolean shouldDiscard;
            Integer newRunForMinutes;
            synchronized (this) {
                shouldDiscard = discardStatsRequested;
                newRunForMinutes = runForMinutesRequested;
                discardStatsRequested = false;
                runForMinutesRequested = null;
            }
            if (shouldDiscard || newRunForMinutes != null) {
                long modNow = System.nanoTime();
                if (shouldDiscard) {
                    log.info("DISCARD-STATS applied — converting elapsed test phase to warm-up{}",
                            newRunForMinutes != null
                                    ? ", setting remaining test duration to " + newRunForMinutes + "m"
                                    : "");
                    // Wipe per-period and cumulative state. We deliberately
                    // do NOT touch testPointStartNanos (used only for the
                    // cosmetic progress display) — the modifications log is
                    // the source of truth for when the reset happened.
                    agg = new CumulativeLatencies();
                    result.results = new TestResult.PerformanceResults();
                    worker.resetStats();
                    oldTime = modNow;
                } else {
                    log.info("RUN-FOR-MINUTES applied — test phase will run for {}m from now",
                            newRunForMinutes);
                }
                if (newRunForMinutes != null) {
                    testEndTime = modNow + Duration.ofMinutes(newRunForMinutes).toNanos();
                    // Update the overrides used by logPeriodStats so the
                    // displayed "elapsed/total minutes" reflects the new
                    // timeline. New test point total = elapsed-so-far +
                    // newRunForMinutes; total-run total adjusts by the
                    // delta vs. the original yaml-derived total.
                    int elapsedMin = (int)((modNow - testPointStartNanos) / 1e9 / 60.0);
                    int newTestPointMinutes = elapsedMin + newRunForMinutes;
                    int originalTestPointMinutes = testPointTotalMinutes(testPoint.runConfig());
                    int delta = newTestPointMinutes - originalTestPointMinutes;
                    testPointMinutesOverride = newTestPointMinutes;
                    totalRunMinutesOverride = runProgress.totalRunMinutes + delta;
                }
                // Don't break — continue measuring (with or without fresh stats).
            }

            if (now >= testEndTime) {
                recordAggregateResults(result, agg);
                break;
            }

            checkForFailedClients(worker.getHealth());

            oldTime = now;
        }

        return result;
    }

    private void logPeriodStats(PeriodStats stats, double publishRate, double publishThroughput,
                                double consumeRate, double consumeThroughput,
                                long sampleStartNanos, long sampleEndNanos,
                                long totalSent, long totalReceived) {
        // Scenario + test point header
        String scenarioInfo = testPoint.totalScenarios() >= 1
                ? String.format("──────── Scenario %d/%d: %s, ", testPoint.scenarioIndex() + 1,
                        testPoint.totalScenarios(), testPoint.scenarioName())
                : "";
        String header = String.format("%sTest point %d/%d: %s ────────",
                scenarioInfo, testPoint.dimensionIndex() + 1, testPoint.totalDimensions(), testPoint.label());

        // Progress lines. Use the override values when /modify
        // --run-for-minutes has reshaped the timeline; fall back to the
        // yaml-derived totals otherwise.
        int testPointMinutes = testPointMinutesOverride > 0
                ? testPointMinutesOverride
                : testPointTotalMinutes(testPoint.runConfig());
        int totalRunMinutes = totalRunMinutesOverride > 0
                ? totalRunMinutesOverride
                : runProgress.totalRunMinutes;
        double elapsedMinutes = (System.nanoTime() - testPointStartNanos) / 1e9 / 60.0;
        int testPointElapsed = Math.min((int) elapsedMinutes, testPointMinutes);
        int testPointPct = testPointMinutes > 0 ? (int) (elapsedMinutes / testPointMinutes * 100) : 100;
        testPointPct = Math.min(testPointPct, 100);

        int totalElapsed = runProgress.completedMinutesBefore + testPointElapsed;
        int totalPct = totalRunMinutes > 0
                ? (int) ((double) (runProgress.completedMinutesBefore + elapsedMinutes) / totalRunMinutes * 100)
                : 100;
        totalPct = Math.min(totalPct, 100);

        String progressLines = String.format(
                "Test point progress: %d/%d minutes, %d%%\n"
                + "Test run total progress: %d/%d minutes, %d%%",
                testPointElapsed, testPointMinutes, testPointPct,
                totalElapsed, totalRunMinutes, totalPct);

        String targetLine = "";
        targetLine = formatTargetLine(sampleStartNanos, sampleEndNanos);

        log.info(header + "\n" + progressLines
                + "\n── Throughput ─────────────────────────────────────────────\n"
                + targetLine
                + String.format("  Pub:    %,7d msg/s  %6.2f MB/s  total sent: %,d%n",
                        (long) publishRate, publishThroughput, totalSent)
                + String.format("  Cons:   %,7d msg/s  %6.2f MB/s  total received: %,d%n%n",
                        (long) consumeRate, consumeThroughput, totalReceived)
                + formatLatencyHeader()
                + formatLatencyRow("Publish Latency (ms)",
                        microsToMillis(stats.publishLatency.getMean()),
                        microsToMillis(stats.publishLatency.getValueAtPercentile(50)),
                        microsToMillis(stats.publishLatency.getValueAtPercentile(99)),
                        microsToMillis(stats.publishLatency.getValueAtPercentile(99.9)),
                        microsToMillis(stats.publishLatency.getMaxValue()))
                + formatLatencyRow("E2E Latency (ms)",
                        microsToMillis(stats.endToEndLatency.getMean()),
                        microsToMillis(stats.endToEndLatency.getValueAtPercentile(50)),
                        microsToMillis(stats.endToEndLatency.getValueAtPercentile(99)),
                        microsToMillis(stats.endToEndLatency.getValueAtPercentile(99.9)),
                        microsToMillis(stats.endToEndLatency.getMaxValue()))
                + formatLatencyRow("Pub Delay (ms)",
                        microsToMillis(stats.publishDelayLatency.getMean()),
                        microsToMillis(stats.publishDelayLatency.getValueAtPercentile(50)),
                        microsToMillis(stats.publishDelayLatency.getValueAtPercentile(99)),
                        microsToMillis(stats.publishDelayLatency.getValueAtPercentile(99.9)),
                        microsToMillis(stats.publishDelayLatency.getMaxValue())));
    }

    private void logAvailabilityStatus() {
        if (workload.availabilityConfig == null) return;
        try {
            AvailabilityResult.TestOutcome outcome = worker.getAvailabilityResult();
            if (outcome == null) return;
            var a = outcome.availability;
            log.info("\n── Availability ({} partitions, {}s) ──────────────────────\n"
                    + formatAvailabilityTable(a),
                    workload.availabilityConfig.probePartitions, a.wallClockMs / 1000);
        } catch (Exception e) {
            log.debug("Failed to get availability status", e);
        }
    }

    private void recordPeriodResults(TestResult result, PeriodStats stats,
                                     double publishRate, double consumeRate) {
        var r = result.results;
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

    private String formatTargetLine(long sampleStartNanos, long sampleEndNanos) {
        if (testPoint.producerRateSchedule() == null) {
            double rate = testPoint.producerRate();
            if (rate <= 0) return "";
            double throughput = rate * testPoint.messageSize() / 1_000_000;
            return String.format("  Target: %,9.1f msg/s  %6.2f MB/s%n", rate, throughput);
        }

        double startRate = scheduledRateAtSampleTime(sampleStartNanos);
        double endRate = scheduledRateAtSampleTime(sampleEndNanos);
        double startThroughput = startRate * testPoint.messageSize() / 1_000_000;
        double endThroughput = endRate * testPoint.messageSize() / 1_000_000;
        return String.format(
                "  Target: [%,.1f -> %,.1f] msg/s   [%.2f -> %.2f] MB/s%n",
                startRate, endRate, startThroughput, endThroughput);
    }

    private double scheduledRateAtSampleTime(long sampleTimeNanos) {
        if (testPoint.producerRateSchedule() == null) {
            return testPoint.producerRate();
        }
        long start = scheduleStartEpochMillis;
        if (start <= 0L) {
            return testPoint.producerRateSchedule().firstRate();
        }
        long nowNanos = System.nanoTime();
        long currentEpochMillis = System.currentTimeMillis();
        long sampleEpochMillis = currentEpochMillis
                - java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(Math.max(0L, nowNanos - sampleTimeNanos));
        return testPoint.producerRateSchedule().rateAt(
                Duration.ofMillis(Math.max(0L, sampleEpochMillis - start)));
    }

    private void recordAggregateResults(TestResult result, CumulativeLatencies agg) {
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

        var r = result.results;
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

    private void recordPerPartitionResults(TestResult result, PerPartitionStats perPartitionStats) {
        result.results.perPartitionResults = new ArrayList<>();

        for (var entry : perPartitionStats.partitions.entrySet()) {
            PerPartitionStats.PartitionMetrics m = entry.getValue();
            TestResult.PartitionResult pr = new TestResult.PartitionResult();
            pr.key = m.key;
            pr.messagesSent = m.messagesSent;
            pr.messagesReceived = m.messagesReceived;

            if (m.publishLatencyBytes != null) {
                try {
                    Histogram pubHist = Histogram.decodeFromCompressedByteBuffer(
                            ByteBuffer.wrap(m.publishLatencyBytes),
                            StatsCollector.NON_E2E_LATENCY_MAX_MICROS);
                    pr.publishLatencyAvg = microsToMillis(pubHist.getMean());
                    pr.publishLatency50pct = microsToMillis(pubHist.getValueAtPercentile(50));
                    pr.publishLatency95pct = microsToMillis(pubHist.getValueAtPercentile(95));
                    pr.publishLatency99pct = microsToMillis(pubHist.getValueAtPercentile(99));
                    pr.publishLatency999pct = microsToMillis(pubHist.getValueAtPercentile(99.9));
                    pr.publishLatencyMax = microsToMillis(pubHist.getMaxValue());
                } catch (Exception e) {
                    log.warn("Failed to decode per-partition publish latency for {}", m.key, e);
                }
            }

            if (m.endToEndLatencyBytes != null) {
                try {
                    Histogram e2eHist = Histogram.decodeFromCompressedByteBuffer(
                            ByteBuffer.wrap(m.endToEndLatencyBytes),
                            StatsCollector.E2E_LATENCY_MAX_MICROS);
                    pr.endToEndLatencyAvg = microsToMillis(e2eHist.getMean());
                    pr.endToEndLatency50pct = microsToMillis(e2eHist.getValueAtPercentile(50));
                    pr.endToEndLatency95pct = microsToMillis(e2eHist.getValueAtPercentile(95));
                    pr.endToEndLatency99pct = microsToMillis(e2eHist.getValueAtPercentile(99));
                    pr.endToEndLatency999pct = microsToMillis(e2eHist.getValueAtPercentile(99.9));
                    pr.endToEndLatencyMax = microsToMillis(e2eHist.getMaxValue());
                } catch (Exception e) {
                    log.warn("Failed to decode per-partition e2e latency for {}", m.key, e);
                }
            }

            result.results.perPartitionResults.add(pr);
        }

        log.info("Per-partition metrics recorded for {} partitions", result.results.perPartitionResults.size());
    }

    private static String formatAvailabilityTable(AvailabilityResult.AvailabilityData a) {
        return String.format("  %-12s %15s %15s%n", "", "Availability %", "Unavailability ms")
                + String.format("  %-12s %14.2f%% %,15d%n", "Producer", a.producerAvailabilityPct, a.producerUnavailableMs)
                + String.format("  %-12s %14.2f%% %,15d%n", "Consumer", a.e2eAvailabilityPct, a.e2eUnavailableMs)
                + String.format("  %-12s %14.2f%% %,15d", "Overall", a.overallAvailabilityPct, a.totalUnavailableMs);
    }

    private static String formatLatencyHeader() {
        return String.format("  %-22s %10s %10s %10s %10s %10s%n", "", "avg", "p50", "p99", "p99.9", "max");
    }

    private static String formatLatencyRow(String label, double avg, double p50,
                                            double p99, double p999, double max) {
        return String.format("  %-22s %10.2f %10.2f %10.2f %10.2f %10.2f%n", label, avg, p50, p99, p999, max);
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

    private static final DecimalFormat dec = new PaddingDecimalFormat("0.000", 4);

    /**
     * Total wall-clock minutes for a single test point: ramp-up + warm-up +
     * recorded test phase. Ramp seconds are rounded up to whole minutes so
     * progress totals never undershoot the actual elapsed time. Returns 0 for
     * a runConfig with no warm-up, no test, and no ramp (defensive).
     */
    private static int testPointTotalMinutes(RunConfig rc) {
        int rampSeconds = (rc.rampIntervals > 0 && rc.rampIntervalSeconds > 0)
                ? rc.rampIntervals * rc.rampIntervalSeconds
                : 0;
        int rampMinutes = (rampSeconds + 59) / 60;
        return rampMinutes + rc.warmupDurationMinutes + rc.testDurationMinutes;
    }

    private static final Logger log = LoggerFactory.getLogger(WorkloadEngine.class);
}
