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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;

/**
 * Orchestrates drain-backlog tests: builds a consumer backlog by pausing consumers,
 * then measures the time to drain the backlog with optional producer load.
 */
public class DrainBacklogEngine extends BaseEngine {

    private static final Logger log = LoggerFactory.getLogger(DrainBacklogEngine.class);

    private final DrainBacklogWorkload workload;
    private final BaseWorkload base;
    private final KafkaConfig fileKafkaConfig;

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

                // Stamp the scenario boundary the first time we touch it, then
                // refresh the end after every test so the exporter knows the
                // exact time range this scenario occupies in Prometheus.
                if (scenarioResult.beginTime == null) {
                    scenarioResult.beginTime = currentScenarioTimestamp();
                }
                DrainBacklogResult.TestOutcome outcome = runSingleTest(point);
                scenarioResult.endTime = currentScenarioTimestamp();
                scenarioResult.tests.add(outcome);

                log.info("    Result: built {}MB in {}s, drained in {}s (producer rate during drain: {})",
                        outcome.drain.backlogSizeBytes / 1_000_000,
                        outcome.drain.buildDurationMs / 1000,
                        outcome.drain.drainDurationMs / 1000,
                        outcome.drain.backlogProducerRate == -1 ? "paused" : outcome.drain.backlogProducerRate + " msg/s");

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

        RepeatMerger.mergeDrainBacklogRepeats(result);

        return result;
    }

    private DrainBacklogResult.TestOutcome runSingleTest(TestPoint point) throws Exception {
        DrainBacklogConfig dbc = point.drainBacklogConfig();

        worker.stopAll();
        worker.awaitStopComplete(java.time.Duration.ofMinutes(10));
        KafkaConfig pointConfig = point.kafkaConfig();
        worker.setKafkaConfig(pointConfig);
        cluster = new KafkaCluster(
                dev.dimster.kafka.KafkaSpi.factory().newAdmin(pointConfig.commonProperties()),
                pointConfig);

        cluster.deleteTestTopics();
        cluster.deleteTestGroups();

        throwOnStopRequested(stopRequested);

        // Plan first, then create the actual Kafka topics from the planned topic names.
        planTopology(point.topics(), point.partitionsPerTopic(),
                point.producersPerTopic(), point.consumerGroupsPerTopic(), point.consumersPerGroup(),
                point.consumerType(), point.consumerProcMsMin(), point.consumerProcMsMax(), point.consumerProcParallelism(),
                point.consumerProcMode(), point.consumerProcCapacity(),
                point.messageDistributor(), point.messageDistributorSkew(),
                false /* correctness off */,
                false /* dynamic consumer assignment */);
        recordPlannedTopology(point.label(), workload.engineConfig.printTopology);
        createPlannedTopics();
        // Set before producers: consumers, created after the backlog is
        // built, read the same dataset's record format.
        configureDataset(point);
        createProducers();
        waitForClientsReady("PRODUCER", 60);

        // Enable per-partition/per-consumer metrics before producers start sending,
        // so build-phase sends populate the per-partition counters for Prometheus.
        if (workload.engineConfig.perPartitionMetrics) {
            worker.enablePerPartitionMetrics();
        }
        if (workload.engineConfig.perConsumerMetrics) {
            worker.enablePerConsumerMetrics();
        }

        // Start producers at build rate
        double buildRate = dbc.buildRate > 0 ? dbc.buildRate : point.producerRate();
        log.info("Starting producers at build rate: {} msg/s", buildRate);
        worker.startLoad(new ProducerWorkAssignment(
                buildRate, point.messageDistributor(),
                point.partitionsPerTopic(), point.messageDistributorSkew()));

        var perf = new TestResult.PerformanceResults();
        var drainData = new DrainBacklogResult.DrainData();
        long oldTime = startMeasurementWindow();

        // === BUILD PHASE ===
        throwOnStopRequested(stopRequested);
        log.info("----- Building backlog (target: {} GB) -----", dbc.backlogSizeGB);
        long buildStart = System.currentTimeMillis();

        long requestedBacklogSize = (long) (dbc.backlogSizeGB * 1_000_000_000.0);
        long backlogMessages = 0;

        while (!stopRequested.get()) {
            Sleeper.sleepOrStop(stopRequested, dbc.sampleRateMillis, true);

            PeriodStats stats = worker.getPeriodStats();
            long now = System.nanoTime();
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

        long buildDuration = System.currentTimeMillis() - buildStart;
        long backlogSizeBytes = backlogMessages * point.messageSize();
        log.info("    Backlog built: {} messages, {}MB in {}s",
                backlogMessages, backlogSizeBytes / 1_000_000, buildDuration / 1000);

        // === DRAIN PHASE ===
        if (dbc.backlogProducerRate > 0) {
            worker.adjustPublishRate(dbc.backlogProducerRate);
            log.info("----- Draining backlog (producer rate: {} msg/s) -----", dbc.backlogProducerRate);
        } else {
            worker.pauseProducers();
            log.info("----- Draining backlog (producers paused) -----");
        }

        // Create consumers now that backlog is built — topology was already planned upfront
        createConsumers();
        worker.setMaxConsumerRecreations(workload.engineConfig.maxConsumerRecreations);
        waitForClientsReady("CONSUMER", 60);

        // Extract unique group names for broker lag reporting
        Set<String> groupNames = plannedTopology.consumers().stream()
                .map(vc -> vc.consumerGroup())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        boolean isShareGroup = point.consumerType() == dev.dimster.ConsumerType.SHARE_GROUP;

        long lagRefreshIntervalMs = dbc.sampleRateMillis;
        long lastLagRefresh = 0;
        long brokerLag = -1; // -1 means not yet fetched

        long drainStart = System.currentTimeMillis();
        String lagDescription = isShareGroup
                ? "broker-reported share group lag"
                : "broker-reported consumer group lag (LogEndOffset - CommittedOffset)";
        log.info("    'expect recv' = total sent x {} consumer group(s)", point.consumerGroupsPerTopic());
        log.info("    'lag' = {}, refreshed every {}s via admin API", lagDescription, lagRefreshIntervalMs / 1000);
        log.info(String.format("    %12s %12s %14s %14s %14s %12s %10s %14s",
                "pub msg/s", "cons msg/s", "total sent", "expect recv", "total recv", "backlog", "backlog MB", "lag"));

        while (!stopRequested.get()) {
            Sleeper.sleepOrStop(stopRequested, dbc.sampleRateMillis, true);

            PeriodStats stats = worker.getPeriodStats();
            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;
            double publishRate = stats.messagesSent / elapsed;
            double consumeRate = stats.messagesReceived / elapsed;
            oldTime = now;

            CountersStats counters = worker.getCountersStats();
            long totalSent = counters.messagesSent();
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

            if (currentBacklog <= 1000) {
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

        long drainDuration = System.currentTimeMillis() - drainStart;
        log.info("    Backlog drained in {}s", drainDuration / 1000);

        // Build outcome
        DrainBacklogResult.TestOutcome outcome = new DrainBacklogResult.TestOutcome();
        outcome.label = point.label();
        populateWorkloadConfig(outcome.workloadConfig, point);
        drainData.backlogSizeGB = dbc.backlogSizeGB;
        drainData.backlogSizeBytes = backlogSizeBytes;
        drainData.backlogMessages = backlogMessages;
        drainData.buildRate = (int) buildRate;
        drainData.buildDurationMs = buildDuration;
        drainData.drainDurationMs = drainDuration;
        drainData.drainTimeoutMinutes = dbc.drainTimeoutMinutes;
        drainData.backlogProducerRate = dbc.backlogProducerRate;
        drainData.sampleRateMillis = dbc.sampleRateMillis;
        outcome.drain = drainData;
        outcome.performance = perf;

        return outcome;
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
        wc.messageDistributor = point.messageDistributor().name();
        wc.producersPerTopic = point.producersPerTopic();
        wc.producerRate = point.producerRate();
        wc.consumerGroupsPerTopic = point.consumerGroupsPerTopic();
        wc.consumersPerGroup = point.consumersPerGroup();
        wc.sampleRateMillis = point.drainBacklogConfig().sampleRateMillis;
    }

    @Override
    public void close() throws Exception {
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
