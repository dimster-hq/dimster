package dev.dimster;

import dev.dimster.flink.FlinkPipeline;
import dev.dimster.flink.FlinkRestClient;
import dev.dimster.flink.FlinkResult;
import dev.dimster.hooks.HookContext;
import dev.dimster.hooks.HookPoint;
import dev.dimster.hooks.Hooks;
import dev.dimster.kafka.KafkaCluster;
import dev.dimster.utils.Sleeper;
import dev.dimster.utils.StopRequestedException;
import dev.dimster.utils.distributor.MessageDistributorType;
import dev.dimster.worker.Worker;
import dev.dimster.worker.commands.ClientHealth;
import dev.dimster.worker.topology.TopologyFormatter;
import dev.dimster.worker.topology.TopologyPlanner;
import dev.dimster.worker.topology.TopologyRecorder;
import dev.dimster.worker.topology.VirtualTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public abstract class BaseEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BaseEngine.class);

    private static final DateTimeFormatter SCENARIO_TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");

    protected final Worker worker;
    protected final TopologyPlanner topologyPlanner;
    protected final AtomicBoolean stopRequested;
    /**
     * Sink the engine calls after each {@link #planTopology} call. Set via
     * {@link #setTopologyRecorder} by the harness (e.g. {@code BenchmarkRunner})
     * so the topology of every test point can be persisted to a results file.
     * Defaults to {@link TopologyRecorder#NO_OP} so engines that haven't been
     * wired up don't trip over a null call.
     */
    private TopologyRecorder topologyRecorder = TopologyRecorder.NO_OP;

    /**
     * The workload's hooks, set by the harness via {@link #setHooks}. Engines
     * decide when each {@link HookPoint} is reached; {@link Hooks} runs the
     * scripts. Defaults to {@link Hooks#NONE}.
     */
    private Hooks hooks = Hooks.NONE;
    protected KafkaCluster cluster;

    /**
     * The {@link VirtualTopology} for the current test point. Built by
     * {@link #planTopology} and consumed by {@link #createProducers} and
     * {@link #createConsumers}. Per-test-point — replaced on every
     * {@code planTopology} call.
     */
    protected VirtualTopology plannedTopology;

    /**
     * Runs the Flink job of test points with a Flink stage. Created on first
     * use, so runs without Flink never contact a Flink cluster.
     */
    private FlinkPipeline flinkPipeline;

    /**
     * Find an existing scenario by name in the list, or create and append a new one.
     */
    protected static <S> S findOrCreateScenario(List<S> scenarios, String name,
                                                 Function<S, String> nameOf,
                                                 Function<String, S> create) {
        for (S s : scenarios) {
            if (name.equals(nameOf.apply(s))) return s;
        }
        S newScenario = create.apply(name);
        scenarios.add(newScenario);
        return newScenario;
    }

    /**
     * Current wall-clock time formatted to match BenchmarkRunner's campaign-level
     * begin/end timestamps. Engines call this around each test point to record
     * per-scenario boundaries that the dashboard exporter uses to keep all test
     * points of a scenario inside the same HTML window file.
     */
    protected static String currentScenarioTimestamp() {
        return SCENARIO_TIMESTAMP_FORMAT.format(LocalDateTime.now(ZoneId.systemDefault()));
    }

    protected BaseEngine(Worker worker, TopologyPlanner topologyPlanner, AtomicBoolean stopRequested) {
        this.worker = worker;
        this.topologyPlanner = topologyPlanner;
        this.stopRequested = stopRequested;
    }

    /**
     * Inject a {@link TopologyRecorder}. {@code BenchmarkRunner} calls this
     * with a recorder that accumulates per-test-point topologies and writes
     * them to a results file. Tests can leave the default {@code NO_OP} in
     * place.
     */
    public void setTopologyRecorder(TopologyRecorder recorder) {
        this.topologyRecorder = recorder != null ? recorder : TopologyRecorder.NO_OP;
    }

    /** Inject the workload's {@link Hooks}. Tests can leave the default {@code NONE}. */
    public void setHooks(Hooks hooks) {
        this.hooks = hooks != null ? hooks : Hooks.NONE;
    }

    /** Whether the workload configures any hook for the point. */
    protected boolean hasHooks(HookPoint hookPoint) {
        return hooks.has(hookPoint);
    }

    /**
     * Run the hooks for a point within a test point.
     *
     * @param topics the topics holding data at this point, passed to the hook
     */
    protected void runHooks(HookPoint hookPoint, ScenarioExpander.TestPoint point, int index, int total,
                            List<String> topics) {
        if (!hooks.has(hookPoint)) return;
        String bootstrap = point.kafkaConfig() != null
                ? point.kafkaConfig().commonProperties().getProperty("bootstrap.servers")
                : null;
        hooks.run(hookPoint, new HookContext(index, total, point.label(), point.scenarioName(), bootstrap, topics));
    }

    /**
     * Hand the latest {@link #plannedTopology} to the recorder and, if
     * {@code printToLog} is true, dump a human-readable rendering to the log.
     * Engines call this once per test point, immediately after
     * {@link #planTopology}.
     */
    protected void recordPlannedTopology(String testPointLabel, boolean printToLog) {
        if (plannedTopology == null) return;
        if (printToLog) {
            log.info("\n{}", TopologyFormatter.format(
                    "Planned topology for test point '" + testPointLabel + "':", plannedTopology));
        }
        try {
            topologyRecorder.record(testPointLabel, plannedTopology);
        } catch (Exception e) {
            // Recorder failure must never break a test run — it's an artifact, not load logic.
            log.warn("Topology recorder failed for test point '{}': {}", testPointLabel, e.toString());
        }
    }

    /**
     * Sleep between test points to let Kafka stabilize and metric scrapers
     * collect all stats before the next test point deletes topics/groups.
     * Runs the afterTestPoint hooks once the test point's resources are gone.
     * Skips the cooldown after the last test point.
     */
    protected void cooldownBetweenTests(int cooldownMinutes, ScenarioExpander.TestPoint point,
                                        int currentIndex, int totalTests) throws IOException {
        cooldownBetweenTests(cooldownMinutes, point, currentIndex, totalTests, Set.of());
    }

    /**
     * As above, keeping the {@code retainTopics} (e.g. a backlog the next
     * test point reuses), which the afterTestPoint hooks are told about.
     */
    protected void cooldownBetweenTests(int cooldownMinutes, ScenarioExpander.TestPoint point,
                                        int currentIndex, int totalTests, Set<String> retainTopics) throws IOException {
        log.info("Test point teardown: Stops all producers, consumers, deletes the topics and groups");
        log.info("Test point teardown: Waiting for 30 seconds for metrics scraping before starting");
        Sleeper.sleepOrStop(stopRequested, 30000, true);

        releaseTestPointResources(retainTopics);
        log.info("Test point teardown: Completed");

        runHooks(HookPoint.AFTER_TEST_POINT, point, currentIndex, totalTests, new ArrayList<>(retainTopics));

        if (cooldownMinutes > 0 && currentIndex < totalTests - 1 && !stopRequested.get()) {
            log.info("Cooldown: waiting {}m before next test point...", cooldownMinutes);
            Sleeper.sleepOrStop(stopRequested, cooldownMinutes * 60_000L, true);
        } else {
            log.info("Cooldown: No cooldown configured, moving onto next test point");
        }
    }

    /**
     * Stops the test point's clients and Flink job and deletes its groups and
     * topics, except {@code retainTopics}. The part of the teardown that frees
     * resources, without the scrape wait, hooks or cooldown.
     */
    protected void releaseTestPointResources(Set<String> retainTopics) throws IOException {
        log.info("Test point teardown: Stopping all producers and consumers");
        worker.stopAll();
        if (!stopRequested.get())
            worker.awaitStopComplete(java.time.Duration.ofMinutes(5));

        // The Flink job reads and writes the test topics: stop it before they go.
        stopFlink();

        log.info("Test point teardown: Deleting topics and groups");
        cluster.deleteTestGroups();
        cluster.deleteTestTopics(retainTopics);
        if (!retainTopics.isEmpty()) {
            log.info("Test point teardown: Kept {} topic(s) for reuse by the next test point", retainTopics.size());
        }
    }

    /**
     * Throw a StopRequestedException if stop has been requested
     */
    protected void throwOnStopRequested(AtomicBoolean stopRequested) {
        if (stopRequested.get())
            throw new StopRequestedException();
    }

    protected void checkForFailedClients(ClientHealth health) {
        if (health.hasFailures()) {
            throw health.failureException();
        }
    }

    protected void waitForClientsReady(String clientTypeFilter, int timeoutSeconds) throws IOException {
        String label = clientTypeFilter.toLowerCase();
        log.info("Waiting for {}s to be running", label);
        Instant deadline = Instant.now().plus(Duration.ofSeconds(timeoutSeconds));
        while (Instant.now().isBefore(deadline)) {
            ClientHealth health = worker.getHealth();
            checkForFailedClients(health);
            ClientHealth.ClientCounts counts = health.countsFor(clientTypeFilter);
            if (counts.total() > 0 && counts.allRunning()) {
                log.info("All {}s are running", label);
                return;
            }
            Sleeper.sleepOrStop(stopRequested, 1000, true);
        }
        log.warn("Timed out waiting for {}s to be running", label);
    }

    protected void ensureReady(List<String> topics, int consumerGroupsPerTopic,
                               int readyTimeoutSeconds) throws IOException {
        awaitAllClientsRunning(readyTimeoutSeconds);

        log.info("Probing topics to verify end-to-end connectivity");
        int expectedMessages = topics.size() * consumerGroupsPerTopic;
        worker.probeProducers();

        Instant deadline = Instant.now().plus(Duration.ofSeconds(120));
        Instant lastProbe = Instant.now();
        while (Instant.now().isBefore(deadline)) {
            long totalReceived = worker.getProbesReceived();
            if (totalReceived >= expectedMessages) {
                log.info("All topics verified — consumers are receiving messages");
                return;
            }
            if (Duration.between(lastProbe, Instant.now()).toSeconds() >= 10) {
                checkForFailedClients(worker.getHealth());
                log.info("{} probe messages received. Performing another producer probe", totalReceived);
                worker.probeProducers();
                lastProbe = Instant.now();
            }
            Sleeper.sleepOrStop(stopRequested, 1000, true);
        }
        log.warn("Timed out waiting for consumers to receive all probe messages. {}/{} received",
                worker.getProbesReceived(), expectedMessages);
    }

    private void awaitAllClientsRunning(int readyTimeoutSeconds) throws IOException {
        log.info("Waiting for all clients to be running");
        Instant deadline = Instant.now().plus(Duration.ofSeconds(readyTimeoutSeconds));
        while (Instant.now().isBefore(deadline)) {
            ClientHealth health = worker.getHealth();
            checkForFailedClients(health);
            if (health.allRunning()) {
                log.info("All clients are running");
                return;
            }
            Sleeper.sleepOrStop(stopRequested, 1000, true);
        }
    }

    /**
     * Readiness when a Flink job sits between producers and consumers. The
     * job is already running ({@link #startFlink} waits for that), so this
     * waits for the clients and sends one round of probes. Probes are
     * ordinary rows, so the workload's SQL may filter them out: a shortfall
     * is expected and only logged, rather than retried for two minutes.
     */
    protected void ensureReadyThroughFlink(List<String> consumerTopics, int consumerGroupsPerTopic,
                                           int readyTimeoutSeconds) throws IOException {
        awaitAllClientsRunning(readyTimeoutSeconds);

        int expectedMessages = consumerTopics.size() * consumerGroupsPerTopic;
        log.info("Sending one probe per producer to the input topics; "
                + "probes reach the consumers only if the Flink SQL keeps them");
        worker.probeProducers();
        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        while (Instant.now().isBefore(deadline)) {
            if (worker.getProbesReceived() >= expectedMessages) {
                log.info("All probes reached the consumers on the output topics");
                return;
            }
            Sleeper.sleepOrStop(stopRequested, 1000, true);
        }
        log.info("{}/{} probes reached the consumers within 30s; continuing "
                        + "(probes are ordinary rows, so the Flink SQL may filter them out)",
                worker.getProbesReceived(), expectedMessages);
    }

    // ─── Flink stage ───────────────────────────────────────────────────

    /** Test hook: use the given pipeline instead of one built from the environment. */
    public void setFlinkPipeline(FlinkPipeline pipeline) {
        this.flinkPipeline = pipeline;
    }

    private FlinkPipeline flinkPipeline() {
        if (flinkPipeline == null) {
            var pipeline = new FlinkPipeline(FlinkRestClient.fromEnvironment(), stopRequested);
            pipeline.checkReachable();
            flinkPipeline = pipeline;
        }
        return flinkPipeline;
    }

    /** For engines that don't support a Flink stage: fails if any test point has one. */
    protected static void rejectFlink(String mode, List<ScenarioExpander.TestPoint> points) {
        for (var point : points) {
            if (point.flink() != null) {
                throw new IllegalArgumentException("Scenario '%s': flink is supported in run and drain-backlog modes, not %s"
                        .formatted(point.scenarioName(), mode));
            }
        }
    }

    /** Output partitions to plan for: the Flink stage's, or 0 without one. */
    protected static int outputPartitionsPerTopic(ScenarioExpander.TestPoint point) {
        return point.flink() != null ? point.flink().outputPartitionsPerTopic : 0;
    }

    /**
     * Cancels Flink jobs left by an interrupted run, alongside the deletion of
     * leftover topics. Only touches Flink for test points with a Flink stage
     * (or once a pipeline exists), so other runs never need a Flink cluster.
     */
    protected void cancelLeftoverFlinkJobs(ScenarioExpander.TestPoint point) throws IOException {
        if (point.flink() != null || flinkPipeline != null) {
            flinkPipeline().cancelLeftovers();
        }
    }

    /**
     * Starts the test point's Flink job over the planned pipelines and waits
     * until it runs. Call after the topics exist and before consumers start.
     */
    protected void startFlink(ScenarioExpander.TestPoint point) throws IOException {
        flinkPipeline().start(point.flink(), point.dataset(), plannedTopology, point.kafkaConfig());
    }

    /** Cancels the running Flink job, if any. Safe to call at any time. */
    protected void stopFlink() {
        if (flinkPipeline != null) {
            flinkPipeline.stop();
        }
    }

    /** What the results record about a test point's Flink job; null without one. */
    protected FlinkResult flinkResult(ScenarioExpander.TestPoint point) {
        if (point.flink() == null || flinkPipeline == null) return null;
        return FlinkResult.of(point.flink(), flinkPipeline.jobId(), flinkPipeline.restarts());
    }

    /**
     * Build a {@link VirtualTopology} for the current test point and store it
     * as {@link #plannedTopology}. The planner generates topic and consumer
     * group names from the supplied counts; engines then create the actual
     * Kafka topics via {@link #createPlannedTopics()} using those names.
     */
    protected void planTopology(int numTopics, int partitionsPerTopic,
                                int producersPerTopic, int consumerGroupsPerTopic, int consumersPerGroup,
                                ConsumerType consumerType, int procMsMin, int procMsMax, int procParallelism,
                                ConsumerProcMode procMode, int procCapacity,
                                MessageDistributorType messageDistributorType, double messageDistributorSkew,
                                boolean correctnessEnabled, boolean staticConsumerPartitions) {
        planTopology(numTopics, partitionsPerTopic, producersPerTopic, consumerGroupsPerTopic, consumersPerGroup,
                consumerType, procMsMin, procMsMax, procParallelism, procMode, procCapacity,
                messageDistributorType, messageDistributorSkew, correctnessEnabled, staticConsumerPartitions, 0);
    }

    /**
     * As above, with a Flink stage when {@code outputPartitionsPerTopic > 0}:
     * consumers then read a {@code <topic>-out} topic of that many partitions,
     * which the Flink job writes.
     */
    protected void planTopology(int numTopics, int partitionsPerTopic,
                                int producersPerTopic, int consumerGroupsPerTopic, int consumersPerGroup,
                                ConsumerType consumerType, int procMsMin, int procMsMax, int procParallelism,
                                ConsumerProcMode procMode, int procCapacity,
                                MessageDistributorType messageDistributorType, double messageDistributorSkew,
                                boolean correctnessEnabled, boolean staticConsumerPartitions,
                                int outputPartitionsPerTopic) {
        planTopology(numTopics, partitionsPerTopic, producersPerTopic, consumerGroupsPerTopic, consumersPerGroup,
                consumerType, procMsMin, procMsMax, procParallelism, procMode, procCapacity,
                messageDistributorType, messageDistributorSkew, correctnessEnabled, staticConsumerPartitions,
                outputPartitionsPerTopic, null);
    }

    /**
     * As above, over existing input topics when {@code inputTopics} is not
     * null (see {@link TopologyPlanner#plan(TopologyPlanner.TopologySpec, List)}).
     */
    protected void planTopology(int numTopics, int partitionsPerTopic,
                                int producersPerTopic, int consumerGroupsPerTopic, int consumersPerGroup,
                                ConsumerType consumerType, int procMsMin, int procMsMax, int procParallelism,
                                ConsumerProcMode procMode, int procCapacity,
                                MessageDistributorType messageDistributorType, double messageDistributorSkew,
                                boolean correctnessEnabled, boolean staticConsumerPartitions,
                                int outputPartitionsPerTopic, List<String> inputTopics) {
        var spec = new TopologyPlanner.TopologySpec(
                cluster.topicPrefix(),
                numTopics,
                partitionsPerTopic,
                producersPerTopic,
                consumerGroupsPerTopic,
                consumersPerGroup,
                consumerType,
                procMsMin, procMsMax, procParallelism, procMode, procCapacity,
                messageDistributorType, messageDistributorSkew,
                correctnessEnabled,
                staticConsumerPartitions,
                outputPartitionsPerTopic);
        this.plannedTopology = topologyPlanner.plan(spec, inputTopics);
        log.info("Planned topology: {} topics, {} producers, {} consumers, {} active producer workers",
                plannedTopology.topicPartitionCounts().size(),
                plannedTopology.producers().size(),
                plannedTopology.consumers().size(),
                plannedTopology.activeProducerWorkerCount());
    }

    /**
     * Create the Kafka topics named in the planned topology. Must be called
     * after {@link #planTopology} and before {@link #createProducers}/{@link #createConsumers}.
     * Returns the topic names so engines can pass them to other helpers (e.g. {@code ensureReady}).
     */
    protected List<String> createPlannedTopics() {
        return createPlannedTopics(Set.of(), Map.of());
    }

    /**
     * As above, skipping the {@code existing} topics (which must already be on
     * the cluster) and applying {@code configOverrides} to the ones created.
     * Returns every planned topic name, existing ones included.
     */
    protected List<String> createPlannedTopics(Set<String> existing, Map<String, String> configOverrides) {
        var topicPartitionCounts = new LinkedHashMap<>(plannedTopology.topicPartitionCounts());
        List<String> allTopics = new ArrayList<>(topicPartitionCounts.keySet());
        topicPartitionCounts.keySet().removeAll(existing);
        int count = topicPartitionCounts.size();
        int progressInterval = Math.max(1, (int) Math.ceil(count / 50.0));
        progressInterval = (int) Math.ceil(progressInterval / 10.0) * 10;
        if (progressInterval > count) {
            progressInterval = count;
        }

        List<String> topicNames = new ArrayList<>(topicPartitionCounts.keySet());
        for (int i = 0; i < topicNames.size(); i++) {
            String topic = topicNames.get(i);
            cluster.createTopic(topic, topicPartitionCounts.get(topic), configOverrides);

            if (i < 10) {
                log.info("Created topic {}", topic);
            } else if (i == 10) {
                log.info("Created topic {} (only prints first 10)...", topic);
            }

            if ((i + 1) % progressInterval == 0 || i + 1 == count) {
                log.info("Created {}/{} topics", i + 1, count);
            }
        }
        // Allow metadata propagation before creating producers and consumers.
        if (count > 0) {
            Sleeper.sleepOrStop(stopRequested, 5000, true);
        }
        return allTopics;
    }

    protected void createProducers() throws IOException {
        worker.createProducers(plannedTopology.producers());
        log.info("Created {} producers", plannedTopology.producers().size());
    }

    protected void createConsumers() throws IOException {
        var consumerSet = new dev.dimster.worker.topology.ConsumerSet(
                plannedTopology.consumers(),
                plannedTopology.consumerType(),
                plannedTopology.consumerProcMsMin(),
                plannedTopology.consumerProcMsMax(),
                plannedTopology.consumerProcParallelism(),
                plannedTopology.consumerProcMode(),
                plannedTopology.consumerProcCapacity());

        if (consumerSet.consumerType() == ConsumerType.SHARE_GROUP) {
            // Each unique (topic, group) needs the share group registered once on the
            // cluster, with the topic's partition count for offset bookkeeping.
            Set<String> seen = new HashSet<>();
            for (var vc : consumerSet.consumers()) {
                String key = vc.topic() + ":" + vc.consumerGroup();
                if (seen.add(key)) {
                    int partitions = plannedTopology.topicPartitionCounts().getOrDefault(vc.topic(), 0);
                    cluster.createShareGroup(vc.consumerGroup(), vc.topic(), partitions);
                }
            }
            // Give the metadata propagation a moment before consumers try to subscribe.
            Sleeper.sleepOrStop(stopRequested, 5000, true);
        }

        worker.createConsumers(consumerSet);
        log.info("Created {} consumers", consumerSet.consumers().size());
    }

    /**
     * Starts a measurement window: discards everything the worker's period
     * counters accumulated so far, then returns the window's start time.
     */
    protected long startMeasurementWindow() throws IOException {
        worker.getPeriodStats();
        return System.nanoTime();
    }

    /**
     * Tells the worker which dataset this test point generates records from
     * (the workload's, or the payload dataset). Must run before clients are
     * created and probed: producers generate rows from it and consumers use
     * its record format.
     */
    protected void configureDataset(ScenarioExpander.TestPoint point) throws IOException {
        worker.setDataset(point.dataset());
        worker.setConsumedThroughFlink(point.flink() != null);
    }

}
