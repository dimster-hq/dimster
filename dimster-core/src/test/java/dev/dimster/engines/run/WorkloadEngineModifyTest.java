package dev.dimster.engines.run;

import dev.dimster.ConsumerProcMode;
import dev.dimster.ConsumerType;
import dev.dimster.ProducerRateSchedule;
import dev.dimster.datagen.DatasetResolver;
import dev.dimster.datagen.ResolvedDataset;
import dev.dimster.ScenarioExpander.TestPoint;
import dev.dimster.coordinator.ModifyRequest;
import dev.dimster.coordinator.ModifyResult;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.utils.distributor.MessageDistributorType;
import dev.dimster.worker.Worker;
import dev.dimster.worker.topology.ConsumerSet;
import dev.dimster.worker.topology.TopologyPlanner;
import dev.dimster.worker.topology.VirtualConsumer;
import dev.dimster.worker.topology.VirtualProducer;
import dev.dimster.worker.topology.VirtualTopology;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link WorkloadEngine#applyModification} as the user-facing
 * facade over {@link TestPointMutator} + {@link DiffApplier}.
 *
 * Coverage targets:
 *  - Engine-internal action flags (terminate / discardStats / runForMinutes)
 *    and their validation rules.
 *  - Field-override path (workloadOverrides + kafkaConfigOverrides) end-
 *    to-end through the mutator and applier, asserting the right Worker
 *    method is called with the right values.
 *
 * Many tests use reflection to set engine state (testPointActive,
 * testPhaseActive, testPoint, plannedTopology). This avoids a test-only
 * setter on the production class — the fields are otherwise tightly
 * controlled by runSingleTest, which we don't want to drive here.
 */
public class WorkloadEngineModifyTest {

    /**
     * Recording stub Worker. Captures every call we care about so
     * assertions can verify what landed.
     */
    private static class RecordingWorker implements Worker {
        Double lastRate = null;
        Integer lastProcMin = null;
        Integer lastProcMax = null;
        List<VirtualProducer> lastAddedProducers = null;
        List<String> lastRemovedProducers = null;
        List<VirtualProducer> lastRestartedProducers = null;
        ConsumerSet lastAddedConsumers = null;
        List<String> lastRemovedConsumers = null;
        ConsumerSet lastRestartedConsumers = null;
        KafkaConfig lastKafkaConfig = null;
        ResolvedDataset lastDataset = null;
        // The dataset that was set when producers were last restarted.
        ResolvedDataset datasetAtRestart = null;
        List<VirtualProducer> lastCreatedProducers = null;
        dev.dimster.worker.commands.ProducerWorkAssignment lastStartLoadAssignment = null;
        ProducerRateSchedule lastRateSchedule = null;
        IOException publishRateException = null;

        @Override public void adjustPublishRate(double publishRate) throws IOException {
            if (publishRateException != null) throw publishRateException;
            this.lastRate = publishRate;
        }
        @Override public void adjustPublishRateSchedule(ProducerRateSchedule schedule) {
            this.lastRateSchedule = schedule;
        }
        @Override public void setConsumerProcessingTime(int minMs, int maxMs) {
            this.lastProcMin = minMs;
            this.lastProcMax = maxMs;
        }
        @Override public void addProducers(List<VirtualProducer> p) { this.lastAddedProducers = p; }
        @Override public void removeProducers(List<String> n) { this.lastRemovedProducers = n; }
        @Override public void restartProducers(List<VirtualProducer> p) {
            this.lastRestartedProducers = p;
            this.datasetAtRestart = lastDataset;
        }
        @Override public void addConsumers(ConsumerSet c) { this.lastAddedConsumers = c; }
        @Override public void removeConsumers(List<String> n) { this.lastRemovedConsumers = n; }
        @Override public void restartConsumers(ConsumerSet c) { this.lastRestartedConsumers = c; }
        @Override public void setKafkaConfig(KafkaConfig c) { this.lastKafkaConfig = c; }
        @Override public void setDataset(ResolvedDataset d) { this.lastDataset = d; }

        // No-op everything else — applyModification doesn't touch them.
        @Override public void setKafkaConfig(File f) {}
        @Override public void createProducers(List p) { this.lastCreatedProducers = p; }
        @Override public void createConsumers(ConsumerSet c) {}
        @Override public void probeProducers() {}
        @Override public void startLoad(dev.dimster.worker.commands.ProducerWorkAssignment a) {
            this.lastStartLoadAssignment = a;
        }
        @Override public void pauseProducers() {}
        @Override public void resumeProducers() {}
        @Override public dev.dimster.worker.commands.CountersStats getCountersStats() {
            return new dev.dimster.worker.commands.CountersStats(0, 0);
        }
        @Override public long getProbesReceived() { return 0; }
        @Override public dev.dimster.worker.commands.PeriodStats getPeriodStats() {
            return new dev.dimster.worker.commands.PeriodStats();
        }
        @Override public void resetStats() {}
        @Override public void stopAll() {}
        @Override public boolean isStopComplete() { return true; }
        @Override public void awaitStopComplete(Duration t) {}
        @Override public dev.dimster.worker.commands.ClientHealth getHealth() {
            return dev.dimster.worker.commands.ClientHealth.empty();
        }
        @Override public void close() {}
    }

    // ─── Helpers ──────────────────────────────────────────────────────

    private WorkloadEngine newEngine(RecordingWorker worker) {
        Workload workload = new Workload();
        workload.name = "test";
        // Real TopologyPlanner so the field-override paths can call into it.
        TopologyPlanner planner = new TopologyPlanner(
                List.of("http://worker-0:8080"),
                TopologyPlanner.WorkerMode.MIXED,
                new TopologyPlanner.RatioSplit(1, 1));
        return new WorkloadEngine(workload, worker, planner, null, new AtomicBoolean(false));
    }

    /** Construct a minimal TestPoint matching the helper fixture topology below. */
    private TestPoint makeTestPoint(int producersPerTopic, int consumersPerGroup,
                                       double producerRate, int procMsMin, int procMsMax) {
        return new TestPoint(
                "default", 0, 1, "default", 0, 1,
                /*topics*/ 1, /*partitionsPerTopic*/ 4,
                producersPerTopic, /*consumerGroupsPerTopic*/ 1, consumersPerGroup,
                /*messageSize*/ 1000, producerRate, null,
                MessageDistributorType.NO_KEY, /*skew*/ 1.0,
                ConsumerType.CONSUMER_GROUP,
                new KafkaConfig(),
                procMsMin, procMsMax, /*procParallelism*/ 1,
                ConsumerProcMode.BLOCKING,
                10000, // consumerProcCapacity
                null, null,
                new dev.dimster.engines.run.RunConfig(),
                DatasetResolver.payloadDataset(1000 - 1, 0.5, 100, null));
    }

    private TestPoint makeTestPointWithDistributor(int producersPerTopic, int consumersPerGroup,
                                                      double producerRate, int procMsMin, int procMsMax,
                                                      MessageDistributorType distributor) {
        return new TestPoint(
                "default", 0, 1, "default", 0, 1,
                1, 4,
                producersPerTopic, 1, consumersPerGroup,
                1000, producerRate, null,
                distributor, 1.0,
                ConsumerType.CONSUMER_GROUP,
                new KafkaConfig(),
                procMsMin, procMsMax, 1,
                ConsumerProcMode.BLOCKING,
                10000, // consumerProcCapacity
                null, null,
                new dev.dimster.engines.run.RunConfig(),
                DatasetResolver.payloadDataset(1000 - 1, 0.5, 100, null));
    }

    /** Variant of makeTopology with PINNED_PARTITIONS pinning (one partition per producer). */
    private VirtualTopology makePinnedTopology(int producersPerTopic) {
        Map<String, Integer> topics = new LinkedHashMap<>();
        topics.put("dimster-topic-0", 4);
        java.util.List<VirtualProducer> producers = new java.util.ArrayList<>();
        for (int i = 0; i < producersPerTopic; i++) {
            producers.add(new VirtualProducer("http://worker-0:8080", "dimster-topic-0",
                    i, new int[]{i % 4}));
        }
        java.util.List<VirtualConsumer> consumers = new java.util.ArrayList<>();
        consumers.add(new VirtualConsumer("http://worker-0:8080", "dimster-topic-0",
                "dimster-grp-0", 0, List.of()));
        return new VirtualTopology(topics, producers, consumers,
                ConsumerType.CONSUMER_GROUP, 0, 0, 1,
                ConsumerProcMode.BLOCKING,
                10000, // consumerProcCapacity
                MessageDistributorType.PINNED_PARTITIONS, 1.0, 1);
    }

    /** Build a fixture VirtualTopology matching the TestPoint above. */
    private VirtualTopology makeTopology(int producersPerTopic, int consumersPerGroup) {
        Map<String, Integer> topics = new LinkedHashMap<>();
        topics.put("dimster-topic-0", 4);
        java.util.List<VirtualProducer> producers = new java.util.ArrayList<>();
        for (int i = 0; i < producersPerTopic; i++) {
            producers.add(new VirtualProducer("http://worker-0:8080", "dimster-topic-0", i, new int[0]));
        }
        java.util.List<VirtualConsumer> consumers = new java.util.ArrayList<>();
        for (int i = 0; i < consumersPerGroup; i++) {
            consumers.add(new VirtualConsumer("http://worker-0:8080", "dimster-topic-0",
                    "dimster-grp-0", i, List.of()));
        }
        return new VirtualTopology(topics, producers, consumers,
                ConsumerType.CONSUMER_GROUP, 0, 0, 1,
                ConsumerProcMode.BLOCKING,
                10000, // consumerProcCapacity
                MessageDistributorType.NO_KEY, 1.0, 1);
    }

    private void primeEngineForFieldOverrides(WorkloadEngine engine,
                                                 int producersPerTopic, int consumersPerGroup,
                                                 double producerRate, int procMsMin, int procMsMax) throws Exception {
        setField(engine, "testPointActive", true);
        setField(engine, "testPhaseActive", true);
        setField(engine, "testPoint",
                makeTestPoint(producersPerTopic, consumersPerGroup, producerRate, procMsMin, procMsMax));
        setField(engine, "plannedTopology", makeTopology(producersPerTopic, consumersPerGroup));
    }

    private void setField(Object obj, String name, Object value) throws Exception {
        Field f = findField(obj.getClass(), name);
        f.setAccessible(true);
        f.set(obj, value);
    }

    private void setBooleanField(Object obj, String name, boolean value) throws Exception {
        Field f = findField(obj.getClass(), name);
        f.setAccessible(true);
        f.setBoolean(obj, value);
    }

    private Field findField(Class<?> cls, String name) throws NoSuchFieldException {
        for (Class<?> c = cls; c != null; c = c.getSuperclass()) {
            try { return c.getDeclaredField(name); } catch (NoSuchFieldException e) { /* keep looking */ }
        }
        throw new NoSuchFieldException(name + " in " + cls);
    }

    private static ModifyRequest workloadReq(Object... kvPairs) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < kvPairs.length; i += 2) {
            map.put((String) kvPairs[i], kvPairs[i + 1]);
        }
        return new ModifyRequest(null, null, null, null, null, null, map, null);
    }

    private static ModifyRequest kafkaReq(String dottedPath, Object value) {
        return new ModifyRequest(null, null, null, null, null, null, null, Map.of(dottedPath, value));
    }

    private static ModifyRequest restartConsumersReq(int count) {
        return new ModifyRequest(null, null, null, null, count, null, null, null);
    }

    private static ModifyRequest restartProducersReq(int count) {
        return new ModifyRequest(null, null, null, null, null, count, null, null);
    }

    @SuppressWarnings("unchecked")
    private List<RunResult.Modification> modifications(WorkloadEngine engine) throws Exception {
        Field field = WorkloadEngine.class.getDeclaredField("currentModifications");
        field.setAccessible(true);
        return (List<RunResult.Modification>) field.get(engine);
    }

    // ─── Validation: empty + no-test-point ────────────────────────────

    @Test
    public void rejectsWhenTestPointInactive() {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        ModifyResult r = engine.applyModification(workloadReq("producerRate", 1000.0));
        assertFalse(r.accepted());
        assertTrue("rejection should mention test point", r.message().contains("test point"));
    }

    @Test
    public void rejectsEmptyRequest() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        setBooleanField(engine, "testPointActive", true);
        ModifyResult r = engine.applyModification(new ModifyRequest(null, null, null, null, null, null, null, null));
        assertFalse(r.accepted());
    }

    @Test
    public void rejectsNullRequest() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        setBooleanField(engine, "testPointActive", true);
        ModifyResult r = engine.applyModification(null);
        assertFalse(r.accepted());
    }

    // ─── Action flags: terminate ──────────────────────────────────────

    @Test
    public void terminateAloneIsAccepted() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        setBooleanField(engine, "testPointActive", true);
        ModifyResult r = engine.applyModification(
                new ModifyRequest(true, null, null, null, null, null, null, null));
        assertTrue(r.message(), r.accepted());
        assertEquals(null, worker.lastRate);
        Field f = WorkloadEngine.class.getDeclaredField("terminateRequested");
        f.setAccessible(true);
        assertTrue(f.getBoolean(engine));
    }

    @Test
    public void rejectsTerminateCombinedWithFieldOverrides() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        setBooleanField(engine, "testPointActive", true);
        ModifyResult r = engine.applyModification(
                new ModifyRequest(true, null, null, null, null, null, Map.of("producerRate", 1000.0), null));
        assertFalse(r.accepted());
        assertTrue(r.message(), r.message().toLowerCase().contains("terminate"));
    }

    @Test
    public void rejectsTerminateCombinedWithDiscardStats() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        setBooleanField(engine, "testPointActive", true);
        ModifyResult r = engine.applyModification(
                new ModifyRequest(true, true, null, null, null, null, null, null));
        assertFalse(r.accepted());
    }

    @Test
    public void terminateRejectedWhenNoTestPointActive() {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        ModifyResult r = engine.applyModification(
                new ModifyRequest(true, null, null, null, null, null, null, null));
        assertFalse(r.accepted());
    }

    // ─── Action flags: discard-stats / run-for-minutes ────────────────

    @Test
    public void discardStatsRejectedDuringWarmup() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        setBooleanField(engine, "testPointActive", true);
        setBooleanField(engine, "testPhaseActive", false);
        ModifyResult r = engine.applyModification(
                new ModifyRequest(null, true, null, null, null, null, null, null));
        assertFalse(r.accepted());
        assertTrue(r.message(), r.message().toLowerCase().contains("test phase"));
    }

    @Test
    public void discardStatsAcceptedInTestPhase() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        setBooleanField(engine, "testPointActive", true);
        setBooleanField(engine, "testPhaseActive", true);
        ModifyResult r = engine.applyModification(
                new ModifyRequest(null, true, null, null, null, null, null, null));
        assertTrue(r.message(), r.accepted());
        Field flag = WorkloadEngine.class.getDeclaredField("discardStatsRequested");
        flag.setAccessible(true);
        assertTrue(flag.getBoolean(engine));
    }

    @Test
    public void discardStatsWithRunForMinutesSetsBoth() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        setBooleanField(engine, "testPointActive", true);
        setBooleanField(engine, "testPhaseActive", true);
        ModifyResult r = engine.applyModification(
                new ModifyRequest(null, true, 30, null, null, null, null, null));
        assertTrue(r.message(), r.accepted());
        Field ext = WorkloadEngine.class.getDeclaredField("runForMinutesRequested");
        ext.setAccessible(true);
        assertEquals(Integer.valueOf(30), ext.get(engine));
    }

    @Test
    public void acceptsRunForMinutesAlone() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        setBooleanField(engine, "testPointActive", true);
        setBooleanField(engine, "testPhaseActive", true);
        ModifyResult r = engine.applyModification(
                new ModifyRequest(null, null, 30, null, null, null, null, null));
        assertTrue(r.message(), r.accepted());
    }

    @Test
    public void rejectsRunForMinutesNonPositive() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        setBooleanField(engine, "testPointActive", true);
        setBooleanField(engine, "testPhaseActive", true);
        ModifyResult r = engine.applyModification(
                new ModifyRequest(null, true, 0, null, null, null, null, null));
        assertFalse(r.accepted());
    }

    @Test
    public void rejectsDiscardStatsCombinedWithFieldOverrides() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        setBooleanField(engine, "testPointActive", true);
        setBooleanField(engine, "testPhaseActive", true);
        ModifyResult r = engine.applyModification(
                new ModifyRequest(null, true, null, null, null, null, Map.of("producerRate", 1000.0), null));
        assertFalse(r.accepted());
    }

    // ─── Action flags: random client restarts ──────────────────────────

    @Test
    public void randomConsumerRestartCallsWorkerAndRecordsSelectedNames() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        primeEngineForFieldOverrides(engine, 3, 4, 1000.0, 0, 0);

        ModifyResult result = engine.applyModification(restartConsumersReq(2));

        assertTrue(result.message(), result.accepted());
        assertEquals(2, worker.lastRestartedConsumers.consumers().size());
        RunResult.Modification modification = modifications(engine).get(0);
        assertEquals(Integer.valueOf(2), modification.restartConsumersCount);
        assertEquals(2, modification.restartedConsumerNames.size());
        assertEquals(2, new java.util.HashSet<>(modification.restartedConsumerNames).size());
    }

    @Test
    public void randomProducerRestartCallsWorkerAndRecordsSelectedNames() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        primeEngineForFieldOverrides(engine, 4, 2, 1000.0, 0, 0);

        ModifyResult result = engine.applyModification(restartProducersReq(3));

        assertTrue(result.message(), result.accepted());
        assertEquals(3, worker.lastRestartedProducers.size());
        RunResult.Modification modification = modifications(engine).get(0);
        assertEquals(Integer.valueOf(3), modification.restartProducersCount);
        assertEquals(3, modification.restartedProducerNames.size());
        assertEquals(3, new java.util.HashSet<>(modification.restartedProducerNames).size());
    }

    @Test
    public void restartActionsMustBeSeparateAndTestPhaseOnly() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        primeEngineForFieldOverrides(engine, 3, 3, 1000.0, 0, 0);

        ModifyResult combined = engine.applyModification(
                new ModifyRequest(null, null, null, null, 1, 1, null, null));
        assertFalse(combined.accepted());
        assertTrue(combined.message(), combined.message().contains("separately"));

        setBooleanField(engine, "testPhaseActive", false);
        ModifyResult duringWarmup = engine.applyModification(restartProducersReq(1));
        assertFalse(duringWarmup.accepted());
        assertTrue(duringWarmup.message(), duringWarmup.message().contains("test phase"));
    }

    // ─── Field overrides: producerRate (hot-patch) ────────────────────

    @Test
    public void appliesProducerRate() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        primeEngineForFieldOverrides(engine, 1, 1, 1000.0, 0, 0);

        ModifyResult r = engine.applyModification(workloadReq("producerRate", 0.0));
        assertTrue(r.message(), r.accepted());
        assertEquals(Double.valueOf(0.0), worker.lastRate);

        ModifyResult r2 = engine.applyModification(workloadReq("producerRate", 1234.5));
        assertTrue(r2.accepted());
        assertEquals(Double.valueOf(1234.5), worker.lastRate);
    }

    @Test
    public void appliesProducerRateScheduleAndFixedRateClearsIt() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        primeEngineForFieldOverrides(engine, 1, 1, 1000.0, 0, 0);

        ProducerRateSchedule schedule = new ProducerRateSchedule();
        schedule.type = ProducerRateSchedule.Type.OSCILLATING;
        schedule.minRate = 100.0;
        schedule.maxRate = 10_000.0;
        schedule.periodSeconds = 60.0;

        ModifyResult scheduled = engine.applyModification(workloadReq("producerRateSchedule", schedule));
        assertTrue(scheduled.message(), scheduled.accepted());
        assertSame(schedule, worker.lastRateSchedule);
        assertNull(worker.lastRate);

        ModifyResult fixed = engine.applyModification(workloadReq("producerRate", 2_000.0));
        assertTrue(fixed.message(), fixed.accepted());
        assertEquals(Double.valueOf(2_000.0), worker.lastRate);
    }

    @Test
    public void appliesConsumerProcMs() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        primeEngineForFieldOverrides(engine, 1, 1, 1000.0, 0, 0);

        ModifyResult r = engine.applyModification(
                workloadReq("consumerProcMsMin", 10, "consumerProcMsMax", 50));
        assertTrue(r.message(), r.accepted());
        assertEquals(Integer.valueOf(10), worker.lastProcMin);
        assertEquals(Integer.valueOf(50), worker.lastProcMax);
    }

    @Test
    public void rejectsNegativeRate() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        primeEngineForFieldOverrides(engine, 1, 1, 1000.0, 0, 0);
        ModifyResult r = engine.applyModification(workloadReq("producerRate", -1.0));
        assertFalse(r.accepted());
    }

    @Test
    public void rejectsMinGreaterThanMax() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        primeEngineForFieldOverrides(engine, 1, 1, 1000.0, 5, 5);
        ModifyResult r = engine.applyModification(
                workloadReq("consumerProcMsMin", 100, "consumerProcMsMax", 50));
        assertFalse(r.accepted());
    }

    // ─── Field overrides: producersPerTopic add / remove ──────────────

    @Test
    public void addProducersOnGrowth() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        primeEngineForFieldOverrides(engine, /*producersPerTopic*/ 2, 1, 1000.0, 0, 0);

        ModifyResult r = engine.applyModification(workloadReq("producersPerTopic", 3));
        assertTrue(r.message(), r.accepted());
        assertTrue("addProducers should have been called",
                worker.lastAddedProducers != null && worker.lastAddedProducers.size() == 1);
        assertEquals(2, worker.lastAddedProducers.get(0).producerIdx());
    }

    @Test
    public void removeProducersOnShrink() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        primeEngineForFieldOverrides(engine, /*producersPerTopic*/ 3, 1, 1000.0, 0, 0);

        ModifyResult r = engine.applyModification(workloadReq("producersPerTopic", 2));
        assertTrue(r.message(), r.accepted());
        assertTrue("removeProducers should have been called",
                worker.lastRemovedProducers != null && worker.lastRemovedProducers.size() == 1);
        assertEquals("producer-dimster-topic-0-i2", worker.lastRemovedProducers.get(0));
    }

    // ─── Field overrides: kafkaConfig restart ─────────────────────────

    @Test
    public void producerConfigChangeRestartsProducers() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        primeEngineForFieldOverrides(engine, 2, 1, 1000.0, 0, 0);

        ModifyResult r = engine.applyModification(
                kafkaReq("producerConfig.batch.size", 65536));
        assertTrue(r.message(), r.accepted());
        // Config pushed first, then producers restarted with that new config.
        assertTrue("setKafkaConfig should have been called", worker.lastKafkaConfig != null);
        assertEquals(65536, worker.lastKafkaConfig.producerConfig.get("batch.size"));
        assertTrue("restartProducers should have been called",
                worker.lastRestartedProducers != null
                        && worker.lastRestartedProducers.size() == 2);
    }

    @Test
    public void messageSizeChangeRestartsProducersWithNewPayloads() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        primeEngineForFieldOverrides(engine, /*producersPerTopic*/ 2, 1, 1000.0, 0, 0);

        ModifyResult r = engine.applyModification(workloadReq("messageSize", 2000));
        assertTrue(r.message(), r.accepted());
        // The payload dataset at the new size is set first, so the restart picks it up.
        assertTrue("the new payload dataset should be set before producers restart",
                worker.datasetAtRestart != null && worker.datasetAtRestart.isPayloadDataset());
        assertEquals(List.of(1999, 1999), List.of(worker.datasetAtRestart.columns().get(0).minLength(),
                worker.datasetAtRestart.columns().get(0).maxLength()));
        // Then producers restarted (existing ones, both kept).
        assertTrue("restartProducers should have been called",
                worker.lastRestartedProducers != null
                        && worker.lastRestartedProducers.size() == 2);
    }

    @Test
    public void classicConsumerConfigChangeRestartsConsumers() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        primeEngineForFieldOverrides(engine, 1, 2, 1000.0, 0, 0);

        ModifyResult r = engine.applyModification(
                kafkaReq("classicConsumerConfig.max.poll.records", 200));
        assertTrue(r.message(), r.accepted());
        assertTrue("restartConsumers should have been called",
                worker.lastRestartedConsumers != null
                        && worker.lastRestartedConsumers.consumers().size() == 2);
    }

    // ─── Disruptive validation: rejected for now ──────────────────────

    @Test
    public void rejectsTopicCountChange() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        primeEngineForFieldOverrides(engine, 1, 1, 1000.0, 0, 0);
        ModifyResult r = engine.applyModification(workloadReq("topics", 2));
        assertFalse(r.accepted());
        assertTrue(r.message(), r.message().toLowerCase().contains("topics"));
    }

    @Test
    public void rejectsPartitionsPerTopicChange() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        primeEngineForFieldOverrides(engine, 1, 1, 1000.0, 0, 0);
        ModifyResult r = engine.applyModification(workloadReq("partitionsPerTopic", 8));
        assertFalse(r.accepted());
    }

    // ─── Field overrides: messageDistributor change ───────────────────

    @Test
    public void distributorChangeNoKeyToPinnedAddsPinning() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        // Default fixture: messageDistributor=NO_KEY, partitionsPerTopic=4,
        // 4 producers (idx 0..3), no pinning.
        primeEngineForFieldOverrides(engine, /*producersPerTopic*/ 4, 1, 1000.0, 0, 0);

        ModifyResult r = engine.applyModification(
                workloadReq("messageDistributor", "PINNED_PARTITIONS"));
        assertTrue(r.message(), r.accepted());

        // RemoveProducers fired with all 4 names.
        assertTrue("removeProducers should have been called",
                worker.lastRemovedProducers != null
                        && worker.lastRemovedProducers.size() == 4);
        // The full new producer set (4 with pinning) was created.
        assertTrue("createProducers should have been called",
                worker.lastCreatedProducers != null
                        && worker.lastCreatedProducers.size() == 4);
        // Each producer pinned to exactly one partition (4 producers / 4 partitions).
        for (int i = 0; i < 4; i++) {
            int[] pinning = worker.lastCreatedProducers.get(i).pinnedPartitions();
            assertEquals("producer " + i + " pinning length", 1, pinning.length);
            assertEquals("producer " + i + " pinned to partition " + i, i, pinning[0]);
        }
        // StartLoad fired with the new distributor.
        assertTrue("startLoad should have been called",
                worker.lastStartLoadAssignment != null);
        assertEquals("PINNED_PARTITIONS",
                worker.lastStartLoadAssignment.messageDistributorType().name());
    }

    @Test
    public void distributorChangePinnedToNoKeyClearsPinning() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        // Override the fixture's distributor + pinning so we start in the
        // PINNED_PARTITIONS state. We do that by replacing the test point's
        // current distributor and seeding the topology with pinned producers.
        primeEngineForFieldOverrides(engine, 4, 1, 1000.0, 0, 0);
        // Re-prime current state to PINNED_PARTITIONS with pinning.
        setField(engine, "testPoint",
                makeTestPointWithDistributor(4, 1, 1000.0, 0, 0,
                        dev.dimster.utils.distributor.MessageDistributorType.PINNED_PARTITIONS));
        setField(engine, "plannedTopology",
                makePinnedTopology(4));

        ModifyResult r = engine.applyModification(
                workloadReq("messageDistributor", "NO_KEY"));
        assertTrue(r.message(), r.accepted());

        assertTrue("createProducers should have been called",
                worker.lastCreatedProducers != null
                        && worker.lastCreatedProducers.size() == 4);
        for (int i = 0; i < 4; i++) {
            assertEquals("producer " + i + " should have empty pinning",
                    0, worker.lastCreatedProducers.get(i).pinnedPartitions().length);
        }
        assertEquals("NO_KEY",
                worker.lastStartLoadAssignment.messageDistributorType().name());
    }

    @Test
    public void distributorChangeBetweenNonPinnedTypesRestartsWithoutPinning() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        primeEngineForFieldOverrides(engine, 2, 1, 1000.0, 0, 0);

        ModifyResult r = engine.applyModification(
                workloadReq("messageDistributor", "KEY_ROUND_ROBIN"));
        assertTrue(r.message(), r.accepted());

        // Producers restarted but pinning stays empty (NO_KEY → KEY_ROUND_ROBIN,
        // both non-pinning).
        assertTrue("createProducers should have been called",
                worker.lastCreatedProducers != null
                        && worker.lastCreatedProducers.size() == 2);
        for (var vp : worker.lastCreatedProducers) {
            assertEquals(0, vp.pinnedPartitions().length);
        }
        assertEquals("KEY_ROUND_ROBIN",
                worker.lastStartLoadAssignment.messageDistributorType().name());
    }

    @Test
    public void producersPerTopicChangeUnderPinnedRecomputesAllPinning() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        // Start in PINNED_PARTITIONS state with 4 producers on 4 partitions
        // (each pinned to one). Reduce to 2 producers — every kept
        // producer's pinning must change (each now covers 2 partitions),
        // not just the dropped 2.
        primeEngineForFieldOverrides(engine, 4, 1, 1000.0, 0, 0);
        setField(engine, "testPoint",
                makeTestPointWithDistributor(4, 1, 1000.0, 0, 0,
                        MessageDistributorType.PINNED_PARTITIONS));
        setField(engine, "plannedTopology", makePinnedTopology(4));

        ModifyResult r = engine.applyModification(workloadReq("producersPerTopic", 2));
        assertTrue(r.message(), r.accepted());

        // Producer-subsystem restart path: removeProducers(all 4) then
        // createProducers(2) with new pinning, then startLoad.
        // Crucially, addProducers / a partial removeProducers should NOT
        // have fired — those would leave kept producers with stale pinning.
        assertEquals("removeProducers gets all 4 old names",
                4, worker.lastRemovedProducers.size());
        assertEquals("addProducers should not have fired",
                null, worker.lastAddedProducers);
        assertEquals("createProducers gets the 2 new producers",
                2, worker.lastCreatedProducers.size());
        // 2 producers / 4 partitions = 2 each.
        for (int i = 0; i < 2; i++) {
            assertEquals("producer " + i + " should pin 2 partitions",
                    2, worker.lastCreatedProducers.get(i).pinnedPartitions().length);
        }
    }

    @Test
    public void distributorChangeWithProducersPerTopicGrowBuildsFullNewSet() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        WorkloadEngine engine = newEngine(worker);
        primeEngineForFieldOverrides(engine, /*producersPerTopic*/ 2, 1, 1000.0, 0, 0);

        ModifyResult r = engine.applyModification(workloadReq(
                "messageDistributor", "PINNED_PARTITIONS",
                "producersPerTopic", 4));
        assertTrue(r.message(), r.accepted());

        // The CreateProducers carries the FULL new set (4 producers),
        // not just the 2 that grew. AddProducers should NOT have fired
        // — distributor change subsumes that path.
        assertEquals("createProducers gets the full new producer set",
                4, worker.lastCreatedProducers.size());
        assertEquals("addProducers should not have fired", null, worker.lastAddedProducers);
        // All 4 pinned (4 producers / 4 partitions).
        for (int i = 0; i < 4; i++) {
            assertEquals(1, worker.lastCreatedProducers.get(i).pinnedPartitions().length);
        }
    }

    @Test
    public void rejectsConsumerTypeChange() throws Exception {
        WorkloadEngine engine = newEngine(new RecordingWorker());
        primeEngineForFieldOverrides(engine, 1, 1, 1000.0, 0, 0);
        ModifyResult r = engine.applyModification(
                workloadReq("consumerType", "SHARE_GROUP"));
        assertFalse(r.accepted());
    }

    @Test
    public void surfacesWorkerFailureAsRejection() throws Exception {
        RecordingWorker worker = new RecordingWorker();
        worker.publishRateException = new IOException("simulated network failure");
        WorkloadEngine engine = newEngine(worker);
        primeEngineForFieldOverrides(engine, 1, 1, 1000.0, 0, 0);

        ModifyResult r = engine.applyModification(workloadReq("producerRate", 100.0));
        assertFalse(r.accepted());
    }
}
