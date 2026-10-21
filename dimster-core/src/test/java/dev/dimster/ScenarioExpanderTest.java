package dev.dimster;

import dev.dimster.ScenarioExpander.TestPoint;
import dev.dimster.engines.explore.ExploreDimensions;
import dev.dimster.engines.explore.ExploreWorkload.ExploreConfig;
import dev.dimster.engines.run.RunConfig;
import dev.dimster.engines.run.RunDimensions;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.utils.distributor.MessageDistributorType;

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests for ScenarioExpander, focusing on KafkaConfig merging across
 * all combinations of file config, embedded base config, scenario config,
 * and per-test-point (varying) config.
 */
public class ScenarioExpanderTest {

    // ─── Helpers ──────────────────────────────────────────────────────

    private static BaseWorkload baseWorkloadWithoutKafkaConfig() {
        var bw = new BaseWorkload();
        bw.topics = 1;
        bw.partitionsPerTopic = 6;
        bw.producersPerTopic = 2;
        bw.consumerGroupsPerTopic = 1;
        bw.consumersPerGroup = 3;
        bw.messageSize = 1024;
        bw.producerRate = 10000;
        bw.messageDistributor = MessageDistributorType.NO_KEY;
        bw.consumerType = ConsumerType.CONSUMER_GROUP;
        return bw;
    }

    private static BaseWorkload baseWorkloadWithKafkaConfig(KafkaConfig kConfig) {
        var bw = baseWorkloadWithoutKafkaConfig();
        bw.kafkaConfig = kConfig;
        return bw;
    }

    private static Scenario emptyScenario(String name) {
        var s = new Scenario();
        s.name = name;
        return s;
    }

    private static KafkaConfig kafkaConfig(String name, short replicationFactor,
                                            Map<String, Object> commonConfig,
                                            Map<String, Object> topicConfig,
                                            Map<String, Object> producerConfig,
                                            Map<String, Object> classicConsumerConfig,
                                            Map<String, Object> shareConsumerConfig) {
        var kc = new KafkaConfig();
        kc.name = name;
        kc.replicationFactor = replicationFactor;
        kc.commonConfig = commonConfig;
        kc.topicConfig = topicConfig;
        kc.producerConfig = producerConfig;
        kc.classicConsumerConfig = classicConsumerConfig;
        kc.shareConsumerConfig = shareConsumerConfig;
        return kc;
    }

    private static Map<String, Object> map(String key, Object value) {
        var m = new LinkedHashMap<String, Object>();
        m.put(key, value);
        return m;
    }

    private static Map<String, Object> map(String k1, Object v1, String k2, Object v2) {
        var m = new LinkedHashMap<String, Object>();
        m.put(k1, v1);
        m.put(k2, v2);
        return m;
    }

    // ─── No KafkaConfig at all ────────────────────────────────────────

    @Test
    public void noKafkaConfigAtAll_noScenarios() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        List<TestPoint> points = ScenarioExpander.expand(base, null, null);

        assertEquals(1, points.size());
        assertNull(points.get(0).kafkaConfig());
        assertEquals("default", points.get(0).scenarioName());
    }

    @Test
    public void noKafkaConfigAtAll_oneScenario() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        Scenario s = emptyScenario("test");
        s.partitionsPerTopic = List.of(12);

        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), null);

        assertEquals(1, points.size());
        assertNull(points.get(0).kafkaConfig());
        assertEquals(12, points.get(0).partitionsPerTopic());
    }

    @Test
    public void fileKafkaConfigOnly() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        KafkaConfig file = kafkaConfig("file-config", (short) 3,
                map("bootstrap.servers", "kafka:9092"),
                map("min.insync.replicas", 2),
                map("acks", "all"),
                map("auto.offset.reset", "earliest"),
                null);

        Scenario s = emptyScenario("test");
        s.partitionsPerTopic = List.of(12);
        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), file);

        assertEquals(1, points.size());
        KafkaConfig result = points.get(0).kafkaConfig();
        assertNotNull(result);
        assertEquals("file-config", result.name);
        assertEquals(3, result.replicationFactor);
        assertEquals("kafka:9092", result.commonConfig.get("bootstrap.servers"));
        assertEquals(2, result.topicConfig.get("min.insync.replicas"));
        assertEquals("all", result.producerConfig.get("acks"));
        assertEquals("earliest", result.classicConsumerConfig.get("auto.offset.reset"));
        assertNull(result.shareConsumerConfig);
    }

    @Test
    public void baseKafkaConfigOnly() {
        KafkaConfig baseConfig = kafkaConfig("file-config", (short) 3,
                map("bootstrap.servers", "kafka:9092"),
                map("min.insync.replicas", 2),
                map("acks", "all"),
                map("auto.offset.reset", "earliest"),
                null);

        BaseWorkload base = baseWorkloadWithKafkaConfig(baseConfig);

        Scenario s = emptyScenario("test");
        s.partitionsPerTopic = List.of(12);
        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), null);

        assertEquals(1, points.size());
        KafkaConfig result = points.get(0).kafkaConfig();
        assertNotNull(result);
        assertEquals(3, result.replicationFactor);
        assertEquals("kafka:9092", result.commonConfig.get("bootstrap.servers"));
        assertEquals(2, result.topicConfig.get("min.insync.replicas"));
        assertEquals("all", result.producerConfig.get("acks"));
        assertEquals("earliest", result.classicConsumerConfig.get("auto.offset.reset"));
        assertNull(result.shareConsumerConfig);
    }

    @Test
    public void fileAndBaseKafkaConfig() {
        short overriddenRF = 1;
        int overiddenMinISR = 1;
        String overriddenAcks = "1";
        boolean overridenAutoCommit = false;
        String overridenAcquireMode = "record_limit";
        KafkaConfig baseConfig = kafkaConfig("base-config", overriddenRF,
                map("bootstrap.servers", "kafka:9092"),
                map("min.insync.replicas", overiddenMinISR),
                map("acks", overriddenAcks, "enable.idempotence", true),
                map("enable.auto.commit", overridenAutoCommit),
                map("share.acquire.mode", overridenAcquireMode));
        BaseWorkload base = baseWorkloadWithKafkaConfig(baseConfig);

        Scenario s = emptyScenario("test");
        s.partitionsPerTopic = List.of(12);

        KafkaConfig file = kafkaConfig("file-config", (short) 1,
                map("bootstrap.servers", "kafka:9092"),
                map("min.insync.replicas", 2),
                map("acks", "all", "enable.idempotence", true),
                map("enable.auto.commit", true, "max.poll.records", 500),
                map("share.acquire.mode", "batch_optimized", "max.poll.records", 500));

        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), file);

        assertEquals(1, points.size());
        KafkaConfig result = points.get(0).kafkaConfig();
        assertNotNull(result);
        assertEquals(overriddenRF, result.replicationFactor);
        assertEquals("kafka:9092", result.commonConfig.get("bootstrap.servers"));
        assertEquals(overiddenMinISR, result.topicConfig.get("min.insync.replicas"));
        assertEquals(overriddenAcks, result.producerConfig.get("acks"));
        assertEquals(true, result.producerConfig.get("enable.idempotence"));
        assertEquals(overridenAutoCommit, result.classicConsumerConfig.get("enable.auto.commit"));
        assertEquals(500, result.classicConsumerConfig.get("max.poll.records"));
        assertEquals(overridenAcquireMode, result.shareConsumerConfig.get("share.acquire.mode"));
        assertEquals(500, result.shareConsumerConfig.get("max.poll.records"));
        assertEquals(12, points.get(0).partitionsPerTopic());
    }

    @Test
    public void fileAndBaseAndScenarioKafkaConfig() {
        short overriddenRF_1 = 1;
        int overiddenMinISR_1 = 1;
        String overriddenAcks_1 = "1";
        boolean overridenAutoCommit_1 = false;
        String overridenAcquireMode_1 = "record_limit";
        KafkaConfig baseConfig = kafkaConfig("base-config", overriddenRF_1,
                map("bootstrap.servers", "kafka:9092"),
                map("min.insync.replicas", overiddenMinISR_1),
                map("acks", overriddenAcks_1, "enable.idempotence", true),
                map("enable.auto.commit", overridenAutoCommit_1),
                map("share.acquire.mode", overridenAcquireMode_1));
        BaseWorkload base = baseWorkloadWithKafkaConfig(baseConfig);

        Scenario s = emptyScenario("test");
        s.partitionsPerTopic = List.of(12);

        short overriddenRF_2 = 2;
        String overiddenMaxPollRecords_2 = "100";
        boolean overridenEOS_2 = false;
        s.kafkaConfig = kafkaConfig("scenario-config", overriddenRF_2,
                null,
                null,
                map("enable.idempotence", overridenEOS_2),
                map("max.poll.records", overiddenMaxPollRecords_2),
                map("max.poll.records", overiddenMaxPollRecords_2));

        KafkaConfig file = kafkaConfig("file-config", (short) 1,
                map("bootstrap.servers", "kafka:9092"),
                map("min.insync.replicas", 2),
                map("acks", "all", "enable.idempotence", true),
                map("enable.auto.commit", true, "max.poll.records", 500),
                map("share.acquire.mode", "batch_optimized", "max.poll.records", 500));

        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), file);

        assertEquals(1, points.size());
        KafkaConfig result = points.get(0).kafkaConfig();
        assertNotNull(result);
        assertEquals(overriddenRF_2, result.replicationFactor);
        assertEquals("kafka:9092", result.commonConfig.get("bootstrap.servers"));
        assertEquals(overiddenMinISR_1, result.topicConfig.get("min.insync.replicas"));
        assertEquals(overriddenAcks_1, result.producerConfig.get("acks"));
        assertEquals(overridenEOS_2, result.producerConfig.get("enable.idempotence"));
        assertEquals(overridenAutoCommit_1, result.classicConsumerConfig.get("enable.auto.commit"));
        assertEquals(overiddenMaxPollRecords_2, result.classicConsumerConfig.get("max.poll.records"));
        assertEquals(overridenAcquireMode_1, result.shareConsumerConfig.get("share.acquire.mode"));
        assertEquals(overiddenMaxPollRecords_2, result.shareConsumerConfig.get("max.poll.records"));
        assertEquals(12, points.get(0).partitionsPerTopic());
    }

    // ─── Map helper for 3 entries ─────────────────────────────────────

    private static Map<String, Object> map(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        var m = new LinkedHashMap<String, Object>();
        m.put(k1, v1);
        m.put(k2, v2);
        m.put(k3, v3);
        return m;
    }

    // ─── ExploreConfig per-scenario overrides ────────────────────────

    @Test
    public void exploreDefaults_noScenarioOverride() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        ExploreConfig defaults = new ExploreConfig();
        defaults.e2eLatencyThresholdMs = 100;
        defaults.stabilizeTimeoutSeconds = 300;

        Scenario s = emptyScenario("test");
        s.partitionsPerTopic = List.of(12);

        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), null, defaults);

        assertEquals(1, points.size());
        ExploreConfig result = points.get(0).exploreConfig();
        assertNotNull(result);
        assertEquals(100, result.e2eLatencyThresholdMs, 0.001);
        assertEquals(300, result.stabilizeTimeoutSeconds);
        // Other fields should be at their ExploreConfig defaults
        assertEquals(75, result.e2eLatencyPercentile, 0.001);
        assertEquals(100, result.initialRate);
    }

    @Test
    public void exploreDefaults_scenarioScalarOverride() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        ExploreConfig defaults = new ExploreConfig();
        defaults.e2eLatencyThresholdMs = 50;
        defaults.searchDurationSeconds = 120;
        defaults.sustainDurationSeconds = 300;

        Scenario s = emptyScenario("test");
        s.partitionsPerTopic = List.of(12);
        // Override just two fields as scalars (single-element lists)
        s.exploreConfig = new ExploreDimensions();
        s.exploreConfig.e2eLatencyThresholdMs = List.of(25.0);
        s.exploreConfig.stabilizeTimeoutSeconds = List.of(600);

        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), null, defaults);

        assertEquals(1, points.size());
        ExploreConfig result = points.get(0).exploreConfig();
        // Overridden fields
        assertEquals(25.0, result.e2eLatencyThresholdMs, 0.001);
        assertEquals(600, result.stabilizeTimeoutSeconds);
        // Non-overridden fields inherit from defaults
        assertEquals(120, result.searchDurationSeconds);
        assertEquals(300, result.sustainDurationSeconds);
    }

    @Test
    public void exploreDefaults_scenarioArrayOverride() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        ExploreConfig defaults = new ExploreConfig();
        defaults.e2eLatencyThresholdMs = 50;
        defaults.stabilizeTimeoutSeconds = 120;

        Scenario s = emptyScenario("test");
        s.partitionsPerTopic = List.of(6, 12);
        s.exploreConfig = new ExploreDimensions();
        s.exploreConfig.e2eLatencyThresholdMs = List.of(25.0, 100.0);

        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), null, defaults);

        assertEquals(2, points.size());
        // First test point
        assertEquals(6, points.get(0).partitionsPerTopic());
        assertEquals(25.0, points.get(0).exploreConfig().e2eLatencyThresholdMs, 0.001);
        assertEquals(120, points.get(0).exploreConfig().stabilizeTimeoutSeconds);
        // Second test point
        assertEquals(12, points.get(1).partitionsPerTopic());
        assertEquals(100.0, points.get(1).exploreConfig().e2eLatencyThresholdMs, 0.001);
        assertEquals(120, points.get(1).exploreConfig().stabilizeTimeoutSeconds);
    }

    @Test
    public void exploreDefaults_scenarioInitialRateOverride() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        ExploreConfig defaults = new ExploreConfig();
        defaults.initialRate = 10000;

        Scenario s = emptyScenario("test");
        s.exploreConfig = new ExploreDimensions();
        s.exploreConfig.initialRate = List.of(99);

        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), null, defaults);

        assertEquals(1, points.size());
        assertEquals(99, points.get(0).exploreConfig().initialRate);
    }

    @Test(expected = IllegalArgumentException.class)
    public void exploreDefaults_dimensionMismatch() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        ExploreConfig defaults = new ExploreConfig();

        Scenario s = emptyScenario("test");
        s.exploreConfig = new ExploreDimensions();
        // Mismatched array sizes within exploreConfig dimensions
        s.exploreConfig.e2eLatencyThresholdMs = List.of(25.0, 50.0, 75.0);
        s.exploreConfig.stabilizeTimeoutSeconds = List.of(120, 300);

        ScenarioExpander.expand(base, List.of(s), null, defaults);
    }

    @Test
    public void exploreDefaults_noScenarios_fromBase() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        ExploreConfig defaults = new ExploreConfig();
        defaults.e2eLatencyThresholdMs = 42;

        List<TestPoint> points = ScenarioExpander.expand(base, null, null, defaults);

        assertEquals(1, points.size());
        assertNotNull(points.get(0).exploreConfig());
        assertEquals(42, points.get(0).exploreConfig().e2eLatencyThresholdMs, 0.001);
    }

    @Test
    public void noExploreDefaults_exploreConfigIsNull() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();

        Scenario s = emptyScenario("test");
        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), null);

        assertEquals(1, points.size());
        assertNull(points.get(0).exploreConfig());
    }

    // ─── RunConfig per-scenario overrides ────────────────────────────

    @Test
    public void runDefaults_noScenarioOverride() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        RunConfig defaults = new RunConfig();
        defaults.testDurationMinutes = 7;
        defaults.warmupDurationMinutes = 2;
        defaults.rampIntervals = 4;
        defaults.rampIntervalSeconds = 15;

        Scenario s = emptyScenario("test");
        s.partitionsPerTopic = List.of(12);

        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), null, null, null, defaults);

        assertEquals(1, points.size());
        RunConfig result = points.get(0).runConfig();
        assertNotNull(result);
        // All fields inherit from workload-level defaults.
        assertEquals(7, result.testDurationMinutes);
        assertEquals(2, result.warmupDurationMinutes);
        assertEquals(4, result.rampIntervals);
        assertEquals(15, result.rampIntervalSeconds);
        // Non-overridden fields fall back to RunConfig() defaults.
        assertEquals(10000, result.sampleRateMillis);
    }

    @Test
    public void runDefaults_scenarioArrayOverride() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        RunConfig defaults = new RunConfig();
        defaults.testDurationMinutes = 5;
        defaults.warmupDurationMinutes = 1;
        defaults.rampIntervals = 3;
        defaults.rampIntervalSeconds = 20;

        Scenario s = emptyScenario("test");
        s.partitionsPerTopic = List.of(6, 12);
        s.runConfig = new RunDimensions();
        s.runConfig.rampIntervals = List.of(3, 10);
        s.runConfig.rampIntervalSeconds = List.of(10, 30);

        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), null, null, null, defaults);

        assertEquals(2, points.size());
        // First test point — swept ramp values, other run fields inherited from defaults.
        assertEquals(6, points.get(0).partitionsPerTopic());
        assertEquals(3, points.get(0).runConfig().rampIntervals);
        assertEquals(10, points.get(0).runConfig().rampIntervalSeconds);
        assertEquals(5, points.get(0).runConfig().testDurationMinutes);
        assertEquals(1, points.get(0).runConfig().warmupDurationMinutes);
        // Second test point.
        assertEquals(12, points.get(1).partitionsPerTopic());
        assertEquals(10, points.get(1).runConfig().rampIntervals);
        assertEquals(30, points.get(1).runConfig().rampIntervalSeconds);
        assertEquals(5, points.get(1).runConfig().testDurationMinutes);
    }

    @Test(expected = IllegalArgumentException.class)
    public void runDefaults_dimensionMismatch() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        RunConfig defaults = new RunConfig();

        Scenario s = emptyScenario("test");
        s.runConfig = new RunDimensions();
        // Mismatched array sizes within runConfig dimensions — must fail validation.
        s.runConfig.rampIntervals = List.of(3, 5, 10);
        s.runConfig.rampIntervalSeconds = List.of(10, 20);

        ScenarioExpander.expand(base, List.of(s), null, null, null, defaults);
    }

    @Test
    public void runDefaults_noScenarios_fromBase() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        RunConfig defaults = new RunConfig();
        defaults.testDurationMinutes = 42;

        List<TestPoint> points = ScenarioExpander.expand(base, null, null, null, null, defaults);

        assertEquals(1, points.size());
        assertNotNull(points.get(0).runConfig());
        assertEquals(42, points.get(0).runConfig().testDurationMinutes);
    }

    @Test
    public void noRunDefaults_runConfigIsNull() {
        // Confirms that engines other than `run` (which don't pass runDefaults)
        // get a null runConfig on each TestPoint — they don't read it.
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();

        Scenario s = emptyScenario("test");
        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), null);

        assertEquals(1, points.size());
        assertNull(points.get(0).runConfig());
    }

    @Test
    public void producerRateSchedule_inheritsFromBaseAndCanVaryByScenario() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        ProducerRateSchedule baseSchedule = oscillatingSchedule(1_000, 10_000);
        base.producerRateSchedule = baseSchedule;

        Scenario first = emptyScenario("inherits");
        List<TestPoint> inherited = ScenarioExpander.expand(base, List.of(first), null);
        assertSame(baseSchedule, inherited.get(0).producerRateSchedule());
        assertEquals(10_000.0, inherited.get(0).producerRate(), 0.0001);

        ProducerRateSchedule low = oscillatingSchedule(100, 1_000);
        ProducerRateSchedule high = oscillatingSchedule(1_000, 100_000);
        Scenario second = emptyScenario("varies");
        second.producerRateSchedule = List.of(low, high);
        second.producerRate = List.of(1_000.0, 100_000.0);

        List<TestPoint> varied = ScenarioExpander.expand(base, List.of(second), null);
        assertEquals(2, varied.size());
        assertSame(low, varied.get(0).producerRateSchedule());
        assertSame(high, varied.get(1).producerRateSchedule());
        assertEquals(1_000.0, varied.get(0).producerRate(), 0.0001);
        assertEquals(100_000.0, varied.get(1).producerRate(), 0.0001);
    }

    private static ProducerRateSchedule oscillatingSchedule(double min, double max) {
        ProducerRateSchedule schedule = new ProducerRateSchedule();
        schedule.type = ProducerRateSchedule.Type.OSCILLATING;
        schedule.minRate = min;
        schedule.maxRate = max;
        schedule.periodSeconds = 60;
        return schedule;
    }

    // ─── Datasets ─────────────────────────────────────────────────────

    private static dev.dimster.datagen.DatasetSpec dataset(String preset,
                                                           dev.dimster.datagen.RecordFormat format) {
        var spec = new dev.dimster.datagen.DatasetSpec();
        spec.preset = preset;
        spec.format = format;
        return spec;
    }

    @Test
    public void datasetIsAPerTestPointDimensionAndSetsTheEstimatedMessageSize() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        Scenario s = emptyScenario("formats");
        s.dataset = List.of(dataset("taxi", dev.dimster.datagen.RecordFormat.AVRO),
                dataset("taxi", dev.dimster.datagen.RecordFormat.JSON));

        List<TestPoint> points = ScenarioExpander.expand(base, List.of(s), null);

        assertEquals(2, points.size());
        assertEquals(dev.dimster.datagen.RecordFormat.AVRO, points.get(0).dataset().format());
        assertEquals(dev.dimster.datagen.RecordFormat.JSON, points.get(1).dataset().format());
        // messageSize is the estimated average record size, not the base 1024.
        int avro = points.get(0).messageSize();
        int json = points.get(1).messageSize();
        assertTrue("avro " + avro, avro > 50 && avro < 200);
        assertTrue("json " + json + " vs avro " + avro, json > 2 * avro);
    }

    @Test
    public void baseDatasetAppliesWithoutScenariosAndDefaultsToThePayloadDataset() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        TestPoint payload = ScenarioExpander.expand(base, null, null).get(0);
        assertTrue(payload.dataset().isPayloadDataset());
        assertEquals(dev.dimster.datagen.RecordFormat.RAW, payload.dataset().format());
        // A 1-byte header plus 1023 payload bytes: normal records are messageSize bytes.
        assertEquals(1023, payload.dataset().columns().get(0).maxLength());
        assertEquals(1024, payload.messageSize());

        base.dataset = dataset("clicks", null);
        TestPoint point = ScenarioExpander.expand(base, null, null).get(0);
        assertEquals("clicks", point.dataset().preset());
        assertEquals(dev.dimster.datagen.RecordFormat.AVRO, point.dataset().format());
    }

    @Test
    public void invalidDatasetCombinationsAreRejectedWithTheScenarioName() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();

        Scenario sizes = emptyScenario("sizes");
        sizes.dataset = List.of(dataset("clicks", null));
        sizes.messageSize = List.of(100, 200);
        assertRejected(base, sizes, "messageSize does not apply");

        Scenario keyWithoutDataset = emptyScenario("keys");
        keyWithoutDataset.messageDistributor = List.of(MessageDistributorType.KEY_COLUMN);
        assertRejected(base, keyWithoutDataset, "requires a dataset");

        Scenario keyColumnUnused = emptyScenario("unused");
        var withKey = dataset("clicks", null);
        withKey.keyColumn = "user_id";
        keyColumnUnused.dataset = List.of(withKey);
        assertRejected(base, keyColumnUnused, "must be used together");

        Scenario badColumn = emptyScenario("bad");
        var bad = dataset("clicks", null);
        bad.columns = new java.util.LinkedHashMap<>();
        var col = new dev.dimster.datagen.ColumnSpec();
        col.cardinality = 0L;
        bad.columns.put("country", col);
        badColumn.dataset = List.of(bad);
        assertRejected(base, badColumn, "dataset column 'country'");

        Scenario mismatched = emptyScenario("mismatched");
        mismatched.dataset = List.of(dataset("clicks", null), dataset("taxi", null));
        mismatched.partitionsPerTopic = List.of(1, 2, 3);
        assertRejected(base, mismatched, "dataset has 2 elements");
    }

    @Test
    public void keyColumnWithKeyColumnDistributorIsAccepted() {
        BaseWorkload base = baseWorkloadWithoutKafkaConfig();
        Scenario s = emptyScenario("keys");
        var withKey = dataset("clicks", null);
        withKey.keyColumn = "user_id";
        s.dataset = List.of(withKey);
        s.messageDistributor = List.of(MessageDistributorType.KEY_COLUMN);
        assertEquals("user_id", ScenarioExpander.expand(base, List.of(s), null).get(0).dataset().keyColumn());
    }

    private static void assertRejected(BaseWorkload base, Scenario s, String message) {
        try {
            ScenarioExpander.expand(base, List.of(s), null);
            fail("expected rejection: " + message);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains(message));
            assertTrue(e.getMessage(), e.getMessage().contains("'" + s.name + "'"));
        }
    }
}
