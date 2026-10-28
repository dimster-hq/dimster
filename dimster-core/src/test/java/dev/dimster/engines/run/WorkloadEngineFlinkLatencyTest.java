package dev.dimster.engines.run;

import dev.dimster.ConsumerProcMode;
import dev.dimster.ConsumerType;
import dev.dimster.ScenarioExpander.TestPoint;
import dev.dimster.TestResult;
import dev.dimster.flink.FlinkSpec;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.utils.distributor.MessageDistributorType;
import dev.dimster.worker.Worker;
import dev.dimster.worker.commands.ClientHealth;
import dev.dimster.worker.commands.CountersStats;
import dev.dimster.worker.commands.PeriodStats;
import dev.dimster.worker.commands.ProducerWorkAssignment;
import dev.dimster.worker.topology.ConsumerSet;
import dev.dimster.worker.topology.TopologyPlanner;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * The run engine reports the end-to-end latency split at the Flink job per
 * period and over the test, and only for test points with a Flink stage.
 */
public class WorkloadEngineFlinkLatencyTest {

    /** Every period: e2e 100 ms, split 30 ms producer → Flink and 70 ms Flink → consumer. */
    private static class FlinkLatencyWorker implements Worker {
        @Override
        public PeriodStats getPeriodStats() {
            PeriodStats stats = new PeriodStats();
            stats.messagesSent = 100;
            stats.messagesReceived = 100;
            stats.endToEndLatency.recordValue(100_000);
            stats.producerToFlinkLatency.recordValue(30_000);
            stats.flinkToConsumerLatency.recordValue(70_000);
            return stats;
        }

        @Override public CountersStats getCountersStats() { return new CountersStats(0, 0); }
        @Override public ClientHealth getHealth() { return ClientHealth.empty(); }
        @Override public void setKafkaConfig(File f) {}
        @Override public void setKafkaConfig(KafkaConfig c) {}
        @Override public void createProducers(List p) {}
        @Override public void createConsumers(ConsumerSet c) {}
        @Override public void probeProducers() {}
        @Override public void startLoad(ProducerWorkAssignment a) {}
        @Override public void adjustPublishRate(double r) {}
        @Override public void pauseProducers() {}
        @Override public void resumeProducers() {}
        @Override public long getProbesReceived() { return 0; }
        @Override public void resetStats() {}
        @Override public void stopAll() {}
        @Override public boolean isStopComplete() { return true; }
        @Override public void close() {}
    }

    @Test
    public void flinkTestPointsReportTheSplit() throws Exception {
        FlinkSpec flink = new FlinkSpec();
        flink.sql = "SELECT * FROM source";
        TestResult.PerformanceResults r = collectStats(flink).results;

        assertFalse(r.producerToFlinkLatency.p99.isEmpty());
        assertEquals(r.endToEndLatency99pct.size(), r.producerToFlinkLatency.p99.size());
        assertEquals(30.0, r.producerToFlinkLatency.p99.get(0), 0.1);
        assertEquals(70.0, r.flinkToConsumerLatency.p99.get(0), 0.1);
        assertEquals(30.0, r.aggregatedProducerToFlinkLatency.p99, 0.1);
        assertEquals(70.0, r.aggregatedFlinkToConsumerLatency.p99, 0.1);
        // End-to-end latency itself is unchanged.
        assertEquals(100.0, r.aggregatedEndToEndLatency99pct, 0.1);
    }

    @Test
    public void otherTestPointsDoNot() throws Exception {
        TestResult.PerformanceResults r = collectStats(null).results;
        assertNull(r.producerToFlinkLatency);
        assertNull(r.aggregatedFlinkToConsumerLatency);
    }

    private static TestResult collectStats(FlinkSpec flink) throws Exception {
        Workload workload = new Workload();
        workload.name = "test";
        TopologyPlanner planner = new TopologyPlanner(List.of("http://worker-0:8080"),
                TopologyPlanner.WorkerMode.MIXED, new TopologyPlanner.RatioSplit(1, 1));
        WorkloadEngine engine = new WorkloadEngine(workload, new FlinkLatencyWorker(), planner, null,
                new AtomicBoolean(false));

        RunConfig runConfig = new RunConfig();
        runConfig.sampleRateMillis = 100;
        TestPoint point = new TestPoint("default", 0, 1, "default", 0, 1,
                1, 1, 1, 1, 1, 100, 1000, null, MessageDistributorType.NO_KEY, 1.0,
                ConsumerType.CONSUMER_GROUP, new KafkaConfig(), 0, 0, 1, ConsumerProcMode.BLOCKING, 10000,
                null, null, runConfig, null, flink);
        setField(engine, "testPoint", point);
        // Used only for the progress line in the period log.
        setField(engine, "runProgress", new WorkloadEngine.RunProgress(1, 0, 0, 1));

        Method m = WorkloadEngine.class.getDeclaredMethod("collectStats", Duration.class, boolean.class);
        m.setAccessible(true);
        return (TestResult) m.invoke(engine, Duration.ofMillis(350), true);
    }

    private static void setField(WorkloadEngine engine, String name, Object value) throws Exception {
        Field f = WorkloadEngine.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(engine, value);
    }
}
