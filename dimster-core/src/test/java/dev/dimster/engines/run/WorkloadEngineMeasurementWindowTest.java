package dev.dimster.engines.run;

import dev.dimster.ConsumerProcMode;
import dev.dimster.ConsumerType;
import dev.dimster.ScenarioExpander.TestPoint;
import dev.dimster.TestResult;
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

/**
 * A phase's first period must count only messages sent during that phase.
 * Before the fix, messages sent between the previous phase's last sample and
 * the start of this phase (seconds of traffic on a slow cluster) were counted
 * in the first period but divided by its own duration, inflating its rate.
 */
public class WorkloadEngineMeasurementWindowTest {

    private static final double RATE = 1000.0;

    /**
     * Simulates a producer sending at RATE msg/s. Each period snapshot
     * returns the messages "sent" since the previous snapshot, in real time,
     * so a window that starts late or early shows up as a wrong rate.
     */
    private static class SteadyRateWorker implements Worker {
        private long lastSnapshotNanos = System.nanoTime();

        @Override
        public synchronized PeriodStats getPeriodStats() {
            long now = System.nanoTime();
            PeriodStats stats = new PeriodStats();
            stats.messagesSent = (long) (RATE * (now - lastSnapshotNanos) / 1e9);
            stats.bytesSent = stats.messagesSent * 100;
            stats.messagesReceived = stats.messagesSent;
            stats.bytesReceived = stats.bytesSent;
            lastSnapshotNanos = now;
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
    public void firstPeriodExcludesTrafficFromBeforeThePhase() throws Exception {
        SteadyRateWorker worker = new SteadyRateWorker();
        WorkloadEngine engine = newEngine(worker);

        // Two seconds of traffic accumulate before the phase starts, as when
        // the remote calls between warm-up and test are slow.
        Thread.sleep(2000);
        TestResult result = collectStats(engine, Duration.ofMillis(1200));

        // Sampling every 200 ms: with the gap counted, the first period would
        // report about (2.0 + 0.2) s of messages over 0.2 s, i.e. ~11x RATE.
        List<Double> rates = result.results.publishRate;
        for (double rate : rates) {
            assertEquals("per-period rates " + rates, RATE, rate, RATE * 0.25);
        }
        assertEquals(RATE * 100 / 1_000_000, result.results.publishBytesRate.get(0), 0.03);
    }

    private static TestResult collectStats(WorkloadEngine engine, Duration duration) throws Exception {
        Method m = WorkloadEngine.class.getDeclaredMethod("collectStats", Duration.class, boolean.class);
        m.setAccessible(true);
        return (TestResult) m.invoke(engine, duration, true);
    }

    private static WorkloadEngine newEngine(Worker worker) throws Exception {
        Workload workload = new Workload();
        workload.name = "test";
        TopologyPlanner planner = new TopologyPlanner(List.of("http://worker-0:8080"),
                TopologyPlanner.WorkerMode.MIXED, new TopologyPlanner.RatioSplit(1, 1));
        WorkloadEngine engine = new WorkloadEngine(workload, worker, planner, null, new AtomicBoolean(false));

        RunConfig runConfig = new RunConfig();
        runConfig.sampleRateMillis = 200;
        TestPoint point = new TestPoint("default", 0, 1, "default", 0, 1,
                1, 1, 1, 1, 1, 100, RATE, null, MessageDistributorType.NO_KEY, 1.0,
                ConsumerType.CONSUMER_GROUP, new KafkaConfig(), 0, 0, 1, ConsumerProcMode.BLOCKING, 10000,
                null, null, runConfig, null);
        setField(engine, "testPoint", point);
        // Used only for the progress line in the period log.
        setField(engine, "runProgress", new WorkloadEngine.RunProgress(1, 0, 0, 1));
        return engine;
    }

    private static void setField(WorkloadEngine engine, String name, Object value) throws Exception {
        Field f = WorkloadEngine.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(engine, value);
    }
}
