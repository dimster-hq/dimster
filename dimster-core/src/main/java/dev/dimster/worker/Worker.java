package dev.dimster.worker;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

import dev.dimster.ProducerRateSchedule;
import dev.dimster.availability.AvailabilityConfig;
import dev.dimster.availability.AvailabilityResult;
import dev.dimster.availability.ProbeAssignment;
import dev.dimster.datagen.ResolvedDataset;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.engines.correctness.CorrectnessConfig;
import dev.dimster.engines.correctness.CorrectnessResult;
import dev.dimster.worker.commands.ClientHealth;
import dev.dimster.worker.commands.CountersStats;
import dev.dimster.worker.commands.PerPartitionStats;
import dev.dimster.worker.commands.PeriodStats;
import dev.dimster.worker.commands.ProducerWorkAssignment;
import dev.dimster.worker.topology.ConsumerSet;
import dev.dimster.worker.topology.VirtualProducer;

public interface Worker extends AutoCloseable {

    void setKafkaConfig(File configurationFile) throws IOException;

    void setKafkaConfig(KafkaConfig config) throws IOException;

    /**
     * Create one Kafka producer per {@link VirtualProducer}. The list is the
     * per-worker subset of the test point's {@code VirtualTopology}; the worker
     * applies the topology literally — partition pinning is read off each
     * {@code VirtualProducer.pinnedPartitions()}, no further routing decisions
     * are made on this side.
     */
    void createProducers(List<VirtualProducer> producers) throws IOException;

    /**
     * Create one Kafka consumer per {@link dev.dimster.worker.topology.VirtualConsumer}
     * in the {@link ConsumerSet}. Static partition assignments (correctness
     * mode) are taken straight off each virtual consumer; dynamic group
     * rebalance is used when the static partition list is empty.
     */
    void createConsumers(ConsumerSet consumers) throws IOException;

    void probeProducers() throws IOException;

    void startLoad(ProducerWorkAssignment producerWorkAssignment) throws IOException;

    void adjustPublishRate(double publishRate) throws IOException;

    /** Replace the active fixed limiter with a variable producer-rate schedule. */
    default void adjustPublishRateSchedule(ProducerRateSchedule schedule) throws IOException {}

    /** Replace the active fixed limiter with a schedule that starts at a shared wall-clock epoch. */
    default void adjustPublishRateSchedule(ProducerRateSchedule schedule, long scheduleStartEpochMillis) throws IOException {
        adjustPublishRateSchedule(schedule);
    }

    void pauseProducers() throws IOException;

    void resumeProducers() throws IOException;

    CountersStats getCountersStats() throws IOException;

    long getProbesReceived() throws IOException;

    PeriodStats getPeriodStats() throws IOException;

    /**
     * Snapshot period stats. The boolean controls only whether per-partition
     * interval histograms are serialized into the wire response when the worker
     * is remote — for LocalWorker the parameter is a no-op because per-partition
     * intervals are always populated in-memory (free, just Map references) so
     * the in-process PrometheusEndpoint can read them. The explore engine
     * passes true so it receives the e2e interval bytes from remote workers
     * for worst-partition detection; other engines use the no-arg overload to
     * keep wire payloads small.
     */
    default PeriodStats getPeriodStats(boolean includePerPartitionIntervals) throws IOException {
        return getPeriodStats();
    }

    void resetStats() throws IOException;

    void stopAll() throws IOException;

    boolean isStopComplete();

    default void awaitStopComplete(Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (!isStopComplete()) {
            if (System.currentTimeMillis() > deadline) {
                throw new RuntimeException("Timed out waiting for stop to complete after " + timeout);
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    ClientHealth getHealth();

    default void enableCorrectness(CorrectnessConfig config) {}

    default CorrectnessResult.TestOutcome getCorrectnessResult() { return null; }

    default void resetCorrectness() {}

    default void enableAvailability(AvailabilityConfig config) {}

    default void startAvailabilityProbes(List<ProbeAssignment> assignments) {}

    default AvailabilityResult.TestOutcome getAvailabilityResult() { return null; }

    default void enablePerPartitionMetrics() {}

    default void enablePerConsumerMetrics() {}

    default PerPartitionStats getPerPartitionStats() { return null; }

    default void setMaxConsumerRecreations(int max) {}

    /** Toggle per-record printing on producers and consumers. The coordinator
     *  is responsible for the rate-guard / Explore-engine policy; the worker
     *  side simply obeys. */
    default void setPrintMessages(boolean enabled) {}

    /** Toggle adding each record's key and contents to the printed lines
     *  (engineConfig.printContents); only has an effect while printing. */
    default void setPrintContents(boolean enabled) {}

    /**
     * Sets the dataset that producers generate records from: the workload's
     * dataset, or the default payload dataset. Producers and consumers both
     * depend on it (consumers find the message type through the dataset's
     * record format), so the engine calls this before creating them, and
     * before {@link #probeProducers()}. A live messageSize change sets a new
     * payload dataset and then restarts producers, which pick it up.
     */
    default void setDataset(ResolvedDataset dataset) throws IOException {}

    /** Adjust the simulated per-record processing time on all active consumers. */
    default void setConsumerProcessingTime(int minMs, int maxMs) {}

    /** Adjust the CONTINUOUS mode queue capacity threshold on all active consumers. */
    default void setConsumerProcCapacity(int capacity) {}

    // ─── Incremental topology operations (used by /modify) ─────────────
    //
    // These six methods let the coordinator apply a topology diff
    // mid-test without the disruption of stopAll + bulk recreate. Default
    // no-ops so engines and tests that don't drive the modify path stay
    // green — only the run engine's TestPointMutator pipeline calls them.
    //
    // Identity is by VirtualProducer.name() / VirtualConsumer.name() (e.g.
    // "producer-topic-i2") — those names are stable across topology
    // re-plans because TopologyPlanner derives them from (topic, idx).

    /**
     * Add the given producers without disturbing existing ones. Each new
     * producer is started immediately with the engine's current rate
     * limiter and message distributor. Caller must have already issued
     * setKafkaConfig() with whatever config the new producers should use.
     */
    default void addProducers(List<VirtualProducer> producers) throws IOException {}

    /**
     * Close and remove producers identified by their stable name (see
     * VirtualProducer.name()). Names that do not match any active producer
     * are silently ignored — the caller's diff has authority over what
     * should exist.
     */
    default void removeProducers(List<String> producerNames) throws IOException {}

    /**
     * Close and recreate the given producers in place. Used when a
     * configuration change requires a fresh KafkaProducer (producerConfig
     * change, distributor change, partition pinning change, etc.). The
     * recreated producer picks up the current setKafkaConfig() value, so
     * the caller must update the worker's stored config first.
     */
    default void restartProducers(List<VirtualProducer> producers) throws IOException {}

    /**
     * Add the given consumers without disturbing existing ones. Same
     * caveat as addProducers regarding setKafkaConfig() ordering.
     */
    default void addConsumers(ConsumerSet consumers) throws IOException {}

    /**
     * Close and remove consumers identified by their stable name. See
     * removeProducers for the identity rules.
     */
    default void removeConsumers(List<String> consumerNames) throws IOException {}

    /**
     * Close and recreate the given consumers in place. Used when a
     * configuration or consumer-type change requires a fresh KafkaConsumer.
     */
    default void restartConsumers(ConsumerSet consumers) throws IOException {}
}
