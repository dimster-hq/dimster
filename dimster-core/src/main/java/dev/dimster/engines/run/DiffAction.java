package dev.dimster.engines.run;

import java.io.IOException;
import java.util.List;

import dev.dimster.ProducerRateSchedule;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.worker.Worker;
import dev.dimster.worker.topology.ConsumerSet;
import dev.dimster.worker.topology.VirtualProducer;

/**
 * One discrete change the {@link DiffApplier} issues against the workers in
 * response to a {@code /modify} request. Each variant maps to exactly one
 * {@link Worker} method, which keeps the per-action dispatch a single
 * {@code action.apply(worker)} call.
 *
 * The {@link #describe()} string lands in the per-test-point modifications
 * log so the result JSON records exactly what was done. Order of execution
 * is decided by {@link TestPointMutator} when it builds the action list.
 */
public sealed interface DiffAction {

    /** Human-readable description for the modifications log. */
    String describe();

    /** Execute the action against the worker. */
    void apply(Worker worker) throws IOException;

    // ─── Hot-patch actions (no client teardown) ──────────────────────────

    record AdjustPublishRate(double rate) implements DiffAction {
        @Override public String describe() { return "adjust-publish-rate to " + rate; }
        @Override public void apply(Worker w) throws IOException { w.adjustPublishRate(rate); }
    }

    record AdjustPublishRateSchedule(ProducerRateSchedule schedule, long scheduleStartEpochMillis)
            implements DiffAction {
        @Override public String describe() {
            return "adjust-publish-rate-schedule to " + schedule.type
                    + " (max=" + schedule.maxConfiguredRate() + ")";
        }
        @Override public void apply(Worker w) throws IOException {
            w.adjustPublishRateSchedule(schedule, scheduleStartEpochMillis);
        }
    }

    record AdjustConsumerProcMs(int min, int max) implements DiffAction {
        @Override public String describe() {
            return "adjust-consumer-proc-ms to [" + min + "," + max + "]";
        }
        @Override public void apply(Worker w) { w.setConsumerProcessingTime(min, max); }
    }

    // ─── Configuration push ──────────────────────────────────────────────

    /**
     * Push a new KafkaConfig to every worker before any restart action runs.
     * The worker stores it; subsequent recreate / addProducers / addConsumers
     * calls pick it up.
     */
    record PushKafkaConfig(KafkaConfig config, String reason) implements DiffAction {
        @Override public String describe() { return "push-kafka-config (" + reason + ")"; }
        @Override public void apply(Worker w) throws IOException { w.setKafkaConfig(config); }
    }

    // ─── Producer lifecycle ──────────────────────────────────────────────

    record AddProducers(List<VirtualProducer> producers, String reason) implements DiffAction {
        @Override public String describe() {
            return "add-producers (" + producers.size() + ") — " + reason;
        }
        @Override public void apply(Worker w) throws IOException { w.addProducers(producers); }
    }

    record RemoveProducers(List<String> producerNames, String reason) implements DiffAction {
        @Override public String describe() {
            return "remove-producers (" + producerNames.size() + ") — " + reason;
        }
        @Override public void apply(Worker w) throws IOException { w.removeProducers(producerNames); }
    }

    record RestartProducers(List<VirtualProducer> producers, String reason) implements DiffAction {
        @Override public String describe() {
            return "restart-producers (" + producers.size() + ") — " + reason;
        }
        @Override public void apply(Worker w) throws IOException { w.restartProducers(producers); }
    }

    // ─── Consumer lifecycle ──────────────────────────────────────────────

    record AddConsumers(ConsumerSet consumers, String reason) implements DiffAction {
        @Override public String describe() {
            return "add-consumers (" + consumers.consumers().size() + ") — " + reason;
        }
        @Override public void apply(Worker w) throws IOException { w.addConsumers(consumers); }
    }

    record RemoveConsumers(List<String> consumerNames, String reason) implements DiffAction {
        @Override public String describe() {
            return "remove-consumers (" + consumerNames.size() + ") — " + reason;
        }
        @Override public void apply(Worker w) throws IOException { w.removeConsumers(consumerNames); }
    }

    record RestartConsumers(ConsumerSet consumers, String reason) implements DiffAction {
        @Override public String describe() {
            return "restart-consumers (" + consumers.consumers().size() + ") — " + reason;
        }
        @Override public void apply(Worker w) throws IOException { w.restartConsumers(consumers); }
    }

    /**
     * Set the dataset producers generate records from. Used for a
     * messageSize change: the engine builds the payload dataset at the new
     * size, and the producer restart that follows picks it up (each
     * runProducer task captures the dataset when it starts).
     */
    record SetDataset(dev.dimster.datagen.ResolvedDataset dataset, int messageSize, String reason)
            implements DiffAction {
        @Override public String describe() {
            return "set-dataset to messageSize=" + messageSize + " — " + reason;
        }
        @Override public void apply(Worker w) throws IOException { w.setDataset(dataset); }
    }

    // ─── Producer-subsystem restart (used by messageDistributor change) ──
    //
    // The `MessageDistributor` object on the worker is set by startLoad
    // and captured by reference in each runProducer thread. To change
    // the distributor mid-test we must restart the producer threads
    // (which also handles the partition-pinning recompute when
    // entering/leaving PINNED_PARTITIONS). The cleanest way is to
    // re-run the initial-setup sequence for the producer side:
    // RemoveProducers (close current) → CreateProducers (build wrappers
    // with new pinning) → StartLoad (refresh distributor / payloads /
    // rate AND spawn the new runProducer threads).

    /**
     * Build new producer wrappers from the given VirtualProducers,
     * mirroring what {@code Worker.createProducers} does at initial
     * test-point setup. Distinct from {@link AddProducers} which
     * spins up runProducer threads immediately — CreateProducers
     * leaves the new wrappers idle, expecting a subsequent
     * {@link StartLoad} to drive them.
     */
    record CreateProducers(java.util.List<VirtualProducer> producers, String reason)
            implements DiffAction {
        @Override public String describe() {
            return "create-producers (" + producers.size() + ") — " + reason;
        }
        @Override public void apply(Worker w) throws IOException { w.createProducers(producers); }
    }

    /**
     * Re-run {@code Worker.startLoad}, refreshing the worker's
     * activeMessageDistributor / activePayloads / rateLimiter from the
     * supplied {@link dev.dimster.worker.commands.ProducerWorkAssignment}
     * and spawning runProducer threads for each currently-known producer.
     * Designed to be paired with a preceding RemoveProducers + CreateProducers.
     */
    record StartLoad(dev.dimster.worker.commands.ProducerWorkAssignment assignment, String reason)
            implements DiffAction {
        @Override public String describe() {
            return "start-load (rate=" + assignment.publishRate()
                    + ", distributor=" + assignment.messageDistributorType()
                    + ", skew=" + assignment.messageDistributorSkew() + ") — " + reason;
        }
        @Override public void apply(Worker w) throws IOException { w.startLoad(assignment); }
    }
}
