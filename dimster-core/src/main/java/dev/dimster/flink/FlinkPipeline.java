package dev.dimster.flink;

import dev.dimster.datagen.ResolvedDataset;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.worker.topology.VirtualTopology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs the Flink job of a test point with a Flink stage. Engines use it as a
 * helper: {@link #start} after the topics exist and before consumers start,
 * {@link #stop} before the topics are deleted (from
 * {@code BaseEngine.cooldownBetweenTests}), and {@link #cancelLeftovers} at the
 * start of each test point to clear jobs a crashed run left behind, like
 * leftover topics.
 *
 * <p>Holds only the id of the running job.
 */
public class FlinkPipeline {

    private static final Logger log = LoggerFactory.getLogger(FlinkPipeline.class);

    /** Prefix of every Dimster job name; leftovers are found by it. */
    public static final String JOB_PREFIX = "dimster-";
    private static final Set<String> TERMINAL_STATES = Set.of("FINISHED", "CANCELED", "FAILED");
    private static final Duration RUNNING_TIMEOUT = Duration.ofSeconds(120);
    private static final Duration CANCEL_TIMEOUT = Duration.ofSeconds(60);
    // Short, because drain-backlog starts its clock when the job is seen running.
    private static final long POLL_MS = 200;

    private static final Duration SLOT_TIMEOUT = Duration.ofSeconds(60);

    private final FlinkRestClient client;
    private final AtomicBoolean stopRequested;
    private final Duration slotTimeout;
    private String jobId;

    public FlinkPipeline(FlinkRestClient client, AtomicBoolean stopRequested) {
        this(client, stopRequested, SLOT_TIMEOUT);
    }

    /** @param slotTimeout how long {@link #start} waits for enough free task slots */
    FlinkPipeline(FlinkRestClient client, AtomicBoolean stopRequested, Duration slotTimeout) {
        this.client = client;
        this.stopRequested = stopRequested;
        this.slotTimeout = slotTimeout;
    }

    public static boolean isTerminal(String state) {
        return TERMINAL_STATES.contains(state);
    }

    /** The running job's id, or null. */
    public String jobId() {
        return jobId;
    }

    /**
     * Checks the Flink cluster is reachable, failing with a message that says
     * how to deploy one. Called once before the first Flink test point.
     */
    public void checkReachable() {
        try {
            var overview = client.overview();
            log.info("Flink cluster at {}: Flink {}, {} task managers, {}/{} slots available",
                    client.baseUrl(), overview.flinkVersion(), overview.taskManagers(),
                    overview.slotsAvailable(), overview.slotsTotal());
        } catch (IOException e) {
            throw new IllegalStateException("The workload uses Flink but no Flink cluster is reachable at "
                    + client.baseUrl() + " (" + e.getMessage() + "). Add a flink block to the environment in "
                    + "dimster-config.yaml and run dimster deploy.", e);
        }
    }

    /** Cancels non-terminal Dimster jobs, such as those of an interrupted run. */
    public void cancelLeftovers() throws IOException {
        for (var job : client.listJobs()) {
            if (job.name().startsWith(JOB_PREFIX) && !isTerminal(job.state()) && !job.id().equals(jobId)) {
                log.info("Cancelling leftover Flink job {} ({}, {})", job.name(), job.id(), job.state());
                client.cancel(job.id());
                awaitTerminal(job.id());
            }
        }
    }

    /**
     * Submits the job for the topology's pipelines and waits until every
     * vertex is RUNNING, so its sources are reading before producers start.
     *
     * @return the job id
     */
    public String start(FlinkSpec flink, ResolvedDataset dataset, VirtualTopology topology,
                        KafkaConfig kafkaConfig) throws IOException {
        if (jobId != null) {
            throw new IllegalStateException("A Flink job is already running: " + jobId);
        }
        if (topology.pipelineTopics().isEmpty()) {
            throw new IllegalArgumentException("The topology has no Flink pipelines");
        }
        awaitSlots(flink.slotsNeeded());

        // Input topics are <prefix>-<suffix>-NNNN: name the job after the suffix.
        String firstTopic = topology.producerTopics().getFirst();
        String base = firstTopic.replaceFirst("-\\d{4}$", "");
        String jobName = base.startsWith(JOB_PREFIX) ? base : JOB_PREFIX + base;
        String groupId = "dimster-grp-flink-" + base.substring(base.lastIndexOf('-') + 1);

        FlinkJobSpec spec = FlinkJobSpec.build(jobName, flink, dataset, topology.pipelineTopics(), groupId,
                kafkaConfig.commonProperties());
        log.info("Submitting Flink job {} ({} pipelines, parallelism {}, source {}, sink {}, {})", jobName,
                spec.pipelines().size(), flink.parallelism, flink.resolved(flink.sourceParallelism),
                flink.resolved(flink.sinkParallelism), flink.deliveryGuarantee);
        jobId = client.submit(spec.toBase64());
        log.info("Flink job {} submitted as {}; waiting for it to be running", jobName, jobId);
        awaitRunning();
        log.info("Flink job {} is running", jobId);
        return jobId;
    }

    /**
     * Waits for enough free task slots. Slots appear when TaskManagers
     * register (just after a deploy) and are freed asynchronously after the
     * previous test point's job is cancelled, so a short wait is normal.
     */
    private void awaitSlots(int needed) throws IOException {
        Instant deadline = Instant.now().plus(slotTimeout);
        FlinkRestClient.Overview overview = client.overview();
        while (overview.slotsAvailable() < needed && Instant.now().isBefore(deadline) && !stopRequested.get()) {
            sleep();
            overview = client.overview();
        }
        if (overview.slotsAvailable() < needed) {
            throw new IllegalStateException(("The Flink job needs %d free task slots (its highest parallelism of"
                    + " flink.parallelism, sourceParallelism and sinkParallelism) but the cluster has %d of %d"
                    + " available. Lower the parallelism or add task managers/slots"
                    + " (flink.taskmanagers in dimster-config.yaml).")
                    .formatted(needed, overview.slotsAvailable(), overview.slotsTotal()));
        }
    }

    private void awaitRunning() throws IOException {
        Instant deadline = Instant.now().plus(RUNNING_TIMEOUT);
        FlinkRestClient.JobDetails details = null;
        while (Instant.now().isBefore(deadline) && !stopRequested.get()) {
            details = client.job(jobId);
            if ("RUNNING".equals(details.state()) && details.allVerticesRunning()) {
                return;
            }
            if (isTerminal(details.state())) {
                String failedJob = jobId;
                jobId = null;
                throw new IllegalStateException("Flink job " + failedJob + " ended as " + details.state()
                        + " before running:\n" + client.rootException(failedJob));
            }
            sleep();
        }
        if (stopRequested.get()) return;
        throw new IllegalStateException("Flink job " + jobId + " was not running after "
                + RUNNING_TIMEOUT.toSeconds() + "s (state " + (details != null ? details.state() : "unknown")
                + ", vertices " + (details != null ? details.vertexStatuses() : "unknown") + ")"
                + exceptionSuffix(jobId));
    }

    /** The job's restart count so far, or -1 when not known. */
    public long restarts() {
        if (jobId == null) return -1;
        try {
            return client.numRestarts(jobId);
        } catch (IOException e) {
            log.warn("Could not read restarts of Flink job {}: {}", jobId, e.getMessage());
            return -1;
        }
    }

    /** Cancels the running job, if any, and waits for it to end. Idempotent. */
    public void stop() {
        if (jobId == null) return;
        String id = jobId;
        jobId = null;
        try {
            var state = client.job(id).state();
            if (!isTerminal(state)) {
                log.info("Cancelling Flink job {}", id);
                client.cancel(id);
                awaitTerminal(id);
            } else {
                log.warn("Flink job {} had already ended as {}{}", id, state, exceptionSuffix(id));
            }
        } catch (IOException e) {
            log.warn("Could not cancel Flink job {}: {}", id, e.getMessage());
        }
    }

    private void awaitTerminal(String id) throws IOException {
        Instant deadline = Instant.now().plus(CANCEL_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            if (isTerminal(client.job(id).state())) return;
            sleep();
        }
        log.warn("Flink job {} did not end within {}s of cancelling", id, CANCEL_TIMEOUT.toSeconds());
    }

    private String exceptionSuffix(String id) {
        String root = client.rootException(id);
        return root.isBlank() ? "" : ":\n" + root;
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
