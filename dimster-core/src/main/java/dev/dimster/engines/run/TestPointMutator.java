package dev.dimster.engines.run;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dimster.ConsumerProcMode;
import dev.dimster.ConsumerType;
import dev.dimster.ProducerRateSchedule;
import dev.dimster.ScenarioExpander.TestPoint;
import dev.dimster.coordinator.ModifyRequest;
import dev.dimster.coordinator.ModifyResult;
import dev.dimster.datagen.ResolvedDataset;
import dev.dimster.worker.commands.ProducerWorkAssignment;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.utils.distributor.MessageDistributorType;
import dev.dimster.worker.topology.ConsumerSet;
import dev.dimster.worker.topology.ConsumerRestartPlanner;
import dev.dimster.worker.topology.TopologyPlanner;
import dev.dimster.worker.topology.VirtualConsumer;
import dev.dimster.worker.topology.VirtualProducer;
import dev.dimster.worker.topology.VirtualTopology;

/**
 * Central mutation pipeline for {@code dimster run --modify}: takes a
 * ModifyRequest of arbitrary workload and Kafka-config overrides, applies
 * them to the current TestPoint, and emits an ordered list of
 * {@link DiffAction}s that the {@link DiffApplier} executes against the
 * workers.
 *
 * <p>This class is the analogue of {@code TopologyPlanner} for the modify
 * path: a single place where "what changed" is reasoned about, so neither
 * the worker side nor the engine grow ad-hoc switch statements per field.
 * The diff classification table (which field → which actions) lives in
 * {@link #buildDiff} below.
 *
 * <p>Identity preservation: the planner's
 * {@code producer-{topic}-i{idx}} / {@code consumer-{topic}-{group}-i{idx}}
 * naming scheme is index-based, so when we add producers (e.g.
 * producersPerTopic 2 → 3) the existing producers stay untouched and only
 * the new index 2 producer is added. Same for consumer growth.
 *
 * <h2>Scope (MVP)</h2>
 * Supported:
 * <ul>
 *   <li>Random producer and consumer subset restarts without topology changes.</li>
 *   <li>{@code producerRate}, {@code consumerProcMsMin}/{@code Max} (hot-patch).</li>
 *   <li>{@code producersPerTopic} increase / decrease (incremental add / remove).</li>
 *   <li>{@code consumersPerGroup} increase / decrease.</li>
 *   <li>Any {@code kafkaConfig.producerConfig.*} or {@code commonConfig.*}
 *       change → restart all producers.</li>
 *   <li>Any {@code kafkaConfig.classicConsumerConfig.*} /
 *       {@code shareConsumerConfig.*} change → restart matching consumers.</li>
 * </ul>
 * Rejected with "not yet supported" (out of scope this round):
 * <ul>
 *   <li>{@code topics} count change.</li>
 *   <li>{@code partitionsPerTopic} change (would need admin createPartitions
 *       and re-pinning logic).</li>
 *   <li>{@code consumerGroupsPerTopic} change (needs new group naming).</li>
 *   <li>{@code consumerType} change (different wrapper class).</li>
 *   <li>{@code consumerProcMode} change (would need to drain inflight records,
 *       recreate ProcessingTimeSimulator, and reinitialize offset trackers).</li>
 *   <li>{@code consumerProcParallelism} change (would need to resize worker
 *       deadline arrays in CONTINUOUS mode, reset deadline tracking).</li>
 *   <li>{@code kafkaConfig.topicConfig.*} change (admin AlterConfigs).</li>
 *   <li>{@code kafkaConfig.replicationFactor} change.</li>
 * </ul>
 */
public class TestPointMutator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Result of {@link #plan}: the new TestPoint, the new topology, and the action list. */
    public record MutationPlan(
            TestPoint newTestPoint,
            VirtualTopology newTopology,
            List<DiffAction> actions) {}

    private final TopologyPlanner planner;
    private final java.util.function.IntFunction<ResolvedDataset> payloadDataset;

    /**
     * @param planner          used for worker pool round-robin when adding
     *                         producers/consumers.
     * @param payloadDataset   builds the payload dataset for a given
     *                         messageSize. Supplied by WorkloadEngine,
     *                         which holds the BaseWorkload settings
     *                         (useRandomizedPayloads, randomBytesRatio,
     *                         payloadFile, ...) that govern how the
     *                         payloads are constructed.
     */
    public TestPointMutator(TopologyPlanner planner,
                            java.util.function.IntFunction<ResolvedDataset> payloadDataset) {
        this.planner = planner;
        this.payloadDataset = payloadDataset;
    }

    /**
     * Build a MutationPlan or throw an IllegalArgumentException with a
     * human-readable rejection message. Caller (WorkloadEngine /
     * DiffApplier) translates the throw into a {@link ModifyResult}.
     */
    public MutationPlan plan(TestPoint current, VirtualTopology currentTopology, ModifyRequest req) {
        // 1) Apply overrides to TestPoint and KafkaConfig.
        TestPoint mutated = applyWorkloadOverrides(current, req.workloadOverrides());
        KafkaConfig newConfig = applyKafkaConfigOverrides(mutated.kafkaConfig(),
                req.kafkaConfigOverrides());
        if (newConfig != mutated.kafkaConfig()) {
            mutated = withKafkaConfig(mutated, newConfig);
        }

        // 2) Validate.
        validate(current, mutated);

        // 3) Compute diff actions.
        List<DiffAction> actions = buildDiff(current, currentTopology, mutated, req);

        // 4) Build the new VirtualTopology (mirrors what's about to land
        //    on the workers). We don't re-run TopologyPlanner — the diff
        //    actions ARE the topology change. Build the new topology by
        //    applying the same changes locally.
        VirtualTopology newTopology = projectNewTopology(currentTopology, mutated, actions);

        return new MutationPlan(mutated, newTopology, actions);
    }

    /**
     * Build a MutationPlan for the --restart-consumers N action flag.
     * Restarts exactly N randomly selected consumers (or all consumers if N
     * >= total count) without changing topology or client counts.
     *
     * @param current           the active test point
     * @param currentTopology   the active topology
     * @param requestedCount    how many consumers to restart (from --restart-consumers flag)
     * @return a MutationPlan containing only the RestartConsumers action
     * @throws IllegalArgumentException if the request is invalid
     */
    public MutationPlan planConsumerRestart(TestPoint current,
                                           VirtualTopology currentTopology,
                                           int requestedCount) {
        return planConsumerRestart(current, currentTopology, requestedCount,
                ThreadLocalRandom.current());
    }

    /**
     * Test seam for consumer restart selection. Production uses
     * {@link ThreadLocalRandom}; tests pass a seeded {@link Random} so they can
     * assert exact identities without weakening the runtime randomness.
     */
    MutationPlan planConsumerRestart(TestPoint current,
                                     VirtualTopology currentTopology,
                                     int requestedCount,
                                     Random random) {
        ConsumerRestartPlanner.Plan restartPlan = ConsumerRestartPlanner.plan(
                currentTopology, requestedCount, random);
        if (restartPlan.consumerSet().isEmpty()) {
            return new MutationPlan(current, currentTopology, List.of());
        }

        List<DiffAction> actions = new ArrayList<>();
        actions.add(new DiffAction.RestartConsumers(
                restartPlan.consumerSet(),
                String.format("--restart-consumers %d (restarting %d of %d consumers)",
                        requestedCount, restartPlan.consumerSet().consumers().size(),
                        currentTopology.consumers().size())));

        // Return plan with unchanged test point and topology
        return new MutationPlan(current, currentTopology, actions);
    }

    /**
     * Producer counterpart to {@link #planConsumerRestart}. The selected
     * VirtualProducer entries retain their worker assignment and pinning, so
     * the existing remote worker client can route and recreate them in place.
     */
    public MutationPlan planProducerRestart(TestPoint current,
                                            VirtualTopology currentTopology,
                                            int requestedCount) {
        return planProducerRestart(current, currentTopology, requestedCount,
                ThreadLocalRandom.current());
    }

    /** Test seam matching the consumer overload above. */
    MutationPlan planProducerRestart(TestPoint current,
                                     VirtualTopology currentTopology,
                                     int requestedCount,
                                     Random random) {
        if (requestedCount < 0) {
            throw new IllegalArgumentException("restartProducersCount must be >= 0, got "
                    + requestedCount);
        }

        int totalProducers = currentTopology.producers().size();
        if (totalProducers == 0) {
            throw new IllegalArgumentException("no producers to restart " +
                    "(topology has 0 producers)");
        }

        int actualCount = Math.min(requestedCount, totalProducers);
        if (actualCount == 0) {
            return new MutationPlan(current, currentTopology, List.of());
        }

        List<VirtualProducer> toRestart = randomSubset(
                currentTopology.producers(), actualCount, random);
        List<DiffAction> actions = List.of(new DiffAction.RestartProducers(
                toRestart,
                String.format("--restart-producers %d (restarting %d of %d producers)",
                        requestedCount, actualCount, totalProducers)));
        return new MutationPlan(current, currentTopology, actions);
    }

    /**
     * Return a uniform subset without replacement while leaving the immutable
     * topology list untouched. Selecting all preserves topology order because
     * there is no meaningful random choice in that case.
     */
    static <T> List<T> randomSubset(List<T> candidates, int requestedCount, Random random) {
        Objects.requireNonNull(candidates, "candidates");
        Objects.requireNonNull(random, "random");
        if (requestedCount < 0) {
            throw new IllegalArgumentException("requestedCount must be >= 0");
        }
        int actualCount = Math.min(requestedCount, candidates.size());
        if (actualCount == 0) return List.of();
        if (actualCount == candidates.size()) return List.copyOf(candidates);

        List<T> shuffled = new ArrayList<>(candidates);
        Collections.shuffle(shuffled, random);
        return List.copyOf(shuffled.subList(0, actualCount));
    }

    // ─── Step 1a: workload override application ────────────────────────

    /**
     * Apply per-field workload overrides. We list each TestPoint scalar
     * explicitly (it's a record — no setters) and pick override-or-current
     * for each. Verbose but self-documenting and avoids reflection.
     */
    private TestPoint applyWorkloadOverrides(TestPoint current, Map<String, Object> overrides) {
        if (overrides == null || overrides.isEmpty()) return current;
        boolean fixedRateOverride = overrides.containsKey("producerRate");
        ProducerRateSchedule schedule = pickSchedule(overrides, "producerRateSchedule",
                fixedRateOverride ? null : current.producerRateSchedule());
        double producerRate = schedule == null
                ? pickDouble(overrides, "producerRate", current.producerRate())
                : schedule.maxConfiguredRate();
        int messageSize = pickInt(overrides, "messageSize", current.messageSize());
        return new TestPoint(
                current.scenarioName(),
                current.scenarioIndex(),
                current.totalScenarios(),
                current.label(),
                current.dimensionIndex(),
                current.totalDimensions(),
                pickInt(overrides, "topics", current.topics()),
                pickInt(overrides, "partitionsPerTopic", current.partitionsPerTopic()),
                pickInt(overrides, "producersPerTopic", current.producersPerTopic()),
                pickInt(overrides, "consumerGroupsPerTopic", current.consumerGroupsPerTopic()),
                pickInt(overrides, "consumersPerGroup", current.consumersPerGroup()),
                messageSize,
                producerRate,
                schedule,
                pickEnum(overrides, "messageDistributor", MessageDistributorType.class,
                        current.messageDistributor()),
                pickDouble(overrides, "messageDistributorSkew", current.messageDistributorSkew()),
                pickEnum(overrides, "consumerType", ConsumerType.class, current.consumerType()),
                current.kafkaConfig(),  // handled separately by applyKafkaConfigOverrides
                pickInt(overrides, "consumerProcMsMin", current.consumerProcMsMin()),
                pickInt(overrides, "consumerProcMsMax", current.consumerProcMsMax()),
                pickInt(overrides, "consumerProcParallelism", current.consumerProcParallelism()),
                pickEnum(overrides, "consumerProcMode", ConsumerProcMode.class, current.consumerProcMode()),
                pickInt(overrides, "consumerProcCapacity", current.consumerProcCapacity()),
                current.drainBacklogConfig(),
                current.exploreConfig(),
                current.runConfig(),
                // Only the payload dataset follows messageSize; workload
                // datasets are not live-modifiable (validate rejects a
                // messageSize change for them).
                messageSize != current.messageSize() && current.dataset().isPayloadDataset()
                        ? payloadDataset.apply(messageSize)
                        : current.dataset());
    }

    private static int pickInt(Map<String, Object> overrides, String key, int current) {
        Object v = overrides.get(key);
        if (v == null) return current;
        if (v instanceof Number n) return n.intValue();
        return Integer.parseInt(v.toString());
    }

    private static double pickDouble(Map<String, Object> overrides, String key, double current) {
        Object v = overrides.get(key);
        if (v == null) return current;
        if (v instanceof Number n) return n.doubleValue();
        return Double.parseDouble(v.toString());
    }

    private static ProducerRateSchedule pickSchedule(Map<String, Object> overrides, String key,
                                                     ProducerRateSchedule current) {
        if (!overrides.containsKey(key)) {
            return current;
        }
        Object v = overrides.get(key);
        if (v == null) {
            return null;
        }
        if (v instanceof ProducerRateSchedule schedule) {
            schedule.validate();
            return schedule;
        }
        ProducerRateSchedule schedule = MAPPER.convertValue(v, ProducerRateSchedule.class);
        schedule.validate();
        return schedule;
    }

    private static <E extends Enum<E>> E pickEnum(Map<String, Object> overrides, String key,
                                                     Class<E> type, E current) {
        Object v = overrides.get(key);
        if (v == null) return current;
        if (type.isInstance(v)) return type.cast(v);
        return Enum.valueOf(type, v.toString());
    }

    /**
     * Reconstruct a TestPoint with a different KafkaConfig. Records are
     * immutable so we copy every field.
     */
    private TestPoint withKafkaConfig(TestPoint base, KafkaConfig newConfig) {
        return new TestPoint(
                base.scenarioName(), base.scenarioIndex(), base.totalScenarios(),
                base.label(), base.dimensionIndex(), base.totalDimensions(),
                base.topics(), base.partitionsPerTopic(), base.producersPerTopic(),
                base.consumerGroupsPerTopic(), base.consumersPerGroup(),
                base.messageSize(), base.producerRate(),
                base.producerRateSchedule(),
                base.messageDistributor(), base.messageDistributorSkew(), base.consumerType(),
                newConfig,
                base.consumerProcMsMin(), base.consumerProcMsMax(), base.consumerProcParallelism(),
                base.consumerProcMode(), base.consumerProcCapacity(),
                base.drainBacklogConfig(), base.exploreConfig(),
                base.runConfig(), base.dataset());
    }

    // ─── Step 1b: Kafka config override application ────────────────────

    /**
     * Apply dotted-path Kafka config overrides. The first path segment
     * routes to a sub-map (commonConfig / producerConfig /
     * classicConsumerConfig / shareConsumerConfig / topicConfig) or to a
     * top-level scalar (replicationFactor / name). The rest is the leaf
     * key — Kafka config keys are themselves dotted strings (e.g.
     * {@code max.poll.records}), so the key isn't further split.
     */
    private KafkaConfig applyKafkaConfigOverrides(KafkaConfig current, Map<String, Object> overrides) {
        if (overrides == null || overrides.isEmpty()) return current;
        // Build an "overlay" KafkaConfig and merge it via the existing
        // KafkaConfig.merge so we keep that path as the single source of
        // truth for how nested config maps are combined.
        KafkaConfig overlay = new KafkaConfig();
        for (Map.Entry<String, Object> e : overrides.entrySet()) {
            applyOneKafkaConfigOverride(overlay, e.getKey(), e.getValue());
        }
        return KafkaConfig.merge(current, overlay);
    }

    private void applyOneKafkaConfigOverride(KafkaConfig overlay, String dottedPath, Object value) {
        int firstDot = dottedPath.indexOf('.');
        if (firstDot < 0) {
            // Top-level scalar: replicationFactor or name
            switch (dottedPath) {
                case "replicationFactor" -> overlay.replicationFactor =
                        ((Number) coerceNumber(value)).shortValue();
                case "name" -> overlay.name = value.toString();
                default -> throw new IllegalArgumentException(
                        "Unknown top-level kafkaConfig field: " + dottedPath);
            }
            return;
        }
        String head = dottedPath.substring(0, firstDot);
        String leaf = dottedPath.substring(firstDot + 1);
        Map<String, Object> targetMap = switch (head) {
            case "commonConfig" -> overlay.commonConfig = ensureMap(overlay.commonConfig);
            case "producerConfig" -> overlay.producerConfig = ensureMap(overlay.producerConfig);
            case "classicConsumerConfig" -> overlay.classicConsumerConfig = ensureMap(overlay.classicConsumerConfig);
            case "shareConsumerConfig" -> overlay.shareConsumerConfig = ensureMap(overlay.shareConsumerConfig);
            case "topicConfig" -> overlay.topicConfig = ensureMap(overlay.topicConfig);
            default -> throw new IllegalArgumentException(
                    "Unknown kafkaConfig sub-section: " + head + " (in path " + dottedPath + ")");
        };
        targetMap.put(leaf, value);
    }

    private static Map<String, Object> ensureMap(Map<String, Object> existing) {
        return existing != null ? existing : new LinkedHashMap<>();
    }

    private static Object coerceNumber(Object v) {
        if (v instanceof Number) return v;
        return Long.parseLong(v.toString());
    }

    // ─── Step 2: validate ──────────────────────────────────────────────

    private void validate(TestPoint current, TestPoint mutated) {
        if (mutated.topics() != current.topics()) {
            throw new IllegalArgumentException(
                    "topics count change is not yet supported (was " + current.topics()
                            + ", got " + mutated.topics() + ")");
        }
        if (mutated.partitionsPerTopic() != current.partitionsPerTopic()) {
            throw new IllegalArgumentException(
                    "partitionsPerTopic change is not yet supported (was " + current.partitionsPerTopic()
                            + ", got " + mutated.partitionsPerTopic() + ")");
        }
        if (mutated.consumerGroupsPerTopic() != current.consumerGroupsPerTopic()) {
            throw new IllegalArgumentException(
                    "consumerGroupsPerTopic change is not yet supported (was "
                            + current.consumerGroupsPerTopic() + ", got "
                            + mutated.consumerGroupsPerTopic() + ")");
        }
        if (mutated.messageSize() <= 0) {
            throw new IllegalArgumentException(
                    "messageSize must be > 0, got " + mutated.messageSize());
        }
        if (!current.dataset().isPayloadDataset() && mutated.messageSize() != current.messageSize()) {
            throw new IllegalArgumentException(
                    "messageSize does not apply when the test point generates a dataset");
        }
        if (mutated.messageDistributor() != current.messageDistributor()
                && (mutated.messageDistributor() == MessageDistributorType.KEY_COLUMN
                        || current.messageDistributor() == MessageDistributorType.KEY_COLUMN)) {
            throw new IllegalArgumentException(
                    "messageDistributor KEY_COLUMN is tied to the dataset's keyColumn and can't be switched live");
        }
        if (mutated.consumerType() != current.consumerType()) {
            throw new IllegalArgumentException(
                    "consumerType change is not yet supported");
        }
        if (mutated.consumerProcMode() != current.consumerProcMode()) {
            throw new IllegalArgumentException(
                    "consumerProcMode change is not yet supported");
        }
        if (mutated.consumerProcParallelism() != current.consumerProcParallelism()) {
            throw new IllegalArgumentException(
                    "consumerProcParallelism change is not yet supported");
        }
        if (mutated.producerRate() < 0) {
            throw new IllegalArgumentException(
                    "producerRate must be >= 0 (got " + mutated.producerRate() + ")");
        }
        if (mutated.producerRateSchedule() != null) {
            mutated.producerRateSchedule().validate();
        }
        if (mutated.producersPerTopic() < 1) {
            throw new IllegalArgumentException(
                    "producersPerTopic must be >= 1 (got " + mutated.producersPerTopic() + ")");
        }
        if (mutated.consumersPerGroup() < 1) {
            throw new IllegalArgumentException(
                    "consumersPerGroup must be >= 1 (got " + mutated.consumersPerGroup() + ")");
        }
        if (mutated.consumerProcMsMin() < 0 || mutated.consumerProcMsMax() < 0) {
            throw new IllegalArgumentException("consumerProcMs* must be >= 0");
        }
        if (mutated.consumerProcMsMin() > mutated.consumerProcMsMax()) {
            throw new IllegalArgumentException(
                    "consumerProcMsMin must be <= consumerProcMsMax");
        }
        // KafkaConfig validation
        KafkaConfig before = current.kafkaConfig();
        KafkaConfig after = mutated.kafkaConfig();
        if (before != null && after != null
                && before.replicationFactor != after.replicationFactor
                && after.replicationFactor > 0) {
            throw new IllegalArgumentException(
                    "kafkaConfig.replicationFactor change is not yet supported");
        }
        if (mapsDiffer(before == null ? null : before.topicConfig,
                after == null ? null : after.topicConfig)) {
            throw new IllegalArgumentException(
                    "kafkaConfig.topicConfig change is not yet supported (needs admin AlterConfigs)");
        }
    }

    // ─── Step 3: build diff actions ────────────────────────────────────

    private List<DiffAction> buildDiff(TestPoint current, VirtualTopology currentTopology,
                                        TestPoint mutated, ModifyRequest req) {
        List<DiffAction> actions = new ArrayList<>();

        // (a) Hot-patch: rate change. A fixed producerRate override clears any
        // active schedule; a schedule override replaces the limiter in place.
        if (!Objects.equals(mutated.producerRateSchedule(), current.producerRateSchedule())) {
            if (mutated.producerRateSchedule() == null) {
                actions.add(new DiffAction.AdjustPublishRate(mutated.producerRate()));
            } else {
                actions.add(new DiffAction.AdjustPublishRateSchedule(
                        mutated.producerRateSchedule(), System.currentTimeMillis()));
            }
        } else if (mutated.producerRateSchedule() == null
                && Double.compare(mutated.producerRate(), current.producerRate()) != 0) {
            actions.add(new DiffAction.AdjustPublishRate(mutated.producerRate()));
        }

        // (b) Hot-patch: consumer proc-ms change.
        if (mutated.consumerProcMsMin() != current.consumerProcMsMin()
                || mutated.consumerProcMsMax() != current.consumerProcMsMax()) {
            actions.add(new DiffAction.AdjustConsumerProcMs(
                    mutated.consumerProcMsMin(), mutated.consumerProcMsMax()));
        }

        // (b2) messageSize change → set the new payload dataset, then
        // restart all producers. The new runProducer threads capture the
        // freshly-set dataset at submission, so the first send after
        // restart ships at the new size. KafkaProducers themselves don't care
        // about message size, but restarting them is the simplest way to
        // get a clean cutover (no in-flight old-size sends mixing with
        // new-size ones).
        boolean messageSizeChanged = mutated.messageSize() != current.messageSize();

        // (c) KafkaConfig changes → push config + restart affected clients.
        // We classify which sub-maps changed so we only restart the
        // necessary side (producers vs consumers vs both).
        boolean producerConfigChanged = subMapChanged(current.kafkaConfig(), mutated.kafkaConfig(),
                KafkaConfigSection.PRODUCER);
        boolean classicConsumerConfigChanged = subMapChanged(current.kafkaConfig(), mutated.kafkaConfig(),
                KafkaConfigSection.CLASSIC_CONSUMER);
        boolean shareConsumerConfigChanged = subMapChanged(current.kafkaConfig(), mutated.kafkaConfig(),
                KafkaConfigSection.SHARE_CONSUMER);
        boolean commonConfigChanged = subMapChanged(current.kafkaConfig(), mutated.kafkaConfig(),
                KafkaConfigSection.COMMON);

        // (b3) messageDistributor / skew change. Triggers a full producer-
        // subsystem restart (RemoveProducers + CreateProducers + StartLoad)
        // because the MessageDistributor object on the worker is set by
        // startLoad and captured by reference in each runProducer thread.
        // It also rebuilds VirtualProducer.pinnedPartitions per the new
        // distributor (PINNED_PARTITIONS adds pinning, others remove it).
        boolean distributorChanged = mutated.messageDistributor() != current.messageDistributor()
                || Double.compare(mutated.messageDistributorSkew(),
                                   current.messageDistributorSkew()) != 0;

        // PINNED_PARTITIONS pinning is a function of (producerCount,
        // partitionCount). Changing producersPerTopic while
        // PINNED_PARTITIONS is the active distributor reshuffles every
        // producer's partition assignment, even for the producers we're
        // keeping — so we must recompute pinning on all of them, not
        // just add/remove. The producer-subsystem restart path handles
        // this: same shape as distributorChanged.
        // (Note: if the distributor is also changing, distributorChanged
        // already covers it — no need to evaluate this case separately.)
        int producerCountDelta = mutated.producersPerTopic() - current.producersPerTopic();
        boolean pinningRecomputeNeeded = !distributorChanged
                && producerCountDelta != 0
                && mutated.messageDistributor()
                        == dev.dimster.utils.distributor.MessageDistributorType.PINNED_PARTITIONS;
        // Combined trigger for the full producer-subsystem restart path
        // (RemoveProducers all + CreateProducers all + StartLoad).
        boolean producerSubsystemRestart = distributorChanged || pinningRecomputeNeeded;

        // messageSize change folds into the producer-restart path: set the
        // new payload dataset, then restart producers (which pick it up at
        // submission time).
        // distributorChanged uses a different restart shape (full
        // CreateProducers + StartLoad) so we don't fold it here — see
        // the distributor-restart block below.
        boolean restartProducers = producerConfigChanged || commonConfigChanged
                || messageSizeChanged;
        boolean restartConsumers = commonConfigChanged
                || (current.consumerType() == ConsumerType.CONSUMER_GROUP && classicConsumerConfigChanged)
                || (current.consumerType() == ConsumerType.SHARE_GROUP && shareConsumerConfigChanged);

        // (d) Producer count delta.
        int producerDelta = mutated.producersPerTopic() - current.producersPerTopic();
        // (e) Consumer count delta.
        int consumerDelta = mutated.consumersPerGroup() - current.consumersPerGroup();

        // If config push is needed, do it FIRST so subsequent
        // restart/add operations create clients with the new config.
        if (restartProducers || restartConsumers || producerSubsystemRestart
                || producerDelta > 0 || consumerDelta > 0) {
            // Always push the new config when any restart/add is on the
            // way — it's idempotent and ensures the workers' stored
            // config matches what the new clients should use. If neither
            // restart nor add is needed but the config did change, we
            // still push it so future stats / logs reflect the truth.
            if (current.kafkaConfig() != mutated.kafkaConfig() || hasKafkaConfigOverrides(req)) {
                actions.add(new DiffAction.PushKafkaConfig(mutated.kafkaConfig(),
                        configChangeReason(producerConfigChanged, classicConsumerConfigChanged,
                                shareConsumerConfigChanged, commonConfigChanged)));
            }
            // Set the new payload dataset before any producer restart/add,
            // including the producer-subsystem restart below, so the new
            // runProducer threads capture it.
            if (messageSizeChanged) {
                actions.add(new DiffAction.SetDataset(mutated.dataset(), mutated.messageSize(),
                        "messageSize " + current.messageSize() + " -> " + mutated.messageSize()));
            }
        }

        // (f) Producer remove (if shrinking) — done before restart so the
        // restart only touches kept producers. Skipped when the
        // producer-subsystem restart path will fire (it removes ALL
        // producers anyway).
        if (producerDelta < 0 && !producerSubsystemRestart) {
            List<String> toRemove = producerNamesToRemove(currentTopology,
                    current.producersPerTopic(), mutated.producersPerTopic());
            if (!toRemove.isEmpty()) {
                actions.add(new DiffAction.RemoveProducers(toRemove,
                        "producersPerTopic " + current.producersPerTopic()
                                + " -> " + mutated.producersPerTopic()));
            }
        }

        // (g) Consumer remove (if shrinking) — same ordering.
        if (consumerDelta < 0) {
            List<String> toRemove = consumerNamesToRemove(currentTopology,
                    current.consumersPerGroup(), mutated.consumersPerGroup());
            if (!toRemove.isEmpty()) {
                actions.add(new DiffAction.RemoveConsumers(toRemove,
                        "consumersPerGroup " + current.consumersPerGroup()
                                + " -> " + mutated.consumersPerGroup()));
            }
        }

        // (h) Restart producers (kafkaConfig and/or messageSize change).
        // Skipped when the producer-subsystem restart path will fire —
        // its RemoveProducers + CreateProducers + StartLoad sequence
        // subsumes a config/size-only restart.
        if (restartProducers && !producerSubsystemRestart) {
            // Restart only the producers we're keeping (already-existing
            // ones with idx < new producersPerTopic). Newly-added
            // producers go through addProducers below using the same new
            // config — no need to restart them.
            int keptCount = Math.min(current.producersPerTopic(), mutated.producersPerTopic());
            List<VirtualProducer> keep = filterProducersByMaxIdx(currentTopology, keptCount);
            if (!keep.isEmpty()) {
                String reason = configChangeReason(producerConfigChanged, false, false, commonConfigChanged);
                if (messageSizeChanged) {
                    reason = reason.isEmpty() || reason.equals(" changed")
                            ? "messageSize changed"
                            : reason + " + messageSize changed";
                }
                actions.add(new DiffAction.RestartProducers(keep, reason));
            }
        }

        // (h2) Producer-subsystem restart. Mirrors the initial test-point
        // setup for the producer side:
        //   RemoveProducers(all current names)
        //   CreateProducers(all new VirtualProducers with new pinning)
        //   StartLoad(new ProducerWorkAssignment)
        // StartLoad refreshes the worker's activeMessageDistributor +
        // rateLimiter and spawns the runProducer
        // threads. Subsumes the producer half of any restartProducers
        // path AND any AddProducers / RemoveProducers path for
        // producer-count changes — the new producer set is rebuilt
        // wholesale. Triggered by either:
        //  - distributorChanged (the runtime MessageDistributor object
        //    must be rebuilt + per-producer pinning may flip), OR
        //  - pinningRecomputeNeeded (PINNED_PARTITIONS active and
        //    producer count changed → every producer's partition split
        //    needs recomputing, even the ones we keep).
        if (producerSubsystemRestart) {
            // All current producer names get removed.
            List<String> oldNames = currentTopology.producers().stream()
                    .map(VirtualProducer::name)
                    .toList();
            String reason = distributorChanged
                    ? describeDistributorChange(current, mutated)
                    : "PINNED_PARTITIONS pinning recompute (producersPerTopic "
                            + current.producersPerTopic() + " -> "
                            + mutated.producersPerTopic() + ")";
            if (!oldNames.isEmpty()) {
                actions.add(new DiffAction.RemoveProducers(oldNames, reason));
            }
            // New producer set: count = mutated producersPerTopic, with
            // pinning recomputed from the new distributor type and counts.
            List<VirtualProducer> newProducers = buildProducerSetForDistributor(
                    currentTopology, mutated);
            actions.add(new DiffAction.CreateProducers(newProducers, reason));
            // Build a fresh ProducerWorkAssignment carrying the new
            // distributor type/skew and the (possibly-new) producer rate.
            // A messageSize change already set the new payload dataset.
            ProducerWorkAssignment assignment = new ProducerWorkAssignment(
                    effectiveTargetRate(mutated), mutated.producerRateSchedule(),
                    mutated.producerRateSchedule() == null ? null : System.currentTimeMillis(),
                    mutated.messageDistributor(),
                    mutated.partitionsPerTopic(),
                    mutated.messageDistributorSkew());
            actions.add(new DiffAction.StartLoad(assignment, reason));
        }

        // (i) Restart consumers (kafkaConfig change).
        if (restartConsumers) {
            int keptCount = Math.min(current.consumersPerGroup(), mutated.consumersPerGroup());
            List<VirtualConsumer> keep = filterConsumersByMaxIdx(currentTopology, keptCount);
            if (!keep.isEmpty()) {
                actions.add(new DiffAction.RestartConsumers(
                        new ConsumerSet(keep, mutated.consumerType(),
                                mutated.consumerProcMsMin(), mutated.consumerProcMsMax(), mutated.consumerProcParallelism(),
                                mutated.consumerProcMode(), mutated.consumerProcCapacity()),
                        configChangeReason(false, classicConsumerConfigChanged,
                                shareConsumerConfigChanged, commonConfigChanged)));
            }
        }

        // (j) Producer add (if growing). Skipped when the producer-
        // subsystem restart path will fire — the CreateProducers in
        // (h2) above already carries the full new producer set
        // (including any extras from a producersPerTopic grow that
        // landed in the same modify).
        if (producerDelta > 0 && !producerSubsystemRestart) {
            List<VirtualProducer> toAdd = newProducersForGrowth(currentTopology,
                    current.producersPerTopic(), mutated.producersPerTopic(), mutated);
            if (!toAdd.isEmpty()) {
                actions.add(new DiffAction.AddProducers(toAdd,
                        "producersPerTopic " + current.producersPerTopic()
                                + " -> " + mutated.producersPerTopic()));
            }
        }

        // (k) Consumer add (if growing).
        if (consumerDelta > 0) {
            ConsumerSet toAdd = newConsumersForGrowth(currentTopology,
                    current.consumersPerGroup(), mutated.consumersPerGroup(), mutated);
            if (!toAdd.consumers().isEmpty()) {
                actions.add(new DiffAction.AddConsumers(toAdd,
                        "consumersPerGroup " + current.consumersPerGroup()
                                + " -> " + mutated.consumersPerGroup()));
            }
        }

        return actions;
    }

    private boolean hasKafkaConfigOverrides(ModifyRequest req) {
        return req.kafkaConfigOverrides() != null && !req.kafkaConfigOverrides().isEmpty();
    }

    // ─── Diff helper: Kafka config sub-map comparison ──────────────────

    private enum KafkaConfigSection { PRODUCER, CLASSIC_CONSUMER, SHARE_CONSUMER, COMMON, TOPIC }

    private boolean subMapChanged(KafkaConfig a, KafkaConfig b, KafkaConfigSection section) {
        Map<String, Object> ma = a == null ? null : sectionMap(a, section);
        Map<String, Object> mb = b == null ? null : sectionMap(b, section);
        return mapsDiffer(ma, mb);
    }

    private Map<String, Object> sectionMap(KafkaConfig c, KafkaConfigSection s) {
        return switch (s) {
            case PRODUCER -> c.producerConfig;
            case CLASSIC_CONSUMER -> c.classicConsumerConfig;
            case SHARE_CONSUMER -> c.shareConsumerConfig;
            case COMMON -> c.commonConfig;
            case TOPIC -> c.topicConfig;
        };
    }

    private static boolean mapsDiffer(Map<String, Object> a, Map<String, Object> b) {
        if (a == null && b == null) return false;
        if (a == null || b == null) return !(a == null ? b.isEmpty() : a.isEmpty());
        // TreeMap so the comparison is order-independent.
        return !new TreeMap<>(a).equals(new TreeMap<>(b));
    }

    private String configChangeReason(boolean producer, boolean classicConsumer,
                                       boolean shareConsumer, boolean common) {
        List<String> parts = new ArrayList<>();
        if (producer) parts.add("producerConfig");
        if (classicConsumer) parts.add("classicConsumerConfig");
        if (shareConsumer) parts.add("shareConsumerConfig");
        if (common) parts.add("commonConfig");
        return String.join("+", parts) + " changed";
    }

    // ─── Diff helper: producer/consumer subset filtering ───────────────

    private List<VirtualProducer> filterProducersByMaxIdx(VirtualTopology topology, int maxIdxExclusive) {
        List<VirtualProducer> out = new ArrayList<>();
        for (VirtualProducer vp : topology.producers()) {
            if (vp.producerIdx() < maxIdxExclusive) out.add(vp);
        }
        return out;
    }

    private List<VirtualConsumer> filterConsumersByMaxIdx(VirtualTopology topology, int maxIdxExclusive) {
        List<VirtualConsumer> out = new ArrayList<>();
        for (VirtualConsumer vc : topology.consumers()) {
            if (vc.consumerIdx() < maxIdxExclusive) out.add(vc);
        }
        return out;
    }

    private List<String> producerNamesToRemove(VirtualTopology topology, int oldCount, int newCount) {
        List<String> names = new ArrayList<>();
        for (VirtualProducer vp : topology.producers()) {
            if (vp.producerIdx() >= newCount && vp.producerIdx() < oldCount) {
                names.add(vp.name());
            }
        }
        return names;
    }

    private List<String> consumerNamesToRemove(VirtualTopology topology, int oldCount, int newCount) {
        List<String> names = new ArrayList<>();
        for (VirtualConsumer vc : topology.consumers()) {
            if (vc.consumerIdx() >= newCount && vc.consumerIdx() < oldCount) {
                names.add(vc.name());
            }
        }
        return names;
    }

    /**
     * Build new VirtualProducer entries for indices [oldCount, newCount).
     * Worker assignment follows the planner's round-robin rule across the
     * producer worker pool, applied to the combined (topic, idx) iteration
     * order so the new instances slot in alongside existing ones with the
     * same algorithm.
     */
    private List<VirtualProducer> newProducersForGrowth(VirtualTopology currentTopology,
                                                        int oldCount, int newCount,
                                                        TestPoint mutated) {
        List<String> producerWorkers = planner.producerWorkers();
        List<String> topics = new ArrayList<>(currentTopology.topicPartitionCounts().keySet());
        // Worker round-robin index continues from where existing producers
        // left off so the assignment matches a fresh full plan() call.
        int rrIdx = currentTopology.producers().size();
        List<VirtualProducer> out = new ArrayList<>();
        for (String topic : topics) {
            for (int idx = oldCount; idx < newCount; idx++) {
                String worker = producerWorkers.get(rrIdx % producerWorkers.size());
                rrIdx++;
                int[] pinning = computePinningForNewProducer(idx, mutated.partitionsPerTopic(),
                        newCount, mutated.messageDistributor());
                out.add(new VirtualProducer(worker, topic, idx, pinning));
            }
        }
        return out;
    }

    /**
     * Build the full producer set (count = mutated.producersPerTopic) for
     * the distributor-restart path. Re-uses each topic's worker
     * assignment from the existing topology where possible (so producer
     * idx 0 stays on the same worker before and after) and falls back to
     * round-robin from the planner pool for any new indices beyond the
     * current count. Pinning is computed from the new distributor type.
     */
    private List<VirtualProducer> buildProducerSetForDistributor(
            VirtualTopology currentTopology, TestPoint mutated) {
        List<String> producerWorkers = planner.producerWorkers();
        List<String> topics = new ArrayList<>(currentTopology.topicPartitionCounts().keySet());
        // Pre-index existing producers by (topic, idx) -> worker so we
        // can reuse the placement of producers that survive the change.
        java.util.Map<String, String> existingPlacement = new java.util.HashMap<>();
        for (VirtualProducer vp : currentTopology.producers()) {
            existingPlacement.put(vp.topic() + "#" + vp.producerIdx(), vp.worker());
        }
        int newCount = mutated.producersPerTopic();
        int rrIdx = 0;
        List<VirtualProducer> out = new ArrayList<>();
        for (String topic : topics) {
            for (int idx = 0; idx < newCount; idx++) {
                String worker = existingPlacement.get(topic + "#" + idx);
                if (worker == null) {
                    worker = producerWorkers.get(rrIdx % producerWorkers.size());
                }
                rrIdx++;
                int[] pinning = computePinningForNewProducer(idx, mutated.partitionsPerTopic(),
                        newCount, mutated.messageDistributor());
                out.add(new VirtualProducer(worker, topic, idx, pinning));
            }
        }
        return out;
    }

    private static <T> List<T> appendAll(List<T> base, List<T> more) {
        base.addAll(more);
        return base;
    }

    private String describeDistributorChange(TestPoint current, TestPoint mutated) {
        if (current.messageDistributor() != mutated.messageDistributor()) {
            return "messageDistributor " + current.messageDistributor()
                    + " -> " + mutated.messageDistributor();
        }
        return "messageDistributorSkew " + current.messageDistributorSkew()
                + " -> " + mutated.messageDistributorSkew();
    }

    /**
     * Compute the partition pinning array for a single producer at the
     * given index, mirroring TopologyPlanner's SPLIT_EVENLY / NONE rules.
     * (Correctness mode's ONE_PER_PRODUCER pinning isn't reachable here —
     * correctness mode uses run mode but doesn't go through /modify.)
     */
    private int[] computePinningForNewProducer(int idx, int partitionsPerTopic,
                                                 int producersPerTopic,
                                                 MessageDistributorType distributor) {
        if (distributor != MessageDistributorType.PINNED_PARTITIONS) {
            return new int[0];
        }
        // SPLIT_EVENLY rule: when producers > partitions, each producer
        // gets exactly one partition (modulo wrap). Otherwise, partitions
        // split with the first (P % N) producers getting one extra.
        if (producersPerTopic > partitionsPerTopic) {
            return new int[] { idx % partitionsPerTopic };
        }
        int base = partitionsPerTopic / producersPerTopic;
        int remainder = partitionsPerTopic % producersPerTopic;
        int extra = idx < remainder ? 1 : 0;
        int len = base + extra;
        int start = idx * base + Math.min(idx, remainder);
        int[] out = new int[len];
        for (int i = 0; i < len; i++) out[i] = start + i;
        return out;
    }

    /**
     * Build new VirtualConsumer entries for indices [oldCount, newCount)
     * within each (topic, group) pair currently in the topology.
     */
    private ConsumerSet newConsumersForGrowth(VirtualTopology currentTopology,
                                                int oldCount, int newCount,
                                                TestPoint mutated) {
        List<String> consumerWorkers = planner.consumerWorkers();
        // Walk existing consumers to discover the (topic, group) pairs
        // that should grow. LinkedHashMap preserves discovery order so the
        // round-robin worker assignment stays deterministic.
        Map<String, List<String>> groupsByTopic = new LinkedHashMap<>();
        for (VirtualConsumer vc : currentTopology.consumers()) {
            List<String> groups = groupsByTopic.computeIfAbsent(vc.topic(), k -> new ArrayList<>());
            if (!groups.contains(vc.consumerGroup())) groups.add(vc.consumerGroup());
        }
        int rrIdx = currentTopology.consumers().size();
        List<VirtualConsumer> out = new ArrayList<>();
        for (Map.Entry<String, List<String>> e : groupsByTopic.entrySet()) {
            String topic = e.getKey();
            for (String group : e.getValue()) {
                for (int idx = oldCount; idx < newCount; idx++) {
                    String worker = consumerWorkers.get(rrIdx % consumerWorkers.size());
                    rrIdx++;
                    // Static partition assignment is dropped on mid-test
                    // grow — the new consumers join via dynamic group
                    // assignment. Correctness mode (which is the only
                    // user of static assignment) doesn't use modify.
                    out.add(new VirtualConsumer(worker, topic, group, idx, List.of()));
                }
            }
        }
        return new ConsumerSet(out, mutated.consumerType(),
                mutated.consumerProcMsMin(), mutated.consumerProcMsMax(), mutated.consumerProcParallelism(),
                mutated.consumerProcMode(), mutated.consumerProcCapacity());
    }

    // ─── Step 4: project the new VirtualTopology ──────────────────────

    /**
     * Build a new VirtualTopology that reflects the diff actions. Used to
     * update the engine's stored topology so subsequent /modify calls see
     * the post-modify state. Conservative: only handles add/remove —
     * topology metadata fields (consumerType, consumerProcMs*, etc.) are
     * pulled from the mutated TestPoint.
     */
    private VirtualTopology projectNewTopology(VirtualTopology current, TestPoint mutated,
                                                List<DiffAction> actions) {
        // CreateProducers (used by the distributor-restart path) supplies
        // the COMPLETE post-restart producer set, so when present it
        // wholesale replaces the producer list (no add/remove walking
        // needed for that scenario). The add/remove walking still
        // applies for producersPerTopic-only changes where CreateProducers
        // isn't emitted.
        List<VirtualProducer> producersFromCreate = null;
        Set<String> removedProducerNames = new java.util.LinkedHashSet<>();
        Set<String> removedConsumerNames = new java.util.LinkedHashSet<>();
        List<VirtualProducer> producersToAdd = new ArrayList<>();
        List<VirtualConsumer> consumersToAdd = new ArrayList<>();
        for (DiffAction a : actions) {
            if (a instanceof DiffAction.RemoveProducers rp) removedProducerNames.addAll(rp.producerNames());
            else if (a instanceof DiffAction.RemoveConsumers rc) removedConsumerNames.addAll(rc.consumerNames());
            else if (a instanceof DiffAction.AddProducers ap) producersToAdd.addAll(ap.producers());
            else if (a instanceof DiffAction.AddConsumers ac) consumersToAdd.addAll(ac.consumers().consumers());
            else if (a instanceof DiffAction.CreateProducers cp) {
                producersFromCreate = (producersFromCreate == null)
                        ? new ArrayList<>(cp.producers())
                        : appendAll(producersFromCreate, cp.producers());
            }
        }
        List<VirtualProducer> newProducers;
        if (producersFromCreate != null) {
            newProducers = producersFromCreate;
        } else {
            newProducers = new ArrayList<>();
            for (VirtualProducer vp : current.producers()) {
                if (!removedProducerNames.contains(vp.name())) newProducers.add(vp);
            }
            newProducers.addAll(producersToAdd);
        }
        List<VirtualConsumer> newConsumers = new ArrayList<>();
        for (VirtualConsumer vc : current.consumers()) {
            if (!removedConsumerNames.contains(vc.name())) newConsumers.add(vc);
        }
        newConsumers.addAll(consumersToAdd);
        int activeProducerWorkers = (int) newProducers.stream()
                .map(VirtualProducer::worker).distinct().count();
        return new VirtualTopology(
                current.topicPartitionCounts(),  // unchanged in MVP
                newProducers,
                newConsumers,
                mutated.consumerType(),
                mutated.consumerProcMsMin(),
                mutated.consumerProcMsMax(),
                mutated.consumerProcParallelism(),
                mutated.consumerProcMode(),
                mutated.consumerProcCapacity(),
                mutated.messageDistributor(),
                mutated.messageDistributorSkew(),
                Math.max(activeProducerWorkers, 1));
    }

    private static double effectiveTargetRate(TestPoint point) {
        return point.producerRateSchedule() == null
                ? point.producerRate()
                : point.producerRateSchedule().maxConfiguredRate();
    }
}
