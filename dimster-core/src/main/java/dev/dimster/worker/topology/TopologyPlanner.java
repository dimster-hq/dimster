package dev.dimster.worker.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.dimster.ConsumerProcMode;
import dev.dimster.ConsumerType;
import dev.dimster.utils.RandomGenerator;
import dev.dimster.utils.distributor.MessageDistributorType;

/**
 * Builds a {@link VirtualTopology} for one test point: generates topic and
 * consumer-group names, decides which workers host which producers and
 * consumers, and pre-computes per-instance partition assignments (producer
 * pinning + consumer static assignment).
 *
 * <p>This class is the single place where topology decisions are made. Both
 * {@code LocalWorker} and {@code RemoteWorkerClient} downstream just iterate
 * the resulting topology and instantiate Kafka clients literally — they make
 * no further routing or partition decisions. Engines call
 * {@link #plan(TopologySpec)} <em>before</em> creating Kafka topics, then read
 * the generated topic names off {@link VirtualTopology#topicPartitionCounts()}
 * to issue the actual {@code createTopic} calls.
 *
 * <h2>Worker assignment</h2>
 *
 * <ul>
 *   <li>{@link WorkerMode#MIXED}: producer pool = consumer pool = all workers.</li>
 *   <li>{@link WorkerMode#SEPARATED}: workers split into two disjoint pools
 *       sized by {@link WorkerSplit}. With a single worker, both pools collapse
 *       to that worker.</li>
 *   <li>Non-correctness: producers and consumers are spread round-robin across
 *       their respective pools. Producers are spread across the
 *       {@code (topic, producerIdx)} pairs in iteration order (all producers
 *       of topic 0, then topic 1, ...); the i-th pair lands on
 *       {@code producerWorkers[i mod size]}. Consumers follow the same pattern
 *       across {@code (topic, groupIdx, consumerIdx)} triples.</li>
 *   <li>Correctness: deterministic topic → worker affinity. Producers and
 *       consumers for the same topic are always co-located on the same
 *       worker. In SEPARATED mode the consumer pool is ignored — consumers
 *       follow producers onto the producer pool.</li>
 * </ul>
 *
 * <h2>Producer partition pinning</h2>
 *
 * <p>Driven by {@link ProducerPinning}, which is itself derived from
 * {@code (correctnessEnabled, messageDistributorType)} via {@link #resolvePinning}:
 *
 * <ul>
 *   <li>{@link ProducerPinning#NONE}: every producer's
 *       {@link VirtualProducer#pinnedPartitions()} is empty.</li>
 *   <li>{@link ProducerPinning#ONE_PER_PRODUCER} (correctness mode): producer
 *       at index {@code i} within a topic is pinned to partition
 *       {@code i mod partitionCount}. When {@code producersPerTopic <
 *       partitionCount}, the higher-numbered partitions receive no traffic
 *       (intentional). When {@code producersPerTopic > partitionCount},
 *       producers wrap modulo and multiple producers share a partition.</li>
 *   <li>{@link ProducerPinning#SPLIT_EVENLY} (PINNED_PARTITIONS): partitions
 *       are divided among producers as evenly as possible. With
 *       {@code partitionCount = N} and {@code producersPerTopic = P}: the
 *       first {@code N % P} producers get {@code ceil(N/P)} partitions, the
 *       rest get {@code floor(N/P)}. When {@code P > N}, producers wrap
 *       modulo so every producer ends up pinned to exactly one partition
 *       (and some partitions are shared by multiple producers).</li>
 * </ul>
 *
 * <h2>Consumer static partitions</h2>
 *
 * <p>When {@link TopologySpec#staticConsumerPartitions} is true, for each
 * {@code (topic, consumerGroup)} the consumers in the group are sorted by
 * {@code consumerIdx} and partitions are dealt out round-robin: consumer at
 * position {@code pos} in the group gets partitions {@code pos, pos +
 * groupSize, pos + 2*groupSize, …}. When false, every consumer's
 * {@link VirtualConsumer#staticPartitions()} is empty (dynamic group
 * assignment).
 */
public class TopologyPlanner {

    private static final Logger log = LoggerFactory.getLogger(TopologyPlanner.class);

    /** Suffix of the Flink stage's output topic for each input topic. */
    public static final String OUTPUT_TOPIC_SUFFIX = "-out";

    public enum WorkerMode { MIXED, SEPARATED }

    /** Constraint on how to split the worker pool in SEPARATED mode. */
    public sealed interface WorkerSplit permits RatioSplit, ExplicitSplit {}

    public record RatioSplit(int producers, int consumers) implements WorkerSplit {
        public RatioSplit {
            if (producers < 1 || consumers < 1) {
                throw new IllegalArgumentException("Ratio values must be >= 1");
            }
        }
    }

    public record ExplicitSplit(int producers, int consumers) implements WorkerSplit {
        public ExplicitSplit {
            if (producers < 1 || consumers < 1) {
                throw new IllegalArgumentException("Producer and consumer counts must be >= 1");
            }
        }
    }

    /** How a topic's partitions should be distributed across that topic's producers. */
    public enum ProducerPinning {
        /** No producer-level pinning. {@code KafkaProducerWrapper} defers to MessageDistributor / Kafka partitioner. */
        NONE,
        /** One partition per producer (modulo wrap if producers > partitions). Used in correctness mode. */
        ONE_PER_PRODUCER,
        /** Partitions split as evenly as possible across producers. Used by MessageDistributorType.PINNED_PARTITIONS. */
        SPLIT_EVENLY
    }

    /**
     * Inputs the planner needs to produce a complete topology. Topic names and
     * consumer-group names are <em>not</em> in this spec — the planner generates
     * them from {@link #topicPrefix} and {@code numTopics}/{@code consumerGroupsPerTopic}
     * counts so that the topology is the single source of truth for naming.
     */
    public record TopologySpec(
            String topicPrefix,
            int numTopics,
            int partitionsPerTopic,
            int producersPerTopic,
            int consumerGroupsPerTopic,
            int consumersPerGroup,
            ConsumerType consumerType,
            int consumerProcMsMin,
            int consumerProcMsMax,
            int consumerProcParallelism,
            ConsumerProcMode consumerProcMode,
            int consumerProcCapacity,
            MessageDistributorType messageDistributorType,
            double messageDistributorSkew,
            boolean correctnessEnabled,
            boolean staticConsumerPartitions,
            int outputPartitionsPerTopic) {

        /** A spec without a Flink stage. */
        public TopologySpec(String topicPrefix, int numTopics, int partitionsPerTopic, int producersPerTopic,
                            int consumerGroupsPerTopic, int consumersPerGroup, ConsumerType consumerType,
                            int consumerProcMsMin, int consumerProcMsMax, int consumerProcParallelism,
                            ConsumerProcMode consumerProcMode, int consumerProcCapacity,
                            MessageDistributorType messageDistributorType, double messageDistributorSkew,
                            boolean correctnessEnabled, boolean staticConsumerPartitions) {
            this(topicPrefix, numTopics, partitionsPerTopic, producersPerTopic, consumerGroupsPerTopic,
                    consumersPerGroup, consumerType, consumerProcMsMin, consumerProcMsMax, consumerProcParallelism,
                    consumerProcMode, consumerProcCapacity, messageDistributorType, messageDistributorSkew,
                    correctnessEnabled, staticConsumerPartitions, 0);
        }

        public TopologySpec {
            if (topicPrefix == null || topicPrefix.isEmpty()) {
                throw new IllegalArgumentException("topicPrefix must be non-empty");
            }
            if (consumerProcMode == ConsumerProcMode.CONTINUOUS && consumerType == ConsumerType.SHARE_GROUP) {
                throw new IllegalArgumentException(
                    "CONTINUOUS processing mode is incompatible with SHARE_GROUP consumers. " +
                    "Share groups use currentBatch for RENEW tracking, which breaks with " +
                    "out-of-order completion. Use BLOCKING mode or CONSUMER_GROUP.");
            }
            if (numTopics < 1) {
                throw new IllegalArgumentException("numTopics must be >= 1");
            }
            if (partitionsPerTopic < 1) {
                throw new IllegalArgumentException("partitionsPerTopic must be >= 1");
            }
            if (producersPerTopic < 0) {
                throw new IllegalArgumentException("producersPerTopic must be >= 0");
            }
            if (consumerGroupsPerTopic < 0) {
                throw new IllegalArgumentException("consumerGroupsPerTopic must be >= 0");
            }
            if (consumersPerGroup < 0) {
                throw new IllegalArgumentException("consumersPerGroup must be >= 0");
            }
            if (outputPartitionsPerTopic < 0) {
                throw new IllegalArgumentException("outputPartitionsPerTopic must be >= 0");
            }
            if (outputPartitionsPerTopic > 0 && correctnessEnabled) {
                // Correctness co-locates each topic's producers and consumers,
                // which assumes they share the topic.
                throw new IllegalArgumentException("A Flink stage is not supported with correctness mode");
            }
        }

        /** Whether a Flink stage sits between producers and consumers. */
        public boolean pipelined() {
            return outputPartitionsPerTopic > 0;
        }
    }

    private final List<String> allWorkers;
    private final List<String> producerWorkers;
    private final List<String> consumerWorkers;
    private final WorkerMode mode;
    /** Source of unique-per-call suffix strings used in generated topic and group names. */
    private final Supplier<String> randomSuffix;

    public TopologyPlanner(List<String> workers, WorkerMode mode, WorkerSplit split) {
        this(workers, mode, split, RandomGenerator::getRandomString);
    }

    /**
     * Test-friendly constructor: pass a deterministic {@code randomSuffix}
     * supplier (e.g. a counter) to make generated topic and group names
     * reproducible.
     */
    public TopologyPlanner(List<String> workers, WorkerMode mode, WorkerSplit split,
                           Supplier<String> randomSuffix) {
        if (workers.isEmpty()) {
            throw new IllegalArgumentException("Workers list must not be empty");
        }
        this.allWorkers = List.copyOf(workers);
        this.mode = mode;
        this.randomSuffix = randomSuffix;

        if (mode == WorkerMode.MIXED) {
            // Every worker can host both producers and consumers; the pools are aliases.
            this.producerWorkers = this.allWorkers;
            this.consumerWorkers = this.allWorkers;
        } else if (workers.size() == 1) {
            // SEPARATED with one worker degenerates to MIXED: one machine has to do both.
            this.producerWorkers = this.allWorkers;
            this.consumerWorkers = this.allWorkers;
        } else {
            int numberOfProducerWorkers = switch (split) {
                case ExplicitSplit e -> {
                    if (e.producers() + e.consumers() != workers.size()) {
                        throw new IllegalArgumentException(
                                "--producers (%d) + --consumers (%d) = %d, but there are %d workers"
                                        .formatted(e.producers(), e.consumers(),
                                                e.producers() + e.consumers(), workers.size()));
                    }
                    yield e.producers();
                }
                case RatioSplit r -> {
                    int total = r.producers() + r.consumers();
                    yield Math.max(1, (workers.size() * r.producers() + total - 1) / total);
                }
            };
            // Always keep at least one worker for the consumer pool so SEPARATED isn't trivially MIXED.
            numberOfProducerWorkers = Math.min(numberOfProducerWorkers, workers.size() - 1);
            this.producerWorkers = this.allWorkers.subList(0, numberOfProducerWorkers);
            this.consumerWorkers = this.allWorkers.subList(numberOfProducerWorkers, workers.size());
        }
    }

    public List<String> allWorkers() { return allWorkers; }
    public List<String> producerWorkers() { return producerWorkers; }
    public List<String> consumerWorkers() { return consumerWorkers; }
    public WorkerMode mode() { return mode; }

    /**
     * Resolves the {@link ProducerPinning} mode from the high-level scenario
     * settings. Single source of truth so engines and tests don't independently
     * re-derive this rule.
     */
    public static ProducerPinning resolvePinning(boolean correctness, MessageDistributorType type) {
        if (correctness) return ProducerPinning.ONE_PER_PRODUCER;
        if (type == MessageDistributorType.PINNED_PARTITIONS) return ProducerPinning.SPLIT_EVENLY;
        return ProducerPinning.NONE;
    }

    /** Build the complete topology for the given spec. */
    public VirtualTopology plan(TopologySpec spec) {
        return plan(spec, null);
    }

    /**
     * Build the topology over existing input topics, e.g. a drain-backlog
     * backlog reused from an earlier test point. Groups (and Flink output
     * topics) are still freshly named, so nothing else carries over.
     *
     * @param inputTopicNames the input topics, or null to name fresh ones
     */
    public VirtualTopology plan(TopologySpec spec, List<String> inputTopicNames) {
        if (inputTopicNames != null && inputTopicNames.size() != spec.numTopics()) {
            throw new IllegalArgumentException("Expected %d input topics, got %d"
                    .formatted(spec.numTopics(), inputTopicNames.size()));
        }
        // 1. Generate topic names. One random suffix per topic preserves the legacy
        //    BaseEngine behavior where every topic in a run got a fresh suffix —
        //    cheap insurance against name collisions with leftover state on the cluster.
        Map<String, Integer> topicPartitionCounts = new LinkedHashMap<>();
        for (int i = 0; i < spec.numTopics(); i++) {
            String name = inputTopicNames != null
                    ? inputTopicNames.get(i)
                    : String.format("%s-%s-%04d", spec.topicPrefix(), randomSuffix.get(), i);
            topicPartitionCounts.put(name, spec.partitionsPerTopic());
        }
        List<String> topicNames = new ArrayList<>(topicPartitionCounts.keySet());

        // 1b. With a Flink stage, each input topic gets an output topic, which
        //     consumers read instead. The "-out" suffix keeps it under the topic
        //     prefix, so test-topic cleanup removes it too. Over existing inputs
        //     the output also gets a fresh suffix: the previous test point's
        //     output of the same input may still be being deleted.
        Map<String, String> pipelineTopics = new LinkedHashMap<>();
        if (spec.pipelined()) {
            for (String topic : topicNames) {
                String output = inputTopicNames != null
                        ? topic + OUTPUT_TOPIC_SUFFIX + "-" + randomSuffix.get()
                        : topic + OUTPUT_TOPIC_SUFFIX;
                pipelineTopics.put(topic, output);
                topicPartitionCounts.put(output, spec.outputPartitionsPerTopic());
            }
        }
        List<String> consumerTopics = spec.pipelined() ? new ArrayList<>(pipelineTopics.values()) : topicNames;

        // 2. Generate per-(topic, groupIdx) consumer group names. Same naming format
        //    as the legacy code (random suffix per group to avoid reusing offsets
        //    from a prior run on the same cluster).
        Map<String, List<String>> groupsByTopic = new LinkedHashMap<>();
        for (String topic : consumerTopics) {
            List<String> groups = new ArrayList<>(spec.consumerGroupsPerTopic());
            for (int g = 0; g < spec.consumerGroupsPerTopic(); g++) {
                groups.add(String.format("dimster-grp-%03d-%s", g, randomSuffix.get()));
            }
            groupsByTopic.put(topic, groups);
        }

        // 3. Topic-to-worker affinity (only meaningful in correctness mode; null otherwise).
        Map<String, String> topicAffinity = spec.correctnessEnabled()
                ? computeTopicAffinity(topicNames)
                : null;

        // 4. Build per-instance virtual producers and consumers.
        ProducerPinning pinning = resolvePinning(spec.correctnessEnabled(), spec.messageDistributorType());

        // PINNED_PARTITIONS only delivers equal load per partition when the producer count
        // and partition count are mutually divisible. Snap producersPerTopic to the nearest
        // valid value so a misconfiguration produces predictable balanced load instead of
        // silent skew. The user's TopologySpec is left untouched — we only override the
        // value used for producer construction.
        int effectiveProducersPerTopic = spec.producersPerTopic();
        if (pinning == ProducerPinning.SPLIT_EVENLY && spec.producersPerTopic() > 0) {
            int adjusted = adjustProducersForBalance(spec.producersPerTopic(), spec.partitionsPerTopic());
            if (adjusted != spec.producersPerTopic()) {
                log.warn(
                        "\n"
                        + "============================================================\n"
                        + " PINNED_PARTITIONS BALANCE ENFORCEMENT\n"
                        + "============================================================\n"
                        + " Requested producersPerTopic={} with partitionsPerTopic={}\n"
                        + " cannot deliver equal load per partition.\n"
                        + " Adjusting producersPerTopic: {} -> {}\n"
                        + " (valid: any divisor of {} or any multiple of it)\n"
                        + "============================================================",
                        spec.producersPerTopic(), spec.partitionsPerTopic(),
                        spec.producersPerTopic(), adjusted, spec.partitionsPerTopic());
                effectiveProducersPerTopic = adjusted;
            }
        }

        List<VirtualProducer> producers = buildProducers(
                spec, topicNames, pinning, topicAffinity, effectiveProducersPerTopic);
        List<VirtualConsumer> consumers = buildConsumers(
                spec, consumerTopics, topicPartitionCounts, groupsByTopic, topicAffinity);

        int activeProducerWorkerCount = (int) producers.stream()
                .map(VirtualProducer::worker)
                .distinct()
                .count();

        return new VirtualTopology(
                topicPartitionCounts,
                producers,
                consumers,
                spec.consumerType(),
                spec.consumerProcMsMin(),
                spec.consumerProcMsMax(),
                spec.consumerProcParallelism(),
                spec.consumerProcMode(),
                spec.consumerProcCapacity(),
                spec.messageDistributorType(),
                spec.messageDistributorSkew(),
                activeProducerWorkerCount,
                pipelineTopics);
    }

    // ------------- worker assignment helpers -------------

    /**
     * In correctness mode, each topic is permanently assigned to one producer
     * worker via round-robin over the producer pool. The mapping is deterministic
     * (depends only on topic order) and is reused for both producers and
     * consumers so they co-locate.
     */
    private Map<String, String> computeTopicAffinity(List<String> topicNames) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < topicNames.size(); i++) {
            map.put(topicNames.get(i), producerWorkers.get(i % producerWorkers.size()));
        }
        return map;
    }

    // ------------- producer construction -------------

    private List<VirtualProducer> buildProducers(TopologySpec spec, List<String> topicNames,
                                                  ProducerPinning pinning,
                                                  Map<String, String> topicAffinity,
                                                  int effectiveProducersPerTopic) {
        if (effectiveProducersPerTopic == 0) return List.of();

        List<VirtualProducer> producers = new ArrayList<>();
        // Iterate (topic, producerIdx) in natural order so round-robin worker assignment
        // matches the legacy expanded-list behavior exactly.
        int globalIdx = 0;
        for (String topic : topicNames) {
            int[][] pinningForTopic = computePinning(
                    effectiveProducersPerTopic, spec.partitionsPerTopic(), pinning);

            for (int producerIdx = 0; producerIdx < effectiveProducersPerTopic; producerIdx++) {
                String worker = spec.correctnessEnabled()
                        ? topicAffinity.get(topic)
                        : producerWorkers.get(globalIdx % producerWorkers.size());
                producers.add(new VirtualProducer(
                        worker, topic, producerIdx, pinningForTopic[producerIdx]));
                globalIdx++;
            }
        }
        return producers;
    }

    /**
     * SPLIT_EVENLY (PINNED_PARTITIONS) only delivers equal load per partition
     * when one of {@code producerCount} or {@code partitionCount} divides the
     * other. Anything else produces silent skew. This helper snaps
     * {@code producerCount} to the closest valid value — either a divisor of
     * {@code partitionCount} (when {@code producers <= partitions}) or a
     * multiple of it (when {@code producers > partitions}). Ties are broken
     * by preferring the smaller (downgrade) value so we never silently spin
     * up more producers than the user requested.
     *
     * <p>Returns {@code 0} unchanged — the caller is expected to skip the
     * adjustment for consumer-only test points.
     */
    static int adjustProducersForBalance(int producerCount, int partitionCount) {
        if (producerCount == 0) return 0;
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be >= 1");
        }
        // Already balanced: one count divides the other.
        if (partitionCount % producerCount == 0 || producerCount % partitionCount == 0) {
            return producerCount;
        }

        int down;
        int up;
        if (producerCount < partitionCount) {
            // Search divisors of partitionCount on either side of producerCount.
            // partitionCount itself is always a divisor, so `up` is bounded by it.
            down = largestDivisorAtMost(partitionCount, producerCount);
            up = smallestDivisorAtLeast(partitionCount, producerCount);
        } else {
            // producerCount > partitionCount and not a multiple — snap to nearest multiple.
            int q = producerCount / partitionCount;
            down = q * partitionCount;
            up = down + partitionCount;
        }

        int distDown = producerCount - down;
        int distUp = up - producerCount;
        // Tie-break: prefer the smaller value (downgrade).
        return distDown <= distUp ? down : up;
    }

    /** Largest divisor of {@code n} that is {@code <= bound}. Always returns at least 1. */
    private static int largestDivisorAtMost(int n, int bound) {
        for (int d = bound; d >= 1; d--) {
            if (n % d == 0) return d;
        }
        return 1;
    }

    /** Smallest divisor of {@code n} that is {@code >= bound}. Capped at {@code n}. */
    private static int smallestDivisorAtLeast(int n, int bound) {
        for (int d = bound; d <= n; d++) {
            if (n % d == 0) return d;
        }
        return n;
    }

    /**
     * Compute the pinned partition list for each producer index in a topic.
     * Returns {@code int[producerCount][]} where entry {@code i} is the
     * partitions assigned to the producer at index {@code i}.
     */
    static int[][] computePinning(int producerCount, int partitionCount, ProducerPinning mode) {
        int[][] out = new int[producerCount][];
        switch (mode) {
            case NONE -> {
                for (int i = 0; i < producerCount; i++) out[i] = new int[0];
            }
            case ONE_PER_PRODUCER -> {
                // Each producer gets a single partition, modulo wrap if producers > partitions.
                // When producerCount <= partitionCount, the higher-numbered partitions are
                // intentionally left without traffic (correctness mode contract).
                for (int i = 0; i < producerCount; i++) {
                    out[i] = new int[] { i % partitionCount };
                }
            }
            case SPLIT_EVENLY -> {
                if (producerCount <= partitionCount) {
                    // Split partitions [0..partitionCount) into producerCount contiguous chunks.
                    // The first (partitionCount % producerCount) producers get one extra partition
                    // so the assignment is as balanced as possible.
                    int base = partitionCount / producerCount;
                    int extra = partitionCount % producerCount;
                    int p = 0;
                    for (int i = 0; i < producerCount; i++) {
                        int size = base + (i < extra ? 1 : 0);
                        out[i] = new int[size];
                        for (int k = 0; k < size; k++) out[i][k] = p++;
                    }
                } else {
                    // More producers than partitions: every producer gets exactly one partition,
                    // wrapping modulo. Some partitions end up shared by multiple producers.
                    for (int i = 0; i < producerCount; i++) {
                        out[i] = new int[] { i % partitionCount };
                    }
                }
            }
        }
        return out;
    }

    // ------------- consumer construction -------------

    private List<VirtualConsumer> buildConsumers(TopologySpec spec, List<String> topicNames,
                                                  Map<String, Integer> topicPartitionCounts,
                                                  Map<String, List<String>> groupsByTopic,
                                                  Map<String, String> topicAffinity) {
        if (spec.consumerGroupsPerTopic() == 0 || spec.consumersPerGroup() == 0) return List.of();

        List<VirtualConsumer> consumers = new ArrayList<>();
        int globalIdx = 0;
        for (String topic : topicNames) {
            for (String group : groupsByTopic.get(topic)) {
                // Pre-compute static partitions for this (topic, group) once, then deal them out by consumerIdx.
                List<List<TopicPartition>> staticByConsumerIdx = spec.staticConsumerPartitions()
                        ? roundRobinPartitions(topic, topicPartitionCounts.get(topic), spec.consumersPerGroup())
                        : null;

                for (int consumerIdx = 0; consumerIdx < spec.consumersPerGroup(); consumerIdx++) {
                    String worker = spec.correctnessEnabled()
                            ? topicAffinity.get(topic)
                            : consumerWorkers.get(globalIdx % consumerWorkers.size());
                    List<TopicPartition> staticPartitions = staticByConsumerIdx != null
                            ? staticByConsumerIdx.get(consumerIdx)
                            : List.of();
                    consumers.add(new VirtualConsumer(
                            worker, topic, group, consumerIdx, staticPartitions));
                    globalIdx++;
                }
            }
        }
        return consumers;
    }

    /**
     * Round-robin a topic's partitions across {@code groupSize} consumers.
     * Consumer at position {@code pos} gets partitions {@code pos, pos +
     * groupSize, pos + 2*groupSize, …}. When {@code groupSize > partitionCount}
     * the trailing consumers get empty partition lists (idle).
     */
    private static List<List<TopicPartition>> roundRobinPartitions(String topic, int partitionCount, int groupSize) {
        List<List<TopicPartition>> out = new ArrayList<>(groupSize);
        for (int pos = 0; pos < groupSize; pos++) {
            List<TopicPartition> mine = new ArrayList<>();
            for (int p = pos; p < partitionCount; p += groupSize) {
                mine.add(new TopicPartition(topic, p));
            }
            out.add(Collections.unmodifiableList(mine));
        }
        return out;
    }
}
