package dev.dimster.worker.topology;

import static org.junit.Assert.*;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import dev.dimster.ConsumerProcMode;
import dev.dimster.ConsumerType;
import dev.dimster.utils.distributor.MessageDistributorType;
import dev.dimster.worker.topology.TopologyPlanner.ExplicitSplit;
import dev.dimster.worker.topology.TopologyPlanner.ProducerPinning;
import dev.dimster.worker.topology.TopologyPlanner.RatioSplit;
import dev.dimster.worker.topology.TopologyPlanner.TopologySpec;
import dev.dimster.worker.topology.TopologyPlanner.WorkerMode;

/**
 * Tests for {@link TopologyPlanner}. Covers:
 *
 * <ul>
 *   <li>Worker-pool / split semantics (ported from the legacy
 *       {@code WorkerAssignmentPlannerTest}).</li>
 *   <li>Topic and consumer-group name generation owned by the planner.</li>
 *   <li>Round-robin and correctness-affinity worker assignment.</li>
 *   <li>Per-producer partition pinning rules
 *       (NONE / ONE_PER_PRODUCER / SPLIT_EVENLY) and the
 *       {@code (correctness, messageDistributorType) → pinning} resolution.</li>
 *   <li>Per-consumer static partition assignment.</li>
 * </ul>
 *
 * <p>Tests use a deterministic counter for the random suffix so generated
 * topic and group names are reproducible.
 */
public class TopologyPlannerTest {

    // =========================================================================
    // Worker pool / split tests
    // =========================================================================

    @Test
    public void singleWorkerSeparatedSharesProducerAndConsumer() {
        var planner = newPlanner(List.of("w1"), WorkerMode.SEPARATED, new RatioSplit(1, 1));
        assertEquals(List.of("w1"), planner.producerWorkers());
        assertEquals(List.of("w1"), planner.consumerWorkers());
    }

    @Test
    public void ratioSplitEvenSplit() {
        var planner = newPlanner(List.of("w1", "w2", "w3", "w4"),
                WorkerMode.SEPARATED, new RatioSplit(1, 1));
        assertEquals(List.of("w1", "w2"), planner.producerWorkers());
        assertEquals(List.of("w3", "w4"), planner.consumerWorkers());
    }

    @Test
    public void ratioSplitOneToTwo() {
        var planner = newPlanner(List.of("w1", "w2", "w3"),
                WorkerMode.SEPARATED, new RatioSplit(1, 2));
        assertEquals(List.of("w1"), planner.producerWorkers());
        assertEquals(List.of("w2", "w3"), planner.consumerWorkers());
    }

    @Test
    public void explicitSplit() {
        var planner = newPlanner(List.of("w1", "w2", "w3", "w4"),
                WorkerMode.SEPARATED, new ExplicitSplit(1, 3));
        assertEquals(List.of("w1"), planner.producerWorkers());
        assertEquals(List.of("w2", "w3", "w4"), planner.consumerWorkers());
    }

    @Test(expected = IllegalArgumentException.class)
    public void explicitSplitMismatchThrows() {
        newPlanner(List.of("w1", "w2", "w3"),
                WorkerMode.SEPARATED, new ExplicitSplit(1, 1));
    }

    @Test
    public void mixedModeAllWorkersAreProducersAndConsumers() {
        var planner = newPlanner(List.of("w1", "w2", "w3"),
                WorkerMode.MIXED, new RatioSplit(1, 1));
        assertEquals(List.of("w1", "w2", "w3"), planner.producerWorkers());
        assertEquals(List.of("w1", "w2", "w3"), planner.consumerWorkers());
    }

    @Test
    public void mixedModeSingleWorkerWorks() {
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        assertEquals(List.of("w1"), planner.producerWorkers());
        assertEquals(List.of("w1"), planner.consumerWorkers());
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyWorkersThrows() {
        newPlanner(List.of(), WorkerMode.SEPARATED, new RatioSplit(1, 1));
    }

    // =========================================================================
    // Topic + consumer group name generation
    // =========================================================================

    @Test
    public void plan_generatesTopicNamesFromPrefixAndCounter() {
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(spec("dimster", 3, 2, 1, 0, 0, false));

        // Suffix counter starts at "s1" (incremented per call), one suffix per topic.
        assertEquals(List.of("dimster-s1-0000", "dimster-s2-0001", "dimster-s3-0002"),
                List.copyOf(topology.topicPartitionCounts().keySet()));
        for (var entry : topology.topicPartitionCounts().entrySet()) {
            assertEquals(Integer.valueOf(2), entry.getValue());
        }
    }

    @Test
    public void plan_generatesConsumerGroupNamesPerTopicAndGroupIdx() {
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(spec("p", 1, 4, 0, 2, 1, false));

        // 1 topic uses suffix s1; then 2 groups use suffix s2 and s3.
        var consumers = topology.consumers();
        assertEquals(2, consumers.size());
        assertEquals("dimster-grp-000-s2", consumers.get(0).consumerGroup());
        assertEquals("dimster-grp-001-s3", consumers.get(1).consumerGroup());
        assertEquals("p-s1-0000", consumers.get(0).topic());
    }

    // =========================================================================
    // Round-robin worker assignment (non-correctness)
    // =========================================================================

    @Test
    public void roundRobin_producersSeparatedMode() {
        var planner = newPlanner(List.of("w1", "w2", "w3", "w4"),
                WorkerMode.SEPARATED, new RatioSplit(1, 1));
        // Producer pool is [w1, w2]. With 5 single-producer topics, the (topic, idx)
        // sequence is t0..t4; round-robin lands them on w1, w2, w1, w2, w1.
        VirtualTopology topology = planner.plan(spec("p", 5, 4, 1, 0, 0, false));

        assertEquals(2, topology.activeProducerWorkerCount());
        assertEquals(3, topology.producersForWorker("w1").size());
        assertEquals(2, topology.producersForWorker("w2").size());
    }

    @Test
    public void roundRobin_producersMixedMode() {
        var planner = newPlanner(List.of("w1", "w2", "w3"),
                WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(spec("p", 6, 4, 1, 0, 0, false));

        assertEquals(3, topology.activeProducerWorkerCount());
        assertEquals(2, topology.producersForWorker("w1").size());
        assertEquals(2, topology.producersForWorker("w2").size());
        assertEquals(2, topology.producersForWorker("w3").size());
    }

    @Test
    public void roundRobin_moreWorkersThanTopics() {
        var planner = newPlanner(List.of("w1", "w2", "w3", "w4", "w5", "w6"),
                WorkerMode.SEPARATED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(spec("p", 2, 4, 1, 0, 0, false));

        // Producer pool is [w1, w2, w3]; only the first two are reached.
        assertEquals(2, topology.activeProducerWorkerCount());
        assertTrue(topology.producersForWorker("w3").isEmpty());
    }

    @Test
    public void roundRobin_consumersSeparatedMode() {
        var planner = newPlanner(List.of("w1", "w2", "w3", "w4"),
                WorkerMode.SEPARATED, new RatioSplit(1, 1));
        // Consumer pool is [w3, w4]. 3 topics x 1 group x 1 consumer round-robin
        // across [w3, w4]: t0->w3, t1->w4, t2->w3.
        VirtualTopology topology = planner.plan(specType(
                "p", 3, 4, 0, 1, 1, ConsumerType.SHARE_GROUP, false));

        assertEquals(2, topology.consumersForWorker("w3").size());
        assertEquals(1, topology.consumersForWorker("w4").size());
        assertEquals(ConsumerType.SHARE_GROUP, topology.consumerType());
    }

    @Test
    public void roundRobin_consumersMixedMode() {
        var planner = newPlanner(List.of("w1", "w2", "w3"),
                WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(spec("p", 4, 4, 0, 1, 1, false));

        assertEquals(2, topology.consumersForWorker("w1").size());
        assertEquals(1, topology.consumersForWorker("w2").size());
        assertEquals(1, topology.consumersForWorker("w3").size());
    }

    // =========================================================================
    // Correctness mode: topic affinity
    // =========================================================================

    @Test
    public void correctness_producersAndConsumers_sameWorkerPerTopic() {
        var planner = newPlanner(List.of("w1", "w2", "w3"),
                WorkerMode.MIXED, new RatioSplit(1, 1));

        // 3 topics, 2 producers each, 1 group of 1 consumer each.
        VirtualTopology topology = planner.plan(spec("p", 3, 4, 2, 1, 1, /*correctness*/ true));

        for (String topic : topology.topicPartitionCounts().keySet()) {
            assertEquals("Topic " + topic + " producers and consumers must be on same worker",
                    oneWorkerForProducerTopic(topology, topic),
                    oneWorkerForConsumerTopic(topology, topic));
        }
    }

    @Test
    public void correctness_separatedMode_consumersGoToProducerWorkers() {
        var planner = newPlanner(List.of("w1", "w2", "w3", "w4"),
                WorkerMode.SEPARATED, new RatioSplit(1, 1));

        VirtualTopology topology = planner.plan(spec("p", 2, 4, 1, 1, 1, /*correctness*/ true));

        // In correctness mode, consumers must follow their producer onto the producer pool.
        Set<String> consumerWorkers = topology.consumers().stream()
                .map(VirtualConsumer::worker)
                .collect(Collectors.toSet());
        assertTrue(planner.producerWorkers().containsAll(consumerWorkers));
        // The consumer pool (w3, w4) should not appear at all.
        assertFalse(consumerWorkers.contains("w3"));
        assertFalse(consumerWorkers.contains("w4"));
    }

    @Test
    public void correctness_topicAffinityDeterministicAcrossPlanCalls() {
        // Two separate planner instances with the same deterministic suffix supplier
        // produce identical topologies — proving topology assignment is a pure function
        // of the spec + planner config.
        var planner1 = newPlanner(List.of("w1", "w2", "w3"),
                WorkerMode.MIXED, new RatioSplit(1, 1));
        var planner2 = newPlanner(List.of("w1", "w2", "w3"),
                WorkerMode.MIXED, new RatioSplit(1, 1));

        TopologySpec s = spec("p", 3, 4, 2, 1, 1, /*correctness*/ true);
        VirtualTopology a = planner1.plan(s);
        VirtualTopology b = planner2.plan(s);

        for (String topic : a.topicPartitionCounts().keySet()) {
            assertEquals(oneWorkerForProducerTopic(a, topic),
                    oneWorkerForProducerTopic(b, topic));
        }
    }

    @Test
    public void correctnessFlag_isPerSpec_notSharedAcrossCalls() {
        // Same planner: a non-correctness spec should NOT inherit topic affinity from
        // a previous correctness spec.
        var planner = newPlanner(List.of("w1", "w2", "w3", "w4"),
                WorkerMode.SEPARATED, new RatioSplit(1, 1));

        // First plan with correctness=true.
        planner.plan(spec("p", 2, 4, 1, 1, 1, true));

        // Second plan with correctness=false — consumers should land on the consumer pool.
        VirtualTopology topology = planner.plan(spec("p", 2, 4, 1, 1, 1, false));
        Set<String> consumerWorkers = topology.consumers().stream()
                .map(VirtualConsumer::worker).collect(Collectors.toSet());
        assertTrue(consumerWorkers.contains("w3") || consumerWorkers.contains("w4"));
        assertFalse(consumerWorkers.contains("w1"));
        assertFalse(consumerWorkers.contains("w2"));
    }

    // =========================================================================
    // Producer partition pinning
    // =========================================================================

    @Test
    public void pinning_none_emptyArrays() {
        int[][] pin = TopologyPlanner.computePinning(4, 8, ProducerPinning.NONE);
        for (int i = 0; i < 4; i++) assertEquals(0, pin[i].length);
    }

    @Test
    public void pinning_onePerProducer_lessProducersThanPartitions() {
        // 5 producers, 10 partitions: producers 0..4 get [0]..[4]; partitions 5..9 unused.
        int[][] pin = TopologyPlanner.computePinning(5, 10, ProducerPinning.ONE_PER_PRODUCER);
        for (int i = 0; i < 5; i++) {
            assertArrayEquals(new int[] { i }, pin[i]);
        }
    }

    @Test
    public void pinning_onePerProducer_equalCounts() {
        int[][] pin = TopologyPlanner.computePinning(4, 4, ProducerPinning.ONE_PER_PRODUCER);
        for (int i = 0; i < 4; i++) {
            assertArrayEquals(new int[] { i }, pin[i]);
        }
    }

    @Test
    public void pinning_onePerProducer_moreProducersThanPartitions_wrapsModulo() {
        // 5 producers, 3 partitions: 0->[0], 1->[1], 2->[2], 3->[0], 4->[1] (modulo wrap).
        int[][] pin = TopologyPlanner.computePinning(5, 3, ProducerPinning.ONE_PER_PRODUCER);
        assertArrayEquals(new int[] { 0 }, pin[0]);
        assertArrayEquals(new int[] { 1 }, pin[1]);
        assertArrayEquals(new int[] { 2 }, pin[2]);
        assertArrayEquals(new int[] { 0 }, pin[3]);
        assertArrayEquals(new int[] { 1 }, pin[4]);
    }

    @Test
    public void pinning_splitEvenly_evenSplit() {
        int[][] pin = TopologyPlanner.computePinning(10, 10, ProducerPinning.SPLIT_EVENLY);
        for (int i = 0; i < 10; i++) {
            assertArrayEquals(new int[] { i }, pin[i]);
        }
    }

    @Test
    public void pinning_splitEvenly_unevenSplit() {
        // 3 producers, 10 partitions: ceil 4, ceil 3 (no, base=3 + first extra=1), floor 3.
        // Producer 0: [0,1,2,3]; 1: [4,5,6]; 2: [7,8,9].
        int[][] pin = TopologyPlanner.computePinning(3, 10, ProducerPinning.SPLIT_EVENLY);
        assertArrayEquals(new int[] { 0, 1, 2, 3 }, pin[0]);
        assertArrayEquals(new int[] { 4, 5, 6 }, pin[1]);
        assertArrayEquals(new int[] { 7, 8, 9 }, pin[2]);
    }

    @Test
    public void pinning_splitEvenly_singleProducer_getsAllPartitions() {
        int[][] pin = TopologyPlanner.computePinning(1, 6, ProducerPinning.SPLIT_EVENLY);
        assertArrayEquals(new int[] { 0, 1, 2, 3, 4, 5 }, pin[0]);
    }

    @Test
    public void pinning_splitEvenly_moreProducersThanPartitions_wrapsModulo() {
        // 5 producers, 3 partitions: each producer pinned to exactly one partition (modulo wrap).
        int[][] pin = TopologyPlanner.computePinning(5, 3, ProducerPinning.SPLIT_EVENLY);
        assertArrayEquals(new int[] { 0 }, pin[0]);
        assertArrayEquals(new int[] { 1 }, pin[1]);
        assertArrayEquals(new int[] { 2 }, pin[2]);
        assertArrayEquals(new int[] { 0 }, pin[3]);
        assertArrayEquals(new int[] { 1 }, pin[4]);
    }

    // =========================================================================
    // PINNED_PARTITIONS balance enforcement (adjustProducersForBalance)
    // =========================================================================

    @Test
    public void adjustProducersForBalance_alreadyBalanced_returnsInput() {
        // Each of these is either a divisor of 10 or a multiple of 10 — already balanced.
        assertEquals(1, TopologyPlanner.adjustProducersForBalance(1, 10));
        assertEquals(2, TopologyPlanner.adjustProducersForBalance(2, 10));
        assertEquals(5, TopologyPlanner.adjustProducersForBalance(5, 10));
        assertEquals(10, TopologyPlanner.adjustProducersForBalance(10, 10));
        assertEquals(20, TopologyPlanner.adjustProducersForBalance(20, 10));
        assertEquals(30, TopologyPlanner.adjustProducersForBalance(30, 10));
    }

    @Test
    public void adjustProducersForBalance_zeroProducers_returnsZero() {
        assertEquals(0, TopologyPlanner.adjustProducersForBalance(0, 10));
    }

    @Test
    public void adjustProducersForBalance_smallerThanPartitions_picksClosestDivisor_downgrade() {
        // 3 producers, 10 partitions: divisors of 10 around 3 are 2 and 5.
        // Distance: 3->2 = 1, 3->5 = 2. Downgrade to 2.
        assertEquals(2, TopologyPlanner.adjustProducersForBalance(3, 10));
    }

    @Test
    public void adjustProducersForBalance_smallerThanPartitions_picksClosestDivisor_upgrade() {
        // 4 producers, 10 partitions: divisors around 4 are 2 and 5.
        // Distance: 4->2 = 2, 4->5 = 1. Upgrade to 5.
        assertEquals(5, TopologyPlanner.adjustProducersForBalance(4, 10));
    }

    @Test
    public void adjustProducersForBalance_largerThanPartitions_picksClosestMultiple_downgrade() {
        // 12 producers, 10 partitions: multiples 10 and 20.
        // Distance: 12->10 = 2, 12->20 = 8. Downgrade to 10.
        assertEquals(10, TopologyPlanner.adjustProducersForBalance(12, 10));
    }

    @Test
    public void adjustProducersForBalance_largerThanPartitions_picksClosestMultiple_upgrade() {
        // 18 producers, 10 partitions: multiples 10 and 20.
        // Distance: 18->10 = 8, 18->20 = 2. Upgrade to 20.
        assertEquals(20, TopologyPlanner.adjustProducersForBalance(18, 10));
    }

    @Test
    public void adjustProducersForBalance_tie_prefersDowngrade() {
        // 15 producers, 10 partitions: 10 and 20 are both distance 5.
        // Tie-break: prefer downgrade (smaller).
        assertEquals(10, TopologyPlanner.adjustProducersForBalance(15, 10));
    }

    @Test
    public void adjustProducersForBalance_primePartitions_collapsesToOne() {
        // 7 partitions has only divisors {1, 7}. 4 producers: distance 4->1 = 3, 4->7 = 3.
        // Tie -> downgrade to 1.
        assertEquals(1, TopologyPlanner.adjustProducersForBalance(4, 7));
        // 5 producers: distance 5->1 = 4, 5->7 = 2. Upgrade to 7.
        assertEquals(7, TopologyPlanner.adjustProducersForBalance(5, 7));
    }

    @Test
    public void adjustProducersForBalance_singlePartition_anyProducerCountIsValid() {
        // partitionCount=1 means every integer is a multiple of 1 — nothing to adjust.
        assertEquals(1, TopologyPlanner.adjustProducersForBalance(1, 1));
        assertEquals(7, TopologyPlanner.adjustProducersForBalance(7, 1));
        assertEquals(100, TopologyPlanner.adjustProducersForBalance(100, 1));
    }

    @Test
    public void resolvePinning_correctnessForcesOnePerProducer() {
        assertEquals(ProducerPinning.ONE_PER_PRODUCER,
                TopologyPlanner.resolvePinning(true, MessageDistributorType.NO_KEY));
        assertEquals(ProducerPinning.ONE_PER_PRODUCER,
                TopologyPlanner.resolvePinning(true, MessageDistributorType.PINNED_PARTITIONS));
        assertEquals(ProducerPinning.ONE_PER_PRODUCER,
                TopologyPlanner.resolvePinning(true, MessageDistributorType.KEY_ROUND_ROBIN));
    }

    @Test
    public void resolvePinning_pinnedPartitionsTriggersSplitEvenly() {
        assertEquals(ProducerPinning.SPLIT_EVENLY,
                TopologyPlanner.resolvePinning(false, MessageDistributorType.PINNED_PARTITIONS));
    }

    @Test
    public void resolvePinning_otherwiseNone() {
        assertEquals(ProducerPinning.NONE,
                TopologyPlanner.resolvePinning(false, MessageDistributorType.NO_KEY));
        assertEquals(ProducerPinning.NONE,
                TopologyPlanner.resolvePinning(false, MessageDistributorType.KEY_ZIPF));
        assertEquals(ProducerPinning.NONE,
                TopologyPlanner.resolvePinning(false, MessageDistributorType.PARTITION_ROUND_ROBIN));
    }

    @Test
    public void plan_pinningCarriedOnVirtualProducers() {
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        // 1 topic, 6 partitions, 3 producers, PINNED_PARTITIONS triggers SPLIT_EVENLY -> 2 each.
        VirtualTopology topology = planner.plan(specWithKey(
                "p", 1, 6, 3, 0, 0, ConsumerType.CONSUMER_GROUP,
                MessageDistributorType.PINNED_PARTITIONS, false, false));

        var producers = topology.producersForWorker("w1");
        assertEquals(3, producers.size());
        assertArrayEquals(new int[] { 0, 1 }, producers.get(0).pinnedPartitions());
        assertArrayEquals(new int[] { 2, 3 }, producers.get(1).pinnedPartitions());
        assertArrayEquals(new int[] { 4, 5 }, producers.get(2).pinnedPartitions());
    }

    @Test
    public void plan_correctnessOverridesPinnedPartitionsKey() {
        // Even when messageDistributor is PINNED_PARTITIONS, correctness mode forces
        // ONE_PER_PRODUCER pinning (one partition per producer).
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(specWithKey(
                "p", 1, 6, 3, 0, 0, ConsumerType.CONSUMER_GROUP,
                MessageDistributorType.PINNED_PARTITIONS, /*correctness*/ true, false));

        var producers = topology.producersForWorker("w1");
        for (int i = 0; i < 3; i++) {
            assertEquals(1, producers.get(i).pinnedPartitions().length);
            assertEquals(i, producers.get(i).pinnedPartitions()[0]);
        }
    }

    @Test
    public void plan_pinnedPartitions_imbalancedProducers_downgradesProducerCount() {
        // PINNED_PARTITIONS with 3x10 (imbalanced). Snaps down to 2 producers (closest valid).
        // Each producer then owns 5 partitions: producer 0 -> [0..4], producer 1 -> [5..9].
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(specWithKey(
                "p", 1, 10, 3, 0, 0, ConsumerType.CONSUMER_GROUP,
                MessageDistributorType.PINNED_PARTITIONS, false, false));

        var producers = topology.producersForWorker("w1");
        assertEquals(2, producers.size());
        assertArrayEquals(new int[] { 0, 1, 2, 3, 4 }, producers.get(0).pinnedPartitions());
        assertArrayEquals(new int[] { 5, 6, 7, 8, 9 }, producers.get(1).pinnedPartitions());
    }

    @Test
    public void plan_pinnedPartitions_moreProducersThanPartitions_snapsToMultiple() {
        // PINNED_PARTITIONS with 7x4 (imbalanced). Multiples of 4 around 7: 4 and 8.
        // Distance 7->4 = 3, 7->8 = 1. Upgrade to 8 producers, each partition owned by 2.
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(specWithKey(
                "p", 1, 4, 7, 0, 0, ConsumerType.CONSUMER_GROUP,
                MessageDistributorType.PINNED_PARTITIONS, false, false));

        var producers = topology.producersForWorker("w1");
        assertEquals(8, producers.size());
        // Each producer pinned to exactly one partition (P > N branch in computePinning).
        for (int i = 0; i < 8; i++) {
            assertArrayEquals(new int[] { i % 4 }, producers.get(i).pinnedPartitions());
        }
    }

    @Test
    public void plan_pinnedPartitions_balancedProducers_unchanged() {
        // 5 producers x 10 partitions: 5 divides 10, no adjustment.
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(specWithKey(
                "p", 1, 10, 5, 0, 0, ConsumerType.CONSUMER_GROUP,
                MessageDistributorType.PINNED_PARTITIONS, false, false));

        assertEquals(5, topology.producersForWorker("w1").size());
    }

    @Test
    public void plan_pinnedPartitions_zeroProducers_noAdjustment() {
        // 0 producers (consumer-only run): adjustment skipped, no producers built.
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(specWithKey(
                "p", 1, 10, 0, 1, 1, ConsumerType.CONSUMER_GROUP,
                MessageDistributorType.PINNED_PARTITIONS, false, false));

        assertTrue(topology.producersForWorker("w1").isEmpty());
    }

    @Test
    public void plan_correctnessMode_imbalancedProducers_notAdjusted() {
        // Correctness forces ONE_PER_PRODUCER pinning, which has its own contract
        // (allows idle partitions). The balance enforcement must NOT apply.
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(specWithKey(
                "p", 1, 10, 3, 0, 0, ConsumerType.CONSUMER_GROUP,
                MessageDistributorType.PINNED_PARTITIONS, /*correctness*/ true, false));

        // 3 producers preserved, partitions 3..9 intentionally idle.
        assertEquals(3, topology.producersForWorker("w1").size());
    }

    @Test
    public void plan_nonPinnedDistributor_imbalancedProducers_notAdjusted() {
        // NO_KEY (or any non-PINNED_PARTITIONS) resolves to ProducerPinning.NONE — the
        // adjustment must not run for these modes.
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(specWithKey(
                "p", 1, 10, 3, 0, 0, ConsumerType.CONSUMER_GROUP,
                MessageDistributorType.NO_KEY, false, false));

        assertEquals(3, topology.producersForWorker("w1").size());
    }

    @Test
    public void plan_producerNamesAndTransactionalIds_areIndexBased() {
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(spec("orders", 1, 4, 2, 0, 0, false));

        // Topic is generated as "orders-s1-0000".
        var producers = topology.producersForWorker("w1");
        String topic = producers.get(0).topic();
        assertEquals("producer-" + topic + "-i0", producers.get(0).name());
        assertEquals("producer-" + topic + "-i1", producers.get(1).name());
        assertEquals("dimster-host-" + topic + "-i0", producers.get(0).transactionalId("host"));
        assertEquals("dimster-host-" + topic + "-i1", producers.get(1).transactionalId("host"));
    }

    // =========================================================================
    // Consumer static partition assignment
    // =========================================================================

    @Test
    public void staticConsumerPartitions_roundRobinAcrossGroup() {
        // 1 topic, 6 partitions, 1 group of 3 consumers, static assignment on.
        // Consumer 0 -> [0, 3]; consumer 1 -> [1, 4]; consumer 2 -> [2, 5].
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(specWithKey(
                "p", 1, 6, 0, 1, 3, ConsumerType.CONSUMER_GROUP,
                MessageDistributorType.NO_KEY, false, /*staticConsumerPartitions*/ true));

        var consumers = topology.consumers();
        String topic = consumers.get(0).topic();
        assertEquals(3, consumers.size());
        assertEquals(List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 3)),
                consumers.get(0).staticPartitions());
        assertEquals(List.of(new TopicPartition(topic, 1), new TopicPartition(topic, 4)),
                consumers.get(1).staticPartitions());
        assertEquals(List.of(new TopicPartition(topic, 2), new TopicPartition(topic, 5)),
                consumers.get(2).staticPartitions());
    }

    @Test
    public void staticConsumerPartitions_singleConsumer_getsAllPartitions() {
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(specWithKey(
                "p", 1, 4, 0, 1, 1, ConsumerType.CONSUMER_GROUP,
                MessageDistributorType.NO_KEY, false, true));

        var partitions = topology.consumers().get(0).staticPartitions();
        String topic = topology.consumers().get(0).topic();
        assertEquals(List.of(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2),
                new TopicPartition(topic, 3)), partitions);
    }

    @Test
    public void staticConsumerPartitions_disabled_returnsEmpty() {
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(spec("p", 1, 6, 0, 1, 3, false));

        for (var vc : topology.consumers()) {
            assertTrue(vc.staticPartitions().isEmpty());
        }
    }

    // =========================================================================
    // ConsumerSet helper on VirtualTopology
    // =========================================================================

    @Test
    public void consumerSetForWorker_carriesUniformParams() {
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(new TopologySpec(
                "p", 1, 4, 0, 1, 2, ConsumerType.CONSUMER_GROUP,
                /*procMin*/ 5, /*procMax*/ 25, /*procParallelism*/ 1,
                ConsumerProcMode.BLOCKING,
                10000, // consumerProcCapacity
                MessageDistributorType.NO_KEY, 1.0,
                /*correctness*/ false, /*staticConsumerPartitions*/ false));

        ConsumerSet set = topology.consumerSetForWorker("w1");
        assertEquals(2, set.consumers().size());
        assertEquals(ConsumerType.CONSUMER_GROUP, set.consumerType());
        assertEquals(5, set.consumerProcMsMin());
        assertEquals(25, set.consumerProcMsMax());
    }

    // =========================================================================
    // Flink stage: consumers on output topics
    // =========================================================================

    private static TopologySpec pipelinedSpec(int numTopics, int partitions, int outputPartitions,
                                              int consumersPerGroup, boolean staticConsumerPartitions) {
        return new TopologySpec("p", numTopics, partitions, 2, 1, consumersPerGroup,
                ConsumerType.CONSUMER_GROUP, 0, 0, 1, ConsumerProcMode.BLOCKING, 10000,
                MessageDistributorType.NO_KEY, 1.0, false, staticConsumerPartitions, outputPartitions);
    }

    @Test
    public void pipelinedTopologyProducesToInputsAndConsumesOutputs() {
        var planner = newPlanner(List.of("w1", "w2"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(pipelinedSpec(2, 4, 6, 2, false));

        List<String> inputs = topology.producerTopics();
        List<String> outputs = topology.consumerTopics();
        assertEquals(2, inputs.size());
        assertEquals(List.of(inputs.get(0) + "-out", inputs.get(1) + "-out"), outputs);
        assertEquals(inputs.get(0) + "-out", topology.pipelineTopics().get(inputs.get(0)));

        // All four topics are created, outputs with their own partition count.
        assertEquals(4, topology.topicPartitionCounts().size());
        assertEquals(Integer.valueOf(4), topology.topicPartitionCounts().get(inputs.get(0)));
        assertEquals(Integer.valueOf(6), topology.topicPartitionCounts().get(outputs.get(0)));

        assertTrue(topology.producers().stream().allMatch(vp -> inputs.contains(vp.topic())));
        assertTrue(topology.consumers().stream().allMatch(vc -> outputs.contains(vc.topic())));
        assertEquals(4, topology.producers().size());
        assertEquals(4, topology.consumers().size());
    }

    @Test
    public void pipelinedStaticPartitionsUseTheOutputPartitionCount() {
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(pipelinedSpec(1, 2, 6, 3, true));
        String output = topology.consumerTopics().get(0);
        Set<Integer> partitions = topology.consumers().stream()
                .flatMap(vc -> vc.staticPartitions().stream())
                .peek(tp -> assertEquals(output, tp.topic()))
                .map(TopicPartition::partition)
                .collect(Collectors.toSet());
        assertEquals(Set.of(0, 1, 2, 3, 4, 5), partitions);
    }

    @Test
    public void unpipelinedTopologyConsumesTheProducerTopics() {
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        VirtualTopology topology = planner.plan(spec("p", 2, 3, 1, 1, 1, false));
        assertTrue(topology.pipelineTopics().isEmpty());
        assertEquals(topology.producerTopics(), topology.consumerTopics());
        assertEquals(2, topology.topicPartitionCounts().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void pipelinedCorrectnessIsRejected() {
        new TopologySpec("p", 1, 1, 1, 1, 1, ConsumerType.CONSUMER_GROUP, 0, 0, 1,
                ConsumerProcMode.BLOCKING, 10000, MessageDistributorType.NO_KEY, 1.0, true, true, 1);
    }

    @Test
    public void planOverExistingInputsKeepsTheirNamesAndFreshensGroupsAndOutputs() {
        var planner = newPlanner(List.of("w1", "w2"), WorkerMode.MIXED, new RatioSplit(1, 1));
        List<String> inputs = List.of("dimster-reused-0000", "dimster-reused-0001");

        VirtualTopology first = planner.plan(pipelinedSpec(2, 4, 6, 2, false), inputs);
        VirtualTopology second = planner.plan(pipelinedSpec(2, 4, 6, 2, false), inputs);

        assertEquals(inputs, List.copyOf(first.pipelineTopics().keySet()));
        assertEquals(inputs, List.copyOf(second.pipelineTopics().keySet()));
        // Output topics never repeat a name a previous test point may still be deleting.
        for (String input : inputs) {
            assertTrue(first.pipelineTopics().get(input).startsWith(input + "-out-"));
            assertNotEquals(first.pipelineTopics().get(input), second.pipelineTopics().get(input));
        }
        assertNotEquals(first.consumers().getFirst().consumerGroup(), second.consumers().getFirst().consumerGroup());
    }

    @Test(expected = IllegalArgumentException.class)
    public void planOverExistingInputsRequiresOneNamePerTopic() {
        var planner = newPlanner(List.of("w1"), WorkerMode.MIXED, new RatioSplit(1, 1));
        planner.plan(spec("p", 2, 4, 1, 1, 1, false), List.of("only-one"));
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Build a planner with a deterministic per-call counter as the suffix supplier. */
    private static TopologyPlanner newPlanner(List<String> workers, WorkerMode mode,
                                              TopologyPlanner.WorkerSplit split) {
        AtomicInteger counter = new AtomicInteger(0);
        Supplier<String> suffix = () -> "s" + counter.incrementAndGet();
        return new TopologyPlanner(workers, mode, split, suffix);
    }

    /** Spec defaulting to NO_KEY and no static consumer partitions. */
    private static TopologySpec spec(String prefix, int numTopics, int partitionsPerTopic,
                                     int producersPerTopic, int groupsPerTopic, int consumersPerGroup,
                                     boolean correctness) {
        return specType(prefix, numTopics, partitionsPerTopic, producersPerTopic,
                groupsPerTopic, consumersPerGroup, ConsumerType.CONSUMER_GROUP, correctness);
    }

    private static TopologySpec specType(String prefix, int numTopics, int partitionsPerTopic,
                                         int producersPerTopic, int groupsPerTopic, int consumersPerGroup,
                                         ConsumerType type, boolean correctness) {
        return specWithKey(prefix, numTopics, partitionsPerTopic, producersPerTopic,
                groupsPerTopic, consumersPerGroup, type,
                MessageDistributorType.NO_KEY, correctness, false);
    }

    private static TopologySpec specWithKey(String prefix, int numTopics, int partitionsPerTopic,
                                            int producersPerTopic, int groupsPerTopic, int consumersPerGroup,
                                            ConsumerType type, MessageDistributorType keyType,
                                            boolean correctness, boolean staticConsumerPartitions) {
        return new TopologySpec(prefix, numTopics, partitionsPerTopic,
                producersPerTopic, groupsPerTopic, consumersPerGroup,
                type, 0, 0, 1, ConsumerProcMode.BLOCKING,
                10000, // consumerProcCapacity
                keyType, 1.0, correctness, staticConsumerPartitions);
    }

    private static String oneWorkerForProducerTopic(VirtualTopology topology, String topic) {
        Set<String> ws = new LinkedHashSet<>();
        for (VirtualProducer vp : topology.producers()) {
            if (vp.topic().equals(topic)) ws.add(vp.worker());
        }
        if (ws.size() != 1) {
            throw new AssertionError("Expected single worker for topic " + topic + ", got " + ws);
        }
        return ws.iterator().next();
    }

    private static String oneWorkerForConsumerTopic(VirtualTopology topology, String topic) {
        Set<String> ws = new LinkedHashSet<>();
        for (VirtualConsumer vc : topology.consumers()) {
            if (vc.topic().equals(topic)) ws.add(vc.worker());
        }
        if (ws.size() != 1) {
            throw new AssertionError("Expected single worker for topic " + topic + ", got " + ws);
        }
        return ws.iterator().next();
    }
}
