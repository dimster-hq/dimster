package dev.dimster.engines.run;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import dev.dimster.ConsumerProcMode;
import dev.dimster.ConsumerType;
import dev.dimster.ScenarioExpander.TestPoint;
import dev.dimster.datagen.DatasetResolver;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.utils.distributor.MessageDistributorType;
import dev.dimster.worker.topology.TopologyPlanner;
import dev.dimster.worker.topology.VirtualConsumer;
import dev.dimster.worker.topology.VirtualProducer;
import dev.dimster.worker.topology.VirtualTopology;

/**
 * Verifies the topology-aware random restart plans used by live modification.
 *
 * <p>The fixture intentionally repeats client indices across topics and groups.
 * That is the shape which an index-based filter handles incorrectly: asking for
 * two consumers must restart two cluster-wide clients, not every consumer whose
 * per-group index is below two.
 */
public class TestPointMutatorRestartTest {

    private final TopologyPlanner planner = new TopologyPlanner(
            List.of("http://worker-0:8080", "http://worker-1:8080"),
            TopologyPlanner.WorkerMode.MIXED,
            new TopologyPlanner.RatioSplit(1, 1));

    @Test
    public void consumerRestartSelectsExactClusterWideCountWithoutReplacement() {
        TestPoint point = testPoint();
        VirtualTopology topology = topology();
        TestPointMutator mutator = mutator();

        TestPointMutator.MutationPlan first = mutator.planConsumerRestart(
                point, topology, 3, new Random(42));
        TestPointMutator.MutationPlan repeated = mutator.planConsumerRestart(
                point, topology, 3, new Random(42));

        DiffAction.RestartConsumers action =
                (DiffAction.RestartConsumers) first.actions().get(0);
        List<String> selected = action.consumers().consumers().stream()
                .map(VirtualConsumer::name)
                .toList();
        List<String> repeatedSelection = ((DiffAction.RestartConsumers)
                repeated.actions().get(0)).consumers().consumers().stream()
                .map(VirtualConsumer::name)
                .toList();

        assertEquals(3, selected.size());
        assertEquals(3, Set.copyOf(selected).size());
        assertEquals(selected, repeatedSelection);
        assertTrue(allConsumerNames(topology).containsAll(selected));
        assertSame(point, first.newTestPoint());
        assertSame(topology, first.newTopology());
    }

    @Test
    public void producerRestartSelectsExactClusterWideCountAndKeepsPlacement() {
        TestPoint point = testPoint();
        VirtualTopology topology = topology();

        TestPointMutator.MutationPlan plan = mutator().planProducerRestart(
                point, topology, 2, new Random(7));
        DiffAction.RestartProducers action =
                (DiffAction.RestartProducers) plan.actions().get(0);

        assertEquals(2, action.producers().size());
        assertEquals(2, action.producers().stream().map(VirtualProducer::name)
                .collect(Collectors.toSet()).size());
        assertTrue(topology.producers().containsAll(action.producers()));
        assertTrue(action.producers().stream().allMatch(producer ->
                producer.worker().equals("http://worker-0:8080")
                        || producer.worker().equals("http://worker-1:8080")));
        assertSame(topology, plan.newTopology());
    }

    @Test
    public void restartCountAtOrAboveTotalSelectsAllInTopologyOrder() {
        TestPoint point = testPoint();
        VirtualTopology topology = topology();

        DiffAction.RestartConsumers consumers = (DiffAction.RestartConsumers)
                mutator().planConsumerRestart(point, topology, 100, new Random(1))
                        .actions().get(0);
        DiffAction.RestartProducers producers = (DiffAction.RestartProducers)
                mutator().planProducerRestart(point, topology,
                        topology.producers().size(), new Random(1))
                        .actions().get(0);

        assertEquals(topology.consumers(), consumers.consumers().consumers());
        assertEquals(topology.producers(), producers.producers());
    }

    @Test
    public void zeroRestartCountIsANoOp() {
        TestPoint point = testPoint();
        VirtualTopology topology = topology();

        assertTrue(mutator().planConsumerRestart(point, topology, 0, new Random(1))
                .actions().isEmpty());
        assertTrue(mutator().planProducerRestart(point, topology, 0, new Random(1))
                .actions().isEmpty());
    }

    @Test
    public void restartRejectsTopologyWithoutRequestedClientType() {
        TestPoint point = testPoint();
        VirtualTopology topology = topology();
        VirtualTopology noConsumers = copyWith(topology, topology.producers(), List.of());
        VirtualTopology noProducers = copyWith(topology, List.of(), topology.consumers());

        assertThrows(IllegalArgumentException.class, () ->
                mutator().planConsumerRestart(point, noConsumers, 1, new Random(1)));
        assertThrows(IllegalArgumentException.class, () ->
                mutator().planProducerRestart(point, noProducers, 1, new Random(1)));
    }

    private TestPointMutator mutator() {
        // Restart plans do not build payloads, but using a real deterministic
        // builder keeps this fixture valid for the mutator's architectural API.
        return new TestPointMutator(planner,
                size -> DatasetResolver.payloadDataset(size - 1, 0.0, 1, null));
    }

    private TestPoint testPoint() {
        return new TestPoint(
                "restart", 0, 1, "restart", 0, 1,
                2, 4, 3, 2, 2,
                100, 1_000.0, null,
                MessageDistributorType.NO_KEY, 1.0,
                ConsumerType.CONSUMER_GROUP, new KafkaConfig(),
                0, 0, 1, ConsumerProcMode.BLOCKING, 10_000,
                null, null, new RunConfig(),
                DatasetResolver.payloadDataset(100 - 1, 0.0, 1, null));
    }

    private VirtualTopology topology() {
        Map<String, Integer> topics = new LinkedHashMap<>();
        topics.put("topic-a", 4);
        topics.put("topic-b", 4);

        List<VirtualProducer> producers = new ArrayList<>();
        List<VirtualConsumer> consumers = new ArrayList<>();
        int placement = 0;
        for (String topic : topics.keySet()) {
            for (int producerIdx = 0; producerIdx < 3; producerIdx++) {
                producers.add(new VirtualProducer(worker(placement++), topic,
                        producerIdx, new int[0]));
            }
            for (int groupIdx = 0; groupIdx < 2; groupIdx++) {
                for (int consumerIdx = 0; consumerIdx < 2; consumerIdx++) {
                    consumers.add(new VirtualConsumer(worker(placement++), topic,
                            "group-" + groupIdx, consumerIdx, List.of()));
                }
            }
        }
        return new VirtualTopology(topics, producers, consumers,
                ConsumerType.CONSUMER_GROUP, 0, 0, 1,
                ConsumerProcMode.BLOCKING, 10_000,
                MessageDistributorType.NO_KEY, 1.0, 2);
    }

    private String worker(int placement) {
        return "http://worker-" + (placement % 2) + ":8080";
    }

    private Set<String> allConsumerNames(VirtualTopology topology) {
        return topology.consumers().stream()
                .map(VirtualConsumer::name)
                .collect(Collectors.toSet());
    }

    private VirtualTopology copyWith(VirtualTopology source,
                                     List<VirtualProducer> producers,
                                     List<VirtualConsumer> consumers) {
        return new VirtualTopology(source.topicPartitionCounts(), producers, consumers,
                source.consumerType(), source.consumerProcMsMin(), source.consumerProcMsMax(),
                source.consumerProcParallelism(), source.consumerProcMode(),
                source.consumerProcCapacity(), source.messageDistributorType(),
                source.messageDistributorSkew(), source.activeProducerWorkerCount());
    }
}
