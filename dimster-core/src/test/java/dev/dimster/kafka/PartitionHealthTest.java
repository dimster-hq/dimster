package dev.dimster.kafka;

import dev.dimster.spi.kafka.PartitionState;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/** When a cluster counts as settled enough to start a measurement. */
public class PartitionHealthTest {

    private static final TopicPartition P0 = new TopicPartition("t", 0);
    private static final TopicPartition P1 = new TopicPartition("t", 1);

    @Test
    public void preferred_leaders_with_full_isr_are_settled() {
        var health = PartitionHealth.assess(Map.of(
                P0, new PartitionState(1, List.of(1, 2, 3), List.of(1, 2, 3)),
                P1, new PartitionState(2, List.of(2, 3, 1), List.of(3, 1, 2))));
        assertTrue(health.settled());
    }

    @Test
    public void leaderless_partition_is_not_settled() {
        var health = PartitionHealth.assess(Map.of(P0, new PartitionState(-1, List.of(1, 2), List.of())));
        assertFalse(health.settled());
        assertEquals(1, health.leaderless());
    }

    @Test
    public void shrunk_isr_is_not_settled() {
        var health = PartitionHealth.assess(Map.of(P0, new PartitionState(1, List.of(1, 2, 3), List.of(1, 2))));
        assertFalse(health.settled());
        assertEquals(1, health.underReplicated());
        // Not offered for election until the ISR is full again.
        assertTrue(health.notPreferred().isEmpty());
    }

    @Test
    public void non_preferred_leader_is_offered_for_election() {
        var health = PartitionHealth.assess(Map.of(
                P0, new PartitionState(2, List.of(1, 2, 3), List.of(1, 2, 3)),
                P1, new PartitionState(2, List.of(2, 3, 1), List.of(2, 3, 1))));
        assertFalse(health.settled());
        assertEquals(Set.of(P0), health.notPreferred());
    }
}
