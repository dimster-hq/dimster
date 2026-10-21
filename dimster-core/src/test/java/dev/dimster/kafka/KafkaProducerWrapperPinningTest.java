package dev.dimster.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Tests for the partition-resolution logic on {@link KafkaProducerWrapper}.
 * Exercises {@code resolvePartition} directly so the test doesn't need a real
 * KafkaProducer or broker — the pinning + per-instance round-robin contract is
 * the entire surface we need to verify here.
 */
public class KafkaProducerWrapperPinningTest {

    @Test
    public void noPinning_andNoOverride_returnsNullForKafkaPartitioner() {
        KafkaProducerWrapper w = wrapper(new int[0]);
        for (int i = 0; i < 5; i++) {
            assertNull("With no pinning and no override, partitioner picks", w.resolvePartition(null));
        }
    }

    @Test
    public void overrideAlwaysWins_evenWhenPinned() {
        KafkaProducerWrapper w = wrapper(new int[] { 7, 8, 9 });
        // Override is honored even when pinning is configured — keeps PARTITION_ZIPF /
        // PARTITION_ROUND_ROBIN distributors working alongside producer-level pinning.
        assertEquals(Integer.valueOf(42), w.resolvePartition(42));
        assertEquals(Integer.valueOf(0),  w.resolvePartition(0));
        assertEquals(Integer.valueOf(99), w.resolvePartition(99));
    }

    @Test
    public void singlePinnedPartition_alwaysReturnsThatPartition() {
        KafkaProducerWrapper w = wrapper(new int[] { 3 });
        for (int i = 0; i < 100; i++) {
            assertEquals(Integer.valueOf(3), w.resolvePartition(null));
        }
    }

    @Test
    public void multiPinnedPartitions_roundRobinInOrder() {
        KafkaProducerWrapper w = wrapper(new int[] { 2, 5, 8 });
        // Three calls cycle through the pinned set in order, fourth wraps.
        assertEquals(Integer.valueOf(2), w.resolvePartition(null));
        assertEquals(Integer.valueOf(5), w.resolvePartition(null));
        assertEquals(Integer.valueOf(8), w.resolvePartition(null));
        assertEquals(Integer.valueOf(2), w.resolvePartition(null));
        assertEquals(Integer.valueOf(5), w.resolvePartition(null));
    }

    @Test
    public void multiPinnedPartitions_distributeEvenlyOverManyCalls() {
        // 3 pinned partitions, 99 sends → each hit exactly 33 times.
        KafkaProducerWrapper w = wrapper(new int[] { 0, 1, 2 });
        Map<Integer, Integer> hits = new HashMap<>();
        for (int i = 0; i < 99; i++) {
            int p = w.resolvePartition(null);
            hits.merge(p, 1, Integer::sum);
        }
        assertEquals(Integer.valueOf(33), hits.get(0));
        assertEquals(Integer.valueOf(33), hits.get(1));
        assertEquals(Integer.valueOf(33), hits.get(2));
    }

    @Test
    public void perInstanceCursors_doNotShareState() {
        // Two wrappers with the same pinning advance independently — no shared cursor
        // (intentionally different from PartitionRoundRobin which is global per test point).
        KafkaProducerWrapper w1 = wrapper(new int[] { 0, 1 });
        KafkaProducerWrapper w2 = wrapper(new int[] { 0, 1 });
        assertEquals(Integer.valueOf(0), w1.resolvePartition(null));
        // w2's cursor hasn't moved — its first send still gets partition 0.
        assertEquals(Integer.valueOf(0), w2.resolvePartition(null));
        assertEquals(Integer.valueOf(1), w1.resolvePartition(null));
        assertEquals(Integer.valueOf(1), w2.resolvePartition(null));
    }

    @Test
    public void getPinnedPartitions_returnsDefensiveCopy() {
        int[] orig = new int[] { 1, 2, 3 };
        KafkaProducerWrapper w = wrapper(orig);
        int[] copy = w.getPinnedPartitions();
        copy[0] = 999;
        // Original wrapper state is unaffected.
        assertEquals(Integer.valueOf(1), w.resolvePartition(null));
    }

    /**
     * Build a wrapper around a {@code null} producer — we only test partition
     * resolution, never call sendAsync, so the underlying KafkaProducer is
     * never dereferenced.
     */
    private static KafkaProducerWrapper wrapper(int[] pinnedPartitions) {
        return new KafkaProducerWrapper(null, "topic", "test-producer", 0, pinnedPartitions, false);
    }
}
