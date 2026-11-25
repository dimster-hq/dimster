package dev.dimster.utils.distributor;

/**
 * Cycles through partitions {@code [0, partitionsPerTopic)} in round-robin
 * order, bypassing Kafka's partitioner entirely. Each producer thread keeps its
 * own cursor via {@link ThreadLocal} to avoid shared cache-line contention and
 * returns pre-allocated {@link Target} instances with zero per-send allocation.
 */
public final class PartitionRoundRobin implements MessageDistributor {

    private final Target[] targets;
    // Stores a 1-element int[] per thread (no back-reference to PartitionRoundRobin).
    private final ThreadLocal<int[]> cursor = ThreadLocal.withInitial(() -> new int[1]);

    public PartitionRoundRobin(int partitionsPerTopic) {
        if (partitionsPerTopic < 1) {
            throw new IllegalArgumentException(
                    "partitionsPerTopic must be >= 1 for PARTITION_ROUND_ROBIN, got "
                            + partitionsPerTopic);
        }
        this.targets = new Target[partitionsPerTopic];
        for (int p = 0; p < partitionsPerTopic; p++) {
            this.targets[p] = Target.ofPartition(p);
        }
    }

    @Override
    public Target next() {
        int[] box = cursor.get();
        int p = box[0]++;
        if (p >= targets.length) {
            p = 0;
            box[0] = 1;
        }
        return targets[p];
    }
}
