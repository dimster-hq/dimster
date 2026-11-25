package dev.dimster.utils.distributor;

/**
 * Forces an explicit partition number drawn from a Zipf distribution.
 * Partition 0 is always the hottest; partition (n-1) is the coldest. The
 * record is sent with a null key — Kafka's partitioner is bypassed via
 * the {@code ProducerRecord(topic, partition, key, value)} constructor.
 */
public final class PartitionZipf implements MessageDistributor {

    private final ZipfSampler sampler;
    // Pre-allocated targets avoid boxing Integer and allocating Target per send.
    private final Target[] targets;

    public PartitionZipf(int partitionsPerTopic, double skew) {
        if (partitionsPerTopic < 1) {
            throw new IllegalArgumentException(
                    "partitionsPerTopic must be >= 1 for PARTITION_ZIPF, got "
                            + partitionsPerTopic);
        }
        this.sampler = new ZipfSampler(partitionsPerTopic, skew);
        this.targets = new Target[partitionsPerTopic];
        for (int p = 0; p < partitionsPerTopic; p++) {
            this.targets[p] = Target.ofPartition(p);
        }
    }

    @Override
    public Target next() {
        // sampler returns [1, n]; partition numbers are 0-based.
        return targets[sampler.sample() - 1];
    }
}
