package dev.dimster.utils.distributor;

/**
 * Picks a key from the shared {@link MessageDistributor#RANDOM_KEYS} pool with
 * a Zipf-distributed index. The first key in the pool is the most-used,
 * the next slightly less, and so on — so a small handful of keys account
 * for most of the produced traffic.
 *
 * <p>The actual partition each chosen key lands on is still decided by
 * Kafka's default partitioner (key hash → partition), so the resulting
 * partition skew is filtered through that hash. For a stronger and
 * deterministic skew where you control which partitions are hot, use
 * {@link PartitionZipf} instead.
 */
public final class KeyZipf implements MessageDistributor {

    private final ZipfSampler sampler;

    public KeyZipf(double skew) {
        // Sample over [1, UNIQUE_COUNT]; convert to 0-based to index the pool.
        this.sampler = new ZipfSampler(UNIQUE_COUNT, skew);
    }

    @Override
    public Target next() {
        int idx = sampler.sample() - 1;
        return KEY_TARGETS[idx];
    }
}
