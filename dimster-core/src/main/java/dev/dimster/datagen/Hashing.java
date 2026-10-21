package dev.dimster.datagen;

/**
 * Deterministic 64-bit mixing used to derive seeds and scramble ranks.
 *
 * <p>Data generation must be reproducible: the value for a given rank of a
 * column must be the same on every producer, worker and run with the same
 * dataset seed, which is what makes cardinality global rather than per
 * producer. Everything derives from {@link #mix64}, the SplitMix64 finaliser
 * (a bijection on 64-bit values with good avalanche), and from
 * {@link String#hashCode()}, whose algorithm is fixed by the Java spec.
 */
final class Hashing {

    private static final long GOLDEN_GAMMA = 0x9E3779B97F4A7C15L;

    private Hashing() {}

    /** SplitMix64 finaliser: a bijective, well-mixed function of {@code z}. */
    static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /** Seed for one column's rank-to-value mapping, independent of the producer. */
    static long columnSeed(long datasetSeed, String columnName) {
        return mix64(datasetSeed * GOLDEN_GAMMA + columnName.hashCode());
    }

    /** Seed for the generator of one rank's value (composite and formatted kinds). */
    static long rankSeed(long columnSeed, long rank) {
        return mix64(columnSeed + rank * GOLDEN_GAMMA);
    }

    /** Seed for one producer's sampling stream, from a hash of the producer's name. */
    static long producerSeed(long datasetSeed, long producerHash) {
        return mix64(mix64(datasetSeed) + producerHash * GOLDEN_GAMMA);
    }
}
