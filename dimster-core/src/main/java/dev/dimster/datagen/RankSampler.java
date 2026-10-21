package dev.dimster.datagen;

import dev.dimster.utils.distributor.ZipfSampler;

import java.util.List;
import java.util.SplittableRandom;

/**
 * Draws the rank of a categorical column's next value, in [0, cardinality),
 * using the producer thread's random stream. The rank is then mapped to a
 * value by a deterministic function (see {@link StringValues}), so the
 * distribution chosen here is the distribution of the column's values.
 *
 * <p>Implementations are immutable and may be shared by threads; all mutable
 * state lives in the caller's {@link SplittableRandom}.
 */
interface RankSampler {

    long next(SplittableRandom rnd);

    /**
     * The sampler for a resolved categorical column. A cardinality of 0
     * (unbounded) draws an arbitrary 64-bit rank, so every row gets a fresh
     * value; weighted enums sample by their weights.
     */
    static RankSampler of(ResolvedDataset.Column c) {
        if (c.weights() != null) {
            return weighted(c.weights());
        }
        if (c.cardinality() == 0) {
            return SplittableRandom::nextLong;
        }
        long cardinality = c.cardinality();
        if (c.distribution() == ColumnSpec.Distribution.ZIPF) {
            ZipfSampler zipf = new ZipfSampler((int) cardinality, c.skew());
            // ZipfSampler draws from [1, n]; rank 0 is the most frequent.
            return rnd -> zipf.sample(rnd) - 1;
        }
        return rnd -> rnd.nextLong(cardinality);
    }

    /** Samples index i with probability weights[i] (weights sum to 1). */
    private static RankSampler weighted(List<Double> weights) {
        double[] cumulative = new double[weights.size()];
        double sum = 0;
        for (int i = 0; i < cumulative.length; i++) {
            sum += weights.get(i);
            cumulative[i] = sum;
        }
        // Guard against rounding leaving the last bound just below 1.
        cumulative[cumulative.length - 1] = Double.POSITIVE_INFINITY;
        return rnd -> {
            double u = rnd.nextDouble();
            int lo = 0;
            int hi = cumulative.length - 1;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (u < cumulative[mid]) {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            return lo;
        };
    }
}
