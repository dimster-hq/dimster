package dev.dimster.utils.distributor;

import java.util.concurrent.ThreadLocalRandom;
import java.util.random.RandomGenerator;

/**
 * Draws samples from a discrete Zipf distribution over the integers [1, n].
 *
 * <p>The probability mass function is P(k) ∝ 1 / k^s, where s ≥ 0 is the
 * "exponent" (skew). With s = 0 the distribution collapses to uniform; the
 * higher s gets, the more probability piles up onto the small values
 * (k = 1, 2, …).
 *
 * <p>For cardinalities up to {@link #ALIAS_TABLE_MAX_N}, sampling uses a
 * precomputed Vose alias table (exact O(1) lookup with zero transcendental math
 * calls per draw, ~15× faster on dataset presets like taxi/clicks and key/partition
 * distributors). Larger cardinalities fall back to the O(1)-memory Hörmann–Derflinger
 * rejection-inversion algorithm.
 *
 * <p>Instances are immutable and thread-safe; randomness comes from
 * {@link ThreadLocalRandom} or a caller-supplied {@link RandomGenerator}.
 */
public final class ZipfSampler {

    // Up to 65,536 elements uses at most ~768 KB for the alias table while covering
    // almost all Zipf columns (taxi zones, clicks URLs/countries, KeyZipf, PartitionZipf).
    static final int ALIAS_TABLE_MAX_N = 65_536;

    private final int n;
    private final double exponent;

    // Non-null when using Vose's alias table (1 < n <= ALIAS_TABLE_MAX_N && exponent > 0).
    private final double[] aliasProb;
    private final int[] aliasIdx;

    // Precomputed constants for the Hörmann–Derflinger fallback (n > ALIAS_TABLE_MAX_N).
    private final double hIntegralX1;
    private final double hIntegralN;
    private final double sParam;

    public ZipfSampler(int n, double exponent) {
        if (n < 1) {
            throw new IllegalArgumentException("n must be >= 1, got " + n);
        }
        if (exponent < 0) {
            throw new IllegalArgumentException("exponent must be >= 0, got " + exponent);
        }
        this.n = n;
        this.exponent = exponent;

        if (n > 1 && exponent > 0.0 && n <= ALIAS_TABLE_MAX_N) {
            double[] prob = new double[n];
            int[] alias = new int[n];
            buildAliasTable(n, exponent, prob, alias);
            this.aliasProb = prob;
            this.aliasIdx = alias;
            this.hIntegralX1 = 0.0;
            this.hIntegralN = 0.0;
            this.sParam = 0.0;
        } else {
            this.aliasProb = null;
            this.aliasIdx = null;
            this.hIntegralX1 = hIntegral(1.5) - 1.0;
            this.hIntegralN = hIntegral(n + 0.5);
            this.sParam = 2.0 - hIntegralInverse(hIntegral(2.5) - h(2.0));
        }
    }

    /**
     * Build Vose's alias table for the exact discrete PMF P(k) = k^(-s) / H_{n,s}.
     * Sums weights from smallest to largest to minimize floating-point rounding error.
     */
    private static void buildAliasTable(int n, double exponent, double[] prob, int[] alias) {
        double[] scaled = new double[n];
        double sum = 0.0;
        for (int i = n - 1; i >= 0; i--) {
            double w = Math.pow(i + 1, -exponent);
            scaled[i] = w;
            sum += w;
        }

        double scale = n / sum;
        int[] small = new int[n];
        int[] large = new int[n];
        int smallTop = 0;
        int largeTop = 0;

        for (int i = 0; i < n; i++) {
            double p = scaled[i] * scale;
            scaled[i] = p;
            if (p < 1.0) {
                small[smallTop++] = i;
            } else {
                large[largeTop++] = i;
            }
        }

        while (smallTop > 0 && largeTop > 0) {
            int s = small[--smallTop];
            int l = large[--largeTop];
            prob[s] = scaled[s];
            alias[s] = l;
            double remaining = (scaled[l] + scaled[s]) - 1.0;
            scaled[l] = remaining;
            if (remaining < 1.0) {
                small[smallTop++] = l;
            } else {
                large[largeTop++] = l;
            }
        }

        while (largeTop > 0) {
            int idx = large[--largeTop];
            prob[idx] = 1.0;
            alias[idx] = idx;
        }
        while (smallTop > 0) {
            int idx = small[--smallTop];
            prob[idx] = 1.0;
            alias[idx] = idx;
        }
    }

    /**
     * Draw a single integer in [1, n]. O(1) per draw.
     */
    public int sample() {
        return sample(ThreadLocalRandom.current());
    }

    /**
     * Draw a single integer in [1, n] using the given random source.
     */
    public int sample(RandomGenerator rnd) {
        if (n == 1) {
            return 1;
        }
        if (exponent == 0.0) {
            return rnd.nextInt(n) + 1;
        }
        if (aliasProb != null) {
            int col = rnd.nextInt(n);
            return (rnd.nextDouble() < aliasProb[col] ? col : aliasIdx[col]) + 1;
        }
        return sampleRejectionInversion(rnd);
    }

    private int sampleRejectionInversion(RandomGenerator rnd) {
        while (true) {
            double u = hIntegralN + rnd.nextDouble() * (hIntegralX1 - hIntegralN);
            double x = hIntegralInverse(u);
            int k = (int) (x + 0.5);
            if (k < 1) {
                k = 1;
            } else if (k > n) {
                k = n;
            }
            if (k - x <= sParam || u >= hIntegral(k + 0.5) - h(k)) {
                return k;
            }
        }
    }

    /** H(x) = ∫ t^{-s} dt — antiderivative of h. */
    private double hIntegral(double x) {
        double logX = Math.log(x);
        return helper2((1.0 - exponent) * logX) * logX;
    }

    /** h(x) = x^{-s} — the (unnormalized) Zipf density. */
    private double h(double x) {
        return Math.exp(-exponent * Math.log(x));
    }

    /** Inverse of {@link #hIntegral} — see Hörmann & Derflinger §3 for derivation. */
    private double hIntegralInverse(double x) {
        double t = x * (1.0 - exponent);
        if (t < -1.0) {
            t = -1.0;
        }
        return Math.exp(helper1(t) * x);
    }

    /** helper1(u) = log(1+u)/u, continuous at u=0 (limit = 1). */
    private static double helper1(double u) {
        if (Math.abs(u) > 1e-8) {
            return Math.log1p(u) / u;
        }
        return 1.0 - u * (0.5 - u * (1.0 / 3.0 - 0.25 * u));
    }

    /** helper2(u) = (e^u - 1)/u, continuous at u=0 (limit = 1). */
    private static double helper2(double u) {
        if (Math.abs(u) > 1e-8) {
            return Math.expm1(u) / u;
        }
        return 1.0 + u * 0.5 * (1.0 + u * (1.0 / 3.0) * (1.0 + 0.25 * u));
    }
}
