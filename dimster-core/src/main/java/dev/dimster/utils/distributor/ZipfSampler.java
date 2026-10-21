package dev.dimster.utils.distributor;

import java.util.concurrent.ThreadLocalRandom;
import java.util.random.RandomGenerator;

/**
 * Draws samples from a discrete Zipf distribution over the integers [1, n].
 *
 * <p>The probability mass function is P(k) ∝ 1 / k^s, where s ≥ 0 is the
 * "exponent" (skew). With s = 0 the distribution collapses to uniform; the
 * higher s gets, the more probability piles up onto the small values
 * (k = 1, 2, …). For dimster's purposes this means: use this sampler to
 * decide which key (or partition) to publish to when you want a
 * deliberately uneven load.
 *
 * <h2>Why this implementation exists</h2>
 *
 * The rest of the project already has {@link MessageDistributor} for picking
 * keys, but every existing strategy spreads load roughly evenly. To exercise
 * skew-sensitive behaviours (hot partitions, share-group fetcher fairness,
 * lag amplification, …) we need a way to draw from a heavy-tailed
 * distribution at high throughput. We do this without pulling in a math
 * library by implementing the Hörmann–Derflinger rejection-inversion
 * algorithm in ~30 lines below — O(1) per draw, no precomputed CDF tables.
 *
 * <p>The class is immutable and safe to share across threads. The actual
 * randomness comes from {@link ThreadLocalRandom} inside {@link #sample()},
 * so producer threads never contend on a shared RNG, or from a caller-supplied
 * generator via {@link #sample(RandomGenerator)}.
 *
 * <p>Reference: W. Hörmann and G. Derflinger, "Rejection-inversion to
 * generate variates from monotone discrete distributions", ACM Transactions
 * on Modelling and Computer Simulation, 6 (3), 1996.
 */
public final class ZipfSampler {

    private final int n;
    private final double exponent;

    // Precomputed constants for the rejection-inversion algorithm.
    // hIntegralX1 and hIntegralN bound the inverse-CDF range we sample over;
    // sParam is the threshold that lets the fast acceptance branch fire most
    // of the time so we rarely need the second (more expensive) check.
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
        this.hIntegralX1 = hIntegral(1.5) - 1.0;
        this.hIntegralN = hIntegral(n + 0.5);
        this.sParam = 2.0 - hIntegralInverse(hIntegral(2.5) - h(2.0));
    }

    /**
     * Draw a single integer in [1, n]. Average cost is O(1) — the rejection
     * loop almost always accepts on the first iteration for typical s values.
     */
    public int sample() {
        return sample(ThreadLocalRandom.current());
    }

    /**
     * Draw a single integer in [1, n] using the given random source. The data
     * generator uses this with seeded generators so that its streams are
     * reproducible; {@link #sample()} uses {@link ThreadLocalRandom}.
     */
    public int sample(RandomGenerator rnd) {
        // Single-element distribution is trivial; short-circuit so we don't
        // have to reason about degenerate inverse-CDF ranges.
        if (n == 1) {
            return 1;
        }
        while (true) {
            // u is a uniform draw from [hIntegralX1, hIntegralN]. Note the
            // boundaries: for exponent > 0 we have hIntegralX1 < hIntegralN.
            double u = hIntegralN + rnd.nextDouble() * (hIntegralX1 - hIntegralN);
            double x = hIntegralInverse(u);
            int k = (int) (x + 0.5);
            if (k < 1) {
                k = 1;
            } else if (k > n) {
                k = n;
            }
            // Fast path (k - x <= sParam) accepts roughly all draws.
            // The second check is the exact rejection condition.
            if (k - x <= sParam || u >= hIntegral(k + 0.5) - h(k)) {
                return k;
            }
        }
    }

    // ─── Math helpers ─────────────────────────────────────────────────
    //
    // The algorithm uses h(x) = x^(-s) (the unnormalized PMF extended to
    // the reals) and its antiderivative H(x). Closed forms differ for
    // s == 1 vs s != 1, but writing them via the helpers below covers
    // both cases continuously and keeps the code branch-free.

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
            // Numerical guard: t = -1 corresponds to log1p(-1) = -∞; clamp
            // before it can produce NaN. The only way to land here is with
            // borderline inputs near the boundary of the sampling range.
            t = -1.0;
        }
        return Math.exp(helper1(t) * x);
    }

    /** helper1(u) = log(1+u)/u, continuous at u=0 (limit = 1). */
    private static double helper1(double u) {
        if (Math.abs(u) > 1e-8) {
            return Math.log1p(u) / u;
        }
        // Taylor series for numerical stability near 0.
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
