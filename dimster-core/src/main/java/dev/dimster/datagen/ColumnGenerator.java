package dev.dimster.datagen;

import dev.dimster.datagen.ColumnSpec.Distribution;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.SplittableRandom;
import java.util.function.LongFunction;

/**
 * Produces one column's values for one producer thread and streams them into
 * a {@link RecordWriter}.
 *
 * <p>The null-rate and run-length handling is identical for every column type,
 * so it lives here: each row is null with probability {@code nullRate};
 * otherwise a new value is sampled only every {@code runLength} non-null rows
 * and held in between, which clusters values within producer batches. The
 * subclasses only sample and write values.
 *
 * <p>Instances are stateful (the held value, run position, sequence counter)
 * and owned by a single {@link RowGenerator}; they are not thread-safe.
 */
abstract class ColumnGenerator {

    private final double nullRate;
    private final int runLength;
    private int remaining;
    private boolean lastNull;

    ColumnGenerator(ResolvedDataset.Column c) {
        this.nullRate = c.nullRate();
        this.runLength = c.runLength();
    }

    /** Generates this row's value and writes it. */
    final void next(SplittableRandom rnd, RecordWriter out) {
        if (nullRate > 0 && rnd.nextDouble() < nullRate) {
            lastNull = true;
            out.writeNull();
            return;
        }
        lastNull = false;
        if (remaining == 0) {
            sample(rnd);
            remaining = runLength;
        }
        remaining--;
        write(out);
    }

    /** The last value as a string, for record keys; null when it was null. */
    final String lastKey() {
        return lastNull ? null : lastAsString();
    }

    /** Samples a new value and holds it. */
    abstract void sample(SplittableRandom rnd);

    /** Writes the held value. */
    abstract void write(RecordWriter out);

    abstract String lastAsString();

    // ─── LONG, INT, TIMESTAMP, SEQUENCE ──────────────────────────────────

    /** Columns whose values are 64-bit integers, written as Avro long or int. */
    abstract static class Integral extends ColumnGenerator {
        private final boolean writeInt;
        long value;

        Integral(ResolvedDataset.Column c) {
            super(c);
            this.writeInt = c.type() == ColumnSpec.Type.INT;
        }

        @Override
        final void write(RecordWriter out) {
            if (writeInt) {
                out.writeInt((int) value);
            } else {
                out.writeLong(value);
            }
        }

        @Override
        final String lastAsString() {
            return Long.toString(value);
        }
    }

    /**
     * A categorical integer: rank in [0, cardinality) mapped into [min, max].
     * Without scrambling the value is {@code min + rank}. With scrambling the
     * mapping is a pseudo-random permutation of the domain (an affine map
     * modulo the domain size, which is injective, so distinct ranks give
     * distinct values) when the domain fits in 31 bits, and a hash beyond that
     * (collisions are then negligible).
     */
    static final class CategoricalIntegral extends Integral {
        private final RankSampler ranks;
        private final boolean scramble;
        private final long min;
        private final long domain;
        private final long multiplier;
        private final long offset;
        private final long columnSeed;

        CategoricalIntegral(ResolvedDataset.Column c, long columnSeed) {
            super(c);
            this.ranks = RankSampler.of(c);
            this.scramble = c.scramble();
            this.min = (long) c.min();
            this.domain = DatasetResolver.domainSize((long) c.min(), (long) c.max());
            this.columnSeed = columnSeed;
            if (domain <= Integer.MAX_VALUE) {
                long m = Long.remainderUnsigned(Hashing.mix64(columnSeed), domain) | 1;
                while (gcd(m, domain) != 1) {
                    m += 2;
                }
                this.multiplier = m % domain;
                this.offset = Long.remainderUnsigned(Hashing.mix64(columnSeed + 1), domain);
            } else {
                this.multiplier = 0;
                this.offset = 0;
            }
        }

        @Override
        void sample(SplittableRandom rnd) {
            value = valueFor(ranks.next(rnd));
        }

        long valueFor(long rank) {
            if (!scramble) {
                return min + rank;
            }
            if (domain <= Integer.MAX_VALUE) {
                // Both factors are below 2^31, so the product can't overflow.
                return min + (rank * multiplier + offset) % domain;
            }
            return min + Long.remainderUnsigned(Hashing.rankSeed(columnSeed, rank), domain);
        }

        private static long gcd(long a, long b) {
            while (b != 0) {
                long t = a % b;
                a = b;
                b = t;
            }
            return a;
        }
    }

    /** An integer drawn from [min, max], uniformly or normally. */
    static final class RangeIntegral extends Integral {
        private final long min;
        private final long max;
        private final boolean normal;

        RangeIntegral(ResolvedDataset.Column c) {
            super(c);
            this.min = (long) c.min();
            this.max = (long) c.max();
            this.normal = c.distribution() == Distribution.NORMAL;
        }

        @Override
        void sample(SplittableRandom rnd) {
            if (normal) {
                double v = normal(rnd, min, max);
                value = Math.max(min, Math.min(max, Math.round(v)));
            } else if (max < Long.MAX_VALUE) {
                value = rnd.nextLong(min, max + 1);
            } else if (min > Long.MIN_VALUE) {
                value = rnd.nextLong(min - 1, max) + 1;
            } else {
                value = rnd.nextLong();
            }
        }
    }

    /** Wall-clock epoch milliseconds minus a uniform jitter in [0, jitterMs]. */
    static final class Timestamp extends Integral {
        private final long jitterMs;

        Timestamp(ResolvedDataset.Column c) {
            super(c);
            this.jitterMs = c.jitterMs();
        }

        @Override
        void sample(SplittableRandom rnd) {
            long now = System.currentTimeMillis();
            value = jitterMs > 0 ? now - rnd.nextLong(jitterMs + 1) : now;
        }
    }

    /**
     * {@code (producerIdx << 40) | counter}: monotonic per producer and unique
     * across the producers of a topic (whose indexes are distinct), as long as
     * a producer sends fewer than 2^40 records. Producers of different topics
     * share index values, so values are unique per topic, like a table key.
     */
    static final class Sequence extends Integral {
        private final long base;
        private long counter;

        Sequence(ResolvedDataset.Column c, int producerIdx) {
            super(c);
            this.base = (long) producerIdx << 40;
        }

        @Override
        void sample(SplittableRandom rnd) {
            value = base | (counter++ & ((1L << 40) - 1));
        }
    }

    // ─── DOUBLE, BOOLEAN ─────────────────────────────────────────────────

    /** A double drawn from [min, max], optionally rounded to {@code scale} places. */
    static final class DoubleColumn extends ColumnGenerator {
        private final double min;
        private final double max;
        private final boolean normal;
        private final double factor;
        private double value;

        DoubleColumn(ResolvedDataset.Column c) {
            super(c);
            this.min = c.min();
            this.max = c.max();
            this.normal = c.distribution() == Distribution.NORMAL;
            this.factor = c.scale() >= 0 ? Math.pow(10, c.scale()) : 0;
        }

        @Override
        void sample(SplittableRandom rnd) {
            double v;
            if (normal) {
                v = Math.max(min, Math.min(max, normal(rnd, min, max)));
            } else {
                v = max > min ? rnd.nextDouble(min, max) : min;
            }
            value = factor > 0 ? Math.round(v * factor) / factor : v;
        }

        @Override
        void write(RecordWriter out) {
            out.writeDouble(value);
        }

        @Override
        String lastAsString() {
            return Double.toString(value);
        }
    }

    static final class BooleanColumn extends ColumnGenerator {
        private final double trueRate;
        private boolean value;

        BooleanColumn(ResolvedDataset.Column c) {
            super(c);
            this.trueRate = c.trueRate();
        }

        @Override
        void sample(SplittableRandom rnd) {
            value = rnd.nextDouble() < trueRate;
        }

        @Override
        void write(RecordWriter out) {
            out.writeBoolean(value);
        }

        @Override
        String lastAsString() {
            return Boolean.toString(value);
        }
    }

    // ─── STRING ──────────────────────────────────────────────────────────

    /** A string looked up by rank in a shared, thread-safe value source. */
    static final class StringColumn extends ColumnGenerator {
        private final RankSampler ranks;
        private final LongFunction<byte[]> values;
        private byte[] value;

        StringColumn(ResolvedDataset.Column c, LongFunction<byte[]> values) {
            super(c);
            this.ranks = RankSampler.of(c);
            this.values = values;
        }

        @Override
        void sample(SplittableRandom rnd) {
            value = values.apply(ranks.next(rnd));
        }

        @Override
        void write(RecordWriter out) {
            out.writeString(value);
        }

        @Override
        String lastAsString() {
            return new String(value, StandardCharsets.UTF_8);
        }
    }

    // ─── BYTES ───────────────────────────────────────────────────────────

    /**
     * Opaque bytes. With a cardinality, values are looked up by rank in a
     * shared value source, like STRING columns; without one, every row gets
     * fresh bytes from the producer's random stream.
     */
    static final class BytesColumn extends ColumnGenerator {
        private final RankSampler ranks;
        private final LongFunction<byte[]> values;
        private final int minLength;
        private final int maxLength;
        private final double randomRatio;
        private byte[] value;

        /** @param values rank -> value, or null for a column without a cardinality */
        BytesColumn(ResolvedDataset.Column c, LongFunction<byte[]> values) {
            super(c);
            this.ranks = values != null ? RankSampler.of(c) : null;
            this.values = values;
            this.minLength = c.minLength();
            this.maxLength = c.maxLength();
            this.randomRatio = c.randomRatio();
        }

        @Override
        void sample(SplittableRandom rnd) {
            value = ranks != null
                    ? values.apply(ranks.next(rnd))
                    : randomValue(rnd, minLength, maxLength, randomRatio);
        }

        @Override
        void write(RecordWriter out) {
            out.writeBytes(value);
        }

        @Override
        String lastAsString() {
            return HexFormat.of().formatHex(value);
        }

        /**
         * A value of [minLength, maxLength] bytes whose leading randomRatio
         * is random and the rest zeros. The zeros are what makes the
         * payload dataset compressible.
         */
        static byte[] randomValue(SplittableRandom rnd, int minLength, int maxLength, double randomRatio) {
            int length = minLength == maxLength ? minLength : rnd.nextInt(minLength, maxLength + 1);
            byte[] v = new byte[length];
            int random = (int) Math.round(length * randomRatio);
            for (int i = 0; i < random; i += 8) {
                long bits = rnd.nextLong();
                for (int j = i; j < Math.min(i + 8, random); j++) {
                    v[j] = (byte) bits;
                    bits >>>= 8;
                }
            }
            return v;
        }
    }

    /** Normal with mean at the midpoint and standard deviation (max - min) / 6. */
    private static double normal(SplittableRandom rnd, double min, double max) {
        return (min + max) / 2 + rnd.nextGaussian() * (max - min) / 6;
    }
}
