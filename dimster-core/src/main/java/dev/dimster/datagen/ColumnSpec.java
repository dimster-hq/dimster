package dev.dimster.datagen;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * One column of a generated dataset, as written in a workload or preset.
 *
 * <p>This is the user-facing configuration model: every field is nullable so
 * that a workload can override a preset column field by field (for example
 * only its {@code cardinality}). {@link DatasetResolver} merges presets and
 * overrides, fills in defaults and validates, producing a
 * {@link ResolvedDataset.Column} that the generators consume. Nothing on the
 * hot path reads this class.
 *
 * <p>Which fields apply depends on {@link #type}:
 * <ul>
 *   <li>{@code LONG}/{@code INT}: either categorical ({@code cardinality},
 *       {@code distribution} UNIFORM or ZIPF, {@code skew}, {@code scramble},
 *       optional {@code min}/{@code max} as the value domain) or a range
 *       ({@code min}/{@code max}, {@code distribution} UNIFORM or NORMAL).</li>
 *   <li>{@code DOUBLE}: a range ({@code min}/{@code max}, UNIFORM or NORMAL)
 *       rounded to {@code scale} decimal places when set.</li>
 *   <li>{@code BOOLEAN}: {@code trueRate}.</li>
 *   <li>{@code STRING}: either a weighted enum ({@code values}, optional
 *       {@code weights}) or a {@code kind} with optional {@code cardinality},
 *       {@code distribution}, {@code skew}, {@code length} (ALPHANUMERIC) and
 *       {@code words} (SENTENCE).</li>
 *   <li>{@code TIMESTAMP}: wall-clock milliseconds minus a random
 *       {@code jitterMs}.</li>
 *   <li>{@code SEQUENCE}: a per-producer counter, unique across producers.</li>
 *   <li>{@code BYTES}: opaque values of {@code length} bytes, of which the
 *       leading {@code randomRatio} are random and the rest zero, with
 *       optional {@code cardinality} and {@code distribution}. The default
 *       {@code payload} dataset is one such column.</li>
 * </ul>
 * {@code nullRate} and {@code runLength} apply to every type.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ColumnSpec {

    /** The column's value type. Determines the encoded schema type. */
    public enum Type {
        LONG,
        INT,
        DOUBLE,
        BOOLEAN,
        STRING,
        /** Epoch milliseconds from the producer's wall clock. */
        TIMESTAMP,
        /** Per-producer monotonic counter; unique across producers. */
        SEQUENCE,
        /** Opaque bytes: a random prefix followed by zeros. */
        BYTES
    }

    /** How a rank (categorical columns) or a value (range columns) is drawn. */
    public enum Distribution {
        UNIFORM,
        /** Categorical only: rank 0 is the most frequent; see {@code skew}. */
        ZIPF,
        /** Ranges only: mean at the midpoint, standard deviation range/6, clamped. */
        NORMAL
    }

    /**
     * What a STRING column's values look like. Dictionary kinds index into an
     * embedded, frequency-ordered dictionary (see
     * {@code datagen/dict/SOURCES.md}); composite kinds assemble a value from
     * dictionaries; formatted kinds build a value from random bits.
     */
    public enum Kind {
        // Formatted.
        ALPHANUMERIC,
        IP,
        UUID,
        // Dictionary.
        FIRST_NAME,
        LAST_NAME,
        CITY,
        COUNTRY,
        DOMAIN,
        USER_AGENT,
        WORD,
        // Composite.
        FULL_NAME,
        STREET_ADDRESS,
        EMAIL,
        URL,
        SENTENCE
    }

    public Type type;
    public Kind kind;

    /** Number of distinct values. Unset means unbounded (or the kind's dictionary size). */
    public Long cardinality;
    public Distribution distribution;
    /** Zipf exponent (0 = uniform, 1 = classic Zipf). */
    public Double skew;
    /**
     * Numeric categoricals: map ranks to pseudo-random values in the domain
     * (true, the default) so hot ranks aren't the smallest values, or to
     * {@code min + rank} (false), which keeps rank order.
     */
    public Boolean scramble;

    public Double min;
    public Double max;
    /** DOUBLE: number of decimal places to round to. */
    public Integer scale;

    /** STRING weighted enum: the values, most frequent first is conventional. */
    public List<String> values;
    /** Relative weights for {@code values}; equal weights when unset. */
    public List<Double> weights;

    /** ALPHANUMERIC: [min, max] length in characters; BYTES: in bytes. */
    public List<Integer> length;
    /** SENTENCE: [min, max] number of words. */
    public List<Integer> words;

    /** BOOLEAN: probability of true. */
    public Double trueRate;
    /** TIMESTAMP: values are now minus a uniform random [0, jitterMs]. */
    public Long jitterMs;
    /**
     * BYTES: fraction of each value that is random; the rest is zeros. Lower
     * values make the column more compressible.
     */
    public Double randomRatio;

    /** Probability that a row's value is null. Makes the column nullable. */
    public Double nullRate;
    /**
     * Hold each sampled value for this many consecutive rows on a producer.
     * Clusters values within batches, which drives compression locality and
     * batch-level min/max skipping.
     */
    public Integer runLength;

    /**
     * Returns a new spec with each non-null field of {@code override} replacing
     * the corresponding field of this one. Used to apply a workload's partial
     * column definition on top of a preset's column.
     */
    public ColumnSpec mergedWith(ColumnSpec override) {
        ColumnSpec m = new ColumnSpec();
        m.type = pick(override.type, type);
        m.kind = pick(override.kind, kind);
        m.cardinality = pick(override.cardinality, cardinality);
        m.distribution = pick(override.distribution, distribution);
        m.skew = pick(override.skew, skew);
        m.scramble = pick(override.scramble, scramble);
        m.min = pick(override.min, min);
        m.max = pick(override.max, max);
        m.scale = pick(override.scale, scale);
        m.values = pick(override.values, values);
        m.weights = pick(override.weights, weights);
        m.length = pick(override.length, length);
        m.words = pick(override.words, words);
        m.trueRate = pick(override.trueRate, trueRate);
        m.jitterMs = pick(override.jitterMs, jitterMs);
        m.randomRatio = pick(override.randomRatio, randomRatio);
        m.nullRate = pick(override.nullRate, nullRate);
        m.runLength = pick(override.runLength, runLength);
        return m;
    }

    private static <T> T pick(T override, T base) {
        return override != null ? override : base;
    }
}
