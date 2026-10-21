package dev.dimster.datagen;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.dimster.datagen.ColumnSpec.Distribution;
import dev.dimster.datagen.ColumnSpec.Kind;
import dev.dimster.datagen.ColumnSpec.Type;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Turns a workload's {@link DatasetSpec} into a {@link ResolvedDataset}:
 * loads the preset, merges inline columns onto it, fills in defaults and
 * validates everything.
 *
 * <p>Resolution runs on the coordinator when a test point is set up, so that a
 * bad dataset fails the run before any load starts, with a message naming the
 * column. Workers receive only the resolved form and never re-validate.
 */
public final class DatasetResolver {

    /** Column names must be valid Avro names; this also keeps JSON field names escape-free. */
    private static final Pattern COLUMN_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern PRESET_NAME = Pattern.compile("[a-z0-9_-]+");

    private static final ObjectMapper PRESET_MAPPER = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    private DatasetResolver() {}

    /**
     * Resolves a workload's {@code spec}. Workloads without a dataset use
     * {@link #payloadDataset} instead.
     *
     * @throws IllegalArgumentException if the dataset is invalid
     */
    public static ResolvedDataset resolve(DatasetSpec spec) {
        RecordFormat format = spec.format != null ? spec.format : RecordFormat.AVRO;
        if (format == RecordFormat.RAW) {
            throw new IllegalArgumentException(
                    "dataset format RAW is the default payload layout; datasets use AVRO or JSON");
        }
        long seed = spec.seed != null ? spec.seed : 0L;

        Map<String, ColumnSpec> merged = mergeColumns(spec);
        if (merged.isEmpty()) {
            throw new IllegalArgumentException("dataset needs a preset or at least one column");
        }

        List<ResolvedDataset.Column> columns = new ArrayList<>();
        for (Map.Entry<String, ColumnSpec> e : merged.entrySet()) {
            columns.add(resolveColumn(e.getKey(), e.getValue()));
        }

        if (spec.keyColumn != null && !merged.containsKey(spec.keyColumn)) {
            throw new IllegalArgumentException(
                    "dataset keyColumn '" + spec.keyColumn + "' is not a column of the dataset");
        }
        return new ResolvedDataset(spec.preset, format, seed, spec.keyColumn, columns);
    }

    /** Name of the payload dataset's one column. */
    public static final String PAYLOAD_COLUMN = "payload";

    /**
     * The default dataset of workloads without a {@code dataset}: schema
     * {@code {_dimster: bytes, payload: bytes}} in RAW encoding, so a record
     * is the header followed by {@code payloadLength} payload bytes.
     *
     * @param payloadLength bytes per payload (messageSize - 1, so that normal
     *                      records are exactly messageSize bytes)
     * @param randomRatio   fraction of each payload that is random, the rest zeros
     * @param poolSize      number of distinct payloads, sampled uniformly
     * @param fixedValue    the only payload (from payloadFile), or null to generate them
     */
    public static ResolvedDataset payloadDataset(int payloadLength, double randomRatio, int poolSize,
                                                 byte[] fixedValue) {
        Errors err = new Errors(PAYLOAD_COLUMN);
        if (payloadLength < 0) {
            throw err.of("messageSize must be >= 1");
        }
        if (randomRatio < 0 || randomRatio > 1) {
            throw err.of("randomBytesRatio must be in [0, 1]");
        }
        if (poolSize < 1) {
            throw err.of("randomizedPayloadPoolSize must be >= 1");
        }
        if (fixedValue != null && fixedValue.length != payloadLength) {
            throw err.of("the payload file must be " + payloadLength + " bytes after its first byte");
        }
        ResolvedDataset.Column payload = column(PAYLOAD_COLUMN, Type.BYTES, null, null, null,
                fixedValue != null ? 1 : poolSize, Distribution.UNIFORM, 0, false, 0, 0, -1, 0, 0, 0, 1,
                new int[]{payloadLength, payloadLength}, new int[]{0, 0}, randomRatio, fixedValue);
        return new ResolvedDataset(ResolvedDataset.PAYLOAD_PRESET, RecordFormat.RAW, 0, null, List.of(payload));
    }

    /** Preset columns in preset order, overridden field by field, then new inline columns. */
    private static Map<String, ColumnSpec> mergeColumns(DatasetSpec spec) {
        Map<String, ColumnSpec> merged = new LinkedHashMap<>();
        if (spec.preset != null) {
            DatasetSpec preset = loadPreset(spec.preset);
            if (preset.columns != null) {
                merged.putAll(preset.columns);
            }
        }
        if (spec.columns != null) {
            for (Map.Entry<String, ColumnSpec> e : spec.columns.entrySet()) {
                if (e.getValue() == null) {
                    throw new IllegalArgumentException("dataset column '" + e.getKey() + "' has no definition");
                }
                ColumnSpec base = merged.get(e.getKey());
                merged.put(e.getKey(), base != null ? base.mergedWith(e.getValue()) : e.getValue());
            }
        }
        return merged;
    }

    /** Loads {@code datagen/presets/<name>.yaml}. Presets are datasets with columns only. */
    static DatasetSpec loadPreset(String name) {
        if (!PRESET_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("invalid dataset preset name '" + name + "'");
        }
        String resource = "datagen/presets/" + name + ".yaml";
        try (InputStream in = DatasetResolver.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalArgumentException("unknown dataset preset '" + name + "'");
            }
            return PRESET_MAPPER.readValue(in, DatasetSpec.class);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load dataset preset " + resource, e);
        }
    }

    private static ResolvedDataset.Column resolveColumn(String name, ColumnSpec c) {
        Errors err = new Errors(name);
        if (!COLUMN_NAME.matcher(name).matches()) {
            throw err.of("names must match [A-Za-z_][A-Za-z0-9_]*");
        }
        if (name.startsWith(ResolvedDataset.HEADER_FIELD)) {
            throw err.of("names starting with '" + ResolvedDataset.HEADER_FIELD + "' are reserved for Dimster");
        }
        if (c.type == null) {
            throw err.of("type is required");
        }

        double nullRate = c.nullRate != null ? c.nullRate : 0.0;
        if (nullRate < 0 || nullRate >= 1) {
            throw err.of("nullRate must be in [0, 1)");
        }
        int runLength = c.runLength != null ? c.runLength : 1;
        if (runLength < 1) {
            throw err.of("runLength must be >= 1");
        }

        // Fields that only make sense for some types are rejected elsewhere,
        // so that a misplaced setting is reported rather than silently ignored.
        boolean string = c.type == Type.STRING;
        boolean bytes = c.type == Type.BYTES;
        boolean integral = c.type == Type.LONG || c.type == Type.INT;
        err.onlyFor(c.kind, "kind", string);
        err.onlyFor(c.values, "values", string);
        err.onlyFor(c.weights, "weights", string);
        err.onlyFor(c.length, "length", string || bytes);
        err.onlyFor(c.randomRatio, "randomRatio", bytes);
        err.onlyFor(c.words, "words", string);
        err.onlyFor(c.trueRate, "trueRate", c.type == Type.BOOLEAN);
        err.onlyFor(c.jitterMs, "jitterMs", c.type == Type.TIMESTAMP);
        err.onlyFor(c.scale, "scale", c.type == Type.DOUBLE);
        err.onlyFor(c.scramble, "scramble", integral);
        err.onlyFor(c.cardinality, "cardinality", integral || string || bytes);
        err.onlyFor(c.min, "min", integral || c.type == Type.DOUBLE);
        err.onlyFor(c.max, "max", integral || c.type == Type.DOUBLE);
        err.onlyFor(c.distribution, "distribution", integral || string || bytes || c.type == Type.DOUBLE);

        return switch (c.type) {
            case LONG, INT -> resolveIntegral(name, c, err, nullRate, runLength);
            case DOUBLE -> resolveDouble(name, c, err, nullRate, runLength);
            case BOOLEAN -> {
                double trueRate = c.trueRate != null ? c.trueRate : 0.5;
                if (trueRate < 0 || trueRate > 1) {
                    throw err.of("trueRate must be in [0, 1]");
                }
                yield column(name, c.type, null, null, null, 0, Distribution.UNIFORM, 0, false,
                        0, 0, -1, trueRate, 0, nullRate, runLength);
            }
            case STRING -> resolveString(name, c, err, nullRate, runLength);
            case BYTES -> resolveBytes(name, c, err, nullRate, runLength);
            case TIMESTAMP -> {
                long jitter = c.jitterMs != null ? c.jitterMs : 0;
                if (jitter < 0) {
                    throw err.of("jitterMs must be >= 0");
                }
                yield column(name, c.type, null, null, null, 0, Distribution.UNIFORM, 0, false,
                        0, 0, -1, 0, jitter, nullRate, runLength);
            }
            case SEQUENCE -> column(name, c.type, null, null, null, 0, Distribution.UNIFORM, 0, false,
                    0, 0, -1, 0, 0, nullRate, runLength);
        };
    }

    private static ResolvedDataset.Column resolveIntegral(String name, ColumnSpec c, Errors err,
                                                          double nullRate, int runLength) {
        boolean isInt = c.type == Type.INT;
        double min = c.min != null ? c.min : 0;
        double max = c.max != null ? c.max : (isInt ? Integer.MAX_VALUE : Long.MAX_VALUE);
        if (min != Math.floor(min) || max != Math.floor(max)) {
            throw err.of("min and max must be whole numbers");
        }
        if (isInt && (min < Integer.MIN_VALUE || max > Integer.MAX_VALUE)) {
            throw err.of("INT min and max must fit in 32 bits");
        }
        if (max < min) {
            throw err.of("max must be >= min");
        }
        long domain = domainSize((long) min, (long) max);

        Distribution dist = c.distribution != null ? c.distribution : Distribution.UNIFORM;
        long cardinality = c.cardinality != null ? c.cardinality : 0;
        if (c.cardinality != null) {
            if (cardinality < 1) {
                throw err.of("cardinality must be >= 1");
            }
            if (cardinality > domain) {
                throw err.of("cardinality " + cardinality + " exceeds the " + domain + " values in [min, max]");
            }
            if (dist == Distribution.NORMAL) {
                throw err.of("NORMAL applies to ranges; categorical columns use UNIFORM or ZIPF");
            }
        } else if (dist == Distribution.ZIPF) {
            throw err.of("ZIPF requires a cardinality");
        }
        double skew = resolveSkew(c, err, dist, cardinality);
        boolean scramble = c.scramble == null || c.scramble;
        return column(name, c.type, null, null, null, cardinality, dist, skew, scramble,
                min, max, -1, 0, 0, nullRate, runLength);
    }

    private static ResolvedDataset.Column resolveDouble(String name, ColumnSpec c, Errors err,
                                                        double nullRate, int runLength) {
        double min = c.min != null ? c.min : 0;
        double max = c.max != null ? c.max : 1;
        if (max < min) {
            throw err.of("max must be >= min");
        }
        Distribution dist = c.distribution != null ? c.distribution : Distribution.UNIFORM;
        if (dist == Distribution.ZIPF) {
            throw err.of("DOUBLE columns use UNIFORM or NORMAL");
        }
        int scale = c.scale != null ? c.scale : -1;
        if (c.scale != null && (scale < 0 || scale > 10)) {
            throw err.of("scale must be in [0, 10]");
        }
        return column(name, c.type, null, null, null, 0, dist, 0, false,
                min, max, scale, 0, 0, nullRate, runLength);
    }

    private static ResolvedDataset.Column resolveString(String name, ColumnSpec c, Errors err,
                                                        double nullRate, int runLength) {
        Distribution dist = c.distribution != null ? c.distribution : Distribution.UNIFORM;
        if (dist == Distribution.NORMAL) {
            throw err.of("STRING columns use UNIFORM or ZIPF");
        }

        if (c.values != null) {
            // Weighted enum: the values list is the whole value space.
            if (c.kind != null || c.cardinality != null || c.distribution != null) {
                throw err.of("values can't be combined with kind, cardinality or distribution");
            }
            if (c.values.isEmpty()) {
                throw err.of("values must not be empty");
            }
            List<Double> weights = normaliseWeights(c, err);
            return column(name, Type.STRING, null, List.copyOf(c.values), weights, c.values.size(),
                    Distribution.UNIFORM, 0, false, 0, 0, -1, 0, 0, nullRate, runLength);
        }
        if (c.weights != null) {
            throw err.of("weights require values");
        }

        Kind kind = c.kind != null ? c.kind : Kind.ALPHANUMERIC;
        int[] length = range(c.length, 8, 16, "length", err);
        int[] words = range(c.words, 5, 15, "words", err);
        long space = StringValues.valueSpace(kind, length[0], length[1], words[0], words[1]);

        long cardinality;
        if (c.cardinality != null) {
            cardinality = c.cardinality;
            if (cardinality < 1) {
                throw err.of("cardinality must be >= 1");
            }
            if (cardinality > space) {
                throw err.of("cardinality " + cardinality + " exceeds the " + space + " values of kind " + kind);
            }
        } else {
            // Dictionary kinds default to the whole dictionary; the others are unbounded.
            cardinality = isDictionary(kind) ? space : 0;
        }
        if (cardinality == 0 && dist == Distribution.ZIPF) {
            throw err.of("ZIPF requires a cardinality");
        }
        double skew = resolveSkew(c, err, dist, cardinality);
        return column(name, Type.STRING, kind, null, null, cardinality, dist, skew, false,
                0, 0, -1, 0, 0, nullRate, runLength, length, words, 0, null);
    }

    private static ResolvedDataset.Column resolveBytes(String name, ColumnSpec c, Errors err,
                                                       double nullRate, int runLength) {
        Distribution dist = c.distribution != null ? c.distribution : Distribution.UNIFORM;
        if (dist == Distribution.NORMAL) {
            throw err.of("BYTES columns use UNIFORM or ZIPF");
        }
        int[] length = range(c.length, 100, 100, "length", err);
        double randomRatio = c.randomRatio != null ? c.randomRatio : 1.0;
        if (randomRatio < 0 || randomRatio > 1) {
            throw err.of("randomRatio must be in [0, 1]");
        }
        // Without a cardinality every row gets fresh bytes.
        long cardinality = c.cardinality != null ? c.cardinality : 0;
        if (c.cardinality != null && cardinality < 1) {
            throw err.of("cardinality must be >= 1");
        }
        if (cardinality == 0 && dist == Distribution.ZIPF) {
            throw err.of("ZIPF requires a cardinality");
        }
        double skew = resolveSkew(c, err, dist, cardinality);
        return column(name, Type.BYTES, null, null, null, cardinality, dist, skew, false,
                0, 0, -1, 0, 0, nullRate, runLength, length, new int[]{0, 0}, randomRatio, null);
    }

    private static double resolveSkew(ColumnSpec c, Errors err, Distribution dist, long cardinality) {
        double skew = c.skew != null ? c.skew : 1.0;
        if (skew < 0) {
            throw err.of("skew must be >= 0");
        }
        if (dist == Distribution.ZIPF && cardinality > Integer.MAX_VALUE) {
            throw err.of("ZIPF supports cardinalities up to " + Integer.MAX_VALUE);
        }
        return skew;
    }

    private static List<Double> normaliseWeights(ColumnSpec c, Errors err) {
        List<Double> raw = c.weights;
        if (raw == null) {
            return null;
        }
        if (raw.size() != c.values.size()) {
            throw err.of("weights must have one entry per value");
        }
        double sum = 0;
        for (Double w : raw) {
            if (w == null || w < 0) {
                throw err.of("weights must be >= 0");
            }
            sum += w;
        }
        if (sum <= 0) {
            throw err.of("weights must not all be zero");
        }
        List<Double> normalised = new ArrayList<>(raw.size());
        for (Double w : raw) {
            normalised.add(w / sum);
        }
        return List.copyOf(normalised);
    }

    private static int[] range(List<Integer> spec, int defMin, int defMax, String field, Errors err) {
        if (spec == null) {
            return new int[]{defMin, defMax};
        }
        if (spec.size() != 2 || spec.get(0) == null || spec.get(1) == null
                || spec.get(0) < 1 || spec.get(1) < spec.get(0)) {
            throw err.of(field + " must be [min, max] with 1 <= min <= max");
        }
        return new int[]{spec.get(0), spec.get(1)};
    }

    private static boolean isDictionary(Kind kind) {
        return switch (kind) {
            case FIRST_NAME, LAST_NAME, CITY, COUNTRY, DOMAIN, USER_AGENT, WORD -> true;
            default -> false;
        };
    }

    /** Number of integers in [min, max], saturating at Long.MAX_VALUE. */
    static long domainSize(long min, long max) {
        long size = max - min + 1;
        return size <= 0 ? Long.MAX_VALUE : size;
    }

    private static ResolvedDataset.Column column(String name, Type type, Kind kind, List<String> values,
                                                 List<Double> weights, long cardinality, Distribution dist,
                                                 double skew, boolean scramble, double min, double max,
                                                 int scale, double trueRate, long jitterMs,
                                                 double nullRate, int runLength) {
        return column(name, type, kind, values, weights, cardinality, dist, skew, scramble, min, max,
                scale, trueRate, jitterMs, nullRate, runLength, new int[]{0, 0}, new int[]{0, 0}, 0, null);
    }

    private static ResolvedDataset.Column column(String name, Type type, Kind kind, List<String> values,
                                                 List<Double> weights, long cardinality, Distribution dist,
                                                 double skew, boolean scramble, double min, double max,
                                                 int scale, double trueRate, long jitterMs,
                                                 double nullRate, int runLength, int[] length, int[] words,
                                                 double randomRatio, byte[] fixedValue) {
        return new ResolvedDataset.Column(name, type, kind, values, weights, cardinality, dist, skew,
                scramble, min, max, scale, length[0], length[1], words[0], words[1],
                trueRate, jitterMs, nullRate, runLength, randomRatio, fixedValue);
    }

    /** Builds validation errors that name the column. */
    private record Errors(String column) {
        IllegalArgumentException of(String message) {
            return new IllegalArgumentException("dataset column '" + column + "': " + message);
        }

        void onlyFor(Object value, String field, boolean allowed) {
            if (value != null && !allowed) {
                throw of(field + " does not apply to this column type");
            }
        }
    }
}
