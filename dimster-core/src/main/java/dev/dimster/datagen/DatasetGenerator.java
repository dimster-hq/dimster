package dev.dimster.datagen;

import dev.dimster.MessageType;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.LongFunction;

/**
 * The worker-side entry point for generating a dataset: built once per worker
 * when a test point's {@link ResolvedDataset} arrives, then asked for one
 * {@link RowGenerator} per producer thread.
 *
 * <p>It owns what is shared by all producer threads: the rank-to-value
 * mappings of STRING and BYTES columns and, for columns with at most
 * {@link #CACHE_MAX_CARDINALITY} values, a cache of the encoded values so that
 * composite values (names, addresses, URLs) and random payloads are built
 * once rather than per row. Values are deterministic per rank, so threads racing to fill the same
 * cache slot store equal values; the {@link AtomicReferenceArray} gives safe
 * publication. Everything here is thread-safe; per-thread state lives in
 * {@link RowGenerator}.
 */
public final class DatasetGenerator {

    /** Columns with more values than this compute values per row instead of caching them. */
    static final int CACHE_MAX_CARDINALITY = 100_000;
    /** BYTES columns whose values could take more than this compute them per row instead. */
    static final long CACHE_MAX_BYTES = 64L << 20;

    private final ResolvedDataset dataset;
    /**
     * Per column: rank -> encoded value for STRING columns and categorical
     * BYTES columns, null otherwise.
     */
    private final LongFunction<byte[]>[] rankValues;
    /** Whole records of the payload dataset, shared by all producers; null for other datasets. */
    private final PrebuiltRecords prebuilt;

    @SuppressWarnings("unchecked")
    public DatasetGenerator(ResolvedDataset dataset) {
        this.dataset = dataset;
        List<ResolvedDataset.Column> columns = dataset.columns();
        this.rankValues = new LongFunction[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            ResolvedDataset.Column c = columns.get(i);
            if (c.type() == ColumnSpec.Type.STRING) {
                rankValues[i] = stringValuesFor(c);
            } else if (c.type() == ColumnSpec.Type.BYTES && c.categorical()) {
                rankValues[i] = bytesValuesFor(c);
            }
        }
        this.prebuilt = PrebuiltRecords.applies(dataset) ? new PrebuiltRecords(dataset.columns().get(0),
                bytesValueFunction(dataset.columns().get(0))) : null;
    }

    public ResolvedDataset dataset() {
        return dataset;
    }

    /**
     * A row generator for one producer thread.
     *
     * @param producerName the producer's stable name, unique across the test;
     *                     seeds the producer's random stream
     * @param producerIdx  the producer's index within its topic, unique per
     *                     topic across workers; makes SEQUENCE values unique
     *                     within a topic
     */
    public RowGenerator newRowGenerator(String producerName, int producerIdx) {
        List<ResolvedDataset.Column> columns = dataset.columns();
        ColumnGenerator[] generators = new ColumnGenerator[columns.size()];
        for (int i = 0; i < generators.length; i++) {
            ResolvedDataset.Column c = columns.get(i);
            long columnSeed = Hashing.columnSeed(dataset.seed(), c.name());
            generators[i] = switch (c.type()) {
                case LONG, INT -> c.categorical()
                        ? new ColumnGenerator.CategoricalIntegral(c, columnSeed)
                        : new ColumnGenerator.RangeIntegral(c);
                case TIMESTAMP -> new ColumnGenerator.Timestamp(c);
                case SEQUENCE -> new ColumnGenerator.Sequence(c, producerIdx);
                case DOUBLE -> new ColumnGenerator.DoubleColumn(c);
                case BOOLEAN -> new ColumnGenerator.BooleanColumn(c);
                case STRING -> new ColumnGenerator.StringColumn(c, rankValues[i]);
                case BYTES -> new ColumnGenerator.BytesColumn(c, rankValues[i]);
            };
        }
        return new RowGenerator(Hashing.producerSeed(dataset.seed(), producerName.hashCode()), generators,
                dataset.format().newWriter(dataset), dataset.keyColumnIndex(),
                prebuilt, prebuilt != null ? RankSampler.of(columns.get(0)) : null);
    }

    /**
     * Average encoded size of a normal record, estimated from {@code rows}
     * generated rows. Engines and results that convert message counts to
     * bytes use this as the test point's message size when a dataset is set.
     */
    public static int estimateAverageRecordSize(ResolvedDataset dataset, int rows) {
        RowGenerator gen = new DatasetGenerator(dataset).newRowGenerator("size-estimate", 0);
        byte[] header = {0};
        long total = 0;
        for (int i = 0; i < rows; i++) {
            total += gen.next(header).length;
        }
        return (int) Math.max(1, Math.round((double) total / rows));
    }

    private LongFunction<byte[]> stringValuesFor(ResolvedDataset.Column c) {
        if (c.values() != null) {
            byte[][] encoded = new byte[c.values().size()][];
            for (int j = 0; j < encoded.length; j++) {
                encoded[j] = c.values().get(j).getBytes(StandardCharsets.UTF_8);
            }
            return rank -> encoded[(int) rank];
        }
        StringValues values = StringValues.of(c.kind(), Hashing.columnSeed(dataset.seed(), c.name()),
                c.minLength(), c.maxLength(), c.minWords(), c.maxWords());
        if (values instanceof StringValues.DictionaryValues || !c.categorical()
                || c.cardinality() > CACHE_MAX_CARDINALITY) {
            // Dictionary lookups are already O(1); unbounded and very large
            // columns would not fit a cache.
            return values::valueFor;
        }
        return cached(c, values::valueFor);
    }

    /**
     * Rank -> value for a categorical BYTES column: the fixed value, or bytes
     * from a random stream seeded by the rank, so every producer agrees on
     * each rank's value.
     */
    private LongFunction<byte[]> bytesValuesFor(ResolvedDataset.Column c) {
        LongFunction<byte[]> values = bytesValueFunction(c);
        if (c.fixedValue() != null || !cacheable(c)) {
            return values;
        }
        return cached(c, values);
    }

    /** The uncached rank -> value function of a categorical BYTES column. */
    private LongFunction<byte[]> bytesValueFunction(ResolvedDataset.Column c) {
        if (c.fixedValue() != null) {
            byte[] fixed = c.fixedValue();
            return rank -> fixed;
        }
        long columnSeed = Hashing.columnSeed(dataset.seed(), c.name());
        return rank -> ColumnGenerator.BytesColumn.randomValue(
                new SplittableRandom(Hashing.rankSeed(columnSeed, rank)),
                c.minLength(), c.maxLength(), c.randomRatio());
    }

    /** Whether all of a BYTES column's values fit the cache limits. */
    private static boolean cacheable(ResolvedDataset.Column c) {
        return c.cardinality() <= CACHE_MAX_CARDINALITY
                && c.cardinality() * (c.maxLength() + 1L) <= CACHE_MAX_BYTES;
    }

    /**
     * The payload dataset's normal records, so that the default path sends
     * prebuilt arrays as the old payload pool did, with no per-record
     * allocation or copying. A normal record is the NORMAL header byte
     * followed by a payload, so it depends only on the payload's rank: every
     * record is built once, up front, and shared by all producer threads.
     * Normal records are nearly all the traffic; probes (a handful per test
     * point) and correctness records (whose header differs per record) are
     * encoded per row.
     *
     * <p>Records are shared and must never be modified; Kafka producers only
     * read them. The array is fully built before the generator is published
     * to producer threads.
     */
    static final class PrebuiltRecords {
        /** Indexed by rank: {@code {NORMAL, payload(rank)...}}. */
        private final byte[][] records;

        PrebuiltRecords(ResolvedDataset.Column payload, LongFunction<byte[]> payloads) {
            records = new byte[(int) payload.cardinality()][];
            for (int rank = 0; rank < records.length; rank++) {
                byte[] value = payloads.apply(rank);
                byte[] record = new byte[value.length + 1];
                record[0] = MessageType.NORMAL;
                System.arraycopy(value, 0, record, 1, value.length);
                records[rank] = record;
            }
        }

        /**
         * Only the payload dataset's shape qualifies: RAW, one non-null BYTES
         * column with a cacheable number of values and no run lengths (a
         * held value would need per-producer state).
         */
        static boolean applies(ResolvedDataset dataset) {
            if (dataset.format() != RecordFormat.RAW || dataset.columns().size() != 1) {
                return false;
            }
            ResolvedDataset.Column c = dataset.columns().get(0);
            return c.type() == ColumnSpec.Type.BYTES && c.categorical() && !c.nullable()
                    && c.runLength() == 1 && cacheable(c);
        }

        /** The normal record carrying the payload of {@code rank}. */
        byte[] normal(long rank) {
            return records[(int) rank];
        }
    }

    /** Wraps a deterministic rank -> value function with a lazily filled, shared cache. */
    private static LongFunction<byte[]> cached(ResolvedDataset.Column c, LongFunction<byte[]> values) {
        AtomicReferenceArray<byte[]> cache = new AtomicReferenceArray<>((int) c.cardinality());
        return rank -> {
            int i = (int) rank;
            byte[] v = cache.get(i);
            if (v == null) {
                v = values.apply(rank);
                cache.set(i, v);
            }
            return v;
        };
    }
}
