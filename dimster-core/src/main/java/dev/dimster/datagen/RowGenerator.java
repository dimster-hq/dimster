package dev.dimster.datagen;

import dev.dimster.MessageType;

import java.util.SplittableRandom;

/**
 * Generates and encodes the records of one producer thread.
 *
 * <p>Each call to {@link #next} produces one complete record value: the given
 * Dimster header followed by one value per column, encoded in the dataset's
 * format. The producer's random stream is seeded from the dataset seed and the
 * producer id, so a run is reproducible, while rank-to-value mappings are
 * shared by all producers (see {@link DatasetGenerator}).
 *
 * <p>For the payload dataset, normal records come from
 * {@link DatasetGenerator.PrebuiltRecords}: shared arrays that callers must
 * not modify.
 *
 * <p>Not thread-safe: the producer loop owns one instance per producer thread.
 */
public final class RowGenerator {

    private final SplittableRandom rnd;
    private final ColumnGenerator[] columns;
    private final RecordWriter writer;
    private final int keyColumn;
    private final DatasetGenerator.PrebuiltRecords prebuilt;
    private final RankSampler prebuiltRanks;

    /**
     * @param prebuilt      the payload dataset's prebuilt records, or null
     * @param prebuiltRanks samples the payload rank when {@code prebuilt} is set
     */
    RowGenerator(long seed, ColumnGenerator[] columns, RecordWriter writer, int keyColumn,
                 DatasetGenerator.PrebuiltRecords prebuilt, RankSampler prebuiltRanks) {
        this.rnd = new SplittableRandom(seed);
        this.columns = columns;
        this.writer = writer;
        this.keyColumn = keyColumn;
        this.prebuilt = prebuilt;
        this.prebuiltRanks = prebuiltRanks;
    }

    /**
     * Generates the next row and returns the encoded record. The array may be
     * shared (see the class comment) and must not be modified.
     */
    public byte[] next(byte[] header) {
        if (prebuilt != null && header.length == 1 && header[0] == MessageType.NORMAL) {
            return prebuilt.normal(prebuiltRanks.next(rnd));
        }
        writer.begin(header);
        for (ColumnGenerator c : columns) {
            c.next(rnd, writer);
        }
        return writer.finish();
    }

    /**
     * The key-column value of the row last returned by {@link #next}, or null
     * when the dataset has no key column or the value was null.
     */
    public String key() {
        return keyColumn >= 0 ? columns[keyColumn].lastKey() : null;
    }
}
