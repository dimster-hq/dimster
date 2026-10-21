package dev.dimster.datagen;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.LinkedHashMap;

/**
 * The {@code dataset} section of a workload: which synthetic rows producers
 * send instead of the default payload dataset (random opaque bytes).
 *
 * <p>A dataset is either a named preset ({@code clicks}, {@code orders},
 * {@code taxi}; YAML resources under {@code datagen/presets/}), inline
 * {@link #columns}, or a preset with inline columns merged onto it by name.
 * Every record is encoded as {@code {_dimster: bytes, <columns>...}} in
 * {@link #format}, where {@code _dimster} is Dimster's own message header
 * (message type, plus correctness fields in correctness mode).
 *
 * <p>This is configuration only. {@link DatasetResolver} turns it into an
 * immutable, validated {@link ResolvedDataset}, which is what the coordinator
 * ships to workers and what the generators read.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatasetSpec {

    /** Optional preset name; inline {@link #columns} are merged onto it. */
    public String preset;

    /** Record encoding: AVRO (the default) or JSON. */
    public RecordFormat format;

    /**
     * Seed for value generation. With the same seed, a column's rank-to-value
     * mapping is identical on every producer and every run. Default 0.
     */
    public Long seed;

    /** Optional column whose value becomes the record key (with the KEY_COLUMN distributor). */
    public String keyColumn;

    /** Column definitions in schema order. Jackson preserves YAML order in a LinkedHashMap. */
    public LinkedHashMap<String, ColumnSpec> columns;
}
