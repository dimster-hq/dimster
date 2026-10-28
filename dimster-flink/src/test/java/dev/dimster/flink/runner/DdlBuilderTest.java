package dev.dimster.flink.runner;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DdlBuilderTest {

    static FlinkJobSpec spec(String format, String guarantee, Map<String, String> sinkOptions) {
        return new FlinkJobSpec("dimster-abc", "SELECT * FROM source", format,
                List.of(new FlinkJobSpec.Column("user_id", "BIGINT NOT NULL"),
                        new FlinkJobSpec.Column("referrer", "STRING")),
                List.of(new FlinkJobSpec.Pipeline("dimster-abc-0000", "dimster-abc-0000-out")),
                "dimster-grp-flink-abc",
                Map.of("bootstrap.servers", "kafka:9092"),
                2, guarantee, 10000, Map.of(), Map.of(), sinkOptions);
    }

    private static final DdlBuilder.SchemaColumn HEADER = new DdlBuilder.SchemaColumn("_dimster", "BYTES NOT NULL");
    private static final DdlBuilder.SchemaColumn TS =
            new DdlBuilder.SchemaColumn("_dimster_ts", "TIMESTAMP(3) WITH LOCAL TIME ZONE");
    private static final DdlBuilder.SchemaColumn KEY = new DdlBuilder.SchemaColumn("_dimster_key", "VARCHAR(2147483647)");
    private static final List<DdlBuilder.SchemaColumn> SOURCE = List.of(HEADER,
            new DdlBuilder.SchemaColumn("user_id", "BIGINT NOT NULL"), KEY, TS);

    @Test
    public void sourceMirrorsTheDatasetWithHeaderFirstAndMetadataLast() {
        String ddl = DdlBuilder.sourceDdl(spec("AVRO", "AT_LEAST_ONCE", Map.of()),
                new FlinkJobSpec.Pipeline("in", "out"));
        int header = ddl.indexOf("`_dimster` BYTES NOT NULL");
        int user = ddl.indexOf("`user_id` BIGINT NOT NULL");
        int ts = ddl.indexOf("`_dimster_ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'");
        assertTrue(header > 0 && header < user && user < ts);
        assertTrue(ddl.contains("'topic' = 'in'"));
        assertTrue(ddl.contains("'properties.bootstrap.servers' = 'kafka:9092'"));
        assertTrue(ddl.contains("'scan.startup.mode' = 'earliest-offset'"));
        assertTrue(ddl.contains("'value.format' = 'avro'"));
        assertTrue(ddl.contains("'value.fields-include' = 'EXCEPT_KEY'"));
    }

    @Test
    public void jsonHeaderIsAHexString() {
        String ddl = DdlBuilder.sourceDdl(spec("JSON", "AT_LEAST_ONCE", Map.of()),
                new FlinkJobSpec.Pipeline("in", "out"));
        assertTrue(ddl.contains("`_dimster` STRING NOT NULL"));
        assertTrue(ddl.contains("'value.format' = 'json'"));
    }

    @Test
    public void sinkPutsHeaderFirstWhateverTheQueryOrder() {
        var city = new DdlBuilder.SchemaColumn("city", "VARCHAR(2147483647)");
        var layout = DdlBuilder.sinkLayout(List.of(TS, city, HEADER), SOURCE);
        assertEquals(List.of("_dimster", "city", "_dimster_ts"), layout.columnNames());

        String ddl = DdlBuilder.sinkDdl(spec("AVRO", "AT_LEAST_ONCE", Map.of()), 0, layout);
        assertTrue(ddl.indexOf("`_dimster` BYTES NOT NULL") < ddl.indexOf("`city`"));
        assertTrue(ddl.contains("'topic' = 'dimster-abc-0000-out'"));
        assertTrue(ddl.contains("'sink.delivery-guarantee' = 'at-least-once'"));
        // Unkeyed output: no key format.
        assertFalse(ddl.contains("key.format"));
    }

    @Test
    public void selectedKeyBecomesTheRecordKey() {
        var layout = DdlBuilder.sinkLayout(List.of(HEADER, KEY, TS), SOURCE);
        assertEquals(List.of("_dimster", "_dimster_key", "_dimster_ts"), layout.columnNames());
        String ddl = DdlBuilder.sinkDdl(spec("AVRO", "AT_LEAST_ONCE", Map.of()), 0, layout);
        assertTrue(ddl.contains("'key.fields' = '_dimster_key'"));
        assertTrue(ddl.contains("'value.fields-include' = 'EXCEPT_KEY'"));
    }

    @Test
    public void missingTimestampIsExplained() {
        var e = assertThrows(IllegalArgumentException.class,
                () -> DdlBuilder.sinkLayout(List.of(HEADER), SOURCE));
        assertTrue(e.getMessage().contains("missing _dimster_ts"));
    }

    @Test
    public void changedHeaderTypeIsRejected() {
        var cast = new DdlBuilder.SchemaColumn("_dimster", "VARCHAR(2147483647)");
        var e = assertThrows(IllegalArgumentException.class,
                () -> DdlBuilder.sinkLayout(List.of(cast, TS), SOURCE));
        assertTrue(e.getMessage().contains("passed through unchanged"));
    }

    @Test
    public void exactlyOnceSetsTransactionsAndUserOptionsWin() {
        var layout = DdlBuilder.sinkLayout(List.of(HEADER, TS), SOURCE);
        String ddl = DdlBuilder.sinkDdl(spec("AVRO", "EXACTLY_ONCE",
                Map.of("properties.transaction.timeout.ms", "60000")), 0, layout);
        assertTrue(ddl.contains("'sink.delivery-guarantee' = 'exactly-once'"));
        assertTrue(ddl.contains("'sink.transactional-id-prefix' = 'dimster-abc-0'"));
        assertTrue(ddl.contains("'properties.transaction.timeout.ms' = '60000'"));
    }

    @Test
    public void sinkWritesTheEmitTimeHeadersAsMetadata() {
        var layout = DdlBuilder.sinkLayout(List.of(HEADER, TS), SOURCE);
        String ddl = DdlBuilder.sinkDdl(spec("AVRO", "AT_LEAST_ONCE", Map.of()), 0, layout);
        // Last, as DimsterSqlJob appends the computed column after the query's.
        assertTrue(ddl, ddl.contains("`_dimster_headers` MAP<STRING, BYTES> METADATA FROM 'headers'\n)"));
    }

    @Test
    public void queryColumnNamedLikeTheHeadersIsRejected() {
        var clash = new DdlBuilder.SchemaColumn("_dimster_headers", "INT");
        var e = assertThrows(IllegalArgumentException.class,
                () -> DdlBuilder.sinkLayout(List.of(HEADER, TS, clash), SOURCE));
        assertTrue(e.getMessage().contains("_dimster_headers"));
    }

    @Test
    public void quotesAreEscaped() {
        assertEquals("`a``b`", DdlBuilder.quote("a`b"));
    }
}
