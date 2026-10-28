package dev.dimster.flink.runner;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;

/**
 * Stamps each output record with the time the Flink job emitted it, so that
 * consumers can split end-to-end latency into producer → Flink and
 * Flink → consumer. The stamp is the {@value #HEADER} Kafka header: 8 bytes,
 * big-endian microseconds since the epoch. {@code dev.dimster.flink.FlinkEmitTime}
 * in dimster-core decodes it; the name and encoding are the contract between
 * the modules, which share no classes.
 *
 * <p>{@link DimsterSqlJob} appends it to the projection that feeds the sink,
 * so the stamp is taken after the workload's query and before the Kafka sink
 * serializes and sends the record. The record timestamp stays the producer's,
 * so end-to-end latency is unchanged.
 */
public final class EmitTimeHeaders extends ScalarFunction {

    public static final String HEADER = "dimster-flink-emit";

    /** The sink's {@code headers} metadata column: just the emit time. */
    @DataTypeHint("MAP<STRING, BYTES>")
    public Map<String, byte[]> eval() {
        return Map.of(HEADER, encode(Instant.now()));
    }

    static byte[] encode(Instant time) {
        long micros = time.getEpochSecond() * 1_000_000L + time.getNano() / 1_000;
        return ByteBuffer.allocate(Long.BYTES).putLong(micros).array();
    }

    /** The planner must call it for every row, not fold it to a constant. */
    @Override
    public boolean isDeterministic() {
        return false;
    }
}
