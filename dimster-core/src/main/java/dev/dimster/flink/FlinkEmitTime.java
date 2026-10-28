package dev.dimster.flink;

import dev.dimster.datagen.RecordFormat;
import dev.dimster.kafka.ConsumerCallback;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * Reads the time a Flink job emitted an output record. The runner's
 * {@code EmitTimeHeaders} (dimster-flink) stamps every record it writes with
 * the {@value #HEADER} header: 8 bytes, big-endian microseconds since the
 * epoch. The modules share no classes, so the name and encoding are the
 * contract.
 *
 * <p>Consumers report it with {@link #report}, so that end-to-end latency
 * splits at the Flink job into producer → Flink and Flink → consumer. Records
 * not written by Flink have no headers, so the lookup costs next to nothing.
 */
public final class FlinkEmitTime {

    public static final String HEADER = "dimster-flink-emit";

    private FlinkEmitTime() {}

    /**
     * Calls {@link ConsumerCallback#flinkRecordReceived} if a Flink job stamped
     * the record. Consumers call it right after {@code messageReceived}.
     */
    public static void report(ConsumerCallback callback, ConsumerRecord<String, byte[]> record,
                              RecordFormat format, Instant processingStartTime) {
        long emitMicros = micros(record.headers());
        if (emitMicros >= 0) {
            callback.flinkRecordReceived(record.value(), format, record.timestamp(), emitMicros,
                    processingStartTime);
        }
    }

    /** The emit time in epoch microseconds, or -1 when the record has no (valid) stamp. */
    public static long micros(Headers headers) {
        Header h = headers.lastHeader(HEADER);
        if (h == null || h.value() == null || h.value().length != Long.BYTES) {
            return -1;
        }
        return ByteBuffer.wrap(h.value()).getLong();
    }
}
