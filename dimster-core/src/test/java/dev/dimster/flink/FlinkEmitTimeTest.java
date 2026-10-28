package dev.dimster.flink;

import dev.dimster.datagen.RecordFormat;
import dev.dimster.kafka.ConsumerCallback;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FlinkEmitTimeTest {

    private static final long EMIT_MICROS = 1_700_000_000_123_456L;

    @Test
    public void decodesTheRunnersEightByteStamp() {
        var headers = new RecordHeaders();
        headers.add(FlinkEmitTime.HEADER, ByteBuffer.allocate(8).putLong(EMIT_MICROS).array());
        assertEquals(EMIT_MICROS, FlinkEmitTime.micros(headers));
    }

    @Test
    public void recordsWithoutAValidStampHaveNone() {
        assertEquals(-1, FlinkEmitTime.micros(new RecordHeaders()));
        var malformed = new RecordHeaders();
        malformed.add(FlinkEmitTime.HEADER, new byte[]{1, 2, 3});
        assertEquals(-1, FlinkEmitTime.micros(malformed));
    }

    @Test
    public void reportsOnlyStampedRecords() {
        List<long[]> reported = new ArrayList<>();
        ConsumerCallback callback = new ConsumerCallback() {
            @Override
            public void messageReceived(int size, long publishMillis, Instant delivery, Instant start) {}

            @Override
            public void error() {}

            @Override
            public void flinkRecordReceived(byte[] payload, RecordFormat format, long publishMillis,
                                            long emitMicros, Instant start) {
                reported.add(new long[]{publishMillis, emitMicros});
            }
        };
        Instant now = Instant.now();
        FlinkEmitTime.report(callback, record(new RecordHeaders()), RecordFormat.RAW, now);
        assertTrue(reported.isEmpty());

        var headers = new RecordHeaders();
        headers.add(FlinkEmitTime.HEADER, ByteBuffer.allocate(8).putLong(EMIT_MICROS).array());
        FlinkEmitTime.report(callback, record(headers), RecordFormat.RAW, now);
        assertEquals(1, reported.size());
        assertEquals(1_700_000_000_000L, reported.get(0)[0]);
        assertEquals(EMIT_MICROS, reported.get(0)[1]);
    }

    private static ConsumerRecord<String, byte[]> record(RecordHeaders headers) {
        return new ConsumerRecord<>("t-out", 0, 0, 1_700_000_000_000L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 1, null, new byte[]{0},
                headers, java.util.Optional.empty());
    }
}
