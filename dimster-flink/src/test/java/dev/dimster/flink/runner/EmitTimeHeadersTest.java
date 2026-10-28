package dev.dimster.flink.runner;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EmitTimeHeadersTest {

    @Test
    public void encodesEpochMicrosAsEightBigEndianBytes() {
        Instant t = Instant.ofEpochSecond(1_700_000_000L, 123_456_789);
        byte[] bytes = EmitTimeHeaders.encode(t);
        assertEquals(8, bytes.length);
        assertEquals(1_700_000_000_123_456L, ByteBuffer.wrap(bytes).getLong());
    }

    @Test
    public void stampsTheCurrentTimeUnderTheHeaderName() {
        long before = System.currentTimeMillis() * 1000;
        byte[] stamp = new EmitTimeHeaders().eval().get(EmitTimeHeaders.HEADER);
        long after = (System.currentTimeMillis() + 1) * 1000;
        long micros = ByteBuffer.wrap(stamp).getLong();
        assertTrue(micros >= before && micros <= after);
    }

    @Test
    public void isEvaluatedPerRow() {
        assertFalse(new EmitTimeHeaders().isDeterministic());
    }
}
