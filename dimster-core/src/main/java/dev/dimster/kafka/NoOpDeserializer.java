package dev.dimster.kafka;

import java.nio.ByteBuffer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Zero-allocation key deserializer used by consumer wrappers when per-message
 * printing is disabled. Unlike Kafka's {@code VoidDeserializer} (which throws
 * on non-null bytes), this silently discards key bytes and returns {@code null},
 * skipping UTF-8 decoding and {@link String} allocation on keyed workloads.
 */
public final class NoOpDeserializer implements Deserializer<String> {

    @Override
    public String deserialize(String topic, byte[] data) {
        return null;
    }

    @Override
    public String deserialize(String topic, Headers headers, ByteBuffer data) {
        return null;
    }
}
