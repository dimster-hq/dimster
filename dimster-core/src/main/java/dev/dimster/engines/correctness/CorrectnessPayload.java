package dev.dimster.engines.correctness;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import dev.dimster.MessageType;

/**
 * Codec for correctness-checking message payloads.
 *
 * Format:
 *   [1 byte: type (CORRECTNESS)][4 bytes: producerId][8 bytes: sequence][8 bytes: timestampMs]
 *   [1 byte: flags][4 bytes: CRC32 if flag set][N bytes: padding]
 *
 * Flags byte: bit 0 = hasCRC.
 * CRC32 covers bytes 0..20 (type + producerId + sequence + timestamp).
 * Minimum payload size: 22 bytes (without CRC), 26 bytes (with CRC).
 */
public final class CorrectnessPayload {

    public static final int HEADER_SIZE = 22;          // type(1) + producerId(4) + sequence(8) + timestamp(8) + flags(1)
    public static final int HEADER_WITH_CRC_SIZE = 26; // header(22) + crc(4)

    private static final byte FLAG_HAS_CRC = 0x01;
    private static final int CRC_DATA_LENGTH = 21;     // bytes covered by CRC (type + producerId + sequence + timestamp)

    private CorrectnessPayload() {}

    public record DecodedMessage(int producerId, long sequence, long timestampMs, boolean crcValid) {}

    /**
     * Encode a correctness payload by stamping the header into an existing byte array.
     * Bytes beyond the header (or header+CRC) are left unchanged, preserving the
     * original payload content (e.g. randomized or file-based data).
     *
     * @param payload     existing payload to write the header into (must be at least
     *                    HEADER_SIZE or HEADER_WITH_CRC_SIZE bytes)
     * @param producerId  unique producer identifier
     * @param sequence    monotonically increasing sequence number per producer
     * @param timestampMs send timestamp in milliseconds
     * @param includeCrc  whether to include a CRC32 checksum
     */
    public static void encode(byte[] payload, int producerId, long sequence, long timestampMs,
                              boolean includeCrc) {
        int minSize = includeCrc ? HEADER_WITH_CRC_SIZE : HEADER_SIZE;
        if (payload.length < minSize) {
            throw new IllegalArgumentException(
                    "Payload size " + payload.length + " is less than minimum " + minSize);
        }

        ByteBuffer buf = ByteBuffer.wrap(payload);
        buf.put(MessageType.CORRECTNESS);
        buf.putInt(producerId);
        buf.putLong(sequence);
        buf.putLong(timestampMs);

        byte flags = 0;
        if (includeCrc) {
            flags |= FLAG_HAS_CRC;
        }
        buf.put(flags);

        if (includeCrc) {
            CRC32 crc = new CRC32();
            crc.update(payload, 0, CRC_DATA_LENGTH);
            buf.putInt((int) crc.getValue());
        }
    }

    /**
     * Encode a standalone correctness header (22 bytes, or 26 with a CRC): the
     * {@code _dimster} field of each correctness record, which precedes the
     * dataset's columns.
     */
    public static byte[] header(int producerId, long sequence, long timestampMs, boolean includeCrc) {
        byte[] header = new byte[includeCrc ? HEADER_WITH_CRC_SIZE : HEADER_SIZE];
        encode(header, producerId, sequence, timestampMs, includeCrc);
        return header;
    }

    /**
     * Decode a correctness payload.
     *
     * The first byte must be {@link MessageType#CORRECTNESS}; if not, returns
     * producerId=-1 and crcValid=false (not a correctness message).
     *
     * Reads the flags byte to determine whether CRC is present.
     * If CRC is present and valid, crcValid=true. If CRC is not present, crcValid=true.
     * If the payload is too small to decode, returns producerId=-1 and crcValid=false.
     */
    public static DecodedMessage decode(byte[] payload) {
        if (payload.length < HEADER_SIZE) {
            return new DecodedMessage(-1, -1, -1, false);
        }

        ByteBuffer buf = ByteBuffer.wrap(payload);
        byte type = buf.get();
        if (type != MessageType.CORRECTNESS) {
            return new DecodedMessage(-1, -1, -1, false);
        }

        int producerId = buf.getInt();
        long sequence = buf.getLong();
        long timestampMs = buf.getLong();
        byte flags = buf.get();

        boolean crcValid = true;
        if ((flags & FLAG_HAS_CRC) != 0) {
            if (payload.length >= HEADER_WITH_CRC_SIZE) {
                int storedCrc = buf.getInt();
                CRC32 crc = new CRC32();
                crc.update(payload, 0, CRC_DATA_LENGTH);
                crcValid = storedCrc == (int) crc.getValue();
            } else {
                // Flag says CRC present but payload too short
                crcValid = false;
            }
        }

        return new DecodedMessage(producerId, sequence, timestampMs, crcValid);
    }

    /**
     * Check if a payload is a correctness message without fully decoding it.
     */
    public static boolean isCorrectnessMessage(byte[] payload) {
        return payload.length >= HEADER_SIZE && payload[0] == MessageType.CORRECTNESS;
    }
}
