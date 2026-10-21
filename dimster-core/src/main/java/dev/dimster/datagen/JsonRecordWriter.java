package dev.dimster.datagen;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Hand-written encoder for dataset records as compact JSON objects.
 *
 * <p>A record looks like
 * {@code {"_dimster":"00","user_id":42,"referrer":null,"country":"India"}}.
 * The header, like any BYTES column, is lowercase hex; it is always the first
 * member, so
 * {@link RecordFormat#JSON} can find the message type at a fixed offset
 * without parsing. Field names are pre-encoded once per writer. JSON is the
 * verbose contrast to Avro in format and compression experiments: field names
 * repeat in every record, which is exactly what compressors exploit.
 */
final class JsonRecordWriter implements RecordWriter {

    /** Bytes that precede the header's hex digits in every record. */
    static final byte[] HEADER_PREFIX =
            ("{\"" + ResolvedDataset.HEADER_FIELD + "\":\"").getBytes(StandardCharsets.US_ASCII);

    private static final byte[] HEX = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] NULL = "null".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] TRUE = "true".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] FALSE = "false".getBytes(StandardCharsets.US_ASCII);

    /** {@code ,"name":} for each column. */
    private final byte[][] fieldPrefixes;
    private byte[] buf = new byte[512];
    private int pos;
    private int column;

    JsonRecordWriter(ResolvedDataset dataset) {
        fieldPrefixes = new byte[dataset.columns().size()][];
        for (int i = 0; i < fieldPrefixes.length; i++) {
            // Column names are validated as [A-Za-z_][A-Za-z0-9_]*, so they need no escaping.
            fieldPrefixes[i] = (",\"" + dataset.columns().get(i).name() + "\":").getBytes(StandardCharsets.US_ASCII);
        }
    }

    @Override
    public void begin(byte[] header) {
        pos = 0;
        column = 0;
        writeRaw(HEADER_PREFIX);
        writeHex(header);
        ensure(1);
        buf[pos++] = '"';
    }

    @Override
    public void writeNull() {
        field();
        writeRaw(NULL);
    }

    @Override
    public void writeLong(long value) {
        field();
        writeAscii(Long.toString(value));
    }

    @Override
    public void writeInt(int value) {
        field();
        writeAscii(Integer.toString(value));
    }

    @Override
    public void writeDouble(double value) {
        field();
        // Double.toString yields valid JSON numbers for finite values
        // ("12.5", "1.0E10"); generators never produce NaN or infinities.
        writeAscii(Double.toString(value));
    }

    @Override
    public void writeBoolean(boolean value) {
        field();
        writeRaw(value ? TRUE : FALSE);
    }

    @Override
    public void writeString(byte[] utf8) {
        field();
        ensure(utf8.length + 2);
        buf[pos++] = '"';
        for (byte b : utf8) {
            // Only quote, backslash and control characters need escaping;
            // multi-byte UTF-8 sequences have the high bit set and pass through.
            if (b == '"' || b == '\\') {
                ensure(2);
                buf[pos++] = '\\';
                buf[pos++] = b;
            } else if (b >= 0 && b < 0x20) {
                ensure(6);
                buf[pos++] = '\\';
                buf[pos++] = 'u';
                buf[pos++] = '0';
                buf[pos++] = '0';
                buf[pos++] = HEX[(b >> 4) & 0xF];
                buf[pos++] = HEX[b & 0xF];
            } else {
                ensure(1);
                buf[pos++] = b;
            }
        }
        ensure(1);
        buf[pos++] = '"';
    }

    @Override
    public void writeBytes(byte[] bytes) {
        field();
        ensure(1);
        buf[pos++] = '"';
        writeHex(bytes);
        ensure(1);
        buf[pos++] = '"';
    }

    @Override
    public byte[] finish() {
        ensure(1);
        buf[pos++] = '}';
        return Arrays.copyOf(buf, pos);
    }

    private void field() {
        writeRaw(fieldPrefixes[column++]);
    }

    private void writeHex(byte[] bytes) {
        ensure(bytes.length * 2);
        for (byte b : bytes) {
            buf[pos++] = HEX[(b >> 4) & 0xF];
            buf[pos++] = HEX[b & 0xF];
        }
    }

    private void writeAscii(String s) {
        int n = s.length();
        ensure(n);
        for (int i = 0; i < n; i++) {
            buf[pos++] = (byte) s.charAt(i);
        }
    }

    private void writeRaw(byte[] bytes) {
        ensure(bytes.length);
        System.arraycopy(bytes, 0, buf, pos, bytes.length);
        pos += bytes.length;
    }

    private void ensure(int extra) {
        if (pos + extra > buf.length) {
            buf = Arrays.copyOf(buf, Math.max(buf.length * 2, pos + extra));
        }
    }
}
