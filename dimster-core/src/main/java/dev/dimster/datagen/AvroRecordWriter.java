package dev.dimster.datagen;

import java.util.Arrays;

/**
 * Hand-written Avro binary encoder for flat dataset records.
 *
 * <p>Records follow {@link ResolvedDataset#avroSchema()}: the {@code _dimster}
 * header as {@code bytes}, then each column, with nullable columns as
 * {@code ["null", T]} unions. Avro binary encoding is simple for flat records,
 * so writing it directly avoids a runtime dependency and the per-record
 * allocations of the Avro library's generic API. The encoding rules (Avro
 * 1.11 specification, "Binary Encoding"):
 * <ul>
 *   <li>int and long: zig-zag, then variable-length base-128, low bits first;</li>
 *   <li>double: 8 bytes, little-endian IEEE 754;</li>
 *   <li>boolean: one byte, 0 or 1;</li>
 *   <li>bytes and string: a long length, then the bytes (UTF-8 for strings);</li>
 *   <li>union: the branch index as a long, then the value (nothing for null).</li>
 * </ul>
 */
final class AvroRecordWriter implements RecordWriter {

    private final boolean[] nullable;
    private byte[] buf = new byte[256];
    private int pos;
    private int column;

    AvroRecordWriter(ResolvedDataset dataset) {
        nullable = new boolean[dataset.columns().size()];
        for (int i = 0; i < nullable.length; i++) {
            nullable[i] = dataset.columns().get(i).nullable();
        }
    }

    @Override
    public void begin(byte[] header) {
        pos = 0;
        column = 0;
        writeVarLong(header.length);
        writeRaw(header);
    }

    @Override
    public void writeNull() {
        if (!nullable[column]) {
            throw new IllegalStateException("column " + column + " is not nullable");
        }
        // Union branch 0 is "null", which has no value bytes.
        writeVarLong(0);
        column++;
    }

    @Override
    public void writeLong(long value) {
        unionBranch();
        writeVarLong(value);
    }

    @Override
    public void writeInt(int value) {
        unionBranch();
        writeVarLong(value);
    }

    @Override
    public void writeDouble(double value) {
        unionBranch();
        ensure(8);
        long bits = Double.doubleToRawLongBits(value);
        for (int i = 0; i < 8; i++) {
            buf[pos++] = (byte) (bits >>> (8 * i));
        }
    }

    @Override
    public void writeBoolean(boolean value) {
        unionBranch();
        ensure(1);
        buf[pos++] = (byte) (value ? 1 : 0);
    }

    @Override
    public void writeString(byte[] utf8) {
        unionBranch();
        writeVarLong(utf8.length);
        writeRaw(utf8);
    }

    @Override
    public void writeBytes(byte[] bytes) {
        // Avro bytes and strings share an encoding: a length, then the bytes.
        writeString(bytes);
    }

    @Override
    public byte[] finish() {
        return Arrays.copyOf(buf, pos);
    }

    /** Writes the non-null union branch index for nullable columns and advances the column. */
    private void unionBranch() {
        if (nullable[column++]) {
            writeVarLong(1);
        }
    }

    /** Zig-zag, then base-128 varint (ints use the same encoding). */
    private void writeVarLong(long value) {
        ensure(10);
        long n = (value << 1) ^ (value >> 63);
        while ((n & ~0x7FL) != 0) {
            buf[pos++] = (byte) ((n & 0x7F) | 0x80);
            n >>>= 7;
        }
        buf[pos++] = (byte) n;
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
