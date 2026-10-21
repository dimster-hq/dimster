package dev.dimster.datagen;

import java.util.Arrays;
import java.util.List;

/**
 * Decodes an Avro-encoded dataset record (see {@link AvroRecordWriter}) and
 * streams its fields into a {@link RecordWriter}. {@link RecordPrinter} uses
 * it with a JSON writer to print Avro records readably; it is not on any hot
 * path.
 */
final class AvroRecordReader {

    private final byte[] buf;
    private int pos;

    private AvroRecordReader(byte[] buf) {
        this.buf = buf;
    }

    /**
     * Decodes {@code value} as a record of {@code dataset} into {@code out}.
     *
     * @throws IllegalArgumentException if the bytes are not such a record
     */
    static void decode(byte[] value, ResolvedDataset dataset, RecordWriter out) {
        AvroRecordReader in = new AvroRecordReader(value);
        try {
            out.begin(in.readBytes());
            List<ResolvedDataset.Column> columns = dataset.columns();
            for (ResolvedDataset.Column c : columns) {
                if (c.nullable() && in.readLong() == 0) {
                    out.writeNull();
                    continue;
                }
                switch (c.type()) {
                    case LONG, SEQUENCE, TIMESTAMP -> out.writeLong(in.readLong());
                    case INT -> out.writeInt((int) in.readLong());
                    case DOUBLE -> out.writeDouble(in.readDouble());
                    case BOOLEAN -> out.writeBoolean(in.readByte() != 0);
                    case STRING -> out.writeString(in.readBytes());
                    case BYTES -> out.writeBytes(in.readBytes());
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("not a record of this dataset", e);
        }
        if (in.pos != value.length) {
            throw new IllegalArgumentException("not a record of this dataset: "
                    + (value.length - in.pos) + " trailing bytes");
        }
    }

    /** Zig-zag base-128 varint, the inverse of AvroRecordWriter.writeVarLong. */
    private long readLong() {
        long n = 0;
        int shift = 0;
        byte b;
        do {
            b = buf[pos++];
            n |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return (n >>> 1) ^ -(n & 1);
    }

    private double readDouble() {
        long bits = 0;
        for (int i = 0; i < 8; i++) {
            bits |= (long) (buf[pos++] & 0xFF) << (8 * i);
        }
        return Double.longBitsToDouble(bits);
    }

    private byte readByte() {
        return buf[pos++];
    }

    private byte[] readBytes() {
        long len = readLong();
        if (len < 0 || pos + len > buf.length) {
            throw new ArrayIndexOutOfBoundsException("bytes field of length " + len + " at " + pos);
        }
        byte[] out = Arrays.copyOfRange(buf, pos, pos + (int) len);
        pos += (int) len;
        return out;
    }
}
