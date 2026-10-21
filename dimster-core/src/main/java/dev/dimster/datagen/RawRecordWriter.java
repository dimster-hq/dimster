package dev.dimster.datagen;

import java.util.Arrays;

/**
 * Encoder for {@link RecordFormat#RAW}, the layout of the default payload
 * dataset: the header bytes, then each BYTES column's bytes, with no lengths
 * or other framing. Consumers only ever read the header, which starts at
 * offset 0, so nothing else needs to be recoverable. Only BYTES columns can
 * be written; the resolver never builds a RAW dataset with other types.
 */
final class RawRecordWriter implements RecordWriter {

    private byte[] buf = new byte[256];
    private int pos;

    @Override
    public void begin(byte[] header) {
        pos = 0;
        writeBytes(header);
    }

    @Override
    public void writeBytes(byte[] bytes) {
        if (pos + bytes.length > buf.length) {
            buf = Arrays.copyOf(buf, Math.max(buf.length * 2, pos + bytes.length));
        }
        System.arraycopy(bytes, 0, buf, pos, bytes.length);
        pos += bytes.length;
    }

    @Override
    public byte[] finish() {
        return Arrays.copyOf(buf, pos);
    }

    @Override
    public void writeNull() {
        throw unsupported();
    }

    @Override
    public void writeLong(long value) {
        throw unsupported();
    }

    @Override
    public void writeInt(int value) {
        throw unsupported();
    }

    @Override
    public void writeDouble(double value) {
        throw unsupported();
    }

    @Override
    public void writeBoolean(boolean value) {
        throw unsupported();
    }

    @Override
    public void writeString(byte[] utf8) {
        throw unsupported();
    }

    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException("RAW records hold only non-null BYTES columns");
    }
}
