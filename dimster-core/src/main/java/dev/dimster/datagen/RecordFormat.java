package dev.dimster.datagen;

import java.util.Arrays;

/**
 * How a record value is laid out on the wire, and how Dimster finds its own
 * {@code _dimster} header in it again on the consumer side.
 *
 * <p>Every record Dimster produces conceptually has the schema
 * {@code {_dimster: bytes, ...}}. The header holds the {@code MessageType}
 * marker as its first byte, followed by type-specific fields (for example the
 * correctness producer id, sequence and CRC). The formats differ in where the
 * header sits:
 * <ul>
 *   <li>{@link #RAW}: the default payload dataset, schema
 *       {@code {_dimster, payload}}. The header is the leading bytes of the
 *       value and the payload bytes follow, with no framing, which is
 *       Dimster's original payload layout. Not selectable for datasets.</li>
 *   <li>{@link #AVRO}: Avro binary encoding. The header is the first field, a
 *       length-prefixed {@code bytes}; headers are at most 26 bytes, so the
 *       length is always one varint byte and the type is at offset 1.</li>
 *   <li>{@link #JSON}: a JSON object whose first member is
 *       {@code "_dimster":"<hex>"}, so the type is two hex characters at a
 *       fixed offset.</li>
 * </ul>
 * Consumers only ever need the type ({@link #typeOf}) or, for correctness
 * checking, the header ({@link #header}); they never decode dataset columns.
 */
public enum RecordFormat {

    RAW {
        @Override
        public int typeOf(byte[] value) {
            return value != null && value.length > 0 ? value[0] : -1;
        }

        @Override
        public byte[] header(byte[] value) {
            // The header decoders read fixed layouts from offset 0 and ignore
            // the trailing payload, so the value itself serves as the header.
            return value;
        }

        @Override
        public RecordWriter newWriter(ResolvedDataset dataset) {
            return new RawRecordWriter();
        }
    },

    AVRO {
        @Override
        public int typeOf(byte[] value) {
            // value[0] is the zig-zag varint length of the header bytes; a
            // header of at least one byte means value[0] >= 2.
            return value != null && value.length > 1 && value[0] >= 2 ? value[1] : -1;
        }

        @Override
        public byte[] header(byte[] value) {
            if (value == null || value.length < 2 || value[0] < 2 || (value[0] & 1) != 0) {
                return new byte[0];
            }
            int len = value[0] >> 1;
            return len + 1 <= value.length ? Arrays.copyOfRange(value, 1, 1 + len) : new byte[0];
        }

        @Override
        public RecordWriter newWriter(ResolvedDataset dataset) {
            return new AvroRecordWriter(dataset);
        }
    },

    JSON {
        @Override
        public int typeOf(byte[] value) {
            int i = JsonRecordWriter.HEADER_PREFIX.length;
            if (value == null || value.length < i + 2) {
                return -1;
            }
            int hi = hexValue(value[i]);
            int lo = hexValue(value[i + 1]);
            return hi < 0 || lo < 0 ? -1 : (byte) ((hi << 4) | lo);
        }

        @Override
        public byte[] header(byte[] value) {
            int start = JsonRecordWriter.HEADER_PREFIX.length;
            if (value == null || value.length < start) {
                return new byte[0];
            }
            int end = start;
            while (end < value.length && value[end] != '"') {
                end++;
            }
            int len = (end - start) / 2;
            byte[] header = new byte[len];
            for (int i = 0; i < len; i++) {
                int hi = hexValue(value[start + 2 * i]);
                int lo = hexValue(value[start + 2 * i + 1]);
                if (hi < 0 || lo < 0) {
                    return new byte[0];
                }
                header[i] = (byte) ((hi << 4) | lo);
            }
            return header;
        }

        @Override
        public RecordWriter newWriter(ResolvedDataset dataset) {
            return new JsonRecordWriter(dataset);
        }
    };

    /**
     * The {@code MessageType} marker of a record (the header's first byte), or
     * -1 when the value is empty or malformed. Cheap enough for every record.
     */
    public abstract int typeOf(byte[] value);

    /**
     * The record's header bytes, for decoding type-specific fields. May return
     * a longer array whose leading bytes are the header (RAW), or an empty one
     * when the value is malformed.
     */
    public abstract byte[] header(byte[] value);

    /** A writer that encodes rows of {@code dataset} in this format. */
    public abstract RecordWriter newWriter(ResolvedDataset dataset);

    private static int hexValue(byte b) {
        if (b >= '0' && b <= '9') return b - '0';
        if (b >= 'a' && b <= 'f') return b - 'a' + 10;
        return -1;
    }
}
