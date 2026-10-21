package dev.dimster.datagen;

/**
 * Encodes one record at a time in a specific {@link RecordFormat}.
 *
 * <p>The row generator streams a record into the writer field by field, with
 * no intermediate row object: {@link #begin} with the Dimster header, then one
 * write call per column in schema order, then {@link #finish}. The writer
 * tracks which column it is on, so it knows each column's name (JSON) and
 * whether it is nullable (Avro unions).
 *
 * <p>Implementations reuse an internal buffer and are not thread-safe; each
 * producer thread owns one. {@link #finish} returns a fresh array because the
 * Kafka producer keeps a reference to the value until the send completes.
 */
public interface RecordWriter {

    /** Starts a record whose {@code _dimster} field holds {@code header}. */
    void begin(byte[] header);

    /** Writes null for the current column, which must be nullable. */
    void writeNull();

    void writeLong(long value);

    void writeInt(int value);

    void writeDouble(double value);

    void writeBoolean(boolean value);

    /** Writes a string given as UTF-8 bytes. */
    void writeString(byte[] utf8);

    /** Writes opaque bytes. */
    void writeBytes(byte[] bytes);

    /** Ends the record and returns its encoded bytes. */
    byte[] finish();
}
