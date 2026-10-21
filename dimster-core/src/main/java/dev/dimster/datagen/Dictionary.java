package dev.dimster.datagen;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The embedded value dictionaries behind dictionary and composite string kinds.
 *
 * <p>Each dictionary is a resource under {@code datagen/dict/} with one value
 * per line, ordered from most to least frequent and free of duplicates (see
 * {@code datagen/dict/SOURCES.md}). Generators index into them by rank, so
 * with a Zipf distribution the first entries are the most common values.
 *
 * <p>Entries are loaded lazily, once per JVM, and kept as UTF-8 byte arrays so
 * that writers can copy them without encoding. The arrays are shared by all
 * producer threads and must never be modified.
 */
public enum Dictionary {
    FIRST_NAMES("first_names"),
    LAST_NAMES("last_names"),
    CITIES("cities"),
    COUNTRIES("countries"),
    DOMAINS("domains"),
    EMAIL_DOMAINS("email_domains"),
    USER_AGENTS("user_agents"),
    WORDS("words"),
    STREET_NAMES("street_names"),
    STREET_SUFFIXES("street_suffixes");

    private final String resource;
    private volatile byte[][] entries;

    Dictionary(String fileName) {
        this.resource = "datagen/dict/" + fileName + ".txt";
    }

    /** The dictionary's entries as UTF-8, most frequent first. */
    public byte[][] entries() {
        byte[][] e = entries;
        if (e == null) {
            synchronized (this) {
                e = entries;
                if (e == null) {
                    e = load();
                    entries = e;
                }
            }
        }
        return e;
    }

    public int size() {
        return entries().length;
    }

    private byte[][] load() {
        try (InputStream in = Dictionary.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("Missing dictionary resource " + resource);
            }
            List<byte[]> values = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty()) {
                    values.add(line.getBytes(StandardCharsets.UTF_8));
                }
            }
            return values.toArray(new byte[0][]);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load dictionary " + resource, e);
        }
    }
}
