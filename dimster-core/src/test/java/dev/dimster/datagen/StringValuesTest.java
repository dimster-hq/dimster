package dev.dimster.datagen;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

/** Checks the shape, determinism and value space of each string kind. */
public class StringValuesTest {

    private static final long SEED = Hashing.columnSeed(42, "col");

    @Test
    public void everyKindIsDeterministicPerRankAndSeed() {
        for (ColumnSpec.Kind kind : ColumnSpec.Kind.values()) {
            StringValues a = StringValues.of(kind, SEED, 8, 16, 5, 15);
            StringValues b = StringValues.of(kind, SEED, 8, 16, 5, 15);
            StringValues other = StringValues.of(kind, Hashing.columnSeed(43, "col"), 8, 16, 5, 15);
            int differing = 0;
            for (long rank = 0; rank < 20; rank++) {
                assertArrayEquals(kind + " rank " + rank, a.valueFor(rank), b.valueFor(rank));
                if (!new String(a.valueFor(rank)).equals(new String(other.valueFor(rank)))) {
                    differing++;
                }
            }
            if (a instanceof StringValues.DictionaryValues) {
                assertEquals(kind + ": dictionaries ignore the seed", 0, differing);
            } else {
                assertTrue(kind + ": the seed changes values", differing > 10);
            }
        }
    }

    @Test
    public void valuesHaveTheExpectedShape() {
        assertShape(ColumnSpec.Kind.EMAIL, "[a-z]+[._]?[a-z]+[0-9]*@[a-z0-9.]+\\.[a-z]+");
        assertShape(ColumnSpec.Kind.IP, "(\\d{1,3}\\.){3}\\d{1,3}");
        assertShape(ColumnSpec.Kind.UUID, "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}");
        assertShape(ColumnSpec.Kind.FULL_NAME, "[A-Z][a-z]+ [A-Z][a-z'-]+");
        assertShape(ColumnSpec.Kind.STREET_ADDRESS, "\\d{1,4} [A-Z][A-Za-z ]+ [A-Z][a-z]+");
        assertShape(ColumnSpec.Kind.URL, "https://www\\.[a-z0-9.-]+(/[a-z]+){1,3}(\\?id=\\d+)?");
        assertShape(ColumnSpec.Kind.SENTENCE, "[A-Z][a-z]*( [a-z]+){4,14}\\.");
        assertShape(ColumnSpec.Kind.ALPHANUMERIC, "[A-Za-z0-9]{8,16}");

        for (long rank = 0; rank < 200; rank++) {
            String ip = string(ColumnSpec.Kind.IP, rank);
            for (String octet : ip.split("\\.")) {
                assertTrue(ip, Integer.parseInt(octet) <= 255);
            }
            java.util.UUID.fromString(string(ColumnSpec.Kind.UUID, rank));
        }
    }

    @Test
    public void dictionaryKindsIndexTheFrequencyOrderedDictionary() {
        assertEquals("Smith", string(ColumnSpec.Kind.LAST_NAME, 0));
        assertEquals("James", string(ColumnSpec.Kind.FIRST_NAME, 0));
        assertEquals("the", string(ColumnSpec.Kind.WORD, 0));
        assertEquals(5000, StringValues.of(ColumnSpec.Kind.LAST_NAME, SEED, 8, 16, 5, 15).valueSpace());
    }

    @Test
    public void dictionariesHaveNoDuplicates() {
        for (Dictionary d : Dictionary.values()) {
            Set<String> seen = new HashSet<>();
            for (byte[] entry : d.entries()) {
                assertTrue(d + " duplicate " + new String(entry), seen.add(new String(entry, StandardCharsets.UTF_8)));
            }
            assertTrue(d + " is empty", d.size() > 0);
        }
    }

    @Test
    public void valueSpacesReflectTheKind() {
        assertEquals(2000L * 5000, StringValues.valueSpace(ColumnSpec.Kind.FULL_NAME, 8, 16, 5, 15));
        assertEquals(62L * 62, StringValues.valueSpace(ColumnSpec.Kind.ALPHANUMERIC, 1, 2, 5, 15));
        assertEquals(Long.MAX_VALUE, StringValues.valueSpace(ColumnSpec.Kind.ALPHANUMERIC, 8, 16, 5, 15));
        // 3000 words, at least 5 per sentence.
        assertEquals(243_000_000_000_000_000L, StringValues.valueSpace(ColumnSpec.Kind.SENTENCE, 8, 16, 5, 15));
        assertEquals(Long.MAX_VALUE, StringValues.valueSpace(ColumnSpec.Kind.SENTENCE, 8, 16, 6, 15));
    }

    private static void assertShape(ColumnSpec.Kind kind, String regex) {
        Pattern p = Pattern.compile(regex);
        for (long rank = 0; rank < 500; rank++) {
            String v = string(kind, rank);
            assertTrue(kind + " value '" + v + "' doesn't match " + regex, p.matcher(v).matches());
        }
    }

    private static String string(ColumnSpec.Kind kind, long rank) {
        return new String(StringValues.of(kind, SEED, 8, 16, 5, 15).valueFor(rank), StandardCharsets.UTF_8);
    }
}
