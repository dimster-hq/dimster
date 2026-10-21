package dev.dimster.datagen;

import dev.dimster.utils.distributor.ZipfSampler;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.SplittableRandom;

/**
 * Maps a rank to a STRING column's value, one implementation per value
 * {@link ColumnSpec.Kind} family.
 *
 * <p>This is where cardinality is enforced. A column samples a rank in
 * [0, cardinality) and asks for {@link #valueFor(long)}, which is a pure
 * function of the column's seed and the rank. The same rank therefore yields
 * the same value on every producer, and a column can never produce more than
 * {@code cardinality} distinct values. Two ranks may map to the same value
 * for composite and formatted kinds, which is negligible when
 * {@link #valueSpace()} is much larger than the cardinality;
 * {@link DatasetResolver} rejects a cardinality above the value space.
 *
 * <ul>
 *   <li>Dictionary kinds return entry {@code rank} directly. Dictionaries are
 *       frequency-ordered, so a Zipf rank distribution gives realistic value
 *       frequencies ("Smith" is the most common last name).</li>
 *   <li>Composite and formatted kinds seed a generator with the rank and
 *       build the value from it, picking dictionary entries with a Zipf skew
 *       so that their parts also have realistic frequencies.</li>
 * </ul>
 * Implementations are immutable and shared by all producer threads.
 */
abstract class StringValues {

    /** Returns the value for {@code rank}; the array must not be modified. */
    abstract byte[] valueFor(long rank);

    /** Number of distinct values this kind can produce (saturating at Long.MAX_VALUE). */
    abstract long valueSpace();

    /**
     * Builds the value mapping for a STRING column with a kind.
     *
     * @param columnSeed see {@link Hashing#columnSeed}
     */
    static StringValues of(ColumnSpec.Kind kind, long columnSeed,
                           int minLength, int maxLength, int minWords, int maxWords) {
        return switch (kind) {
            case FIRST_NAME -> new DictionaryValues(Dictionary.FIRST_NAMES);
            case LAST_NAME -> new DictionaryValues(Dictionary.LAST_NAMES);
            case CITY -> new DictionaryValues(Dictionary.CITIES);
            case COUNTRY -> new DictionaryValues(Dictionary.COUNTRIES);
            case DOMAIN -> new DictionaryValues(Dictionary.DOMAINS);
            case USER_AGENT -> new DictionaryValues(Dictionary.USER_AGENTS);
            case WORD -> new DictionaryValues(Dictionary.WORDS);
            case FULL_NAME -> new FullName(columnSeed);
            case STREET_ADDRESS -> new StreetAddress(columnSeed);
            case EMAIL -> new Email(columnSeed);
            case URL -> new Url(columnSeed);
            case SENTENCE -> new Sentence(columnSeed, minWords, maxWords);
            case ALPHANUMERIC -> new Alphanumeric(columnSeed, minLength, maxLength);
            case IP -> new Ip(columnSeed);
            case UUID -> new Uuid(columnSeed);
        };
    }

    /** The value space of a kind without building it (used for validation). */
    static long valueSpace(ColumnSpec.Kind kind, int minLength, int maxLength, int minWords, int maxWords) {
        return of(kind, 0, minLength, maxLength, minWords, maxWords).valueSpace();
    }

    // ─── Dictionary kinds ────────────────────────────────────────────────

    /** Entry {@code rank} of a frequency-ordered dictionary. */
    static final class DictionaryValues extends StringValues {
        private final byte[][] entries;

        DictionaryValues(Dictionary dictionary) {
            this.entries = dictionary.entries();
        }

        @Override
        byte[] valueFor(long rank) {
            return entries[(int) rank];
        }

        @Override
        long valueSpace() {
            return entries.length;
        }
    }

    // ─── Composite kinds ─────────────────────────────────────────────────

    /**
     * A dictionary plus a Zipf sampler over it, for picking parts of a
     * composite value with realistic frequencies.
     */
    private record Part(byte[][] entries, ZipfSampler sampler) {
        Part(Dictionary dictionary, double skew) {
            this(dictionary.entries(), new ZipfSampler(dictionary.size(), skew));
        }

        byte[] pick(SplittableRandom rnd) {
            return entries[sampler.sample(rnd) - 1];
        }

        long size() {
            return entries.length;
        }
    }

    /** Base for kinds that build a value from a per-rank random generator. */
    private abstract static class Seeded extends StringValues {
        private final long columnSeed;

        Seeded(long columnSeed) {
            this.columnSeed = columnSeed;
        }

        @Override
        final byte[] valueFor(long rank) {
            Builder out = new Builder();
            build(new SplittableRandom(Hashing.rankSeed(columnSeed, rank)), out);
            return out.toArray();
        }

        abstract void build(SplittableRandom rnd, Builder out);
    }

    /** "Mary Smith". */
    static final class FullName extends Seeded {
        private final Part first = new Part(Dictionary.FIRST_NAMES, 0.8);
        private final Part last = new Part(Dictionary.LAST_NAMES, 0.8);

        FullName(long columnSeed) {
            super(columnSeed);
        }

        @Override
        void build(SplittableRandom rnd, Builder out) {
            out.append(first.pick(rnd)).append(' ').append(last.pick(rnd));
        }

        @Override
        long valueSpace() {
            return saturatingProduct(first.size(), last.size());
        }
    }

    /** "1234 Oak Avenue". */
    static final class StreetAddress extends Seeded {
        private static final int MAX_NUMBER = 9999;
        private final ZipfSampler number = new ZipfSampler(MAX_NUMBER, 0.5);
        private final Part street = new Part(Dictionary.STREET_NAMES, 0.8);
        private final Part suffix = new Part(Dictionary.STREET_SUFFIXES, 1.2);

        StreetAddress(long columnSeed) {
            super(columnSeed);
        }

        @Override
        void build(SplittableRandom rnd, Builder out) {
            out.append(number.sample(rnd)).append(' ')
                    .append(street.pick(rnd)).append(' ')
                    .append(suffix.pick(rnd));
        }

        @Override
        long valueSpace() {
            return saturatingProduct(MAX_NUMBER, street.size(), suffix.size());
        }
    }

    /** "mary.smith@gmail.com", "msmith@...", "mary_smith1987@...", etc. */
    static final class Email extends Seeded {
        private static final int PATTERNS = 5;
        private final Part first = new Part(Dictionary.FIRST_NAMES, 0.8);
        private final Part last = new Part(Dictionary.LAST_NAMES, 0.8);
        private final Part domain = new Part(Dictionary.EMAIL_DOMAINS, 1.2);

        Email(long columnSeed) {
            super(columnSeed);
        }

        @Override
        void build(SplittableRandom rnd, Builder out) {
            byte[] f = first.pick(rnd);
            byte[] l = last.pick(rnd);
            int pattern = rnd.nextInt(10);
            if (pattern < 4) {
                out.appendLower(f).append('.').appendLower(l);
            } else if (pattern < 6) {
                out.appendLower(f).appendLower(l);
            } else if (pattern < 8) {
                out.appendLower(Arrays.copyOf(f, 1)).appendLower(l);
            } else if (pattern < 9) {
                out.appendLower(f).append('.').appendLower(l).append(rnd.nextInt(10, 100));
            } else {
                out.appendLower(f).append('_').appendLower(l).append(rnd.nextInt(1950, 2011));
            }
            out.append('@').append(domain.pick(rnd));
        }

        @Override
        long valueSpace() {
            return saturatingProduct(first.size(), last.size(), domain.size(), PATTERNS);
        }
    }

    /** "https://www.amazon.com/time/house?id=4821". */
    static final class Url extends Seeded {
        private final Part domain = new Part(Dictionary.DOMAINS, 1.0);
        private final Part word = new Part(Dictionary.WORDS, 1.0);

        Url(long columnSeed) {
            super(columnSeed);
        }

        @Override
        void build(SplittableRandom rnd, Builder out) {
            out.append("https://www.").append(domain.pick(rnd));
            int segments = rnd.nextInt(1, 4);
            for (int i = 0; i < segments; i++) {
                out.append('/').append(word.pick(rnd));
            }
            if (rnd.nextInt(10) < 3) {
                out.append("?id=").append(rnd.nextInt(1, 100_000));
            }
        }

        @Override
        long valueSpace() {
            // Counting one path segment only; the real space is far larger.
            return saturatingProduct(domain.size(), word.size());
        }
    }

    /** "The house was not far from the old road." */
    static final class Sentence extends Seeded {
        private final Part word = new Part(Dictionary.WORDS, 1.0);
        private final int minWords;
        private final int maxWords;

        Sentence(long columnSeed, int minWords, int maxWords) {
            super(columnSeed);
            this.minWords = minWords;
            this.maxWords = maxWords;
        }

        @Override
        void build(SplittableRandom rnd, Builder out) {
            int n = rnd.nextInt(minWords, maxWords + 1);
            for (int i = 0; i < n; i++) {
                byte[] w = word.pick(rnd);
                if (i == 0) {
                    out.appendCapitalised(w);
                } else {
                    out.append(' ').append(w);
                }
            }
            out.append('.');
        }

        @Override
        long valueSpace() {
            return saturatingPower(word.size(), minWords);
        }
    }

    // ─── Formatted kinds ─────────────────────────────────────────────────

    /** Random [A-Za-z0-9] of a length in [minLength, maxLength]. */
    static final class Alphanumeric extends Seeded {
        private static final byte[] ALPHABET =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".getBytes(StandardCharsets.US_ASCII);
        private final int minLength;
        private final int maxLength;

        Alphanumeric(long columnSeed, int minLength, int maxLength) {
            super(columnSeed);
            this.minLength = minLength;
            this.maxLength = maxLength;
        }

        @Override
        void build(SplittableRandom rnd, Builder out) {
            int n = rnd.nextInt(minLength, maxLength + 1);
            for (int i = 0; i < n; i++) {
                out.append(ALPHABET[rnd.nextInt(ALPHABET.length)]);
            }
        }

        @Override
        long valueSpace() {
            return saturatingPower(ALPHABET.length, maxLength);
        }
    }

    /** A public-looking IPv4 address, first octet in [1, 223]. */
    static final class Ip extends Seeded {
        Ip(long columnSeed) {
            super(columnSeed);
        }

        @Override
        void build(SplittableRandom rnd, Builder out) {
            out.append(rnd.nextInt(1, 224)).append('.')
                    .append(rnd.nextInt(256)).append('.')
                    .append(rnd.nextInt(256)).append('.')
                    .append(rnd.nextInt(256));
        }

        @Override
        long valueSpace() {
            return 223L * 256 * 256 * 256;
        }
    }

    /** A version 4 UUID in canonical form. */
    static final class Uuid extends Seeded {
        Uuid(long columnSeed) {
            super(columnSeed);
        }

        @Override
        void build(SplittableRandom rnd, Builder out) {
            long msb = (rnd.nextLong() & ~0xF000L) | 0x4000L;                       // version 4
            long lsb = (rnd.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L; // IETF variant
            out.append(new java.util.UUID(msb, lsb).toString());
        }

        @Override
        long valueSpace() {
            return Long.MAX_VALUE;
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────

    static long saturatingProduct(long... factors) {
        long p = 1;
        for (long f : factors) {
            if (f != 0 && p > Long.MAX_VALUE / f) {
                return Long.MAX_VALUE;
            }
            p *= f;
        }
        return p;
    }

    static long saturatingPower(long base, int exponent) {
        long p = 1;
        for (int i = 0; i < exponent; i++) {
            p = saturatingProduct(p, base);
        }
        return p;
    }

    /** Minimal growable byte array for assembling ASCII/UTF-8 values. */
    static final class Builder {
        private byte[] buf = new byte[64];
        private int len;

        Builder append(byte[] bytes) {
            ensure(bytes.length);
            System.arraycopy(bytes, 0, buf, len, bytes.length);
            len += bytes.length;
            return this;
        }

        Builder append(String ascii) {
            ensure(ascii.length());
            for (int i = 0; i < ascii.length(); i++) {
                buf[len++] = (byte) ascii.charAt(i);
            }
            return this;
        }

        Builder append(char c) {
            return append((byte) c);
        }

        Builder append(byte b) {
            ensure(1);
            buf[len++] = b;
            return this;
        }

        Builder append(int n) {
            return append(Integer.toString(n));
        }

        /** Appends with ASCII letters lowercased (dictionaries are ASCII). */
        Builder appendLower(byte[] bytes) {
            ensure(bytes.length);
            for (byte b : bytes) {
                buf[len++] = (b >= 'A' && b <= 'Z') ? (byte) (b + 32) : b;
            }
            return this;
        }

        /** Appends with the first ASCII letter uppercased. */
        Builder appendCapitalised(byte[] bytes) {
            int start = len;
            append(bytes);
            if (len > start && buf[start] >= 'a' && buf[start] <= 'z') {
                buf[start] -= 32;
            }
            return this;
        }

        byte[] toArray() {
            return Arrays.copyOf(buf, len);
        }

        private void ensure(int extra) {
            if (len + extra > buf.length) {
                buf = Arrays.copyOf(buf, Math.max(buf.length * 2, len + extra));
            }
        }
    }
}
