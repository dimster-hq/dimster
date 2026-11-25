package dev.dimster.utils.distributor;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Tests for the {@link MessageDistributor} family. The Zipf-based distributors
 * (KEY_ZIPF, PARTITION_ZIPF) have statistical assertions — they use large
 * sample sizes so the bounds are loose enough to not flake.
 */
public class MessageDistributorTest {

    // ─── Existing distributors ────────────────────────────────────────

    @Test
    public void noKey_returnsNullKeyAndNullPartition() {
        MessageDistributor d = MessageDistributor.of(MessageDistributorType.NO_KEY, 1, 0);
        for (int i = 0; i < 10; i++) {
            MessageDistributor.Target t = d.next();
            assertNull(t.key());
            assertNull(t.partition());
        }
    }

    @Test
    public void keyRoundRobin_cyclesKeys() {
        MessageDistributor d = MessageDistributor.of(MessageDistributorType.KEY_ROUND_ROBIN, 1, 0);
        Set<String> seen = new HashSet<>();
        // After UNIQUE_COUNT calls we should have seen every key in the pool.
        for (int i = 0; i < MessageDistributor.UNIQUE_COUNT; i++) {
            MessageDistributor.Target t = d.next();
            assertNotNull(t.key());
            assertNull(t.partition());
            seen.add(t.key());
        }
        assertEquals(MessageDistributor.UNIQUE_COUNT, seen.size());
    }

    @Test
    public void randomNano_returnsKeysFromPool() {
        MessageDistributor d = MessageDistributor.of(MessageDistributorType.RANDOM_NANO, 1, 0);
        Set<String> pool = new HashSet<>(Arrays.asList(MessageDistributor.RANDOM_KEYS));
        for (int i = 0; i < 1000; i++) {
            MessageDistributor.Target t = d.next();
            assertNotNull(t.key());
            assertTrue("key should be from the pool", pool.contains(t.key()));
            assertNull(t.partition());
        }
    }

    // ─── KeyZipf ──────────────────────────────────────────────────────

    @Test
    public void keyZipf_topKeyDominates() {
        // s = 1.5 should give a strongly skewed distribution. With 10K keys
        // and 200K samples, the most-frequent key should be picked
        // significantly more often than under uniform (200K / 10K = 20).
        MessageDistributor d = new KeyZipf(1.5);
        int[] counts = new int[MessageDistributor.UNIQUE_COUNT];
        for (int i = 0; i < 200_000; i++) {
            String key = d.next().key();
            assertNotNull(key);
            int idx = indexOf(key);
            counts[idx]++;
        }
        int max = 0;
        for (int c : counts) if (c > max) max = c;
        // Loose assertion: the top key should be at least 100× the uniform rate.
        // Empirically with s=1.5 and N=10000, the top key gets ~10–15% of mass
        // (~20K hits) — well above 2K.
        assertTrue("top key should dominate but got max=" + max,
                max > 2_000);
    }

    @Test
    public void keyZipf_skewZeroIsApproximatelyUniform() {
        // s = 0 → uniform Zipf. Use a small N so we can check the spread cheaply.
        ZipfSampler uniform = new ZipfSampler(10, 0.0);
        int[] counts = new int[10];
        for (int i = 0; i < 100_000; i++) {
            counts[uniform.sample() - 1]++;
        }
        for (int c : counts) {
            // Each bin expected ~10K; allow ±25% slack.
            assertTrue("uniform sample skewed: " + Arrays.toString(counts),
                    c > 7_500 && c < 12_500);
        }
    }

    // ─── PartitionZipf ────────────────────────────────────────────────

    @Test
    public void partitionZipf_respectsPartitionCount() {
        int partitions = 16;
        MessageDistributor d = new PartitionZipf(partitions, 1.0);
        int[] counts = new int[partitions];
        for (int i = 0; i < 50_000; i++) {
            MessageDistributor.Target t = d.next();
            assertNull("partition mode should emit null key", t.key());
            assertNotNull(t.partition());
            int p = t.partition();
            assertTrue("partition out of range: " + p, p >= 0 && p < partitions);
            counts[p]++;
        }
        // Partition 0 should be the most-hit (or at worst tied for it).
        int max = 0, maxIdx = 0;
        for (int i = 0; i < partitions; i++) {
            if (counts[i] > max) { max = counts[i]; maxIdx = i; }
        }
        assertEquals("partition 0 should be the hottest", 0, maxIdx);
    }

    @Test
    public void partitionZipf_singlePartitionAlwaysReturnsZero() {
        MessageDistributor d = new PartitionZipf(1, 1.0);
        for (int i = 0; i < 100; i++) {
            assertEquals(Integer.valueOf(0), d.next().partition());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void partitionZipf_zeroPartitionsThrows() {
        new PartitionZipf(0, 1.0);
    }

    // ─── PartitionRoundRobin ─────────────────────────────────────────

    @Test
    public void partitionRoundRobin_cyclesAllPartitions() {
        int partitions = 8;
        MessageDistributor d = new PartitionRoundRobin(partitions);
        // After exactly partitions calls, every partition [0, n) should appear once.
        int[] counts = new int[partitions];
        for (int i = 0; i < partitions; i++) {
            MessageDistributor.Target t = d.next();
            assertNull("partition mode should emit null key", t.key());
            assertNotNull(t.partition());
            int p = t.partition();
            assertTrue("partition out of range: " + p, p >= 0 && p < partitions);
            counts[p]++;
        }
        for (int i = 0; i < partitions; i++) {
            assertEquals("partition " + i + " should appear exactly once",
                    1, counts[i]);
        }
    }

    @Test
    public void partitionRoundRobin_wrapsAround() {
        int partitions = 5;
        MessageDistributor d = new PartitionRoundRobin(partitions);
        int[] counts = new int[partitions];
        int cycles = 4;
        for (int i = 0; i < partitions * cycles; i++) {
            counts[d.next().partition()]++;
        }
        for (int i = 0; i < partitions; i++) {
            assertEquals("partition " + i + " should appear exactly " + cycles + " times",
                    cycles, counts[i]);
        }
    }

    @Test
    public void partitionRoundRobin_singlePartitionAlwaysReturnsZero() {
        MessageDistributor d = new PartitionRoundRobin(1);
        for (int i = 0; i < 100; i++) {
            assertEquals(Integer.valueOf(0), d.next().partition());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void partitionRoundRobin_zeroPartitionsThrows() {
        new PartitionRoundRobin(0);
    }

    // ─── ZipfSampler basics ──────────────────────────────────────────

    @Test(expected = IllegalArgumentException.class)
    public void zipfSampler_negativeExponentThrows() {
        new ZipfSampler(10, -0.5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void zipfSampler_zeroNThrows() {
        new ZipfSampler(0, 1.0);
    }

    @Test
    public void zipfSampler_threadSafe() throws Exception {
        // Sanity check: ZipfSampler is shared across threads. Concurrent draws
        // should never produce out-of-range values or exceptions, since the
        // sampler delegates randomness to ThreadLocalRandom.
        int n = 32;
        ZipfSampler sampler = new ZipfSampler(n, 1.2);
        int threads = 8;
        int perThread = 50_000;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        AtomicInteger errors = new AtomicInteger();
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        for (int t = 0; t < threads; t++) {
            exec.submit(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        int v = sampler.sample();
                        if (v < 1 || v > n) {
                            errors.incrementAndGet();
                        }
                    }
                } catch (Throwable th) {
                    firstError.compareAndSet(null, th);
                    errors.incrementAndGet();
                }
            });
        }
        exec.shutdown();
        assertTrue(exec.awaitTermination(30, TimeUnit.SECONDS));
        if (firstError.get() != null) {
            throw new AssertionError("sampler threw under concurrency", firstError.get());
        }
        assertEquals(0, errors.get());
    }

    @Test
    public void zipfSampler_aliasTableMatchesExactPmfAndLargeNFallbackWorks() {
        // Verify Vose alias table PMF against exact analytical P(k) = k^(-s) / H_{n,s}.
        int n = 265;
        double s = 1.2;
        double harmonic = 0.0;
        for (int k = 1; k <= n; k++) {
            harmonic += Math.pow(k, -s);
        }
        ZipfSampler sampler = new ZipfSampler(n, s);
        var rnd = new java.util.SplittableRandom(12345L);
        int draws = 200_000;
        int[] counts = new int[n + 1];
        for (int i = 0; i < draws; i++) {
            int k = sampler.sample(rnd);
            assertTrue(k >= 1 && k <= n);
            counts[k]++;
        }
        for (int k : new int[]{1, 2, 3, 10}) {
            double expected = Math.pow(k, -s) / harmonic;
            double actual = counts[k] / (double) draws;
            assertEquals("k=" + k, expected, actual, 0.005);
        }

        // Verify fallback rejection-inversion path (n > ALIAS_TABLE_MAX_N) stays deterministic and skewed.
        int largeN = ZipfSampler.ALIAS_TABLE_MAX_N + 5_000;
        ZipfSampler largeA = new ZipfSampler(largeN, 1.0);
        ZipfSampler largeB = new ZipfSampler(largeN, 1.0);
        var r1 = new java.util.SplittableRandom(77L);
        var r2 = new java.util.SplittableRandom(77L);
        int topHits = 0;
        for (int i = 0; i < 10_000; i++) {
            int v1 = largeA.sample(r1);
            int v2 = largeB.sample(r2);
            assertEquals(v1, v2);
            assertTrue(v1 >= 1 && v1 <= largeN);
            if (v1 == 1) topHits++;
        }
        assertTrue("rank 1 should dominate for largeN", topHits > 500);
    }

    @Test
    public void roundRobinDistributorsAreThreadSafeAndPreAllocated() throws Exception {
        MessageDistributor keyRr = MessageDistributor.of(MessageDistributorType.KEY_ROUND_ROBIN, 8, 0);
        MessageDistributor partRr = MessageDistributor.of(MessageDistributorType.PARTITION_ROUND_ROBIN, 8, 0);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        AtomicReference<Throwable> error = new AtomicReference<>();
        for (int t = 0; t < 4; t++) {
            exec.submit(() -> {
                try {
                    Set<Integer> parts = new HashSet<>();
                    for (int i = 0; i < 8; i++) {
                        parts.add(partRr.next().partition());
                    }
                    assertEquals(8, parts.size());
                    for (int i = 0; i < 20_000; i++) {
                        assertNotNull(keyRr.next().key());
                    }
                } catch (Throwable th) {
                    error.compareAndSet(null, th);
                }
            });
        }
        exec.shutdown();
        assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
        if (error.get() != null) {
            throw new AssertionError("Concurrent round-robin distributor failed", error.get());
        }
    }

    // ─── Helpers ──────────────────────────────────────────────────────

    private static int indexOf(String key) {
        // Linear scan is fine for tests — only called inside the hot loop here
        // because the key→index lookup mirrors what KeyZipf is doing internally
        // but in reverse.
        List<String> pool = Arrays.asList(MessageDistributor.RANDOM_KEYS);
        return pool.indexOf(key);
    }
}
