/*
 * Derived from io.openmessaging.benchmark.utils.distributor.KeyDistributor
 * in OpenMessaging Benchmark (https://github.com/openmessaging/benchmark)
 * and modified for Dimster.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.dimster.utils.distributor;

import java.util.Base64;
import java.util.Random;

/**
 * Decides how each producer record is routed to a partition. Strategies fall
 * into two families:
 *
 * <ul>
 *   <li><b>Key-based</b> ({@link NoKeyDistributor}, {@link KeyRoundRobin},
 *       {@link RandomNano}, {@link KeyZipf}) — return a string key and let
 *       Kafka's configured partitioner decide which partition it lands on.</li>
 *   <li><b>Partition-targeted</b> ({@link PartitionZipf},
 *       {@link PartitionRoundRobin}) — explicitly name the partition number
 *       and bypass the partitioner.</li>
 * </ul>
 *
 * Both kinds funnel through the same {@link Target} return type so the
 * producer loop in {@code LocalWorker} stays uniform.
 */
public sealed interface MessageDistributor
        permits NoKeyDistributor, KeyRoundRobin, RandomNano, KeyZipf, PartitionZipf, PartitionRoundRobin,
                PinnedPartitionsDistributor, KeyColumnDistributor {

    int UNIQUE_COUNT = 10_000;
    int KEY_BYTE_SIZE = 7;
    String[] RANDOM_KEYS = generateKeys();

    /**
     * The result of one {@link #next()} call. Either field may be null:
     * <ul>
     *   <li>{@code key=null, partition=null} — broker partitioner decides
     *       (the "no key" sticky case).</li>
     *   <li>{@code key=non-null, partition=null} — broker partitioner uses
     *       the key (the standard hashing path).</li>
     *   <li>{@code key=null, partition=non-null} — caller forces the
     *       partition explicitly (used by {@link PartitionZipf}).</li>
     * </ul>
     */
    record Target(String key, Integer partition) {
        public static final Target NULL_KEY = new Target(null, null);
        public static Target ofKey(String key) { return new Target(key, null); }
        public static Target ofPartition(int partition) { return new Target(null, partition); }
    }

    /**
     * Produce the next routing target. Implementations must be safe to call
     * from many producer threads concurrently — the typical pattern is a
     * single {@code MessageDistributor} instance shared by all producers in a
     * test point.
     */
    Target next();

    /**
     * Build a distributor for the given type. {@code partitionsPerTopic} is
     * consulted by the partition-targeted strategies ({@link PartitionZipf},
     * {@link PartitionRoundRobin}); {@code skew} is only used by the Zipf
     * strategies. Both arguments are accepted for every type so that
     * {@code LocalWorker} doesn't have to special-case the call.
     */
    static MessageDistributor of(MessageDistributorType type, int partitionsPerTopic, double skew) {
        return switch (type) {
            case NO_KEY          -> new NoKeyDistributor();
            case KEY_ROUND_ROBIN -> new KeyRoundRobin();
            case RANDOM_NANO     -> new RandomNano();
            case KEY_ZIPF        -> new KeyZipf(skew);
            case PARTITION_ZIPF        -> new PartitionZipf(partitionsPerTopic, skew);
            case PARTITION_ROUND_ROBIN -> new PartitionRoundRobin(partitionsPerTopic);
            // PINNED_PARTITIONS is a marker: the per-producer round-robin over an assigned
            // partition subset is performed by KafkaProducerWrapper using the pinned partitions
            // computed by TopologyPlanner. The distributor itself is a no-op.
            case PINNED_PARTITIONS     -> new PinnedPartitionsDistributor();
            // KEY_COLUMN is a marker too: LocalWorker takes the key from the generated row.
            case KEY_COLUMN            -> new KeyColumnDistributor();
        };
    }

    private static String[] generateKeys() {
        var keys = new String[UNIQUE_COUNT];
        var random = new Random();
        var encoder = Base64.getUrlEncoder().withoutPadding();
        byte[] buffer = new byte[KEY_BYTE_SIZE];
        for (int i = 0; i < keys.length; i++) {
            random.nextBytes(buffer);
            keys[i] = encoder.encodeToString(buffer);
        }
        return keys;
    }
}
