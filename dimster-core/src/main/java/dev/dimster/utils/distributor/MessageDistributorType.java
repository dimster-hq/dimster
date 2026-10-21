/*
 * Derived from io.openmessaging.benchmark.utils.distributor.KeyDistributorType
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

public enum MessageDistributorType {
    /**
     * Key distributor that returns null keys to have default publish semantics
     */
    NO_KEY,

    /**
     * Genarate a finite number of "keys" and cycle through them in round-robin fashion
     */
    KEY_ROUND_ROBIN,

    /**
     * Random distribution based on System.nanoTime()
     */
    RANDOM_NANO,

    /**
     * Pick a key from the shared pool with a Zipf-distributed index. A few
     * keys are produced far more often than the rest, which produces hot
     * partitions through the broker's default partitioner. Skewness is
     * controlled by the workload-level {@code messageDistributorSkew} field.
     */
    KEY_ZIPF,

    /**
     * Bypass the partitioner entirely and target a specific partition number
     * drawn from a Zipf distribution. Maximally skewed and deterministic in
     * terms of which partition becomes hot (always partition 0 is the
     * hottest). Requires {@code partitionsPerTopic > 1} to do anything
     * interesting. Skewness is controlled by {@code messageDistributorSkew}.
     */
    PARTITION_ZIPF,

    /**
     * Bypass the partitioner entirely and cycle through partitions
     * {@code [0, partitionsPerTopic)} in round-robin order. Produces a
     * perfectly uniform load across all partitions. Useful when you want
     * even distribution without relying on the broker's key-hash
     * partitioner.
     */
    PARTITION_ROUND_ROBIN,

    /**
     * Producer-level pinning: each producer is statically pinned to a
     * subset of partitions and round-robins through that subset for every
     * send. The partition subset for each producer is computed once by
     * {@code TopologyPlanner} at producer-creation time (partitions are
     * split as evenly as possible across the producers of a topic), and
     * the per-message round-robin is performed by
     * {@code KafkaProducerWrapper} using its own pinned cursor — no shared
     * state across producers. The {@link MessageDistributor} for this type is
     * a no-op that returns a null key and null partition; the wrapper does
     * the actual work. The single-partition special case
     * (partitionsPerTopic / producersPerTopic = 1) is identical to the
     * partition pinning used by correctness mode.
     */
    PINNED_PARTITIONS,

    /**
     * Use the value of the dataset's {@code keyColumn} as the record key, so
     * that key (and partition) skew follows that column's distribution. Only
     * valid with a {@code dataset} that sets {@code keyColumn}. Like
     * {@link #PINNED_PARTITIONS} the distributor itself is a no-op: the
     * producer loop takes the key from the generated row.
     */
    KEY_COLUMN,
}
