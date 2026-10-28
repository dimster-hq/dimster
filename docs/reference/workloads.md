# Workload Reference

Workload YAML files define what a benchmark test does: the topic topology, producer/consumer configuration, message properties, and mode-specific behavior. Each workload targets a specific mode determined by which config section is present.

Workload files are typically stored in `run/workloads/`.

---

## Workload Modes

| Mode | CLI Command | Config Section | Description |
|------|-------------|----------------|-------------|
| Run | `dimster run` | `runConfig` | Standard timed benchmark with optional scenarios. |
| Explore | `dimster explore` | `exploreConfig` | Automated throughput limit discovery. |
| Correctness | `dimster correctness` | `correctnessConfig` | Message ordering, duplicate, and CRC verification. |
| Drain Backlog | `dimster drain-backlog` | `drainBacklogConfig` | Measures time to drain a consumer backlog. |

---

## Workload File Structure

All workload types share a common structure:

```yaml
name: <string>                    # Workload name

engineConfig:                     # Optional, operational settings shared
  ...                             # across all engine modes

<modeConfig>:                     # One of: runConfig, exploreConfig,
                                  # correctnessConfig, drainBacklogConfig
  ...

availabilityConfig:               # Optional, only with runConfig
  ...

baseWorkload:                     # Default field values for all test points
  ...

scenarios:                        # Optional list of test variations
  - name: <string>
    ...
```

---

## engineConfig (optional)

Operational settings shared across all engine modes. Lives at the top level of the workload file (next to `runConfig`/`exploreConfig`/`correctnessConfig`/`drainBacklogConfig`). When omitted, defaults apply.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `maxConsumerRecreations` | integer | `0` | Maximum number of times a consumer can be recreated after a fatal error before the test gives up on it. `0` disables recreation. |
| `perPartitionMetrics` | boolean | `false` | Emit per-partition metrics (in addition to per-topic aggregates). Increases metric cardinality significantly on high-partition topologies. |
| `perConsumerMetrics` | boolean | `false` | Emit per-consumer-instance metrics. Off by default to keep metric cardinality manageable. |
| `cooldownMinutes` | integer | `0` | Minutes to wait between test points before tearing down topics and consumer groups. Gives Kafka time to stabilise and metric scrapers time to collect final samples. |
| `printMessages` | boolean | `false` | Log every successfully produced (`PRODUCED`) and consumed (`PROCESSED`) record at INFO level: topic, partition, offset, timestamp, and for consumers the end-to-end latency and message type. Intended for low-rate debugging only -- the coordinator forces this off for any test point with producer rate > 50 msg/s, and always off for the explore engine (where rate is dynamic). |
| `printContents` | boolean | `false` | Also print each record's key and contents on those lines (`key=... value=...`). Generated [dataset](#generated-datasets) records are shown as JSON (Avro records are decoded with the dataset's schema); payload-pool records, which are mostly random bytes, as their message type, size and first 16 bytes. Turns printing on by itself, under the same rate limit as `printMessages`. |
| `hooks` | object | none | Scripts to run at points in the workload's lifecycle: `beforeWorkload`, `afterWorkload`, `beforeTestPoint`, `afterTestPoint`, and `beforeDrain` (drain-backlog only). Each is a list of `{script, args, timeoutSeconds, onFailure}`. See [Workload Hooks](../concepts/hooks.md). |

### engineConfig Example

```yaml
engineConfig:
  perPartitionMetrics: true
  cooldownMinutes: 1
```

---

## baseWorkload Fields

These fields define the default test parameters. Each field can be overridden per scenario.

| Field | Type | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|-------|------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `kafkaConfig` | object | -- | Embedded [Kafka configuration](kafka-configs.md). Merged with the `-k` file if both are present.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `label` | string | -- | Label prefix used in chart output and result filenames.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `topics` | integer | `1` | Number of test topics to create.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `partitionsPerTopic` | integer | `1` | Number of partitions per topic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `producersPerTopic` | integer | `1` | Number of producer instances per topic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `consumerGroupsPerTopic` | integer | `1` | Number of consumer groups per topic (controls fan-out).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `consumersPerGroup` | integer | `1` | Number of consumers per consumer group.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `messageSize` | integer | `1000` | Message payload size in bytes. Does not apply when `dataset` is set: the test point's message size is then the dataset's estimated average record size. |
| `producerRate` | integer | `10000` | Target aggregate producer rate in messages per second for fixed-rate workloads. When `producerRateSchedule` is set, Dimster automatically resolves this to the schedule's max rate in test-point metadata/results.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `producerRateSchedule` | object | _(unset)_ | Optional variable producer-rate schedule. When set, producers follow the schedule instead of the fixed `producerRate`. See [Producer Rate Schedules](#producer-rate-schedules).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `useRandomizedPayloads` | boolean | `true` | Without a `dataset`, records are the header followed by `messageSize - 1` payload bytes (the default [payload dataset](#generated-datasets)). When true, the payloads are generated; when false, they come from `payloadFile`. |
| `randomBytesRatio` | double | `0.5` | Fraction of each generated payload that is random (0.0 to 1.0); the rest is zeros, which controls compressibility. |
| `randomizedPayloadPoolSize` | integer | `100` | Number of distinct generated payloads, sampled uniformly. Every producer draws from the same payloads. |
| `payloadFile` | string | _(unset)_ | Path to a payload file of exactly `messageSize` bytes, sent as every payload (its first byte is replaced by the header). Required when `useRandomizedPayloads` is `false`; otherwise ignored. |
| `dataset` | object | _(unset)_ | Optional generated dataset. When set, producers send schema-encoded rows (Avro or JSON) generated from a preset or inline column definitions instead of the payload dataset. See [Generated Datasets](#generated-datasets). |
| `flink` | object | _(unset)_ | Optional Flink SQL stage between producers and consumers (run and drain-backlog modes). Requires a `dataset`. See [Flink SQL Stage](#flink-sql-stage). |
| `messageDistributor` | enum | `NO_KEY` | Message distribution strategy. See [Message Distributor Types](#message-distributor-types).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `messageDistributorSkew` | double | `1.0` | Zipf exponent used by `KEY_ZIPF` and `PARTITION_ZIPF`. Higher values produce a steeper, more skewed distribution. Ignored by other strategies.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `consumerType` | enum | `CONSUMER_GROUP` | Consumer type. See [Consumer Types](#consumer-types).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `consumerProcMsMin` | integer | `0` | Minimum simulated per-record consumer processing time in milliseconds. Each record is assigned a random processing time between `consumerProcMsMin` and `consumerProcMsMax` ms, modelling downstream work. The actual sleep pattern depends on `consumerProcParallelism` (see below).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `consumerProcMsMax` | integer | `0` | Maximum simulated per-record consumer processing time in milliseconds. `0` disables the simulated processing delay.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `consumerProcParallelism` | integer | `1` | Concurrent processing mode: `1` = sequential (process messages one at a time), `>1` = constrained parallelism (simulate N parallel workers), `-1` = full parallelism (all messages start processing simultaneously). With constrained parallelism, each polled batch is divided among N workers; each worker processes its assigned messages serially, and the consumer sleeps once for the slowest worker's total time. With full parallelism, all messages start processing at the same time, and the consumer sleeps once for the maximum processing time across the batch. Simulates realistic application behavior where consumers process multiple messages in parallel. **Note:** In explore mode, concurrent processing (`!= 1`) disables the theoretical-max fast-path because the max rate depends on unknown poll batch sizes; the engine falls back to standard ramp + binary search. |

### Producer Rate Schedules

`producerRateSchedule` supports two schedule types.

Run-mode `rampIntervals` still applies when a schedule is configured. Dimster
ramps from `firstScheduledRate / rampIntervals` to the first scheduled rate,
holds the first scheduled rate through warm-up, then starts the schedule from
elapsed zero when the measured test phase begins.

When producer load is spread across multiple workers, the coordinator sends a
shared wall-clock schedule start time to the workers. This keeps the schedule
phase aligned without a heavyweight synchronization protocol, but it relies on
normal host clock synchronization. Avoid very short schedule periods where
small clock skew or request timing differences would be material.

`OSCILLATING` runs a smooth cosine wave starting at `minRate`, reaching `maxRate` halfway through `periodSeconds`, then returning to `minRate` at the end of the period.

```yaml
baseWorkload:
  producerRateSchedule:
    type: OSCILLATING
    minRate: 1000
    maxRate: 100000
    periodSeconds: 600
```

`REPEATING` runs a sequence of hold and ramp segments. Ramps use smooth interpolation by default; set `interpolation: LINEAR` for a linear ramp.
Segment rates may be `0` to pause producer traffic; negative rates are invalid, and each schedule must include at least one positive rate.

```yaml
baseWorkload:
  producerRateSchedule:
    type: REPEATING
    segments:
      - fromRate: 1000
        toRate: 100000
        durationSeconds: 60
      - rate: 100000
        durationSeconds: 120
      - fromRate: 100000
        toRate: 1000
        durationSeconds: 60
      - rate: 1000
        durationSeconds: 300
```

With `dimster run --modify`, a schedule can be hot-swapped with inline JSON:

```bash
./dimster run localBeefy --modify --set 'producerRateSchedule={"type":"OSCILLATING","minRate":1000,"maxRate":100000,"periodSeconds":600}'
```

Setting `producerRate` through `--modify` clears any active schedule and returns producers to fixed-rate mode. Setting `producerRateSchedule` updates the resolved `producerRate` metadata to that schedule's max rate automatically.

### Message Distributor Types

The message distributor controls how each producer record is routed to a partition. Strategies fall into two families:

- **Key-based** — return a string key and let Kafka's configured partitioner decide which partition it lands on. The partition mapping depends on the partitioner algorithm (typically murmur2 hash mod partition count). Use these when you want to model realistic producer behavior where applications set message keys and rely on the broker for partition placement.
- **Partition-targeted** — explicitly name the partition number and bypass the partitioner entirely. The record is sent with a null key and a specific partition. Use these when you want direct, deterministic control over which partitions receive load — for example, to create a known-skewed or perfectly-uniform distribution independent of key hashing.

#### Key-based strategies

| Value | Description |
|-------|-------------|
| `NO_KEY` | No message key (null). Messages use Kafka's default sticky partitioner, which batches records into the same partition until the batch is full, then picks a new one. Good baseline for throughput tests where partition distribution doesn't matter. |
| `KEY_ROUND_ROBIN` | Cycle through a pool of 10,000 pre-generated keys in round-robin order. Each key always hashes to the same partition, so partition load depends on how evenly the partitioner distributes the key space. Provides a stable, repeatable distribution across runs. |
| `RANDOM_NANO` | Pick a random key from the pool on each send using `System.nanoTime()`. Produces a roughly uniform partition distribution through the hash partitioner, but with more randomness per-message than `KEY_ROUND_ROBIN`. |
| `KEY_ZIPF` | Pick a key from the pool with a Zipf-distributed index. A small subset of keys is produced far more often than the rest, leading to hot partitions through the broker's hash partitioner. The resulting partition skew depends on how keys happen to hash — it's realistic but not fully deterministic. Skew controlled by `messageDistributorSkew`. |
| `KEY_COLUMN` | Use the value of the dataset's `keyColumn` as the record key, so key and partition skew follow that column's distribution (for example a Zipf-distributed `user_id`). Requires a `dataset` with a `keyColumn`, and a `keyColumn` requires this distributor. |

#### Partition-targeted strategies

| Value | Description |
|-------|-------------|
| `PARTITION_ZIPF` | Target a specific partition number drawn from a Zipf distribution. Partition 0 is always the hottest; partition (n-1) is the coldest. Produces a maximally skewed, fully deterministic load profile — useful for stressing share-group fetcher fairness, per-partition back-pressure, or rebalance behavior under imbalance. Requires `partitionsPerTopic > 1` to be meaningful. Skew controlled by `messageDistributorSkew`. |
| `PARTITION_ROUND_ROBIN` | Cycle through partitions `[0, partitionsPerTopic)` in round-robin order. Produces a perfectly uniform distribution across all partitions without relying on key hashing. Useful when you want guaranteed even load regardless of the partitioner algorithm — for example, when testing per-partition throughput limits or comparing consumer group scaling with a known-uniform input. |

#### Choosing a Zipf skew (`messageDistributorSkew`)

The probability of partition `k` (1-indexed) is proportional to `1 / k^skew`. The hot/cold ratio grows quickly with skew but the share of partition 0 grows more slowly, so values like 1.0–1.1 produce only a mild imbalance. Reference table for the share of messages landing on partition 0 (the hottest), and the ratio between the hottest and coldest partition, for `partitionsPerTopic = 10`:

| skew | p0 share | p0 : p(n-1) ratio | Notes |
|------|----------|-------------------|-------|
| `1.0` | ~34% | 10× | Classic Zipf-1. Soft skew, long tail still meaningful. |
| `1.1` | ~37% | ~13× | Barely distinguishable from `1.0` at low partition counts. |
| `1.5` | ~50% | ~32× | First value where p0 dominates the topic. |
| `2.0` | ~65% | ~99× | Heavy skew; bottom half of partitions barely fire. |

Two notes when picking a value:

- The differences between low skew values widen as `partitionsPerTopic` grows. At 10 partitions the gap between `1.0` and `1.5` is moderate; at 100+ partitions p0's share for `skew=1.0` drops below 20% while `skew=1.5` stays near 38%.
- For "realistic production-like skew" use `1.1`–`1.5`. For "stress fairness / hot-partition isolation" experiments where you want one partition to clearly dominate, use `2.0` or higher.

### Generated Datasets

`dataset` replaces the default payload dataset (opaque random bytes) with generated rows that look like real data, for comparing record formats and measuring compression. The concepts (how values, cardinality and skew are generated) are described in [Data Generation](../concepts/data-generation.md), which also has [column recipes](../concepts/data-generation.md#column-recipes) for common cases (number ranges, fixed value sets, dates, names, text); this section lists the fields.

```yaml
baseWorkload:
  messageDistributor: KEY_COLUMN
  dataset:
    preset: clicks          # clicks | orders | taxi, optional
    format: AVRO            # AVRO (default) | JSON
    seed: 42                # default 0
    keyColumn: user_id      # optional; requires messageDistributor KEY_COLUMN
    columns:                # optional; merged onto the preset's columns by name
      country: {cardinality: 20}
      coupon:  {type: STRING, kind: ALPHANUMERIC, length: [8, 8], cardinality: 500, nullRate: 0.9}
```

Every record has the schema `{_dimster: bytes, <columns>...}`. The `_dimster` field is Dimster's own header: the message type (normal, probe or correctness), plus the producer id, sequence and CRC in correctness mode. Dimster writes and reads it; it is never a dataset column, and column names starting with `_dimster` are rejected.

Without a `dataset`, every record still has a schema: the payload dataset, `{_dimster: bytes, payload: bytes}`, in RAW encoding (the header bytes, then the payload bytes, with no framing). The payload is `messageSize - 1` bytes built from `useRandomizedPayloads`, `randomBytesRatio`, `randomizedPayloadPoolSize` and `payloadFile`, so normal records are exactly `messageSize` bytes. Correctness records are larger by the extra header bytes (21, or 25 with a CRC), since the header precedes the payload. Results report this dataset as preset `payload`, format `RAW`.

#### dataset Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `preset` | string | _(unset)_ | A built-in dataset: `clicks` (web clickstream events, 12 columns), `orders` (e-commerce orders with customer names, emails and addresses, 15 columns) or `taxi` (trips shaped like NYC TLC yellow-taxi records, 19 columns). Each is defined in `dimster-core/src/main/resources/datagen/presets/<preset>.yaml`, which lists its columns; see [Where presets are defined](../concepts/data-generation.md#where-presets-are-defined). |
| `format` | enum | `AVRO` | Record encoding. `AVRO` is Avro binary encoding of the schema above (nullable columns are `["null", T]` unions); `JSON` is a compact JSON object with `_dimster` as its first member, in lowercase hex. |
| `seed` | long | `0` | Seed for value generation. With the same seed, each column maps the same rank to the same value on every producer and in every run. |
| `keyColumn` | string | _(unset)_ | Column whose value becomes the record key. Requires `messageDistributor: KEY_COLUMN`. |
| `columns` | map | _(unset)_ | Column definitions in schema order, keyed by column name (`[A-Za-z_][A-Za-z0-9_]*`). A column that matches a preset column overrides it field by field; other columns are appended. |

#### Overriding Preset Columns

`columns` merges onto the preset rather than replacing it:

- A column whose name matches a preset column overrides only the fields it sets; its other fields are inherited from the preset, and it keeps its position.
- A column with a new name is appended after the preset's columns, in workload order.
- Preset columns that `columns` doesn't mention are kept as they are.

For example, `orders` defines `customer_id: {type: LONG, min: 1, max: 9999999, cardinality: 200000, distribution: ZIPF, skew: 0.7}`. With

```yaml
dataset:
  preset: orders
  columns:
    customer_id: {cardinality: 1000}                            # override one field
    coupon: {type: STRING, kind: ALPHANUMERIC, length: [8, 8]}  # new column, appended
```

`customer_id` becomes `{type: LONG, min: 1, max: 9999999, cardinality: 1000, distribution: ZIPF, skew: 0.7}`, every other `orders` column is unchanged, and `coupon` is the last column.

Because overrides merge:

- A preset column can't be removed, and an inherited field can't be cleared, only given another value. To leave out preset columns, omit `preset` and define every column inline; the preset's file (see [Where presets are defined](../concepts/data-generation.md#where-presets-are-defined)) is a starting point to copy.
- Changing a column's `type` keeps its other inherited fields. Fields that don't apply to the new type are rejected with an error naming the column and field, so define a differently named column instead.
- Each element of a scenario's `dataset` list is a complete dataset definition: it merges onto its own `preset`, not onto `baseWorkload.dataset`.

#### Column Fields

| Field | Applies to | Default | Description |
|-------|------------|---------|-------------|
| `type` | all | _(required)_ | `LONG`, `INT`, `DOUBLE`, `BOOLEAN`, `STRING`, `TIMESTAMP` (wall-clock epoch milliseconds), `SEQUENCE` (a per-producer counter, unique within a topic) or `BYTES` (opaque bytes: a random prefix followed by zeros; Avro `bytes`, lowercase hex in JSON). |
| `cardinality` | `LONG`, `INT`, `STRING`, `BYTES` | see description | Number of distinct values. For numbers, unset means a range (see `min`/`max`). For strings, unset means the whole dictionary for dictionary kinds and unbounded (a fresh value per row) otherwise. For bytes, unset means unbounded. |
| `distribution` | `LONG`, `INT`, `DOUBLE`, `STRING`, `BYTES` | `UNIFORM` | `UNIFORM`; `ZIPF` (categorical only: rank 0 is the most frequent, see `skew`); or `NORMAL` (ranges only: mean at the midpoint, standard deviation of a sixth of the range, clamped). Which types accept which: see [Distributions](#distributions). |
| `skew` | `ZIPF` columns | `1.0` | Zipf exponent: 0 is uniform, 1 is classic Zipf, 2 and above concentrate heavily on the first values. |
| `min`, `max` | `LONG`, `INT`, `DOUBLE` | `0`, type maximum (`DOUBLE`: `0`, `1`) | The value range. For categorical integers, the domain the values are drawn from. |
| `scramble` | `LONG`, `INT` | `true` | Categorical integers: map ranks to pseudo-random values in `[min, max]` (true) or to `min + rank` (false, which makes the most frequent values the smallest). |
| `scale` | `DOUBLE` | _(no rounding)_ | Number of decimal places to round to, such as `2` for money. |
| `values`, `weights` | `STRING` | _(unset)_ | A weighted enum: the values, and optional relative weights (equal when unset). Can't be combined with `kind`, `cardinality` or `distribution`. |
| `kind` | `STRING` | `ALPHANUMERIC` | What values look like. Dictionary kinds: `FIRST_NAME`, `LAST_NAME`, `CITY`, `COUNTRY`, `DOMAIN`, `USER_AGENT`, `WORD`. Composite kinds: `FULL_NAME`, `STREET_ADDRESS`, `EMAIL`, `URL`, `SENTENCE`. Formatted kinds: `ALPHANUMERIC`, `IP`, `UUID`. |
| `length` | `ALPHANUMERIC`, `BYTES` | `[8, 16]`, `[100, 100]` | `[min, max]` length in characters (`ALPHANUMERIC`) or bytes (`BYTES`). |
| `randomRatio` | `BYTES` | `1.0` | Fraction of each value that is random; the rest is zeros. Lower values compress better. |
| `words` | `SENTENCE` | `[5, 15]` | `[min, max]` number of words. |
| `trueRate` | `BOOLEAN` | `0.5` | Probability of `true`. |
| `jitterMs` | `TIMESTAMP` | `0` | Values are the current time minus a uniform random `[0, jitterMs]`. |
| `nullRate` | all | `0` | Probability that a value is null. A nonzero rate makes the column nullable. |
| `runLength` | all | `1` | Hold each sampled value for this many consecutive rows on a producer. Clusters values within batches, which affects compression and batch-level min/max skipping. |

#### Distributions

`distribution` is one of three values:

- `UNIFORM` (the default): every value, or every rank, equally likely.
- `ZIPF`: for categorical columns, those with a `cardinality`. Rank 0 is the most frequent, and rank `k` has probability proportional to `1 / (k + 1)^skew` (see `skew`). Cardinalities up to 2^31 - 1.
- `NORMAL`: for ranges, numbers without a `cardinality`. The mean is the midpoint of `[min, max]` and the standard deviation a sixth of the range; values outside the range are clamped to it. The mean and spread can't be set separately.

Which ones a column accepts depends on its type:

| Column | `UNIFORM` | `ZIPF` | `NORMAL` |
|---|---|---|---|
| `LONG`, `INT` with `cardinality` | yes | yes | no |
| `LONG`, `INT` without `cardinality` (a range) | yes | no (needs a `cardinality`) | yes |
| `DOUBLE` | yes | no | yes |
| `STRING` with a dictionary `kind` | yes | yes (the cardinality defaults to the dictionary size) | no |
| `STRING` with another `kind` | yes | needs a `cardinality` | no |
| `BYTES` | yes | needs a `cardinality` | no |
| `STRING` with `values` | not settable: use `weights` | | |
| `BOOLEAN` | not settable: use `trueRate` | | |
| `TIMESTAMP`, `SEQUENCE` | not settable | | |

A combination marked no or not settable is rejected with an error naming the column. Message keys and partitions have their own, separate distributions; see [Message Distributor Types](#message-distributor-types).

Record sizes follow from the data, so `messageSize` does not apply. Dimster estimates the dataset's average record size from a sample of rows and uses it as the test point's `messageSize`, so that byte-based settings and figures (backlog sizes, explore MB/s) stay approximately right. The results also carry measured byte rates (`publishBytesRate`, `consumeBytesRate`) and `avgRecordBytes`, which the charts use when present.

Lists in a scenario are zipped, not crossed, so a dataset dimension combined with a `kafkaConfig` dimension is written point by point. See `run/workloads/datagen-test/` for examples.

### Flink SQL Stage

`flink` puts a Flink SQL job between the producers and the consumers: producers write the test topics, the job runs `sql` over each topic and writes the result to a `<topic>-out` topic, and consumers read the `-out` topics. The environment needs a `flink` block (see [`dimster-config.yaml`](dimster-config.md#flink)). How it works, the `source` table's columns and the type mapping are described in [Flink SQL Stage](../concepts/flink.md).

```yaml
baseWorkload:
  dataset: {preset: clicks, format: AVRO}
  flink:
    sql: |
      SELECT * FROM source
      WHERE event_type IN ('purchase', 'add_to_cart')
    parallelism: 2
    deliveryGuarantee: AT_LEAST_ONCE
    sinkOptions:
      properties.linger.ms: 5
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sql` | string | _(required)_ | A `SELECT` over the table `source` (the dataset's columns plus `_dimster`, `_dimster_key` and `_dimster_ts`). It must return `_dimster` and `_dimster_ts` unchanged; `SELECT *` keeps both. Returning `_dimster_key` keys the output records. Filters, projections and transformations only: no aggregations or joins. |
| `parallelism` | integer | `1` | Parallelism of the job: of the query's operators, and of the source and sink unless they are set. |
| `sourceParallelism` | integer | `parallelism` | Parallelism of the Kafka source (its `scan.parallelism`). Subtasks beyond the input partition count are idle. |
| `sinkParallelism` | integer | `parallelism` | Parallelism of the Kafka sink (its `sink.parallelism`). |
| `checkpointIntervalMs` | integer | `10000` | Checkpoint interval in milliseconds. `0` disables checkpointing. |
| `deliveryGuarantee` | enum | `AT_LEAST_ONCE` | Kafka sink guarantee: `AT_LEAST_ONCE`, `EXACTLY_ONCE` (transactions committed on checkpoints; requires checkpointing, and consumers default to `isolation.level: read_committed`) or `NONE`. |
| `outputPartitionsPerTopic` | integer | `partitionsPerTopic` | Partitions of each output topic. |

The job needs as many free task slots as the highest of `parallelism`, `sourceParallelism` and `sinkParallelism`. See [Source, query and sink parallelism](../concepts/flink.md#source-query-and-sink-parallelism).
| `config` | map | _(empty)_ | Extra Flink job configuration, e.g. `table.exec.mini-batch.enabled: true` or `pipeline.object-reuse: true`. |
| `sourceOptions` | map | _(empty)_ | Extra options of the Kafka source table, e.g. `properties.fetch.min.bytes`. |
| `sinkOptions` | map | _(empty)_ | Extra options of the Kafka sink table, e.g. `properties.linger.ms` or `sink.partitioner`. |

Dimster sets the tables' `connector`, `topic`, formats, key options, `properties.bootstrap.servers`, `properties.group.id` and `scan.startup.mode`, so `sourceOptions` and `sinkOptions` can't. `scan.parallelism` and `sink.parallelism` are set with `sourceParallelism` and `sinkParallelism` instead. The workload's Kafka `commonConfig` (bootstrap servers and security settings) is passed to both tables, and the `-out` topics get the `replicationFactor` and `topicConfig`. `producerConfig` and the consumer configs apply only to Dimster's producers and consumers; tune the Flink source and sink with `sourceOptions` and `sinkOptions` instead. With transactional producers, the source defaults to `properties.isolation.level: read_committed`. See [Kafka configuration](../concepts/flink.md#kafka-configuration).

In drain-backlog mode the job starts when the backlog is built, and the drain completes when the consumers have received one record per record produced, so the query must not filter (see [Drain-backlog mode](../concepts/flink.md#drain-backlog-mode)).

Scenarios sweep the fields `sql`, `parallelism`, `sourceParallelism`, `sinkParallelism`, `checkpointIntervalMs`, `deliveryGuarantee` and `outputPartitionsPerTopic` through a nested `flink` block, like `runConfig`:

```yaml
scenarios:
  - name: flink-parallelism
    label: ["p1", "p2", "p4"]
    flink:
      parallelism: [1, 2, 4]
```

The maps (`config`, `sourceOptions`, `sinkOptions`) come from `baseWorkload.flink` only. `flink` can't be changed with `dimster run --modify`.

### Consumer Types

| Value | Description |
|-------|-------------|
| `CONSUMER_GROUP` | Standard Kafka consumer groups with partition assignment. |
| `SHARE_GROUP` | Kafka share groups (KIP-932). Shared consumption model without partition assignment. Requires Kafka 4.0+ and appropriate broker configuration. |

---

## Scenarios

Scenarios allow running multiple test variations from a single workload file. Each scenario can override any `baseWorkload` field.

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Scenario name, used in result metadata. |
| `label` | list of string | Per-test-point labels for chart output. |
| `topics` | list of integer | Override `topics` per test point. |
| `partitionsPerTopic` | list of integer | Override `partitionsPerTopic` per test point. |
| `producersPerTopic` | list of integer | Override `producersPerTopic` per test point. |
| `consumerGroupsPerTopic` | list of integer | Override `consumerGroupsPerTopic` per test point. |
| `consumersPerGroup` | list of integer | Override `consumersPerGroup` per test point. |
| `messageSize` | list of integer | Override `messageSize` per test point. |
| `producerRate` | list of integer | Override `producerRate` per test point. |
| `producerRateSchedule` | list of object | Override `producerRateSchedule` per test point. |
| `dataset` | list of object | Override `dataset` per test point. Each element is a whole dataset definition (preset, format, columns). Can't be combined with a `messageSize` list. |
| `messageDistributor` | list of enum | Override `messageDistributor` per test point. |
| `messageDistributorSkew` | list of double | Override `messageDistributorSkew` per test point. |
| `consumerType` | list of enum | Override `consumerType` per test point. |
| `consumerProcMsMin` | list of integer | Override `consumerProcMsMin` per test point. |
| `consumerProcMsMax` | list of integer | Override `consumerProcMsMax` per test point. |
| `consumerProcParallelism` | list of integer | Override `consumerProcParallelism` per test point. |
| `kafkaConfig` | object | Override [Kafka configuration](kafka-configs.md). Individual property values within its config maps may be lists to express dimensions (see [Per-scenario Kafka config overrides](kafka-configs.md#per-scenario-kafka-config-overrides)). |
| `drainBacklogConfig` | object | Drain-backlog mode only. Per-scenario overrides for `drainBacklogConfig` fields. Each field is a list expressing a per-test-point dimension; omitted fields inherit from the workload-level `drainBacklogConfig`. |
| `exploreConfig` | object | Explore mode only. Per-scenario overrides for `exploreConfig` fields. Each field is a list expressing a per-test-point dimension; omitted fields inherit from the workload-level `exploreConfig`. |
| `flink` | object | Per-scenario overrides for `baseWorkload.flink` fields (`sql`, `parallelism`, `sourceParallelism`, `sinkParallelism`, `checkpointIntervalMs`, `deliveryGuarantee`, `outputPartitionsPerTopic`). Each field is a list expressing a per-test-point dimension; omitted fields inherit from `baseWorkload.flink`. See [Flink SQL Stage](#flink-sql-stage). |
| `runConfig` | object | Run mode only. Per-scenario overrides for `runConfig` fields (`testDurationMinutes`, `warmupDurationMinutes`, `sampleRateMillis`, `rampIntervals`, `rampIntervalSeconds`). Each field is a list expressing a per-test-point dimension; omitted fields inherit from the workload-level `runConfig`. |
| `repeat` | integer | Number of times to repeat this scenario back-to-back. Default `1`. Useful for stability/reproducibility runs. |

**Rules:**

- **Scalar values** apply to all test points (e.g. `messageSize: 2048` sets the same size for every point).
- **Array values** create multiple test points. Each index becomes a separate test run. All arrays in a scenario must have the same length -- this includes list-valued properties inside `kafkaConfig`, `drainBacklogConfig`, `exploreConfig`, and `runConfig`.
- Fields not specified in a scenario inherit from `baseWorkload` (or the workload-level `drainBacklogConfig` / `exploreConfig` / `runConfig` for those nested overrides).
- If `scenarios` is absent or empty, a single test runs using the `baseWorkload` values directly.

When `repeat` is greater than `1`, Dimster keeps each repeated scenario result and also adds a synthetic `<scenario name> (merged)` result. The merged result is an average of the repeated runs' summary fields. Latency percentiles in the merged result are therefore averages of the per-run percentile values, not percentiles recalculated from a single pooled latency distribution. Use the individual repeated runs when run-to-run variance or strict pooled percentile semantics matter.

### Scenario Example

```yaml
scenarios:
  - name: producer-rate
    label: ["100k", "200k", "300k"]
    producerRate: [100000, 200000, 300000]
```

This produces three test runs using all other values from `baseWorkload`, varying only `producerRate`.

### Concurrent Processing Example

The `consumerProcParallelism` field simulates realistic application behavior where consumers process multiple messages in parallel rather than strictly sequentially.

```yaml
baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
  producersPerTopic: 1
  producerRate: 1000
  consumerProcMsMin: 50
  consumerProcMsMax: 100
  consumerProcParallelism: 8  # Simulate 8 parallel workers per consumer

scenarios:
  - name: processing-modes
    label: ["sequential", "parallel-4", "parallel-8", "full-parallel"]
    consumerProcParallelism: [1, 4, 8, -1]
```

This creates four test points comparing different processing strategies:
- **Sequential (1)**: Process one message at a time (existing behavior). With 50-100ms per message, max throughput is ~10-20 msg/s per consumer.
- **Parallel (4)**: Simulate 4 parallel workers. Each polled batch is divided among 4 workers; each worker processes its records serially. Total sleep time = slowest worker.
- **Parallel (8)**: Simulate 8 parallel workers. Higher parallelism increases throughput proportionally (up to poll batch size limits).
- **Full parallel (-1)**: All messages in each polled batch get processed concurrently. Sleep once for the max processing time across the entire batch. Maximum possible throughput for a given processing time.

**Explore mode note:** When `consumerProcParallelism != 1`, the explore engine cannot calculate a theoretical maximum rate (depends on poll batch sizes) and uses standard ramp + binary search instead of the fast-path.

**Correctness mode note:** When `consumerProcParallelism != 1`, the correctness tracker may report out-of-order violations.

---

## Run Mode (runConfig)

Standard benchmark mode. Produces and consumes at a target rate for a fixed duration.

### runConfig Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `testDurationMinutes` | integer | `5` | Duration of the measurement phase in minutes. |
| `warmupDurationMinutes` | integer | `1` | Duration of the warmup phase in minutes. Stats collected during warmup are discarded. |
| `rampIntervals` | integer | `3` | Number of equal increments in the ramp-up phase before warmup. When both `rampIntervals` and `rampIntervalSeconds` are `> 0`, fixed-rate workloads step from `producerRate / rampIntervals` up to `producerRate`; scheduled workloads step from the first scheduled rate divided by `rampIntervals` up to the first scheduled rate. Scheduled workloads then hold that first rate through warm-up and start the schedule from elapsed zero when the measured test phase begins. Stats during the ramp are discarded. Set to `0` to disable ramp-up. |
| `rampIntervalSeconds` | integer | `20` | Duration in seconds of each ramp-up increment. Set to `0` to disable ramp-up. |
| `sampleRateMillis` | integer | `10000` | Stats sampling interval in milliseconds. |

### availabilityConfig (optional)

When present in a run-mode workload, enables per-partition availability probes that measure producer availability and failover times alongside the benchmark.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `unavailabilityThresholdMs` | integer | `5000` | Milliseconds without a successful probe before a partition is considered unavailable. |
| `probeTimeoutSeconds` | integer | `30` | Timeout in seconds for each availability probe operation. |
| `probePartitions` | integer | `1` | Number of partitions on the dedicated probe topic. |

### Run Mode Example

```yaml
name: throughput-scaling

runConfig:
  testDurationMinutes: 5
  warmupDurationMinutes: 1

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageSize: 1000
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
  producersPerTopic: 6
  producerRate: 50000

scenarios:
  - name: rate-sweep
    label: ["100k", "200k", "300k"]
    producerRate: [100000, 200000, 300000]
```

### Sweeping runConfig Fields

Scenarios may sweep the run-mode phase settings via a nested `runConfig` block. All five `runConfig` fields are sweepable: `testDurationMinutes`, `warmupDurationMinutes`, `sampleRateMillis`, `rampIntervals`, `rampIntervalSeconds`. Omitted fields inherit from the workload-level `runConfig`. Arrays inside `scenarios.runConfig` must have the same length as any other dimensions in the scenario.

```yaml
name: ramp-sweep

runConfig:
  testDurationMinutes: 10
  warmupDurationMinutes: 1

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageSize: 1000
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
  producersPerTopic: 6
  producerRate: 100000

scenarios:
  - name: ramp-shapes
    label: ["short", "medium", "long"]
    runConfig:
      rampIntervals: [3, 5, 10]
      rampIntervalSeconds: [10, 20, 30]
```

This produces three test points at the same `producerRate` of 100k msg/s, each warmed up via a different ramp profile (30 s, 100 s, and 5 min respectively).

### Run Mode with Availability Probes

```yaml
name: availability-test

runConfig:
  testDurationMinutes: 5
  warmupDurationMinutes: 0

availabilityConfig:
  unavailabilityThresholdMs: 5000
  probeTimeoutSeconds: 30
  probePartitions: 6

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageSize: 1000
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 3
  producersPerTopic: 3
  producerRate: 1000
```

---

## Explore Mode (exploreConfig)

Finds the maximum sustainable throughput under latency constraints using a three-phase algorithm:

1. **Ramp** (phase 1): Start at `initialRate` and double every `rampIntervalSeconds` until latency thresholds are breached.
2. **Search** (phase 2): Binary search between the last good rate and the first bad rate. Each midpoint is tested for `searchDurationSeconds`, checked every `searchIntervalSeconds`. Three consecutive bad checks mark a rate as saturated.
3. **Sustain** (phase 3): Hold the discovered rate for `sustainDurationSeconds`. If 80%+ of checks pass, the rate is confirmed. Otherwise, reduce the upper bound and retry search+sustain (up to `searchMaxRetries`).

### exploreConfig Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `e2eLatencyPercentile` | double | `75` | Latency percentile to evaluate (e.g. `75` for p75). |
| `e2eLatencyThresholdMs` | double | `50` | Maximum acceptable end-to-end latency in milliseconds at the given percentile. |
| `publishDelayThresholdMs` | double | `200` | Maximum acceptable publish delay in milliseconds at the given percentile. Measures how far behind schedule producers fall. |
| `sustainLeniency` | double | `0.10` | Fraction by which the sustain phase relaxes thresholds (e.g. `0.10` = 10% more lenient than the search phase). |
| `initialRate` | integer | `100` | Starting rate in messages/sec for the ramp-up phase. Doubled each interval. |
| `rampIntervalSeconds` | integer | `10` | Measurement interval in seconds during ramp-up (phase 1). |
| `rampWarmupSeconds` | integer | `10` | Warmup seconds before each test point (at initial rate, stats discarded). |
| `searchDurationSeconds` | integer | `120` | Duration in seconds of each binary search measurement window (phase 2). |
| `searchIntervalSeconds` | integer | `10` | How often to check for latency violations during binary search, in seconds. 3 consecutive bad checks = saturated. |
| `searchMaxRetries` | integer | `3` | Maximum retries of binary search + sustain if the discovered rate cannot be sustained. |
| `sustainDurationSeconds` | integer | `300` | Duration in seconds to sustain the discovered peak rate (phase 3). 80%+ good checks = pass. |
| `stabilizeRate` | integer | `0` | Producer rate (msgs/s) to maintain during the inter-phase stabilization drain. A small trickle keeps share-group consumers progressing on Kafka versions affected by the stop-drain bug. `0` means no producer activity during stabilization. |
| `stabilizeTimeoutSeconds` | integer | `120` | Maximum seconds to wait for consumers to drain remaining messages between search/sustain phases. Prevents stale messages from a failed high-rate attempt contaminating the next measurement. |
| `minViableRate` | double | `1.0` | Minimum rate (msgs/s) below which binary search declares the system unable to sustain any load for this test point. |
| `useDeliveryLatencyForThreshold` | boolean | `false` | When `true`, threshold checks use e2e delivery latency (measured at poll return, before per-record processing) instead of standard e2e latency. Useful when `consumerProcMsMin`/`consumerProcMsMax` are set, as standard e2e latency includes simulated processing time. Combines with `perPartitionE2eMode`: when both are true, worst per-partition delivery latency is used. **Note:** When `consumerProcParallelism != 1`, the explore engine cannot calculate a theoretical maximum rate (it depends on poll batch sizes) and disables the fast-path, falling back to standard ramp + binary search regardless of processing time configuration. |

### Explore Mode Example

```yaml
name: explore-limits

exploreConfig:
  e2eLatencyPercentile: 75
  e2eLatencyThresholdMs: 50
  publishDelayThresholdMs: 200
  sustainLeniency: 0.1
  initialRate: 10000
  searchDurationSeconds: 120
  sustainDurationSeconds: 300

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageSize: 1000
  producersPerTopic: 1
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
  useRandomizedPayloads: true
  randomBytesRatio: 0.5

scenarios:
  - name: partition-scaling
    label: ["1-part", "6-part"]
    partitionsPerTopic: [1, 6]
    consumersPerGroup: [1, 6]
```

---

## Correctness Mode (correctnessConfig)

Verifies message delivery correctness. Each producer is pinned to a specific partition (round-robin within each topic), bypassing the partitioner so every partition receives a single ordered stream from one producer — this is what makes out-of-order detection meaningful.

### correctnessConfig Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `testMode` | enum | `CONCURRENT` | `CONCURRENT` runs producers and consumers in parallel for `testDurationMinutes`. `DRAIN` builds a backlog (sized by `backlogSizeGB`, or by `testDurationMinutes` when `backlogSizeGB=0`) with consumers paused, then starts consumers and measures while they drain. |
| `testDurationMinutes` | integer | `5` | Duration of the test in minutes (CONCURRENT mode), or duration of the backlog-build phase when `backlogSizeGB=0` in DRAIN mode. |
| `backlogSizeGB` | double | `0` | DRAIN mode only. Target backlog size in GB before consumers start. When `> 0` overrides `testDurationMinutes` for the build phase. Supports fractions (e.g. `0.05` = 50 MB). `0` means use `testDurationMinutes`. |
| `gracePeriodSeconds` | integer | `30` | Seconds to wait after producers stop for consumers to drain remaining in-flight messages. |
| `detectDuplicates` | boolean | `true` | Enable duplicate message detection. |
| `detectOutOfOrder` | boolean | `true` | Enable out-of-order message detection. |
| `enableCrc` | boolean | `false` | Enable CRC verification on message payloads to detect corruption. |
| `staticConsumerAssignment` | boolean | `true` | Use static partition assignment for consumers (each consumer is pinned to a fixed set of partitions) rather than relying on the consumer group protocol. Removes rebalance noise from correctness measurements. |
| `consumerRestartSeed` | integer | `0` | Seed used by correctness live consumer restarts. The random sequence resets for each test point so equivalent protocol cases select equivalent consumer ordinals. |
| `sampleRateMillis` | integer | `10000` | Stats sampling interval in milliseconds. |
| `logDuplicateCount` | integer | `100` | Number of duplicate messages to log before suppressing further output. |
| `failOnLoss` | boolean | `true` | Fail the test if message loss is detected. |
| `failOnDuplicates` | boolean | `false` | Fail the test if duplicates are detected (only relevant when `detectDuplicates=true`). |
| `failOnOutOfOrder` | boolean | `true` | Fail the test if out-of-order delivery is detected (only relevant when `detectOutOfOrder=true`). |
| `failOnCrc` | boolean | `true` | Fail the test on CRC mismatch (only relevant when `enableCrc=true`). |

### Correctness Mode Example

```yaml
name: correctness-check

correctnessConfig:
  testDurationMinutes: 5
  gracePeriodSeconds: 30
  detectDuplicates: true
  detectOutOfOrder: true
  enableCrc: true

baseWorkload:
  topics: 1
  partitionsPerTopic: 10
  messageSize: 256
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 5
  producersPerTopic: 10
  producerRate: 1000
```

---

## Drain Backlog Mode (drainBacklogConfig)

Measures how quickly consumers can drain a pre-built backlog. The engine first builds a backlog by producing without consumers, then starts consumers and measures drain time.

### drainBacklogConfig Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `backlogSizeGB` | double | `1` | Size of the backlog to build before draining, in GB. Supports fractions (e.g. `0.05` = 50 MB). |
| `buildRate` | integer | `-1` | Producer rate while building the backlog. `-1` means use the `baseWorkload.producerRate`. |
| `backlogProducerRate` | integer | `0` | Producer rate during the drain phase. `0` means producers are paused (pure consumer drain). |
| `sampleRateMillis` | integer | `10000` | Stats sampling interval in milliseconds. |
| `drainTimeoutMinutes` | integer | `0` | Maximum minutes to wait for the drain to complete. `0` means no timeout (wait indefinitely). |
| `warmup` | boolean | `false` | Run each scenario's first test point once, unrecorded and without hooks, before the scenario, so its first recorded drain runs on warm JVMs. Also settable per scenario (`scenarios[].drainBacklogConfig.warmup`), but not sweepable. See [Warm-up](../modes/drain-backlog.md#warm-up). |
| `reuseBacklog` | boolean | `false` | Build a backlog once and drain it again in each following test point that differs only in drain-side settings. Requires `backlogProducerRate: 0`. Workload-level only. See [Reusing a backlog](../modes/drain-backlog.md#reusing-a-backlog). |
| `settleTimeoutMinutes` | integer | `10` | Before a drain that follows a `beforeDrain` hook or reuses a backlog, the maximum minutes to wait for every partition to have a full ISR and its preferred leader. Workload-level only. |

### Drain Backlog Mode Example

```yaml
name: drain-50GB

drainBacklogConfig:
  backlogSizeGB: 50
  backlogProducerRate: 0
  buildRate: 100000
  sampleRateMillis: 10000

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageSize: 1000
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
  producersPerTopic: 6
```

---

## Classic vs Share Group Comparison Example

No embedded kafkaConfig, so assumed to use `-k path/to/kafka-config.yaml` in the dimster command.

```yaml
name: classic-vs-share

runConfig:
  testDurationMinutes: 5
  warmupDurationMinutes: 1

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  producersPerTopic: 1
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
  messageSize: 1000
  producerRate: 50000

scenarios:
  - name: consumer-type
    consumerType: [CONSUMER_GROUP, SHARE_GROUP]
    label: ["Classic", "Share Group"]
```
