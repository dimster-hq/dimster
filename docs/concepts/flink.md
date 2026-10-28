# Flink SQL Stage

A workload can put an Apache Flink SQL job between its producers and consumers:

```
producers ──► dimster-<s>-0000 ──► Flink SQL job ──► dimster-<s>-0000-out ──► consumers
```

Producers send [dataset](data-generation.md) records to the test topics as usual. A Flink job reads each topic, runs the workload's SQL over it, and writes the result to a matching `-out` topic. The consumers read the `-out` topics and measure end-to-end latency from the producer's send to their receipt, so the latency includes the time spent in Flink. Flink also stamps each output record with the time it emitted it, so the latency is also reported in two parts: [producer to Flink and Flink to consumer](#latency-through-flink).

The SQL is a `SELECT` over a table named `source`. It can filter, project and transform rows, but it can't aggregate or join: each input row gives at most one output row. Dimster writes everything else: the Kafka source and sink tables, their formats, and the plumbing that keeps the fields Dimster needs.

This is supported in run and drain-backlog modes.

## Setting it up

Two pieces are needed: a Flink cluster in the environment, and a `flink` block in the workload.

### The Flink cluster

Add a `flink` block to the environment in `dimster-config.yaml` and deploy:

```yaml
environments:
  local:
    image: dimster
    flink:
      taskmanagers:
        replicas: 1
        slots: 2
```

```bash
./dimster deploy local
```

This deploys a Flink session cluster: one JobManager and the configured TaskManagers, from the stock `flink` image. The coordinator runs one Flink job per test point on it. See [`flink` in the config reference](../reference/dimster-config.md#flink) for sizing, and `--only flink` to deploy just Flink.

A job of parallelism P needs P free task slots (replicas × slots in total). Flink's metrics are scraped by Prometheus and shown on the **Dimster - Flink** Grafana dashboard. `dimster dashboard` also forwards the Flink web UI to http://localhost:8081, and `dimster logs <env> flink` shows the Flink logs.

### The workload

```yaml
baseWorkload:
  dataset: {preset: clicks, format: AVRO}
  flink:
    sql: |
      SELECT * FROM source
      WHERE event_type IN ('purchase', 'add_to_cart')
    parallelism: 2
```

A Flink workload needs a `dataset`, because Flink reads the records through the dataset's schema. The default payload dataset is opaque bytes, so it can't be used. Field reference: [`flink` in the workload reference](../reference/workloads.md#flink-sql-stage). The examples are in `run/workloads/flink-test/`.

## The `source` table

`source` has the dataset's columns, plus three Dimster columns:

| Column | Type | What it is |
|---|---|---|
| `_dimster` | `BYTES` (AVRO), `STRING` (JSON, lowercase hex) | Dimster's record header: the message type and, in correctness mode, sequence numbers. |
| *dataset columns* | see below | In the dataset's order. A column with a `nullRate` is nullable; the others are `NOT NULL`. |
| `_dimster_key` | `STRING` | The record key, or null. |
| `_dimster_ts` | `TIMESTAMP_LTZ(3)` | The record's Kafka timestamp: when the producer sent it. |

Dataset column types map to Flink types as follows:

| Dataset type | Flink type |
|---|---|
| `LONG`, `SEQUENCE` | `BIGINT` |
| `TIMESTAMP` | `BIGINT` (epoch milliseconds; `TO_TIMESTAMP_LTZ(col, 3)` converts it) |
| `INT` | `INT` |
| `DOUBLE` | `DOUBLE` |
| `BOOLEAN` | `BOOLEAN` |
| `STRING` | `STRING` |
| `BYTES` | `BYTES` (AVRO), `STRING` of lowercase hex (JSON) |

## What the SQL must keep

The query must return `_dimster` and `_dimster_ts`, unchanged:

- Consumers read the message type from `_dimster`, which must be the first field of each record (Dimster puts it first in the output whatever the query's column order).
- `_dimster_ts` is written back as the output record's timestamp, which end-to-end latency is measured from. Without it, latency would be measured from when Flink wrote the record, not when the producer sent it.

`SELECT * FROM source WHERE ...` keeps both. A projection lists them explicitly:

```sql
SELECT _dimster, _dimster_ts, user_id, UPPER(country) AS country, duration_ms / 1000 AS duration_s
FROM source
```

A query without them fails when the test point starts, with an error saying so.

`_dimster_key` is optional. Keeping it makes it the output record's key, so the output topic is partitioned like the input (for example by `user_id` with a `KEY_COLUMN` distributor). Without it, output records have no key, and the sink spreads them with Kafka's default partitioner.

The output records are in the dataset's format (AVRO or JSON) with the query's columns. Dimster derives the output schema from the query, so there is nothing to declare (see [How schemas flow](#how-schemas-flow)). With `SELECT *`, output records are byte for byte the input records.

## How schemas flow

There is no schema registry, and records don't carry a schema or a schema id. The only schema anyone writes is the workload's `dataset`. Every other schema is derived from it, and no component ever needs the full output schema:

```
workload dataset ──► coordinator resolves it (ResolvedDataset)
                       │
                       ├──► producers:  encode rows with the dataset's schema
                       │
                       ├──► Flink job:  source table generated from the dataset's columns
                       │                 └─ SELECT ──► planner resolves the output schema ──► sink table
                       │
                       └──► consumers:  read only the format, to find _dimster
```

### The coordinator

When it expands the workload into test points, the coordinator resolves each test point's `dataset` (a preset or explicit columns) into a `ResolvedDataset`: the format and an ordered list of columns, each with its type and nullability. At the start of a test point, it sends the `ResolvedDataset` to every worker as JSON (`/set-dataset`), and tells the consumer workers that their records come through Flink (`/set-consumed-through-flink`).

### Producers

Producers encode each generated row in the dataset's format, with `_dimster` first and then the columns in dataset order:

- **AVRO**: Avro binary, as described by `ResolvedDataset.avroSchema()`. Nullable columns are `["null", T]` unions. There is no schema or schema id in the record, only the field values in order.
- **JSON**: an object whose first member is `"_dimster":"<hex>"`, followed by the columns. `BYTES` columns are lowercase hex.

The record timestamp is left unset, so the Kafka client stamps it with the send time.

### Flink: the source table

The coordinator turns the dataset's columns into Flink SQL columns (`FlinkJobSpec.columns()`, using the [type mapping](#the-source-table) above). It sends them in the job spec, a JSON document passed base64-encoded as the runner jar's only argument when the job is submitted.

The runner generates the `source` table from them: `_dimster`, then the dataset columns in dataset order, then `_dimster_key` from the record key (`key.format = raw`) and `_dimster_ts` from the record's timestamp metadata. The value format is Flink's `avro` or `json`, not `avro-confluent`, because there is no registry.

This works because both sides use the same plain encoding. Flink's `avro` format builds its Avro schema from the table's columns, in column order, with nullable columns as `["null", T]` unions, and reads records with no schema id. That is exactly how producers wrote them, so Flink decodes them as they are. Logical types don't matter: a `TIMESTAMP` column is `timestamp-millis` in Dimster's Avro schema and `BIGINT` in Flink, and both are encoded as a long.

Avro binary is read by position, so if the generated columns didn't match the producers' encoding (in order, type or nullability), Flink would read garbage or fail to decode. It wouldn't report a missing field. `FlinkJobSpecTest` checks `columns()` against `avroSchema()` for the presets. JSON is matched by field name, so it is less sensitive to this.

### Flink: the sink table

The output schema is derived from the query:

1. The runner asks Flink's planner for the query's resolved schema (`tEnv.sqlQuery(sql).getResolvedSchema()`), which type-checks the `SELECT` against `source`. For example, `UPPER(country)` resolves to `STRING`.
2. It checks that `_dimster` and `_dimster_ts` are present with their source types, and puts the columns in sink order: `_dimster`, the other columns in query order, `_dimster_key` if selected, then `_dimster_ts`.
3. It creates the `sink` table with those columns, and inserts the query, reordered, into it. `_dimster_ts` is written as the record timestamp, `_dimster_key` as the record key, and the rest as the value in the dataset's format.
4. It adds a `_dimster_headers` column, written as the record headers, which carries the time Flink emitted the record (see [Latency through Flink](#latency-through-flink)). It isn't part of the value, and a query can't return a column with that name.

Flink's format then encodes the output with a schema built from the sink's columns. `_dimster` is first because it is the first column. With `SELECT *` the sink columns are the source's, so output records are byte for byte the input records.

The runner's `main` runs on the JobManager while the job is being submitted, so this resolution happens at submission. A query that doesn't type-check, drops `_dimster` or `_dimster_ts`, or aggregates fails the submission, and the test point fails before any producer starts.

### Consumers

The output schema isn't stored or sent anywhere: it exists only inside the Flink job. Consumers don't need it, because they never decode dataset columns. They read only:

- the message type and header from `_dimster`, found from the format alone: in Avro it is a length-prefixed byte string at the start of the value, and in JSON it is the first member;
- the record timestamp, for end-to-end latency, which is the producer's send time carried through `_dimster_ts`;
- the `dimster-flink-emit` header, for the [latency split](#latency-through-flink) at the Flink job.

This is why `_dimster` must stay first and unchanged, and why the consumers only need to be told the format, which is the dataset's. The one place the schema would be needed is `printContents`: consumed JSON prints as it is, but consumed Avro prints only its type and size, because the consumers can't decode it with the dataset's schema.

## Latency through Flink

End-to-end latency through Flink covers the input topic, the Flink job, Flink's writes to the output topic and the consumers' reads of it. To show where the time goes, Dimster also reports it in two parts:

- **Producer to Flink**: from the producer's send to the Flink job emitting the output record. This covers the input topic, the Flink source and the query.
- **Flink to consumer**: from the Flink job emitting the record to a Dimster consumer starting to process it. This covers the Flink Kafka sink (batching, acknowledgements and, with `EXACTLY_ONCE`, the wait for the checkpoint that commits the transaction), the output topic and the consumer.

To measure the split, the runner adds a function to the projection that feeds the sink. It reads the clock for every row and writes the result as the `dimster-flink-emit` record header: 8 bytes, big-endian microseconds since the epoch. The record timestamp is still the producer's, so end-to-end latency is measured exactly as before. Consumers read the header from each record, which needs no knowledge of the output schema. The header adds about 30 bytes to each output record.

The two parts are recorded as separate histograms. Percentiles don't add up, so the p99 of the two parts doesn't sum to the end-to-end p99.

Like end-to-end latency, each part compares two hosts' clocks: the producer's and the TaskManager's for the first part, and the TaskManager's and the consumer's for the second. If the TaskManager's clock is ahead, the first part reads high and the second low by the same amount. End-to-end latency doesn't involve the TaskManager's clock. Values of zero or below are dropped, as for end-to-end latency.

In run mode, the result's `results` record both parts alongside end-to-end latency, with the same fields:

| Part | Per period | Over the measured phase |
|---|---|---|
| Producer to Flink | `producerToFlinkLatencyAvg`, `producerToFlinkLatency50pct`, ... `producerToFlinkLatency99pct`, ... `producerToFlinkLatencyMax` | `aggregatedProducerToFlinkLatency50pct`, ... `aggregatedProducerToFlinkLatencyMax`, `aggregatedProducerToFlinkLatencyQuantiles` |
| Flink to consumer | `flinkToConsumerLatency...` | `aggregatedFlinkToConsumerLatency...` |

The fields appear only for test points with a Flink stage. The run charts show both parts, the period log and aggregated results print them under end-to-end latency, and the **Dimster - Flink** Grafana dashboard shows them next to end-to-end latency (`client_producer_to_flink_latency` and `client_flink_to_consumer_latency` in Prometheus).

## Topics, jobs and test points

For each test point, Dimster:

1. creates the test topics and a `<topic>-out` topic for each, with `flink.outputPartitionsPerTopic` partitions (default: the input's partition count);
2. submits one Flink job, named `dimster-<suffix>`, with one pipeline per topic, and waits until it is running;
3. creates the consumers, on the `-out` topics, then the producers;
4. at the end of the test point, cancels the job before deleting the topics.

Flink jobs left by an interrupted run are cancelled at the start of the next test point, like leftover topics.

The readiness probe still runs. Probes are ordinary rows, though, so a filter may drop them: Dimster waits up to 30 seconds for the probes that pass and then continues.

## Delivery guarantees and checkpoints

`flink.deliveryGuarantee` sets the Kafka sink's guarantee:

- `AT_LEAST_ONCE` (the default) writes records as they arrive and flushes on checkpoints.
- `EXACTLY_ONCE` writes in Kafka transactions that commit on each checkpoint, so output becomes visible only at checkpoints. End-to-end latency then includes up to one checkpoint interval. Consumers default to `isolation.level: read_committed` (an explicit setting is kept). The sink's transaction timeout is set to 15 minutes, the broker's default maximum.
- `NONE` sends without waiting for acknowledgements on checkpoints.

`flink.checkpointIntervalMs` (default 10 s; 0 disables checkpointing) controls how often the job checkpoints. `EXACTLY_ONCE` requires checkpointing.

## Kafka configuration

The Flink job's Kafka source and sink are Kafka clients of their own, configured by the Flink connector. They take only part of the workload's [Kafka configuration](../reference/kafka-configs.md) (the `-k` file merged with `baseWorkload.kafkaConfig`):

| `kafkaConfig` field | Used by Flink | How |
|---|---|---|
| `commonConfig` | Yes | Passed to both the source and sink tables as `properties.*` options: `bootstrap.servers`, `security.protocol`, the `ssl.*` and `sasl.*` settings, timeouts and so on. Serializer and deserializer settings are left out, because the connector sets its own. |
| `replicationFactor`, `topicConfig` | Yes, through the topics | The `-out` topics are created with the input topics, so they get the same replication factor and topic configuration. |
| `producerConfig` | No | It configures Dimster's producers only. The Flink sink's producer uses the connector's defaults, `flink.sinkOptions` (such as `properties.linger.ms`, `properties.batch.size` or `properties.compression.type`) and `flink.deliveryGuarantee`, which replaces Dimster's `dimster.transactions` settings. |
| `classicConsumerConfig`, `shareConsumerConfig` | No | They configure Dimster's consumers only, which read the `-out` topics. The Flink source's consumer uses the connector's defaults and `flink.sourceOptions` (such as `properties.fetch.min.bytes` or `properties.max.poll.records`). |

So a workload that tunes the producers, for example with `linger.ms` or `compression.type` in `producerConfig`, doesn't tune Flink's writes to the `-out` topics. Set the same keys in `flink.sinkOptions`, prefixed with `properties.`, to tune those too.

Two defaults link the Flink job to the producer and consumer settings:

- With `flink.deliveryGuarantee: EXACTLY_ONCE`, Dimster's consumers default to `isolation.level: read_committed`, so that they only see Flink's committed output.
- With transactional producers (`dimster.transactions: true` in `producerConfig`), the Flink source defaults to `properties.isolation.level: read_committed`, as Dimster's consumers do without Flink. Otherwise the source would read at Kafka's default, `read_uncommitted`, and pass on records from open and aborted transactions.

An explicit setting is kept in both cases: `isolation.level` in the consumer config, or `properties.isolation.level` in `flink.sourceOptions`.

## Results

Each test point's result has a `flink` section with the SQL, job id, checkpoint interval, restart count and, in run mode, the selectivity: records consumed per record produced and consumer group over the measured phase. A pass-through query gives 1.0; a filter gives the fraction of rows it keeps. The result's `workloadConfig` also records `flinkParallelism`, `flinkDeliveryGuarantee` and `flinkOutputPartitionsPerTopic`, so they work as chart dimensions.

The consumed rate is the output rate. With a filter it is below the publish rate, which is expected.

## Drain-backlog mode

In drain-backlog mode the backlog builds on the input topics while no Flink job is running. When the backlog is built, the consumers start on the (still empty) output topics, then the Flink job starts. The drain clock starts when the job is running. The drain time is how long Flink takes to process the backlog plus how long the consumers take to read Flink's output.

Completion counts one output record per input record, so **drain-backlog needs a non-filtering query** (projections and transformations). With a filter, the expected count is never reached and the drain runs until `drainTimeoutMinutes`. This isn't validated.

## Limitations

- No aggregations or joins: one input topic per pipeline and at most one output row per input row. Flink rejects updating queries (such as a `GROUP BY`) when the job is submitted.
- Run and drain-backlog modes only. Explore and correctness reject a `flink` block.
- A generated dataset is required; the payload dataset can't be used.
- Only a Flink cluster deployed by Dimster is supported.
- The Flink version is fixed by the runner jar Dimster ships: Flink 2.2.1 with Kafka connector 5.0.0-2.2. A custom `flink.image` must be the same Flink minor version.
- `flink` can't be changed with `dimster run --modify`.
- With `printContents`, consumed JSON records print as they are. Consumed Avro records show only their type and size, because their schema comes from the SQL.
