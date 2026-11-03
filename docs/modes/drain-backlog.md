# Drain Backlog Mode

Measures consumer catch-up performance using a two-phase approach: first build a backlog of messages, then measure how quickly consumers can drain it. Optionally, set a produce rate during the backlog drain phase.

## CLI Usage

```bash
dimster drain-backlog <env> -k <kafka-config> -w <workload> [-s <system>]
```

- `-k` / `--kafka-config` -- Path to Kafka config YAML. Optional if the workload embeds its own `kafkaConfig`.
- `-w` / `--workload` -- Path to drain-backlog workload YAML.
- `-s` / `--system` -- Label for the system under test.

## Phases

### Phase 1 -- Build

Producers write messages until the accumulated backlog reaches `backlogSizeGB`. The producer rate during this phase is controlled by `buildRate` (-1 means use the workload's `producerRate`). Consumers are not started during this phase.

### Phase 2 -- Drain

Consumers start and begin draining the backlog, 10 seconds after the backlog is ready (and after any `beforeDrain` hooks and the wait for the cluster to settle). The pause lets the cluster quieten down so each drain starts from a comparable state; it is not part of the measured drain time. Producer behavior during drain is controlled by `backlogProducerRate`:

- `0` (default) -- Producers are paused, so the drain is pure consumer throughput.
- Any positive value -- Producers continue at the specified rate, simulating a system catching up while still receiving new traffic.

Stats are collected at `sampleRateMillis` intervals throughout the drain phase, tracking consumer throughput and progress.

### With a Flink SQL stage

With a `baseWorkload.flink` block, the backlog builds on the input topics with no Flink job running. At the drain phase the consumers start on the output topics, then the Flink job starts, and the drain time is measured from when the job runs: it covers Flink processing the backlog plus the consumers reading its output. The drain counts one output record per input record, so the query must not filter; a filter keeps the drain going until `drainTimeoutMinutes`. See [Flink SQL Stage](../concepts/flink.md#drain-backlog-mode).

## drainBacklogConfig Fields

| Field | Default | Description                                                               |
|-------|---------|---------------------------------------------------------------------------|
| `backlogSizeGB` | 1       | Size of backlog to build (GB)                                             |
| `buildRate` | -1      | Producer rate while building backlog (-1 = use workload's `producerRate`) |
| `backlogProducerRate` | 0       | Producer rate during drain (0 = pause producers)                          |
| `sampleRateMillis` | 10000   | Stats sampling interval in milliseconds                                   |
| `drainTimeoutMinutes` | 0       | Bound the time the test will run (0 unlimited)                            |
| `warmup` | false   | Run the workload's first test point once before it, unrecorded; see [Warm-up](#warm-up). Workload-level only: not settable in scenarios. |
| `reuseBacklog` | false   | Drain one backlog in several test points; see [Reusing a backlog](#reusing-a-backlog). Not sweepable. |
| `reuseTopic` | false   | Keep backlogs on the cluster and drain them in any later test point, workload or run with the same build settings; see [Keeping backlogs across workloads and runs](#keeping-backlogs-across-workloads-and-runs). Not sweepable. |
| `settleTimeoutMinutes` | 10      | How long to wait for every partition to have a full ISR and its preferred leader before a drain that follows a `beforeDrain` hook or reuses a backlog. Not sweepable. |

## Workload Example

```yaml
name: 50GB backlog drain

drainBacklogConfig:
  backlogSizeGB: 50
  buildRate: 200000
  backlogProducerRate: 0
  drainTimeoutMinutes: 20

baseWorkload:
  topics: 1
  partitionsPerTopic: 12
  messageSize: 1000
  producersPerTopic: 1
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 12
```

To test drain performance while producers continue at a reduced rate:

```yaml
name: 50GB backlog drain with ongoing production

drainBacklogConfig:
  backlogSizeGB: 50
  buildRate: 200000
  backlogProducerRate: 10000
  drainTimeoutMinutes: 20

baseWorkload:
  topics: 1
  partitionsPerTopic: 12
  messageSize: 1000
  producersPerTopic: 1
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 12
```

You can simulate processing time on the consumers using the `consumerProcMsMin` and `consumerProcMsMax` fields in the workload. For example, the following will limit each consumer to no more than 1000 msgs per second:

```yaml
name: 50GB backlog drain with 1ms processing time

drainBacklogConfig:
  backlogSizeGB: 50
  buildRate: 200000
  backlogProducerRate: 0
  drainTimeoutMinutes: 20

baseWorkload:
  topics: 1
  partitionsPerTopic: 12
  messageSize: 1000
  producersPerTopic: 1
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 12
  consumerProcMsMin: 1
  consumerProcMsMax: 1
```

## Warm-up

The first drain after a deploy runs on freshly started JVMs. The worker's consumer code and the brokers' read path have not yet been compiled by the JIT, so that drain can be much slower than the ones after it. With `warmup: true`, the workload's first test point is run once before it, as a test point 0 that is in no result: nothing is recorded, no [hooks](../concepts/hooks.md) run, and it lies outside every scenario's time window. The workload then runs as normal.

```yaml
drainBacklogConfig:
  backlogSizeGB: 5
  warmup: true

scenarios:
  - name: partitions
    partitionsPerTopic: [1, 6, 12]
  - name: more-partitions
    partitionsPerTopic: [24, 48]
```

The warm-up is a workload-level setting and runs once per run, before the first test point that run executes. `dimster resume` warms up again before the test point it resumes at, since a resumed run may follow a redeploy.

Without `reuseBacklog`, the warm-up builds and drains its own backlog, and the first test point then builds its own, so the first build is paid twice. With `reuseBacklog`, the warm-up's backlog is built outside it like any other (see [Reusing a backlog](#reusing-a-backlog)), and the first test point drains it again, so a warm-up costs one extra drain. That test point's result reports the build time and `drainedByWarmUp: true`.

## Reusing a backlog

Building a large backlog can take far longer than draining it, and many sweeps only change the consumers. With `reuseBacklog: true`, a test point drains the backlog of the previous test point again, instead of building its own, when the two differ only in drain-side settings:

- consumer settings:
  - `consumersPerGroup`
  - `consumerType`
  - `consumerProc*`
  - `classicConsumerConfig`
  - `shareConsumerConfig`
- the Flink stage (`flink`)
- `drainTimeoutMinutes` and `sampleRateMillis`
- scenario labels and repeats

Any other difference means the backlog is rebuilt. That covers topics, partitions, message size, dataset, producers, `producerConfig`, topic config, replication factor, `backlogSizeGB`, `buildRate` and `consumerGroupsPerTopic`. Those settings shape what is on disk, so changing them is a different backlog.

```yaml
name: reuse across consumer counts

engineConfig:
  hooks:
    beforeDrain:
      - script: evict-topic-cache.sh   # start every drain with a cold page cache

drainBacklogConfig:
  backlogSizeGB: 50
  buildRate: 200000
  reuseBacklog: true

baseWorkload:
  topics: 1
  partitionsPerTopic: 12
  messageSize: 1000
  producersPerTopic: 4
  consumerGroupsPerTopic: 1

scenarios:
  - name: consumers
    label: ["2", "4", "8", "12"]
    consumersPerGroup: [2, 4, 8, 12]
```

With `reuseBacklog`, backlogs are built outside the test points: before the first test point that needs each one, and before that test point's `beforeTestPoint` hooks. Every test point, including the first, then only drains, from offset 0 with fresh consumer groups, so every recorded test point is measured the same way and its time series cover the drain only. Here the backlog is built once, before the first test point. Builds fall inside the scenario's time window but in no test point's.

Each build is reported once, on the backlog's first recorded drain:

- `buildDurationMs` is the build's time on the backlog's first recorded drain, and 0 on later drains of it.
- `reusedBacklogFrom` is the index of the test point that first drained the backlog. It is absent on the first recorded drain itself.
- `drainedByWarmUp: true` marks a first recorded drain that the [warm-up](#warm-up) drained before.

Topics of a reusable backlog are created with unlimited retention (`retention.ms` and `retention.bytes` of -1), so a broker's retention cannot shrink them between drains.

Before reusing, the coordinator checks that the backlog is intact: its topics exist, nothing was deleted from the start of a partition, and nothing was appended. Otherwise it rebuilds, again outside the test point. The same check lets `dimster resume` carry on with a backlog from before the interruption.

### Requirements

- `backlogProducerRate` must be 0: producing during a drain would grow the backlog for every later test point.
- Classic consumers (without a Flink stage) need `auto.offset.reset: earliest` in `classicConsumerConfig`, or they would skip the backlog. The workload fails at start otherwise.

### Isolation between drains

Draining the same backlog again is not the same as draining a fresh one, because state carries over between test points:

- **The broker page cache** is the main one. After a drain, the most recently read segments are cached, so the next drain reads more from memory and looks faster. Results then drift with the order of the test points.
  - **Restarting brokers does not fix this.** The cache belongs to the node's kernel and outlives the broker process. On local NVMe volumes it outlives the pod too.
  - **Evict it explicitly** with the `evict-topic-cache.sh` [hook](../concepts/hooks.md) before every drain. "Every" includes the first drain, which otherwise starts with the hot tail the build left behind. The hook evicts every replica, not just leaders, since leadership can move.
  - **How much this matters** depends on how much of the backlog fits in the cache. A broker pod's page cache counts against its memory limit, so the cache is roughly the pod's memory limit minus the heap.
- **Broker JVM state** carries over too: the JIT, the heap, and mmapped indexes. Restart the brokers with `rolling-restart-brokers.sh` (and evict afterwards) if that matters for your comparison. After restarts, the coordinator elects preferred leaders and waits for a full ISR before draining (`settleTimeoutMinutes`).
- **Cloud burst credits** are time-based, not state-based. Back-to-back drains use up EC2 network, EBS-optimized instance and gp2 burst credits faster than drains separated by builds. Later test points can then slow down, especially with a cold cache, which reads everything from disk. Use `engineConfig.cooldownMinutes` to let credits recover, or a `repeat` to see whether the first and last test points agree.

Cold-cache drains of a reused backlog are a deliberately repeatable regime. They are not directly comparable with fresh-backlog results, whose drains start with the most recent part of the backlog still cached.

Use the drain charts' disk reads to check that eviction worked on your storage, rather than assuming it did. They are the bytes the nodes read from disk between each drain's `drainStartTime` and `drainEndTime`, and that as a fraction of the bytes drained. Near 1 means the drain was served from disk, near 0 from the page cache. The end-of-run metrics export measures them in Prometheus (node-exporter) and saves them to `drain-disk-reads.json` in the results directory. Disks of every scraped node are counted, so other disk readers on those nodes add to it. The System dashboard's Disk Read Ratio panel shows the same ratio over time: disk reads divided by the log bytes the brokers served to consumers and followers.


## Keeping backlogs across workloads and runs

`reuseBacklog` shares a backlog between consecutive test points of one workload. With `reuseTopic: true`, a backlog is kept on the cluster instead, and any test point with the same build settings drains it. That includes later test points that aren't adjacent, other workloads in the same run, and later runs. A multi-workload run then builds each distinct backlog once.

```yaml
name: drain-backlog reuse topic

drainBacklogConfig:
  backlogSizeGB: 5
  buildRate: 50000
  reuseTopic: true

baseWorkload:
  topics: 1
  partitionsPerTopic: 1
  messageSize: 1000
  consumerGroupsPerTopic: 1
  kafkaConfig:
    classicConsumerConfig:
      auto.offset.reset: earliest

scenarios:
  - name: no-compression
    kafkaConfig:
      producerConfig:
        compression.type: none
  - name: zstd
    kafkaConfig:
      producerConfig:
        compression.type: zstd
```

A kept backlog lives in topics named from a hash of its build settings: `dimst-no-delete-<hash>-0000`, `-0001` and so on. The build settings are the ones listed under [Reusing a backlog](#reusing-a-backlog) as forcing a rebuild. Equal settings always give the same name, and different settings (here, the two compression types) give different names, so a kept topic is never drained with the wrong data. Scenario names, labels and drain-side settings don't affect the name, and neither do the connection settings in `commonConfig` (`bootstrap.servers`, `security.*`, `ssl.*`, `sasl.*`).

Everything described under [Reusing a backlog](#reusing-a-backlog) applies too:
- builds happen outside the test points;
- topics get unlimited retention;
- the backlog is checked before each drain;
- the cluster settles before each drain;
- fresh consumer groups and Flink output topics are used per drain;
- the same [requirements](#requirements) apply.

`reuseTopic` also rejects transactional producers. A kept backlog's record count is read from its offsets, and transaction markers would inflate it.

Before building a backlog, the coordinator looks for its kept topics:

- **None exist**: it builds the backlog into them.
- **They hold a complete backlog**: all topics exist with the right partition count, nothing was deleted from the start of a partition, and they hold at least the records a build of `backlogSizeGB` produces. It drains them without building. The result reports `reusedTopic: true` and `buildDurationMs: 0`.
- **Otherwise**, most often a build that was interrupted (`dimster stop`, a failure): it logs why, deletes those topics and builds them again.

**Dimster never deletes kept topics otherwise**, not at the end of a test point, a workload or a run, and test-topic cleanup doesn't match their prefix. They take disk space until you delete them. Every result lists a test point's kept topics in `backlogTopics`. To delete all of them:

```bash
kafka-topics.sh --bootstrap-server <broker> --delete --topic 'dimst-no-delete-.*'
```

A Dimster upgrade that adds a build-side workload setting changes the hash, so the new version builds its own backlogs rather than draining the old ones.

## Results

The output includes:

- **Time to drain** -- Total elapsed time to consume the backlog.
- **Consumer throughput** -- Messages/sec and MB/s during the drain phase.
- **Drain rate over time** -- Time series showing how consumer throughput varies as the backlog shrinks.
- **Disk reads** -- Bytes read from disk during the drain, and as a share of the bytes drained, in the charts (measured by the end-of-run metrics export).
- **Drain window** -- `drainStartTime` and `drainEndTime`, for finding the drain in Grafana.
- **Byte totals** -- `performance.bytesSent` and `bytesReceived`, the encoded bytes produced and consumed over the test point's samples. Consumers only read during the drain, so `bytesReceived` is what the drain consumed.

## When to Use

- **Consumer throughput testing** -- Measure raw consumer read performance without producer contention.
- **Outage recovery** -- Evaluate how fast a system recovers from an outage-induced backlog. Simulate with `backlogProducerRate` to model ongoing traffic during catch-up.
- **Consumer configuration comparison** -- Compare fetch sizes, poll intervals, and other consumer settings by draining the same backlog with different configurations.
- **Partition scaling** -- Use scenarios to test drain performance across different partition counts.

> Note, when testing this in the cloud, take into account that some clouds offer burst network throughput which can run out. Consumers will drain the backlog as fast as they can go, potentially exceeding the instance baseline network bandwidth.

## Scenarios

Drain-backlog workloads support scenarios for sweeping parameters:

```yaml
scenarios:
  - name: partition-scaling
    label: ["1-part", "6-part", "12-part"]
    partitionsPerTopic: [1, 6, 12]
    consumersPerGroup: [1, 6, 12]
```

The drainBacklogConfig also supports this:

```yaml
scenarios:
  - name: backlog-scaling
    drainBacklogConfig:
      label: ["10-GB", "50-GB", "100-GB"]
      backlogSizeGB: [10, 50, 100]
```

## See Also

See [Workload Reference](../reference/workloads.md) for `baseWorkload` fields and scenario mechanics.
