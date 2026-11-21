# Run Mode

The standard fixed-rate benchmark. Each **test point** runs producers and consumers at a fixed message rate for a fixed duration, measuring throughput and end-to-end latency. Optional ramp-up and warmup phases run first, during which latency stats are discarded so that JVM warmup, cache priming, and cold-cluster transients do not skew results.

For reliable benchmarks, both a ramp-up and warm-up phase is recommended. For stressful benchmarks, near the performance limits, the warm-up should exceed 20 minutes. Apache Kafka performance settles in over time, getting better with time. Short benchmarks get polluted with latency spikes that do not occur once the cluster has settled in.

## CLI Usage

```bash
dimster run <env> -k <kafka-config> -w <workload>
```

- `-k` / `--kafka-config` -- Path to Kafka config YAML. Optional if the workload embeds its own `kafkaConfig` section.
- `-w` / `--workload` -- Path to workload YAML.

Worker assignment (mixed vs. separated, producer/consumer ratio or explicit counts) and the `system` label are configured in the environment under `workers.assignment` and `system` respectively. See the [dimster-config.yaml reference](../reference/dimster-config.md).

Multiple test campaign workloads can be run back-to-back in the order specified:

```bash
dimster run aws \
  -k run/kafka-configs/kafka_no-sync_rf-3_minisr-2_acks-all_linger-1ms.yaml \
  -w run/workloads/workload-1.yaml \
  -w run/workloads/workload-2.yaml
```

## Test Campaign Lifecycle

A test campaign consists of 1 or more test points (N scenarios with M test points each). Each test point goes through the following stages:

1. **Topic creation** -- Test topics and consumer groups left under the env's [topic tag](../deployment/10-shared-kafka.md#topic-tags-each-env-owns-its-topics) (`dimster-<tag>-…`) are deleted, then the coordinator creates the topics according to the workload's `baseWorkload` settings. Other envs' topics on a shared cluster are left alone.
2. **Worker setup** -- Producer and consumer instances are created on the workers.
3. **Ramp-up phase** (optional) -- When `rampIntervals` and `rampIntervalSeconds` are both `> 0`, the producer rate is stepped up in equal increments from `producerRate / rampIntervals` to the full `producerRate`. Each step is held for `rampIntervalSeconds` and stats collected during the ramp are discarded. Enabled by default with 3 intervals of 20 seconds. Useful for high-rate runs where slamming a cold cluster with full load produces a spurious latency spike that doesn't represent steady-state behaviour.
4. **Warmup phase** -- Producers run at the configured `producerRate` but all stats collected during this phase are discarded.
5. **Test phase** -- Producers continue to run at the workload's `producerRate`. Stats are collected at `sampleRateMillis` intervals, including throughput, publish latency, schedule latency (time for the producer to accept the async send -- a back-pressure signal), end-to-end latency percentiles, and end-to-end delivery latency percentiles (measured at poll return, before per-record processing).
6. **Results written** -- JSON results are written to the coordinator's PVC on the cluster.
7. **Checkpoint written** -- A checkpoint is written.

At the end of the campaign the results and logs are downloaded and charts generated.

## runConfig Fields

| Field | Default | Description |
|-------|---------|-------------|
| `testDurationMinutes` | 5       | Duration of the test phase |
| `warmupDurationMinutes` | 1       | Warmup duration (stats discarded) |
| `rampIntervals` | 3       | Number of equal increments in the ramp-up phase. `0` (with `rampIntervalSeconds`) disables ramp-up. |
| `rampIntervalSeconds` | 20      | Duration of each ramp-up increment in seconds. `0` disables ramp-up. |
| `sampleRateMillis` | 10000   | Stats sampling interval in milliseconds |

**Ramp-up example.** With `producerRate: 100000`, `rampIntervals: 10`, and `rampIntervalSeconds: 30`, the ramp lasts 5 minutes: producers send at 10000 msg/s for 30 seconds, then 20000 for 30 seconds, and so on, hitting the full 100000 msg/s in the final interval. Warmup then begins at the full rate. Live `--modify` is rejected during ramp-up since the ramp itself is mutating the publish rate; modifications are accepted again once warmup begins.

## Workload Example

```yaml
name: 100k throughput test

runConfig:
  testDurationMinutes: 10
  warmupDurationMinutes: 2

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageSize: 1000
  producersPerTopic: 1
  producerRate: 100000
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
```

`runConfig` fields can also be swept per-test-point via a `runConfig:` block inside a scenario (mirrors `drainBacklogConfig` and `exploreConfig`). All five fields are sweepable: `testDurationMinutes`, `warmupDurationMinutes`, `sampleRateMillis`, `rampIntervals`, `rampIntervalSeconds`. See [Sweeping runConfig Fields](../reference/workloads.md#sweeping-runconfig-fields) in the workload reference.

## Availability Checking

Run mode optionally supports availability monitoring. Add an `availabilityConfig` section to the workload to enable per-partition availability probes during the test phase. See [Availability](availability.md) for configuration details.

## Flink SQL Stage

A workload with a `baseWorkload.flink` block puts a Flink SQL job between producers and consumers: consumers read the job's output topics, so end-to-end latency includes Flink. With a filtering query the consume rate is below the publish rate; the result's `flink.selectivity` records the ratio. See [Flink SQL Stage](../concepts/flink.md).

## Live Modification

A running test point can be modified from a separate terminal using `dimster run <env> --modify ...`. This lets you change `producerRate` or the consumer simulated processing time mid-flight, end the current ramp-up or warm-up phase early with `--skip-phase` (proceeds to the next phase without discarding the test point), or end the whole test point early with `--terminate`. Each accepted modification is recorded into the test entry's `modifications` array in the result JSON. See [Live Workload Modification](../concepts/live-modification.md) for the full reference and the workflow patterns it enables (interactive throughput discovery, bursty traffic simulation, consumer back-pressure probing).

## See Also

See [Workload Reference](../reference/workloads.md) for `baseWorkload` fields and scenario mechanics.
