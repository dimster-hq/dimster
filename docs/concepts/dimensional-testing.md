# Dimensional testing

Dimster's core design principle is making it easy to compare performance across multiple configurations in a single run. Instead of manually running separate benchmarks for each variation, you define all variations in one workload file.

All results are shareable assets:
- Detailed results in JSON and CSV format which includes:
    - producer/consumer throughput and various latencies
    - per-partition and per-consumer latencies
    - the client, broker, hardware configuration (for reproducibility)
- Configuration:
  - Broker version and configuration.
  - The workload, Kafka client configs.
- Charts as a single interactive html file
- Logs of all Kafka brokers, workers and the coordinator
- Grafana dashboards converted to HTML files
- Optionally, a Prometheus snapshot of the run to reopen in Grafana later, without the cluster

## The problem in one paragraph

Performance work is rarely a single point. You want to compare partition counts, consumer counts, message sizes, batch settings, throughput levels — often several at once. Doing that by hand means a tangle of shell scripts, inconsistent naming, results scattered across folders, and no sane way to chart them together. Dimensional testing solves all of that by letting **one workload file** describe **many runs** that share a base, get tagged consistently, and land in one results folder ready to chart side by side.

## The mental model

```
[CAMPAIGN]              ← one workload file = one campaign
 │
 ├─ [Base workload]     ← defaults inherited by every test point
 │   topics: 1
 │   partitionsPerTopic: 10
 │   producerRate: 50000
 │   ...
 │
 ├─ [Scenario A]        ← inherits the base, overrides selected fields
 │   ├─ TestPoint 0     ← (base ⊕ scenario @ array index 0)
 │   ├─ TestPoint 1
 │   └─ TestPoint 2
 │
 └─ [Scenario B]
     ├─ TestPoint 0
     └─ TestPoint 1
```

Four nouns, in priority order:

| Noun           | What it is                                                                                                | Plurality    |
|----------------|-----------------------------------------------------------------------------------------------------------|--------------|
| **Campaign**   | The whole thing. One YAML file, one results folder.                                                       | One per file |
| **Base workload** | Default values that every test point starts from.                                                      | One per campaign |
| **Scenario**   | An override layer on top of the base. Each scenario produces N test points by sweeping one or more arrays. | Zero or more per campaign |
| **Test point** | The fully resolved config for one actual benchmark run: `base ⊕ scenario @ array index i`.                | N per scenario |

Test points run **sequentially**, one at a time, and all test points from the entire campaign share a single results folder.

A campaign with no scenarios runs the base workload once as a single test point.

## The two override mechanisms

A scenario can override base-workload fields in two ways. The difference is what changes between test points.

### Scalar override — same value for every test point in the scenario

```yaml
scenarios:
  - name: high-throughput
    producerRate: 200000        # scalar
    #...
```

`producerRate` is a single value.

### Array override — one value per test point (the co-variance rule)

```yaml
scenarios:
  - name: scaling-consumers
    producerRate: 200000        # scalar (applies to all three test points of the scenario)
    label:              ["1-con", "10-con", "50-con"]
    consumersPerGroup:  [1,       10,       50]
    partitionsPerTopic: [1,       10,       50]
```

Every array inside one scenario **must have the same length**, and they **co-vary by index**. Test point 0 gets `consumersPerGroup=1, partitionsPerTopic=1, label="1-con"`. Test point 2 gets `consumersPerGroup=50, partitionsPerTopic=50, label="50-con"`.

> Mismatched array lengths within a scenario will refuse to start the campaign. That's deliberate — a length mismatch almost always means the author forgot to update one of the arrays.

### Overrides reach into nested fields too

Anything in the workload schema can be overridden, including nested Kafka client settings. Only the path you mention changes; the rest of the structure is inherited from the base:

```yaml
baseWorkload:
  kafkaConfig:
    producerConfig:
      acks: all
      linger.ms: 10

scenarios:
  - name: batch-size
    label: ["8k", "32k", "128k"]
    kafkaConfig:
      producerConfig:
        batch.size: [8192, 32768, 131072]   # acks and linger.ms still inherited
```

## Scenarios and test points mapped to charts

Charts are generated from the results, including throughputs and latencies. Each set of charts is duplicated for each scenario and the test points are series on each chart.

For example, the following test campaign has a single scenario with `consumersPerGroup` as a dimension of 6 test points.

```yaml
runConfig:
  testDurationMinutes: 4
  warmupDurationMinutes: 1

baseWorkload:
  topics: 1
  partitionsPerTopic: 10
  keyDistributor: "KEY_ROUND_ROBIN"
  messageSize: 500
  consumerType: SHARE_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 1
  producersPerTopic: 10
  producerRate: 100000
  kafkaConfig:
    replicationFactor: 3
    topicConfig:
      min.insync.replicas: 2
    producerConfig:
      acks: all
      enable.idempotence: true
      linger.ms: 0
    shareConsumerConfig:
      max.poll.records: 500
      share.acknowledgement.mode: implicit
      share.acquire.mode: batch_optimized
      dimster.commit.mode: Poll

scenarios:
  - name: 500k-msgs-defaults
    producerRate: 500000
    label: [ "10-con", "50-con", "100-con", "250-con", "500-con", "1000-con" ]
    consumersPerGroup: [10, 50, 100, 250, 500, 1000]
```

This produced the followed e2e latency distribution plot:

![e2e latency all](../images/e2e_latency_chart_all.png)

*Fig 1. End-to-end latencies of the four test points*

The e2e latency of 1000 consumers dominated. You can unselect a subset of series to see the remaining series better:

![e2e latency series subset](../images/e2e_latency_chart_series.png)

*Fig 2. End-to-end latencies with 1000 consumers unselected*

You can also select a portion of the x-axis to see a specific range in more detail:

![e2e latency zoomed](../images/e2e_latency_chart_zoomed.png)

*Fig 3. End-to-end latencies of the four test points, zoomed into a sub-range of latencies*

Charts and result rows identify each test point by its label. Two pieces compose the final label:

- `baseWorkload.label` — optional prefix shared by every test point in the campaign.
- `scenario.label` — array, one entry per test point in that scenario.

| Set                         | Final label per test point     |
|-----------------------------|--------------------------------|
| Only `scenario.label`       | `<scenario label>`             |
| Both                        | `<base label> - <scenario label>` |
| Only `baseWorkload.label`   | `<base label>` (probably not what you want — every point has the same label) |
| Neither                     | `unlabelled`                   |

Use the base label to mark something the *whole campaign* has in common (e.g. the cluster shape) and the scenario label to identify *which test point*.

## A worked example

Comparing CONSUMER_GROUP vs SHARE_GROUP consumers across three throughput levels:

```yaml
name: Consumer type comparison

runConfig:
  testDurationMinutes: 10
  warmupDurationMinutes: 2

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageSize: 1000
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
  producersPerTopic: 4

scenarios:
  - name: 50MBs
    producerRate: 50000              # scalar
    label:        ["classic",        "share"]
    consumerType: [CONSUMER_GROUP,   SHARE_GROUP]
  - name: 100MBs
    producerRate: 100000
    label:        ["classic",        "share"]
    consumerType: [CONSUMER_GROUP,   SHARE_GROUP]
  - name: 200MBs
    producerRate: 200000
    label:        ["classic",        "share"]
    consumerType: [CONSUMER_GROUP,   SHARE_GROUP]
```

Three scenarios × two array elements each = **6 test points**, executed in this order:

| # | Scenario | Idx | Label   | Rate    | Consumer Type |
|---|----------|-----|---------|---------|---------------|
| 1 | 50MBs    | 0   | classic | 50,000  | CONSUMER_GROUP       |
| 2 | 50MBs    | 1   | share   | 50,000  | SHARE_GROUP   |
| 3 | 100MBs   | 0   | classic | 100,000 | CONSUMER_GROUP       |
| 4 | 100MBs   | 1   | share   | 100,000 | SHARE_GROUP   |
| 5 | 200MBs   | 0   | classic | 200,000 | CONSUMER_GROUP       |
| 6 | 200MBs   | 1   | share   | 200,000 | SHARE_GROUP   |

Each test point runs for the full 10 minutes (after a 2-minute warmup), then results are collected and the next test point begins. The chart html file plots all six, grouped by scenario, classic/share as series/bars.

## Where to go next

- [Workload Reference](../reference/workloads.md) — every field you can set under `baseWorkload` or override in a scenario.
- [Getting started — page 08](../getting-started/08-dimensional-testing.md) — two minimal worked examples you can run on the `local` env.
- [The four modes](../getting-started/09-the-four-modes.md) — how `run`, `explore`, `drain-backlog`, and `correctness` each consume a workload file.
- [Compare command vs dimensions](comparing-campaigns.md) — what to do when the thing you want to vary lives in the cluster, not the workload.
