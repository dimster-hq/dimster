# Live Workload Modification

Dimster `run` mode executes each test point with its yaml-defined parameters from start to finish as a fixed throughput workload. The `--modify` flag lets the user change those parameters mid-test, such as adjusting producer rate, changing message distribution, changing producer and/or consumer counts, changing a Kafka client config, or ending the test point early to gather the results.

This page covers what can be changed, how to do it, how to track changes in the coordinator output, how they appear in the final JSON result, and what happens when something goes wrong.

## What can be changed

A `dimster run <env> --modify ...` invocation does **not** start a new
benchmark. It POSTs a small JSON request to the running coordinator's
`/modify` endpoint. The coordinator delegates the request to the active run or
correctness engine. Correctness exposes only the consumer-restart action
described below.

There are three categories of change:

### 1. Field overrides with --set

Workload and Kafka config fields can be set using `--set key=value` syntax. Multiple key=value pairs can follow a single `--set` flag:

```bash
./dimster run localBeefy --modify --set producerRate=50000
./dimster run localBeefy --modify --set producersPerTopic=8 consumersPerGroup=4
./dimster run localBeefy --modify --set messageSize=2000 messageDistributor=PINNED_PARTITIONS
./dimster run localBeefy --modify --set consumerProcMsMin=50 consumerProcMsMax=100
```

Workload field names use the same camelCase as the YAML. Kafka config uses dotted paths prefixed with `kafkaConfig.`:

```bash
./dimster run localBeefy --modify --set kafkaConfig.producerConfig.batch.size=65536
./dimster run localBeefy --modify --set kafkaConfig.producerConfig.linger.ms=5
./dimster run localBeefy --modify --set kafkaConfig.classicConsumerConfig.max.poll.records=200
./dimster run localBeefy --modify --set kafkaConfig.shareConsumerConfig.share.acquire.mode=batch_optimized
./dimster run localBeefy --modify --set kafkaConfig.commonConfig.request.timeout.ms=60000
```

Workload and Kafka config can be mixed in a single `--set`:

```bash
./dimster run localBeefy --modify --set producerRate=100000 kafkaConfig.producerConfig.batch.size=65536
```

### 2. Action flags

Six flags trigger actions rather than parameter changes:

| Flag                      | Effect                                                                                                                                                                                                                                                                                                                                                                                                                                |
|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--terminate`             | Ends the current test point immediately as if its duration limit had been reached. Aggregate results are recorded and the run advances to the next test point (if any). Distinct from `dimster stop` which ends the entire run. Must be sent alone.                                                                                                                                                                                   |
| `--skip-phase`            | Ends the current ramp-up or warm-up phase early and proceeds straight to the next phase of the same test point — useful when the operator can see the cluster has already settled. In ramp-up: jumps directly to the full `producerRate` and starts warm-up. In warm-up: starts the recorded test phase. Rejected during the test phase (use `--terminate` to end the test point). The only flag accepted during ramp-up. Must be sent alone. |
| `--discard-stats`         | Wipes the per-period stats and cumulative histograms accumulated so far in the current test phase, and resets worker recorders. Effectively converts the elapsed test phase into additional warmup so subsequent measurement starts fresh. Test phase only (rejected during warmup) and cannot be combined with field overrides. Often paired with `--run-for-minutes` to also reset the remaining duration.                          |
| `--run-for-minutes <N>`   | Resets the test phase remaining duration to N minutes from the moment the engine processes the request. Stands alone (extends the current test phase) or pairs with `--discard-stats` (wipes stats AND gives a fresh N-minute window). Test phase only.                                                                                                                                                                               |
| `--restart-consumers <N>` | Restarts N consumers chosen uniformly without replacement from every consumer in the topology, or all consumers if N is at least the total. The total count and topology do not change. Useful for simulating consumer failures or testing resilience. Test phase only. Must be sent alone. |
| `--restart-producers <N>` | Restarts N producers chosen uniformly without replacement from every producer in the topology, or all producers if N is at least the total. The total count, worker placement, and pinning do not change. Test phase only. Must be sent alone. |

### Combining flags

Multiple field overrides can be combined with `--set`:

```bash
./dimster run localBeefy --modify --set producerRate=100000 producersPerTopic=8 kafkaConfig.producerConfig.batch.size=65536
```

Action flags (`--terminate`, `--skip-phase`, `--discard-stats`,
`--run-for-minutes`, `--restart-consumers`, `--restart-producers`) cannot be
combined with `--set` field overrides. The two restart flags must also be sent
in separate modify calls. The exception is `--discard-stats`, which can pair
with `--run-for-minutes`.

```bash
./dimster run localBeefy --modify --restart-consumers 10
./dimster run localBeefy --modify --restart-producers 5
```

### Correctness consumer restarts

Correctness mode accepts `--restart-consumers <N>` only while a `CONCURRENT`
test point is in its measured phase. Setup, the initial stabilization wait, the
grace period, test-point transitions, and all `DRAIN` phases reject it. All
other modify flags and field overrides remain run-mode only.

Classic correctness consumers use manual partition assignment by default,
which does not exercise the Kafka group protocol. Set dynamic assignment when
the restart is intended to cause a rebalance:

```yaml
correctnessConfig:
  testMode: CONCURRENT
  staticConsumerAssignment: false
  consumerRestartSeed: 0
```

The seed is reset at the beginning of every test point. Equivalent test points
that receive the same sequence of restart commands therefore select equivalent
consumer ordinals. Each accepted attempt is recorded in the correctness test
outcome's `consumerRestarts` array. A worker dispatch failure is recorded and
forces that correctness outcome to fail because the intended fault may have
been only partially applied.

### Scope: current test point only

Each `--modify` call applies only to the **current test point**. The next test point in the same run starts fresh from its yaml-defined values. So if you halve the producer rate at minute 3 of a 10-minute test point, the next test point still starts at the original rate. Interactive use is best suited to single-test-point campaigns.

## How to use it

To send a `--modify`, either:

- Press Ctrl+C in the original `dimster run` window to detach (the run keeps going on the coordinator); reattach later with `dimster reattach <env>`.
- Or just open a second terminal.

From the this terminal, issue `--modify` commands. They return immediately once the coordinator responds. The coordinator may reject commands if they do not meet certain criteria, such as a test point being in-progress.

### What's rejected

A modification is rejected (HTTP 409 from the coordinator, "ERROR: Modify rejected by coordinator" from the CLI) in any of these cases:

- The coordinator is not in `RUNNING` state (idle, stopping, failed).
- The current mode does not expose the requested action. Explore and
  drain-backlog cannot be modified. Correctness accepts only consumer restarts
  under the restrictions above.
- The run is between test points (during topic creation, worker setup, or teardown).
- The current test point is in its [ramp-up phase](../modes/run.md#runconfig-fields), AND the request is anything other than `--skip-phase` or `--terminate`. The ramp is mutating the publish rate in equal increments, so accepting a concurrent rate change would race with the next ramp tick. `--skip-phase` (ends the ramp and proceeds to warm-up) and `--terminate` (ends the whole test point) are the two exceptions because neither races with the ramp. All other modifications are accepted once warm-up begins.
- `--skip-phase` is sent during the test phase. The recorded measurement is already in flight, so skipping past it makes no sense; use `--terminate` to end the test point early.
- A request validation rule fails (negative `producerRate`, partial proc-ms, invalid combinations of action flags, etc.).
- The change is one of a small set that's still unsupported: changing `topics` count, `partitionsPerTopic`, `consumerGroupsPerTopic`, `consumerType`, `kafkaConfig.replicationFactor`, or `kafkaConfig.topicConfig.*`. Each rejects with a clear "not yet supported" message. Use standard multi-test-point test campaigns for these.

## How to track execution

When a `--modify` is accepted, the coordinator translates it into a sequence of discrete actions against the workers and executes them in order. 

For example, changing from 1 producer to 20:

```bash
./dimster run local --modify --set producersPerTopic=20
==> Sending modify to coordinator pod: benchmark-coordinator-5dbf8dc9dd-vgztn
{"workloadOverrides":{"producersPerTopic":20}}
{"status":"accepted","message":"1 actions applied"}
```

In the coordinator we see the following output:

```
MODIFY action: add-producers (19) — producersPerTopic 1 -> 20
MODIFY complete (1 actions applied)
```

Other changes can be more disruptive. For example, changing the MessageDistributor of our 20 producers from NO_KEY to PINNED_PARTITIONS, which involves the coordinator assigning partitions to producers and having them publish directly to partitions instead of using the default partitioner. This requires multiple steps, including stopping the producers and starting them again with their new configuration:

```
MODIFY action: push-kafka-config (producerConfig changed)
MODIFY action: remove-producers (20) — messageDistributor NO_KEY -> PINNED_PARTITIONS 
MODIFY action: create-producers (20) — messageDistributor NO_KEY -> PINNED_PARTITIONS
MODIFY action: start-load (rate=50000.0, distributor=PINNED_PARTITIONS) — messageDistributor NO_KEY -> PINNED_PARTITIONS
MODIFY complete (4 actions applied)
```

## How it appears in the final result JSON

Every accepted modification is appended to a per-test-point list and written into the final result JSON under each test entry as a `modifications` array:

```json
{
  "label": "rate=10000",
  "results": { ... },
  "modifications": [
    {
      "timestamp": "2026-05-14-09-47-31",
      "workloadOverrides": { "producerRate": 50000.0 },
      "actions": [ "adjust-publish-rate to 50000.0" ]
    },
    {
      "timestamp": "2026-05-14-09-48-12",
      "workloadOverrides": { "messageDistributor": "PINNED_PARTITIONS", "producersPerTopic": 8 },
      "actions": [
        "remove-producers (4) — messageDistributor NO_KEY -> PINNED_PARTITIONS",
        "create-producers (8) — messageDistributor NO_KEY -> PINNED_PARTITIONS",
        "start-load (rate=50000.0, distributor=PINNED_PARTITIONS, skew=1.0) — messageDistributor NO_KEY -> PINNED_PARTITIONS"
      ]
    },
    {
      "timestamp": "2026-05-14-09-49-05",
      "kafkaConfigOverrides": { "producerConfig.batch.size": 65536 },
      "actions": [
        "push-kafka-config (producerConfig changed)",
        "restart-producers (8) — producerConfig changed"
      ]
    },
    {
      "timestamp": "2026-05-14-09-50-30",
      "discardStats": true,
      "runForMinutes": 30
    },
    {
      "timestamp": "2026-05-14-10-00-00",
      "restartConsumersCount": 2,
      "restartedConsumerNames": [
        "consumer-dimster-local-abcd-0000-dimster-local-grp-000-efgh-i1",
        "consumer-dimster-local-abcd-0000-dimster-local-grp-000-efgh-i4"
      ],
      "actions": [
        "restart-consumers (2) — --restart-consumers 2 (restarting 2 of 8 consumers)"
      ]
    },
    {
      "timestamp": "2026-05-14-10-12-00",
      "terminate": true
    }
  ]
}
```

Each entry captures **what was requested** (the override maps and/or action
flags) and **what the engine did about it** (the `actions` list — the same
human-readable strings that landed in the coordinator log). Random restart
entries also include `restartedConsumerNames` or `restartedProducerNames`, so
the selected stable client identities can be correlated with coordinator and
worker logs. Test points that ran untouched do not get a `modifications` field
at all (it's omitted when empty), so the result JSON for an unmodified run is
byte-identical to its pre-feature shape — older tooling stays happy.

The recorded list is "what was actually applied" — modifications that were rejected outright don't appear. Modifications that **partially applied** before encountering a worker error appear with whatever actions succeeded plus a recovery action (see below) so the JSON tells the full story of what happened.

## When something goes wrong

If a worker action fails partway through a multi-action `--modify` (say, a config change that the Kafka client rejects, or a transient broker error), the coordinator does **not** leave the cluster in a half-applied state. Instead it issues a **force reset** that restores the workers to the last known good test point and topology — the state they were in before the failed `--modify` started.

The force reset:

1. Stops everything (producers and consumers), stats wiped.
2. Re-pushes the previous Kafka config.
3. Re-creates consumers and producers from the previous topology.
4. Re-starts the producer load with the previous rate, distributor, and payloads.

Once recovery completes, the workers are back to the last working configuration and you can investigate what went wrong before trying again.

The CLI surfaces the failure with a message that names what was attempted and what was applied:

```
ERROR: Modify rejected by coordinator
{"error":"2 of 3 actions applied; action 'restart-producers (1) — producerConfig changed' failed: ... — force-reset to previous state succeeded"}
```

The result JSON's `modifications` array also captures the failure: the partial-success actions appear in the `actions` list followed by the recovery note, so the timeline is fully traceable after the run.

If the recovery itself fails (rare — e.g. the cluster is genuinely down), the failure message says so and you'll need to `dimster stop` and restart from a fresh deploy.

A side effect of the force reset: `stopAll` resets accumulated worker stats, so the sample-tick stats immediately after recovery will dip to zero before climbing back up. Legitimate cost of unwinding a bad change.

## Workflow: interactive throughput discovery

Dimster `explore` mode is the right tool when you want a precise, automated, repeatable measurement of the maximum sustainable throughput under a latency SLO. However, its three-phase ramp/search/sustain algorithm takes time. When you just want a rough sense of "where does this break", or you want to feel out the latency knee on a configuration you've never benchmarked before, run mode + `--modify` is significantly faster (though manual).

The pattern:

1. Author a single-test-point workload with a long `testDurationMinutes` (e.g. 60 minutes) and `warmupDurationMinutes: 0`. Set `producerRate` to a deliberately low starting value, something you're confident the cluster can handle, e.g. 10% of an order-of-magnitude estimate.
2. Start the run as usual: `dimster run localBeefy -w workload.yaml`. Watch the coordinator log.
3. Once the test starts producing stable stats, ramp the rate up in steps from a second terminal:
   ```bash
   ./dimster run localBeefy --modify --set producerRate=50000
   # wait ~30-60s, watch latency in the original window
   ./dimster run localBeefy --modify --set producerRate=100000
   # wait ~30s-60s
   ./dimster run localBeefy --modify --set producerRate=200000
   ```
4. When you see latency degrade or back up (publish delay rising, end-to-end latency spiking), back off:
   ```bash
   ./dimster run localBeefy --modify --set producerRate=150000
   ```
5. Once you've located a rough knee, you can start measuring real stats by discarding the accumulated stats so far and setting a new running time: 
   ```bash
   ./dimster run localBeefy --modify --discard-stats --run-for-minutes 30
   ```

Alternatively you can use the result in another test campaign and terminate the test point early to gather the results.

```bash
./dimster run localBeefy --modify --terminate
```

The result JSON's `modifications` array gives you the exact rate ladder and timestamps so you can correlate against the time-series chart.

## Workflow: locking in a measurement window after experimentation

After ramping the rate or trying different distributor / batch-size combinations, the accumulated stats include a lot of noise from the ramp-up period. To get a clean measurement of whatever final configuration you settled on, use `--discard-stats` to wipe the noise and `--run-for-minutes` to set a fresh measurement window:

```bash
# After 10 minutes of ramping and tweaking, the right config is producerRate=80000.
# Discard the noisy ramp-up and measure cleanly for 30 minutes:
./dimster run localBeefy --modify --discard-stats --run-for-minutes 30
```

The 30 minutes of clean measurement become the headline numbers for this test point.

## Workflow: simulating bursty traffic

Setting `producerRate=0` pauses producers; a subsequent non-zero value resumes them. So a single test point can simulate a burst-quiet-burst pattern that no fixed-rate workload could produce:

```bash
# At minute 2 of the test phase, stop the producers
./dimster run localBeefy --modify --set producerRate=0
# At minute 4, restart at full rate
./dimster run localBeefy --modify --set producerRate=100000
# At minute 6, stop again
./dimster run localBeefy --modify --set producerRate=0
```

Useful for watching how the consumer side drains a backlog after a producer outage, or for measuring how quickly broker request latency recovers when load drops to zero. The drain shape ends up captured in the test point's regular periodic stats — no extra plumbing needed.

## Workflow: probing consumer-side back-pressure

Increase the consumer processing time mid-test to simulate slower downstream consumers and watch the backlog build:

```bash
# Healthy consumers
# (test point starts with consumerProcMsMin=0, consumerProcMsMax=0)

# Inject a slow consumer simulation
./dimster run localBeefy --modify --set consumerProcMsMin=50 consumerProcMsMax=100

# After a few minutes, recover
./dimster run localBeefy --modify --set consumerProcMsMin=0 consumerProcMsMax=0
```

The consumer simulation runs per-record sleeps inside the consumer loop on the worker, so it directly reduces consumer throughput. Combined with a fixed `producerRate`, this lets you measure how much backlog builds at a given producer/consumer mismatch.

## Workflow: terminating early

`--terminate` is independent of the throughput flags and useful in a few situations:

- **You've seen enough.** A 30-minute test point reaches steady state in 5 minutes and the rest is redundant. `--terminate` records the aggregate results and moves to the next test point.
- **Skipping past a known-bad configuration in a multi-test-point sweep.** If a test point isn't going to produce useful data, `--terminate` lets you quickly walk past it without killing the whole campaign (which is what `dimster stop` would do).
- **Quick interactive iteration.** Combined with the rate-ladder workflow above, `--terminate` lets you collapse the "wait for testDurationMinutes to elapse" delay between exploratory runs.

`--terminate` is **not** the same as `dimster stop`. `dimster stop` ends the entire run; `--terminate` ends only the current test point and lets the run continue.

## Limitations

- **Reliability.** This mode is inherently more complex than fixed configuration benchmarks as the coordinator has to compute a new topology, perform a diff between the new and current topology, figure out what changes to make on the fly. Failures can occur and recovery needed. This is designed for exploratory work and may not be as reliable as the standard benchmarks which consist of N fixed config test points. If you want to explore a set of different configurations, the usual multi-test-point test campaign is recommended. If you want to kick the tyres, do exploratory, interactive work, use the `--modify` commands with `run` mode.
- **Restricted correctness support.** Correctness supports only seeded consumer
  restarts during the measured `CONCURRENT` phase with dynamic assignment.
  Explore and Drain-Backlog do not register a modification target.
- **Current test point only.** Modifications never persist to the next test point. If you want a permanent change, edit the workload yaml.
- **Some changes are still unsupported.** Topic count, partition count, consumer group count, consumer type, replication factor, and topic config can't yet be changed live.
- **No undo.** A modification is applied immediately. If you make a mistake, send another `--modify` with the desired value (or rely on the recovery path if the mistake also caused a worker failure).
- **Coarse timestamps.** The `modifications` log uses second-precision timestamps. Modifications applied within the same second land with identical timestamps.

## See also

- [Run Mode](../modes/run.md) — the standard fixed-rate benchmark
- [Explore Limits](../modes/explore-limits.md) — automated maximum-throughput discovery
- [CLI Reference: run](../reference/cli.md#run) — every `--modify` flag in one table
