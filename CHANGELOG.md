# Changelog

All notable user-visible changes to Dimster are recorded here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/) under the 0.x
relaxed rules described in [RELEASING.md](RELEASING.md).

While Dimster is on 0.x, **minor version bumps may include breaking changes**
— to the workload YAML schema, broker-config schema, CLI flags, K8s manifest
layout, or results JSON schema. Patch bumps are bug fixes and additive
non-breaking changes only.

## [0.1.0]

Initial public release.

## [0.1.1]

### Added

- Users can set environment variables in Kafka pods via the Dimster config.

### Changed

- Restarting producers and consumers as part of a --modify command now restarts them in parallel, rather than sequentially.

## [0.2.0]

### Changed

- Live-interaction: Changes to workload fields now done via the --set sub-command and workload fields use field=value format (breaking change).
- Live-interaction: Rejects unrecognized sub-commands and unsupported workload fields.

## [0.3.0]

### Added

- The consumerProcParallelism workload field to simulate parallel processing of records in consumers

## [0.3.1]

### Added

- BLOCKING and CONTINUOUS parallel processing

## [0.3.2]

### Added

- Producer rate schedule REPEATING and OSCILLATING

### Changed

- Improvements to simulated processing time

## [0.4.0]

### Added

- Infrastructure dimension support via extra metadata arguments and overridable Dimster envs.

## [0.5.0]

### Added

- Live modification can restart a random subset of producers with `--restart-producers <N>`.
- Correctness `CONCURRENT` tests can restart dynamically assigned consumers with deterministic per-test-point selection via `consumerRestartSeed`.
- Correctness results record cumulative loss, duplicate, out-of-order, and CRC counter arrays at the configured sample interval. Correctness charts and `dimster compare --directory` render complete duplicate, out-of-order, and CRC time series alongside aggregate violation bars. Message loss remains a final aggregate because intermediate missing-message counts represent lag.
- Generated datasets: the `dataset` workload field (and scenario dimension) makes producers send schema-encoded rows (Avro or JSON) from a preset (`clicks`, `orders`, `taxi`) or inline column definitions, with configurable cardinality, Zipf skew, null rates and clustering, and realistic names, addresses, emails, URLs and text. See docs/concepts/data-generation.md.
- `BYTES` dataset columns: opaque values with a configurable length, random fraction (`randomRatio`) and cardinality.
- The `KEY_COLUMN` message distributor, which keys records by a dataset column.
- `engineConfig.printContents`, which adds each record's key and readable contents to the printed message lines.
- Measured byte rates in results (`publishBytesRate`, `consumeBytesRate`, `avgRecordBytes`); charts and `dimster csv` use them when present.
- Prometheus snapshots: the `metricsOutput` env setting (and `--metrics-output` on `run`, `explore`, `correctness`, `drain-backlog`, `resume`, `reattach` and `results`) chooses which metrics to save after a run: `html` (the existing dashboard pages), `snapshot` (raw samples of every metric the dashboards use, for the run window only, in `prometheus-snapshot/`), both, or none. Defaults to `html`.
- `dimster dashboard --snapshot <result-dir>` opens a saved snapshot in a throwaway local Prometheus + Grafana in Docker, with no cluster needed.

### Changed

- `dimster results --dashboards` is replaced by `--metrics-output [OUTPUTS]`; `--dashboards` still works as an alias for `--metrics-output html`.
- `--restart-consumers <N>` now selects exactly N consumers uniformly from the full topology instead of filtering by per-group consumer index. Random producer and consumer selections are recorded in coordinator logs and result JSON.
- Correctness consumer restart attempts are recorded in each test outcome, and a failed worker dispatch invalidates that outcome.
- Every record now has a schema. Normal records are still exactly `messageSize` bytes but "dataset" based records depend on the schema, values and format.
- `printMessages` lines (`PRODUCED`, `PROCESSED`) are now logged at INFO rather than DEBUG, so they appear without a debug log config. `printContent` added to visualize text-renderable payloads.
- Latency histograms now use 4 significant digits (0.01% resolution) instead of 5 to reduce sizes with minimal impact on accuracy.

### Fixed

- Correctness workloads using static consumer assignment now serialize their topic-partition topology correctly when dispatching consumers to remote workers and when writing topology result files.
- Correctness violation entries retain their detection timestamps so future comparison timelines preserve temporal resolution.
- Packaged images omitted kafka-clients' runtime compression codecs (lz4, zstd, snappy), so any `compression.type` other than `none` failed with `NoClassDefFoundError` on Kubernetes.
- With no `image:` (or `kubernetes:`/`results:`) block in `dimster-config.yaml`, as written by `dimster init`, the CLI used an empty image tag and `dimster build` failed.
- The first period of each run, correctness and drain-backlog phase counted messages sent before the phase began (between the previous phase's last sample and this phase's start), inflating its rate. Most visible on slow clusters, where that gap spans seconds.