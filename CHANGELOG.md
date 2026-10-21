# Changelog

All notable user-visible changes to Dimster are recorded here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/) under the 0.x
relaxed rules described in [RELEASING.md](RELEASING.md).

While Dimster is on 0.x, **minor version bumps may include breaking changes**
— to the workload YAML schema, broker-config schema, CLI flags, K8s manifest
layout, or results JSON schema. Patch bumps are bug fixes and additive
non-breaking changes only.

## [Unreleased]

### Added

- Live modification can restart a random subset of producers with
  `--restart-producers <N>`.
- Correctness `CONCURRENT` tests can restart dynamically assigned consumers
  with deterministic per-test-point selection via `consumerRestartSeed`.
- Correctness results record cumulative loss, duplicate, out-of-order, and CRC
  counter arrays at the configured sample interval. Correctness charts and
  `dimster compare --directory` render complete duplicate, out-of-order, and
  CRC time series alongside aggregate violation bars. Message loss remains a
  final aggregate because intermediate missing-message counts represent lag.

### Changed

- `--restart-consumers <N>` now selects exactly N consumers uniformly from the
  full topology instead of filtering by per-group consumer index. Random
  producer and consumer selections are recorded in coordinator logs and result
  JSON.
- Correctness consumer restart attempts are recorded in each test outcome, and
  a failed worker dispatch invalidates that outcome.
- Latency histograms now use 4 significant digits (0.01% resolution) instead of 5. Each was 14-21 MB at 5 digits, so every stats sample copied about 100 MB and cost about a CPU-second; on CPU-limited workers this stretched sampling intervals and caused GC pauses. Reported percentiles change by at most 0.01%.

### Fixed

- Correctness workloads using static consumer assignment now serialize their
  topic-partition topology correctly when dispatching consumers to remote
  workers and when writing topology result files.
- Correctness violation entries retain their detection timestamps so future
  comparison timelines preserve temporal resolution.
- Packaged images omitted kafka-clients' runtime compression codecs (lz4, zstd, snappy), so any `compression.type` other than `none` failed with `NoClassDefFoundError` on Kubernetes.
- With no `image:` (or `kubernetes:`/`results:`) block in `dimster-config.yaml`, as written by `dimster init`, the CLI used an empty image tag and `dimster build` failed.

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
