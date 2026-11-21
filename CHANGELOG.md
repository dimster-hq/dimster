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

## [0.6.0]

### Added

- Flink SQL stage: a `baseWorkload.flink` block puts an Apache Flink SQL job (filters, projections and transformations) between producers and consumers in run and drain-backlog modes. The workload gives a `SELECT` over a `source` table derived from the dataset; Dimster creates the Kafka source and sink tables, derives the output schema from the query, and keeps the record header and producer timestamp so consumers measure end-to-end latency through Flink. Scenarios sweep `flink.parallelism` and other fields. Results record the job, restarts and selectivity. See docs/concepts/flink.md.
- A `flink` environment block deploys a Flink session cluster (JobManager and TaskManagers) with `dimster deploy`; `--only flink`, `dimster logs <env> flink`, saved Flink logs in results, a Flink UI port-forward in `dimster dashboard`, and a "Dimster - Flink" Grafana dashboard.

### Changed

- Saved dashboard HTML pages are skipped for dashboards with no data in the run window (for example the Kafka dashboard with external Kafka).

### Fixed

- `dimster deploy` now restarts Prometheus when its scrape configuration changes; previously a changed configuration was only picked up after a manual restart.

## [Unreleased]

### Added

- Workload hooks: `engineConfig.hooks` runs scripts at `beforeWorkload`, `afterWorkload`, `beforeTestPoint`, `afterTestPoint` and (drain-backlog) `beforeDrain`. The CLI ships the referenced scripts in the run request, and the coordinator runs them as local processes with a timeout and a failure policy. Every execution is recorded under `metadata.hooks` in the result JSON. On Kubernetes, scripts run in the coordinator pod with `kubectl` and a namespace-scoped ServiceAccount. See docs/concepts/hooks.md.
- Reference hook scripts in `run/hooks/`: `evict-topic-cache.sh` (unprivileged per-topic page-cache eviction on every broker), `rolling-restart-brokers.sh` and `drop-node-caches.sh`.
- Drain-backlog `reuseBacklog`: consecutive test points that differ only in drain-side settings drain one backlog instead of each building their own. Backlogs are built outside the test points, before the first one that needs each, so every recorded test point only drains and is measured the same way. Each build's time is reported once, on the backlog's first recorded drain, and later drains name it in `reusedBacklogFrom`. The coordinator checks the backlog is intact before reusing it, and it survives `dimster resume`. Before a drain that follows a `beforeDrain` hook or reuses a backlog, the coordinator elects preferred leaders and waits for a full ISR (`settleTimeoutMinutes`).
- Drain-backlog `reuseTopic`: backlogs are kept on the cluster across workloads and runs. Each lives in topics named from a hash of its build settings (`dimst-no-delete-<hash>-NNNN`), outside the `dimster` prefix, so Dimster never deletes them. Any test point, in any workload or run, with the same build settings drains the kept topics instead of building. Kept topics that can't be drained as-is (most often an interrupted build) are deleted and rebuilt. Results record `backlogTopics`, and `reusedTopic: true` for a backlog found on the cluster. Deleting kept topics is up to the user. Not supported with transactional producers.
- Drain-backlog `warmup`: runs the workload's first test point once before it, as a test point 0 that is in no result (unrecorded, without hooks, outside every scenario's window), so the first recorded drain does not run on freshly started JVMs. Workload-level only. With `reuseBacklog`, the first test point drains the warm-up's backlog again and its result reports `drainedByWarmUp: true`.
- Results record the bytes produced and consumed over each test point's samples as `bytesSent`/`bytesReceived` beside the per-sample byte rates.
- Drain-backlog results record `drainStartTime`/`drainEndTime`. The end-of-run metrics export measures each drain's disk reads in Prometheus and saves them to `drain-disk-reads.json`. The drain charts show them, with the disk read fraction, to tell whether a drain was served from disk or from the page cache. Drain charts also mark reused test points.
- System dashboard: a Disk Read Ratio panel, the bytes read from disk divided by the log bytes the brokers served (to consumers and followers). Near 1 means fetches are served from disk, near 0 from the page cache.
- Flink `sourceParallelism` and `sinkParallelism`: set (and sweep) the Kafka source's and sink's parallelism separately from the job's `parallelism`, for example one source subtask for a single-partition topic fanned out to four sink subtasks and a four-partition output topic. Both default to `parallelism`; the job waits for as many slots as the highest of the three, and results record them as `flinkSourceParallelism`/`flinkSinkParallelism`. Example: `run/workloads/flink-test/quick-flink-sink-fanout.yaml`.
- Flink latency split: the Flink job stamps each output record with its emit time (the `dimster-flink-emit` header), and consumers report end-to-end latency through Flink in two parts, producer to Flink and Flink to consumer. End-to-end latency is unchanged. Run results record `producerToFlinkLatency*`/`flinkToConsumerLatency*` and their `aggregated*` summaries for Flink test points; the run charts, period log, Prometheus metrics and the Flink Grafana dashboard show them. See docs/concepts/flink.md#latency-through-flink.
- Flink `outputColumns`: `DIMSTER_ONLY` makes the Flink sink write only Dimster's fields (`_dimster`, the key, the timestamp and the emit-time header), not the query's columns. This measures producer-to-Flink latency without the cost of writing full records. The default is `ALL`. It can be swept, and results record it as `flinkOutputColumns`. Example: `run/workloads/flink-test/quick-flink-dimster-only.yaml`.

- Hardware resources report: for each test point of a run or drain-backlog run, the CPU, disk bytes and IOPS, and network used by the brokers, Flink and the workers. It also shows CPU cost per GB produced and consumed, a page cache vs disk read estimate, disk reads per byte consumed, and flags for saturated CPU, disk and network. The metrics export measures each test point's window and saves `resource-metrics.json`. The chart step writes `charts/<result>-resources.html` and `-resources.csv`, `-resources-nodes.csv` and `-resources-timeseries.csv` beside the result. Run results now record each test point's measured window as `measureStartTime`/`measureEndTime`. Each test point also gets its own over-time charts, from a finer series around its window. `dimster compare` of run or drain-backlog results with resource data also writes `compare-<labels>-resources.html` and `.csv`, which set the entries' summary, page cache vs disk and bottleneck rows side by side. `resource-metrics.json` holds counter totals over each window, and the page cache section shows those totals and the consumers' `bytesReceived` rather than averaged rates. See docs/concepts/results-and-charts.md#resources-report.
- `monitoring.cgroupMetrics`: Prometheus scrapes every kubelet's cAdvisor for per-container CPU, disk and network, so the resources report can split usage by service even on a single-machine cluster. It needs a ClusterRole on nodes, which `dimster deploy` creates and `dimster teardown` removes, so it is off by default. A new Containers Grafana dashboard shows these metrics per pod.

- `dimster compare` accepts drain-backlog results. The comparison shows each entry's drain table, then grouped bars for drain duration, consumer throughput during the drain and, where `drain-disk-reads.json` exists, the disk read fraction. Each test point also gets overlays of backlog messages and consume rate over time. With `--directory`, drain-backlog results are found by their `metadata.label`, as run results are.

- `dimster compare --name NAME` (`-n`) names the output files `compare-NAME.html`, `compare-NAME.csv` and `compare-NAME-resources.*`, in place of every label joined by `-vs-`. Without it, a comparison whose file names would exceed the 255-byte limit now fails before writing anything and suggests `--name`.

- `dimster compare --pivot` lays a comparison out by test point rather than by scenario: one section per test point, with the scenarios on the x-axis of the aggregate bars and a bar per entry, and time series with a line per scenario per entry (colour for the scenario, dash for the entry). Each scenario has its own colour; entries are told apart by dash, then past four entries by shade, and the chart legends draw each line's style. It writes `compare-NAME-pivot.html` and takes run and drain-backlog results whose scenarios share the same test points. `--scenarios` (`-s`) picks and orders the scenarios to include, as for `dimster pivot`.
- `dimster pivot` accepts drain-backlog results: per test point, drain duration, consumer throughput and disk read fraction bars across the scenarios, a drain table per scenario, and backlog and consume rate over time. Its page header now shows the same metadata as the compare page, and each scenario has its own colour (the previous palette paired each hue with a lighter shade of it, so two scenarios could both be blue).
- The resources report shows totals beside the averages: each service's CPU core-seconds, disk GB read and written, disk read and write operations, and network GB received and transmitted per test point, in a Totals table, as bar charts in each service's whole-run section, and as columns at the end of the resources CSV. The Nodes section and nodes CSV gain disk and network totals, and the `dimster compare` resources page and CSV gain the totals too. The report's charts are a fixed 460px tall and use the page's full width, and multi-scenario x labels put the scenario and test label on two lines, so long labels no longer squash the bars. The report, and the compare resources page, warn when the pods on a node read and wrote over 1.5x what the node's whole disks did, the sign of a `resource-metrics.json` exported before per-pod disk I/O was limited to whole disks.

- Client dashboard: a Bytes Received panel beside Bytes Sent, and the panels above Consumer Details are grouped into a Throughput row (message and byte rates, totals, errors, client counts) and a Latencies row (every latency panel). Saved dashboard HTML now shows each row's title as a heading over its panels.

- Broker dashboard: a Broker Saturation section, right after Throughput, for saturation of every kind with the network processors first. Network processor idle time for the average and for the hottest processor (connections are pinned to one processor, so one can saturate alone). Processor loop busy time, a proxy for how long requests sit unread before a processor reads them, which no Kafka request timing covers. Where the processors' busy time goes (socket I/O, or the rest, which includes blocking on a full request queue). Response queue and send time, request queue time, acceptor blocked time and CPU throttling (with `monitoring.cgroupMetrics`). And Produce Latency Outside the Broker: the producers' request latency to each broker minus the broker's own Produce time, which rises when requests back up before the broker reads them. Request queue time stands in for Kafka's `RequestQueueSize`, which with broker and controller in one process reports the controller's queue.
- `deploy: external-env`: an env that runs its workloads against another env's Kafka cluster, named in a new `externalEnv.environment` block, so several envs can run workloads at the same time against one cluster and show how they interact. It deploys like `deploy: external` (workers, coordinator and monitoring, no Kafka) into its own namespace. At run time it uses the in-cluster brokers of a `full` env, waiting for them to be ready, or the `connection` of an `external` env. `dimster inspect`, `dimster kafka`, `dimster logs <env> kafka` and `dimster download-kafka-logs` reach the hosting env's brokers, and tearing down a hosting env's Kafka lists the envs using it. `dimster init` adds a commented `exampleLocalShared` env. See docs/deployment/10-shared-kafka.md.

### Changed

- The drain-backlog build identity (which decides whether `reuseBacklog` can drain the previous backlog again) ignores the connection settings in `commonConfig`: `bootstrap.servers`, `security.*`, `ssl.*` and `sasl.*`.
- Over a reused backlog, Flink output topics are named `dimster-<suffix>-NNNN-out` rather than `<input>-out-<suffix>`.
- `dimster deploy` keeps Kafka's data PVCs when only settings that need a restart change: broker config, heap, `kafka.env`, CPU and memory, `nodeLabel`, log config or TLS. It restarts every broker on its existing data, so topics, including drain-backlog kept topics, survive a redeploy with a new broker config. Changes that shape storage (`kafka.replicas`, `kafka.version`, `kafka.pvcSize`, `kafka.pvcStorageClass`, or broker config keys such as `log.dirs` and `node.id`) still delete the PVCs. The StatefulSet records the new `dimster.dev/kafka-storage-hash` annotation; Kafka deployed without it is deleted and deployed fresh the first time its spec changes.
- The broker dashboard's Network & Handler Saturation row is folded into the new Broker Saturation section: its network idle, request handler idle and memory pool panels moved there.
- A run's result directory is named by its start time plus a random 4-character suffix, e.g. `results/2026-11-21-17-15-06-k3x9/`, so runs started in the same second (such as envs sharing a Kafka cluster) no longer collide. The coordinator only accepts this shape.
- Every env's Kafka topics and consumer groups are named under its topic tag, `dimster-<tag>-…`, and a coordinator's test-point cleanup deletes only those, no longer every `dimster*` topic on the cluster. The tag defaults to the env name; the new `topicTag` env field overrides it (letters, digits, `.` and `_`). The availability probe topic is `dimster-<tag>-avail-probe`, Flink jobs and their groups are named under the tag too, and kept drain backlogs are `dimst-no-delete-<tag>-<hash>-NNNN`, so backlogs kept under the old names are no longer found. Embedded mode keeps the untagged names. See docs/deployment/10-shared-kafka.md.

### Fixed

- Drain-backlog `reuseBacklog` and `reuseTopic` no longer fail a workload when a few records land in a backlog after it is recorded, such as sends still in flight when the build's producers paused. Up to 0.05% of the backlog's records may be appended; the drain counts them as part of the backlog. A backlog that fails the check right before a test point drains no longer logs that it is building a new backlog, since the workload fails there.
- The drain-backlog docs and chart table treated `backlogProducerRate: -1` as "paused"; the default is 0, and any value of 0 or less pauses producers.
- With `monitoring.cgroupMetrics`, per-pod disk I/O (the resources report's disk MB/s, IOPS and page cache estimates, and the Containers dashboard) counted each operation once per block device layer. On a dm volume, such as an emulated network volume, that tripled it: cAdvisor reports the dm device, the disk under it and a series with no device. Per-pod disk I/O now counts whole disks only, as the node disk metrics do.
- The broker dashboard's Network Processor Avg Idle % always read 100%. With broker and controller in one process (Dimster's deployed Kafka), both register Kafka's `NetworkProcessorAvgIdlePercent` and the controller's, whose network threads are nearly always idle, is the one exported. The panel now charts the client listeners' network thread idle time from their selectors' io-wait counters (`kafka_server_socketservermetrics_io_wait_time_ns_total`, every listener but `CONTROLLER`), which a new JMX exporter rule names stably.
- The Request Handler Avg Idle % panel charts the broker's own handlers (`BrokerRequestHandlerAvgIdlePercent`, or the aggregate where Kafka lacks it) and the controller's, dropping the aggregate, which averages in the controller's nearly idle handlers. The Memory Pool panel, which shows the controller's pool in combined mode, and the FetchFollower panels, which also count the KRaft voters' fetches, say so.
- Request latency and size means on the broker dashboard (Produce, FetchConsumer, FetchFollower, ShareFetch and ShareAcknowledge times, request bytes, share partition metrics) covered the broker's whole lifetime, so they mostly reflected earlier runs on a long-lived cluster. They are now means over the last minute, from each histogram's mean and count. The max lines, also lifetime values that can't be windowed, are removed; p99 and p99.9 remain.
