# Results and Charts

Dimster produces structured results from every benchmark run and can generate interactive charts for analysis.

All results are shareable assets:
- Detailed JSON results which include:
    - Per test-point throughput and various latencies
    - The effective workload and client configs that produced each test point result
    - Information on the hardware and software version
- The workload file(s), Kafka client config file (if used), the effective broker config, and the environment definition that selected the cluster topology and resource sizes — all captured as the run started so a results archive is self-describing
- Charts as a single interactive html file. Time periods can be selected and the legend is clickable to hide/show individual series (one series per test point)
- Logs of all Kafka brokers, workers and the coordinator
- Grafana dashboards converted to HTML files
- A hardware resources report: CPU, disk and network per service and test point
- Optionally, a Prometheus snapshot of the run that can be reopened in Grafana later, without the cluster

The idea is to have full traceability and the ability to easily reproduce a benchmark based on its results.

## Result Storage

After a benchmark run completes, the CLI downloads results (stored as JSON files on the coordinator's PVC), logs, dashboards and generate charts. The following assets are created in your local result dir (within a timestamped subdirectory (e.g., `results/2026-03-21-17-25-49/`):
- **JSON result file**: a single json result file with all throughput, latencies and all configuration data.
- **`configs/` sub-directory**: the inputs that produced the run, captured at run start so each result is self-describing:
    - `workload-<name>.yaml` — one file per workload submitted to the run (the original file, byte-for-byte).
    - `kafka.yaml` — the Kafka client config file passed via `-k`, if any.
    - `broker.yaml` — the **effective** broker config (the broker config file merged with any inline `kafka.extraConfig` overrides), so the captured file matches what the broker actually ran with.
    - `environment.yaml` — the environment block from `dimster-config.yaml` that selected the cluster topology, resource sizes, monitoring layout, and so on. The `image`, `context`, and `connection` fields are stripped so deployment artifacts and external-cluster credentials don't end up in shared archives.
- **Logs**: Coordinator, worker and Kafka logs are downloaded to the `logs` sub-directory.
- **Dashboards**: Grafana dashboards are translated into HTML files and written to `dashboards` sub-directory.
- **Prometheus snapshot** (opt-in): the Prometheus data behind the dashboards, written to the `prometheus-snapshot` sub-directory. See [Prometheus Snapshots](#prometheus-snapshots).
- **Charts**: A set of charts are generated based on the JSON result file.
- **Resources report**: per-service hardware usage for each test point. See [Resources Report](#resources-report).

## Repeated Scenario Results

When a scenario uses `repeat` greater than `1`, Dimster writes each repeated run and adds a synthetic `<scenario name> (merged)` result for convenience. That merged result averages the repeated runs' summary fields. For latency metrics, this means a merged p99 is the average of each repeated run's p99, not the p99 recalculated from all latency samples pooled together. This is useful as a compact run-level summary, but the individual repeated runs remain the source of truth for run-to-run variance and strict pooled-percentile analysis.

## Grafana Dashboards

During a benchmark run, live Grafana dashboards are available via:

```
dimster dashboard [env]
```

This port-forwards Grafana (localhost:3000) and Prometheus (localhost:9090). These dashboards are pre-provisioned:

- **Client Metrics** -- publish latency, schedule latency, end-to-end latency percentiles, message rates, byte rates
- **Kafka Broker** -- cluster health, throughput, request latency, replication lag, JVM stats
- **System (Linux)** -- CPU, memory, disk I/O, network (via node-exporter)
- **Containers** -- CPU, CPU throttling, disk I/O, page cache and network per pod (via the kubelets' cAdvisor; empty unless the environment sets [`monitoring.cgroupMetrics`](#per-pod-metrics))

## Saving Metrics After a Run

At the end of each run, Dimster saves the Prometheus metrics behind the dashboards into the result directory. There are two output formats, chosen with `metricsOutput` in the environment (see [dimster-config.yaml](../reference/dimster-config.md#core-fields)):

| Output | Written to | Use it for |
|--------|------------|------------|
| `html` | `dashboards/` | Opening in any browser and sharing. A fixed rendering of each dashboard. |
| `snapshot` | `prometheus-snapshot/` | Exploring the run later in real Grafana: zoom, edit panel queries, run ad-hoc PromQL. |

```yaml
environments:
  local:
    metricsOutput: [html, snapshot]
```

If `metricsOutput` is left out, only `html` is saved. An empty list (`metricsOutput: []`) saves no metrics at all. You can override the environment for a single run with `--metrics-output`, which takes a comma-separated list or `none`:

```
dimster run local -w workload.yaml --metrics-output html,snapshot
dimster run local -w workload.yaml --metrics-output none
```

`--metrics-output` is accepted by every command that collects results at the end: `run`, `explore`, `correctness`, `drain-backlog`, `resume` and `reattach`.

You can also save metrics for a run after it has finished, as long as the cluster's Prometheus still holds its data (Prometheus keeps 7 days):

```
dimster results local --metrics-output snapshot
```

This exports metrics for the latest run on the coordinator's PVC. With no value, `--metrics-output` uses the environment's `metricsOutput`.

### Dashboard HTML

The `html` output exports each Grafana dashboard as a static HTML file into the `dashboards` sub-directory, providing a permanent record of what the live dashboards showed during the run.

Long campaigns are split across multiple HTML files so each file stays a manageable size. The split points align with **scenario boundaries**: every test point in a given scenario is guaranteed to land in the same HTML file, so a single scenario is never cut in half by a window boundary. The exporter bin-packs scenarios into windows up to 1.5x the target window length, which keeps short scenarios grouped together and long scenarios in a window of their own. Each engine stamps the begin/end times of each scenario into the JSON result file; the exporter reads those ranges to compute the splits.

### Prometheus Snapshots

The HTML pages are fixed renderings: you can't change a query, break a panel down by another label, or look at a metric the dashboards don't chart. Once the cluster is torn down, the Prometheus data behind them is gone too. The `snapshot` output keeps that data, so a run can be reopened in a real Grafana long afterwards.

A snapshot holds:

- `metrics.om.gz`: the raw scraped samples of every metric the three dashboards reference, from 10 minutes before the run to 10 minutes after, as gzipped [OpenMetrics](https://openmetrics.io/) text.
- `dashboards/`: the Grafana dashboard JSON as it was at run time, so the snapshot always opens with dashboards that match its data.

Only metrics the dashboards use are kept, and only for the run window, so snapshots stay small (a few hundred KiB for a short run). They grow with run length and with the number of series (partitions, clients, brokers).

To view a snapshot:

```
dimster dashboard --snapshot results/2026-03-21-17-25-49
```

This needs Docker but no Kubernetes cluster. The CLI loads the snapshot into a temporary Prometheus container, starts Grafana with the snapshot's dashboards, and prints a link to each dashboard set to the run's time range:

```
==> Snapshot of results/2026-03-21-17-25-49
    clients    http://localhost:3000/d/dimster-clients?from=...&to=...
    kafka      http://localhost:3000/d/dimster-kafka?from=...&to=...
    system     http://localhost:3000/d/dimster-system?from=...&to=...
    Prometheus http://localhost:9090
    Press Ctrl+C to stop
```

Prometheus is exposed too, for ad-hoc PromQL. Ports 3000 and 9090 are used when free; otherwise free ports are picked, so a snapshot can be open alongside `dimster dashboard <env>`. Ctrl+C removes the containers and the temporary data; the snapshot itself is never modified.

> **Note:** A snapshot holds every sample, but not Prometheus' staleness markers, which live Prometheus records when a series stops being scraped. Without them, a series that pauses or ends can appear to continue for up to 30 seconds in the snapshot viewer. This shows up mostly in per-partition client metrics.

See [CLI Reference](../reference/cli.md) for chart and dashboard commands.

## Resources Report

The resources report shows what the hardware did during each test point of a `run` or `drain-backlog` run. Usage is split by service: Kafka brokers, Flink and workers. Coordinator and monitoring pods are left out.

At the end of the run, the metrics export measures each test point's window in Prometheus and saves the result to `resource-metrics.json`:

- **Run mode:** the measured phase, after ramp-up and warm-up. Results record it as `measureStartTime` and `measureEndTime` on each test.
- **Drain-backlog mode:** the drain, from `drainStartTime` to `drainEndTime`.

The chart step then joins those measurements with the result's throughput and writes:

| File | Contents |
|------|----------|
| `charts/<result>-resources.html` | The report page (see below). |
| `<result>-resources.csv` | One row per test point and service, plus an `All` row summing the three. Holds the per-service averages, CPU cost per GB and, on the brokers row, the page cache figures. Only written when per-pod metrics were collected. |
| `<result>-resources-nodes.csv` | One row per test point and node: CPU utilisation, IO wait, busiest disk's busy %, disk and network throughput, pressure stall (PSI). |
| `<result>-resources-timeseries.csv` | Per-pod and per-node usage in long format (`series, scenario, label, timestamp, seconds, scope, name, service, metric, value`), in each metric's raw unit (cores, bytes/s, ops/s, fractions). `series` is `run` for the whole run, or `test point` for a test point's own series (see below), with its scenario and label. `seconds` is since the series' start. |

The report page contains:

- **Summary:** each service's average CPU (cores, summed over its pods), peak pod CPU (the busiest pod's 1-minute peak), CPU throttling, disk read/write MB/s, read/write IOPS and network rx/tx MB/s for every test point. CPU throttling is the share of CFS periods in which the service's pods were held back by their CPU limits, weighted by each pod's periods; pods without a CPU limit have no periods, so it shows as `–` for a service without limits. It also shows CPU cost as core-seconds per GB produced and per GB consumed. Throughput comes from the result's client byte rates.
- **Page cache vs disk:** totals over each test point's window. It compares the brokers' logical reads (Kafka bytes out plus replication bytes out, from the brokers' JMX metrics) with their disk reads. It also shows the bytes the Kafka consumers received (the result's `bytesReceived`), disk reads per byte consumed and, for drains, per byte of backlog.
- **Bottlenecks:** saturated resources for each test point:
    - a pod at 90% or more of its CPU limit, or throttled in more than 10% of CFS periods;
    - a node whose CPU averages 90% or more, or peaks at 98% or more;
    - a node whose busiest disk is busy 90% of the time or more, or where tasks wait on IO 25% of the time or more;
    - a node whose network reaches 80% of its link speed, where the link speed is known.

    Each flag names the services on that node. A node hosting more than one service is marked as shared.
- **Whole run, a section per service:** bar charts across test points for each metric, including CPU throttling, then each metric over the whole run per pod, with each test point's window shaded.
- **Whole run, Nodes:** whole-node usage from node-exporter, across test points and over the whole run.
- **A section per test point:** the same per-pod and per-node charts over just that test point, from its own series: its window (shaded) and 30 s either side, sampled every 10 s, with time in seconds since the test point started. The whole-run series is sampled every 15 s or more, which would leave a short test point such as a quick drain with only a couple of points. Prometheus scrapes every 10 s, so even a test point's own series has only a few samples when it lasts under a minute.

Each sample in the over-time charts is a rate over the preceding interval (a minute for the whole run, 30 s for a test point). It is plotted at the middle of that interval, so the lines line up with the shaded windows rather than trailing them.

To regenerate the report from a result directory, without the cluster, run `dimster chart <dir>`.

### Per-pod metrics

node-exporter reports per node, so it can't tell which service used the CPU, disk or network of a node that runs several services. That is always the case on a single-machine cluster such as k3d, where every node also shares one kernel and reports the same machine-wide CPU and disk.

To split usage by service, set `monitoring.cgroupMetrics` in the environment and redeploy:

```yaml
environments:
  local:
    monitoring:
      cgroupMetrics: true
```

Prometheus then scrapes every kubelet's cAdvisor, which reads each container's cgroup counters: CPU time and throttling, block device reads and writes, page cache and network. These are per container even when services share a node. Disk counters only include I/O that reaches a block device, so page cache hits aren't counted. The kernel's later writeback of a container's dirty pages is charged to that container.

Scraping the kubelets needs a ClusterRole on nodes (`get`, `list` and `watch` on `nodes`, `nodes/proxy` and `nodes/metrics`), bound to the Prometheus ServiceAccount. `dimster deploy` creates it as `dimster-prometheus-<namespace>`, and `dimster teardown` removes it. Because it's a cluster-scoped permission, it's off by default. Only the namespace's containers and the metrics the report uses are stored.

Without `cgroupMetrics`, the report shows only the node-level figures, and the page cache estimate uses the disk reads of every node.

### What the numbers mean

- **Totals and averages:** `resource-metrics.json` holds each counter's growth over the window (CPU seconds, bytes, operations), with `measuredSeconds`, the time that growth covers. Averages are the growth divided by `measuredSeconds`. Windows of a minute or more are read between the scrapes at their bounds. Shorter windows, such as quick drains, are read from before their start to just past the scrape after their end, so their totals (bytes read, Kafka bytes) are complete. Their `measuredSeconds` is that whole span, so their averages include about 15 s of slack after the window, and the summary names them. CPU cost per GB is CPU seconds over that span divided by the bytes the result's clients produced or consumed (`bytesSent`/`bytesReceived`). A pod or node whose counter reset during the window (a restart) is left out of that window.
- **Page cache figures are estimates.** Logical reads not read from disk are assumed to come from the page cache. Readahead and index reads can push disk reads above logical reads; the cache share is then shown as 0. Without per-pod metrics, every node's disk reads are counted, including those of other services. There are no page cache figures without the brokers' JMX metrics (external Kafka).
- **Disk busy** is the share of time a device had I/O in flight. NVMe devices serve many requests in parallel, so a busy NVMe disk may still have headroom.

## Other visualization options

`dimster csv <result-file.json>` converts the JSON result file into a CSV file, which can be used with other charting options / placing in a spreadsheet.

`dimster pivot <result-file.json>` creates charts which create a plot per dimension index and each scenario is a series (rather than a plot per scenario with test points as series).
