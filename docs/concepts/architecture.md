# Architecture

A one-page mental model of how Dimster is put together. After reading this you should be able to picture the system without having to memorize anything — port numbers, lifecycle states, and exact CLI flags all live in the reference docs.

## The 30-second version

Dimster is a **local CLI** that drives a **Java benchmark engine running in Kubernetes**, with **Prometheus + Grafana** alongside for live observability and post-run dashboard exports.

Whether running a serious benchmark in the cloud or on production-like hardware, or running a toy example on your laptop, the architecture is the same via the use of Kubernetes as a standardized platform.

![Dimster architecture](../images/dimster_architecture_full_deploy.png)
*Fig 1. A full Dimster deployment to a remote Kubernetes cluster*

![Dimster architecture](../images/dimster_architecture_local.png)
*Fig 2. Dimster running on a laptop in local Kubernetes distro*

Keep the following roles in your head:

- **CLI** — the remote control on your laptop.
- **Coordinator** — one long-lived pod that orchestrates each benchmark.
- **Workers** — N pods that produce and consume against Kafka.
- **Monitoring** — Node exporter, Prometheus and Grafana. 

Everything else on the page is detail behind those four roles.

## The four roles

### 1. CLI — the remote control

`dimster` is a Rust binary that runs on your laptop. It builds the coordinator/worker image, generates Kubernetes manifests from your `dimster-config.yaml`, applies them, sends benchmark requests to the coordinator, and copies results back when a run finishes.

The CLI never runs benchmark code itself, nor does it SSH onto anything (no worries about disconnection mid-test). It's a thin convenience layer — every command boils down to "shell out to `docker`, `kubectl`, or HTTP-to-coordinator."

### 2. Coordinator — the brain

One pod, one replica. The coordinator owns the benchmark lifecycle: creating topics, telling workers what to produce and consume, collecting their stats, writing results to a PersistentVolumeClaim (PVC), and reporting status back to the CLI.

- **It's long-lived.** It does not exit when a run finishes. You can start a multi-hour campaign, close your laptop, and reattach later with `dimster reattach <env>`.
- **It's resumable.** Progress is checkpointed to its PVC, so if the pod is killed mid-campaign you can pick up where it left off with `dimster resume <env>`.
- **Results as shareable files** Benchmark results are stored in a JSON file, with accompanying charts as well as log files and the workload/config files. These files can be shared among colleagues easily and are self-contained.

### 3. Workers — the muscle

A Kubernetes Deployment of N pods, each running Kafka producers and consumers. The coordinator distributes producers and consumers across them, asks for stats periodically, and drives them through the test points one at a time.

Workers are stateless and identical — scale them by changing `workers.replicas` in your env block and re-deploying.

### 4. Monitoring

Prometheus and Grafana are deployed alongside the benchmark components. Prometheus scrapes the coordinator, workers, Kafka, node exporter. Grafana comes with three dashboards (Client metrics, Kafka metrics and System metrics). At the end of the run the CLI starts a short-lived pod that queries Prometheus and saves the run's metrics into the results: the dashboards rendered as HTML files and, optionally, a Prometheus snapshot that can be reopened in Grafana later with `dimster dashboard --snapshot` (see [Results and Charts](results-and-charts.md#saving-metrics-after-a-run)).

> The coordinator gathers metrics from workers and writes them out to the results JSON file at the end of a run. This is a separate process to Prometheus scraping.

## Deployment modes

Dimster has two ways to get a Kafka cluster to point at:

| Mode       | Where Kafka comes from                                            | When to use                                        |
|------------|-------------------------------------------------------------------|----------------------------------------------------|
| `full`     | Dimster deploys a Kafka StatefulSet (KRaft mode) into the cluster | Self-contained benchmarks; you control the broker  |
| `external` | You point workers at an existing bootstrap server                 | Confluent Cloud, MSK, an existing in-house cluster |

![Dimster external deployment](../images/dimster_architecture_external_deploy.png)
*Fig 3. Dimster workers pointed at an external Kafka cluster or service*

## Why Kubernetes

The Java engine itself has zero Kubernetes dependency — it's just a JVM process that talks HTTP and Kafka. You could run the coordinator and workers as plain `java -jar` processes on bare metal and write your own orchestration against their HTTP APIs.

The **CLI**, on the other hand, is built around Kubernetes from the ground up: it generates Kustomize overlays, calls `kubectl apply`, discovers pods, port-forwards Grafana, copies results off PVCs. So in practice, "using Dimster" means "using Kubernetes," even though the engine doesn't require it.

The trade-off was deliberate: leaning on Kubernetes gives you the same deployment shape on a laptop (minikube/k3d/kind) as on a production EKS or GKE cluster, which means the workflow you learn locally is the same one you use for real benchmarks. The cost is that you cannot use the CLI without a `kubectl` context.

## How information flows during a run

1. **CLI → Coordinator** — the CLI sends the run request over HTTP (tunneled through `kubectl exec`).
2. **Coordinator → Workers** — direct pod-to-pod HTTP for configuration, producer/consumer lifecycle, and stats collection.
3. **Workers → Kafka** — standard Kafka client protocol.
4. **Coordinator → PVC** — JSON results are written as the run progresses.
5. **CLI ← Coordinator (PVC)** — when the run finishes, the CLI copies the results, logs, and dashboard HTML exports back to your laptop.

That five-step loop is the whole runtime story.

## Where to go next

- [Dimensional testing](dimensional-testing.md) — how one workload file produces many test points.
- [Results and charts](results-and-charts.md) — what shows up in your results directory and how to make charts from it.
- [Deployment overview](../deployment/) — how the env block in `dimster-config.yaml` becomes Kubernetes resources.
- [`dimster-config.yaml` reference](../reference/dimster-config.md) — every field.
