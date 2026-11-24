# Getting Started with Dimster

A hands-on walkthrough that takes you from an empty laptop to your first finished Dimster benchmark on a local Kubernetes cluster — and then nudges you toward the four benchmark modes and a beefier local environment.

## Who this is for

You are a developer who:

- Knows their way around the command line.
- Already understands Kafka basics (topics, partitions, producers, consumers).
- Is **new to Kubernetes** or new to Dimster.

If any of that is wildly off, these pages will still work — they just spend more time hand-holding the Kubernetes parts than a Kubernetes expert would want.

> Operating systems covered here: **macOS** and **Linux**. Windows is not covered (use WSL2 if you must, but it isn't tested).

## How to read this

Follow the numbered files in order on your first pass. Each step depends on the previous one and most commands assume you're at the project root.

| Page | What you'll do                                                                                                 |
|------|----------------------------------------------------------------------------------------------------------------|
| [01 — Prerequisites](01-prerequisites.md) | Install Rust, Docker, kubectl, your local k8s distro of choice if you want to run benchmarks locally, Python 3 |
| [02 — Compile the CLI](02-compile-the-cli.md) | Build the `dimster` binary from the Rust source                                                                |
| [03 — Start a local Kubernetes](03-start-local-k8s.md) | Bring up a local cluster (minikube, k3d, kind, Docker Desktop, or OrbStack)                                    |
| [04 — Initialize the config](04-dimster-init.md) | Run `dimster init` and tour `dimster-config.yaml`                                                              |
| [05 — Build and deploy](05-build-and-deploy.md) | Build the Dimster Docker image (Maven runs inside the container) and deploy to k8s                             |
| [06 — Your first benchmark](06-first-benchmark.md) | Run the quick-test workload and watch live dashboards                                                          |
| [07 — Teardown](07-teardown.md) | Clean up Dimster (and decide when to stop or delete your cluster)                                              |
| [08 — Dimensional testing](08-dimensional-testing.md) | A 5-minute intro to scenarios and test points using two minimal examples                                       |
| [09 — The four modes](09-the-four-modes.md) | One-page tour of `run`, `explore`, `drain-backlog`, `correctness`                                              |
| [10 — A beefier local env](10-localBeefy-env.md) | Add a `localBeefy` env when your laptop has the headroom                                                       |
| [11 — Running without Kubernetes](11-embedded-mode-without-k8s.md) | Side path: run benchmarks in a single JVM against a local broker — smoke tests, CI, quick repros               |
| [Modifying Dimster itself](../development.md) | Extra setup if you want to hack on the Java engine or the CLIs                                                 |

After you've finished page 10 you'll have:

- A working local benchmarking lab.
- A `dimster-config.yaml` you can edit and grow.
- Hands-on experience with each of the four benchmark modes.
- A clean teardown path between sessions.

Page 11 is an optional side path for when you want to run a benchmark without Kubernetes at all — useful for CI smoke tests and quick local repros, but with reduced capabilities.

## Where to go next

For real benchmarks, you'll need to deploy to a real Kubernetes cluster. Read the [Deploying Dimster](../deployment/) section, which covers:

- The mental model: what Dimster expects from any Kubernetes cluster, and how `dimster-config.yaml` maps to k8s resources.
- The two deploy modes (`full` vs `external`) and when to use each.
- Why `nodeLabel` and `pvcStorageClass` are required for any benchmark whose numbers you'd publish.
- Per-target guides: AWS EKS (with `eks-setup`), GCP GKE (with `gke-setup`), Azure/on-prem, and external Kafka services like Confluent Cloud and MSK.

Also:

- [CLI Reference](../reference/cli.md) — every flag for every command.
- [`dimster-config.yaml` Reference](../reference/dimster-config.md) — full schema.
- [Workload Reference](../reference/workloads.md) — every workload field.
- [Dimensional Testing](../concepts/dimensional-testing.md) — sweep parameters across many runs.
