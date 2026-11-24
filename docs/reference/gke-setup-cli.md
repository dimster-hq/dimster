# GKE Setup CLI Reference

The `gke-setup` CLI automates creation, preflight checking, and deletion of single-zone GKE Standard clusters sized for Dimster benchmarks. It runs interactively, replays from an answers file, previews with `--dry-run`, or emits a shell script with `--generate-script`.

Binary location: `cloud/target/release/gke-setup`

Build and copy to project root:

```bash
cd cloud && cargo build --release -p gke-setup && cp target/release/gke-setup .. && cd ..
```

## Commands

### check

Verify local CLI tools (`gcloud`, `kubectl`, `gke-gcloud-auth-plugin`), active project, service account impersonation setting, and project IAM permissions.

```bash
gke-setup check [--project <PROJECT_ID>]
```

---

### create

Create an Artifact Registry Docker repository, a single-zone GKE Standard cluster (`default-pool` labeled `role=infra`, plus `kafka` and `workers` node pools), a tuned Kafka `StorageClass` (or Local NVMe SSD DaemonSet + static `local-storage` PVs), and the Dimster environment config.

During interactive setup (and `--dry-run` / `--answers` preview), `gke-setup create` queries your project and regional GCP quotas (project `CPUS_ALL_REGIONS`, per-family vCPUs, external IPs, Hyperdisk/PD/Local SSD limits, and live usage) so you can see available headroom before choosing machine types and disks. Once all sizing decisions are complete, it prints a quota validation summary and an itemized on-demand hourly USD cost estimate before prompting for confirmation.

```bash
gke-setup create [options]
```

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--answers <path>` | No | -- | Path to an answers YAML file (replay mode — skips interactive prompts). |
| `--instance-types <path>` | No | `cloud/gke-cli/instance-types.yaml` | Path to the GCP machine types catalog YAML. |
| `--dry-run` | No | `false` | Show commands and manifests without running anything. |
| `--generate-script` | No | `false` | Output a standalone bash script instead of executing commands. |
| `-y`, `--yes` | No | `false` | Skip all interactive confirmations. |

Generated outputs:
- **Dimster environment block** — inserted into `dimster-config.yaml` or written as `dimster-config-{dimsterName}.yaml`.
- **`gke-answers-{dimsterName}.yaml`** — replay file capturing all choices.

---

### delete

Gracefully delete PVCs (pinned explicitly to `gke_{project}_{zone}_{name}-benchmark`), delete the GKE cluster, sweep any orphaned GCE disks labeled `dimster-cluster={name}-benchmark`, and optionally delete the Artifact Registry repository.

```bash
gke-setup delete [options]
```

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--answers <path>` | Conditional | -- | Path to answers YAML file. Required unless `--name`, `--project`, `--region`, and `--zone` are all passed. |
| `--name <name>` | Conditional | -- | Dimster name prefix (cluster `{name}-benchmark`). |
| `--project <project>` | Conditional | -- | GCP project ID. |
| `--region <region>` | Conditional | -- | GCP region (Artifact Registry location). |
| `--zone <zone>` | Conditional | -- | GCP zone (GKE cluster and GCE disks location). |
| `--keep-registry` | No | `false` | Keep the Artifact Registry repository. |
| `--dry-run` | No | `false` | Preview commands without running anything. |
| `--generate-script` | No | `false` | Output a teardown bash script instead of executing. |
| `-y`, `--yes` | No | `false` | Skip all interactive confirmations. |

---

## Answers YAML Format

Fields use camelCase. All options can be edited by hand and replayed via `gke-setup create --answers <file>`.

```yaml
envName: gke
gcp:
  project: my-gcp-project
  dimsterName: my-dimster
  region: us-central1
  zone: us-central1-a
  releaseChannel: regular           # regular | stable | rapid | none
  # gkeVersion: "1.32"              # optional explicit Kubernetes version
  # existingImage: "us-central1-docker.pkg.dev/my-gcp-project/existing/dimster"
  dataplaneV2: true
  # network: "my-vpc"               # optional custom VPC (e.g. VPC peering)
  # subnetwork: "my-subnet"         # optional custom subnet
  # nodeServiceAccount: "node-sa@my-gcp-project.iam.gserviceaccount.com"
deployMode: full                    # full | external
# bootstrapServers: "broker:9092"   # used when deployMode is "external"
# kafkaClientVersion: "4.2.0"       # optional kafka_client_version override
cpuManagerStatic: false             # true configures kubelet cpuManagerPolicy: static
kafka:
  version: "4.2.0"
  instanceType: c3-standard-8
  count: 3
  heap: "-Xms8G -Xmx8G"
  cpu: "6"
  memory: "26Gi"
  tier1Networking: false
  brokerConfig: run/broker-configs/default.yaml
  # extraConfig: "num.network.threads=8,num.io.threads=16"
storage:
  type: hyperdisk-balanced          # hyperdisk-balanced | hyperdisk-extreme | hyperdisk-throughput | pd-ssd | pd-balanced | local-ssd
  size: 500Gi
  performance: provisioned          # basic | provisioned (hyperdisk-balanced)
  iops: 16000
  throughputMibs: 800
  # localSsdCount: 2                # for local-ssd (fixed on -lssd or 1/2/4/8 on n2/n2d)
workers:
  instanceType: c3-standard-4
  count: 2
  min: 1
  max: 4
  heap: "-Xms2G -Xmx8G"
  cpu: "3"
  memory: "11Gi"
  tier1Networking: false
coordinator:
  infraInstanceType: e2-standard-2
  heap: "-Xms512M -Xmx512M"
  cpu: "500m"
  memory: "1Gi"
monitoring:
  prometheusCpu: "250m"
  prometheusMemory: "1843Mi"
  grafanaCpu: "250m"
  grafanaMemory: "1228Mi"
```

---

## Computed Defaults

When running interactively, `gke-setup` calculates resource defaults from `cloud/gke-cli/instance-types.yaml`:

- **GKE allocatable memory**: Instance RAM minus GKE Standard tiered kubelet reservation (`25%` first 4 GiB + `20%` next 4 GiB + `10%` next 8 GiB + `6%` next 112 GiB + `2%` above 128 GiB), minus `100 MiB` eviction buffer, minus `500 MiB` Container-Optimized OS / containerd overhead, minus `1 GiB` system pods.
- **Kafka heap**: 25% of instance memory, clamped to 1–8 GiB.
- **Kafka CPU**: Instance vCPUs minus 2, minimum 1.
- **Worker heap**: `-Xmx` = 80% of allocatable memory; `-Xms` = 1/3 of `-Xmx`.
- **Worker CPU**: Instance vCPUs minus 1, minimum 1.
- **Local NVMe PV capacity**: 90% of `localSsdCount × 375 GiB`.
- **Monitoring**: Remaining infra allocatable memory after coordinator split 60% Prometheus / 40% Grafana; remaining CPU split 50/50.

## IAM Helper Script

`cloud/gke-cli/grant-sa-permissions.sh` enables the required GCP APIs and grants the minimal roles to a deployer service account and the GKE node VM service account. Run `./cloud/gke-cli/grant-sa-permissions.sh --help` for usage.
