# EKS Setup CLI Reference

The `eks-setup` CLI automates creation and deletion of EKS clusters sized for Dimster benchmarks. It can run interactively, replay from an answers file, or generate a shell script.

Binary location: `cloud/target/release/eks-setup`

Build: `cd cloud && cargo build --release -p eks-setup`

Or build and copy to project root (for convenience): `cd cloud && cargo build --release -p eks-setup && cp target/release/eks-setup .. && cd ..`

## Commands

### create

Create a new EKS cluster with node groups for Kafka brokers, workers, and infrastructure (coordinator, Grafana, Prometheus).

```
eks-setup create [options]
```

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--answers <path>` | No | -- | Path to an answers YAML file. Replays the configuration without interactive prompts. |
| `--instance-types <path>` | No | `cloud/eks-cli/instance-types.yaml` | Path to the instance types definition YAML. |
| `--dry-run` | No | `false` | Show the commands that would be executed without running anything. |
| `--generate-script` | No | `false` | Output a shell script instead of executing commands directly. |
| `-y`, `--yes` | No | `false` | Skip all interactive confirmations. |

When run without `--answers`, the CLI walks through an interactive questionnaire covering:

1. **Environment name** -- The key used under `environments:` in `dimster-config.yaml` (default: `aws`)
2. **AWS configuration** -- Dimster name (used to derive the cluster name `{name}-benchmark`), AWS region, availability zone, EKS version
3. **Deploy mode** -- `full` (deploy Kafka on EKS) or `external` (connect to an existing Kafka cluster)
4. **Kafka sizing** (full mode only) -- Kafka version, instance type, broker count, JVM heap, CPU, memory
5. **Storage** (full mode only) -- EBS or instance store, volume size, optional provisioned IOPS and throughput
6. **Worker sizing** -- Instance type, worker count (with min/max for autoscaling), JVM heap, CPU, memory
7. **Coordinator/infra** -- Instance type for the infrastructure node, coordinator heap/CPU/memory

After the questionnaire, a summary is displayed for confirmation before proceeding.

#### Generated outputs

On successful completion, the tool generates:

- **Environment configuration** -- either inserted directly into an existing `dimster-config.yaml` (replacing/adding the environment entry) or written as a separate `dimster-config-{name}.yaml` file. The user is prompted to choose.
- `eks-answers-{name}.yaml` -- Answers file capturing all decisions, usable with `--answers` for replay.

---

### delete

Delete an EKS cluster and its associated AWS resources.

```
eks-setup delete [options]
```

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--answers <path>` | Conditional | -- | Path to answers file (provides cluster name and region). Required unless `--name` and `--region` are specified. |
| `--name <name>` | Conditional | -- | Dimster name. The cluster name is derived as `{name}-benchmark`. Required with `--region` if `--answers` is not provided. |
| `--region <region>` | Conditional | -- | AWS region. Required with `--name` if `--answers` is not provided. |
| `--keep-ecr` | No | `false` | Keep the ECR repository; only delete the EKS cluster and node groups. |
| `--dry-run` | No | `false` | Show what would be executed without running anything. |
| `--generate-script` | No | `false` | Generate a shell script instead of executing commands. |
| `-y`, `--yes` | No | `false` | Skip all interactive confirmations. |

Examples:

```bash
# Delete using an answers file
eks-setup delete --answers eks-answers-mytest.yaml

# Delete by name and region
eks-setup delete --name mytest --region us-east-1

# Delete cluster but keep the ECR repo for reuse
eks-setup delete --answers eks-answers-mytest.yaml --keep-ecr
```

---

## Answers YAML Format

The answers file captures all configuration for an EKS cluster. It can be created interactively (the CLI saves it after prompting) or written by hand for repeatable deployments. Fields use camelCase.

### Full Example

```yaml
envName: aws

aws:
  dimsterName: my-dimster
  region: us-west-2
  az: us-west-2b
  eksVersion: "1.32"

deployMode: full
# bootstrapServers: "broker1:9092,broker2:9092"  # required when deployMode is "external" (generates bootstrap.servers in dimster-config.yaml)

kafka:
  version: "4.2.0"
  instanceType: m6i.2xlarge
  count: 3
  heap: "-Xms2G -Xmx2G"
  cpu: "6"
  memory: "28Gi"

storage:
  type: ebs
  size: 500Gi
  performance: basic
  # iops: 16000            # required when performance is "provisioned"
  # throughputMbs: 1000    # required when performance is "provisioned"

workers:
  instanceType: m6i.xlarge
  count: 2
  min: 1
  max: 4
  heap: "-Xms1G -Xmx2G"
  cpu: "3"
  memory: "12Gi"

coordinator:
  infraInstanceType: t3.medium
  heap: "-Xms512M -Xmx512M"
  cpu: 500m
  memory: 1Gi
```

### Field Reference

#### aws

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dimsterName` | string | `my-dimster` | Resource name prefix. The EKS cluster is named `{dimsterName}-benchmark`. |
| `region` | string | `us-west-2` | AWS region. |
| `az` | string | `{region}b` | Availability zone. The CLI queries available AZs from AWS when running interactively. |
| `eksVersion` | string | `1.32` | EKS Kubernetes version. |

#### Top-level

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `envName` | string | `aws` | Environment name used as the key under `environments:` in `dimster-config.yaml`. |
| `deployMode` | string | `full` | `full` deploys Kafka + benchmark clients. `external` connects to an existing Kafka cluster (skips Kafka node group creation). |
| `bootstrapServers` | string | -- | External Kafka bootstrap servers. Required when `deployMode` is `external`. Omitted from the file when null. Emitted as `bootstrap.servers` in the generated `dimster-config.yaml`. |

#### kafka

These fields are present in the file but ignored when `deployMode` is `external`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `version` | string | `4.2.0` | Kafka version for the broker image. |
| `instanceType` | string | `m6i.2xlarge` | EC2 instance type for Kafka broker nodes. |
| `count` | integer | `3` | Number of Kafka broker instances. |
| `heap` | string | computed | JVM heap flags (`-Xms/-Xmx`). Default is computed as 25% of instance memory, clamped to 1-8 GiB. |
| `cpu` | string | computed | Kubernetes CPU request. Default is instance vCPUs minus 2, minimum 1. |
| `memory` | string | computed | Kubernetes memory request. Default is the instance's allocatable memory (total minus EKS reserved overhead). |

#### storage

These fields are present in the file but ignored when `deployMode` is `external`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | `ebs` | Storage type: `ebs` or `instance-store`. Instance store is available when the Kafka instance type has NVMe drives. |
| `size` | string | `500Gi` | Volume or PV capacity. For instance store, defaults to 90% of the NVMe capacity. |
| `performance` | string | `basic` | EBS gp3 performance tier: `basic` (125 MB/s, 3000 IOPS) or `provisioned` (custom). Only applies when `type` is `ebs`. |
| `iops` | integer | -- | Provisioned IOPS (max 16000). Required when `performance` is `provisioned`. |
| `throughputMbs` | integer | -- | Provisioned throughput in MB/s (max 1000). Required when `performance` is `provisioned`. |

#### workers

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `instanceType` | string | `m6i.xlarge` | EC2 instance type for worker nodes. |
| `count` | integer | `2` | Initial number of worker nodes. |
| `min` | integer | `1` | Minimum worker count for autoscaling. |
| `max` | integer | max(`count`, 4) | Maximum worker count for autoscaling. |
| `heap` | string | computed | JVM heap flags. Default: max is 80% of allocatable memory, min is 1/3 of max. |
| `cpu` | string | computed | Kubernetes CPU request. Default is instance vCPUs minus 1, minimum 1. |
| `memory` | string | computed | Kubernetes memory request. Default is the instance's allocatable memory. |

#### coordinator

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `infraInstanceType` | string | `t3.medium` | EC2 instance type for the infrastructure node (runs coordinator, Grafana, and Prometheus). |
| `heap` | string | `-Xms512M -Xmx512M` | JVM heap flags for the coordinator. |
| `cpu` | string | `500m` | Kubernetes CPU request for the coordinator. |
| `memory` | string | `1Gi` | Kubernetes memory request for the coordinator. |

---

## Computed Defaults

When running interactively, the CLI computes resource defaults from the selected instance type's specifications:

- **Kafka heap**: 25% of instance memory, clamped between 1 GiB and 8 GiB.
- **Kafka CPU**: Instance vCPUs minus 2, minimum 1.
- **Worker heap**: 80% of allocatable memory for `-Xmx`, 1/3 of that for `-Xms`.
- **Worker CPU**: Instance vCPUs minus 1, minimum 1.
- **Memory requests**: Instance memory minus EKS kubelet reservations (tiered formula: 255 MiB + 25% of first 4 GiB + 20% of next 4 GiB + 10% of next 8 GiB + 6% of next 112 GiB + 2% above 128 GiB), minus 100 MiB eviction threshold, minus ~1 GiB for system pods.
- **Instance store size**: 90% of the total NVMe capacity.
