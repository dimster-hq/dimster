# AWS EKS Setup via `eks-setup`

The `eks-setup` CLI is a wizard that creates an AWS EKS cluster sized for Dimster, an ECR repository for the image, a tuned `gp3` (or local NVMe) `StorageClass`, and a populated env block in your `dimster-config.yaml`. After it finishes, three commands (`build`, `deploy`, `run`) get you to a benchmark.

If you'd rather drive `eksctl` and `aws` by hand — for IaC integration, restricted permissions, or just to know exactly what's happening — run `eks-setup` with `--generate-script` to emit a shell script you can read, edit, and run.

## What `eks-setup` builds

Starting with an AWS account:

```
┌──────────────────────────────────────────────────────────┐
│   AWS account                                            │
│   ┌────────────────────────────────────────────────────┐ │
│   │ ECR repo: {dimsterName}                            │ │
│   └────────────────────────────────────────────────────┘ │
│   ┌────────────────────────────────────────────────────┐ │
│   │ EKS cluster: {dimsterName}-benchmark               │ │
│   │                                                    │ │
│   │   Nodegroup: kafka   (label role=kafka)            │ │
│   │     N × {kafkaInstanceType}, all in 1 AZ           │ │
│   │   Nodegroup: workers (label role=worker)           │ │
│   │     M × {workerInstanceType}, all in 1 AZ          │ │
│   │   Nodegroup: infra   (label role=infra)            │ │
│   │     1 × {infraInstanceType}, same AZ               │ │
│   │                                                    │ │
│   │   StorageClass: gp3 or gp3-fast                    │ │
│   │   EBS CSI driver + IRSA                            │ │
│   └────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
              ▲
              │  populates env block in
              │
   dimster-config.yaml
     environments:
       aws:
         image:   {accountId}.dkr.ecr.{region}.amazonaws.com/{dimsterName}
         context: arn:aws:eks:{region}:{accountId}:cluster/{dimsterName}-benchmark
         kafka:   { nodeLabel: role=kafka, pvcStorageClass: gp3-fast, ... }
         workers: { nodeLabel: role=worker, ... }
         coordinator + monitoring: { nodeLabel: role=infra, ... }
```

**Single-AZ on purpose.** All nodegroups land in the same availability zone. This is to avoid cross-AZ data transfer, which costs $0.01/GB *each way* and dominates your AWS bill on any sustained benchmark. EKS still requires the *control plane* in 2+ AZs, but every Dimster pod sits in one.

## Prerequisites

Local tools:

```bash
brew install awscli eksctl kubectl
```

Configure credentials:

```bash
aws configure
# or, for SSO:
aws sso login --profile your-profile
aws sts get-caller-identity   # confirms you're authenticated
```

You'll need IAM permissions to create EKS clusters, EC2 instances, IAM roles (for IRSA), and ECR repos. The `AdministratorAccess` policy is the simplest answer if you have it; otherwise your account admin will know the minimum set.

## Build the `eks-setup` binary

From the repository root:

```bash
cd cloud
cargo build --release -p eks-setup
cp target/release/eks-setup ..
cd ..
```

> Note, on macOS, you may need to sign the binary: `codesign --force --sign - ./eks-setup`.

You now have the `./eks-setup` binary next to `./dimster` at the project root.

## Run the wizard

```bash
./eks-setup create -y
```

This walks through a series of questions:

1. **Environment name** — the key under `environments:` in `dimster-config.yaml`. Default `aws`.
2. **AWS configuration** — Dimster name (used as a resource prefix; cluster ends up named `{dimsterName}-benchmark`), region, AZ, EKS version.
3. **Deploy mode** — `full` (Dimster manages Kafka in the cluster) or `external` (workers only, points at an external Kafka). For external, see [06 — External Kafka](../../docs/deployment/06-external-kafka.md) for what to put in the `connection:` block afterwards.
4. **Kafka sizing** (full mode) — Kafka version, instance type (with computed defaults from `cloud/eks-cli/instance-types.yaml`), broker count, JVM heap, CPU and memory requests.
5. **Storage** (full mode) — EBS or instance-store. For EBS, choose `basic` (gp3 defaults: 125 MB/s, 3000 IOPS) or `provisioned` (you specify IOPS up to 16k and throughput up to 1000 MB/s).
6. **Worker sizing** — instance type, count, autoscaling min/max, JVM heap, CPU/memory.
7. **Coordinator/infra** — small instance type for the infra node (default `t3.medium`); coordinator JVM heap, CPU, memory.

For each numeric field, the CLI suggests a sensible default based on the instance type you picked (heap = 25% of memory clamped to 1-8 GiB for Kafka, etc. — see the [CLI reference](../../docs/reference/eks-setup-cli.md) for the full formula list).

After the questionnaire, a summary screen lists every choice. Confirm to start the build. Without the `-y` flag, it will ask to confirm every action it takes against the AWS CLI.

For background on what makes a sensible instance choice — and why a few EC2 quirks (network burst, EBS bandwidth limits) bite hardest — see [Picking instance types](#picking-instance-types) below.

### Replaying the wizard with an answers file

When the wizard finishes, it writes `eks-answers-{dimsterName}.yaml` capturing every answer. To recreate the exact same cluster later:

```bash
./eks-setup create --answers eks-answers-mytest.yaml
```

To skip every confirmation prompt, add `-y`:

```bash
./eks-setup create --answers eks-answers-mytest.yaml -y
```

To preview without doing anything, add `--dry-run`. To get a shell script you can review and run yourself, add `--generate-script`. The script is the source of truth — if you want to know exactly what `eks-setup` does, generate the script and read it.

## What you get when the wizard finishes

Two files (or one populated file, depending on your choice mid-wizard):

- `dimster-config.yaml` (or `dimster-config-{name}.yaml`) — a fully-populated env block ready to use.
- `eks-answers-{name}.yaml` — replay file.

Plus, in AWS:

- An ECR repo named `{dimsterName}`.
- An EKS cluster named `{dimsterName}-benchmark` with three nodegroups (`kafka`, `workers`, `infra`) all labelled.
- A `gp3` StorageClass (or `gp3-fast` if you chose provisioned performance).
- The EBS CSI driver installed and bound to an IRSA role so volumes provision correctly.
- Your local `kubectl` context set to the new cluster (eksctl does this automatically).

The whole thing takes 15-25 minutes — most of it waiting on EKS control-plane creation and nodegroup provisioning.

## Build and deploy Dimster

You must perform a Docker login. `eks-setup` prints out a working docker login command which you can copy and paste. Alternatively, you can run the commands below:

```bash
# Log in to ECR (token lasts 12 hours)
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=us-west-2
aws ecr get-login-password --region $AWS_REGION | \
  docker login --username AWS --password-stdin \
  ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
```

Next you perform the usual `dimster build <env>` and `dimster deploy <env>`.

```bash
# Build + push to ECR (the env's image: field points at the ECR repo)
./dimster build aws

# Deploy to the cluster
./dimster deploy aws
```

If you're on Apple Silicon and your EKS nodegroups are x86_64, your Docker daemon needs to build x86_64 images. With Colima, there are two helper scripts:

```bash
./restart-colima-x86-64.sh   # before dimster build aws
./restart-colima.sh          # after, to switch back
```

Alternatively, pick `arm64` instance types in the wizard (`m6g`, `m7g`, `m8g`, `i4g`) and build natively.

## Run a benchmark

```bash
./dimster run aws \
  -k run/kafka-configs/kafka_safe.yaml \
  -w run/workloads/quick-test/quick-run-test.yaml
```

In a second terminal, port-forward the dashboards:

```bash
./dimster dashboard aws
# Grafana: http://localhost:3000  (Dimster folder)
# Prometheus: http://localhost:9090
```

Results land in `./results/` on your laptop after the run completes.

## Picking instance types

A poorly-chosen instance will cap your benchmark on AWS infrastructure limits — EBS bandwidth, network baseline, EBS IOPS — and you'll think you've found a problem in the system under test when really you've found a problem in the EC2 spec sheet. This section is the cheat sheet for picking instance types that don't lie to you.

For automated selection, the wizard reads `cloud/eks-cli/instance-types.yaml` and computes sensible defaults. This section is the *why* behind those choices, and the reference for picking by hand.

### The four properties that matter

#### 1. Storage: EBS-only vs NVMe

| | EBS-only (m / c / t / r series) | NVMe (i3, i3en, i4i, i4g, i7i, i7ie, im4gn) |
|---|---|---|
| Backing | Network-attached EBS volumes | Local NVMe SSD on the instance |
| Setup | StorageClass → EBS provisioner | Local volume provisioner; `pvcStorageClass: local-storage` |
| Disk I/O budget | Dedicated EBS path on every EBS-optimized instance (which is all the instances Dimster recommends) — sized by `ebsBandwidthMBs` per type, separate from the main NIC | Fixed by the instance's NVMe count and per-device throughput — no way to provision more without sizing up |
| Throughput ceiling per broker | Up to 1000 MB/s per `gp3` PVC (or higher with `io2`), capped only by the instance's `ebsBandwidthMBs` | Whatever the instance's NVMe spec gives you |
| Survives instance replacement? | Yes (volume reattaches) | **No** (data is gone on instance termination) |
| Best for | Almost all Kafka benchmarks. Sequential writes don't need NVMe latency, and per-volume throughput on `gp3-fast` is independently provisionable | Specific cases: instances whose NVMe throughput exceeds their EBS budget (e.g. `i3en.6xlarge`); workloads where bundled-storage cost wins; tests where you don't care about durability |

**EBS gp3 (and `gp3-fast`) is the right default for Kafka.** Sequential write workloads do well on EBS — Kafka isn't latency-sensitive in the way OLTP databases are. With each broker getting its own PVC, a 3-broker cluster on `gp3-fast` can deliver up to 3 × 1000 MB/s of disk write capacity, subject only to per-instance EBS bandwidth.

NVMe wins in narrower cases than people often assume. The two real ones are: (a) you've picked an instance whose NVMe throughput is meaningfully higher than its EBS bandwidth (rare; check `instance-types.yaml`), or (b) you want the cost benefit of bundled storage on i-series instances. The trade-off is durability — instance termination loses the data.

> See [03 — Scheduling and storage](../../docs/deployment/03-scheduling-and-storage.md#nvme-instances) for the local-storage StorageClass setup.

#### 2. Network bandwidth

This is the limit that bites first on Kafka. Every produce → broker hop and every replicate → follower hop lives on this budget.

For replication factor 3, **each MB/s of producer throughput becomes ~3 MB/s on the leader's network**: 1 MB/s in from the producer, 2 MB/s out to the two followers. Then consumer fetch traffic adds more on top.

> **Size against the baseline, not the burst.** EC2 instances advertise an "up to N Gbps" network rate which is a *burst* number, sustained only while burst credits last (typically 5-30 minutes). Once credits exhaust, the instance falls back to its baseline rate without warning — so a benchmark sized to burst will look great for the first few minutes and then mysteriously cliff. Always size your benchmark to fit within the **baseline** throughput. The baseline value for every supported instance type is in `cloud/eks-cli/instance-types.yaml` under the `networkBaselineGbps` field. The "Baseline ≠ burst" section below covers the same trap for EBS bandwidth.

Network-optimized variants (`m6in`, `m7in`, `i3en`, `i4i`, `i7ie`) carry 2-4× the network baseline of their standard counterparts at the same vCPU count. If your benchmark is going to push real throughput, jump straight to the `n` variants.

#### 3. EBS bandwidth

On EBS-optimized instances (every type listed in `instance-types.yaml`), the instance has a **dedicated path to EBS** that does not compete with network traffic to other instances. EBS bandwidth has its own per-instance ceiling (`ebsBandwidthMBs` in the YAML), independent of the main NIC budget.

On EBS-only instances, this dedicated path is the ceiling for *all* storage I/O — every Kafka write hits this limit. On NVMe instances, EBS bandwidth only matters if you've also attached EBS volumes (e.g. for the OS root volume).

The per-instance limit can still be the binding constraint when your StorageClass IOPS/throughput is provisioned high — `gp3-fast` at 1000 MB/s is meaningless on an `m6i.xlarge` whose EBS baseline is 156 MB/s.

#### 4. CPU generation

Newer is better per-core (`m8i` > `m7i` > `m6i`). Kafka itself is rarely CPU-bound — usually it's network or disk first — but at very high partition counts, with TLS, or with compression, CPU can become the binding constraint.

Graviton (arm64) — `m6g`, `m7g`, `m8g`, `i4g`, `c7g` — gives 20-40% better price/performance than the equivalent x86_64 generation. Dimster supports both architectures; pick one and stay consistent across the cluster (don't mix).

### Baseline ≠ burst

This is the trap. AWS specs advertise both:

- **Baseline** — sustained throughput indefinitely.
- **Burst** — peak throughput for some number of minutes, until burst credits exhaust.

Most spec sheets emphasise burst with phrases like "**up to** 10 Gbps". Burst credits typically last 5-30 minutes depending on the instance and how empty your bucket was. After that, the instance falls back to baseline — without warning.

A benchmark sustaining above-baseline throughput will look great for the first 10 minutes and then mysteriously cliff. You'll spend an afternoon hunting for the broker bug that doesn't exist. **Always size on baseline.**

#### Querying baseline with the AWS CLI

```bash
# Network baseline (Gbps)
aws ec2 describe-instance-types \
  --instance-types m6i.2xlarge \
  --query 'InstanceTypes[0].NetworkInfo.NetworkCards[0].BaselineBandwidthInGbps'

# EBS baseline (MB/s)
aws ec2 describe-instance-types \
  --instance-types m6i.2xlarge \
  --query 'InstanceTypes[0].EbsInfo.EbsOptimizedInfo.BaselineBandwidthInMbps'
```

Or — much faster — open `cloud/eks-cli/instance-types.yaml`, which has `networkBaselineGbps` and `ebsBandwidthMBs` for every supported instance type. That YAML is the source of truth `eks-setup` uses.

### Picking instances for Kafka brokers

The bottleneck order is almost always: **network → disk → memory → CPU**.

| Constraint | What to look at | What to do |
|------------|-----------------|------------|
| Network | `networkBaselineGbps` × 125 = MB/s ceiling. Compare against `dimster resources` peak network value | If too low → bigger size, or jump to `m6in` / `i3en` / `i4i` |
| Disk | EBS: min(instance `ebsBandwidthMBs`, StorageClass throughput). NVMe: instance NVMe spec | If too low → provisioned `gp3-fast` (up to 1000 MB/s) or `io2`, or NVMe |
| Memory | Page cache hit rate for tail consumers | Bigger instance memory or fewer brokers per box |
| CPU | Rare. Watch when partitions ≫ 10k or compression/TLS in play | Newer generation or bigger size |

#### Sizing example

Use `dimster resources` to compute per-broker requirements before picking instances:

```bash
./dimster resources -b 3 \
  -k run/kafka-configs/kafka_safe.yaml \
  -w run/workloads/run-test/run-classic-scaling-10-120MBs.yaml

=== Workload: run-classic-scaling-10-120MBs (run) ===
  Per broker (3 brokers):
  LABEL                NET TOTAL    DISK WR
  120000-msg/s         320 MB/s     120 MB/s
```

So the peak demands per broker are ~320 MB/s network and ~120 MB/s disk write.

- 320 MB/s = 2.56 Gbps network. `m6i.2xlarge` baseline is 3.125 Gbps — *just* enough; `m6in.xlarge` is 6.25 Gbps and gives headroom.
- 120 MB/s disk write is right at the default `gp3` ceiling (125 MB/s). Provision a `gp3-fast` StorageClass with explicit throughput (e.g. 500 MB/s) and you're well clear — `m6i.2xlarge` has 312 MB/s of EBS bandwidth, more than enough for 120 MB/s of writes.

That's the workflow: `dimster resources` tells you the demand, `instance-types.yaml` tells you what each instance can supply, and you pick the smallest instance whose baselines clear the demand by a comfortable margin (1.5×-2× headroom is healthy).

### Picking instances for workers

Workers run producer and consumer clients. The dominant constraints:

1. **Network** — total ingress (consumer fetch responses) + egress (produce requests) across all clients on this worker.
2. **CPU** — serialization, compression, rate limiting, virtual-thread scheduling.
3. **Memory** — producer buffers + consumer fetch buffers, scales with partition count.

Workers are far less demanding than brokers. A common pattern: `workers.replicas: 2` of `m6i.xlarge` is enough for most benchmarks up to ~500 MB/s aggregate throughput. Above that, scale wider (more workers) before going larger (bigger workers) — Kafka clients parallelise cleanly, so 4× `xlarge` beats 1× `4xlarge` for throughput per dollar.

### Picking instances for infra (coordinator + monitoring)

Coordinator + Prometheus + Grafana. None are throughput-sensitive. `t3.medium` is the default and is fine for almost everything. Bump to `t3.large` only if Prometheus runs out of memory at very high cardinality (lots of partitions × lots of consumer groups).

Burstable T-instances are *appropriate* here — these workloads are bursty (Grafana queries, Prometheus scrapes) so burst credits actually cover the real usage pattern.

### Quick-pick by workload shape

| Target throughput (aggregate, RF=3) | Broker pick (3-broker cluster)                                     | Worker pick (2 workers) |
|----|--------------------------------------------------------------------|----|
| < 100 MB/s ingress | `m6i.xlarge` + default `gp3`                                       | `m6i.large` |
| 100-300 MB/s ingress | `m6i.2xlarge` + `gp3-fast` (3000 IOPS, 500 MB/s)                   | `m6i.xlarge` |
| 300-700 MB/s ingress | `m6in.2xlarge` + `gp3-fast` (3000 IOPS, 1000 MB/s)                 | `m6i.2xlarge` |
| 700+ MB/s ingress | `m6in.4xlarge` (1562 MB/s EBS) + `gp3-fast` (5000 IOPS, 1000 MB/s) | `m6in.xlarge` or `m6in.2xlarge` |

For NVMe-specific cases (cost optimisation, ephemeral OK, or instances like `i3en.6xlarge` whose NVMe exceeds their EBS budget), substitute the relevant i-series instance with `pvcStorageClass: local-storage`.

Numbers above are starting points. Always run `dimster resources` against your actual workload to verify the network/disk math holds.

## Scaling nodegroups

Worker autoscaling: change the `--nodes` value at any time.

```bash
eksctl scale nodegroup \
  --cluster ${DIMSTER_NAME}-benchmark \
  --region $AWS_REGION \
  --name workers \
  --nodes 4
```

After scaling, update `workers.replicas` in `dimster-config.yaml` to match, then re-run `./dimster deploy aws`.

You can scale the Kafka nodegroup the same way, but **be aware**: scaling Kafka *down* loses data on the brokers that go away. Treat broker count as something you set up-front and rebuild around if you need to change.

## Teardown

Two scopes — pick the right one.

**Tear down Dimster only, keep the k8s cluster running** (for back-to-back benchmarks):

```bash
./dimster teardown aws
```

**Delete the entire cluster** (when you're done for the day or week):

```bash
./eks-setup delete --answers eks-answers-mytest.yaml
```

The answers file carries the cluster name and region, so this is the lowest-friction path. To delete by name without an answers file:

```bash
./eks-setup delete --name mytest --region us-west-2
```

To keep the ECR repo (so you don't have to re-push the image next time):

```bash
./eks-setup delete --answers eks-answers-mytest.yaml --keep-ecr
```

> **Cost reminder.** EKS control plane is ~$0.10/hr (~$73/month) on top of EC2 instance costs. A 3× `m6i.2xlarge` + 2× `m6i.xlarge` + 1× `t3.medium` setup is roughly $1.30/hr in `us-west-2` on-demand pricing. Delete clusters when not in use.

## Troubleshooting

**Pods stuck in `Pending`.** Either the nodegroup isn't large enough, or labels don't match. Check with `kubectl describe nodes` (Dimster runs this automatically when its readiness check times out). The wizard sizes nodegroups to fit, so this is usually a sign that you increased `kafka.replicas` or `workers.replicas` in the env block past what the nodegroup can host. Either reduce the replica count or scale the nodegroup.

**EBS volumes stuck in `Pending`.** The EBS CSI driver isn't installed or its IAM role is wrong. The wizard handles this automatically — if it failed mid-run, install it manually:

```bash
# 1. OIDC provider (required for IRSA)
eksctl utils associate-iam-oidc-provider \
  --region $AWS_REGION \
  --cluster ${DIMSTER_NAME}-benchmark \
  --approve

# 2. IAM service account
eksctl create iamserviceaccount \
  --cluster ${DIMSTER_NAME}-benchmark \
  --region $AWS_REGION \
  --name ebs-csi-controller-sa \
  --namespace kube-system \
  --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \
  --approve \
  --override-existing-serviceaccounts

# 3. Get the role ARN
EBS_CSI_ROLE_ARN=$(kubectl get sa ebs-csi-controller-sa -n kube-system \
  -o jsonpath='{.metadata.annotations.eks\.amazonaws\.com/role-arn}')

# 4. Install the addon
eksctl create addon \
  --cluster ${DIMSTER_NAME}-benchmark \
  --region $AWS_REGION \
  --name aws-ebs-csi-driver \
  --service-account-role-arn $EBS_CSI_ROLE_ARN \
  --force
```

Verify with `kubectl get pods -n kube-system | grep ebs`.

**Image pull errors (`ErrImagePull`).** Your ECR login expired (12-hour TTL). Re-run the `aws ecr get-login-password ... docker login ...` command from the build section.

**`kubectl` context wrong after creation.** `eksctl` should have set it. Confirm with `kubectl config current-context`. If it's wrong, force-set it: `aws eks update-kubeconfig --region $AWS_REGION --name ${DIMSTER_NAME}-benchmark`.

**Cross-AZ traffic showing up on the AWS bill.** Confirm every nodegroup landed in the same AZ: `kubectl get nodes -L topology.kubernetes.io/zone` should show one zone, and `kubectl get pods -n dimster -o wide` should show every pod on a node in that zone.

## See also

- [05 — Cloud Kubernetes](../../docs/deployment/05-cloud-k8s.md) — shared overview across cloud Kubernetes targets.
- [EKS Setup CLI Reference](../../docs/reference/eks-setup-cli.md) — flags, answers YAML schema, and default sizing formulas.
- [06 — External Kafka](../../docs/deployment/06-external-kafka.md) — if you chose `deploy: external` in the wizard.
- [Internals — what `nodeLabel` and `pvcStorageClass` actually do](../../docs/deployment/internals-scheduling-storage.md) — exact Kustomize patches and supported/unsupported features.
