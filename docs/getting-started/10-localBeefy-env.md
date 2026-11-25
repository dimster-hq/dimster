# 10 — A beefier local env

The starter `local` env is a minimal env (no kafka/workers/coordinator sections), so deploy fills in tiny defaults — 1 broker, 250m CPU, 768Mi memory, 1Gi disk. That's enough to learn the workflow but will throttle hard on anything more ambitious than the quick-test workloads.

If your laptop/workstation has plenty of resources, defining a `localBeefy` env (though name it what you want) gives you headroom to push more representative load, with multiple brokers.

## Add the env block

Open `dimster-config.yaml` and add this block under `environments:` (next to your existing `local` env):

```yaml
    localBeefy:
    image: dimster
    context: minikube
    hardware: minikube
    deploy: full
    kafka:
      replicas: 3
      version: 4.2.0
      pvcSize: 10Gi
      heap: -Xms256M -Xmx512M
      requests:
        cpu: 1000m
        memory: 2Gi
      brokerConfig: run/broker-configs/default.yaml
      logConfig: run/log4j-configs/info.yaml
    workers:
      replicas: 1
      heap: -Xms256M -Xmx512M
      requests:
        cpu: 1
        memory: 768M
    coordinator:
      heap: -Xms256M -Xmx512M
      requests:
        cpu: 500m
        memory: 768M
```

This asks for roughly **5 CPU and 9 GiB of memory** across the Dimster pods.

Dimster has experimental validation for kubelet CPU Manager settings. If you
want the local cluster to behave more like a smaller benchmark machine, you can
ask Dimster to validate those settings before deploy:

```yaml
    cpuManager:
      policy: static
      fullPcpusOnly: true
```

This is validation-only and experimental. Dimster does not configure kubelet,
because CPU Manager is node-level configuration rather than a namespaced
Kubernetes resource. With `policy: static`, Kafka and worker CPU requests must
be whole effective CPUs (`"1"`, `"2"`, `"4"`, or `"4000m"`), and the selected
nodes must already run kubelet `cpuManagerPolicy: static`. With
`fullPcpusOnly: true`, those nodes must also enable
`cpuManagerPolicyOptions.full-pcpus-only`, so a `cpu: "4"` pod is allocated
complete physical cores on SMT systems instead of arbitrary hardware threads.
If your cluster blocks kubelet `/configz` access, keep `cpuManager.policy: none`
and manage CPU Manager outside Dimster.

If you run Kafka outside of Dimster, create a `localBeefyExt` env:

```yaml
  localBeefy:
    image: dimster
    context: minikube              # change to your kubectl context if not minikube
    distro: minikube               # Options: minikube | k3d | kind | docker-desktop
    deploy: external
    connection:
      bootstrap.servers: host.minikube.internal:19092 # or whatever your bootstrap servers is
    workers:
      replicas: 1
      heap: "-Xms512M -Xmx2G"
      requests:
        cpu: "2"
        memory: "3Gi"
    coordinator:
      heap: "-Xms512M -Xmx1G"
      requests:
        cpu: "500m"
        memory: "1Gi"
```

> A reminder from page 04: the built-in tiny defaults apply to any env that has no `kafka:`, `workers:`, or `coordinator:` sections (a "minimal" env). As soon as you add any of those sections — as this env does — you take over explicit sizing. That's why this block is longer than `local`'s.

## Resize the cluster to fit

The new env asks for ~5 CPU and 9 GiB; your cluster needs at least that, plus headroom. The exact resize commands depend on which distro you started on page 03.

**minikube** — stop and restart with bigger sizing (state survives):
```bash
minikube stop
minikube start --cpus=6 --memory=10g --disk-size=40g
```

**k3d / kind** — these inherit limits from your Docker daemon. On macOS with Colima, restart Colima with bigger sizing:

```bash
colima stop
colima start --cpu 6 --memory 10 --disk 40
# then re-create the cluster (k3d cluster create / kind create cluster)
```

**Docker Desktop / OrbStack** — bump CPU/Memory in the app's Settings → Resources panel; the embedded Kubernetes picks up the new limits after a restart.

The extra disk room matters: each Kafka PVC alone is 10 GiB and 3+ GB drain-backlog tests can easily fill a small default disk.

## Build, deploy, run

The same three-step flow as before, just pointed at the new env:

```bash
./dimster build localBeefy
./dimster deploy localBeefy
./dimster run localBeefy \
    -k run/kafka-configs/kafka_local.yaml \
    -w run/workloads/quick-test/quick-run-test.yaml
```

Note: `dimster build localBeefy` rebuilds the same Docker image as `dimster build local` (both envs use `image: dimster`); the build is cached, so this step is near-instant if you've already built `local`.

Tear it down the same way as `local`:

```bash
./dimster teardown localBeefy
```

## Running two instances in parallel

If your laptop has the headroom, you can run `local` and `localBeefy` (or any two envs) at the same time on the same cluster. Each env carries its own namespace so the two deployments stay completely isolated — separate pods, services, results PVCs, and Prometheus/Grafana stacks. Useful when you want to A/B two configurations without tearing one down to deploy the other.

Add a per-env `namespace:` to whichever env should land outside the default `dimster` namespace:

```yaml
  local:
    image: dimster
    context: minikube
    # (uses the global kubernetes.namespace, which defaults to `dimster`)
    ...

  localBeefy:
    image: dimster
    context: minikube
    namespace: dimster2          # side-by-side with `local` in `dimster`
    ...
```

Then in two terminals:

```bash
# terminal A
./dimster build local && ./dimster deploy local
./dimster run local -k ... -w ...

# terminal B (concurrently)
./dimster build localBeefy && ./dimster deploy localBeefy
./dimster run localBeefy -k ... -w ...
```

`dimster teardown local` only touches the `dimster` namespace, leaving `localBeefy` running. The same pattern works across separate clusters too — set a different `context:` per env (e.g. `k3d-dimster` and `k3d-banana`) to deploy onto two k3d / kind clusters simultaneously.

## When `localBeefy` still isn't enough

For any serious benchmarking you'll need to deploy to a production-like environment. The next stop is a real multi-node Kubernetes cluster:

- [Deploying Dimster — overview](../deployment/) — start here for the mental model and to pick your target.
- [Cloud Kubernetes](../deployment/05-cloud-k8s.md) — production-scale benchmarks on AWS EKS (`eks-setup`) or GCP GKE (`gke-setup`), with separate nodegroups for Kafka, workers, and monitoring.
- [External Kafka](../deployment/06-external-kafka.md) — point Dimster workers at Confluent Cloud, MSK, or anything else that speaks the Kafka protocol.

The `exampleRemoteFull` and `exampleRemoteExt` blocks already in your `dimster-config.yaml` are starter templates for those shapes.

## If something looks wrong

- `dimster deploy localBeefy` leaves pods in `Pending` — your local cluster doesn't have the headroom you asked for. Resize it (see the "Resize the cluster to fit" section above) or trim the resource requests in your env block.
- Throughput is barely better than `local` — confirm the deploy is actually using the new sizes with `kubectl describe pod kafka-0 -n dimster | grep -E "(Requests|Limits)"`. A typo in the env block will silently fall back to the in-code defaults for non-`local` envs (which are bigger than `local` but not as big as `localBeefy`).
