# dimster-config.yaml Reference

The `dimster-config.yaml` file is the central configuration for all Dimster CLI operations that interact with the k8s cluster. It defines Kubernetes settings, Docker image details, results storage, and one or more named environments.

The CLI looks for this file in the project root directory (where `pom.xml` lives). Override the path with the `DIMSTER_CONFIG` environment variable.

---

## Full Schema

```yaml
kubernetes:
  namespace: <string>         # K8s namespace (default: "dimster")
  context: <string>           # Default kubectl context (optional)

image:
  tag: <string>               # Docker image tag (default: "latest")

results:
  dir: <string>               # Local results directory (default: "./results")

environments:
  <name>:                     # Named environment (e.g. "local", "aws")
    image: <string>           # Docker image name or registry URL (optional)
    context: <string>         # kubectl context for this environment (optional)
    namespace: <string>       # K8s namespace for this env (optional, falls back to kubernetes.namespace)
    distro: <string>          # Local k8s distro: minikube|k3d|kind|docker-desktop (optional)
    system: <string>          # System-under-test label (optional)
    hardware: <string>        # Hardware description for metadata (optional)
    kafka_client_version: <string>  # Concrete kafka-clients version, e.g. "3.7.2", "4.1.0", "4.2.0" (default: "4.2.0")
    deploy: <string>          # "full" or "external" (default: "full")
    cpuManager:               # Experimental kubelet CPU Manager validation
      policy: <string>        # none|static (default: none)
      fullPcpusOnly: <bool>   # Require full physical cores when policy is static
    metricsOutput: [<string>] # Metrics saved after a run: html, snapshot (default: [html])

    connection:               # Required when deploy is "external"
      bootstrap.servers: <string>
      <key>: <value>          # Additional Kafka client properties

    kafka:                    # Kafka broker config (full mode only)
      replicas: <int>
      image: <string>           # Docker image name (default: "apache/kafka")
      version: <string>         # Combined with image, e.g. "4.2.0" ==> "apache/kafka:4.2.0"
      pvcSize: <string>         # Per-broker PVC size (e.g. "500Gi")
      pvcStorageClass: <string> # Kubernetes StorageClass for broker PVCs
      heap: <string>            # JVM heap flags
      env:                      # Arbitrary environment variables for broker pods
        <ENV_VAR_NAME>: <value>
      requests:                 # Kubernetes resource requests/limits
        cpu: <string>
        memory: <string>
      brokerConfig: <string>    # Path to broker config YAML
      extraConfig: <string>     # Inline overrides appended after brokerConfig
      logConfig: <string>       # Path to log4j config YAML
      logMaxFileSize: <string>  # log4j MaxFileSize override (e.g. "200MB")
      logMaxBackupIndex: <int>  # log4j MaxBackupIndex override
      nodeLabel: <string>

    workers:                  # Worker pod config
      replicas: <int>
      heap: <string>
      requests:
        cpu: <string>
        memory: <string>
      nodeLabel: <string>
      assignment:             # Producer/consumer split (optional)
        mode: <string>          # mixed (default) | separated
        ratio: <string>         # e.g. "1:2" (separated mode)
        producers: <int>        # explicit producer count (separated mode)
        consumers: <int>        # explicit consumer count (separated mode)

    coordinator:              # Coordinator pod config
      heap: <string>
      requests:
        cpu: <string>
        memory: <string>

    monitoring:               # Monitoring stack config
      nodeLabel: <string>       # Node selector for Grafana and Prometheus
      prometheus:               # Resource overrides for Prometheus (optional)
        requests:
          cpu: <string>
          memory: <string>
      grafana:                  # Resource overrides for Grafana (optional)
        requests:
          cpu: <string>
          memory: <string>
```

---

## Top-level Sections

### kubernetes

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `namespace` | string | `dimster` | Default Kubernetes namespace for Dimster resources. Overridden by per-environment `namespace` if set. |
| `context` | string | -- | Default kubectl context. Overridden by per-environment `context` if set. |

### image

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tag` | string | `latest` | Docker image tag used during build and deploy. |

### results

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dir` | string | `./results` | Local directory for storing benchmark results, logs, and charts. |

---

## Environment Fields

Each entry under `environments` defines a named deployment target.

### Core Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `image` | string | -- | Docker image name. Plain names (no dots) are treated as local images (loaded into the local k8s distro with `imagePullPolicy: IfNotPresent`). Names containing dots are treated as registry URLs (pushed with `docker push`, pulled with `imagePullPolicy: Always`). |
| `context` | string | -- | kubectl context for this environment. Overrides the global `kubernetes.context`. |
| `namespace` | string | -- | Kubernetes namespace for this environment. Overrides the global `kubernetes.namespace`. Set this per-env (and optionally `context`) to deploy two dimster instances side-by-side — e.g. `local` in `dimster` and `localTwo` in `dimster2`. `dimster build/deploy/run/teardown <env>` then operates entirely within that namespace. |
| `distro` | string | `minikube` | Local Kubernetes distribution used to load freshly built local images. One of `minikube`, `k3d`, `kind`, or `docker-desktop` (also accepts `orbstack`). Ignored for envs whose image is pushed to a registry. |
| `system` | string | -- | System-under-test label recorded in result metadata (e.g. `"Kafka 4.2"`, `"Redpanda 24.3"`). |
| `hardware` | string | -- | Hardware description recorded in result metadata (e.g. `"3x m7i.2xlarge"`). |
| `kafka_client_version` | string | `4.2.0` | Concrete `kafka-clients` version the worker and coordinator pods are built against (e.g. `"3.7.2"`, `"3.9.0"`, `"4.1.0"`, `"4.2.0"`). The CLI maps the version to the matching impl module and passes it through to Maven as `-Dkafka.version=<version>`, so the bundled jar is exactly what you ask for. Independent of `kafka.version` (the broker image tag). See [Kafka Versioning](../concepts/kafka-versioning.md) for the supported versions and the cross-version compatibility matrix. |
| `deploy` | string | `full` | Deploy mode. `full` deploys Kafka brokers alongside workers. `external` connects to an existing Kafka cluster via the `connection` section. |
| `metricsOutput` | list of strings | `[html]` | Prometheus metrics saved into the result directory after each run. `html` renders the Grafana dashboards as static HTML; `snapshot` saves the raw Prometheus data for the run, viewable later with `dimster dashboard --snapshot`. `[]` saves no metrics. Overridden per command by `--metrics-output`. See [Results and Charts](../concepts/results-and-charts.md#saving-metrics-after-a-run). |
| `cpuManager.policy` | string | `none` | Experimental, validation-only kubelet CPU Manager requirement. `none` disables the check. `static` makes `dimster deploy` verify benchmark nodes are running kubelet `cpuManagerPolicy: static` before applying manifests. |
| `cpuManager.fullPcpusOnly` | boolean | `false` | Experimental. When `policy: static`, require kubelet `cpuManagerPolicyOptions.full-pcpus-only: true` so whole physical cores are allocated on SMT systems. |

### cpuManager

`cpuManager` is experimental. It is useful when you want local or remote
benchmark pods to behave more like a smaller machine, but it depends on kubelet
configuration details that vary across Kubernetes distributions and managed
clusters. Dimster does not configure kubelet settings, because they live on the
node rather than in namespaced Kubernetes manifests. Instead, `dimster deploy`
validates that the selected benchmark nodes already match the environment's
expectation and fails before any resources are applied if they do not.

Because this feature is experimental, treat validation failures as a signal to
inspect your cluster's kubelet configuration directly. If `/configz` is blocked
by RBAC or by the provider, either grant that access for the deploy check or set
`cpuManager.policy: none` and manage CPU Manager outside Dimster.

The check applies to Kafka brokers and workers only. Coordinator and monitoring
pods are not benchmark dataplane pods, so they are not part of the CPU Manager
preflight.

Example:

```yaml
cpuManager:
  policy: static
  fullPcpusOnly: true
```

With Kubernetes CPU Manager `static`, Guaranteed pods with whole-CPU requests can
receive exclusive CPUs. Dimster already renders equal CPU/memory requests and
limits for Kafka and workers, so use whole effective CPU values such as `"1"`,
`"2"`, `"4"`, or `"4000m"`. Fractional values such as `"250m"` or `"1.5"` remain
in the shared CPU pool and are rejected when `policy: static` is configured.

With `fullPcpusOnly: true`, kubelet must have the `full-pcpus-only` policy option
enabled. On a typical SMT=2 machine, a pod requesting `cpu: "4"` is then admitted
only if kubelet can allocate two complete physical cores.

References: Kubernetes [Resource managers](https://kubernetes.io/docs/concepts/workloads/resource-managers/)
and [Pod QoS classes](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/).

### connection

Required when `deploy` is `external`. All properties are injected into the Kafka clients' `commonConfig` at runtime.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `bootstrap.servers` | string | Yes | Kafka bootstrap servers address. |
| *(any other key)* | string | No | Additional Kafka client properties injected into `commonConfig`. |

Common connection properties:

| Property | Example Values |
|----------|---------------|
| `security.protocol` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `sasl.mechanism` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER` |
| `sasl.jaas.config` | JAAS login module configuration string |
| `ssl.truststore.location` | Path to truststore file |

### kafka

Kafka broker deployment settings. Only used when `deploy` is `full`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `replicas` | integer | -- | Number of Kafka broker pods. |
| `image` | string | `apache/kafka` | Docker image name (without tag). Combined with `version` to form the full image reference (e.g. `apache/kafka:4.2.0`). |
| `version` | string | -- | Kafka version (e.g. `4.2.0`). Used as the image tag. The default `apache/kafka` repository's oldest published tag is `3.7` — to benchmark against an older broker, point `image` at a different repository (e.g. a Bitnami or custom build). |
| `pvcSize` | string | -- | Persistent volume size per broker (e.g. `"500Gi"`). |
| `pvcStorageClass` | string | -- | Kubernetes StorageClass name for broker PVCs. |
| `heap` | string | -- | JVM heap flags (e.g. `"-Xms8G -Xmx8G"`). Shorthand for `env.KAFKA_HEAP_OPTS`. |
| `env` | map | -- | Arbitrary environment variables set on broker pods. Keys are env var names (case-sensitive, e.g. `KAFKA_OPTS`, `CUSTOM_VAR`). When both `heap` and `env.KAFKA_HEAP_OPTS` are set, the explicit `env` value wins. Use for JVM tuning, custom Docker images, or any broker startup config that requires env vars. See [Environment Variables and JVM Options](#environment-variables-and-jvm-options). |
| `requests.cpu` | string | -- | CPU request/limit. Also sets the `KAFKA_CPU` env var (integer cores, used for Kafka thread calculations). Accepts millicores (e.g. `"250m"`) or whole cores (e.g. `"6"`). |
| `requests.memory` | string | -- | Memory request/limit (e.g. `"16Gi"`). |
| `brokerConfig` | string | -- | Path to a broker config YAML file. Properties are injected as `KAFKA_EXTRA_CONFIG` at deploy time. See [Broker Configuration](#broker-configuration). |
| `extraConfig` | string | -- | Additional Kafka broker properties, appended after `brokerConfig` (last-wins). See [Broker Configuration](#broker-configuration). |
| `logConfig` | string | -- | Path to a log4j config YAML file applied to the broker. |
| `logMaxFileSize` | string | -- | Override log4j `MaxFileSize` for the server log file (e.g. `"200MB"`). Applied on top of `logConfig` if both are set. |
| `logMaxBackupIndex` | integer | -- | Override log4j `MaxBackupIndex` for the server log file (e.g. `20`). Applied on top of `logConfig` if both are set. |
| `nodeLabel` | string | -- | Kubernetes node selector label (e.g. `"role=kafka"`). Only applied when explicitly set -- no default node affinity is added. |

When `replicas` is 1, a single-broker ConfigMap is automatically applied with adjusted replication factors for internal topics.

### workers

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `replicas` | integer | -- | Number of worker pods. |
| `heap` | string | -- | JVM heap flags (e.g. `"-Xms4G -Xmx4G"`). |
| `requests.cpu` | string | -- | CPU request/limit. |
| `requests.memory` | string | -- | Memory request/limit. |
| `nodeLabel` | string | -- | Kubernetes node selector label (e.g. `"role=worker"`). |
| `assignment` | object | -- | How producer/consumer instances are split across the worker pool. See below. |

#### workers.assignment

Controls whether each worker pod runs both producers and consumers (`mixed`) or whether producer and consumer roles are pinned to dedicated workers (`separated`). Forwarded to the coordinator on each `dimster run`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mode` | string | `mixed` | `mixed` (every worker runs both roles) or `separated` (dedicated producer/consumer workers). |
| `ratio` | string | -- | Producer:consumer worker ratio in `separated` mode (e.g. `"1:2"`). Mutually exclusive with `producers`/`consumers`. |
| `producers` | integer | -- | Explicit number of producer workers in `separated` mode. Pair with `consumers`. |
| `consumers` | integer | -- | Explicit number of consumer workers in `separated` mode. Pair with `producers`. |

Example:

```yaml
workers:
  replicas: 6
  assignment:
    mode: separated
    ratio: "1:2"
```

### coordinator

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `heap` | string | -- | JVM heap flags. |
| `requests.cpu` | string | -- | CPU request/limit. |
| `requests.memory` | string | -- | Memory request/limit. |

The coordinator deployment uses `strategy: Recreate` because it has a ReadWriteOnce PVC that cannot be attached to multiple nodes simultaneously.

### monitoring

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `nodeLabel` | string | -- | Node selector label for Grafana and Prometheus pods. |
| `prometheus.requests.cpu` | string | -- | Override Prometheus CPU request/limit. |
| `prometheus.requests.memory` | string | -- | Override Prometheus memory request/limit. |
| `grafana.requests.cpu` | string | -- | Override Grafana CPU request/limit. |
| `grafana.requests.memory` | string | -- | Override Grafana memory request/limit. |

`prometheus` and `grafana` resource overrides only take effect when both `cpu` and `memory` are set; otherwise the base manifest defaults apply.

---

## Broker Configuration

The `brokerConfig` and `extraConfig` fields control Kafka server properties applied at deploy time. These are distinct from the [Kafka client configs](kafka-configs.md) used by producers and consumers during benchmarks.

### How it works

At deploy time, the CLI reads broker config properties and passes them to the Kafka StatefulSet as the `KAFKA_EXTRA_CONFIG` environment variable. On pod startup, the entrypoint script:

1. Generates `/tmp/server.properties` from the base ConfigMap template
2. Calculates `num.network.threads` and `num.io.threads` from `KAFKA_CPU` (see [Thread Calculation](#thread-calculation))
3. Appends `KAFKA_EXTRA_CONFIG` properties (last-wins, so they override any earlier values)
4. Starts Kafka with the generated config

This means `/tmp/server.properties` is the actual config file used by Kafka -- not the on-disk `server.properties` or `broker.properties`. The config is regenerated on every pod restart, so it survives restarts.

### Property precedence (last-wins)

```
Base ConfigMap template (lowest priority)
  ↓
num.network.threads / num.io.threads (auto-calculated from KAFKA_CPU)
  ↓
brokerConfig file properties
  ↓
extraConfig properties (highest priority)
```

If the same property appears in multiple layers, the last one wins.

### Broker config files

Broker config files live in `run/broker-configs/` and have this format:

```yaml
name: default
properties:
  default.replication.factor: 3
  min.insync.replicas: 2
  log.retention.hours: 1
  # ... any Kafka server property
```

Available configs:

| File | Description |
|------|-------------|
| `run/broker-configs/default.yaml` | Multi-broker defaults (RF=3, ISR=2, share groups enabled) |
| `run/broker-configs/single-broker.yaml` | Single-broker defaults (RF=1, ISR=1) |

Reference a broker config in your environment:

```yaml
kafka:
  brokerConfig: "run/broker-configs/default.yaml"
```

### extraConfig

Use `extraConfig` for inline property overrides. It is a comma-separated string of `key=value` pairs, appended after the broker config file properties:

```yaml
kafka:
  brokerConfig: "run/broker-configs/default.yaml"
  extraConfig: "num.network.threads=8,num.io.threads=16"
```

This is useful for overriding specific properties without creating a new broker config file.

### Thread calculation

The entrypoint automatically sets thread counts based on the `cpu` field:

| Property | Formula | Minimum |
|----------|---------|---------|
| `num.network.threads` | 1 per CPU core | 3 |
| `num.io.threads` | 2 per CPU core | 8 |

For example, `cpu: "6"` produces `num.network.threads=6` and `num.io.threads=12`.

To override these defaults, set them in `extraConfig`:

```yaml
kafka:
  cpu: "6"
  extraConfig: "num.network.threads=4,num.io.threads=8"
```

### Single-broker mode

When `replicas` is 1 and no `brokerConfig` is specified, Dimster automatically applies a single-broker ConfigMap with adjusted replication factors (all set to 1). If you specify a `brokerConfig` with `replicas: 1`, the automatic single-broker config is skipped and your broker config is used instead -- make sure it sets appropriate replication factors for a single broker.

---

## Environment Variables and JVM Options

The `kafka.env` field lets you set arbitrary environment variables on broker pods. This is useful for:

- JVM tuning (when using the `apache/kafka` image or similar)
- Custom startup scripts that read env vars
- Overriding image-specific defaults
- Working with non-Apache Kafka images (e.g. Confluent, Redpanda) that use different env var names

### Using env with apache/kafka

The `apache/kafka` image's `kafka-run-class.sh` script reads several env vars to control JVM behavior:

- **`KAFKA_HEAP_OPTS`** — heap size (e.g. `"-Xms8G -Xmx8G"`). Shorthand: use `kafka.heap` instead of setting this directly.
- **`KAFKA_JVM_PERFORMANCE_OPTS`** — GC selector and performance flags. Upstream default: `"-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15"`. Set this to replace the defaults (e.g. switch to ZGC, tune G1 parameters).
- **`KAFKA_OPTS`** — arbitrary JVM flags. The JMX exporter agent (`-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9404:/opt/jmx-exporter/config.yml`) is automatically prepended when you set this via `env`, so Grafana metrics always work. Any flags you specify are appended after the agent.
- **`KAFKA_GC_LOG_OPTS`** — GC logging config. Upstream default writes to `$LOG_DIR/kafkaServer-gc.log` with rotation. Override to change log path or rotation settings.

**Example: Replace GC and add diagnostics**

```yaml
kafka:
  heap: "-Xms8G -Xmx8G"
  env:
    KAFKA_JVM_PERFORMANCE_OPTS: "-server -XX:+UseZGC"
    KAFKA_OPTS: "-XX:+AlwaysPreTouch -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/var/kafka-data/heap-dumps/"
```

Note: The JMX exporter agent is automatically prepended to `KAFKA_OPTS`, so you only need to specify your additional flags.

**Example: Use heap shorthand + tune G1**

The `heap` field is just sugar for `env.KAFKA_HEAP_OPTS`. If you set both, the explicit `env` value wins:

```yaml
kafka:
  heap: "-Xms8G -Xmx8G"  # Shorthand
  env:
    KAFKA_JVM_PERFORMANCE_OPTS: "-server -XX:+UseG1GC -XX:MaxGCPauseMillis=50 -XX:G1HeapRegionSize=16M -XX:+AlwaysPreTouch"
```

### Non-Apache images

For custom Docker images that don't use the standard Kafka env var names, set whatever vars your image expects:

```yaml
kafka:
  image: myregistry/custom-kafka
  version: "1.0"
  env:
    CUSTOM_HEAP_SIZE: "8G"
    CUSTOM_GC_TYPE: "G1"
    MY_FEATURE_FLAG: "enabled"
```

---

## Image Resolution

The `image` field determines how Docker images are handled during `dimster build`:

| Image Format | Example | Behavior |
|-------------|---------|----------|
| Plain name (no dots) | `dimster` | Loaded into Minikube via `minikube image load`. Uses `imagePullPolicy: IfNotPresent`. |
| Registry URL (contains dots) | `123456789.dkr.ecr.us-east-1.amazonaws.com/dimster` | Tagged and pushed to the registry. Uses `imagePullPolicy: Always`. |

---

## Examples

### Local development

```yaml
kubernetes:
  namespace: dimster

image:
  tag: latest

results:
  dir: ./results

environments:
  local:
    image: dimster
    distro: minikube
    context: minikube
    kafka:
      replicas: 1
      version: "4.2.0"
      pvcSize: 1Gi
      heap: "-Xms256M -Xmx256M"
      requests:
        cpu: "250m"
        memory: "512Mi"
    workers:
      replicas: 1
      heap: "-Xms512M -Xmx512M"
      requests:
        cpu: "500m"
        memory: "768Mi"
    coordinator:
      heap: "-Xms512M -Xmx512M"
      requests:
        cpu: "250m"
        memory: "768Mi"
```

### AWS EKS with full deploy

```yaml
kubernetes:
  namespace: dimster

image:
  tag: latest

results:
  dir: ./results

environments:
  aws:
    image: 123456789.dkr.ecr.us-east-1.amazonaws.com/dimster
    context: dimster@dimster-benchmark.us-east-1.eksctl.io
    system: "Kafka 4.2"
    hardware: "3x m7i.2xlarge"
    deploy: full
    kafka:
      replicas: 3
      version: "4.2.0"
      pvcSize: 500Gi
      pvcStorageClass: gp3-kafka
      brokerConfig: "run/broker-configs/default.yaml"
      heap: "-Xms8G -Xmx8G"
      requests:
        cpu: "6"
        memory: "24Gi"
      nodeLabel: "role=kafka"
    workers:
      replicas: 4
      heap: "-Xms4G -Xmx4G"
      requests:
        cpu: "3"
        memory: "12Gi"
      nodeLabel: "role=worker"
    coordinator:
      heap: "-Xms2G -Xmx2G"
      requests:
        cpu: "1"
        memory: "4Gi"
    monitoring:
      nodeLabel: "role=infra"
```

### External Kafka cluster

```yaml
environments:
  confluent:
    image: 123456789.dkr.ecr.us-east-1.amazonaws.com/dimster
    context: dimster@dimster-benchmark.us-east-1.eksctl.io
    system: "Confluent Cloud"
    deploy: external
    connection:
      bootstrap.servers: pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: >-
        org.apache.kafka.common.security.plain.PlainLoginModule required
        username="APIKEY" password="APISECRET";
    workers:
      replicas: 4
      heap: "-Xms4G -Xmx4G"
      requests:
        cpu: "3"
        memory: "12Gi"
```

In `external` mode, Dimster does not deploy Kafka pods. The `connection` properties are injected into the `commonConfig` of all Kafka client configurations at runtime, replacing any existing `bootstrap.servers` lines and appending additional connection properties.
