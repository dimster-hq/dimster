# 07 — External Kafka

Dimster can use an external Kafka cluster/service instead of deploying Kafka in the Dimster Kubernetes cluster. When `deploy: external` is set in the Dimster environment, Dimster deploys workers + coordinator + monitoring into your Kubernetes cluster and points them at the bootstrap servers you provide. 

> One valid option here is to simply provision a single Cloud instance with enough CPU, memory and network, install a local K8s distro such as Minikube or k3d and deploy Dimster into that, which some users might find easier than deploying a full Kubernetes service (such as EKS or GKE). With an external Kafka, Dimster has no specific storage or multi-server needs. 

For the conceptual difference between `full` and `external` mode, see [page 02](02-deploy-modes.md). This page is the per-target recipe book.

To use the Kafka cluster of another Dimster env, for example to run a second workload against it at the same time, use `deploy: external-env` instead of copying its bootstrap servers into a `connection` block. See [10 — Sharing a Kafka cluster](10-shared-kafka.md).

## How `connection:` works

When `deploy: external` is used, the `connection` section tells Dimster how to connect to the Kafka external cluster. Every key under `connection:` is passed straight through to the Kafka client `commonConfig`. So whatever your Kafka client supports — SASL, mTLS, OAuth, ACLs, custom serializers — you put it under `connection:`.

```yaml
environments:
  myExt:
    deploy: external
    connection:
      bootstrap.servers: "broker1:9092,broker2:9092"
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: '...'
      # any other Kafka client property goes here
```

This separation keeps **kafka-config** and **workload** YAML files environment-agnostic. The same workload YAML runs against your laptop's Docker Compose Kafka, your team's staging cluster, and Confluent Cloud — only the env block changes.

## Example of a managed Kafka

The following is an example of pointing Dimster hosted in EKS at a Confluent Cloud Kafka cluster.

```yaml
environments:
  cc:
    image: 123456789.dkr.ecr.us-west-2.amazonaws.com/dimster
    context: arn:aws:eks:us-west-2:123456789:cluster/my-cluster
    deploy: external
    system: "Confluent Cloud Standard cluster"
    connection:
      bootstrap.servers: "pkc-abc123.us-west-2.aws.confluent.cloud:9092"
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: 'org.apache.kafka.common.security.plain.PlainLoginModule required username="API_KEY" password="API_SECRET";'
    workers:
      replicas: 2
      heap: "-Xms2G -Xmx4G"
      requests: { cpu: "2", memory: "6Gi" }
      nodeLabel: role=worker
    coordinator:
      heap: "-Xms512M -Xmx1G"
      requests: { cpu: "500m", memory: "2Gi" }
      nodeLabel: role=infra
```

Get the API key/secret from the Confluent Cloud console (Cluster → API Keys → Create key). The key needs **DeveloperWrite** at minimum — `dimster run` creates topics; without DeveloperManage on the cluster, topic creation will fail.

> **Don't commit secrets to `dimster-config.yaml`.** Either keep the file out of git (it usually is — `.gitignore` covers it after `dimster init`) or use a Kubernetes Secret + `envFrom` patch. The connection block doesn't currently support secret references natively; if that's a hard requirement, the workaround today is to template the file from outside Dimster.

If you plan on test some kind of non-internet facing connectivity, VPC peering is the recommended approach for something like benchmarking which doesn't incur the costs of private networking. 

The exact `connection` fields will be determined by the Kafka service you are using. 

## Recipe 2 — Self-managed Kafka (without mTLS)

Same env, just need to set `bootstrap.servers` in the `connection` section.


```yaml
    connection:
      bootstrap.servers: "kafka:9092"
      # no other fields needed
```

## Recipe 3 — Self-managed Kafka (with mTLS)

For an *external* Kafka cluster, mTLS is just a few extra `connection:` keys — Dimster passes them straight through to the Kafka client. You supply the keystore + truststore files yourself (typically by mounting a Kubernetes Secret into the worker/coordinator pods) and reference them by path:

```yaml
    connection:
      bootstrap.servers: "kafka.example.com:9094"
      security.protocol: SSL
      ssl.keystore.type: PKCS12
      ssl.keystore.location: /etc/kafka/tls/keystore.p12
      ssl.keystore.password: changeit
      ssl.truststore.type: PKCS12
      ssl.truststore.location: /etc/kafka/tls/truststore.p12
      ssl.truststore.password: changeit
```

For mTLS against an in-cluster Kafka cluster that *Dimster* manages (`deploy: full`), the cert generation, Secret distribution, and per-workload opt-in are all automatic — see [07 — In-cluster mTLS](07-mtls-full-mode.md).

## Recipe 4 — Local Docker Compose Kafka

The lightest path: clients in your laptop's local k8s cluster, Kafka as Docker containers on the same laptop. Two ready-to-use compose files live in `docs/examples/external-mode/`:

- `docker-compose.yaml` — single combined broker+controller node. Good for quick smoke tests; can't host RF>1 topics.
- `docker-compose-3node.yaml` — three combined broker+controller nodes. Use this when your workload needs `replicationFactor: 3` or you want to exercise multi-broker behavior.

```bash
docker compose -f docs/examples/external-mode/docker-compose.yaml up -d
# or
docker compose -f docs/examples/external-mode/docker-compose-3node.yaml up -d
```

The single-node compose file exposes two listeners: `localhost:9092` (for local CLI tools) and `host.minikube.internal:19092` (for pods inside Minikube). The three-node file advertises each broker as `host.minikube.internal` on a unique port (`19092`, `19192`, `19292`); list all three in `bootstrap.servers` so the client can complete metadata discovery even if one broker is unreachable. Use the external listener(s) in the env block:

```yaml
environments:
  localExt:
    image: dimster
    context: minikube
    distro: minikube
    deploy: external
    system: "Kafka 4.2 (Docker Compose)"
    connection:
      # Single-node: just the one address.
      # Three-node:  "host.minikube.internal:19092,host.minikube.internal:19192,host.minikube.internal:19292"
      bootstrap.servers: "host.minikube.internal:19092"
    workers:
      replicas: 1
      heap: "-Xms256M -Xmx512M"
      requests: { cpu: "250m", memory: "768Mi" }
    coordinator:
      heap: "-Xms256M -Xmx512M"
      requests: { cpu: "250m", memory: "768Mi" }
```

For Docker Desktop's built-in Kubernetes, replace `host.minikube.internal` with `host.docker.internal`. For kind, the right hostname depends on Docker network mode — check with `kubectl run nettest --rm -it --image=busybox -- nslookup host.docker.internal` first.

### k3d

k3d does not inject a `host.k3d.internal` name by default — pods can't resolve it out of the box. The cleanest fix is to add a `hostAliases` entry to your k3d config so every pod gets an `/etc/hosts` entry pointing at the host. Then both the Compose `KAFKA_ADVERTISED_LISTENERS` and the dimster `bootstrap.servers` can reference the same friendly name.

First, find the gateway IP of the k3d Docker network — that's the host as seen from inside the cluster:

```bash
docker network inspect k3d-<cluster-name> | grep -i gateway
# e.g. "Gateway": "172.18.0.1"
```

Add it as a host alias in your k3d config file (k3d v5.3+):

```yaml
apiVersion: k3d.io/v1alpha5
kind: Simple
metadata:
  name: dimster
hostAliases:
  - ip: 172.18.0.1
    hostnames:
      - host.k3d.internal
```

Recreate the cluster with `k3d cluster create --config k3d-config.yaml`. Verify resolution from inside:

```bash
kubectl run nettest --rm -it --image=busybox --restart=Never -- \
  nc -zv host.k3d.internal 19092
```

Then update the Compose file's `KAFKA_ADVERTISED_LISTENERS` to advertise `host.k3d.internal` (replacing the `host.minikube.internal` entry), restart the broker, and set the same name in the dimster env:

```yaml
    connection:
      bootstrap.servers: "host.k3d.internal:19092"
```

If you can't (or don't want to) edit the k3d config, the fallback is to use the gateway IP directly in both the advertised listener and `bootstrap.servers`. It works, but the IP can change if the k3d network is recreated, so you'll have to update both places again.

Also worth confirming the Kafka container actually publishes on `0.0.0.0`, not `127.0.0.1` — otherwise pods can't reach it via the host gateway IP at all:

```bash
docker ps --format '{{.Names}}\t{{.Ports}}' | grep -i kafka
# want: 0.0.0.0:19092->19092/tcp
```

Build, deploy, run as usual:

```bash
./dimster build localExt
./dimster deploy localExt
./dimster run localExt -k run/kafka-configs/kafka_safe.yaml \
  -w run/workloads/quick-test/quick-run-test.yaml
```

Teardown both:

```bash
./dimster teardown localExt
docker compose -f docs/examples/external-mode/docker-compose.yaml down
```

## What's different at runtime in external mode

- **No Kafka pods.** The `kafka` StatefulSet isn't created.
- **No Kafka readiness check.** `dimster run` skips the broker readiness probe and starts the benchmark immediately. Make sure your external Kafka is *actually* ready, or you'll watch your benchmark error out for the first few seconds.
- **Empty Kafka Broker dashboard.** Grafana's Kafka Broker dashboard scrapes JMX from the in-cluster Kafka pods. With no pods, no data. Other dashboards (workers, coordinator, system) work fine.
- **`dimster kafka` unavailable.** That command shells into a Kafka pod for ad-hoc topic ops; doesn't apply when there's no pod.
- **`replicationFactor` must match what the cluster supports.** A workload set up for RF=3 will fail to create topics on a single-broker Docker Compose Kafka.

## Troubleshooting

**Workers can't connect.** Test from inside a pod, not from your laptop, to rule out network path issues:

```bash
kubectl -n dimster run nettest --rm -it --image=busybox --restart=Never -- \
  nc -vz pkc-abc123.us-west-2.aws.confluent.cloud 9092
```

If that fails, the issue is networking (security group, VPC peering, DNS) — fix that before touching the env block.

**Authentication errors in worker logs.** `kubectl logs -n dimster -l app=benchmark-worker` and look for `SaslAuthenticationException`. Confirm the JAAS config string format — single quotes around the whole value, semicolon at the end, no escape weirdness.

**Topic creation fails.** Either the auth principal lacks topic-create permission, or the workload's `replicationFactor` exceeds the broker count. For Confluent Cloud, the API key needs `DeveloperManage` on the cluster.

**`host.minikube.internal` doesn't resolve.** Older Minikube versions don't ship with this. Either upgrade Minikube, or get the host IP and use that:

```bash
minikube ssh -- route -n | grep ^0.0.0.0 | awk '{print $2}'
```

Use the printed IP in both the Docker Compose `KAFKA_ADVERTISED_LISTENERS` env var and the `connection` section's `bootstrap.servers` field.

## What's next

- [02 — Deploy modes](02-deploy-modes.md): the conceptual difference between `full` and `external`.
- [05 — Cloud Kubernetes](05-cloud-k8s.md): if you want Dimster to own Kafka on EKS or GKE instead of pointing at MSK/CC.
