# 10 — Sharing a Kafka cluster between envs

Sometimes you want to see how two workloads interact: how a backlog drain affects the latency of a steady producer, or how a share-group workload competes with a classic consumer group. Dimster does this by running each workload from its own env, with its own coordinator, workers and monitoring, against the same Kafka cluster. The envs are independent; they share nothing but the Kafka cluster.

The Kafka cluster can be deployed by one of the envs, or be external to all of them.

## One env deploys Kafka, the others use it

Give the other envs `deploy: external-env` and name the env that hosts the Kafka cluster in the `externalEnv` block:

```yaml
environments:
  local:                       # deploys Kafka (deploy: full is the default)
    image: dimster
    context: minikube
    kafka:
      replicas: 3

  localB:
    image: dimster
    context: minikube          # the same Kubernetes cluster as local
    namespace: dimster-b       # its own namespace (see below)
    deploy: external-env
    externalEnv:
      environment: local       # the env hosting the Kafka cluster
    workers:
      replicas: 1
```

`external-env` works like `external` (page [02](02-deploy-modes.md)), except that the connection comes from the env named in `externalEnv.environment` rather than from a `connection` block:

- `dimster deploy localB` deploys a coordinator, workers, Prometheus and Grafana into `dimster-b`, and no Kafka.
- `dimster run localB …` waits for `local`'s brokers to be ready, then points the workload at them: `kafka-0.kafka-headless.dimster.svc.cluster.local:9092,…`. Each broker's DNS name resolves from any namespace.
- It uses the brokers' PLAINTEXT listener, which stays up when the hosting env enables TLS. An `external-env` env has no TLS settings of its own, so it can't run [TLS workloads](07-mtls-full-mode.md).

Dimster checks the env before any command runs. The env named in `externalEnv.environment` must exist and be a `full` or `external` env, not another `external-env` env. A `full` host must be on the same kubectl `context`, because its brokers are only reachable inside its Kubernetes cluster. The env must not have a `connection` or `kafka` block.

## Kafka external to every env

When the cluster isn't deployed by Dimster (Confluent Cloud, MSK, a team's cluster), either:

- give every env `deploy: external` and the same `connection` block, or
- give one env the `connection` block and point the others at it with `deploy: external-env`, so the credentials live in one place.

The envs can then run on any Kubernetes clusters, the same or different.

## Each env needs its own namespace

Every resource Dimster deploys has a fixed name: the `benchmark-coordinator` and `benchmark-worker` Deployments, the `benchmark-results` PVC, `prometheus`, `grafana`. Commands also find the coordinator and workers by fixed labels. Two envs in one namespace would overwrite each other's pods, and each run would pick up the other env's workers. So envs on the same Kubernetes cluster need different `namespace:` values (the existing per-env field). An `external-env` env that would share its host's namespace is rejected with a message saying so.

## Topic tags: each env owns its topics

At the start and end of every test point, a coordinator deletes the test topics and consumer groups left on the cluster. On a shared cluster it must only delete its own. So every env names everything it creates under its **topic tag**:

| Resource | Name |
|---|---|
| Test topics | `dimster-<tag>-<suffix>-0000`, … |
| Flink output topics | `dimster-<tag>-<suffix>-0000-out`, … |
| Availability probe topic | `dimster-<tag>-avail-probe` |
| Consumer and share groups | `dimster-<tag>-grp-000-<suffix>`, … |
| Flink job source group | `dimster-<tag>-grp-flink-<suffix>` |
| Kept drain backlogs (`reuseTopic`) | `dimst-no-delete-<tag>-<hash>-0000`, … |

Its cleanup deletes only `dimster-<tag>-…` topics and groups. Kept backlogs are never deleted by cleanup, and including the tag in their names means two envs with the same build settings keep separate backlogs.

The tag is the env name unless the env sets `topicTag`:

```yaml
  localB:
    topicTag: drain            # topics dimster-drain-…
```

A tag can use letters, digits, `.` and `_`, but not `-`. The `-` after the tag ends it, so one tag's names never start with another's prefix: `dimster-a-` never matches `dimster-ab-…`. An env whose name has other characters, such as `local-2`, must set `topicTag`.

Every env is tagged, whether or not it shares its cluster. Only [embedded mode](../getting-started/11-embedded-mode-without-k8s.md), which has no env, uses the untagged names (`dimster-<suffix>-0000`, `dimster-grp-…`). Its cleanup deletes every `dimster*` topic, so don't run it against a shared cluster.

## Running the workloads

Deploy the envs, then start a workload on each from its own terminal:

```bash
dimster deploy local
dimster deploy localB

dimster run local  -w run/workloads/steady.yaml      # terminal 1
dimster run localB -w run/workloads/drain.yaml       # terminal 2
```

Each run produces its own result directory, named by its start time plus a random suffix so runs started in the same second don't collide, with the env name in its metadata.

Commands that act on Kafka follow `externalEnv` to the hosting env's brokers, so `dimster inspect localB` lists the topics of both envs. `dimster kafka`, `dimster logs <env> kafka` and `dimster download-kafka-logs` also reach the hosting env's brokers.

## Where the metrics are

- **Client metrics:** each env's own Grafana (`dimster dashboard localB`) shows its own producers and consumers only.
- **Broker metrics:** the Kafka dashboard is in the hosting env's Grafana (`dimster dashboard local`). It shows the brokers' combined load from every workload. The per-topic panels tell the envs apart by the tags in the topic names.
- **Saved metrics:** like an `external` env, an `external-env` env saves no Kafka dashboard with its results. Its resources report covers its own pods.

## Limitations

- **node-exporter:** each env's monitoring includes a node-exporter DaemonSet on host port 9100. On nodes where one already runs, the second env's node-exporter pod stays Pending, so its System dashboard is empty. The first env's System dashboard covers every node.
- **Tearing down the hosting env** stops the other envs from running. `dimster teardown local` notes which envs use its Kafka.
- **Hooks that act on brokers** affect every workload on the cluster: rolling broker restarts, broker config changes and page cache drops. A hook in an `external-env` env runs with its own namespace's permissions, so it can't find the brokers.
- **`dimster results <env> --charts`** on its own charts the newest local result directory, whichever env produced it. Without `--charts` alone it finds the env's own latest result.
