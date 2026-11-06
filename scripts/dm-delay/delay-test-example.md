# An example walkthrough

Optional, the first time only: set up the platform and cluster. See [One-time setup](../../docs/deployment/09-emulated-network-storage.md#one-time-setup) for Linux, k3d and kind.


First have an env in dimster-config.yaml that uses local-storage.

```yaml
  localDmDelay:
    image: dimster
    context: minikube
    distro: minikube
    deploy: full
    metricsOutput: [html, snapshot]
    kafka:
      version: "4.2.0"
      replicas: 3
      tls:
        enabled: true
        interBroker: true
      pvcSize: "7Gi"
      pvcStorageClass: local-storage
      heap: "-Xms1G -Xmx1G"
      requests:
        cpu: "1"
        memory: "2Gi"
      brokerConfig: run/broker-configs/default.yaml
      logConfig: run/log4j-configs/info.yaml
    workers:
      replicas: 1
      heap: "-Xms1G -Xmx1G"
      requests:
        cpu: "1"
        memory: "1500Mi"
    coordinator:
      heap: "-Xms512M -Xmx512M"
      requests:
        cpu: "500m"
        memory: "1Gi"
```

Next create the delay volumes, ensure the pv-yaml args match the `pvcSize` in the env.

> On macOS with Colima, replace sudo scripts/dm-delay/dm-delay.sh with scripts/dm-delay/colima.sh (no sudo).

```bash
for i in 0 1 2; do
  sudo scripts/dm-delay/dm-delay.sh create ebs$i --size 8G --mount /mnt/dimster-ebs$i
  sudo scripts/dm-delay/dm-delay.sh node-mount ebs$i --container minikube
  sudo scripts/dm-delay/dm-delay.sh pv-yaml ebs$i --node minikube --size 7Gi | kubectl --context minikube apply -f -
done
```

Next deploy Dimster.

`./dimster deploy localDmDelay`

Next, we run the same benchmark twice, with and without a delay/throttling.

Without delay (recording an extra dimension `storage`).

```bash
for i in 0 1 2; do sudo scripts/dm-delay/dm-delay.sh set ebs$i --profile none; done
./dimster drain-backlog localDmDelay -w run/workloads/drain-backlog-test/drain-backlog-reuse-topic.yaml -k run/kafka-configs/kafka_safe.yaml -d storage=none
```

With a delay.

```bash
for i in 0 1 2; do sudo scripts/dm-delay/dm-delay.sh set ebs$i --profile gp3 --reads-only; done
sudo scripts/dm-delay/dm-delay.sh check ebs0      # expect ~1 ms, ~3000 IOPS, ~125 MiB/s
./dimster drain-backlog localDmDelay -w run/workloads/drain-backlog-test/drain-backlog-reuse-topic.yaml -k run/kafka-configs/kafka_safe.yaml -d storage=gp3
```

Then clean up. The following completely deletes everything.

```bash
./dimster teardown localDmDelay
for i in 0 1 2; do
  kubectl --context minikube delete pv dimster-ebs$i-pv-0
  sudo scripts/dm-delay/dm-delay.sh node-umount ebs$i --container minikube
  sudo scripts/dm-delay/dm-delay.sh destroy ebs$i --yes
done
```
