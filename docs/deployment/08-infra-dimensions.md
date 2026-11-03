# 08 — Infrastructure dimensions

Dimster supports dimensional testing of workloads via workload field sweeps. However, when the desired dimension is the infrastructure itself you must use a different approach. Dimster does not and will not take responsibility for infrastructure beyond deploying Kafka pods when using `deploy: full` mode. For additional infrastructure, **you must use your own automation**.

However, Dimster has two key features to allow for dimensional testing to include infrastructure.

## Infrastructure dimension support

### Additional metadata and extra dimensions

You can pass additional metadata `dimster run` as passthrough metadata recorded in the final JSON result file. This can include extra dimensions to be recorded in the results.

#### Metadata: series and instance

Extra metadata fields are passed when executing `dimster run` via the `-m` argument using key=value pairs. There are two metadata fields that control where Dimster places the results.

1. `series`: The name of the series, e.g, "broker-count"
2. `instance`: A unique id of a specific series run, e.g. "broker-count-1784188859"

The Dimster CLI will automatically place the results in the directory `results/<series>/<instance>/`.

For example, when `dimster run` is executed three times with the additional arguments `-m series=broker-count -m instance=brokers-1784188859`, the CLI places the three result directories as follows:

```
results/broker-count/brokers-1784188859
├── 2026-07-16-10-01-12
├── 2026-07-16-10-07-22
└── 2026-07-16-10-13-35
```

This helps you keep track of results of a single series. The `dimster compare` command accepts a directory and will create a comparison chart and single CSV from all child results.

```bash
./dimster compare --directory results/broker-count/brokers-1784188859
==> Comparing 3-brokers vs 5-brokers vs 7-brokers...
Generated: results/broker-count/brokers-1784188859/compare-3-brokers-vs-5-brokers-vs-7-brokers.html
==> Comparison charts saved to results/broker-count/brokers-1784188859/
==> Comparison CSV saved to results/broker-count/brokers-1784188859/compare-3-brokers-vs-5-brokers-vs-7-brokers.csv
```

#### Arbitrary metadata

Using `-m key=value`, you can pass arbitrary metadata fields, as passthrough metadata that ends up in the metadata section of the result JSON file. If the metadata field name clashes with an existing one, it will be rejected.

#### Extra Dimensions

Pass any extra dimensions that are outside of the workload, via the `-d key=value` argument, these are passed through to an `extraDimensions` section in the JSON result and a column per dimension in the CSV files. This allows for easy charting and comparison in post-benchmark analysis.

#### Environment overrides

If the infrastructure is part of the `dimster-config.yaml`, such as number of brokers, the amount of memory, JVM options, etc, you can use the `dimster deploy` argument `--env-override` which will override the value of a field in the chosen environment. For example: `dimster deploy local --env-override kafka.replicas=5`

Whether the brokers keep their data across these redeploys depends on the field. An override that only needs a restart, such as `kafka.brokerConfig` or `kafka.heap`, keeps it. One that shapes storage, such as `kafka.replicas`, `kafka.version` or `kafka.pvcSize`, deletes it, so each case starts from empty brokers. See [deploy](../reference/cli.md#deploy). Broker settings that only apply when a topic is created, such as internal topics' replication factors, don't change topics that already exist.

### Examples

#### Example 1 -  Broker count

```
instance="$(date +%s)"
./dimster build myEnv

echo "3 brokers"
./dimster deploy myEnv --env-override kafka.replicas=3
./dimster run myEnv -w workload.yaml -m series=broker-count -m "instance=${instance}" -d broker-count=3

echo "5 brokers"
./dimster deploy myEnv --env-override kafka.replicas=5
./dimster run myEnv -w workload.yaml -m series=broker-count -m "instance=${instance}" -d broker-count=5

echo "7 brokers"
./dimster deploy myEnv --env-override kafka.replicas=7
./dimster run myEnv -w workload.yaml -m series=broker-count -m "instance=${instance}" -d broker-count=7
```

The result directories will be found under `results/broker-count/${instance}/`

Finally run `dimster compare --directory "results/broker-count/${instance}"` to generate comparison charts and that concatenates all the results into one CSV file.

#### Example 2 -  Public internet vs VPC peering

You are testing VPC-peering vs public-internet connection when benchmarking a cloud Kafka service, you could specify an extra dimension such as `clusterAccess`.

Assume we have two Dimster envs, pointing at two different EKS clusters and two different external Kafka clusters.

```yaml
environments:
  publicInternetEnv:
    image: 123456789.dkr.ecr.us-west-2.amazonaws.com/dimster
    context: arn:aws:eks:us-west-2:123456789:cluster/my-public-internet-cluster
    deploy: external
    connection:
      bootstrapServers: "my-public-internet-cluster.example.com:9092"
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"...\" password=\"...\";"
    workers:
    # ... etc

  vpcPeeringEnv:
    image: 123456789.dkr.ecr.us-west-2.amazonaws.com/dimster
    context: arn:aws:eks:us-west-2:123456789:cluster/my-vpc-peered-cluster
    deploy: external
    connection:
      bootstrapServers: "my-vpc-peered-cluster.example.com:9092"
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"...\" password=\"...\";"
    workers:
    # ... etc
```

The workflow per campaign in the series is to upload the Dimster image to the image repository of the EKS cluster, deploy the Dimster workers and monitoring to the EKS cluster, run the benchmark, then teardown (optionally).

```bash
instance="$(date +%s)"

./dimster build publicInternetEnv
./dimster deploy publicInternetEnv
./dimster run publicInternetEnv \
    -w workload.yaml \
    -m series=cluster-access \
    -m instance=$instance \
    -d clusterAccess=public-internet
./dimster teardown publicInternetEnv

./dimster build vpcPeeringEnv
./dimster deploy vpcPeeringEnv
./dimster run vpcPeeringEnv \
    -w workload.yaml \
    -m series=cluster-access \
    -m instance=$instance \
    -d clusterAccess=vpc-peering
./dimster teardown vpcPeeringEnv
```

The result directories will be found under `results/cluster-access/${instance}/`

Finally run `dimster compare --directory "results/cluster-access/${instance}"` to generate comparison charts and that concatenates all the results into one CSV file.

#### Example scripts

See the examples in the `run/series/examples` directory. Copy them to the project root to run.
