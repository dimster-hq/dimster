package dev.dimster.coordinator;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import dev.dimster.kafka.KafkaCluster;

@JsonIgnoreProperties(ignoreUnknown = true)
public record RunRequest(
    String fileType,
    String mode,
    String kafkaConfigYaml,
    Map<String, String> workloads,
    List<String> workers,
    String topology,
    String workerRatio,
    Integer producerWorkers,
    Integer consumerWorkers,
    String outputDir,
    String system,
    String environment,
    String hardware,
    Map<String, Object> metadata,
    Map<String, Object> extraDimensions,
    // CLI-chosen results subdirectory name (yyyy-MM-dd-HH-mm-ss plus a random
    // 4-character suffix, so same-second runs don't collide). When set,
    // CoordinatorServer writes results into {outputDir}/{resultsSubdir}
    // instead of generating its own timestamp. Lets the CLI stage workload/
    // kafka/broker config captures into the same directory the coordinator
    // will fill with result JSONs. Optional for backwards compatibility:
    // older clients that don't send this field still get the original
    // server-generated timestamp behavior.
    String resultsSubdir,
    // Hook scripts the workloads reference (engineConfig.hooks), keyed by the
    // name the workload uses, with the script's contents. The CLI reads them
    // from the user's machine; the coordinator installs and runs only these.
    Map<String, String> hookScripts,
    // The env's topic tag (the env name unless the env sets topicTag). The
    // coordinator names and cleans up only dimster-<tag>-* topics and groups,
    // so envs can share a Kafka cluster; see KafkaCluster. Null keeps the
    // untagged names.
    String topicTag
) {
    public RunRequest {
        if (fileType == null || fileType.isBlank()) {
            fileType = "dimster-run-request";
        }
        // Reject a bad tag at the request, before anything runs.
        KafkaCluster.validateTag(topicTag);
    }
}
