package dev.dimster.flink;

import com.fasterxml.jackson.annotation.JsonInclude;

import dev.dimster.kafka.KafkaConfig;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The optional {@code flink} block of a workload's {@code baseWorkload}: a
 * Flink SQL stage between the producers' topics and the consumers' topics.
 *
 * <p>When set, producers write to the test topics as usual, a Flink job
 * (one per test point) runs {@link #sql} over each topic and writes the result
 * to a matching {@code <topic>-out} topic, and consumers read the output
 * topics. {@link FlinkPipeline} runs the job; the runner jar in the
 * {@code dimster-flink} module turns the SELECT into the full Kafka-to-Kafka
 * SQL. Scenarios sweep fields through {@link FlinkDimensions}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FlinkSpec {

    public enum DeliveryGuarantee { AT_LEAST_ONCE, EXACTLY_ONCE, NONE }

    /**
     * Kafka table options Dimster sets itself, which the workload's
     * {@link #sourceOptions}/{@link #sinkOptions} may not override.
     */
    static final Set<String> RESERVED_OPTIONS = Set.of(
            "connector", "topic", "topic-pattern", "format",
            "properties.bootstrap.servers", "properties.group.id", "scan.startup.mode");
    private static final List<String> RESERVED_OPTION_PREFIXES = List.of("key.", "value.");
    /** Table options set from {@link #sourceParallelism} and {@link #sinkParallelism}. */
    static final String SCAN_PARALLELISM = "scan.parallelism";
    static final String SINK_PARALLELISM = "sink.parallelism";

    /** A SELECT over the {@code source} table; must keep {@code _dimster} and {@code _dimster_ts}. */
    public String sql;
    /** The job's default parallelism: the query's operators, and the source and sink unless set. */
    public int parallelism = 1;
    /** Parallelism of the Kafka source. Null means {@link #parallelism}; resolved per test point. */
    public Integer sourceParallelism;
    /** Parallelism of the Kafka sink. Null means {@link #parallelism}; resolved per test point. */
    public Integer sinkParallelism;
    /** 0 disables checkpointing. */
    public long checkpointIntervalMs = 10_000;
    public DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
    /** Partitions of each output topic. Null means the input's partitionsPerTopic. */
    public Integer outputPartitionsPerTopic;
    /** Extra Flink job configuration, e.g. {@code table.exec.*} or {@code pipeline.*}. */
    public Map<String, String> config = new LinkedHashMap<>();
    /** Extra options of the Kafka source table, e.g. {@code properties.fetch.min.bytes}. */
    public Map<String, String> sourceOptions = new LinkedHashMap<>();
    /** Extra options of the Kafka sink table, e.g. {@code properties.linger.ms}. */
    public Map<String, String> sinkOptions = new LinkedHashMap<>();

    public FlinkSpec copy() {
        FlinkSpec c = new FlinkSpec();
        c.sql = sql;
        c.parallelism = parallelism;
        c.sourceParallelism = sourceParallelism;
        c.sinkParallelism = sinkParallelism;
        c.checkpointIntervalMs = checkpointIntervalMs;
        c.deliveryGuarantee = deliveryGuarantee;
        c.outputPartitionsPerTopic = outputPartitionsPerTopic;
        c.config = config == null ? new LinkedHashMap<>() : new LinkedHashMap<>(config);
        c.sourceOptions = sourceOptions == null ? new LinkedHashMap<>() : new LinkedHashMap<>(sourceOptions);
        c.sinkOptions = sinkOptions == null ? new LinkedHashMap<>() : new LinkedHashMap<>(sinkOptions);
        return c;
    }

    /**
     * Task slots the job needs: with Flink's default slot sharing, one per
     * subtask of its most parallel operator.
     */
    public int slotsNeeded() {
        return Math.max(parallelism, Math.max(resolved(sourceParallelism), resolved(sinkParallelism)));
    }

    /** Sets an unset source or sink parallelism to {@link #parallelism}, once per test point. */
    public void resolveOperatorParallelism() {
        sourceParallelism = resolved(sourceParallelism);
        sinkParallelism = resolved(sinkParallelism);
    }

    int resolved(Integer operatorParallelism) {
        return operatorParallelism != null ? operatorParallelism : parallelism;
    }

    /**
     * With EXACTLY_ONCE, Flink writes the output in transactions committed on
     * checkpoints. Consumers then default to {@code read_committed}, as they do
     * for transactional Dimster producers, so that latency is measured to when
     * records become visible. An explicit {@code isolation.level} is kept.
     */
    public KafkaConfig applyConsumerDefaults(KafkaConfig config) {
        if (deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE) return config;
        KafkaConfig overlay = new KafkaConfig();
        if (config == null || config.classicConsumerConfig == null
                || !config.classicConsumerConfig.containsKey(ISOLATION_LEVEL)) {
            overlay.classicConsumerConfig = new LinkedHashMap<>(Map.of(ISOLATION_LEVEL, "read_committed"));
        }
        if (config == null || config.shareConsumerConfig == null
                || !config.shareConsumerConfig.containsKey(ISOLATION_LEVEL)) {
            overlay.shareConsumerConfig = new LinkedHashMap<>(Map.of(ISOLATION_LEVEL, "read_committed"));
        }
        return KafkaConfig.merge(config, overlay);
    }

    private static final String ISOLATION_LEVEL = "isolation.level";
    private static final String SOURCE_ISOLATION_LEVEL = "properties." + ISOLATION_LEVEL;

    /**
     * With transactional Dimster producers, the Flink source defaults to
     * {@code read_committed}, as Dimster's consumers do, so that aborted and
     * open transactions don't reach the output. Flink's Kafka source leaves
     * Kafka's {@code read_uncommitted} default. An explicit
     * {@code properties.isolation.level} in {@link #sourceOptions} is kept.
     */
    public void applySourceDefaults(KafkaConfig config) {
        if (config == null || !config.transactionsEnabled()) return;
        if (sourceOptions == null) sourceOptions = new LinkedHashMap<>();
        sourceOptions.putIfAbsent(SOURCE_ISOLATION_LEVEL, "read_committed");
    }

    /** Validates a fully resolved test point's spec. */
    public void validate(String scenarioName) {
        if (sql == null || sql.isBlank()) {
            fail(scenarioName, "flink.sql is required (a SELECT over the table 'source')");
        }
        if (parallelism < 1) {
            fail(scenarioName, "flink.parallelism must be at least 1 (got " + parallelism + ")");
        }
        if (sourceParallelism != null && sourceParallelism < 1) {
            fail(scenarioName, "flink.sourceParallelism must be at least 1 (got " + sourceParallelism + ")");
        }
        if (sinkParallelism != null && sinkParallelism < 1) {
            fail(scenarioName, "flink.sinkParallelism must be at least 1 (got " + sinkParallelism + ")");
        }
        if (checkpointIntervalMs < 0) {
            fail(scenarioName, "flink.checkpointIntervalMs cannot be negative");
        }
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE && checkpointIntervalMs == 0) {
            fail(scenarioName, "flink.deliveryGuarantee EXACTLY_ONCE requires checkpointing"
                    + " (flink.checkpointIntervalMs > 0): output is committed on checkpoints");
        }
        if (outputPartitionsPerTopic != null && outputPartitionsPerTopic < 1) {
            fail(scenarioName, "flink.outputPartitionsPerTopic must be at least 1");
        }
        checkOptions(scenarioName, "sourceOptions", sourceOptions);
        checkOptions(scenarioName, "sinkOptions", sinkOptions);
    }

    private static void checkOptions(String scenarioName, String field, Map<String, String> options) {
        if (options == null) return;
        for (String key : options.keySet()) {
            if (key.equals(SCAN_PARALLELISM) || key.equals(SINK_PARALLELISM)) {
                fail(scenarioName, "flink." + field + " cannot set '" + key
                        + "': use flink.sourceParallelism or flink.sinkParallelism");
            }
            if (RESERVED_OPTIONS.contains(key) || RESERVED_OPTION_PREFIXES.stream().anyMatch(key::startsWith)) {
                fail(scenarioName, "flink." + field + " cannot set '" + key
                        + "': Dimster sets the connector, topics, formats, key and bootstrap servers");
            }
        }
    }

    private static void fail(String scenarioName, String message) {
        throw new IllegalArgumentException("Scenario '%s': %s".formatted(scenarioName, message));
    }
}
