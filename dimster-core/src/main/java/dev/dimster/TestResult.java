package dev.dimster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import dev.dimster.availability.AvailabilityResult;
import dev.dimster.datagen.ResolvedDataset;
import dev.dimster.worker.commands.PeriodStats;

@JsonPropertyOrder({"mode", "metadata", "kafkaConfig", "workloadConfig", "results"})
public class TestResult {
    public String mode = "run";
    public Metadata metadata = new Metadata();
    public KafkaConfigSummary kafkaConfig = new KafkaConfigSummary();
    public WorkloadConfig workloadConfig = new WorkloadConfig();
    public PerformanceResults results = new PerformanceResults();

    /** Availability data, populated by WorkloadEngine when availability probes are enabled. */
    @JsonIgnore
    public AvailabilityResult.AvailabilityData availability;

    @JsonPropertyOrder({"workload", "workloadFile", "kafkaConfigName", "environment", "system",
            "label", "scenarioName", "dimensionIndex", "totalDimensions", "beginTime", "endTime"})
    public static class Metadata {
        public String workload;
        public String workloadFile;
        public String kafkaConfigName;
        public String environment;
        public String system;
        public String label;
        public String scenarioName;
        public int dimensionIndex = -1;
        public int totalDimensions;
        public String beginTime;
        public String endTime;
    }

    @JsonPropertyOrder({"name", "replicationFactor", "topicConfig", "producerConfig", "classicConsumerConfig"})
    public static class KafkaConfigSummary {
        public String name;
        public short replicationFactor;
        public Map<String, Object> topicConfig;
        public Map<String, Object> producerConfig;
        public Map<String, Object> classicConsumerConfig;
        public Map<String, Object> shareConsumerConfig;

        public static KafkaConfigSummary fromKafkaConfig(dev.dimster.kafka.KafkaConfig kc) {
            if (kc == null) return null;
            var summary = new KafkaConfigSummary();
            summary.name = kc.name;
            summary.replicationFactor = kc.replicationFactor;
            summary.topicConfig = kc.topicConfig;
            summary.producerConfig = kc.producerConfig;
            summary.classicConsumerConfig = kc.classicConsumerConfig;
            summary.shareConsumerConfig = kc.shareConsumerConfig;
            return summary;
        }
    }

    @JsonPropertyOrder({"messageDistributor", "messageDistributorSkew", "consumerType", "messageSize",
            "topics", "partitionsPerTopic",
            "producersPerTopic", "producerRate", "consumerGroupsPerTopic",
            "consumersPerGroup", "consumerProcMsMin", "consumerProcMsMax",
            "testDurationMinutes", "warmupDurationMinutes",
            "rampIntervals", "rampIntervalSeconds",
            "sampleRateMillis"})
    public static class WorkloadConfig {
        public String messageDistributor;
        // Zipf exponent — only meaningful for KEY_ZIPF / PARTITION_ZIPF, but
        // recorded for every test point so the JSON shape stays uniform.
        public double messageDistributorSkew;
        public String consumerType;
        public long messageSize;
        public int topics;
        public int partitionsPerTopic;
        public int producersPerTopic;
        public double producerRate;
        public ProducerRateSchedule producerRateSchedule;
        public int consumerGroupsPerTopic;
        public int consumersPerGroup;
        public int consumerProcMsMin;
        public int consumerProcMsMax;
        public int testDurationMinutes;
        public int warmupDurationMinutes;
        // Ramp-up settings used for this test point. Both 0 means no ramp
        // was performed (legacy behaviour).
        public int rampIntervals;
        public int rampIntervalSeconds;
        public int sampleRateMillis;
        // Dataset the test point generated records from: "payload"/RAW when
        // the workload sets none. For other datasets, messageSize is the
        // estimated average record size. Absent in results from before
        // datasets existed.
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String datasetPreset;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String datasetFormat;

        /** Records which dataset the test point generated records from. */
        public void setDataset(ResolvedDataset dataset) {
            if (dataset != null) {
                datasetPreset = dataset.preset() != null ? dataset.preset() : "custom";
                datasetFormat = dataset.format().name();
            }
        }
    }

    public static class PerformanceResults {
        public List<Long> sent = new ArrayList<>();
        public List<Long> consumed = new ArrayList<>();
        public List<Long> publishFailed = new ArrayList<>();
        public List<Long> consumeFailed = new ArrayList<>();

        public List<Double> publishRate = new ArrayList<>();
        public List<Double> consumeRate = new ArrayList<>();

        // Measured throughput per period in MB/s (10^6 bytes), from the actual
        // encoded record sizes. Unlike rate x messageSize this is exact when
        // record sizes vary, as they do with generated datasets.
        public List<Double> publishBytesRate = new ArrayList<>();
        public List<Double> consumeBytesRate = new ArrayList<>();
        /** Average encoded size of the records sent, over all recorded periods. */
        public double avgRecordBytes;

        // Running totals behind avgRecordBytes; not part of the result JSON.
        @JsonIgnore
        private long recordedBytesSent;
        @JsonIgnore
        private long recordedMessagesSent;

        /**
         * Records one period's measured byte rates, given the period's message
         * rates. Bytes per second follow as rate x (bytes / messages) for the
         * period, so callers need not pass the elapsed time again.
         */
        public void addByteRates(PeriodStats stats, double publishRate, double consumeRate) {
            publishBytesRate.add(stats.messagesSent > 0
                    ? publishRate * stats.bytesSent / stats.messagesSent / 1_000_000 : 0.0);
            consumeBytesRate.add(stats.messagesReceived > 0
                    ? consumeRate * stats.bytesReceived / stats.messagesReceived / 1_000_000 : 0.0);
            recordedBytesSent += stats.bytesSent;
            recordedMessagesSent += stats.messagesSent;
            avgRecordBytes = recordedMessagesSent > 0 ? (double) recordedBytesSent / recordedMessagesSent : 0;
        }

        public List<Double> scheduleLatencyMin = new ArrayList<>();
        public List<Double> scheduleLatency50pct = new ArrayList<>();
        public List<Double> scheduleLatency75pct = new ArrayList<>();
        public List<Double> scheduleLatency99pct = new ArrayList<>();
        public List<Double> scheduleLatencyMax = new ArrayList<>();
        public Map<Double, Double> aggregatedScheduleLatencyQuantiles = new TreeMap<>();

        public List<Double> publishLatencyMin = new ArrayList<>();
        public List<Double> publishLatencyAvg = new ArrayList<>();
        public List<Double> publishLatency50pct = new ArrayList<>();
        public List<Double> publishLatency75pct = new ArrayList<>();
        public List<Double> publishLatency95pct = new ArrayList<>();
        public List<Double> publishLatency99pct = new ArrayList<>();
        public List<Double> publishLatency999pct = new ArrayList<>();
        public List<Double> publishLatency9999pct = new ArrayList<>();
        public List<Double> publishLatencyMax = new ArrayList<>();

        public List<Double> publishDelayLatencyAvg = new ArrayList<>();
        public List<Double> publishDelayLatency50pct = new ArrayList<>();
        public List<Double> publishDelayLatency75pct = new ArrayList<>();
        public List<Double> publishDelayLatency95pct = new ArrayList<>();
        public List<Double> publishDelayLatency99pct = new ArrayList<>();
        public List<Double> publishDelayLatency999pct = new ArrayList<>();
        public List<Double> publishDelayLatency9999pct = new ArrayList<>();
        public List<Double> publishDelayLatencyMax = new ArrayList<>();

        public double aggregatedPublishLatencyAvg;
        public double aggregatedPublishLatency50pct;
        public double aggregatedPublishLatency75pct;
        public double aggregatedPublishLatency95pct;
        public double aggregatedPublishLatency99pct;
        public double aggregatedPublishLatency999pct;
        public double aggregatedPublishLatency9999pct;
        public double aggregatedPublishLatencyMax;

        public double aggregatedPublishDelayLatencyAvg;
        public double aggregatedPublishDelayLatency50pct;
        public double aggregatedPublishDelayLatency75pct;
        public double aggregatedPublishDelayLatency95pct;
        public double aggregatedPublishDelayLatency99pct;
        public double aggregatedPublishDelayLatency999pct;
        public double aggregatedPublishDelayLatency9999pct;
        public double aggregatedPublishDelayLatencyMax;

        public Map<Double, Double> aggregatedPublishLatencyQuantiles = new TreeMap<>();
        public Map<Double, Double> aggregatedPublishDelayLatencyQuantiles = new TreeMap<>();

        public List<Double> endToEndLatencyMin = new ArrayList<>();
        public List<Double> endToEndLatencyAvg = new ArrayList<>();
        public List<Double> endToEndLatency50pct = new ArrayList<>();
        public List<Double> endToEndLatency75pct = new ArrayList<>();
        public List<Double> endToEndLatency95pct = new ArrayList<>();
        public List<Double> endToEndLatency99pct = new ArrayList<>();
        public List<Double> endToEndLatency999pct = new ArrayList<>();
        public List<Double> endToEndLatency9999pct = new ArrayList<>();
        public List<Double> endToEndLatencyMax = new ArrayList<>();

        public Map<Double, Double> aggregatedEndToEndLatencyQuantiles = new TreeMap<>();

        public double aggregatedEndToEndLatencyAvg;
        public double aggregatedEndToEndLatency50pct;
        public double aggregatedEndToEndLatency75pct;
        public double aggregatedEndToEndLatency95pct;
        public double aggregatedEndToEndLatency99pct;
        public double aggregatedEndToEndLatency999pct;
        public double aggregatedEndToEndLatency9999pct;
        public double aggregatedEndToEndLatencyMax;

        // Delivery latency: publish timestamp → poll return time (before per-record processing)
        public List<Double> endToEndDeliveryLatencyMin = new ArrayList<>();
        public List<Double> endToEndDeliveryLatencyAvg = new ArrayList<>();
        public List<Double> endToEndDeliveryLatency50pct = new ArrayList<>();
        public List<Double> endToEndDeliveryLatency75pct = new ArrayList<>();
        public List<Double> endToEndDeliveryLatency95pct = new ArrayList<>();
        public List<Double> endToEndDeliveryLatency99pct = new ArrayList<>();
        public List<Double> endToEndDeliveryLatency999pct = new ArrayList<>();
        public List<Double> endToEndDeliveryLatency9999pct = new ArrayList<>();
        public List<Double> endToEndDeliveryLatencyMax = new ArrayList<>();

        public Map<Double, Double> aggregatedEndToEndDeliveryLatencyQuantiles = new TreeMap<>();

        public double aggregatedEndToEndDeliveryLatencyAvg;
        public double aggregatedEndToEndDeliveryLatency50pct;
        public double aggregatedEndToEndDeliveryLatency75pct;
        public double aggregatedEndToEndDeliveryLatency95pct;
        public double aggregatedEndToEndDeliveryLatency99pct;
        public double aggregatedEndToEndDeliveryLatency999pct;
        public double aggregatedEndToEndDeliveryLatency9999pct;
        public double aggregatedEndToEndDeliveryLatencyMax;

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public List<PartitionResult> perPartitionResults;
    }

    public static class PartitionResult {
        public String key;
        public long messagesSent;
        public long messagesReceived;
        public double publishLatencyAvg;
        public double publishLatency50pct;
        public double publishLatency95pct;
        public double publishLatency99pct;
        public double publishLatency999pct;
        public double publishLatencyMax;
        public double endToEndLatencyAvg;
        public double endToEndLatency50pct;
        public double endToEndLatency95pct;
        public double endToEndLatency99pct;
        public double endToEndLatency999pct;
        public double endToEndLatencyMax;
    }
}
