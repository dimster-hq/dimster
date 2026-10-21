package dev.dimster.engines.explore;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.ArrayList;
import java.util.List;

import dev.dimster.ResultMetadata;

/**
 * Result of an explore-limits run. Contains the configuration and
 * the outcome of each test in each scenario.
 */
@JsonPropertyOrder({"fileType", "mode", "metadata", "config", "scenarioResults"})
public class ExploreResult {
    public String fileType = "dimster-result";
    public String mode = "explore";
    public ResultMetadata metadata = new ResultMetadata();

    /** The explore workload config that was used */
    public ExploreWorkload config;

    /** Results grouped by scenario */
    public List<ScenarioResult> scenarioResults = new ArrayList<>();

    @JsonPropertyOrder({"name", "beginTime", "endTime", "tests"})
    public static class ScenarioResult {
        public String name;
        // Wall-clock timestamps bracketing this scenario's test points. The
        // dashboard exporter uses these to align HTML-window boundaries with
        // scenario boundaries so a scenario's test points stay co-located in
        // a single file. Null on the synthetic "(merged)" repeat-aggregate.
        public String beginTime;
        public String endTime;
        public List<TestOutcome> tests = new ArrayList<>();
    }

    @JsonPropertyOrder({"label", "topics", "partitionsPerTopic", "producersPerTopic",
            "consumerGroupsPerTopic", "consumersPerGroup",
            "messageSize", "messageDistributor", "consumerType",
            "peakSustainableRate", "peakSustainableThroughputMBs",
            "foundRateUpper",
            "converged", "reason",
            "publishLatency", "endToEndLatency", "endToEndDeliveryLatency"})
    public static class TestOutcome {
        /** Label for charting */
        public String label;

        // Full configuration for this test point
        public int topics;
        public int partitionsPerTopic;
        public int producersPerTopic;
        public int consumerGroupsPerTopic;
        public int consumersPerGroup;
        public int messageSize;
        public String messageDistributor;
        public String consumerType;
        // Dataset the test point generated records from: "payload"/RAW when
        // the workload sets none. For other datasets, messageSize is the
        // estimated average record size.
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String datasetPreset;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String datasetFormat;

        /** Peak sustainable rate in msgs/sec */
        public double peakSustainableRate;

        /** Peak sustainable throughput in MB/s */
        public double peakSustainableThroughputMBs;

        /** Upper bound of the converged search range in msgs/s */
        public double foundRateUpper;

        /** Whether the search converged to a stable result */
        public boolean converged;

        /** Human-readable explanation of the outcome */
        public String reason;

        /** Publish latency percentiles at peak rate (ms), aggregated across sustain phase */
        public LatencyBreakdown publishLatency = new LatencyBreakdown();

        /** End-to-end latency percentiles at peak rate (ms), aggregated across sustain phase */
        public LatencyBreakdown endToEndLatency = new LatencyBreakdown();

        /** E2E delivery latency (publish → poll return) percentiles at peak rate (ms) */
        public LatencyBreakdown endToEndDeliveryLatency = new LatencyBreakdown();
    }

    @JsonPropertyOrder({"p50", "p75", "p99", "p999", "p9999"})
    public static class LatencyBreakdown {
        public double p50;
        public double p75;
        public double p99;
        public double p999;
        public double p9999;
    }
}
