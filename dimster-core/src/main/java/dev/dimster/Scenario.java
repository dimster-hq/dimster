package dev.dimster;

import com.fasterxml.jackson.annotation.JsonInclude;

import dev.dimster.datagen.DatasetSpec;
import dev.dimster.engines.drainbacklog.DrainBacklogDimensions;
import dev.dimster.engines.explore.ExploreDimensions;
import dev.dimster.engines.run.RunDimensions;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.utils.distributor.MessageDistributorType;

import java.util.List;

/**
 * A scenario within a workload. Each field can be a scalar or an array.
 * Jackson's ACCEPT_SINGLE_VALUE_AS_ARRAY coerces scalars into single-element lists.
 * All arrays in a scenario must be the same length (co-varying dimensions).
 * Null fields inherit from the baseWorkload.
 *
 * kafkaConfig is a single object (not a list). Individual properties within
 * its config maps can be arrays to express dimensions.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Scenario {
    public String name;

    /** Optional per-test-point labels for charting */
    public List<String> label;

    public List<Integer> topics;
    public List<Integer> partitionsPerTopic;
    public List<Integer> producersPerTopic;
    public List<Integer> consumerGroupsPerTopic;
    public List<Integer> consumersPerGroup;
    public List<Integer> messageSize;
    public List<Double> producerRate;
    /** Optional per-test-point variable producer-rate schedules. */
    public List<ProducerRateSchedule> producerRateSchedule;
    public List<MessageDistributorType> messageDistributor;
    /** Per-test-point Zipf skew override for KEY_ZIPF / PARTITION_ZIPF. */
    public List<Double> messageDistributorSkew;
    public List<ConsumerType> consumerType;
    public KafkaConfig kafkaConfig;
    /**
     * Optional per-test-point generated datasets (whole objects, like
     * {@link #producerRateSchedule}). Overrides {@code baseWorkload.dataset}.
     */
    public List<DatasetSpec> dataset;

    public List<Integer> consumerProcMsMin;
    public List<Integer> consumerProcMsMax;
    public List<Integer> consumerProcParallelism;
    public List<ConsumerProcMode> consumerProcMode;
    public List<Integer> consumerProcCapacity;

    /** Drain-backlog-only: per-scenario overrides for drain config fields (supports dimensions) */
    public DrainBacklogDimensions drainBacklogConfig;

    /** Explore-only: per-scenario overrides for explore config fields (supports dimensions) */
    public ExploreDimensions exploreConfig;

    /** Run-only: per-scenario overrides for runConfig fields (supports dimensions) */
    public RunDimensions runConfig;

    /** Number of times to repeat this scenario. Default 1 (no repeat). */
    public int repeat = 1;
}
