package dev.dimster;

import com.fasterxml.jackson.annotation.JsonInclude;

import dev.dimster.datagen.DatasetSpec;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.utils.distributor.MessageDistributorType;

/**
 * Shared workload parameters used by both standard (run) and explore modes.
 * Provides defaults that can be overridden per scenario.
 */
public class BaseWorkload {
    /** Optional embedded Kafka configuration (merged with -k file if both present) */
    public KafkaConfig kafkaConfig;
    /** Optional label used as a prefix for charting */
    public String label;

    public int topics = 1;
    public int partitionsPerTopic = 1;
    public int producersPerTopic = 1;
    public int consumerGroupsPerTopic = 1;
    public int consumersPerGroup = 1;

    public int messageSize = 1000;
    public double producerRate = 10000;
    /**
     * Optional variable-rate schedule. When present, producer threads follow
     * this schedule instead of the fixed producerRate while producerRate stays
     * available as the nominal/max scalar for older tooling and charts.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ProducerRateSchedule producerRateSchedule;

    // Payloads of the default payload dataset, used when no dataset is set: each
    // record is the header followed by messageSize - 1 payload bytes. Precedence:
    // useRandomizedPayloads wins. When true, randomizedPayloadPoolSize distinct
    // payloads are generated, each randomBytesRatio random and the rest zeros, and
    // payloadFile is ignored. When false, payloadFile must be set, is loaded from
    // disk and must be messageSize bytes; its first byte is replaced by the header.
    public boolean useRandomizedPayloads = true;
    public double randomBytesRatio = 0.5;
    public int randomizedPayloadPoolSize = 100;
    /** Only used when useRandomizedPayloads is false. Null otherwise so it's omitted from result JSON. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String payloadFile = null;

    /**
     * Optional generated dataset. When set, producers send schema-encoded rows
     * of this dataset instead of the payload dataset above, and messageSize
     * does not apply (record sizes follow from the data). Null by default.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public DatasetSpec dataset = null;

    public MessageDistributorType messageDistributor = MessageDistributorType.NO_KEY;
    /**
     * Zipf exponent for {@code KEY_ZIPF} / {@code PARTITION_ZIPF}. Ignored
     * by the other strategies. Higher values produce a steeper, more skewed
     * distribution (s = 0 → uniform; s ≈ 1 → classic Zipf; s ≥ 2 → very
     * heavy concentration on a handful of keys / partitions).
     */
    public double messageDistributorSkew = 1.0;
    public ConsumerType consumerType = ConsumerType.CONSUMER_GROUP;

    public int consumerProcMsMin = 0;
    public int consumerProcMsMax = 0;
    public int consumerProcParallelism = 1;
    public ConsumerProcMode consumerProcMode = ConsumerProcMode.BLOCKING;
    public int consumerProcCapacity = 10000;
}
