/*
 * Originally derived from io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment
 * in OpenMessaging Benchmark (https://github.com/openmessaging/benchmark, Apache 2.0)
 * and substantially modified for Dimster. The current implementation is largely
 * Dimster-original; see NOTICE at the repo root for context.
 */
package dev.dimster.worker.commands;

import dev.dimster.ProducerRateSchedule;
import dev.dimster.utils.distributor.MessageDistributorType;

/**
 * Carries everything {@code LocalWorker.startLoad} needs to spin up the
 * producer threads for one test point. {@code partitionsPerTopic} and
 * {@code messageDistributorSkew} are only consumed by the Zipf-based key
 * distributors but are passed unconditionally to keep the wiring uniform.
 */
public record ProducerWorkAssignment(
        double publishRate,
        ProducerRateSchedule publishRateSchedule,
        Long scheduleStartEpochMillis,
        MessageDistributorType messageDistributorType,
        int partitionsPerTopic,
        double messageDistributorSkew) {

    public ProducerWorkAssignment(double publishRate,
                                  MessageDistributorType messageDistributorType,
                                  int partitionsPerTopic,
                                  double messageDistributorSkew) {
        this(publishRate, null, null, messageDistributorType,
                partitionsPerTopic, messageDistributorSkew);
    }

    public ProducerWorkAssignment(double publishRate,
                                  ProducerRateSchedule publishRateSchedule,
                                  MessageDistributorType messageDistributorType,
                                  int partitionsPerTopic,
                                  double messageDistributorSkew) {
        this(publishRate, publishRateSchedule, null, messageDistributorType,
                partitionsPerTopic, messageDistributorSkew);
    }

    public ProducerWorkAssignment withPublishRate(double newRate) {
        return new ProducerWorkAssignment(
                newRate, publishRateSchedule, scheduleStartEpochMillis, messageDistributorType,
                partitionsPerTopic, messageDistributorSkew);
    }

    public ProducerWorkAssignment dividedBy(double divisor) {
        ProducerRateSchedule dividedSchedule = publishRateSchedule == null
                ? null
                : publishRateSchedule.copyDividedBy(divisor);
        return new ProducerWorkAssignment(
                publishRate / divisor, dividedSchedule, scheduleStartEpochMillis,
                messageDistributorType, partitionsPerTopic, messageDistributorSkew);
    }
}
