/*
 * Originally derived from io.openmessaging.benchmark.worker.commands.CumulativeLatencies
 * in OpenMessaging Benchmark (https://github.com/openmessaging/benchmark, Apache 2.0)
 * and substantially modified for Dimster. The current implementation is largely
 * Dimster-original; see NOTICE at the repo root for context.
 */
package dev.dimster.worker.commands;

import org.HdrHistogram.Histogram;

import com.fasterxml.jackson.annotation.JsonIgnore;

import static dev.dimster.stats.StatsCollector.E2E_LATENCY_MAX_MICROS;
import static dev.dimster.stats.StatsCollector.LATENCY_SIG_DIGITS;
import static dev.dimster.stats.StatsCollector.NON_E2E_LATENCY_MAX_MICROS;

public class CumulativeLatencies {

    @JsonIgnore
    public Histogram publishLatency = new Histogram(NON_E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    public byte[] publishLatencyBytes;

    @JsonIgnore
    public Histogram publishDelayLatency = new Histogram(NON_E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    public byte[] publishDelayLatencyBytes;

    @JsonIgnore
    public Histogram endToEndLatency = new Histogram(E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    public byte[] endToEndLatencyBytes;

    @JsonIgnore
    public Histogram endToEndDeliveryLatency = new Histogram(E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    public byte[] endToEndDeliveryLatencyBytes;

    @JsonIgnore
    public Histogram producerToFlinkLatency = new Histogram(E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);

    @JsonIgnore
    public Histogram flinkToConsumerLatency = new Histogram(E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);

    @JsonIgnore
    public Histogram scheduleLatency = new Histogram(NON_E2E_LATENCY_MAX_MICROS, LATENCY_SIG_DIGITS);
    public byte[] scheduleLatencyBytes;
}
