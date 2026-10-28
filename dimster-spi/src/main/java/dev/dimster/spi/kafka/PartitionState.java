package dev.dimster.spi.kafka;

import java.util.List;

/**
 * Replica placement and health of one partition, returned by
 * {@link RawAdmin#describePartitions}. dimster-core uses it to wait for the
 * cluster to settle (every partition led by its preferred replica with a full
 * ISR) before a measurement, e.g. after a hook restarted brokers.
 *
 * @param leader   broker id of the current leader, or -1 when there is none
 * @param replicas assigned replica broker ids; the first is the preferred leader
 * @param isr      broker ids of the in-sync replicas
 */
public record PartitionState(int leader, List<Integer> replicas, List<Integer> isr) {

    public PartitionState {
        replicas = List.copyOf(replicas);
        isr = List.copyOf(isr);
    }
}
