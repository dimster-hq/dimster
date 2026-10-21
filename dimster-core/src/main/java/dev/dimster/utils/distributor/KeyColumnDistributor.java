package dev.dimster.utils.distributor;

/**
 * Marker distributor for {@link MessageDistributorType#KEY_COLUMN}. Keys come
 * from the generated row's key column, which only the producer loop knows, so
 * {@code LocalWorker} recognises this type and sets the key itself; this
 * distributor returns no key and no partition.
 */
public final class KeyColumnDistributor implements MessageDistributor {

    @Override
    public Target next() {
        return Target.NULL_KEY;
    }
}
