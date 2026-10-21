package dev.dimster.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import dev.dimster.datagen.DatasetResolver;
import dev.dimster.kafka.KafkaProducerWrapper;
import dev.dimster.spi.kafka.RawProducer;
import dev.dimster.utils.distributor.MessageDistributorType;
import dev.dimster.worker.commands.ClientHealth;
import dev.dimster.worker.commands.ProducerWorkAssignment;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.Test;

/**
 * Tests the producer lifecycle edges used by live /modify restarts. The fake
 * RawProducer keeps these tests broker-free while still exercising
 * LocalWorker's real runProducer task, stop signalling, and health reporting.
 */
public class LocalWorkerProducerRestartTest {

    @Test
    public void removeProducersWakesSleepingTaskBeforeClosingProducer() throws Exception {
        LocalWorker worker = new LocalWorker(false);
        FakeRawProducer raw = new FakeRawProducer();
        String name = "producer-topic-i0";
        KafkaProducerWrapper wrapper = new KafkaProducerWrapper(
                raw, "topic", name, 0, new int[0], false);
        setField(worker, "producers", new ArrayList<>(List.of(wrapper)));

        worker.setDataset(DatasetResolver.payloadDataset(9, 0.5, 1, null));
        worker.startLoad(new ProducerWorkAssignment(
                0.1, MessageDistributorType.NO_KEY, 1, 1.0));
        assertTrue("producer should send once before entering the long rate-limit sleep",
                raw.firstSend.await(2, TimeUnit.SECONDS));

        worker.removeProducers(List.of(name));

        assertTrue("removeProducers should close the wrapper", raw.closed.get());
        assertEquals("the stopped task must not wake later and send on the closed producer",
                1, raw.sendCount.get());
        assertFalse("sendAsync should never be called after close", raw.sendAfterClose.get());
        assertTrue("removed producer should no longer be reported in health",
                worker.getHealth().isEmpty());
        worker.close();
    }

    @Test
    public void staleProducerFailureDoesNotPoisonReplacementWithSameName() throws Exception {
        LocalWorker worker = new LocalWorker(false);
        String name = "producer-topic-i0";
        KafkaProducerWrapper replacement = new KafkaProducerWrapper(
                new FakeRawProducer(), "topic", name, 0, new int[0], false);
        setField(worker, "producers", new ArrayList<>(List.of(replacement)));

        long oldGeneration = registerProducerInstance(worker, name);
        long replacementGeneration = registerProducerInstance(worker, name);

        recordProducerFailureIfCurrent(worker, name, oldGeneration,
                new IllegalStateException("old task failed after replacement"));
        assertEquals(1, worker.getHealth().producers().running());
        assertTrue(worker.getHealth().failedClients().isEmpty());

        recordProducerFailureIfCurrent(worker, name, replacementGeneration,
                new IllegalStateException("current task failed"));
        ClientHealth failedHealth = worker.getHealth();
        assertEquals(1, failedHealth.producers().failed());
        assertEquals("FAILED", failedHealth.failedClients().get(0).status());
        assertTrue(failedHealth.failedClients().get(0).error().contains("current task failed"));
        worker.close();
    }

    private static long registerProducerInstance(LocalWorker worker, String name) throws Exception {
        Method m = LocalWorker.class.getDeclaredMethod("registerProducerInstance", String.class);
        m.setAccessible(true);
        return (Long) m.invoke(worker, name);
    }

    private static void recordProducerFailureIfCurrent(LocalWorker worker, String name,
                                                       long generation, Throwable failure)
            throws Exception {
        Method m = LocalWorker.class.getDeclaredMethod(
                "recordProducerFailureIfCurrent",
                String.class, long.class, String.class, Throwable.class);
        m.setAccessible(true);
        m.invoke(worker, name, generation, "topic", failure);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static class FakeRawProducer implements RawProducer<String, byte[]> {
        final CountDownLatch firstSend = new CountDownLatch(1);
        final AtomicInteger sendCount = new AtomicInteger(0);
        final AtomicBoolean closed = new AtomicBoolean(false);
        final AtomicBoolean sendAfterClose = new AtomicBoolean(false);

        @Override
        public Future<RecordMetadata> send(ProducerRecord<String, byte[]> record, Callback callback) {
            if (closed.get()) {
                sendAfterClose.set(true);
                throw new IllegalStateException("Cannot perform operation after producer has been closed");
            }
            sendCount.incrementAndGet();
            firstSend.countDown();
            return new CompletableFuture<>();
        }

        @Override public void initTransactions() {}
        @Override public void beginTransaction() {}
        @Override public void commitTransaction() {}
        @Override public void abortTransaction() {}
        @Override public Map<MetricName, ? extends Metric> metrics() { return Map.of(); }
        @Override public void close() { closed.set(true); }
    }
}
