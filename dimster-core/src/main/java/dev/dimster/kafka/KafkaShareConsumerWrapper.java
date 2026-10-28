package dev.dimster.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.dimster.MessageType;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.datagen.RecordPrinter;
import dev.dimster.flink.FlinkEmitTime;
import dev.dimster.spi.kafka.AckType;
import dev.dimster.spi.kafka.RawShareConsumer;
import dev.dimster.spi.kafka.ShareAckListener;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaShareConsumerWrapper implements AutoCloseable, ShareAckListener, KafkaMetricsSource {

    private static final Logger log = LoggerFactory.getLogger(KafkaShareConsumerWrapper.class);
    private static final String MANUAL_COMMIT_LINGER_CONFIG = "dimster.commit.linger.ms";
    private static final String MANUAL_COMMIT_MODE = "dimster.commit.mode";
    private static final String POLL_TIMEOUT_CONFIG = "dimster.poll.timeout.ms";
    private static final String POLL_TIMEOUT_DEFAULT = "100";

    // SPI handle to the underlying kafka-clients share consumer. The concrete
    // impl is chosen at startup via ServiceLoader (KafkaClientFactory) and is
    // only available on dimster builds against Kafka 4.2+; pre-4.2 builds fail
    // fast at workload-build time via the factory's capabilities() check.
    private final RawShareConsumer<String, byte[]> consumer;
    private final ConsumerCallback callback;
    private final String name;
    private final boolean explicitAck;
    private final ExecutorService executor;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private volatile boolean running = false;
    private volatile String failureReason = null;

    private enum ManualCommitMode {
        CommitSync,
        CommitAsync,
        Poll;
    }

    private final ManualCommitMode manualCommitMode;
    private final Duration pollTimeout;

    private volatile int procMsMin;
    private volatile int procMsMax;
    private volatile int procParallelism;
    private volatile ProcessingTimeSimulator processingTimeSimulator;

    /** When true, every consumed record is logged with its partition + timestamp.
     *  Wired in from {@code engineConfig.printMessages}; the upstream
     *  coordinator already enforces the rate guard, so this wrapper just obeys. */
    private final boolean printMessages;
    /** Renders record contents for those lines (engineConfig.printContents),
     *  or null to print none. */
    private final RecordPrinter recordPrinter;
    /** Layout of the consumed values (RAW for the payload dataset, else the
     *  dataset's format), used to find each record's message type. */
    private final RecordFormat recordFormat;

    /**
     * @param name canonical client name from {@code VirtualConsumer.name()}
     *     (e.g. {@code "consumer-{topic}-{group}-i{idx}"}). Same identity
     *     the planner uses, so {@code LocalWorker.removeConsumers} can
     *     look up wrappers by name.
     */
    public KafkaShareConsumerWrapper(RawShareConsumer<String, byte[]> consumer,
                                     Properties consumerConfig,
                                     ConsumerCallback callback,
                                     String topic,
                                     String name,
                                     int procMsMin,
                                     int procMsMax,
                                     int procParallelism,
                                     boolean printMessages,
                                     RecordPrinter recordPrinter,
                                     RecordFormat recordFormat) {
        this.consumer = consumer;
        this.callback = callback;
        this.name = name;
        this.printMessages = printMessages;
        this.recordPrinter = recordPrinter;
        this.recordFormat = recordFormat;

        this.explicitAck = "explicit".equalsIgnoreCase(
                (String) consumerConfig.getOrDefault("share.acknowledgement.mode", "implicit"));
        this.manualCommitMode = ManualCommitMode.valueOf(
                (String) consumerConfig.getOrDefault(MANUAL_COMMIT_MODE, "Poll"));
        this.pollTimeout = Duration.ofMillis(Long.parseLong(
                (String) consumerConfig.getOrDefault(POLL_TIMEOUT_CONFIG, POLL_TIMEOUT_DEFAULT)));
        this.procMsMin = procMsMin;
        this.procMsMax = procMsMax;
        this.procParallelism = procParallelism;
        this.processingTimeSimulator = new ProcessingTimeSimulator(procMsMin, procMsMax, procParallelism);

        if (explicitAck) {
            consumer.setAckCallback(this);
        }
        consumer.subscribe(Set.of(topic));

        // this.pollThread = Thread.ofVirtual()
        //         .name(name)
        //         .start(this::pollLoop);
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, name);
//            t.setDaemon(true);
            return t;
        });
        this.executor.submit(this::pollLoop);
    }

    /** Adjust the simulated per-record processing time at runtime. Thread-safe. */
    public void setProcessingTime(int minMs, int maxMs) {
        this.procMsMin = minMs;
        this.procMsMax = maxMs;
        this.processingTimeSimulator = new ProcessingTimeSimulator(minMs, maxMs, procParallelism);
    }

    private void pollLoop() {
        log.info("Share consumer poll loop started: {}", name);
        try {
            while (!closing.get()) {
                try {
                    var records = consumer.poll(pollTimeout);
                    if (log.isDebugEnabled()) log.debug("Poll Complete. client={} records={}", this.name, records.count());
                    if (closing.get()) return;
                    running = true;
                    Instant deliveryTime = Instant.now();
                    callback.pollCompleted(records.count());
                    callback.batchReceived(name, records.count());
                    boolean needsCommit = !records.isEmpty();

                    // Convert records to list for index-based access
                    var recordList = new java.util.ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>>();
                    for (var record : records) {
                        if (printMessages && log.isDebugEnabled()) {
                            log.debug("RECEIVED client={} topic={} partition={} offset={}", this.name,
                                    record.topic(), record.partition(), record.offset());
                        }
                        recordList.add(record);
                    }

                    // Process the records with simulated processing time --------------
                    processingTimeSimulator.processBatch(
                            recordList,
                            closing,
                            callback,
                            recordFormat,
                            deliveryTime,
                            (messageIdx, simulatedProcessingStartTime) -> {
                                // Process single message with its simulated processing start time
                                var record = recordList.get(messageIdx);

                                // Print the messages if configured
                                if (printMessages) {
                                    long e2eLatencyMs = simulatedProcessingStartTime.toEpochMilli() - record.timestamp();
                                    byte[] value = record.value();
                                    String msgType = MessageType.nameOf(recordFormat.typeOf(value));
                                    log.info("PROCESSED client={} topic={} partition={} offset={} timestamp={} e2eLatencyMs={} type={}{}",
                                            this.name,
                                            record.topic(), record.partition(),
                                            record.offset(), record.timestamp(), e2eLatencyMs, msgType,
                                            RecordPrinter.logSuffix(recordPrinter, record.key(), value));
                                }

                                // Records stats
                                callback.messageReceived(record.value(), recordFormat, record.topic(), record.partition(),
                                        record.timestamp(), deliveryTime, simulatedProcessingStartTime);
                                FlinkEmitTime.report(callback, record, recordFormat, simulatedProcessingStartTime);

                                // Maybe acknowledge
                                // Note: acknowledge(ACCEPT) is local-only (no network I/O),
                                // so its position relative to the aggregate sleep is immaterial.
                                if (explicitAck) {
                                    consumer.acknowledge(record, AckType.ACCEPT);
                                }
                            }
                    );

                    // Maybe do manual commit --------------
                    if (needsCommit && manualCommitMode != ManualCommitMode.Poll) {
                        if (log.isDebugEnabled()) log.debug("Commit. client={}", this.name);
                        if (manualCommitMode == ManualCommitMode.CommitAsync) {
                            consumer.commitAsync();
                            callback.commitAsyncCompleted();
                        } else if (manualCommitMode == ManualCommitMode.CommitSync) {
                            long startNanos = System.nanoTime();
                            consumer.commitSync();
                            long latencyMicros = java.util.concurrent.TimeUnit.NANOSECONDS.toMicros(
                                    System.nanoTime() - startNanos);
                            callback.commitSyncCompleted(latencyMicros);
                        }
                    }
                } catch (WakeupException e) {
                    // Ignore exception if closing
                    if (!closing.get()) throw e;
                }
            }
        } catch (Throwable t) {
            failureReason = t.getClass().getName() + ": " + t.getMessage();
            log.error("Fatal error in share consumer poll loop", t);
        } finally {
            // Perform one last commit before close -------------
            try {
                consumer.commitSync();
            } catch (Throwable t) {
                log.error("Failed while performing final commitSync", t);
            }
            running = false;
            log.info("Share consumer poll loop ended: {}", name);
        }
    }

    public String getName() {
        return name;
    }

    public boolean isRunning() {
        return running;
    }

    public String getFailureReason() {
        return failureReason;
    }

    @Override
    public Map<MetricName, ? extends Metric> kafkaMetrics() {
        return consumer.metrics();
    }

    @Override
    public String clientName() {
        return name;
    }

    /**
     * SPI ack-commit callback. The 4.2 impl flattens Kafka's
     * Map&lt;TopicIdPartition, Set&lt;Long&gt;&gt; to Map&lt;TopicPartition, Set&lt;Long&gt;&gt;
     * before invoking us; the topic id was never read here anyway.
     */
    @Override
    public void onComplete(Map<TopicPartition, Set<Long>> committedOffsets, Throwable e) {
        if (e != null) {
            log.warn("Error in share group acknowledgement commit", e);
            callback.error();
        }
    }

    @Override
    public void close() {
        if (!closing.compareAndSet(false, true)) {
            log.info("Consumer close of {} already in progress, waiting", name);
            closeFuture.join();
            return;
        }
        try {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    log.warn("Consumer {} executor did not exit it's poll loop, forcing shutdown", name);
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.warn("Consumer {} interrupted while awaiting executor shutdown", name);
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            // The 60-second close timeout lives inside RawShareConsumer's
            // per-version impls now; close() on the SPI is parameterless.
            consumer.close();
        } finally {
            closeFuture.complete(null);
        }
    }
}
