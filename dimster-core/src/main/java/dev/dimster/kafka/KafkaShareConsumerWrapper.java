package dev.dimster.kafka;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a Kafka share-group consumer (KIP-932) and translates polling,
 * acknowledgement, and simulated processing into Dimster's callback and metrics
 * abstractions. Uses zero-copy {@link ByteBuffer} values and a single-pass fast
 * path when {@code procMsMax == 0}.
 */
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
    private final RawShareConsumer<String, ByteBuffer> consumer;
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
    /** When true, inspect record headers for Flink emit-time stamps; skipped on non-Flink runs. */
    private final boolean checkFlinkEmitTime;

    /**
     * @param name canonical client name from {@code VirtualConsumer.name()}
     *     (e.g. {@code "consumer-{topic}-{group}-i{idx}"}). Same identity
     *     the planner uses, so {@code LocalWorker.removeConsumers} can
     *     look up wrappers by name.
     */
    public KafkaShareConsumerWrapper(RawShareConsumer<String, ByteBuffer> consumer,
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
        this(consumer, consumerConfig, callback, topic, name, procMsMin, procMsMax,
                procParallelism, printMessages, recordPrinter, recordFormat, true);
    }

    public KafkaShareConsumerWrapper(RawShareConsumer<String, ByteBuffer> consumer,
                                     Properties consumerConfig,
                                     ConsumerCallback callback,
                                     String topic,
                                     String name,
                                     int procMsMin,
                                     int procMsMax,
                                     int procParallelism,
                                     boolean printMessages,
                                     RecordPrinter recordPrinter,
                                     RecordFormat recordFormat,
                                     boolean checkFlinkEmitTime) {
        this.consumer = consumer;
        this.callback = callback;
        this.name = name;
        this.printMessages = printMessages;
        this.recordPrinter = recordPrinter;
        this.recordFormat = recordFormat;
        this.checkFlinkEmitTime = checkFlinkEmitTime;

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
        this.processingTimeSimulator = new ProcessingTimeSimulator(minMs, maxMs, procParallelism);
        this.procMsMin = minMs;
        this.procMsMax = maxMs;
    }

    private void pollLoop() {
        log.info("Share consumer poll loop started: {}", name);
        try {
            while (!closing.get()) {
                try {
                    ConsumerRecords<String, ByteBuffer> records = consumer.poll(pollTimeout);
                    if (closing.get()) return;
                    running = true;
                    Instant deliveryTime = Instant.now();
                    int recordCount = records.count();
                    if (log.isDebugEnabled()) {
                        log.debug("Poll Complete. client={} records={}", this.name, recordCount);
                    }
                    callback.pollCompleted(recordCount);
                    callback.batchReceived(name, recordCount);
                    if (recordCount == 0) {
                        continue;
                    }

                    if (procMsMax == 0) {
                        processFastPath(records, deliveryTime);
                    } else {
                        processWithSimulatedDelay(records, recordCount, deliveryTime);
                    }

                    // Maybe do manual commit --------------
                    if (manualCommitMode != ManualCommitMode.Poll) {
                        if (log.isDebugEnabled()) log.debug("Commit. client={}", this.name);
                        if (manualCommitMode == ManualCommitMode.CommitAsync) {
                            consumer.commitAsync();
                            callback.commitAsyncCompleted();
                        } else if (manualCommitMode == ManualCommitMode.CommitSync) {
                            long startNanos = System.nanoTime();
                            consumer.commitSync();
                            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(
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

    private void processFastPath(ConsumerRecords<String, ByteBuffer> records, Instant deliveryTime) {
        boolean debugReceive = printMessages && log.isDebugEnabled();
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, ByteBuffer>> partRecords = records.records(tp);
            int size = partRecords.size();
            if (size == 0) {
                continue;
            }
            if (!printMessages) {
                callback.partitionBatchReceived(tp.topic(), tp.partition(), partRecords,
                        recordFormat, deliveryTime, checkFlinkEmitTime);
                if (explicitAck) {
                    for (int i = 0; i < size; i++) {
                        consumer.acknowledge(partRecords.get(i), AckType.ACCEPT);
                    }
                }
            } else {
                for (int i = 0; i < size; i++) {
                    ConsumerRecord<String, ByteBuffer> record = partRecords.get(i);
                    if (debugReceive) {
                        log.debug("RECEIVED client={} topic={} partition={} offset={}", this.name,
                                record.topic(), record.partition(), record.offset());
                    }
                    logProcessedRecord(record, deliveryTime);
                    callback.messageReceived(record.value(), recordFormat, tp.topic(), tp.partition(),
                            record.timestamp(), deliveryTime, deliveryTime);
                    if (checkFlinkEmitTime) {
                        FlinkEmitTime.reportBuffer(callback, record, recordFormat, deliveryTime);
                    }
                    if (explicitAck) {
                        consumer.acknowledge(record, AckType.ACCEPT);
                    }
                }
            }
        }
    }

    private void processWithSimulatedDelay(ConsumerRecords<String, ByteBuffer> records,
                                           int recordCount, Instant deliveryTime) {
        boolean debugReceive = printMessages && log.isDebugEnabled();
        var recordList = new ArrayList<ConsumerRecord<String, ByteBuffer>>(recordCount);
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, ByteBuffer>> partRecords = records.records(tp);
            if (debugReceive) {
                for (int i = 0, n = partRecords.size(); i < n; i++) {
                    var r = partRecords.get(i);
                    log.debug("RECEIVED client={} topic={} partition={} offset={}", this.name,
                            r.topic(), r.partition(), r.offset());
                }
            }
            recordList.addAll(partRecords);
        }

        processingTimeSimulator.processBatch(
                recordList,
                closing,
                callback,
                recordFormat,
                deliveryTime,
                (messageIdx, simulatedProcessingStartTime) -> {
                    var record = recordList.get(messageIdx);
                    if (printMessages) {
                        logProcessedRecord(record, simulatedProcessingStartTime);
                    }
                    callback.messageReceived(record.value(), recordFormat, record.topic(), record.partition(),
                            record.timestamp(), deliveryTime, simulatedProcessingStartTime);
                    if (checkFlinkEmitTime) {
                        FlinkEmitTime.reportBuffer(callback, record, recordFormat, simulatedProcessingStartTime);
                    }
                    // acknowledge(ACCEPT) is local-only (no network I/O).
                    if (explicitAck) {
                        consumer.acknowledge(record, AckType.ACCEPT);
                    }
                }
        );
    }

    private void logProcessedRecord(ConsumerRecord<String, ByteBuffer> record, Instant processingStartTime) {
        long e2eLatencyMs = processingStartTime.toEpochMilli() - record.timestamp();
        ByteBuffer value = record.value();
        String msgType = MessageType.nameOf(recordFormat.typeOfBuffer(value));
        String suffix = recordPrinter != null
                ? RecordPrinter.logSuffix(recordPrinter, record.key(), RecordFormat.toByteArray(value))
                : "";
        log.info("PROCESSED client={} topic={} partition={} offset={} timestamp={} e2eLatencyMs={} type={}{}",
                this.name, record.topic(), record.partition(),
                record.offset(), record.timestamp(), e2eLatencyMs, msgType, suffix);
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
