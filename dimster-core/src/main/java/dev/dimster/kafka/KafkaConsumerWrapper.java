package dev.dimster.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.dimster.ConsumerProcMode;
import dev.dimster.MessageType;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.datagen.RecordPrinter;
import dev.dimster.flink.FlinkEmitTime;
import dev.dimster.spi.kafka.RawConsumer;
import dev.dimster.utils.Sleeper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a classic Kafka consumer and translates its polling, commit, and
 * rebalance activity into Dimster's callback and metrics abstractions.
 */
public class KafkaConsumerWrapper implements AutoCloseable, OffsetCommitCallback,
        ConsumerRebalanceListener, KafkaMetricsSource {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerWrapper.class);
    private static final String MANUAL_COMMIT_LINGER_CONFIG = "dimster.commit.linger.ms";
    private static final String MANUAL_COMMIT_MODE = "dimster.commit.mode";
    private static final String POLL_TIMEOUT_CONFIG = "dimster.poll.timeout.ms";
    private static final String POLL_TIMEOUT_DEFAULT = "100";

    // SPI handle to the underlying kafka-clients consumer. The concrete impl is
    // chosen at startup via ServiceLoader (see KafkaClientFactory) and bound to
    // whichever Kafka version this dimster build was compiled against.
    private final RawConsumer<String, byte[]> consumer;
    private final ConsumerCallback callback;
    private final String name;
    private final ExecutorService executor;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private volatile boolean running = false;
    private volatile String failureReason = null;

    private enum ManualCommitMode {
        CommitSync,
        CommitAsync
    }

    private long manualCommitLingerMs;
    private final ManualCommitMode manualCommitMode;
    private boolean autoCommit;
    private final Duration pollTimeout;
    private long lastCommitTs = 0;

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

    // CONTINUOUS mode state
    private final ConsumerProcMode procMode;
    private final Map<TopicPartition, PartitionOffsetTracker> offsetTrackers;
    private final Map<TopicPartition, Long> lastCommittedOffsets;
    private volatile int queueCapacityThreshold; // soft cap on continuous mode queue capacity

    public KafkaConsumerWrapper(RawConsumer<String, byte[]> consumer,
                                Properties consumerConfig,
                                ConsumerCallback callback,
                                String topic,
                                String name,
                                int procMsMin,
                                int procMsMax,
                                int procParallelism,
                                List<TopicPartition> staticPartitions,
                                boolean printMessages,
                                RecordPrinter recordPrinter,
                                RecordFormat recordFormat,
                                ConsumerProcMode procMode,
                                int procCapacity) {
        this.consumer = consumer;
        this.callback = callback;
        this.name = name;
        this.printMessages = printMessages;
        this.recordPrinter = recordPrinter;
        this.recordFormat = recordFormat;
        this.procMode = procMode;

        this.manualCommitLingerMs = Long.parseLong(
                (String) consumerConfig.getOrDefault(MANUAL_COMMIT_LINGER_CONFIG, "0"));
        this.manualCommitMode = ManualCommitMode.valueOf(
                (String) consumerConfig.getOrDefault(MANUAL_COMMIT_MODE, "CommitAsync"));
        this.autoCommit = Boolean.parseBoolean(
                (String) consumerConfig.getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"));
        this.pollTimeout = Duration.ofMillis(Long.parseLong(
                (String) consumerConfig.getOrDefault(POLL_TIMEOUT_CONFIG, POLL_TIMEOUT_DEFAULT)));
        this.procMsMin = procMsMin;
        this.procMsMax = procMsMax;
        this.procParallelism = procParallelism;

        // Initialize simulator with mode
        boolean continuousMode = (procMode == ConsumerProcMode.CONTINUOUS);
        this.processingTimeSimulator = new ProcessingTimeSimulator(
            procMsMin, procMsMax, procParallelism, continuousMode);

        // Initialize CONTINUOUS mode state
        if (continuousMode) {
            this.autoCommit = false;
            this.manualCommitLingerMs = 0;
            this.offsetTrackers = new ConcurrentHashMap<>();
            this.lastCommittedOffsets = new HashMap<>();
            this.queueCapacityThreshold = procCapacity;
            log.info("Consumer {} initialized in CONTINUOUS mode (capacity={})",
                     name, queueCapacityThreshold);
        } else {
            this.offsetTrackers = null;
            this.lastCommittedOffsets = null;
            this.queueCapacityThreshold = 0;
            log.info("Consumer {} initialized in BLOCKING mode", name);
        }

        if (staticPartitions != null) {
            consumer.assign(staticPartitions);
            log.info("Consumer {} using static partition assignment: {}", name, staticPartitions);
        } else {
            // Kafka invokes this wrapper on the poll thread whenever group-managed
            // ownership changes. Static assign() consumers deliberately bypass it.
            consumer.subscribe(List.of(topic), this);
        }

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
        this.processingTimeSimulator.updateProcessingTime(minMs, maxMs);
    }

    /** Adjust the CONTINUOUS mode queue capacity threshold at runtime. Thread-safe. */
    public void setQueueCapacity(int capacity) {
        this.queueCapacityThreshold = capacity;
    }

    private void pollLoop() {
        log.info("Consumer poll loop started: {} (mode={})", name, procMode);

        if (procMode == ConsumerProcMode.CONTINUOUS) {
            pollLoopContinuous();
        } else {
            pollLoopBlocking();
        }

        log.info("Consumer poll loop ended: {}", name);
    }

    private void pollLoopBlocking() {
        try {
            while (!closing.get()) {
                try {
                    var records = consumer.poll(pollTimeout);
                    if (closing.get()) return;

                    running = true;
                    Instant deliveryTime = Instant.now();
                    callback.pollCompleted(records.count());
                    callback.batchReceived(name, records.count());

                    // Convert records to list for index-based access
                    var recordList = new java.util.ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>>();
                    for (var record : records) {
                        if (printMessages && log.isDebugEnabled()) {
                            log.debug("RECEIVED client={} topic={} partition={} offset={}",
                                    this.name, record.topic(), record.partition(), record.offset());
                        }
                        recordList.add(record);
                    }

                    // Find highest offset per partition in this batch (for commit)
                    Map<TopicPartition, Long> batchHighestOffsets = new HashMap<>();
                    if (!autoCommit) {
                        for (var record : recordList) {
                            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                            batchHighestOffsets.merge(tp, record.offset(), Math::max);
                        }
                    }

                    if (log.isDebugEnabled()) log.debug("Poll Complete. client={} records={} maxOffsets={}",
                            this.name, records.count(), batchHighestOffsets);

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

                                if (printMessages) {
                                    long e2eLatencyMs = simulatedProcessingStartTime.toEpochMilli() - record.timestamp();
                                    byte[] value = record.value();
                                    String msgType = MessageType.nameOf(recordFormat.typeOf(value));
                                    log.info("PROCESSED client={} topic={} partition={} offset={} timestamp={} e2eLatencyMs={} type={}{}",
                                            this.name, record.topic(), record.partition(),
                                            record.offset(), record.timestamp(), e2eLatencyMs, msgType,
                                            RecordPrinter.logSuffix(recordPrinter, record.key(), value));
                                }

                                callback.messageReceived(record.value(), recordFormat, record.topic(),
                                        record.partition(), record.timestamp(),
                                        deliveryTime, simulatedProcessingStartTime);
                                FlinkEmitTime.report(callback, record, recordFormat, simulatedProcessingStartTime);
                            }
                    );

                    // Build offset map from batch highest offsets --------------
                    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
                    if (!autoCommit) {
                        for (var entry : batchHighestOffsets.entrySet()) {
                            offsetMap.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
                        }
                    }

                    // Maybe do manual commit --------------
                    long now = System.currentTimeMillis();
                    if (!autoCommit && !offsetMap.isEmpty()
                            && now - lastCommitTs >= manualCommitLingerMs) {
                        if (log.isDebugEnabled()) log.debug("Commit. client={} offsets={}", this.name, batchHighestOffsets);
                        if (manualCommitMode == ManualCommitMode.CommitAsync) {
                            consumer.commitAsync(offsetMap, this);
                            callback.commitAsyncCompleted();
                        } else {
                            long startNanos = System.nanoTime();
                            consumer.commitSync(offsetMap, Duration.ofSeconds(30));
                            long latencyMicros = java.util.concurrent.TimeUnit.NANOSECONDS.toMicros(
                                    System.nanoTime() - startNanos);
                            callback.commitSyncCompleted(latencyMicros);
                        }
                        lastCommitTs = now;
                    }
                } catch (WakeupException e) {
                    // Ignore exception if closing
                    if (!closing.get()) throw e;
                }
            }
        } catch (Throwable t) {
            failureReason = t.getClass().getName() + ": " + t.getMessage();
            log.error("Fatal error in BLOCKING consumer poll loop", t);
        } finally {
            running = false;
        }
    }

    private void pollLoopContinuous() {
        try {
            while (!closing.get()) {
                try {
                    // Step 1: Drain completed records from simulator
                    List<ProcessingTimeSimulator.CompletedRecord> completed =
                        processingTimeSimulator.drainCompleted();

                    // Process and track completed records
                    for (var cr : completed) {
                        processAndMarkComplete(cr);
                    }

                    // Step 2: Check capacity before polling
                    if (processingTimeSimulator.getQueueSize() < queueCapacityThreshold) {
                        // Step 3: Poll new records
                        var records = consumer.poll(pollTimeout);
                        if (log.isDebugEnabled()) log.debug("Poll Complete. client={} records={}", this.name, records.count());
                        if (closing.get()) return;

                        running = true;
                        Instant deliveryTime = Instant.now();
                        callback.pollCompleted(records.count());
                        callback.batchReceived(name, records.count());

                        // Step 4: Offer records to simulator for async processing
                        for (var record : records) {
                            if (printMessages && log.isDebugEnabled()) {
                                log.debug("RECEIVED client={} topic={} partition={} offset={}",
                                        this.name, record.topic(), record.partition(), record.offset());
                            }

                            // Mark offset as received (for gap tracking)
                            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                            PartitionOffsetTracker tracker = offsetTrackers.computeIfAbsent(
                                tp, k -> new PartitionOffsetTracker());
                            tracker.markReceived(record.offset());

                            // Offer to simulator - returns CompletedRecord if processed immediately
                            ProcessingTimeSimulator.CompletedRecord immediateResult =
                                processingTimeSimulator.offerAsync(record, deliveryTime, recordFormat);

                            // Handle immediate completion (procMs==0 and queue empty)
                            if (immediateResult != null) {
                                processCompletedRecord(immediateResult);
                                offsetTrackers.get(tp).markComplete(record.offset());
                            }
                        }
                    } else {
                        // We cannot consumer.poll until we've processed some of the inflight
                        // records. Do a blocking take on the inflight delayed records.
                        if (log.isDebugEnabled()) {
                            log.debug("Poll Skipped. client={} reason={} inflight records reached exceeds soft cap of {}",
                                    this.name, processingTimeSimulator.getQueueSize(), queueCapacityThreshold);
                        }
                        ProcessingTimeSimulator.CompletedRecord cr =
                                processingTimeSimulator.takeCompleted(1, TimeUnit.SECONDS);
                        if (cr != null) {
                            processAndMarkComplete(cr);
                        }
                    }

                    // Step 5: Maybe commit offsets
                    maybeCommitOffsets();

                } catch (WakeupException e) {
                    // Ignore exception if closing
                    if (!closing.get()) throw e;
                }
            }
        } catch (Throwable t) {
            failureReason = t.getClass().getName() + ": " + t.getMessage();
            log.error("Fatal error in CONTINUOUS consumer poll loop", t);
        } finally {
            maybeCommitOffsets();
            running = false;
        }
    }

    private void processAndMarkComplete(ProcessingTimeSimulator.CompletedRecord cr) {
        // Process the record (print, record metrics)
        processCompletedRecord(cr);

        // Update offset tracker
        var record = cr.record();
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        PartitionOffsetTracker tracker = offsetTrackers.computeIfAbsent(
                tp, k -> new PartitionOffsetTracker());
        tracker.markComplete(record.offset());
    }

    /**
     * Process a completed record (print if configured, record metrics).
     * Called for records drained from the DelayQueue in CONTINUOUS mode.
     */
    private void processCompletedRecord(ProcessingTimeSimulator.CompletedRecord cr) {
        var record = cr.record();

        // Print if configured
        if (printMessages) {
            long e2eMs = cr.simulatedStartTime().toEpochMilli() - record.timestamp();
            byte[] value = record.value();
            String msgType = MessageType.nameOf(recordFormat.typeOf(value));
            log.info("PROCESSED client={} topic={} partition={} offset={} timestamp={} " +
                    "e2eLatencyMs={} type={}{}",
                    this.name, record.topic(), record.partition(), record.offset(),
                    record.timestamp(), e2eMs, msgType,
                    RecordPrinter.logSuffix(recordPrinter, record.key(), value));
        }

        // Record metrics
        callback.messageReceived(record.value(), recordFormat, record.topic(),
                record.partition(), record.timestamp(),
                cr.deliveryTime(), cr.simulatedStartTime());
        FlinkEmitTime.report(callback, record, recordFormat, cr.simulatedStartTime());
    }

    /**
     * Commit offsets if manual commit mode and linger period has elapsed (CONTINUOUS mode).
     */
    private void maybeCommitOffsets() {
        if (this.procMode == ConsumerProcMode.BLOCKING) return;

        // Build offset map from trackers (only include partitions with progress)
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        for (var entry : offsetTrackers.entrySet()) {
            TopicPartition tp = entry.getKey();
            long highest = entry.getValue().getHighestCommittable();
            if (highest >= 0) {
                long lastCommitted = lastCommittedOffsets.getOrDefault(tp, -1L);
                if (highest > lastCommitted) {
                    offsetMap.put(tp, new OffsetAndMetadata(highest + 1));
                }
            }
        }

        if (offsetMap.isEmpty()) return;

        // Commit
        if (log.isDebugEnabled()) log.debug("Commit. client={}", this.name);
        if (manualCommitMode == ManualCommitMode.CommitAsync) {
            consumer.commitAsync(offsetMap, this);
            callback.commitAsyncCompleted();
        } else {
            long startNanos = System.nanoTime();
            consumer.commitSync(offsetMap, Duration.ofSeconds(30));
            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(
                System.nanoTime() - startNanos);
            callback.commitSyncCompleted(latencyMicros);
        }

        // Update last committed offsets (commit semantics: offset is next-to-read)
        for (var entry : offsetMap.entrySet()) {
            lastCommittedOffsets.put(entry.getKey(), entry.getValue().offset() - 1);
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
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        callback.partitionsAssigned(partitions.size());
        log.info("Consumer {} assigned {} partition(s): {}", name, partitions.size(), partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        callback.partitionsRevoked(partitions.size());
        log.info("Consumer {} revoked {} partition(s): {}", name, partitions.size(), partitions);
    }

    @Override
    public Map<MetricName, ? extends Metric> kafkaMetrics() {
        return consumer.metrics();
    }

    @Override
    public String clientName() {
        return name;
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
        if (e != null) {
            log.warn("Error committing offsets", e);
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
                    log.warn("Consumer {} executor did not terminate in time, forcing shutdown", name);
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.warn("Consumer {} interrupted while awaiting executor shutdown", name);
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            // The 60-second close timeout lives inside RawConsumer's per-version
            // impls now; close() on the SPI is parameterless.
            consumer.close();
        } finally {
            closeFuture.complete(null);
        }
    }

    /**
     * Tracks received and completed offsets for a single partition in CONTINUOUS mode.
     * Handles Kafka's natural offset gaps (transaction markers, aborted transactions, compaction).
     *
     * <p>An offset is committable if ALL received offsets ≤ it are also completed.
     * This ensures we never commit an offset with gaps before it, preventing message loss on restart.
     */
}
