package dev.dimster.worker;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.dimster.ConsumerType;
import dev.dimster.MessageType;
import dev.dimster.ProducerRateSchedule;
import dev.dimster.availability.AvailabilityConfig;
import dev.dimster.availability.AvailabilityPayload;
import dev.dimster.availability.AvailabilityRecorder;
import dev.dimster.availability.AvailabilityResult;
import dev.dimster.availability.ProbeAssignment;
import dev.dimster.datagen.DatasetGenerator;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.datagen.RecordPrinter;
import dev.dimster.datagen.ResolvedDataset;
import dev.dimster.datagen.RowGenerator;
import dev.dimster.engines.correctness.CorrectnessConfig;
import dev.dimster.engines.correctness.CorrectnessPayload;
import dev.dimster.engines.correctness.CorrectnessResult;
import dev.dimster.engines.correctness.CorrectnessTracker;
import dev.dimster.engines.correctness.ModelChecker;
import dev.dimster.kafka.ConsumerCallback;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.kafka.KafkaConsumerWrapper;
import dev.dimster.kafka.KafkaSpi;
import dev.dimster.spi.kafka.RawConsumer;
import dev.dimster.spi.kafka.RawProducer;
import dev.dimster.kafka.KafkaMetricsSource;
import dev.dimster.kafka.KafkaProducerWrapper;
import dev.dimster.kafka.KafkaShareConsumerWrapper;
import dev.dimster.metrics.PrometheusEndpoint;
import dev.dimster.metrics.ShareGroupLagMonitor;
import dev.dimster.stats.StatsCollector;

import dev.dimster.utils.HostUtil;
import dev.dimster.utils.ScheduledRateLimiter;
import dev.dimster.utils.UniformRateLimiter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.dimster.utils.Timer;
import dev.dimster.utils.distributor.KeyColumnDistributor;
import dev.dimster.utils.distributor.MessageDistributor;
import dev.dimster.worker.commands.ClientHealth;
import dev.dimster.worker.commands.ClientStatus;
import dev.dimster.worker.commands.CountersStats;
import dev.dimster.worker.commands.PerPartitionStats;
import dev.dimster.worker.commands.PeriodStats;
import dev.dimster.worker.commands.ProducerWorkAssignment;
import dev.dimster.worker.topology.ConsumerSet;
import dev.dimster.worker.topology.VirtualConsumer;
import dev.dimster.worker.topology.VirtualProducer;

public class LocalWorker implements Worker {

    private KafkaConfig kafkaConfig = null;

    private List<KafkaProducerWrapper> producers = new ArrayList<>();
    private List<AutoCloseable> consumers = new ArrayList<>();

    private volatile UniformRateLimiter rateLimiter = new UniformRateLimiter(1.0);
    private volatile ScheduledRateLimiter scheduledRateLimiter = null;
    private final ConcurrentHashMap<String, String> producerFailures = new ConcurrentHashMap<>();

    // private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private final StatsCollector stats = new StatsCollector();
    private final PrometheusEndpoint metricsEndpoint;

    // Correctness mode state. Note: the toggle for whether classic consumers use static
    // partition assignments is now a topology-level decision (TopologyPlanner reads
    // CorrectnessConfig.staticConsumerAssignment and either populates VirtualConsumer.staticPartitions
    // or leaves it empty), so we no longer carry that flag here.
    private volatile boolean correctnessEnabled = false;
    private volatile boolean enableCrc = false;
    private CorrectnessConfig correctnessConfig = null;
    private CorrectnessTracker correctnessTracker = null;
    private final ConcurrentHashMap<Integer, java.util.concurrent.atomic.AtomicLong> producerSequences =
            new ConcurrentHashMap<>();

    // Availability mode state
    private volatile boolean availabilityEnabled = false;
    private AvailabilityRecorder availabilityRecorder = null;
    private AvailabilityConfig availabilityConfig = null;
    private final List<RawProducer<String, byte[]>> probeProducersList = new ArrayList<>();
    private RawConsumer<String, byte[]> probeConsumer = null;

    private volatile boolean producersPaused = false;
    private volatile boolean testCompleted = false;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private volatile boolean stopComplete = true;
    private final String workerPrefix = Integer.toHexString(
            ThreadLocalRandom.current().nextInt(0x1000, 0xFFFF)).substring(0, 3);

    // Share group lag monitoring
    private ShareGroupLagMonitor lagMonitor = null;

    // ─── Incremental modify support ─────────────────────────────────────
    // The /modify pipeline (TestPointMutator + DiffApplier) calls into the
    // add/remove/restart methods below mid-test. To avoid disturbing every
    // running producer when only one needs to stop, runProducer() consults
    // this set on each loop iteration — a producer whose name appears here
    // exits cleanly. The cleanup path then closes the underlying KafkaProducer.
    private final Set<String> stoppedProducerNames = ConcurrentHashMap.newKeySet();

    // Captured at startLoad() so addProducers() invoked later can spin up a
    // matching runProducer task with the same distributor / rate.
    // Null until startLoad fires.
    private volatile MessageDistributor activeMessageDistributor;
    // Producer wrapper ownership lives in the runProducer task, not in the
    // modify thread. Restart/remove waits on these tasks before closing the
    // wrappers so a rate-limited producer cannot wake up and send after close().
    private final ConcurrentHashMap<String, Future<?>> producerTasks = new ConcurrentHashMap<>();
    // The thread map lets removeProducers wake a task that is parked in the
    // rate limiter. Generations prevent a late failure from an old task from
    // being reported against the replacement producer with the same name.
    private final ConcurrentHashMap<String, Thread> producerThreads = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> activeProducerGenerations = new ConcurrentHashMap<>();
    private final AtomicLong producerGenerationCounter = new AtomicLong(0);
    // Per-startLoad producer-id counter. Each runProducer task gets a unique
    // id used for correctness-mode sequence tracking. addProducers() takes
    // the next free id rather than re-using the count of existing producers
    // (which would collide if a removeProducers happened in between).
    private final java.util.concurrent.atomic.AtomicInteger producerIdCounter =
            new java.util.concurrent.atomic.AtomicInteger(0);

    // Generates every record of the current test point: the workload's
    // dataset, or the default payload dataset. Set by setDataset before
    // producers and consumers are created; each producer task captures it
    // when it starts, like the correctness flags, so a dataset change takes
    // effect when producers are restarted.
    private volatile DatasetGenerator datasetGenerator = null;

    // Header of every non-correctness data record (Dimster's _dimster field).
    private static final byte[] NORMAL_HEADER = {MessageType.NORMAL};
    private static final byte[] PROBE_HEADER = {MessageType.PROBE};

    // Per-message printing (engineConfig.printMessages). Volatile because
    // setPrintMessages is invoked once at engine startup (potentially on a
    // different thread than producer/consumer threads) and must be visible
    // to the producer success callback and the consumer wrappers' constructors.
    private volatile boolean printMessages = false;
    // engineConfig.printContents: add each record's key and contents to the
    // printed lines. Kept so setDataset can rebuild the printer, which needs
    // the dataset to decode Avro records.
    private volatile boolean printContents = false;
    // Renders record contents for the printed lines; null unless
    // printContents is on. Producers read it per send, and consumer wrappers
    // take it at construction, like printMessages.
    private volatile RecordPrinter recordPrinter = null;
    // Layout of the record values consumers receive: RAW for the payload
    // pool, else the dataset's format. Set by setDataset before consumers
    // are created, and given to each consumer wrapper.
    private volatile RecordFormat recordFormat = RecordFormat.RAW;

    // Consumer set kept for the recreation loop: when a consumer fails we re-instantiate
    // it from its VirtualConsumer entry (preserving its name, group, and static partitions).
    // The uniform consumer-type and processing-time settings live here so the recreation
    // path doesn't have to thread them through.
    private ConsumerSet consumerSet = null;
    private ConsumerCallback consumerCallback = null;
    private int maxConsumerRecreations = 0;
    private final Map<Integer, Integer> recreationCounts = new ConcurrentHashMap<>();
    private final AtomicLong totalRecreations = new AtomicLong(0);
    private Thread recreationThread = null;

    public LocalWorker() {
        this(true);
    }

    LocalWorker(boolean startMetricsEndpoint) {
        this.metricsEndpoint = new PrometheusEndpoint(stats);
        if (startMetricsEndpoint) {
            this.metricsEndpoint.run();
        }
    }

    @Override
    public void setKafkaConfig(File configFile) throws IOException {
        setKafkaConfig(mapper.readValue(configFile, KafkaConfig.class));
    }

    @Override
    public void setKafkaConfig(KafkaConfig config) throws IOException {
        testCompleted = false;
        kafkaConfig = config;
        log.info("Kafka config: {}", writer.writeValueAsString(kafkaConfig));
    }

    @Override
    public void createProducers(List<VirtualProducer> virtualProducers) {
        Timer timer = new Timer();

        var producerProps = kafkaConfig.producerProperties();
        printProperties("Producer", producerProps);

        boolean transactional = kafkaConfig.transactionsEnabled();
        // Hostname is shared with PrometheusEndpoint's host="..." label so that
        // dashboards can correlate per-host transaction metrics back to the
        // producers that emitted them. Used to derive a stable transactional.id
        // for each VirtualProducer.
        String hostname = transactional ? HostUtil.hostname() : null;

        // The topology is the single source of truth: we just iterate it. Per-producer
        // partition pinning (single-partition correctness pinning, multi-partition
        // PINNED_PARTITIONS, or none) is already encoded in vp.pinnedPartitions().
        for (VirtualProducer vp : virtualProducers) {
            Properties perProducerProps = transactional
                    ? cloneWithTransactionalId(producerProps, vp.transactionalId(hostname))
                    : producerProps;
            // Concrete kafka-clients producer is built by the SPI factory bound at startup
            // (KafkaSpi.factory() does the ServiceLoader lookup once and caches it).
            var kafkaProducer = KafkaSpi.factory().<String, byte[]>newProducer(perProducerProps);
            var wrapper = new KafkaProducerWrapper(kafkaProducer, vp.topic(), vp.name(),
                    vp.producerIdx(), vp.pinnedPartitions(), transactional);
            initTransactionsTimed(wrapper, perProducerProps);
            registerProducerInstance(vp.name());
            producers.add(wrapper);
            log.info("Producer {} topic={} pinnedPartitions={}{}",
                    vp.name(), vp.topic(),
                    Arrays.toString(vp.pinnedPartitions()),
                    transactional
                            ? " (transactional.id=" + perProducerProps.getProperty("transactional.id") + ")"
                            : "");
        }

        log.info("Created {} producers in {} ms", producers.size(), timer.elapsedMillis());
        updateKafkaMetricsSources();
    }

    /**
     * Returns a copy of {@code base} with {@code transactional.id} set.
     * Each transactional producer needs its own unique id; reusing one would
     * cause Kafka to fence prior instances. Cloning rather than mutating keeps
     * the base producerProps reusable across iterations of the loop above.
     */
    private static Properties cloneWithTransactionalId(Properties base, String transactionalId) {
        Properties copy = new Properties();
        copy.putAll(base);
        copy.setProperty("transactional.id", transactionalId);
        return copy;
    }

    /**
     * Calls {@link KafkaProducerWrapper#initTransactionsIfEnabled()} and logs
     * how long coordinator discovery took. This is a blocking call that can
     * stall for several seconds when the cluster is under load, so visibility
     * into its timing helps debugging slow startup.
     */
    private static void initTransactionsTimed(KafkaProducerWrapper wrapper, Properties props) {
        if (!wrapper.isTransactional()) return;
        long start = System.nanoTime();
        wrapper.initTransactionsIfEnabled();
        long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        log.info("initTransactions completed for transactional.id={} in {} ms",
                props.getProperty("transactional.id"), ms);
    }

    @Override
    public void createConsumers(ConsumerSet consumerSet) {
        Timer timer = new Timer();
        ConsumerCallback callback = correctnessEnabled ? correctnessTracker : stats;
        this.consumerSet = consumerSet;
        this.consumerCallback = callback;
        this.recreationCounts.clear();

        if (consumerSet.consumerType() == ConsumerType.SHARE_GROUP) {
            printProperties("Consumer", kafkaConfig.shareConsumerProperties());
        } else {
            printProperties("Consumer", kafkaConfig.classicConsumerProperties());
        }

        // Topology already carries the static partition assignments per consumer
        // (computed by TopologyPlanner). Just instantiate.
        for (int i = 0; i < consumerSet.consumers().size(); i++) {
            consumers.add(createSingleConsumer(i));
        }

        // Delivery-latency histogram is only informative when consumers simulate
        // processing time — skip the recording cost otherwise.
        stats.setRecordDeliveryLatency(this.consumerSet.consumerProcMsMax() > 0);

        // Start share group lag monitoring so Prometheus can scrape lag metrics.
        // The lag monitor needs the (topic, group) pairs the worker is consuming.
        if (consumerSet.consumerType() == ConsumerType.SHARE_GROUP) {
            var tcgs = consumerSet.consumers().stream()
                    .map(vc -> new dev.dimster.worker.commands.TopicConsumerGroup(vc.topic(), vc.consumerGroup()))
                    .toList();
            // Lag monitor owns its own admin handle (separate from KafkaCluster's) since
            // it polls on a 10s schedule and shouldn't contend with topic-management calls.
            lagMonitor = new ShareGroupLagMonitor(
                    KafkaSpi.factory().newAdmin(kafkaConfig.commonProperties()), tcgs);
            metricsEndpoint.setLagMonitor(lagMonitor);
            lagMonitor.start();
        }

        log.info("Created {} consumers in {} ms", consumers.size(), timer.elapsedMillis());
        updateKafkaMetricsSources();
    }

    /**
     * Rebuilds the list of Kafka client metrics sources from the current
     * producers and consumers lists and pushes an immutable snapshot to
     * the PrometheusEndpoint.
     */
    private void updateKafkaMetricsSources() {
        var sources = new ArrayList<KafkaMetricsSource>();
        for (var p : producers) sources.add(p);
        for (var c : consumers) {
            if (c instanceof KafkaMetricsSource s) sources.add(s);
        }
        metricsEndpoint.setKafkaMetricsSources(sources);
    }

    private void printProperties(String who, Properties props) {
        var output = props.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ", "{", "}"));

        log.info("Kafka {} Properties: {}", who, output);
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) {
        if (producerWorkAssignment.publishRateSchedule() != null) {
            scheduledRateLimiter = new ScheduledRateLimiter(
                    producerWorkAssignment.publishRateSchedule(),
                    producerWorkAssignment.scheduleStartEpochMillis());
            rateLimiter = null;
            producersPaused = false;
            log.info("Start variable publish-rate schedule: {} (max {} msg/s)",
                    producerWorkAssignment.publishRateSchedule().type,
                    producerWorkAssignment.publishRateSchedule().maxConfiguredRate());
        } else {
            scheduledRateLimiter = null;
            if (producerWorkAssignment.publishRate() == 0.0) {
                // A scheduled workload can deliberately start at zero for
                // ramp/warm-up. Keep a harmless fixed limiter in place while
                // the producer loop is paused, then the measured schedule will
                // replace it before traffic is allowed again.
                rateLimiter = new UniformRateLimiter(1.0);
                producersPaused = true;
            } else {
                rateLimiter = new UniformRateLimiter(producerWorkAssignment.publishRate());
                producersPaused = false;
            }
        }
        MessageDistributor messageDistributor = MessageDistributor.of(
                producerWorkAssignment.messageDistributorType(),
                producerWorkAssignment.partitionsPerTopic(),
                producerWorkAssignment.messageDistributorSkew());

        // Stash for any addProducers() call that arrives later via /modify
        // — it needs the same distributor to spin up a matching runProducer
        // task on the new producer.
        this.activeMessageDistributor = messageDistributor;
        this.producerIdCounter.set(0);
        this.stoppedProducerNames.clear();
        this.producerTasks.clear();
        this.producerThreads.clear();

        for (KafkaProducerWrapper producer : producers) {
            int producerId = producerIdCounter.getAndIncrement();
            submitProducerTask(producer, messageDistributor, producerId);
        }
    }

    @Override
    public void probeProducers() {
        // Probes are ordinary generated records with a PROBE header, so they
        // are valid records of the topic's schema.
        DatasetGenerator dataset = requireDataset();
        // Transactional producers cannot send outside a transaction once
        // initTransactions has put them in READY state — every probe send
        // must be wrapped in begin/commit. Probes are issued before startLoad
        // (see BaseEngine.run), so there is no race with the per-producer
        // runProducer threads that drive the actual workload.
        producers.forEach(producer -> {
            byte[] probePayload = dataset.newRowGenerator(producer.clientName(), producer.producerIdx())
                    .next(PROBE_HEADER);
            if (producer.isTransactional()) {
                producer.beginTransaction();
                producer.sendAsync("key", probePayload);
                producer.commitTransaction();
            } else {
                producer.sendAsync("key", probePayload);
            }
        });
    }

    /** The current dataset; setDataset must have been called for this test point. */
    private DatasetGenerator requireDataset() {
        DatasetGenerator dataset = datasetGenerator;
        if (dataset == null) {
            throw new IllegalStateException("setDataset must be called before producers send");
        }
        return dataset;
    }

    private void runProducer(KafkaProducerWrapper producer, MessageDistributor messageDistributor,
                             int producerId, long producerGeneration) {
        // Correctness state for this producer
        boolean correctness = correctnessEnabled;
        boolean crc = enableCrc;
        ModelChecker checker = correctness && correctnessTracker != null
                ? correctnessTracker.checker() : null;
        java.util.concurrent.atomic.AtomicLong sequenceCounter = correctness
                ? producerSequences.computeIfAbsent(producerId,
                        k -> new java.util.concurrent.atomic.AtomicLong(0))
                : null;

        // Every record is a generated row; KEY_COLUMN takes the key from it.
        RowGenerator rowGen = requireDataset().newRowGenerator(producer.clientName(), producer.producerIdx());
        boolean keyFromRow = messageDistributor instanceof KeyColumnDistributor;

        // Transaction state: when transactional, group sends into batches and
        // call commitTransaction when EITHER trigger fires (OR semantics):
        //   * sentInTxn >= messagesPerTxn  (count-based)
        //   * elapsed nanos >= maxDurationNanos  (time-based)
        // Disabled triggers use sentinel values (NO_MESSAGE_LIMIT /
        // Long.MAX_VALUE) so the OR check naturally collapses.
        // commitTransaction() flushes all in-flight sends and throws
        // KafkaException if any failed, so it is the single source of truth
        // for "did this batch succeed" — no shared cross-thread flag needed.
        final boolean transactional = producer.isTransactional();
        final TxnTriggers triggers = transactional ? readTxnTriggers() : null;
        final int messagesPerTxn = transactional ? triggers.messagesPerTxn() : 0;
        final long maxDurationNanos = transactional && triggers.maxDurationMs() != TxnTriggers.NO_TIME_LIMIT
                ? TimeUnit.MILLISECONDS.toNanos(triggers.maxDurationMs())
                : Long.MAX_VALUE;
        boolean inTxn = false;
        int sentInTxn = 0;
        long txnStartNanos = 0L;

        // Cache the producer's name so the per-producer stop check is a
        // straight set lookup (not a clientName() round-trip per loop).
        final String producerName = producer.clientName();
        producerThreads.put(producerName, Thread.currentThread());
        try {
            while (!testCompleted && !stoppedProducerNames.contains(producerName)) {
                if (producersPaused) {
                    // Don't leave a half-open transaction parked indefinitely:
                    // close it out (commit or abort) before sleeping.
                    if (transactional && inTxn) {
                        finishTransaction(producer);
                        inTxn = false;
                        sentInTxn = 0;
                    }
                    try { Thread.sleep(100); } catch (InterruptedException e) { return; }
                    continue;
                }

                // The correctness fields become the record's _dimster header.
                final long seq;
                final byte[] header;
                if (correctness) {
                    seq = sequenceCounter.getAndIncrement();
                    header = CorrectnessPayload.header(producerId, seq, System.currentTimeMillis(), crc);
                } else {
                    seq = -1;
                    header = NORMAL_HEADER;
                }
                byte[] payloadData = rowGen.next(header);

                final long intendedSendTime = acquireNextSendTime();
                if (!sleepUntilSendTimeOrProducerStopped(intendedSendTime, producerName)) {
                    break;
                }
                final long sendTime = System.nanoTime();

                // Record publish delay before sendAsync(), else a period of unavailability
                // or spike in publish latency delays us from recording this value.
                stats.recordPublishDelay(sendTime, intendedSendTime);

                // Open a new transaction at the start of each batch. Doing this
                // *after* the rate-limit acquire keeps the transaction window
                // tight to actual sends — a slow rate-limiter wait wouldn't
                // hold an empty transaction open against the coordinator.
                if (transactional && !inTxn) {
                    producer.beginTransaction();
                    inTxn = true;
                    sentInTxn = 0;
                    txnStartNanos = System.nanoTime();
                }

                // The record key is either generated from the MessageDistributor itself,
                // or from the row itself. When MessageDistributor is KEY_COLUMN the key
                // is taken from the key column of the row, else it is generated by
                // the non-KEY_COLUMN MessageDistributor to satisfy the specified distribution.
                MessageDistributor.Target target = messageDistributor.next();
                String key = keyFromRow ? rowGen.key() : target.key();

                CompletableFuture<KafkaProducerWrapper.SendResult> f = producer.sendAsync(
                        key, target.partition(), payloadData);
                if (transactional) sentInTxn++;

                stats.recordScheduleLatency(
                        TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTime));

                final int pSize = payloadData.length;
                final boolean print = printMessages;
                final String topicForLog = producer.getTopic();
                // Rendered now, while the record is at hand; empty unless printContents is on.
                final String contents = print ? RecordPrinter.logSuffix(recordPrinter, key, payloadData) : "";
                f.thenAccept((sendResult) -> {
                    stats.recordSendSuccess(sendTime, pSize, sendResult.topicPartition());
                    if (print) {
                        log.info("PRODUCED topic={} partition={} offset={} timestamp={}{}",
                                topicForLog, sendResult.partition(),
                                sendResult.offset(), sendResult.timestamp(), contents);
                    }
                    if (checker != null) {
                        checker.messageSent(producerId, seq, producer.getTopic());
                    }
                }).exceptionally(ex -> {
                    // Stats only — never mutate loop-visible state from here.
                    // For transactional producers, commitTransaction() will
                    // surface the same failure on the producer-loop thread,
                    // which is the single source of truth for the abort
                    // decision. This avoids a cross-thread race where a late
                    // callback for batch A could poison batch B.
                    stats.recordSendError(ex, sendTime);
                    log.warn("On complete error: " + ex);
                    return null;
                });

                if (transactional
                        && (sentInTxn >= messagesPerTxn
                                || System.nanoTime() - txnStartNanos >= maxDurationNanos)) {
                    finishTransaction(producer);
                    inTxn = false;
                    sentInTxn = 0;
                }
            }
        } catch (Throwable t) {
            recordProducerFailureIfCurrent(producerName, producerGeneration,
                    producer.getTopic(), t);
            log.error("Fatal error in producer for topic: {}", producer.getTopic(), t);
        } finally {
            // Drain any in-flight transaction at shutdown so we don't leak
            // an open txn on the coordinator.
            if (transactional && inTxn) {
                finishTransaction(producer);
            }
            producerThreads.remove(producerName, Thread.currentThread());
        }
    }

    /**
     * Submit one producer loop and record the Future under the producer's
     * stable topology name. That Future is the handshake used by remove and
     * restart: the KafkaProducer is closed only after its owning loop exits.
     */
    private void submitProducerTask(KafkaProducerWrapper producer, MessageDistributor messageDistributor,
                                    int producerId) {
        long generation = activeProducerGenerations.computeIfAbsent(producer.clientName(),
                name -> producerGenerationCounter.incrementAndGet());
        Future<?> task = executor.submit(() -> runProducer(
                producer, messageDistributor, producerId, generation));
        producerTasks.put(producer.clientName(), task);
    }

    private long registerProducerInstance(String producerName) {
        long generation = producerGenerationCounter.incrementAndGet();
        activeProducerGenerations.put(producerName, generation);
        producerFailures.remove(producerName);
        return generation;
    }

    private void recordProducerFailureIfCurrent(String producerName, long producerGeneration,
                                                String topic, Throwable t) {
        Long currentGeneration = activeProducerGenerations.get(producerName);
        if (currentGeneration != null && currentGeneration == producerGeneration) {
            producerFailures.put(producerName,
                    t.getClass().getName() + ": " + t.getMessage() + " (topic=" + topic + ")");
            return;
        }
        log.warn("Ignoring stale producer failure for {} generation {} on topic {}: {}",
                producerName, producerGeneration, topic, t.toString());
    }

    /**
     * Sleep until the shared rate limiter's next send time. removeProducers
     * unparks the producer thread after setting the stop flag, so a parked
     * task can leave promptly instead of waking later and sending after close.
     */
    private boolean sleepUntilSendTimeOrProducerStopped(long intendedTime, String producerName) {
        long sleepNs;
        while ((sleepNs = intendedTime - System.nanoTime()) > 0) {
            if (testCompleted || stoppedProducerNames.contains(producerName)) {
                return false;
            }
            LockSupport.parkNanos(sleepNs);
        }
        return !(testCompleted || stoppedProducerNames.contains(producerName));
    }

    /**
     * Always attempts commit; commitTransaction is the single source of truth
     * for "did this batch succeed". It flushes all in-flight sends and throws
     * KafkaException if any send failed, in which case we abort and count the
     * txn as aborted — matching Kafka's contract that a failed commit must be
     * followed by abortTransaction before the producer can be reused.
     */
    private void finishTransaction(KafkaProducerWrapper producer) {
        long commitStart = System.nanoTime();
        try {
            producer.commitTransaction();
            stats.recordTransactionCommit(
                    TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - commitStart));
        } catch (Exception ce) {
            log.warn("commitTransaction failed for {} — aborting: {}",
                    producer.getTopic(), ce.toString());
            producer.abortTransaction();
            stats.recordTransactionAbort();
        }
    }

    /**
     * Resolved per-producer commit triggers. {@code messagesPerTxn} of
     * {@link #NO_MESSAGE_LIMIT} or {@code maxDurationMs} of
     * {@link #NO_TIME_LIMIT} signals "this trigger is disabled" — the loop
     * uses these sentinels so the OR check naturally collapses to whichever
     * trigger(s) the user actually configured.
     */
    private record TxnTriggers(int messagesPerTxn, long maxDurationMs) {
        static final int NO_MESSAGE_LIMIT = Integer.MAX_VALUE;
        static final long NO_TIME_LIMIT = Long.MAX_VALUE;
    }

    /**
     * Resolve transaction commit triggers from the current KafkaConfig. At
     * least one of {@code dimster.transactions.messages.per.txn} or
     * {@code dimster.transactions.max.txn.duration.ms} must be set; either
     * alone is valid, both is valid (OR semantics in the loop). Scenario
     * sweeps already collapse list values to scalars via
     * {@code KafkaConfig.resolveAtIndex}, so by producer-thread start these
     * are single values.
     */
    private TxnTriggers readTxnTriggers() {
        Object msgVal = readDimsterProducerKey(KafkaConfig.DIMSTER_TRANSACTIONS_MESSAGES_PER_TXN);
        Object timeVal = readDimsterProducerKey(KafkaConfig.DIMSTER_TRANSACTIONS_MAX_DURATION_MS);
        if (msgVal == null && timeVal == null) {
            throw new IllegalStateException(
                    KafkaConfig.DIMSTER_TRANSACTIONS_ENABLED + "=true requires at least one of "
                            + KafkaConfig.DIMSTER_TRANSACTIONS_MESSAGES_PER_TXN + " or "
                            + KafkaConfig.DIMSTER_TRANSACTIONS_MAX_DURATION_MS
                            + " to be set in producerConfig");
        }
        int messagesPerTxn = msgVal == null
                ? TxnTriggers.NO_MESSAGE_LIMIT
                : parsePositiveInt(KafkaConfig.DIMSTER_TRANSACTIONS_MESSAGES_PER_TXN, msgVal);
        long maxDurationMs = timeVal == null
                ? TxnTriggers.NO_TIME_LIMIT
                : parsePositiveLong(KafkaConfig.DIMSTER_TRANSACTIONS_MAX_DURATION_MS, timeVal);
        return new TxnTriggers(messagesPerTxn, maxDurationMs);
    }

    private Object readDimsterProducerKey(String key) {
        return kafkaConfig.producerConfig != null
                ? kafkaConfig.producerConfig.get(key)
                : null;
    }

    private static int parsePositiveInt(String key, Object val) {
        int parsed;
        try {
            parsed = Integer.parseInt(val.toString());
        } catch (NumberFormatException nfe) {
            throw new IllegalStateException(key + " must be an integer, got " + val);
        }
        if (parsed <= 0) {
            throw new IllegalStateException(key + " must be > 0, got " + parsed);
        }
        return parsed;
    }

    private static long parsePositiveLong(String key, Object val) {
        long parsed;
        try {
            parsed = Long.parseLong(val.toString());
        } catch (NumberFormatException nfe) {
            throw new IllegalStateException(key + " must be a long, got " + val);
        }
        if (parsed <= 0) {
            throw new IllegalStateException(key + " must be > 0, got " + parsed);
        }
        return parsed;
    }

    @Override
    public void adjustPublishRate(double publishRate) throws IOException {
        if (publishRate == 0.0) {
            pauseProducers();
            return;
        } else if (producersPaused) {
            resumeProducers();
        }
        
        log.info("Adjust publish-rate: {} msg/s", publishRate);
        scheduledRateLimiter = null;
        rateLimiter = new UniformRateLimiter(publishRate);
    }

    @Override
    public void adjustPublishRateSchedule(ProducerRateSchedule schedule) throws IOException {
        adjustPublishRateSchedule(schedule, System.currentTimeMillis());
    }

    public void adjustPublishRateSchedule(ProducerRateSchedule schedule, long scheduleStartEpochMillis) throws IOException {
        if (schedule == null) {
            throw new IllegalArgumentException("producerRateSchedule cannot be null");
        }
        if (producersPaused) {
            resumeProducers();
        }

        log.info("Adjust variable publish-rate schedule: {} (max {} msg/s)",
                schedule.type, schedule.maxConfiguredRate());
        scheduledRateLimiter = new ScheduledRateLimiter(schedule, scheduleStartEpochMillis);
        rateLimiter = null;
    }

    private long acquireNextSendTime() {
        ScheduledRateLimiter schedule = scheduledRateLimiter;
        if (schedule != null) {
            return schedule.acquire();
        }
        return rateLimiter.acquire();
    }

    @Override
    public PeriodStats getPeriodStats() {
        PeriodStats periodStats = stats.snapshotAndReset();
        periodStats.producerCount = producers.size();
        periodStats.consumerCount = consumers.size();
        periodStats.consumerRecreations = totalRecreations.get();
        metricsEndpoint.updateStats(periodStats);
        return periodStats;
    }

    @Override
    public PeriodStats getPeriodStats(boolean includePerPartitionIntervals) {
        // The boolean is meaningful only for RemoteWorkerClient (controls wire
        // serialization). In-process per-partition intervals are always populated.
        return getPeriodStats();
    }

    @Override
    public CountersStats getCountersStats() throws IOException {
        return stats.counters();
    }

    @Override
    public long getProbesReceived() {
        return stats.getProbesReceived();
    }

    @Override
    public void pauseProducers() throws IOException {
        producersPaused = true;
        log.info("Pausing producers");
    }

    @Override
    public void resumeProducers() throws IOException {
        producersPaused = false;
        log.info("Resuming producers");
    }

    @Override
    public void resetStats() throws IOException {
        stats.resetRecorders();
    }

    @Override
    public void stopAll() throws IOException {
        if (!stopping.compareAndSet(false, true)) {
            log.info("stopAll already in progress, skipping");
            return;
        }

        stopComplete = false;
        stats.resetAll();
        metricsEndpoint.reset();
        metricsEndpoint.setKafkaMetricsSources(null);
        producersPaused = false;
        testCompleted = true;

        // Wait for recreation thread to finish so no orphan consumers are created after snapshot
        if (recreationThread != null) {
            try {
                recreationThread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            recreationThread = null;
        }

        // Stop the lag monitor
        if (lagMonitor != null) {
            lagMonitor.close();
            lagMonitor = null;
            metricsEndpoint.setLagMonitor(null);
        }

        // Snapshot current lists and launch async close
        var producerSnapshot = List.copyOf(producers);
        var consumerSnapshot = List.copyOf(consumers);
        var probeProducerSnapshot = List.copyOf(probeProducersList);

        // Thread.ofVirtual().name("stop-all").start(() -> {
        Thread.ofPlatform().name("stop-all").start(() -> {
            try {
                Thread.sleep(1000); // grace period for poll loops to exit

                closeInParallel("producers", producerSnapshot);
                closeInParallel("consumers", consumerSnapshot);
                closeInParallel("probe-producers", probeProducerSnapshot);
            } catch (Exception e) {
                log.error("Error during parallel close", e);
            } finally {
                producers.clear();
                producerTasks.clear();
                producerThreads.clear();
                activeProducerGenerations.clear();
                consumers.clear();
                probeProducersList.clear();
                probeConsumer = null;
                producerFailures.clear();
                producerSequences.clear();
                correctnessEnabled = false;
                correctnessConfig = null;
                correctnessTracker = null;
                consumerSet = null;
                consumerCallback = null;
                availabilityEnabled = false;
                availabilityRecorder = null;
                availabilityConfig = null;
                kafkaConfig = null;

                stopComplete = true;
                stopping.set(false);
                log.info("stopAll complete");
            }
        });
    }

    private void closeInParallel(String label, List<? extends AutoCloseable> closeables) {
        if (closeables.isEmpty()) return;

        log.info("Closing {} {} in parallel", closeables.size(), label);
        var semaphore = new Semaphore(50);
        var futures = closeables.stream().map(c -> CompletableFuture.runAsync(() -> {
            try {
                semaphore.acquire();
                try {
                    c.close();
                } finally {
                    semaphore.release();
                }
            } catch (Exception e) {
                log.warn("Error closing {}: {}", label, e);
            }
        }, executor)).toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
        log.info("Closed {} {}", closeables.size(), label);
    }

    @Override
    public boolean isStopComplete() {
        return stopComplete;
    }

    @Override
    public ClientHealth getHealth() {
        var health = ClientHealth.builder();
        // Snapshot lists to avoid ConcurrentModificationException from concurrent stopAll/setup
        var producerSnapshot = List.copyOf(producers);
        var consumerSnapshot = List.copyOf(consumers);
        for (var producer : producerSnapshot) {
            // Use clientName() (the planner's stable identity, e.g.
            // "producer-{topic}-i{idx}") so each producer shows up
            // distinctly. Failures are keyed by this same name, not by
            // topic, so one failed producer task does not poison every
            // replacement/current producer on that topic.
            String name = producer.clientName();
            String error = producerFailures.get(name);
            if (error != null) {
                health.addProducer(ClientStatus.failed(name, "PRODUCER", error));
            } else {
                health.addProducer(ClientStatus.running(name, "PRODUCER"));
            }
        }
        for (int i = 0; i < consumerSnapshot.size(); i++) {
            var consumer = consumerSnapshot.get(i);
            if (consumer instanceof KafkaConsumerWrapper cc) {
                health.addConsumer(consumerStatus(i, cc.getName(), "CLASSIC_CONSUMER",
                        cc.isRunning(), cc.getFailureReason()));
            } else if (consumer instanceof KafkaShareConsumerWrapper sc) {
                health.addConsumer(consumerStatus(i, sc.getName(), "SHARE_CONSUMER",
                        sc.isRunning(), sc.getFailureReason()));
            }
        }
        return health.build();
    }

    private ClientStatus consumerStatus(int index, String name, String type, boolean running, String failureReason) {
        if (failureReason != null) {
            if (maxConsumerRecreations > 0
                    && recreationCounts.getOrDefault(index, 0) < maxConsumerRecreations) {
                return ClientStatus.recreating(name, type, failureReason);
            }
            return ClientStatus.failed(name, type, failureReason);
        } else if (running) {
            return ClientStatus.running(name, type);
        } else {
            return ClientStatus.pending(name, type);
        }
    }

    @Override
    public void setDataset(ResolvedDataset dataset) {
        if (dataset == null) {
            throw new IllegalArgumentException("dataset is required; workloads without one use the payload dataset");
        }
        this.datasetGenerator = new DatasetGenerator(dataset);
        this.recordFormat = dataset.format();
        updateRecordPrinter();
        log.info("Dataset: preset={} format={} columns={} keyColumn={}",
                dataset.preset(), dataset.format(), dataset.columns().size(), dataset.keyColumn());
    }

    @Override
    public void setPrintMessages(boolean enabled) {
        this.printMessages = enabled;
        log.info("Per-message printing {}", enabled ? "enabled" : "disabled");
    }

    @Override
    public void setPrintContents(boolean enabled) {
        this.printContents = enabled;
        updateRecordPrinter();
        log.info("Printing of record contents {}", enabled ? "enabled" : "disabled");
    }

    /**
     * Rebuilds the printer, which depends on both the printContents flag and
     * the current dataset (Avro records are decoded with its schema).
     */
    private void updateRecordPrinter() {
        DatasetGenerator dataset = datasetGenerator;
        recordPrinter = printContents && dataset != null ? new RecordPrinter(dataset.dataset()) : null;
    }

    @Override
    public void setMaxConsumerRecreations(int max) {
        this.maxConsumerRecreations = max;
        if (max > 0 && recreationThread == null) {
            log.info("Consumer recreation enabled (max {} per consumer), starting background thread", max);
            // recreationThread = Thread.ofVirtual()
            //         .name("consumer-recreation")
            //         .start(this::recreationLoop);
            recreationThread = Thread.ofPlatform()
                    .name("consumer-recreation")
                    .start(this::recreationLoop);
        }
    }

    @Override
    public void setConsumerProcessingTime(int minMs, int maxMs) {
        log.info("Adjusting consumer processing time: min={}, max={}", minMs, maxMs);
        for (var consumer : consumers) {
            if (consumer instanceof KafkaConsumerWrapper kc) {
                kc.setProcessingTime(minMs, maxMs);
            } else if (consumer instanceof KafkaShareConsumerWrapper ksc) {
                ksc.setProcessingTime(minMs, maxMs);
            }
        }
        // Flip the delivery-latency gate so /modify can turn the metric on/off mid-run.
        stats.setRecordDeliveryLatency(maxMs > 0);
        log.info("Consumer processing time adjusted");
    }

    @Override
    public void setConsumerProcCapacity(int capacity) {
        log.info("Adjusting consumer proc capacity: {}", capacity);
        for (var consumer : consumers) {
            if (consumer instanceof KafkaConsumerWrapper kc) {
                kc.setQueueCapacity(capacity);
            }
        }
        log.info("Consumer proc capacity adjusted");
    }

    // ─── Incremental topology operations (driven by /modify pipeline) ────

    @Override
    public synchronized void addProducers(List<VirtualProducer> virtualProducers) throws IOException {
        if (virtualProducers.isEmpty()) return;
        if (kafkaConfig == null) {
            throw new IOException("addProducers: setKafkaConfig must be called first");
        }
        var producerProps = kafkaConfig.producerProperties();
        boolean transactional = kafkaConfig.transactionsEnabled();
        String hostname = transactional ? HostUtil.hostname() : null;

        for (VirtualProducer vp : virtualProducers) {
            Properties perProducerProps = transactional
                    ? cloneWithTransactionalId(producerProps, vp.transactionalId(hostname))
                    : producerProps;
            // Defence-in-depth: if a prior remove left this name in the
            // stop set (the bug fixed in removeProducers), claim it back
            // here so the about-to-start runProducer thread doesn't see
            // its name as "stopped" on the very first iteration.
            stoppedProducerNames.remove(vp.name());
            var kafkaProducer = KafkaSpi.factory().<String, byte[]>newProducer(perProducerProps);
            var wrapper = new KafkaProducerWrapper(kafkaProducer, vp.topic(), vp.name(),
                    vp.producerIdx(), vp.pinnedPartitions(), transactional);
            initTransactionsTimed(wrapper, perProducerProps);
            registerProducerInstance(vp.name());
            producers.add(wrapper);
            log.info("ADD-PRODUCER {} topic={} pinnedPartitions={}",
                    vp.name(), vp.topic(), Arrays.toString(vp.pinnedPartitions()));
            // If startLoad has already fired, spin up the runProducer task
            // right away so the new producer joins the load immediately. It
            // captures the current dataset, which is why messageSize changes
            // set the new payload dataset before restarting producers.
            if (activeMessageDistributor != null) {
                int producerId = producerIdCounter.getAndIncrement();
                submitProducerTask(wrapper, activeMessageDistributor, producerId);
            }
        }
        updateKafkaMetricsSources();
    }

    @Override
    public synchronized void removeProducers(List<String> producerNames) throws IOException {
        if (producerNames.isEmpty()) return;
        // Phase 1: signal each producer's runProducer task to exit cleanly.
        // The loop checks this set before and after its rate-limiter sleep.
        for (String name : producerNames) {
            stoppedProducerNames.add(name);
        }
        unparkProducerTasks(producerNames);
        waitForProducerTasksToStop(producerNames);
        // Phase 2: snapshot matching wrappers, drop them from `producers`
        // before closing (so readers don't see closing-but-still-listed
        // wrappers), then close in parallel. close() is blocking and flushes
        // any remaining in-flight sends; doing them concurrently keeps a
        // many-producer restart from serialising on per-producer flush time.
        Set<String> toRemove = new HashSet<>(producerNames);
        List<KafkaProducerWrapper> toClose = new ArrayList<>();
        for (Iterator<KafkaProducerWrapper> it = producers.iterator(); it.hasNext(); ) {
            KafkaProducerWrapper p = it.next();
            if (toRemove.contains(p.clientName())) {
                toClose.add(p);
                it.remove();
            }
        }
        // Wrap so per-name success / failure logging survives the parallel
        // close (closeInParallel's own catch only carries the label).
        List<AutoCloseable> wrapped = toClose.stream().map(p -> (AutoCloseable) () -> {
            try {
                p.close();
                log.info("REMOVE-PRODUCER {} closed", p.clientName());
            } catch (Exception e) {
                log.warn("REMOVE-PRODUCER {} close failed: {}", p.clientName(), e.toString());
            }
        }).toList();
        closeInParallel("producers", wrapped);
        producerTasks.keySet().removeAll(producerNames);
        producerThreads.keySet().removeAll(producerNames);
        activeProducerGenerations.keySet().removeAll(producerNames);
        producerFailures.keySet().removeAll(producerNames);
        // Clean up stoppedProducerNames for ALL requested names, not just
        // those whose wrappers we found. A name can appear in the request
        // without a corresponding wrapper if a prior failed
        // restartProducers removed the wrapper but didn't get to re-add
        // it (e.g. the new KafkaProducer constructor threw). Without this
        // cleanup, the next addProducers for the same name would spawn a
        // runProducer that exits on its first iteration because
        // stoppedProducerNames still flags the name as stopped.
        stoppedProducerNames.removeAll(producerNames);
        updateKafkaMetricsSources();
    }

    private void unparkProducerTasks(List<String> producerNames) {
        for (String name : producerNames) {
            Thread thread = producerThreads.get(name);
            if (thread != null) {
                LockSupport.unpark(thread);
            }
        }
    }

    /**
     * Wait briefly for producer loops to acknowledge their stop flags before
     * close() is called on the underlying KafkaProducer. The timeout keeps a
     * modify from hanging forever if a producer loop is already wedged in a
     * Kafka call; in that rare case close() is still attempted and the warning
     * gives the worker log a direct clue about the lifecycle race.
     */
    private void waitForProducerTasksToStop(List<String> producerNames) {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        for (String name : producerNames) {
            Future<?> task = producerTasks.get(name);
            if (task == null || task.isDone()) {
                continue;
            }
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                log.warn("Timed out waiting for producer task {} to stop before close", name);
                continue;
            }
            try {
                task.get(remainingNanos, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                log.warn("Timed out waiting for producer task {} to stop before close", name);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException e) {
                log.warn("Producer task {} exited with failure before close: {}",
                        name, e.getCause() == null ? e.toString() : e.getCause().toString());
            }
        }
    }

    @Override
    public synchronized void restartProducers(List<VirtualProducer> virtualProducers) throws IOException {
        if (virtualProducers.isEmpty()) return;
        // Restart = remove + add. The new wrappers pick up the latest
        // kafkaConfig (set by the caller via setKafkaConfig before this
        // call) and any updated pinning. Identity (name, transactional.id)
        // is preserved because VirtualProducer.name() is index-based.
        List<String> names = virtualProducers.stream().map(VirtualProducer::name).toList();
        log.info("RESTART-PRODUCERS ({}): {}", names.size(), names);
        removeProducers(names);
        addProducers(virtualProducers);
    }

    @Override
    public synchronized void addConsumers(ConsumerSet newConsumers) throws IOException {
        if (newConsumers.consumers().isEmpty()) return;
        if (kafkaConfig == null) {
            throw new IOException("addConsumers: setKafkaConfig must be called first");
        }
        // Merge the new consumers into our stored consumerSet so the
        // recreation thread (and any subsequent addConsumers / restart)
        // sees them. Indices in the merged set match positions in the
        // `consumers` list after appending.
        ConsumerSet merged = mergeConsumerSet(consumerSet, newConsumers);
        int firstNewIndex = consumerSet == null ? 0 : consumerSet.consumers().size();
        this.consumerSet = merged;
        for (int i = 0; i < newConsumers.consumers().size(); i++) {
            VirtualConsumer vc = newConsumers.consumers().get(i);
            var wrapper = createSingleConsumer(firstNewIndex + i);
            consumers.add(wrapper);
            log.info("ADD-CONSUMER {} topic={} group={}",
                    vc.name(), vc.topic(), vc.consumerGroup());
        }
        // Re-evaluate the delivery-latency gate from the merged ConsumerSet —
        // newly added consumers may carry a different procMs range.
        stats.setRecordDeliveryLatency(this.consumerSet.consumerProcMsMax() > 0);
        updateKafkaMetricsSources();
    }

    @Override
    public synchronized void removeConsumers(List<String> consumerNames) throws IOException {
        if (consumerNames.isEmpty()) return;
        // Drop matching wrappers + matching VirtualConsumer entries from
        // consumerSet. We rebuild consumerSet rather than mutate so the
        // record's invariants stay intact.
        Set<String> toRemove = new HashSet<>(consumerNames);
        // Snapshot matching wrappers and drop them from `consumers` before
        // closing — readers like getHealth / updateKafkaMetricsSources won't
        // see closing-but-still-listed wrappers — then close in parallel.
        List<AutoCloseable> toClose = new ArrayList<>();
        for (Iterator<AutoCloseable> it = consumers.iterator(); it.hasNext(); ) {
            AutoCloseable c = it.next();
            String name = consumerName(c);
            if (name != null && toRemove.contains(name)) {
                toClose.add(c);
                it.remove();
            }
        }
        // Wrap so per-name success / failure logging survives the parallel
        // close (closeInParallel's own catch only carries the label).
        List<AutoCloseable> wrapped = toClose.stream().map(c -> (AutoCloseable) () -> {
            String name = consumerName(c);
            try {
                c.close();
                log.info("REMOVE-CONSUMER {} closed", name);
            } catch (Exception e) {
                log.warn("REMOVE-CONSUMER {} close failed: {}", name, e.toString());
            }
        }).toList();
        closeInParallel("consumers", wrapped);
        if (consumerSet != null) {
            List<VirtualConsumer> kept = new ArrayList<>();
            for (VirtualConsumer vc : consumerSet.consumers()) {
                if (!toRemove.contains(vc.name())) kept.add(vc);
            }
            consumerSet = new ConsumerSet(kept, consumerSet.consumerType(),
                    consumerSet.consumerProcMsMin(), consumerSet.consumerProcMsMax(), consumerSet.consumerProcParallelism(),
                    consumerSet.consumerProcMode(), consumerSet.consumerProcCapacity());
        }
        updateKafkaMetricsSources();
    }

    @Override
    public synchronized void restartConsumers(ConsumerSet toRestart) throws IOException {
        if (toRestart.consumers().isEmpty()) return;
        List<String> names = toRestart.consumers().stream().map(VirtualConsumer::name).toList();
        log.info("RESTART-CONSUMERS ({}): {}", names.size(), names);
        // restart-with-new-consumer-type is supported: the toRestart
        // ConsumerSet carries the new consumerType, and addConsumers will
        // use it. mergeConsumerSet uses the newer type when types differ.
        removeConsumers(names);
        addConsumers(toRestart);
    }

    /** Look up a consumer wrapper's stable name (returns null for unknown wrapper types). */
    private static String consumerName(AutoCloseable c) {
        if (c instanceof KafkaConsumerWrapper kc) return kc.getName();
        if (c instanceof KafkaShareConsumerWrapper sc) return sc.getName();
        return null;
    }

    /**
     * Merge two ConsumerSets, appending the second's consumers to the
     * first's. The merged set takes the consumerType / processing-time
     * settings from {@code added} (so a restartConsumers that flips
     * CONSUMER_GROUP ↔ SHARE_GROUP applies cleanly). When {@code base} is null,
     * returns the added set unchanged.
     */
    private static ConsumerSet mergeConsumerSet(ConsumerSet base, ConsumerSet added) {
        if (base == null) return added;
        List<VirtualConsumer> all = new ArrayList<>(base.consumers());
        all.addAll(added.consumers());
        return new ConsumerSet(all, added.consumerType(),
                added.consumerProcMsMin(), added.consumerProcMsMax(), added.consumerProcParallelism(),
                added.consumerProcMode(), added.consumerProcCapacity());
    }

    private void recreationLoop() {
        log.info("Consumer recreation thread started");
        while (!stopping.get()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
            recreateFailedConsumers();
        }
        log.info("Consumer recreation thread ended");
    }

    private void recreateFailedConsumers() {
        if (maxConsumerRecreations <= 0 || consumers.isEmpty() || consumerSet == null) {
            return;
        }

        for (int i = 0; i < consumers.size() && !stopping.get(); i++) {
            var consumer = consumers.get(i);
            String failureReason = null;
            String name = null;

            if (consumer instanceof KafkaConsumerWrapper cc) {
                failureReason = cc.getFailureReason();
                name = cc.getName();
            } else if (consumer instanceof KafkaShareConsumerWrapper sc) {
                failureReason = sc.getFailureReason();
                name = sc.getName();
            }

            if (failureReason == null) continue;

            int count = recreationCounts.getOrDefault(i, 0);
            if (count >= maxConsumerRecreations) {
                log.error("Consumer '{}' at index {} has failed {} times, not recreating", name, i, count);
                continue;
            }

            log.warn("Consumer '{}' failed: {}. Recreating ({}/{})", name, failureReason, count + 1, maxConsumerRecreations);

            try {
                consumer.close();
            } catch (Exception e) {
                log.warn("Error closing failed consumer '{}': {}", name, e.getMessage());
            }

            try {
                var replacement = createSingleConsumer(i);
                consumers.set(i, replacement);
                updateKafkaMetricsSources();
                log.info("Successfully recreated consumer at index {} ({}/{})", i, count + 1, maxConsumerRecreations);
            } catch (Exception e) {
                log.error("Failed to recreate consumer at index {}: {}", i, e.getMessage(), e);
            } finally {
                recreationCounts.put(i, count + 1);
                totalRecreations.incrementAndGet();
            }
        }
    }

    private AutoCloseable createSingleConsumer(int index) {
        VirtualConsumer vc = consumerSet.consumers().get(index);

        if (consumerSet.consumerType() == ConsumerType.SHARE_GROUP) {
            // Fail fast with a clear remediation message if this dimster build is bound to
            // a Kafka client that doesn't support share groups (e.g. dimster-kafka-3.x).
            // Without this, we'd get an UnsupportedOperationException from deep inside the
            // SPI which is harder to interpret in the worker logs.
            KafkaSpi.requireShareGroups(
                    "Consumer at index " + index + " (group=" + vc.consumerGroup() + ", topic=" + vc.topic() + ")");

            var consumerProps = kafkaConfig.shareConsumerProperties();
            var clientProps = new Properties();
            clientProps.put(ConsumerConfig.GROUP_ID_CONFIG, vc.consumerGroup());
            clientProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120_000);

            var allProps = new Properties();
            consumerProps.forEach((k, v) -> {
                String key = (String) k;
                if (!key.startsWith("dimster.")) {
                    clientProps.put(k, v);
                }
                allProps.put(k, v);
            });

            var kafkaConsumer = KafkaSpi.factory().<String, byte[]>newShareConsumer(clientProps);
            return new KafkaShareConsumerWrapper(kafkaConsumer, allProps, consumerCallback, vc.topic(),
                    vc.name(),
                    consumerSet.consumerProcMsMin(), consumerSet.consumerProcMsMax(), consumerSet.consumerProcParallelism(),
                    printMessages, recordPrinter, recordFormat);
        } else {
            var consumerProps = kafkaConfig.classicConsumerProperties();
            var clientProps = new Properties();
            clientProps.put(ConsumerConfig.GROUP_ID_CONFIG, vc.consumerGroup());
            clientProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120_000);

            var allProps = new Properties();
            consumerProps.forEach((k, v) -> {
                String key = (String) k;
                if (!key.startsWith("dimster.")) {
                    clientProps.put(k, v);
                }
                allProps.put(k, v);
            });

            // Static partitions come straight off the VirtualConsumer (computed by TopologyPlanner).
            // KafkaConsumerWrapper treats null/empty the same way: empty -> dynamic group assignment.
            List<TopicPartition> staticPartitions = vc.staticPartitions().isEmpty()
                    ? null
                    : vc.staticPartitions();

            var kafkaConsumer = KafkaSpi.factory().<String, byte[]>newConsumer(clientProps);
            return new KafkaConsumerWrapper(kafkaConsumer, allProps, consumerCallback, vc.topic(),
                    vc.name(),
                    consumerSet.consumerProcMsMin(), consumerSet.consumerProcMsMax(), consumerSet.consumerProcParallelism(),
                    staticPartitions, printMessages, recordPrinter, recordFormat,
                    consumerSet.consumerProcMode(), consumerSet.consumerProcCapacity());
        }
    }

    @Override
    public void enableCorrectness(CorrectnessConfig config) {
        // Worker-side correctness state covers payload encoding (sequence + CRC), the
        // ModelChecker for duplicate/out-of-order detection, and the consumer callback wiring.
        // Static partition assignments are no longer the worker's concern — they are baked
        // into the VirtualTopology that the engine builds via TopologyPlanner.
        this.correctnessEnabled = true;
        this.correctnessConfig = config;
        this.enableCrc = config.enableCrc;
        ModelChecker checker = new ModelChecker(config.detectDuplicates, config.detectOutOfOrder, config.logDuplicateCount);
        this.correctnessTracker = new CorrectnessTracker(stats, checker);
        log.info("Correctness mode enabled (duplicates={}, outOfOrder={}, crc={})",
                config.detectDuplicates, config.detectOutOfOrder, config.enableCrc);
    }

    @Override
    public CorrectnessResult.TestOutcome getCorrectnessResult() {
        if (correctnessTracker == null) return null;

        ModelChecker checker = correctnessTracker.checker();
        ModelChecker.LossReport loss = checker.computeLoss();

        var outcome = new CorrectnessResult.TestOutcome();
        outcome.correctness.totalSent = checker.getTotalSent();
        outcome.correctness.totalReceived = checker.getTotalReceived();
        outcome.correctness.lostMessages = loss.totalLost();
        outcome.correctness.duplicateCount = checker.getDuplicateCount();
        outcome.correctness.outOfOrderCount = checker.getOutOfOrderCount();
        outcome.correctness.crcFailureCount = checker.getCrcFailureCount();
        boolean passed = true;
        if (correctnessConfig.failOnLoss && loss.totalLost() > 0) passed = false;
        if (correctnessConfig.failOnDuplicates && checker.getDuplicateCount() > 0) passed = false;
        if (correctnessConfig.failOnOutOfOrder && checker.getOutOfOrderCount() > 0) passed = false;
        if (correctnessConfig.failOnCrc && checker.getCrcFailureCount() > 0) passed = false;
        outcome.correctness.passed = passed;

        for (var range : loss.ranges()) {
            outcome.correctness.lostMessageRanges.add(
                    new CorrectnessResult.LostMessageRangeEntry(
                            range.producerId(), range.topic(),
                            range.fromSequence(), range.toSequence(), range.count()));
        }

        // Sort violations by (producerId, topic, partition, type, sequence) so that
        // consecutive violations from the same stream are adjacent for range collapsing.
        List<ModelChecker.Violation> sorted = new ArrayList<>(checker.getViolations());
        sorted.sort((a, b) -> {
            int cmp = Integer.compare(violationProducerId(a), violationProducerId(b));
            if (cmp != 0) return cmp;
            cmp = violationTopic(a).compareTo(violationTopic(b));
            if (cmp != 0) return cmp;
            cmp = Integer.compare(violationPartition(a), violationPartition(b));
            if (cmp != 0) return cmp;
            cmp = violationType(a).compareTo(violationType(b));
            if (cmp != 0) return cmp;
            return Long.compare(violationSequence(a), violationSequence(b));
        });

        for (var v : sorted) {
            String type;
            int producerId;
            long sequence;
            String topic;
            int partition;
            long highSequence;
            long timestampMs = violationTimestampMs(v);

            switch (v) {
                case ModelChecker.DuplicateViolation d -> {
                    type = "DUPLICATE"; producerId = d.producerId();
                    sequence = d.sequence(); topic = d.topic(); partition = d.partition();
                    highSequence = d.highSequence();
                }
                case ModelChecker.OutOfOrderViolation o -> {
                    type = "OUT_OF_ORDER"; producerId = o.producerId();
                    sequence = o.sequence(); topic = o.topic(); partition = o.partition();
                    highSequence = o.highSequence();
                }
                case ModelChecker.CrcFailureViolation c -> {
                    type = "CRC_FAILURE"; producerId = c.producerId();
                    sequence = c.sequence(); topic = c.topic(); partition = c.partition();
                    highSequence = -1;
                }
            }

            // Try to extend the last entry if it's the same type/producer/topic/partition/highSequence
            List<CorrectnessResult.ViolationEntry> violations = outcome.correctness.violations;
            if (!violations.isEmpty()) {
                var last = violations.getLast();
                if (last.type.equals(type) && last.producerId == producerId
                        && last.topic.equals(topic) && last.partition == partition
                        // Preserve timeline resolution: adjacent sequence
                        // violations detected at different times must remain
                        // separate entries rather than becoming one range.
                        && last.timestampMs == timestampMs
                        && last.tryExtend(sequence, highSequence)) {
                    continue;
                }
            }
            violations.add(new CorrectnessResult.ViolationEntry(
                    timestampMs, type, producerId, topic, sequence, partition, highSequence));
        }

        return outcome;
    }

    // -- Helpers for sorting violation records by their fields --

    private static int violationProducerId(ModelChecker.Violation v) {
        return switch (v) {
            case ModelChecker.DuplicateViolation d -> d.producerId();
            case ModelChecker.OutOfOrderViolation o -> o.producerId();
            case ModelChecker.CrcFailureViolation c -> c.producerId();
        };
    }

    private static String violationTopic(ModelChecker.Violation v) {
        return switch (v) {
            case ModelChecker.DuplicateViolation d -> d.topic();
            case ModelChecker.OutOfOrderViolation o -> o.topic();
            case ModelChecker.CrcFailureViolation c -> c.topic();
        };
    }

    private static int violationPartition(ModelChecker.Violation v) {
        return switch (v) {
            case ModelChecker.DuplicateViolation d -> d.partition();
            case ModelChecker.OutOfOrderViolation o -> o.partition();
            case ModelChecker.CrcFailureViolation c -> c.partition();
        };
    }

    private static String violationType(ModelChecker.Violation v) {
        return switch (v) {
            case ModelChecker.DuplicateViolation ignored -> "DUPLICATE";
            case ModelChecker.OutOfOrderViolation ignored -> "OUT_OF_ORDER";
            case ModelChecker.CrcFailureViolation ignored -> "CRC_FAILURE";
        };
    }

    private static long violationSequence(ModelChecker.Violation v) {
        return switch (v) {
            case ModelChecker.DuplicateViolation d -> d.sequence();
            case ModelChecker.OutOfOrderViolation o -> o.sequence();
            case ModelChecker.CrcFailureViolation c -> c.sequence();
        };
    }

    /**
     * Return the time at which the checker detected a violation.
     *
     * <p>This is package-visible for a focused contract test. Using result
     * collection time here would collapse an entire run's timeline into the
     * finalization instant, making correctness event charts misleading.
     */
    static long violationTimestampMs(ModelChecker.Violation v) {
        return switch (v) {
            case ModelChecker.DuplicateViolation d -> d.timestampMs();
            case ModelChecker.OutOfOrderViolation o -> o.timestampMs();
            case ModelChecker.CrcFailureViolation c -> c.timestampMs();
        };
    }

    @Override
    public void resetCorrectness() {
        if (correctnessTracker != null) {
            correctnessTracker.checker().reset();
        }
        producerSequences.values().forEach(seq -> seq.set(0));
    }

    @Override
    public void enableAvailability(AvailabilityConfig config) {
        this.availabilityEnabled = true;
        this.availabilityConfig = config;
        this.availabilityRecorder = new AvailabilityRecorder(config.unavailabilityThresholdMs);
        log.info("Availability mode enabled (threshold={}ms, probeTimeout={}s)",
                config.unavailabilityThresholdMs, config.probeTimeoutSeconds);
    }

    @Override
    public void startAvailabilityProbes(List<ProbeAssignment> assignments) {
        if (!availabilityEnabled || availabilityConfig == null) {
            log.warn("startAvailabilityProbes called but availability mode is not enabled");
            return;
        }

        // Group by topic — one probe producer per topic
        var producerProps = kafkaConfig.producerProperties();
        Map<String, List<ProbeAssignment>> byTopic = new LinkedHashMap<>();
        for (var a : assignments) {
            byTopic.computeIfAbsent(a.topic(), k -> new ArrayList<>()).add(a);
        }
        for (var entry : byTopic.entrySet()) {
            var probeProducer = KafkaSpi.factory().<String, byte[]>newProducer(producerProps);
            probeProducersList.add(probeProducer);
            for (var assignment : entry.getValue()) {
                executor.submit(() -> runAvailabilityProbe(
                        probeProducer, assignment.topic(), assignment.partition()));
            }
        }

        // Dedicated probe consumer with manual partition assignment
        var consumerProps = kafkaConfig.classicConsumerProperties();
        consumerProps.remove(ConsumerConfig.GROUP_ID_CONFIG);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        probeConsumer = KafkaSpi.factory().<String, byte[]>newConsumer(consumerProps);
        var topicPartitions = assignments.stream()
                .map(a -> new TopicPartition(a.topic(), a.partition()))
                .toList();
        probeConsumer.assign(topicPartitions);
        executor.submit(() -> runAvailabilityConsumer(probeConsumer));

        log.info("Started availability probes: {} assignments, {} probe threads + 1 consumer",
                assignments.size(), assignments.size());
    }

    private void runAvailabilityProbe(RawProducer<String, byte[]> producer,
                                       String topic, int partition) {
        long sequence = 0;
        int timeoutSeconds = availabilityConfig.probeTimeoutSeconds;

        try {
            while (!testCompleted) {
                long startMs = System.currentTimeMillis();
                byte[] payload = AvailabilityPayload.encode(partition, sequence, startMs);
                var record = new ProducerRecord<>(topic, partition, "avail-" + partition, payload);

                // Register before sending so consumer can match immediately
                availabilityRecorder.probeStarted(partition, sequence, startMs);

                try {
                    // Pass null callback - we get acknowledgement via .get() on the Future.
                    producer.send(record, null).get(timeoutSeconds, java.util.concurrent.TimeUnit.SECONDS);
                    long durationMs = System.currentTimeMillis() - startMs;
                    availabilityRecorder.probeProduced(partition, sequence, startMs, durationMs);
                } catch (Exception e) {
                    long failureDurationMs = System.currentTimeMillis() - startMs;
                    availabilityRecorder.probeFailed(partition, sequence, startMs,
                            failureDurationMs, e.getClass().getSimpleName() + ": " + e.getMessage());
                }

                sequence++;

                // Always wait 1 second between probes
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        } catch (Throwable t) {
            log.error("Fatal error in availability probe for topic={} partition={}", topic, partition, t);
        }
    }

    private void runAvailabilityConsumer(RawConsumer<String, byte[]> consumer) {
        try {
            while (!testCompleted) {
                var records = consumer.poll(java.time.Duration.ofMillis(500));
                for (var record : records) {
                    var probe = AvailabilityPayload.decode(record.value());
                    if (probe != null) {
                        availabilityRecorder.probeReceived(
                                probe.partition(), probe.sequence(),
                                probe.produceTimestampMs(), System.currentTimeMillis());
                    }
                }
            }
        } catch (Exception e) {
            if (!testCompleted) {
                log.error("Availability consumer error", e);
            }
        } finally {
            consumer.close();
        }
    }

    @Override
    public void enablePerPartitionMetrics() {
        stats.setPerPartitionMetrics(true);
        log.info("Per-partition metrics enabled");
    }

    @Override
    public void enablePerConsumerMetrics() {
        stats.setPerConsumerMetrics(true);
        log.info("Per-consumer metrics enabled");
    }

    @Override
    public PerPartitionStats getPerPartitionStats() {
        return stats.getPerPartitionCumulativeStats();
    }

    @Override
    public AvailabilityResult.TestOutcome getAvailabilityResult() {
        if (availabilityRecorder == null) return null;

        var outcome = new AvailabilityResult.TestOutcome();
        outcome.availability = availabilityRecorder.computeResult(System.currentTimeMillis());
        return outcome;
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private static final Logger log = LoggerFactory.getLogger(LocalWorker.class);
}
