package dev.dimster.worker;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.dimster.ProducerRateSchedule;
import dev.dimster.availability.AvailabilityConfig;
import dev.dimster.availability.AvailabilityResult;
import dev.dimster.datagen.ResolvedDataset;
import dev.dimster.engines.correctness.CorrectnessConfig;
import dev.dimster.engines.correctness.CorrectnessResult;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.kafka.KafkaSpi;
import dev.dimster.worker.commands.PeriodStats;
import dev.dimster.worker.commands.ProducerWorkAssignment;
import dev.dimster.worker.topology.ConsumerSet;
import dev.dimster.worker.topology.VirtualProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerHttpServer {

    private static final Logger log = LoggerFactory.getLogger(WorkerHttpServer.class);

    private static final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private final Worker localWorker;
    private final ByteBuffer histogramSerializationBuffer = ByteBuffer.allocate(1024 * 1024);

    static class Arguments {
        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;

        @Parameter(names = {"-p", "--port"}, description = "HTTP port to listen on")
        public int httpPort = 8080;
    }

    public static void main(String[] args) {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("benchmark-worker");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            System.exit(-1);
        }

        try {
            // Resolve the Kafka SPI binding eagerly so the bound kafka-clients version
            // (and capability set) lands in the worker log before the HTTP server starts
            // accepting traffic. Any misbuild (zero or multiple SPI impls on the
            // classpath) fails here with a clear error rather than later inside a
            // wrapper construction.
            KafkaSpi.factory();

            log.info("Starting worker on port {}", arguments.httpPort);
            new WorkerHttpServer(arguments.httpPort);
        } catch (Exception e) {
            log.error("Worker terminating due to exception at startup", e);
            System.exit(-1);
        }
    }

    public WorkerHttpServer(int port) throws IOException {
        this.localWorker = new LocalWorker();

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        // server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.setExecutor(Executors.newCachedThreadPool());

        server.createContext("/set-kafka-config", this::handleSetKafkaConfig);
        server.createContext("/create-producers", this::handleCreateProducers);
        server.createContext("/probe-producers", this::handleProbeProducers);
        server.createContext("/create-consumers", this::handleCreateConsumers);
        server.createContext("/pause-producers", this::handlePauseProducers);
        server.createContext("/resume-producers", this::handleResumeProducers);

        server.createContext("/start-load", this::handleStartLoad);
        server.createContext("/adjust-publish-rate", this::handleAdjustPublishRate);
        server.createContext("/adjust-publish-rate-schedule", this::handleAdjustPublishRateSchedule);
        server.createContext("/stop-all", this::handleStopAll);
        server.createContext("/stop-status", this::handleStopStatus);
        server.createContext("/period-stats", this::handlePeriodStats);
        server.createContext("/period-stats-pp", this::handlePeriodStatsPp);
        server.createContext("/counters-stats", this::handleCountersStats);
        server.createContext("/reset-stats", this::handleResetStats);
        server.createContext("/health", this::handleHealth);
        server.createContext("/enable-correctness", this::handleEnableCorrectness);
        server.createContext("/correctness-result", this::handleCorrectnessResult);
        server.createContext("/reset-correctness", this::handleResetCorrectness);
        server.createContext("/enable-availability", this::handleEnableAvailability);
        server.createContext("/start-availability-probes", this::handleStartAvailabilityProbes);
        server.createContext("/availability-result", this::handleAvailabilityResult);
        server.createContext("/enable-per-partition-metrics", this::handleEnablePerPartitionMetrics);
        server.createContext("/enable-per-consumer-metrics", this::handleEnablePerConsumerMetrics);
        server.createContext("/per-partition-stats", this::handlePerPartitionStats);
        server.createContext("/set-max-consumer-recreations", this::handleSetMaxConsumerRecreations);
        server.createContext("/set-print-messages", this::handleSetPrintMessages);
        server.createContext("/set-print-contents", this::handleSetPrintContents);
        server.createContext("/set-dataset", this::handleSetDataset);
        server.createContext("/set-consumed-through-flink", this::handleSetConsumedThroughFlink);
        server.createContext("/set-consumer-processing-time", this::handleSetConsumerProcessingTime);
        server.createContext("/set-consumer-proc-capacity", this::handleSetConsumerProcCapacity);
        server.createContext("/probes-received", this::handleProbesReceived);

        // Incremental topology operations driven by /modify pipeline.
        server.createContext("/add-producers", this::handleAddProducers);
        server.createContext("/remove-producers", this::handleRemoveProducers);
        server.createContext("/restart-producers", this::handleRestartProducers);
        server.createContext("/add-consumers", this::handleAddConsumers);
        server.createContext("/remove-consumers", this::handleRemoveConsumers);
        server.createContext("/restart-consumers", this::handleRestartConsumers);

        server.start();
        log.info("Worker HTTP server started on port {}", port);
    }

    private void handleSetKafkaConfig(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            KafkaConfig config = yamlMapper.readValue(body, KafkaConfig.class);
            localWorker.setKafkaConfig(config);
        });
    }

    private void handleCreateProducers(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            List<VirtualProducer> producers = mapper.readValue(
                    exchange.getRequestBody(),
                    mapper.getTypeFactory().constructCollectionType(List.class, VirtualProducer.class));
            log.info("Received create producers request: {} producers", producers.size());
            localWorker.createProducers(producers);
        });
    }

    private void handleProbeProducers(HttpExchange exchange) throws IOException {
        handle(exchange, () -> localWorker.probeProducers());
    }

    private void handleCreateConsumers(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            ConsumerSet consumerSet = mapper.readValue(exchange.getRequestBody(), ConsumerSet.class);
            log.info("Received create consumers request: {} consumers", consumerSet.consumers().size());
            localWorker.createConsumers(consumerSet);
        });
    }

    private void handlePauseProducers(HttpExchange exchange) throws IOException {
        handle(exchange, () -> localWorker.pauseProducers());
    }

    private void handleResumeProducers(HttpExchange exchange) throws IOException {
        handle(exchange, () -> localWorker.resumeProducers());
    }


    private void handleStartLoad(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            ProducerWorkAssignment assignment = mapper.readValue(exchange.getRequestBody(), ProducerWorkAssignment.class);
            String rateText = assignment.publishRateSchedule() == null
                    ? assignment.publishRate() + " msg/s"
                    : assignment.publishRateSchedule().type + " schedule";
            log.info("Start load publish-rate: {}", rateText);
            localWorker.startLoad(assignment);
        });
    }

    private void handleAdjustPublishRate(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            Double publishRate = mapper.readValue(exchange.getRequestBody(), Double.class);
            localWorker.adjustPublishRate(publishRate);
        });
    }

    private void handleAdjustPublishRateSchedule(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            ProducerRateScheduleUpdate update = mapper.readValue(exchange.getRequestBody(), ProducerRateScheduleUpdate.class);
            localWorker.adjustPublishRateSchedule(update.schedule(), update.scheduleStartEpochMillis());
        });
    }

    private record ProducerRateScheduleUpdate(
            ProducerRateSchedule schedule,
            long scheduleStartEpochMillis) {}

    private void handleStopAll(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            log.info("Stop All");
            localWorker.stopAll();
        });
    }

    private void handleStopStatus(HttpExchange exchange) throws IOException {
        handleWithResponse(exchange, () -> {
            var status = java.util.Map.of("complete", localWorker.isStopComplete());
            return writer.writeValueAsBytes(status);
        });
    }

    private void handlePeriodStats(HttpExchange exchange) throws IOException {
        handleWithResponse(exchange, () -> {
            PeriodStats stats = localWorker.getPeriodStats();
            serializeHistograms(stats);
            return writer.writeValueAsBytes(stats);
        });
    }

    // Returns period stats with per-partition e2e interval histograms
    // serialized for the explore engine's worst-partition check. Wire
    // serialization is the expensive part (compressed histogram bytes per
    // partition); the in-memory map is always populated by getPeriodStats.
    private void handlePeriodStatsPp(HttpExchange exchange) throws IOException {
        handleWithResponse(exchange, () -> {
            PeriodStats stats = localWorker.getPeriodStats();
            serializeHistograms(stats);
            serializePerPartitionE2eHistograms(stats);
            return writer.writeValueAsBytes(stats);
        });
    }

    private void handleCountersStats(HttpExchange exchange) throws IOException {
        handleWithResponse(exchange, () -> writer.writeValueAsBytes(localWorker.getCountersStats()));
    }

    private void handleResetStats(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            log.info("Reset stats");
            localWorker.resetStats();
        });
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        handleWithResponse(exchange, () -> writer.writeValueAsBytes(localWorker.getHealth()));
    }

    private void handleEnableCorrectness(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            CorrectnessConfig config = mapper.readValue(exchange.getRequestBody(), CorrectnessConfig.class);
            log.info("Enable correctness mode: duplicates={}, outOfOrder={}, crc={}",
                    config.detectDuplicates, config.detectOutOfOrder, config.enableCrc);
            localWorker.enableCorrectness(config);
        });
    }

    private void handleCorrectnessResult(HttpExchange exchange) throws IOException {
        handleWithResponse(exchange, () -> {
            CorrectnessResult.TestOutcome outcome = localWorker.getCorrectnessResult();
            return writer.writeValueAsBytes(outcome);
        });
    }

    private void handleResetCorrectness(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            log.info("Reset correctness");
            localWorker.resetCorrectness();
        });
    }

    private void handleEnableAvailability(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            AvailabilityConfig config = mapper.readValue(exchange.getRequestBody(), AvailabilityConfig.class);
            log.info("Enable availability mode: threshold={}ms, probeTimeout={}s",
                    config.unavailabilityThresholdMs, config.probeTimeoutSeconds);
            localWorker.enableAvailability(config);
        });
    }

    private void handleStartAvailabilityProbes(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            List<dev.dimster.availability.ProbeAssignment> assignments = mapper.readValue(
                    exchange.getRequestBody(),
                    mapper.getTypeFactory().constructCollectionType(
                            List.class, dev.dimster.availability.ProbeAssignment.class));
            log.info("Start availability probes: {} assignments", assignments.size());
            localWorker.startAvailabilityProbes(assignments);
        });
    }

    private void handleAvailabilityResult(HttpExchange exchange) throws IOException {
        handleWithResponse(exchange, () -> {
            AvailabilityResult.TestOutcome outcome = localWorker.getAvailabilityResult();
            return writer.writeValueAsBytes(outcome);
        });
    }

    private void handleEnablePerPartitionMetrics(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            log.info("Enable per-partition metrics");
            localWorker.enablePerPartitionMetrics();
        });
    }

    private void handleEnablePerConsumerMetrics(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            log.info("Enable per-consumer metrics");
            localWorker.enablePerConsumerMetrics();
        });
    }

    private void handlePerPartitionStats(HttpExchange exchange) throws IOException {
        handleWithResponse(exchange, () -> writer.writeValueAsBytes(localWorker.getPerPartitionStats()));
    }

    private void handleSetMaxConsumerRecreations(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            Integer max = mapper.readValue(exchange.getRequestBody(), Integer.class);
            localWorker.setMaxConsumerRecreations(max);
        });
    }

    private void handleSetPrintMessages(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            Boolean enabled = mapper.readValue(exchange.getRequestBody(), Boolean.class);
            localWorker.setPrintMessages(enabled);
        });
    }

    private void handleSetPrintContents(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            Boolean enabled = mapper.readValue(exchange.getRequestBody(), Boolean.class);
            localWorker.setPrintContents(enabled);
        });
    }

    // Body is a ResolvedDataset as JSON: the workload's dataset or the payload dataset.
    private void handleSetDataset(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            ResolvedDataset dataset = mapper.readValue(exchange.getRequestBody(), ResolvedDataset.class);
            localWorker.setDataset(dataset);
        });
    }

    private void handleSetConsumedThroughFlink(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            Boolean throughFlink = mapper.readValue(exchange.getRequestBody(), Boolean.class);
            localWorker.setConsumedThroughFlink(throughFlink);
        });
    }

    // Payload is a 2-element int array: [minMs, maxMs]. See RemoteWorkerClient#setConsumerProcessingTime.
    private void handleSetConsumerProcessingTime(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            int[] range = mapper.readValue(exchange.getRequestBody(), int[].class);
            if (range.length != 2) {
                throw new IOException("set-consumer-processing-time expects [minMs, maxMs], got length " + range.length);
            }
            localWorker.setConsumerProcessingTime(range[0], range[1]);
        });
    }

    // Payload is an int: capacity. See RemoteWorkerClient#setConsumerProcCapacity.
    private void handleSetConsumerProcCapacity(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            int capacity = mapper.readValue(exchange.getRequestBody(), int.class);
            localWorker.setConsumerProcCapacity(capacity);
        });
    }

    private void handleProbesReceived(HttpExchange exchange) throws IOException {
        handleWithResponse(exchange, () -> writer.writeValueAsBytes(localWorker.getProbesReceived()));
    }

    // ─── Incremental topology endpoints (driven by /modify) ─────────────
    // Same wire conventions as /create-producers and /create-consumers:
    // List<VirtualProducer> for add/restart producers, List<String>
    // (producer/consumer names) for removes, ConsumerSet for add/restart
    // consumers. The remote client (RemoteWorkerClient) handles per-worker
    // splitting before invoking these — by the time the request lands here,
    // the payload is already filtered to this worker's subset.

    private void handleAddProducers(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            List<VirtualProducer> producers = mapper.readValue(
                    exchange.getRequestBody(),
                    mapper.getTypeFactory().constructCollectionType(List.class, VirtualProducer.class));
            log.info("Received add producers request: {} producers", producers.size());
            localWorker.addProducers(producers);
        });
    }

    private void handleRemoveProducers(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            List<String> names = mapper.readValue(
                    exchange.getRequestBody(),
                    mapper.getTypeFactory().constructCollectionType(List.class, String.class));
            log.info("Received remove producers request: {} producers", names.size());
            localWorker.removeProducers(names);
        });
    }

    private void handleRestartProducers(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            List<VirtualProducer> producers = mapper.readValue(
                    exchange.getRequestBody(),
                    mapper.getTypeFactory().constructCollectionType(List.class, VirtualProducer.class));
            log.info("Received restart producers request: {} producers", producers.size());
            localWorker.restartProducers(producers);
        });
    }

    private void handleAddConsumers(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            ConsumerSet consumerSet = mapper.readValue(exchange.getRequestBody(), ConsumerSet.class);
            log.info("Received add consumers request: {} consumers", consumerSet.consumers().size());
            localWorker.addConsumers(consumerSet);
        });
    }

    private void handleRemoveConsumers(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            List<String> names = mapper.readValue(
                    exchange.getRequestBody(),
                    mapper.getTypeFactory().constructCollectionType(List.class, String.class));
            log.info("Received remove consumers request: {} consumers", names.size());
            localWorker.removeConsumers(names);
        });
    }

    private void handleRestartConsumers(HttpExchange exchange) throws IOException {
        handle(exchange, () -> {
            ConsumerSet consumerSet = mapper.readValue(exchange.getRequestBody(), ConsumerSet.class);
            log.info("Received restart consumers request: {} consumers", consumerSet.consumers().size());
            localWorker.restartConsumers(consumerSet);
        });
    }

    private void serializeHistograms(PeriodStats stats) {
        synchronized (histogramSerializationBuffer) {
            histogramSerializationBuffer.clear();
            stats.publishLatency.encodeIntoCompressedByteBuffer(histogramSerializationBuffer);
            stats.publishLatencyBytes = new byte[histogramSerializationBuffer.position()];
            histogramSerializationBuffer.flip();
            histogramSerializationBuffer.get(stats.publishLatencyBytes);

            histogramSerializationBuffer.clear();
            stats.scheduleLatency.encodeIntoCompressedByteBuffer(histogramSerializationBuffer);
            stats.scheduleLatencyBytes = new byte[histogramSerializationBuffer.position()];
            histogramSerializationBuffer.flip();
            histogramSerializationBuffer.get(stats.scheduleLatencyBytes);

            histogramSerializationBuffer.clear();
            stats.publishDelayLatency.encodeIntoCompressedByteBuffer(histogramSerializationBuffer);
            stats.publishDelayLatencyBytes = new byte[histogramSerializationBuffer.position()];
            histogramSerializationBuffer.flip();
            histogramSerializationBuffer.get(stats.publishDelayLatencyBytes);

            histogramSerializationBuffer.clear();
            stats.endToEndLatency.encodeIntoCompressedByteBuffer(histogramSerializationBuffer);
            stats.endToEndLatencyBytes = new byte[histogramSerializationBuffer.position()];
            histogramSerializationBuffer.flip();
            histogramSerializationBuffer.get(stats.endToEndLatencyBytes);

            histogramSerializationBuffer.clear();
            stats.endToEndDeliveryLatency.encodeIntoCompressedByteBuffer(histogramSerializationBuffer);
            stats.endToEndDeliveryLatencyBytes = new byte[histogramSerializationBuffer.position()];
            histogramSerializationBuffer.flip();
            histogramSerializationBuffer.get(stats.endToEndDeliveryLatencyBytes);

            histogramSerializationBuffer.clear();
            stats.producerToFlinkLatency.encodeIntoCompressedByteBuffer(histogramSerializationBuffer);
            stats.producerToFlinkLatencyBytes = new byte[histogramSerializationBuffer.position()];
            histogramSerializationBuffer.flip();
            histogramSerializationBuffer.get(stats.producerToFlinkLatencyBytes);

            histogramSerializationBuffer.clear();
            stats.flinkToConsumerLatency.encodeIntoCompressedByteBuffer(histogramSerializationBuffer);
            stats.flinkToConsumerLatencyBytes = new byte[histogramSerializationBuffer.position()];
            histogramSerializationBuffer.flip();
            histogramSerializationBuffer.get(stats.flinkToConsumerLatencyBytes);
        }
    }

    private void serializePerPartitionE2eHistograms(PeriodStats stats) {
        synchronized (histogramSerializationBuffer) {
            stats.perPartitionE2eLatencyBytes = serializePerPartitionMap(stats.perPartitionE2eLatency);
            stats.perPartitionE2eDeliveryLatencyBytes = serializePerPartitionMap(stats.perPartitionE2eDeliveryLatency);
        }
    }

    // Must be called while holding histogramSerializationBuffer's monitor.
    private java.util.Map<String, byte[]> serializePerPartitionMap(java.util.Map<String, org.HdrHistogram.Histogram> map) {
        if (map == null) return null;
        java.util.Map<String, byte[]> result = new java.util.TreeMap<>();
        for (var entry : map.entrySet()) {
            histogramSerializationBuffer.clear();
            entry.getValue().encodeIntoCompressedByteBuffer(histogramSerializationBuffer);
            byte[] bytes = new byte[histogramSerializationBuffer.position()];
            histogramSerializationBuffer.flip();
            histogramSerializationBuffer.get(bytes);
            result.put(entry.getKey(), bytes);
        }
        return result;
    }

    private void handle(HttpExchange exchange, ThrowingRunnable action) throws IOException {
        long startNanos = System.nanoTime();
        try {
            action.run();
            exchange.sendResponseHeaders(200, -1);
        } catch (Exception e) {
            long elapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            if (logRequestError(exchange, e, elapsedMs)) {
                return;
            }
            sendError(exchange, e);
        } finally {
            exchange.close();
        }
    }

    private void handleWithResponse(HttpExchange exchange, ThrowingSupplier<byte[]> action) throws IOException {
        long startNanos = System.nanoTime();
        try {
            byte[] response = action.get();
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        } catch (Exception e) {
            long elapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            if (logRequestError(exchange, e, elapsedMs)) {
                return;
            }
            sendError(exchange, e);
        } finally {
            exchange.close();
        }
    }

    private boolean logRequestError(HttpExchange exchange, Exception e, long elapsedMs) {
        String path = exchange.getRequestURI().getPath();
        boolean clientDisconnect = isClientDisconnect(e);
        if (clientDisconnect) {
            log.debug("Request handler: {} - client disconnected after {}ms: {}", path, elapsedMs, e.getMessage());
        } else {
            log.error("Request handler: {} - Exception after {}ms: {}", path, elapsedMs, e.getMessage(), e);
        }
        return clientDisconnect;
    }

    static boolean isClientDisconnect(Throwable t) {
        while (t != null) {
            if (isClientDisconnectMessage(t)) {
                return true;
            }
            for (Throwable suppressed : t.getSuppressed()) {
                if (isClientDisconnect(suppressed)) {
                    return true;
                }
            }
            t = t.getCause();
        }
        return false;
    }

    private static boolean isClientDisconnectMessage(Throwable t) {
        if (!(t instanceof java.io.IOException) || t.getMessage() == null) {
            return false;
        }
        // JDK HttpServer surfaces ordinary peer disconnects with platform-specific
        // IOException messages. Treat only these narrow write/read failures as
        // benign so real worker exceptions still get logged and returned as 500s.
        String message = t.getMessage();
        return "Broken pipe".equals(message)
                || "Connection reset by peer".equals(message)
                || "Connection reset".equals(message);
    }

    private void sendError(HttpExchange exchange, Exception e) throws IOException {
        String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
        byte[] body = msg.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(500, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
