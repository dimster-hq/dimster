package dev.dimster.worker;

import static dev.dimster.stats.StatsCollector.E2E_LATENCY_MAX_MICROS;
import static dev.dimster.stats.StatsCollector.NON_E2E_LATENCY_MAX_MICROS;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.net.http.HttpTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.zip.DataFormatException;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import com.fasterxml.jackson.core.type.TypeReference;

import dev.dimster.ConsumerProcMode;
import dev.dimster.ConsumerType;
import dev.dimster.ProducerRateSchedule;
import dev.dimster.availability.AvailabilityConfig;
import dev.dimster.availability.AvailabilityRecorder;
import dev.dimster.availability.AvailabilityResult;
import dev.dimster.availability.ProbeAssignment;
import dev.dimster.datagen.ResolvedDataset;
import dev.dimster.engines.correctness.CorrectnessConfig;
import dev.dimster.engines.correctness.CorrectnessResult;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.worker.commands.ClientHealth;
import dev.dimster.worker.commands.CountersStats;
import dev.dimster.worker.commands.PerPartitionStats;
import dev.dimster.worker.commands.PeriodStats;
import dev.dimster.worker.commands.ProducerWorkAssignment;
import dev.dimster.worker.topology.ConsumerSet;
import dev.dimster.worker.topology.TopologyPlanner;
import dev.dimster.worker.topology.VirtualConsumer;
import dev.dimster.worker.topology.VirtualProducer;

public class RemoteWorkerClient implements Worker {

    private static final Logger log = LoggerFactory.getLogger(RemoteWorkerClient.class);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(300);

    private final TopologyPlanner planner;
    private final HttpClient httpClient;

    // Number of remote workers actually hosting at least one producer this test point.
    // Captured from the most recent createProducers() call and used by startLoad() and
    // adjustPublishRate() to divide the global publish rate evenly across producer workers.
    private volatile int activeProducerWorkerCount = 0;

    public RemoteWorkerClient(TopologyPlanner planner) {
        this.planner = planner;

        log.info("Mode: {} - producers: {} - consumers: {}", planner.mode(),
                planner.producerWorkers(), planner.consumerWorkers());

        // this.httpClient = HttpClient.newBuilder()
        //         .executor(Executors.newVirtualThreadPerTaskExecutor())
        //         .build();
        this.httpClient = HttpClient.newBuilder()
                .executor(Executors.newCachedThreadPool())
                .build();
    }

    @Override
    public void setKafkaConfig(File configurationFile) throws IOException {
        byte[] confFileContent = Files.readAllBytes(Paths.get(configurationFile.toString()));
        sendPost(planner.allWorkers(), "/set-kafka-config", confFileContent);
    }

    @Override
    public void setKafkaConfig(KafkaConfig config) throws IOException {
        byte[] yamlBytes = yamlWriter.writeValueAsBytes(config);
        sendPost(planner.allWorkers(), "/set-kafka-config", yamlBytes);
    }

    @Override
    public void createProducers(List<VirtualProducer> producers) {
        // Group by worker so each remote worker only receives the producers it should host.
        // Iteration order is preserved per worker (LinkedHashMap) so producer indices stay stable.
        Map<String, List<VirtualProducer>> byWorker = new LinkedHashMap<>();
        for (VirtualProducer vp : producers) {
            byWorker.computeIfAbsent(vp.worker(), k -> new ArrayList<>()).add(vp);
        }
        this.activeProducerWorkerCount = byWorker.size();
        log.info("Distributing {} producers across {} workers",
                producers.size(), activeProducerWorkerCount);

        List<CompletableFuture<Void>> futures = byWorker.entrySet().stream().map(entry -> {
            try {
                return sendPostAsync(entry.getKey(), "/create-producers",
                        writer.writeValueAsBytes(entry.getValue()));
            } catch (Exception e) {
                return CompletableFuture.<Void>failedFuture(e);
            }
        }).toList();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    @Override
    public void createConsumers(ConsumerSet consumerSet) {
        // Group consumers by worker, then re-wrap each per-worker subset in a ConsumerSet
        // so the remote workers receive the same shape the LocalWorker API takes.
        Map<String, List<VirtualConsumer>> byWorker = new LinkedHashMap<>();
        for (VirtualConsumer vc : consumerSet.consumers()) {
            byWorker.computeIfAbsent(vc.worker(), k -> new ArrayList<>()).add(vc);
        }

        ConsumerType type = consumerSet.consumerType();
        int procMin = consumerSet.consumerProcMsMin();
        int procMax = consumerSet.consumerProcMsMax();
        int procParallelism = consumerSet.consumerProcParallelism();
        ConsumerProcMode procMode = consumerSet.consumerProcMode();
        int procCapacity = consumerSet.consumerProcCapacity();
        log.info("Distributing {} consumers across {} workers",
                consumerSet.consumers().size(), byWorker.size());

        List<CompletableFuture<Void>> futures = byWorker.entrySet().stream().map(entry -> {
            try {
                ConsumerSet workerSet = new ConsumerSet(entry.getValue(), type, procMin, procMax, procParallelism, procMode, procCapacity);
                return sendPostAsync(entry.getKey(), "/create-consumers",
                        writer.writeValueAsBytes(workerSet));
            } catch (Exception e) {
                return CompletableFuture.<Void>failedFuture(e);
            }
        }).toList();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) throws IOException {
        // Divide by the number of producer workers from the most recent createProducers().
        // Falling back to 1 keeps the math sane if startLoad is somehow called without a prior
        // producer creation (shouldn't happen in practice).
        int divisor = Math.max(1, activeProducerWorkerCount);
        ProducerWorkAssignment distributed = producerWorkAssignment.dividedBy(divisor);
        sendPost(planner.producerWorkers(), "/start-load", writer.writeValueAsBytes(distributed));
    }

    @Override
    public void probeProducers() throws IOException {
        sendPost(planner.producerWorkers(), "/probe-producers", new byte[0]);
    }

    @Override
    public void adjustPublishRate(double publishRate) throws IOException {
        int divisor = Math.max(1, activeProducerWorkerCount);
        double distributed = publishRate / divisor;
        sendPost(planner.producerWorkers(), "/adjust-publish-rate", writer.writeValueAsBytes(distributed));
    }

    @Override
    public void adjustPublishRateSchedule(ProducerRateSchedule schedule) throws IOException {
        adjustPublishRateSchedule(schedule, System.currentTimeMillis());
    }

    @Override
    public void adjustPublishRateSchedule(ProducerRateSchedule schedule, long scheduleStartEpochMillis) throws IOException {
        int divisor = Math.max(1, activeProducerWorkerCount);
        ProducerRateSchedule distributed = schedule.copyDividedBy(divisor);
        sendPost(planner.producerWorkers(), "/adjust-publish-rate-schedule",
                writer.writeValueAsBytes(new ProducerRateScheduleUpdate(distributed, scheduleStartEpochMillis)));
    }

    private record ProducerRateScheduleUpdate(
            ProducerRateSchedule schedule,
            long scheduleStartEpochMillis) {}

    @Override
    public void stopAll() {
        sendPost(planner.allWorkers(), "/stop-all", new byte[0]);
        activeProducerWorkerCount = 0;
    }

    @Override
    public boolean isStopComplete() {
        List<Map<String, Object>> results = get(planner.allWorkers(), "/stop-status",
                new TypeReference<>() {});
        return results.stream().allMatch(m -> Boolean.TRUE.equals(m.get("complete")));
    }

    @Override
    public void awaitStopComplete(Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (!isStopComplete()) {
            if (System.currentTimeMillis() > deadline) {
                throw new RuntimeException("Timed out waiting for workers to complete stop after " + timeout);
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void pauseProducers() throws IOException {
        sendPost(planner.producerWorkers(), "/pause-producers", new byte[0]);
    }

    @Override
    public void resumeProducers() throws IOException {
        sendPost(planner.producerWorkers(), "/resume-producers", new byte[0]);
    }

    @Override
    public PeriodStats getPeriodStats() {
        return getPeriodStats(false);
    }

    @Override
    public PeriodStats getPeriodStats(boolean includePerPartitionIntervals) {
        String endpoint = includePerPartitionIntervals ? "/period-stats-pp" : "/period-stats";
        List<PeriodStats> individualStats = get(planner.allWorkers(), endpoint, PeriodStats.class);
        PeriodStats stats = new PeriodStats();
        for (PeriodStats is : individualStats) {
            is.decodeHistograms();
            stats.mergeFrom(is);
        }
        return stats;
    }

    @Override
    public CountersStats getCountersStats() throws IOException {
        List<CountersStats> individualStats = get(planner.allWorkers(), "/counters-stats", CountersStats.class);
        long totalSent = 0, totalReceived = 0;
        for (CountersStats is : individualStats) {
            totalSent += is.messagesSent();
            totalReceived += is.messagesReceived();
        }
        return new CountersStats(totalSent, totalReceived);
    }

    @Override
    public long getProbesReceived() throws IOException {
        List<Long> counts = get(planner.allWorkers(), "/probes-received", Long.class);
        long total = 0;
        for (long c : counts) total += c;
        return total;
    }

    @Override
    public void resetStats() throws IOException {
        sendPost(planner.allWorkers(), "/reset-stats", new byte[0]);
    }

    @Override
    public void setMaxConsumerRecreations(int max) {
        try {
            sendPost(planner.consumerWorkers(), "/set-max-consumer-recreations", writer.writeValueAsBytes(max));
        } catch (IOException e) {
            throw new RuntimeException("Failed to set max consumer recreations", e);
        }
    }

    @Override
    public void setPrintMessages(boolean enabled) {
        try {
            sendPost(planner.allWorkers(), "/set-print-messages", writer.writeValueAsBytes(enabled));
        } catch (IOException e) {
            throw new RuntimeException("Failed to set print messages", e);
        }
    }

    @Override
    public void setPrintContents(boolean enabled) {
        try {
            sendPost(planner.allWorkers(), "/set-print-contents", writer.writeValueAsBytes(enabled));
        } catch (IOException e) {
            throw new RuntimeException("Failed to set print contents", e);
        }
    }

    // Producers generate the rows and consumers read the record format, so
    // every worker needs the dataset. A null dataset is sent as JSON null.
    @Override
    public void setDataset(ResolvedDataset dataset) throws IOException {
        sendPost(planner.allWorkers(), "/set-dataset", writer.writeValueAsBytes(dataset));
    }

    // Forward to consumer workers only — producers don't simulate processing time.
    // Wire payload is a 2-element int array [minMs, maxMs]; see WorkerHttpServer#handleSetConsumerProcessingTime.
    @Override
    public void setConsumerProcessingTime(int minMs, int maxMs) {
        try {
            sendPost(planner.consumerWorkers(), "/set-consumer-processing-time",
                    writer.writeValueAsBytes(new int[] { minMs, maxMs }));
        } catch (IOException e) {
            throw new RuntimeException("Failed to set consumer processing time", e);
        }
    }

    @Override
    public void setConsumerProcCapacity(int capacity) {
        try {
            sendPost(planner.consumerWorkers(), "/set-consumer-proc-capacity",
                    writer.writeValueAsBytes(capacity));
        } catch (IOException e) {
            throw new RuntimeException("Failed to set consumer proc capacity", e);
        }
    }

    // ─── Incremental topology operations (driven by /modify pipeline) ────
    //
    // Producers and consumers are split per worker the same way as
    // createProducers / createConsumers — a producer's worker assignment
    // (vp.worker()) decides which remote endpoint receives the request.
    // For removes, the caller supplies a flat name list which we likewise
    // split per worker by looking up each name in the topology.

    @Override
    public void addProducers(List<VirtualProducer> producers) throws IOException {
        Map<String, List<VirtualProducer>> byWorker = new LinkedHashMap<>();
        for (VirtualProducer vp : producers) {
            byWorker.computeIfAbsent(vp.worker(), k -> new ArrayList<>()).add(vp);
        }
        // We deliberately do NOT recompute activeProducerWorkerCount here.
        // Tracking precise per-worker producer counts from the client side
        // would require querying each worker — we keep the value set at
        // last startLoad / createProducers. In the typical add path the
        // new producers land on workers that already host some, so the
        // count is unchanged. If the modify shape ever produces a worker
        // that didn't previously host any producers, adjustPublishRate
        // will distribute slightly below the true rate — acceptable
        // tradeoff for keeping the client state simple.
        List<CompletableFuture<Void>> futures = byWorker.entrySet().stream()
                .map(e -> sendPostAsync(e.getKey(), "/add-producers", writeBytes(e.getValue())))
                .toList();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    /**
     * Per-worker name routing for remove ops. Caller supplies a flat name
     * list; we split it by walking the topology to find each name's
     * current worker. Names with no current owner are ignored — they were
     * either already removed or never existed.
     */
    @Override
    public void removeProducers(List<String> producerNames) throws IOException {
        if (producerNames.isEmpty()) return;
        // Names don't carry their host worker, and tracking per-worker
        // ownership client-side would duplicate state held in the
        // topology. Broadcast to all producer workers; LocalWorker
        // ignores names it doesn't host.
        sendPost(planner.producerWorkers(), "/remove-producers", writeBytes(producerNames));
    }

    @Override
    public void restartProducers(List<VirtualProducer> producers) throws IOException {
        Map<String, List<VirtualProducer>> byWorker = new LinkedHashMap<>();
        for (VirtualProducer vp : producers) {
            byWorker.computeIfAbsent(vp.worker(), k -> new ArrayList<>()).add(vp);
        }
        List<CompletableFuture<Void>> futures = byWorker.entrySet().stream()
                .map(e -> sendPostAsync(e.getKey(), "/restart-producers", writeBytes(e.getValue())))
                .toList();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    @Override
    public void addConsumers(ConsumerSet consumers) throws IOException {
        Map<String, List<VirtualConsumer>> byWorker = new LinkedHashMap<>();
        for (VirtualConsumer vc : consumers.consumers()) {
            byWorker.computeIfAbsent(vc.worker(), k -> new ArrayList<>()).add(vc);
        }
        List<CompletableFuture<Void>> futures = byWorker.entrySet().stream()
                .map(e -> {
                    ConsumerSet workerSet = new ConsumerSet(e.getValue(), consumers.consumerType(),
                            consumers.consumerProcMsMin(), consumers.consumerProcMsMax(), consumers.consumerProcParallelism(),
                            consumers.consumerProcMode(), consumers.consumerProcCapacity());
                    return sendPostAsync(e.getKey(), "/add-consumers", writeBytes(workerSet));
                })
                .toList();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    @Override
    public void removeConsumers(List<String> consumerNames) throws IOException {
        // Consumer names don't encode their host worker; without an
        // authoritative topology lookup we'd have to broadcast. Broadcasting
        // is correct (LocalWorker silently ignores names it doesn't host)
        // and the workers per close are bounded — keep it simple.
        sendPost(planner.consumerWorkers(), "/remove-consumers", writeBytes(consumerNames));
    }

    @Override
    public void restartConsumers(ConsumerSet consumers) throws IOException {
        Map<String, List<VirtualConsumer>> byWorker = new LinkedHashMap<>();
        for (VirtualConsumer vc : consumers.consumers()) {
            byWorker.computeIfAbsent(vc.worker(), k -> new ArrayList<>()).add(vc);
        }
        List<CompletableFuture<Void>> futures = byWorker.entrySet().stream()
                .map(e -> {
                    ConsumerSet workerSet = new ConsumerSet(e.getValue(), consumers.consumerType(),
                            consumers.consumerProcMsMin(), consumers.consumerProcMsMax(), consumers.consumerProcParallelism(),
                            consumers.consumerProcMode(), consumers.consumerProcCapacity());
                    return sendPostAsync(e.getKey(), "/restart-consumers", writeBytes(workerSet));
                })
                .toList();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private byte[] writeBytes(Object payload) {
        try {
            return writer.writeValueAsBytes(payload);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    @Override
    public ClientHealth getHealth() {
        List<String> allWorkers = planner.allWorkers();
        List<CompletableFuture<ClientHealth>> futures = allWorkers.stream()
                .map(h -> getAsync(h, "/health", ClientHealth.class))
                .toList();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        return ClientHealth.aggregate(futures.stream()
                .map(CompletableFuture::join)
                .toList());
    }

    @Override
    public void enableCorrectness(CorrectnessConfig config) {
        // Topology-side correctness behavior (one-partition-per-producer pinning, topic
        // affinity, static consumer assignment) is requested via TopologySpec on the next
        // planner.plan() call — no planner state to flip here. This method only forwards
        // the worker-side payload-encoding/tracker config to the remote workers.
        try {
            sendPost(planner.allWorkers(), "/enable-correctness", writer.writeValueAsBytes(config));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CorrectnessResult.TestOutcome getCorrectnessResult() {
        List<CorrectnessResult.TestOutcome> outcomes =
                get(planner.allWorkers(), "/correctness-result", CorrectnessResult.TestOutcome.class);

        // Aggregate across workers — each worker computes passed using CorrectnessConfig
        CorrectnessResult.TestOutcome combined = new CorrectnessResult.TestOutcome();
        combined.correctness.passed = true;
        for (CorrectnessResult.TestOutcome o : outcomes) {
            if (o == null) continue;
            combined.correctness.totalSent += o.correctness.totalSent;
            combined.correctness.totalReceived += o.correctness.totalReceived;
            combined.correctness.lostMessages += o.correctness.lostMessages;
            combined.correctness.duplicateCount += o.correctness.duplicateCount;
            combined.correctness.outOfOrderCount += o.correctness.outOfOrderCount;
            combined.correctness.crcFailureCount += o.correctness.crcFailureCount;
            combined.correctness.lostMessageRanges.addAll(o.correctness.lostMessageRanges);
            combined.correctness.violations.addAll(o.correctness.violations);
            if (!o.correctness.passed) combined.correctness.passed = false;
        }

        // Sort and re-collapse violations from all workers so adjacent ranges merge
        collapseViolations(combined.correctness.violations);

        return combined;
    }

    /**
     * Sort violations by (producerId, topic, partition, type, fromSequence) and collapse
     * adjacent entries into ranges using tryExtend. Operates in-place.
     */
    private static void collapseViolations(List<CorrectnessResult.ViolationEntry> violations) {
        if (violations.size() <= 1) return;

        violations.sort((a, b) -> {
            int cmp = Integer.compare(a.producerId, b.producerId);
            if (cmp != 0) return cmp;
            cmp = a.topic.compareTo(b.topic);
            if (cmp != 0) return cmp;
            cmp = Integer.compare(a.partition, b.partition);
            if (cmp != 0) return cmp;
            cmp = a.type.compareTo(b.type);
            if (cmp != 0) return cmp;
            return Long.compare(a.fromSequence, b.fromSequence);
        });

        List<CorrectnessResult.ViolationEntry> collapsed = new ArrayList<>();
        collapsed.add(violations.get(0));
        for (int i = 1; i < violations.size(); i++) {
            var v = violations.get(i);
            var last = collapsed.getLast();
            if (last.type.equals(v.type) && last.producerId == v.producerId
                    && last.topic.equals(v.topic) && last.partition == v.partition
                    // Do not erase temporal information while coalescing
                    // worker results for correctness event timelines.
                    && last.timestampMs == v.timestampMs
                    && last.tryExtend(v.fromSequence, v.highSequence)) {
                continue;
            }
            collapsed.add(v);
        }

        violations.clear();
        violations.addAll(collapsed);
    }

    @Override
    public void resetCorrectness() {
        sendPost(planner.allWorkers(), "/reset-correctness", new byte[0]);
    }

    @Override
    public void enableAvailability(AvailabilityConfig config) {
        try {
            sendPost(planner.allWorkers(), "/enable-availability", writer.writeValueAsBytes(config));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void startAvailabilityProbes(List<ProbeAssignment> assignments) {
        try {
            List<String> workers = planner.allWorkers();
            Map<String, List<ProbeAssignment>> perWorker = new LinkedHashMap<>();
            for (String w : workers) perWorker.put(w, new ArrayList<>());
            for (int i = 0; i < assignments.size(); i++) {
                perWorker.get(workers.get(i % workers.size())).add(assignments.get(i));
            }
            for (var entry : perWorker.entrySet()) {
                sendPost(List.of(entry.getKey()), "/start-availability-probes",
                        writer.writeValueAsBytes(entry.getValue()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AvailabilityResult.TestOutcome getAvailabilityResult() {
        List<AvailabilityResult.TestOutcome> outcomes =
                get(planner.allWorkers(), "/availability-result", AvailabilityResult.TestOutcome.class);

        // Aggregate across workers
        AvailabilityResult.TestOutcome combined = new AvailabilityResult.TestOutcome();
        for (AvailabilityResult.TestOutcome o : outcomes) {
            if (o == null) continue;
            combined.availability.partitionResults.addAll(o.availability.partitionResults);
        }

        // Recompute overall availability from combined partition results (time-based)
        long totalElapsedMs = 0;
        long maxElapsedMs = 0;
        long totalProducerUnavailMs = 0;
        long totalE2eUnavailMs = 0;
        for (var pr : combined.availability.partitionResults) {
            totalElapsedMs += pr.elapsedMs;
            maxElapsedMs = Math.max(maxElapsedMs, pr.elapsedMs);
            totalProducerUnavailMs += pr.producerUnavailableMs;
            totalE2eUnavailMs += pr.e2eUnavailableMs;
        }
        combined.availability.partitions = combined.availability.partitionResults.size();
        combined.availability.wallClockMs = maxElapsedMs;
        combined.availability.totalElapsedMs = totalElapsedMs;
        combined.availability.producerUnavailableMs = totalProducerUnavailMs;
        combined.availability.e2eUnavailableMs = totalE2eUnavailMs;
        if (totalElapsedMs > 0) {
            combined.availability.producerAvailabilityPct =
                    100.0 * Math.max(0, totalElapsedMs - totalProducerUnavailMs) / totalElapsedMs;
            combined.availability.e2eAvailabilityPct =
                    100.0 * Math.max(0, totalElapsedMs - totalE2eUnavailMs) / totalElapsedMs;
        }
        long totalUnavailMs = 0;
        for (var pr : combined.availability.partitionResults) {
            totalUnavailMs += AvailabilityRecorder.mergedUnavailableDuration(pr.unavailabilityEvents);
        }
        combined.availability.totalUnavailableMs = totalUnavailMs;
        if (totalElapsedMs > 0) {
            combined.availability.overallAvailabilityPct =
                    100.0 * Math.max(0, totalElapsedMs - totalUnavailMs) / totalElapsedMs;
        }

        return combined;
    }

    @Override
    public void enablePerPartitionMetrics() {
        sendPost(planner.allWorkers(), "/enable-per-partition-metrics", new byte[0]);
    }

    @Override
    public void enablePerConsumerMetrics() {
        sendPost(planner.allWorkers(), "/enable-per-consumer-metrics", new byte[0]);
    }

    @Override
    public PerPartitionStats getPerPartitionStats() {
        List<PerPartitionStats> individualStats = get(planner.allWorkers(), "/per-partition-stats", PerPartitionStats.class);

        PerPartitionStats merged = new PerPartitionStats();
        for (PerPartitionStats ws : individualStats) {
            if (ws == null || ws.partitions == null) continue;
            for (var entry : ws.partitions.entrySet()) {
                String key = entry.getKey();
                PerPartitionStats.PartitionMetrics wm = entry.getValue();
                PerPartitionStats.PartitionMetrics mm = merged.partitions.computeIfAbsent(
                        key, k -> {
                            var m = new PerPartitionStats.PartitionMetrics();
                            m.key = k;
                            return m;
                        });
                mm.messagesSent += wm.messagesSent;
                mm.messagesReceived += wm.messagesReceived;

                if (wm.publishLatencyBytes != null) {
                    try {
                        Histogram h = Histogram.decodeFromCompressedByteBuffer(
                                ByteBuffer.wrap(wm.publishLatencyBytes), NON_E2E_LATENCY_MAX_MICROS);
                        if (mm.publishLatencyHistogram == null) {
                            mm.publishLatencyHistogram = h;
                        } else {
                            mm.publishLatencyHistogram.add(h);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                if (wm.endToEndLatencyBytes != null) {
                    try {
                        Histogram h = Histogram.decodeFromCompressedByteBuffer(
                                ByteBuffer.wrap(wm.endToEndLatencyBytes), E2E_LATENCY_MAX_MICROS);
                        if (mm.endToEndLatencyHistogram == null) {
                            mm.endToEndLatencyHistogram = h;
                        } else {
                            mm.endToEndLatencyHistogram.add(h);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        // Serialize merged histograms back to bytes, freeing each after serialization
        // Use a shared buffer to avoid per-histogram allocation of getEstimatedFootprintInBytes()
        // (which returns the uncompressed size — far larger than the compressed output)
        ByteBuffer serializeBuf = ByteBuffer.allocate(1024 * 1024); // 1MB, resized if needed
        for (var m : merged.partitions.values()) {
            if (m.publishLatencyHistogram != null) {
                serializeBuf = serializeHistogram(m.publishLatencyHistogram, serializeBuf);
                m.publishLatencyBytes = extractBytes(serializeBuf);
                m.publishLatencyHistogram = null;
            }
            if (m.endToEndLatencyHistogram != null) {
                serializeBuf = serializeHistogram(m.endToEndLatencyHistogram, serializeBuf);
                m.endToEndLatencyBytes = extractBytes(serializeBuf);
                m.endToEndLatencyHistogram = null;
            }
        }

        return merged;
    }

    private static ByteBuffer serializeHistogram(Histogram histogram, ByteBuffer buffer) {
        while (true) {
            try {
                buffer.clear();
                histogram.encodeIntoCompressedByteBuffer(buffer);
                return buffer;
            } catch (ArrayIndexOutOfBoundsException e) {
                // Buffer too small — double it and retry
                buffer = ByteBuffer.allocate(buffer.capacity() * 2);
            }
        }
    }

    private static byte[] extractBytes(ByteBuffer buffer) {
        byte[] result = new byte[buffer.position()];
        buffer.flip();
        buffer.get(result);
        return result;
    }

    @Override
    public void close() throws Exception {
        // HttpClient doesn't need explicit close in Java 21
    }

    // --- HTTP helpers ---

    private void sendPost(List<String> hosts, String path, byte[] body) {
        List<CompletableFuture<Void>> futures = hosts.stream()
                .map(h -> sendPostAsync(h, path, body))
                .toList();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private static final int MAX_RETRIES = 5;
    private static final long INITIAL_BACKOFF_MS = 1000;

    private CompletableFuture<Void> sendPostAsync(String host, String path, byte[] body) {
        return sendPostWithRetry(host, path, body, 0);
    }

    private CompletableFuture<Void> sendPostWithRetry(String host, String path, byte[] body, int attempt) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(host + path))
                .timeout(REQUEST_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    if (response.statusCode() == 200) {
                        return CompletableFuture.completedFuture(null);
                    }

                    if (response.statusCode() >= 500 && attempt < MAX_RETRIES) {
                        long backoffMs = INITIAL_BACKOFF_MS * (1L << attempt); // exponential: 1s, 2s, 4s, 8s, 16s
                        log.warn("POST {}{} failed (attempt {}/{}, status {}), retrying in {}ms",
                                host, path, attempt + 1, MAX_RETRIES + 1,
                                response.statusCode(), backoffMs);
                        return CompletableFuture.runAsync(() -> {
                            try { Thread.sleep(backoffMs); } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }).thenCompose(ignored -> sendPostWithRetry(host, path, body, attempt + 1));
                    }

                    log.error("POST {}{} failed -- code: {} body: {}", host, path,
                            response.statusCode(), response.body());
                    return CompletableFuture.<Void>failedFuture(
                            new RuntimeException("HTTP POST failed: " + host + path + " status=" + response.statusCode()));
                })
                .exceptionally(ex -> {
                    Throwable cause = (ex instanceof CompletionException) ? ex.getCause() : ex;
                    if (cause instanceof HttpTimeoutException) {
                        log.error("POST {}{} timed out after {}s", host, path, REQUEST_TIMEOUT.toSeconds());
                    }
                    throw (ex instanceof CompletionException ce) ? ce : new CompletionException(ex);
                });
    }

    private <T> List<T> get(List<String> hosts, String path, Class<T> clazz) {
        List<CompletableFuture<T>> futures = hosts.stream()
                .map(h -> getAsync(h, path, clazz))
                .toList();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        return futures.stream().map(CompletableFuture::join).toList();
    }

    private <T> List<T> get(List<String> hosts, String path, TypeReference<T> typeRef) {
        List<CompletableFuture<T>> futures = hosts.stream()
                .map(h -> getAsync(h, path, typeRef))
                .toList();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        return futures.stream().map(CompletableFuture::join).toList();
    }

    private <T> CompletableFuture<T> getAsync(String host, String path, TypeReference<T> typeRef) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(host + path))
                .timeout(REQUEST_TIMEOUT)
                .GET()
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() != 200) {
                        log.error("GET {}{} failed -- code: {}", host, path, response.statusCode());
                        throw new RuntimeException("HTTP GET failed: " + host + path + " status=" + response.statusCode());
                    }
                    try {
                        return mapper.readValue(response.body(), typeRef);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .exceptionally(ex -> {
                    Throwable cause = (ex instanceof CompletionException) ? ex.getCause() : ex;
                    if (cause instanceof HttpTimeoutException) {
                        log.error("GET {}{} timed out after {}s", host, path, REQUEST_TIMEOUT.toSeconds());
                    } else {
                        log.error("GET {}{} failed with error {}", host, path, ex);
                    }
                    throw (ex instanceof CompletionException ce) ? ce : new CompletionException(ex);
                });
    }

    private <T> CompletableFuture<T> getAsync(String host, String path, Class<T> clazz) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(host + path))
                .timeout(REQUEST_TIMEOUT)
                .GET()
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() != 200) {
                        log.error("GET {}{} failed -- code: {}", host, path, response.statusCode());
                        throw new RuntimeException("HTTP GET failed: " + host + path + " status=" + response.statusCode());
                    }
                    try {
                        return mapper.readValue(response.body(), clazz);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .exceptionally(ex -> {
                    Throwable cause = (ex instanceof CompletionException) ? ex.getCause() : ex;
                    if (cause instanceof HttpTimeoutException) {
                        log.error("GET {}{} timed out after {}s", host, path, REQUEST_TIMEOUT.toSeconds());
                    } else {
                        log.error("GET {}{} failed with error {}", host, path, ex);
                    }
                    throw (ex instanceof CompletionException ce) ? ce : new CompletionException(ex);
                });
    }

    private <T> T post(String host, String path, byte[] body, Class<T> clazz) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(host + path))
                .timeout(REQUEST_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                log.error("POST {}{} failed -- code: {}", host, path, response.statusCode());
                throw new RuntimeException("HTTP POST failed: " + host + path + " status=" + response.statusCode());
            }
            return mapper.readValue(response.body(), clazz);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
    private static final ObjectWriter yamlWriter = new ObjectMapper(new YAMLFactory()).writer();

    private static final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }
}
