package dev.dimster;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import dev.dimster.engines.correctness.CorrectnessEngine;
import dev.dimster.engines.correctness.CorrectnessResult;
import dev.dimster.engines.correctness.CorrectnessWorkload;
import dev.dimster.engines.drainbacklog.DrainBacklogEngine;
import dev.dimster.engines.drainbacklog.DrainBacklogResult;
import dev.dimster.engines.drainbacklog.DrainBacklogWorkload;
import dev.dimster.engines.explore.ExploreEngine;
import dev.dimster.engines.explore.ExploreResult;
import dev.dimster.engines.explore.ExploreWorkload;
import dev.dimster.engines.run.RunResult;
import dev.dimster.engines.run.Workload;
import dev.dimster.engines.run.WorkloadEngine;
import dev.dimster.coordinator.ModificationTarget;
import dev.dimster.hooks.HookContext;
import dev.dimster.hooks.HookExecutor;
import dev.dimster.hooks.HookPoint;
import dev.dimster.hooks.Hooks;
import dev.dimster.hooks.ScriptHookExecutor;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.utils.StopRequestedException;
import dev.dimster.worker.Worker;
import dev.dimster.worker.topology.JsonFileTopologyRecorder;
import dev.dimster.worker.topology.TopologyPlanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkRunner {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkRunner.class);
    private static final ObjectMapper jsonMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final ObjectWriter jsonWriter = jsonMapper.writerWithDefaultPrettyPrinter();
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

    private final String mode;
    private final Worker worker;
    // The TopologyPlanner is the single source of truth for "where things run":
    // it maps workers -> producers/consumers and pre-computes per-instance partition pinning.
    // Engines call planner.plan(spec) per test point to produce a VirtualTopology that the
    // worker (Local or Remote) executes literally.
    private final TopologyPlanner topologyPlanner;
    private final KafkaConfig fileKafkaConfig;
    private final Map<String, String> workloadYamls;
    private final File resultsDir;
    private final File outputBase;
    private final AtomicBoolean stopRequested;
    private final String system;
    private final String environment;
    private final String hardware;
    private final Map<String, Object> userMetadata;
    private final Map<String, Object> extraDimensions;
    private final Checkpoint checkpoint;

    // Optional callback to publish/clear the currently-running modification
    // target so the coordinator's /modify endpoint can find it. Run and
    // correctness modes publish their engines; explore and drain-backlog stay
    // unpublished. Null when constructed without a coordinator.
    private final Consumer<ModificationTarget> activeEngineSink;

    // Runs the scripts named in workloads' engineConfig.hooks. Defaults to one
    // with no scripts, so a workload that configures hooks without the run
    // request shipping them fails validation up front.
    private HookExecutor hookExecutor = new ScriptHookExecutor(Map.of());

    /**
     * A callback that engines call after each test point to persist progress.
     */
    @FunctionalInterface
    public interface CheckpointCallback {
        void onTestPointCompleted(int completedTestPointIndex, Object partialResult);
    }

    public BenchmarkRunner(String mode, Worker worker, TopologyPlanner topologyPlanner,
                           File kafkaConfigFile,
                           Map<String, String> workloadYamls, File resultsDir,
                           AtomicBoolean stopRequested, String system, String environment,
                           String hardware) throws Exception {
        this(mode, worker, topologyPlanner, kafkaConfigFile, workloadYamls, resultsDir, stopRequested,
                system, environment, hardware, null, null, null, null);
    }

    public BenchmarkRunner(String mode, Worker worker, TopologyPlanner topologyPlanner,
                           File kafkaConfigFile,
                           Map<String, String> workloadYamls, File resultsDir,
                           AtomicBoolean stopRequested, String system, String environment,
                           String hardware, Checkpoint checkpoint) throws Exception {
        this(mode, worker, topologyPlanner, kafkaConfigFile, workloadYamls, resultsDir, stopRequested,
                system, environment, hardware, null, null, checkpoint, null);
    }

    public BenchmarkRunner(String mode, Worker worker, TopologyPlanner topologyPlanner,
                           File kafkaConfigFile,
                           Map<String, String> workloadYamls, File resultsDir,
                           AtomicBoolean stopRequested, String system, String environment,
                           String hardware, Map<String, Object> userMetadata,
                           Map<String, Object> extraDimensions, Checkpoint checkpoint,
                           Consumer<ModificationTarget> activeEngineSink) throws Exception {
        this.mode = mode;
        this.worker = worker;
        this.topologyPlanner = topologyPlanner;
        this.fileKafkaConfig = kafkaConfigFile != null
                ? yamlMapper.readValue(kafkaConfigFile, KafkaConfig.class)
                : null;
        this.workloadYamls = workloadYamls;
        this.resultsDir = resultsDir;
        this.outputBase = resultsDir.getParentFile();
        this.stopRequested = stopRequested;
        this.system = system;
        this.environment = environment;
        this.hardware = hardware;
        this.userMetadata = userMetadata;
        this.extraDimensions = extraDimensions;
        this.checkpoint = checkpoint;
        this.activeEngineSink = activeEngineSink;
    }

    /** Set the executor for workload hooks; see {@link ScriptHookExecutor#install}. */
    public void setHookExecutor(HookExecutor hookExecutor) {
        this.hookExecutor = hookExecutor;
    }

    /**
     * Run all workloads. Returns true if all completed successfully.
     */
    public boolean run() {
        resultsDir.mkdirs();
        log.info("Results will be saved to {}", resultsDir.getAbsolutePath());

        List<String> workloadNames = new ArrayList<>(workloadYamls.keySet());

        // Initialize checkpoint for this run
        Checkpoint cp = checkpoint;
        if (cp == null) {
            cp = new Checkpoint();
            cp.resultsDir = resultsDir.getAbsolutePath();
            cp.mode = mode;
            cp.workloadNames = workloadNames;
            cp.completedWorkloadCount = 0;
            cp.currentWorkload = null;
        }

        int startWorkload = cp.completedWorkloadCount;

        try {
            for (int w = startWorkload; w < workloadNames.size(); w++) {
                if (stopRequested.get()) {
                    log.info("Stop requested — skipping remaining workloads");
                    break;
                }

                String workloadName = workloadNames.get(w);
                String yaml = workloadYamls.get(workloadName);

                int startTestPoint = 0;
                if (cp.currentWorkload != null && workloadName.equals(cp.currentWorkload.name)) {
                    startTestPoint = cp.currentWorkload.nextTestPointIndex;
                    log.info("Resuming workload '{}' from test point {}", workloadName, startTestPoint);
                }

                try {
                    switch (mode) {
                        case "run" -> runStandardWorkload(workloadName, yaml, startTestPoint, cp);
                        case "explore" -> runExploreWorkload(workloadName, yaml, startTestPoint, cp);
                        case "correctness" -> runCorrectnessWorkload(workloadName, yaml, startTestPoint, cp);
                        case "drain-backlog" -> runDrainBacklogWorkload(workloadName, yaml, startTestPoint, cp);
                        default -> throw new IllegalArgumentException(
                                "Unknown benchmark mode: '" + mode + "'. Valid modes: run, explore, correctness, drain-backlog");
                    }

                    // Workload complete — advance checkpoint
                    cp.completedWorkloadCount = w + 1;
                    cp.currentWorkload = null;
                    cp.save(outputBase);
                } catch (StopRequestedException e) {
                    // Stop arrived mid-workload. Do NOT advance the checkpoint
                    // or write the final result file — the per-test-point
                    // callback already saved the in-progress partial result
                    // and the checkpoint points at the next test to run.
                    log.info("Workload '{}' stopped early — checkpoint preserved for resume", workloadName);
                    return false;
                } catch (Exception e) {
                    // Checkpoint already reflects last successful test point
                    log.error("Failed to run the workload '{}'", workloadName, e);
                    return false;
                }
            }

            if (stopRequested.get()) {
                log.info("Run stopped early — checkpoint preserved for resume");
                return false;
            }

            // All workloads complete — delete checkpoint
            Checkpoint.delete(outputBase);
            return true;
        } finally {
            log.info("Benchmark run complete");
        }
    }

    // ─── Shared helpers ──────────────────────────────────────────────

    /**
     * Compute the base Kafka config summary by merging the -k file config with the
     * baseWorkload's embedded kafkaConfig. This represents the config before any
     * scenario-specific overrides.
     */
    private TestResult.KafkaConfigSummary resolveBaseKafkaConfigSummary(BaseWorkload base) {
        KafkaConfig merged = KafkaConfig.merge(fileKafkaConfig, base.kafkaConfig);
        return TestResult.KafkaConfigSummary.fromKafkaConfig(merged);
    }

    /**
     * Stamp provenance that is common to every top-level result format.
     * Keeping this in one helper prevents future result modes from recording
     * built-in metadata but silently dropping campaign-series annotations.
     */
    private void stampMetadata(ResultMetadata metadata, String beginTime, String endTime, Hooks hooks) {
        metadata.beginTime = beginTime;
        metadata.hooks = new ArrayList<>(hooks.executions());
        metadata.endTime = endTime;
        metadata.environment = environment;
        metadata.system = system;
        metadata.hardware = hardware;
        metadata.putUserMetadata(userMetadata);
        metadata.putExtraDimensions(extraDimensions);
    }

    /**
     * Build and validate the workload's hooks. On resume, executions from
     * before the interruption are carried over from the partial result.
     */
    private Hooks makeHooks(EngineConfig engineConfig, String workloadName, ResultMetadata previous) {
        Hooks hooks = new Hooks(engineConfig.hooks, hookExecutor, mode, workloadName, stopRequested,
                previous != null ? previous.hooks : List.of());
        hooks.validate();
        return hooks;
    }

    // ─── Standard "run" mode ───────────────────────────────────────────

    private void runStandardWorkload(String workloadName, String yaml,
                                      int startTestPoint, Checkpoint cp) throws Exception {
        WorkloadEngine engine = null;
        try {
            worker.stopAll();
            worker.awaitStopComplete(Duration.ofMinutes(10));
            log.info("Stopped any running workers");

            String partialFileName = workloadName + "-" + mode + "-in-progress.json";

            Workload workload = yamlMapper.readValue(yaml, Workload.class);

            RunResult partialResult = startTestPoint > 0
                    ? loadPartialResult(partialFileName, RunResult.class) : null;

            // Preserve the original begin time across resume so the result's
            // beginTime points at the run's actual start, not the resume time.
            String beginTime = (partialResult != null && partialResult.metadata.beginTime != null)
                    ? partialResult.metadata.beginTime
                    : dateFormat.format(new Date());

            Hooks hooks = makeHooks(workload.engineConfig, workloadName,
                    partialResult != null ? partialResult.metadata : null);

            CheckpointCallback callback = (completedIndex, partial) -> {
                // Stamp metadata on the partial so an interrupted run still has
                // timestamps for downstream tooling (the final write below also
                // sets these, but is skipped on StopRequestedException).
                if (partial instanceof RunResult rr) {
                    stampMetadata(rr.metadata, beginTime, dateFormat.format(new Date()), hooks);
                }
                savePartialResult(partialFileName, partial);
                cp.currentWorkload = new Checkpoint.CurrentWorkload(
                        workloadName, completedIndex + 1, partialFileName);
                cp.save(outputBase);
            };

            engine = new WorkloadEngine(workload, worker, topologyPlanner, fileKafkaConfig, stopRequested);
            engine.setTopologyRecorder(makeTopologyRecorder(workloadName));
            engine.setHooks(hooks);
            // Publish the engine for the coordinator's /modify endpoint.
            // Cleared in the outer finally so an exception during run() can't
            // leave a stale engine reference visible.
            if (activeEngineSink != null) {
                activeEngineSink.accept(engine);
            }
            log.info("=============== WORKLOAD : {} ===============", workload.name);

            hooks.run(HookPoint.BEFORE_WORKLOAD, HookContext.workload());
            RunResult result = engine.run(startTestPoint, partialResult, callback);
            hooks.run(HookPoint.AFTER_WORKLOAD, HookContext.workload());
            stampMetadata(result.metadata, beginTime, dateFormat.format(new Date()), hooks);

            var baseKafkaSummary = resolveBaseKafkaConfigSummary(workload.baseWorkload);
            if (baseKafkaSummary != null) {
                result.metadata.kafkaConfigName = baseKafkaSummary.name;
            }
            result.baseKafkaConfig = baseKafkaSummary;

            String finalFileName = String.format("%s-run-%s.json",
                    workloadName, dateFormat.format(new Date()));
            File resultFile = new File(resultsDir, finalFileName);

            log.info("Writing run result into {}", resultFile.getPath());
            jsonWriter.writeValue(resultFile, result);
            deletePartialResult(partialFileName);
        } finally {
            if (activeEngineSink != null) {
                activeEngineSink.accept(null);
            }
            if (engine != null) {
                try { engine.close(); } catch (Exception e) {}
            }
        }
    }

    // ─── Explore mode ──────────────────────────────────────────────────

    private void runExploreWorkload(String workloadName, String yaml,
                                     int startTestPoint, Checkpoint cp) throws Exception {
        ExploreEngine engine = null;
        try {
            worker.stopAll();
            worker.awaitStopComplete(Duration.ofMinutes(10));
            log.info("Stopped any running workers");

            String partialFileName = workloadName + "-" + mode + "-in-progress.json";

            ExploreWorkload exploreWorkload = yamlMapper.readValue(yaml, ExploreWorkload.class);

            ExploreResult partialResult = startTestPoint > 0
                    ? loadPartialResult(partialFileName, ExploreResult.class) : null;

            String beginTime = (partialResult != null && partialResult.metadata.beginTime != null)
                    ? partialResult.metadata.beginTime
                    : dateFormat.format(new Date());

            Hooks hooks = makeHooks(exploreWorkload.engineConfig, workloadName,
                    partialResult != null ? partialResult.metadata : null);

            CheckpointCallback callback = (completedIndex, partial) -> {
                if (partial instanceof ExploreResult er) {
                    stampMetadata(er.metadata, beginTime, dateFormat.format(new Date()), hooks);
                }
                savePartialResult(partialFileName, partial);
                cp.currentWorkload = new Checkpoint.CurrentWorkload(
                        workloadName, completedIndex + 1, partialFileName);
                cp.save(outputBase);
            };

            engine = new ExploreEngine(exploreWorkload, worker, topologyPlanner, fileKafkaConfig, stopRequested);
            engine.setTopologyRecorder(makeTopologyRecorder(workloadName));
            engine.setHooks(hooks);
            log.info("=============== EXPLORE LIMITS : {} ===============", exploreWorkload.name);

            hooks.run(HookPoint.BEFORE_WORKLOAD, HookContext.workload());
            ExploreResult result = engine.run(startTestPoint, partialResult, callback);
            hooks.run(HookPoint.AFTER_WORKLOAD, HookContext.workload());
            stampMetadata(result.metadata, beginTime, dateFormat.format(new Date()), hooks);

            var baseKafkaSummary = resolveBaseKafkaConfigSummary(exploreWorkload.baseWorkload);
            if (baseKafkaSummary != null) {
                result.metadata.kafkaConfigName = baseKafkaSummary.name;
            }
            result.config.baseKafkaConfig = baseKafkaSummary;

            String finalFileName = String.format("%s-explore-%s.json",
                    workloadName, dateFormat.format(new Date()));
            File resultFile = new File(resultsDir, finalFileName);

            log.info("Writing explore result into {}", resultFile.getPath());
            jsonWriter.writeValue(resultFile, result);
            deletePartialResult(partialFileName);
        } finally {
            if (engine != null) {
                try { engine.close(); } catch (Exception e) {}
            }
        }
    }

    // ─── Correctness mode ──────────────────────────────────────────────

    private void runCorrectnessWorkload(String workloadName, String yaml,
                                         int startTestPoint, Checkpoint cp) throws Exception {
        CorrectnessEngine engine = null;
        try {
            worker.stopAll();
            worker.awaitStopComplete(Duration.ofMinutes(10));
            log.info("Stopped any running workers");

            String partialFileName = workloadName + "-" + mode + "-in-progress.json";

            CorrectnessWorkload correctnessWorkload = yamlMapper.readValue(yaml, CorrectnessWorkload.class);

            CorrectnessResult partialResult = startTestPoint > 0
                    ? loadPartialResult(partialFileName, CorrectnessResult.class) : null;

            String beginTime = (partialResult != null && partialResult.metadata.beginTime != null)
                    ? partialResult.metadata.beginTime
                    : dateFormat.format(new Date());

            Hooks hooks = makeHooks(correctnessWorkload.engineConfig, workloadName,
                    partialResult != null ? partialResult.metadata : null);

            CheckpointCallback callback = (completedIndex, partial) -> {
                if (partial instanceof CorrectnessResult cr) {
                    stampMetadata(cr.metadata, beginTime, dateFormat.format(new Date()), hooks);
                }
                savePartialResult(partialFileName, partial);
                cp.currentWorkload = new Checkpoint.CurrentWorkload(
                        workloadName, completedIndex + 1, partialFileName);
                cp.save(outputBase);
            };

            engine = new CorrectnessEngine(correctnessWorkload, worker, topologyPlanner, fileKafkaConfig, stopRequested);
            engine.setTopologyRecorder(makeTopologyRecorder(workloadName));
            engine.setHooks(hooks);
            // Correctness publishes for the whole engine run. Its internal
            // controller accepts requests only during the measured CONCURRENT
            // phase and supplies precise rejection reasons at other times.
            if (activeEngineSink != null) {
                activeEngineSink.accept(engine);
            }
            log.info("=============== CORRECTNESS : {} ===============", correctnessWorkload.name);

            hooks.run(HookPoint.BEFORE_WORKLOAD, HookContext.workload());
            CorrectnessResult result = engine.run(startTestPoint, partialResult, callback);
            hooks.run(HookPoint.AFTER_WORKLOAD, HookContext.workload());
            stampMetadata(result.metadata, beginTime, dateFormat.format(new Date()), hooks);

            var baseKafkaSummary = resolveBaseKafkaConfigSummary(correctnessWorkload.baseWorkload);
            if (baseKafkaSummary != null) {
                result.metadata.kafkaConfigName = baseKafkaSummary.name;
            }
            result.config.baseKafkaConfig = baseKafkaSummary;

            String finalFileName = String.format("%s-correctness-%s.json",
                    workloadName, dateFormat.format(new Date()));
            File resultFile = new File(resultsDir, finalFileName);

            log.info("Writing correctness result into {}", resultFile.getPath());
            jsonWriter.writeValue(resultFile, result);
            deletePartialResult(partialFileName);
        } finally {
            if (activeEngineSink != null) {
                activeEngineSink.accept(null);
            }
            if (engine != null) {
                try { engine.close(); } catch (Exception e) {}
            }
        }
    }

    // ─── Drain-backlog mode ────────────────────────────────────────────

    private void runDrainBacklogWorkload(String workloadName, String yaml,
                                          int startTestPoint, Checkpoint cp) throws Exception {
        DrainBacklogEngine engine = null;
        try {
            worker.stopAll();
            worker.awaitStopComplete(Duration.ofMinutes(10));
            log.info("Stopped any running workers");

            String partialFileName = workloadName + "-" + mode + "-in-progress.json";

            DrainBacklogWorkload drainWorkload = yamlMapper.readValue(yaml, DrainBacklogWorkload.class);

            DrainBacklogResult partialResult = startTestPoint > 0
                    ? loadPartialResult(partialFileName, DrainBacklogResult.class) : null;

            String beginTime = (partialResult != null && partialResult.metadata.beginTime != null)
                    ? partialResult.metadata.beginTime
                    : dateFormat.format(new Date());

            Hooks hooks = makeHooks(drainWorkload.engineConfig, workloadName,
                    partialResult != null ? partialResult.metadata : null);

            CheckpointCallback callback = (completedIndex, partial) -> {
                if (partial instanceof DrainBacklogResult dr) {
                    stampMetadata(dr.metadata, beginTime, dateFormat.format(new Date()), hooks);
                }
                savePartialResult(partialFileName, partial);
                cp.currentWorkload = new Checkpoint.CurrentWorkload(
                        workloadName, completedIndex + 1, partialFileName);
                cp.save(outputBase);
            };

            engine = new DrainBacklogEngine(drainWorkload, worker, topologyPlanner, fileKafkaConfig, stopRequested);
            engine.setTopologyRecorder(makeTopologyRecorder(workloadName));
            engine.setHooks(hooks);
            log.info("=============== DRAIN-BACKLOG : {} ===============", drainWorkload.name);

            hooks.run(HookPoint.BEFORE_WORKLOAD, HookContext.workload());
            DrainBacklogResult result = engine.run(startTestPoint, partialResult, callback);
            hooks.run(HookPoint.AFTER_WORKLOAD, HookContext.workload());
            stampMetadata(result.metadata, beginTime, dateFormat.format(new Date()), hooks);

            var baseKafkaSummary = resolveBaseKafkaConfigSummary(drainWorkload.baseWorkload);
            if (baseKafkaSummary != null) {
                result.metadata.kafkaConfigName = baseKafkaSummary.name;
            }
            result.config.baseKafkaConfig = baseKafkaSummary;

            String finalFileName = String.format("%s-drain-backlog-%s.json",
                    workloadName, dateFormat.format(new Date()));
            File resultFile = new File(resultsDir, finalFileName);

            log.info("Writing drain-backlog result into {}", resultFile.getPath());
            jsonWriter.writeValue(resultFile, result);
            deletePartialResult(partialFileName);
        } finally {
            if (engine != null) {
                try { engine.close(); } catch (Exception e) {}
            }
        }
    }

    // ─── Topology recording ────────────────────────────────────────────

    /**
     * Build a {@link JsonFileTopologyRecorder} for the workload about to run.
     * The recorder writes to {@code <resultsDir>/<workloadName>-topologies-<ts>.json}
     * — same pattern as the result file so the CLI's results download (which
     * pulls every {@code *.json}) picks it up automatically. The timestamp is
     * captured once here so the file name is stable across all the workload's
     * test points.
     */
    private JsonFileTopologyRecorder makeTopologyRecorder(String workloadName) {
        String fileName = String.format("%s-topologies-%s.json",
                workloadName, dateFormat.format(new Date()));
        File file = new File(resultsDir, fileName);
        return new JsonFileTopologyRecorder(workloadName, file);
    }

    // ─── Partial result persistence ────────────────────────────────────

    private void savePartialResult(String fileName, Object result) {
        try {
            File file = new File(resultsDir, fileName);
            jsonWriter.writeValue(file, result);
        } catch (IOException e) {
            log.error("Failed to save partial result: {}", fileName, e);
        }
    }

    private <T> T loadPartialResult(String fileName, Class<T> clazz) {
        File file = new File(resultsDir, fileName);
        if (!file.exists()) return null;
        try {
            return jsonMapper.readValue(file, clazz);
        } catch (IOException e) {
            log.error("Failed to load partial result: {}", fileName, e);
            return null;
        }
    }

    private void deletePartialResult(String fileName) {
        File file = new File(resultsDir, fileName);
        if (file.exists()) {
            file.delete();
        }
    }
}
