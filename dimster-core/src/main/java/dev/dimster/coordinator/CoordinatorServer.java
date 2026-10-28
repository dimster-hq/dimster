package dev.dimster.coordinator;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.dimster.BenchmarkRunner;
import dev.dimster.Checkpoint;
import dev.dimster.hooks.ScriptHookExecutor;
import dev.dimster.kafka.KafkaSpi;
import dev.dimster.worker.RemoteWorkerClient;
import dev.dimster.worker.topology.TopologyPlanner;
import dev.dimster.worker.Worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorServer {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorServer.class);

    private static final ObjectMapper jsonMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final ObjectWriter jsonWriter = jsonMapper.writerWithDefaultPrettyPrinter();

    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // Strict allow-list for client-provided results subdirectory names. The
    // CLI generates a yyyy-MM-dd-HH-mm-ss timestamp (digits and hyphens only)
    // and we use it directly as a path component under outputDir. Refusing
    // anything that doesn't match this exact shape neutralises path traversal
    // (../foo), absolute paths (/etc/...), Windows drive letters, NUL bytes,
    // and any other escape from the intended results directory.
    private static final Pattern SAFE_SUBDIR =
            Pattern.compile("^\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}$");

    static {
        yamlMapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    enum State { IDLE, RUNNING, STOPPING, FAILED }

    private final AtomicReference<State> state = new AtomicReference<>(State.IDLE);
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private volatile String lastError = null;

    // Reference to the current run/correctness modification target, published
    // by BenchmarkRunner and cleared in its finally block. Explore and
    // drain-backlog leave it null. AtomicReference safely publishes the
    // engine to HTTP handler threads; each engine serializes its own actions.
    private final AtomicReference<ModificationTarget> activeEngine = new AtomicReference<>();

    static class Arguments {
        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;

        @Parameter(names = {"-p", "--port"}, description = "HTTP port to listen on")
        public int httpPort = 8080;
    }

    public static void main(String[] args) {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("benchmark-coordinator");

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
            // is in the coordinator log too (helps debugging "which dimster image
            // version is running here?"). The coordinator itself doesn't use Kafka
            // clients, but it's bundled in the same per-flavor tarball as the worker,
            // so the SPI impl is always on the classpath.
            KafkaSpi.factory();

            log.info("Starting coordinator on port {}", arguments.httpPort);
            new CoordinatorServer(arguments.httpPort);
        } catch (Exception e) {
            log.error("Coordinator terminating due to exception at startup", e);
            System.exit(-1);
        }
    }

    public CoordinatorServer(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        // server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.setExecutor(Executors.newCachedThreadPool());

        server.createContext("/health", this::handleHealth);
        server.createContext("/status", this::handleStatus);
        server.createContext("/run", this::handleRun);
        server.createContext("/resume", this::handleResume);
        server.createContext("/stop", this::handleStop);
        server.createContext("/modify", this::handleModify);

        server.start();
        log.info("Coordinator HTTP server started on port {}", port);
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        State current = state.get();
        String outputBase = "/benchmark/results";
        boolean hasCheckpoint = new File(outputBase, "checkpoint.json").exists();

        String json;
        if (current == State.FAILED && lastError != null) {
            json = String.format("{\"state\":\"%s\",\"error\":\"%s\",\"checkpoint\":%s}",
                    current.name(), lastError.replace("\"", "'"), hasCheckpoint);
        } else {
            json = String.format("{\"state\":\"%s\",\"checkpoint\":%s}",
                    current.name(), hasCheckpoint);
        }
        respondJson(exchange, 200, json);
    }

    private void handleRun(HttpExchange exchange) throws IOException {
        State current = state.get();
        if (current != State.IDLE && current != State.FAILED) {
            respondJson(exchange, 409, "{\"error\":\"Coordinator is " + current.name() + "\"}");
            return;
        }
        if (!state.compareAndSet(current, State.RUNNING)) {
            respondJson(exchange, 409, "{\"error\":\"Coordinator state changed concurrently\"}");
            return;
        }

        RunRequest request;
        try {
            request = jsonMapper.readValue(exchange.getRequestBody(), RunRequest.class);
        } catch (Exception e) {
            state.set(State.IDLE);
            respondJson(exchange, 400,
                    "{\"error\":\"Invalid request: " + e.getMessage().replace("\"", "'") + "\"}");
            return;
        }

        lastError = null;
        stopRequested.set(false);
        // Thread.startVirtualThread(() -> runBenchmark(request, null));
        Thread.ofPlatform().name("benchmark-run").start(() -> runBenchmark(request, null));
        respondJson(exchange, 200, "{\"status\":\"started\"}");
    }

    private void handleResume(HttpExchange exchange) throws IOException {
        State current = state.get();
        if (current != State.IDLE && current != State.FAILED) {
            respondJson(exchange, 409, "{\"error\":\"Coordinator is " + current.name() + "\"}");
            return;
        }
        if (!state.compareAndSet(current, State.RUNNING)) {
            respondJson(exchange, 409, "{\"error\":\"Coordinator state changed concurrently\"}");
            return;
        }

        // Read checkpoint
        File outputBase = new File("/benchmark/results");
        Checkpoint checkpoint = Checkpoint.load(outputBase);
        if (checkpoint == null) {
            state.set(State.IDLE);
            respondJson(exchange, 404, "{\"error\":\"No checkpoint found to resume from\"}");
            return;
        }

        // Load saved RunRequest
        RunRequest savedRequest;
        try {
            savedRequest = jsonMapper.readValue(new File(checkpoint.runRequestFile), RunRequest.class);
        } catch (Exception e) {
            state.set(State.IDLE);
            respondJson(exchange, 500,
                    "{\"error\":\"Failed to load saved run request: " + e.getMessage().replace("\"", "'") + "\"}");
            return;
        }

        // Read fresh worker list from resume request
        ResumeRequest resumeReq;
        try {
            resumeReq = jsonMapper.readValue(exchange.getRequestBody(), ResumeRequest.class);
        } catch (Exception e) {
            state.set(State.IDLE);
            respondJson(exchange, 400,
                    "{\"error\":\"Invalid resume request: " + e.getMessage().replace("\"", "'") + "\"}");
            return;
        }

        // Build RunRequest with saved config but fresh workers.
        // Carry resultsSubdir through unchanged: the resumed run reuses the
        // original directory (via the checkpoint), but the in-memory request
        // should still reflect what was originally requested.
        RunRequest request = new RunRequest(
                savedRequest.fileType(),
                savedRequest.mode(),
                savedRequest.kafkaConfigYaml(),
                savedRequest.workloads(),
                resumeReq.workers(),
                savedRequest.topology(),
                savedRequest.workerRatio(),
                savedRequest.producerWorkers(),
                savedRequest.consumerWorkers(),
                savedRequest.outputDir(),
                savedRequest.system(),
                savedRequest.environment(),
                savedRequest.hardware(),
                savedRequest.metadata(),
                savedRequest.extraDimensions(),
                savedRequest.resultsSubdir(),
                savedRequest.hookScripts()
        );

        lastError = null;
        stopRequested.set(false);
        // Thread.startVirtualThread(() -> runBenchmark(request, checkpoint));
        Thread.ofPlatform().name("benchmark-resume").start(() -> runBenchmark(request, checkpoint));
        respondJson(exchange, 200, "{\"status\":\"resuming\"}");
    }

    private void handleStop(HttpExchange exchange) throws IOException {
        State current = state.get();
        if (current == State.RUNNING) {
            stopRequested.set(true);
            state.set(State.STOPPING);
            log.info("Stop requested via HTTP");
            respondJson(exchange, 200, "{\"status\":\"stopping\"}");
        } else {
            respondJson(exchange, 409, "{\"error\":\"Coordinator is " + current.name() + "\"}");
        }
    }

    /**
     * Side-channel modification of a running benchmark. Rejected unless the
     * coordinator is RUNNING and the active mode exposes a modification
     * target. Mode-specific validation remains inside that target.
     */
    private void handleModify(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            respondJson(exchange, 405, "{\"error\":\"method not allowed (use POST)\"}");
            return;
        }
        if (state.get() != State.RUNNING) {
            respondJson(exchange, 409, "{\"error\":\"Coordinator is " + state.get().name()
                    + " (modifications are only accepted while RUNNING)\"}");
            return;
        }
        ModificationTarget engine = activeEngine.get();
        if (engine == null) {
            // The active mode is unmodifiable or its target has already been
            // cleared during teardown. Either way there is nothing to act on.
            respondJson(exchange, 409,
                    "{\"error\":\"no active modifiable test point\"}");
            return;
        }

        ModifyRequest request;
        try {
            request = jsonMapper.readValue(exchange.getRequestBody(), ModifyRequest.class);
        } catch (Exception e) {
            respondJson(exchange, 400,
                    "{\"error\":\"Invalid request: " + e.getMessage().replace("\"", "'") + "\"}");
            return;
        }

        ModifyResult result = engine.applyModification(request);
        // Echo the engine-side message back so the operator sees the same
        // text in their CLI as appears in the coordinator log.
        String message = result.message().replace("\"", "'");
        if (result.accepted()) {
            respondJson(exchange, 200, "{\"status\":\"accepted\",\"message\":\"" + message + "\"}");
        } else {
            respondJson(exchange, 409, "{\"error\":\"" + message + "\"}");
        }
    }

    /**
     * Called by BenchmarkRunner when a modifiable engine starts and again
     * with null in its finally block. Unmodifiable modes never register.
     */
    public void setActiveEngine(ModificationTarget engine) {
        activeEngine.set(engine);
    }

    private void respondJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] response = json.getBytes();
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
        exchange.close();
    }

    private void runBenchmark(RunRequest request, Checkpoint checkpoint) {
        Worker worker = null;
        try {
            // Write kafka config to temp file if provided
            File kafkaConfigFile = null;
            if (request.kafkaConfigYaml() != null && !request.kafkaConfigYaml().isEmpty()) {
                kafkaConfigFile = File.createTempFile("kafka-config-", ".yaml");
                kafkaConfigFile.deleteOnExit();
                Files.writeString(kafkaConfigFile.toPath(), request.kafkaConfigYaml());
            }

            // Pass raw workload YAMLs — BenchmarkRunner detects type
            Map<String, String> workloads = new TreeMap<>(request.workloads());

            // Build planner + worker
            TopologyPlanner.WorkerSplit split = buildWorkerSplit(request);
            TopologyPlanner.WorkerMode mode = buildMode(request);
            TopologyPlanner topologyPlanner = new TopologyPlanner(request.workers(), mode, split);
            worker = new RemoteWorkerClient(topologyPlanner);

            // Build results directory
            File resultsSubDir;
            if (checkpoint != null) {
                // Resume: use existing results directory
                resultsSubDir = new File(checkpoint.resultsDir);
            } else {
                // New run: prefer the CLI-chosen subdirectory name when
                // provided so the CLI-staged configs and our result JSONs
                // land in the same directory. Fall back to server-generated
                // timestamp for older clients (no resultsSubdir field).
                // Validate strictly to prevent path traversal — see SAFE_SUBDIR.
                String outputBase = request.outputDir() != null ? request.outputDir() : ".";
                String subdirName;
                if (request.resultsSubdir() != null && !request.resultsSubdir().isEmpty()) {
                    if (!SAFE_SUBDIR.matcher(request.resultsSubdir()).matches()) {
                        throw new IllegalArgumentException(
                                "Invalid resultsSubdir (must match yyyy-MM-dd-HH-mm-ss): "
                                        + request.resultsSubdir());
                    }
                    subdirName = request.resultsSubdir();
                } else {
                    DateFormat dirDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
                    subdirName = dirDateFormat.format(new Date());
                }
                resultsSubDir = new File(outputBase, subdirName);
            }

            // Save RunRequest for resume (only on new runs)
            if (checkpoint == null) {
                resultsSubDir.mkdirs();
                File runRequestFile = new File(resultsSubDir, "run-request.json");
                jsonWriter.writeValue(runRequestFile, request);
                log.info("Saved run request to {}", runRequestFile.getPath());

                // Initialize checkpoint with runRequestFile path
                Checkpoint cp = new Checkpoint();
                cp.runRequestFile = runRequestFile.getAbsolutePath();
                cp.resultsDir = resultsSubDir.getAbsolutePath();
                cp.mode = request.mode() != null ? request.mode() : "run";
                cp.workloadNames = new java.util.ArrayList<>(workloads.keySet());
                cp.completedWorkloadCount = 0;
                cp.save(resultsSubDir.getParentFile());
                checkpoint = cp;
            }

            String benchmarkMode = request.mode() != null ? request.mode() : "run";
            BenchmarkRunner runner = new BenchmarkRunner(
                    benchmarkMode, worker, topologyPlanner, kafkaConfigFile, workloads, resultsSubDir,
                    stopRequested, request.system(), request.environment(), request.hardware(),
                    request.metadata(), request.extraDimensions(),
                    checkpoint, this::setActiveEngine);
            runner.setHookExecutor(ScriptHookExecutor.install(
                    request.hookScripts(), new File(resultsSubDir, "hooks")));
            boolean success = runner.run();

            if (kafkaConfigFile != null) kafkaConfigFile.delete();

            if (!success) {
                lastError = "Benchmark failed — check logs for details";
                state.set(State.FAILED);
                log.info("Coordinator is FAILED (checkpoint preserved for resume)");
                return;
            }
        } catch (Exception e) {
            log.error("Benchmark run failed", e);
            lastError = e.getMessage();
            state.set(State.FAILED);
            log.info("Coordinator is FAILED (checkpoint preserved for resume)");
            return;
        } finally {
            if (worker != null) {
                try { worker.close(); } catch (Exception e) {}
            }
            if (state.get() != State.FAILED) {
                state.set(State.IDLE);
                stopRequested.set(false);
                log.info("Coordinator is IDLE");
            } else {
                stopRequested.set(false);
            }
        }
    }

    private TopologyPlanner.WorkerSplit buildWorkerSplit(RunRequest request) {
        if (request.producerWorkers() != null && request.consumerWorkers() != null) {
            return new TopologyPlanner.ExplicitSplit(request.producerWorkers(), request.consumerWorkers());
        }
        if (request.workerRatio() != null && !request.workerRatio().isEmpty()) {
            String[] parts = request.workerRatio().split(":");
            if (parts.length == 2) {
                return new TopologyPlanner.RatioSplit(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
            }
        }
        return new TopologyPlanner.RatioSplit(1, 1);
    }

    private TopologyPlanner.WorkerMode buildMode(RunRequest request) {
        String assignment = request.topology();
        if ("separated".equals(assignment) || "ensemble".equals(assignment)) {
            log.info("Using separated worker assignment");
            return TopologyPlanner.WorkerMode.SEPARATED;
        }
        // Default: mixed (also accepts legacy "swarm")
        log.info("Using mixed worker assignment");
        return TopologyPlanner.WorkerMode.MIXED;
    }
}
