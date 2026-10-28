package dev.dimster;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.kafka.KafkaSpi;
import dev.dimster.worker.topology.TopologyPlanner;
import dev.dimster.worker.LocalWorker;
import dev.dimster.worker.Worker;
import dev.dimster.hooks.HookScripts;
import dev.dimster.hooks.ScriptHookExecutor;

/**
 * Single-process embedded-worker entry point for Dimster.
 *
 * <p>This main runs the orchestrator and an in-process {@link LocalWorker} in
 * a single JVM, against a user-supplied Kafka broker. It is intended for
 * ad-hoc local runs without a Kubernetes deployment.
 *
 * <p>For the standard distributed flow, use the {@code dimster} CLI against a
 * Kubernetes deployment: the CLI drives {@code CoordinatorServer} over HTTP,
 * which in turn drives remote {@code WorkerHttpServer}s. Worker-pool sizing
 * and producer/consumer split are configured there, not here.
 */
public class Dimster {

    static class Arguments {

        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = { "-k",
                "--kafka-configs" }, description = "Kafka config list. eg.: run/kafka-configs/kafka.yaml")
        public List<String> kafkaConfigs;

        @Parameter(names = { "-b", "--bootstrap-servers" },
                description = "Kafka bootstrap servers (e.g. localhost:9092). Injected into the kafka-config's commonConfig, overriding any value present in the file.")
        public String bootstrapServers;

        @Parameter(names = { "-w", "--workloads" }, description = "Workload file(s). eg: run/workloads/quick-test/quick-run-test.yaml")
        public List<String> workloads;

        @Parameter(names = { "-o", "--output" },
                description = "Output base directory; results and the log file land under <output>/<timestamp>/. Defaults to ./output.",
                required = false)
        public String output;

        @Parameter(names = { "-s", "--system" }, description = "System under test (e.g. 'Kafka 4.2'), embedded in the final result", required = false)
        public String system;

        @Parameter(names = { "-m", "--mode" }, description = "Benchmark mode: run, explore, correctness, or drain-backlog")
        public String mode = "run";
    }

    public static void main(String[] args) throws Exception {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("dimster");

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

        if (arguments.workloads == null) {
            System.err.println("At least one workload must be specified");
            System.exit(-1);
        }

        // Per-run output directory: <output-base>/<timestamp>/. This is computed
        // up front (before any log call) so we can redirect the log file into the
        // same directory as the results JSON — making each embedded-mode run
        // self-contained in one directory.
        DateFormat dirDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        String resultsDirName = dirDateFormat.format(new Date());
        String outputBase = (arguments.output != null && !arguments.output.isEmpty())
                ? arguments.output
                : "output";
        File resultsSubDir = new File(outputBase, resultsDirName);
        if (!resultsSubDir.mkdirs() && !resultsSubDir.isDirectory()) {
            System.err.println("Failed to create output directory: " + resultsSubDir);
            System.exit(-1);
        }

        // Redirect the rolling log file into the run's output directory unless the
        // user has explicitly set DIMSTER_LOG_FILE in the environment (the k8s
        // deployment yamls do this to land logs on a PVC). log4j2 has already
        // loaded its config by now — bound by the static `log` field initializer
        // below — so we set the system property the config falls back to and
        // force a reconfigure to rebuild the RollingFile appender with the new path.
        if (System.getenv("DIMSTER_LOG_FILE") == null) {
            System.setProperty("DIMSTER_LOG_FILE",
                    new File(resultsSubDir, "dimster.log").getAbsolutePath());
            ((LoggerContext) LogManager.getContext(false)).reconfigure();
        }

        // Resolve the Kafka SPI binding eagerly so the bound kafka-clients version
        // (and capability set) is logged before any worker setup runs. Any misbuild
        // (zero or multiple SPI impls on the classpath) fails here with a clear
        // error rather than later inside a wrapper construction.
        KafkaSpi.factory();

        log.info("Starting benchmark with config: {}", writer.writeValueAsString(arguments));

        Map<String, String> workloads = new TreeMap<>();
        // Hook scripts the workloads reference, read from local files (there
        // is no CLI here to ship them in a run request).
        Map<String, String> hookScripts = new TreeMap<>();
        for (String path : arguments.workloads) {
            File file = new File(path);
            String name = file.getName().substring(0, file.getName().lastIndexOf('.'));
            String yaml = java.nio.file.Files.readString(file.toPath());
            workloads.put(name, yaml);
            hookScripts.putAll(HookScripts.load(yaml, file.getAbsoluteFile().toPath().getParent()));
        }

        log.info("Workloads: {}", workloads.keySet());

        // Single-process mode: orchestrator drives an in-process LocalWorker.
        // The synthetic single-worker planner keeps engines uniform — they call
        // planner.plan(spec) without caring whether the worker is local or remote.
        TopologyPlanner topologyPlanner = new TopologyPlanner(
                List.of("local"),
                TopologyPlanner.WorkerMode.MIXED,
                new TopologyPlanner.RatioSplit(1, 1));
        Worker worker = new LocalWorker();

        // Kafka config file is optional when config is embedded in workloads
        File kafkaConfigFile = (arguments.kafkaConfigs != null && !arguments.kafkaConfigs.isEmpty())
                ? new File(arguments.kafkaConfigs.get(0))
                : null;

        // --bootstrap-servers convenience: in embedded mode the broker URL is not
        // injected by the (k8s) CLI, so we let the user supply it on the command
        // line. We parse the kafka-config (or start from an empty one), set
        // commonConfig.bootstrap.servers, and write the merged config to a temp
        // file that BenchmarkRunner reads instead of the original.
        if (arguments.bootstrapServers != null) {
            KafkaConfig kc = kafkaConfigFile != null
                    ? yamlMapper.readValue(kafkaConfigFile, KafkaConfig.class)
                    : new KafkaConfig();
            if (kc.commonConfig == null) {
                kc.commonConfig = new HashMap<>();
            }
            kc.commonConfig.put("bootstrap.servers", arguments.bootstrapServers);
            File tempKafkaFile = File.createTempFile("dimster-embedded-kafka-", ".yaml");
            tempKafkaFile.deleteOnExit();
            yamlMapper.writeValue(tempKafkaFile, kc);
            kafkaConfigFile = tempKafkaFile;
            log.info("Injected bootstrap.servers={} into kafka config (temp file {})",
                    arguments.bootstrapServers, tempKafkaFile);
        }

        AtomicBoolean stopRequested = new AtomicBoolean(false);

        BenchmarkRunner runner = new BenchmarkRunner(
                arguments.mode, worker, topologyPlanner, kafkaConfigFile, workloads, resultsSubDir,
                stopRequested, arguments.system, null, null);
        runner.setHookExecutor(ScriptHookExecutor.install(hookScripts, new File(resultsSubDir, "hooks")));

        boolean success = false;
        try {
            success = runner.run();
        } finally {
            worker.close();
        }

        System.exit(success ? 0 : 1);
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    private static final Logger log = LoggerFactory.getLogger(Dimster.class);
}
