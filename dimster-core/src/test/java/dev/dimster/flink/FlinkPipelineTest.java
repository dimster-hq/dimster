package dev.dimster.flink;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import dev.dimster.ConsumerProcMode;
import dev.dimster.ConsumerType;
import dev.dimster.datagen.DatasetResolver;
import dev.dimster.datagen.DatasetSpec;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.datagen.ResolvedDataset;
import dev.dimster.kafka.KafkaConfig;
import dev.dimster.utils.distributor.MessageDistributorType;
import dev.dimster.worker.topology.TopologyPlanner;
import dev.dimster.worker.topology.VirtualTopology;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * {@link FlinkPipeline} and {@link FlinkRestClient} against a stub of the
 * Flink REST endpoints they use, with jobs whose state the test controls.
 */
public class FlinkPipelineTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    /** Minimal in-memory Flink JobManager. */
    static class StubFlink {
        int slotsAvailable = 4;
        final List<String> uploads = new ArrayList<>();
        final Map<String, String> jobStates = new LinkedHashMap<>();
        final Map<String, String> jobNames = new LinkedHashMap<>();
        final List<String> cancelled = new ArrayList<>();
        final List<String> runBodies = new ArrayList<>();
        /** State a newly submitted job reports first. */
        String initialState = "RUNNING";
        /** Answer the next jar run with 404, as after a JobManager restart. */
        boolean loseJar = false;

        HttpServer server;

        void start() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private synchronized void handle(HttpExchange ex) throws IOException {
            String path = ex.getRequestURI().getPath();
            String method = ex.getRequestMethod();
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            if (path.equals("/overview")) {
                reply(ex, 200, "{\"taskmanagers\":1,\"slots-total\":4,\"slots-available\":" + slotsAvailable
                        + ",\"flink-version\":\"2.2.1\"}");
            } else if (path.equals("/jars/upload")) {
                String id = "jar" + uploads.size() + "_dimster-flink-runner.jar";
                uploads.add(id);
                reply(ex, 200, "{\"filename\":\"/tmp/flink-web/flink-web-upload/" + id + "\",\"status\":\"success\"}");
            } else if (path.startsWith("/jars/") && path.endsWith("/run")) {
                if (loseJar) {
                    loseJar = false;
                    reply(ex, 404, "{\"errors\":[\"Jar file does not exist\"]}");
                    return;
                }
                runBodies.add(body);
                String id = "job" + jobStates.size();
                jobStates.put(id, initialState);
                jobNames.put(id, "dimster-abc");
                reply(ex, 200, "{\"jobid\":\"" + id + "\"}");
            } else if (path.equals("/jobs/overview")) {
                StringBuilder sb = new StringBuilder("{\"jobs\":[");
                String sep = "";
                for (var e : jobStates.entrySet()) {
                    sb.append(sep).append("{\"jid\":\"").append(e.getKey()).append("\",\"name\":\"")
                            .append(jobNames.get(e.getKey())).append("\",\"state\":\"").append(e.getValue()).append("\"}");
                    sep = ",";
                }
                reply(ex, 200, sb.append("]}").toString());
            } else if (path.matches("/jobs/[^/]+") && method.equals("PATCH")) {
                String id = path.substring("/jobs/".length());
                cancelled.add(id);
                jobStates.put(id, "CANCELED");
                reply(ex, 202, "{}");
            } else if (path.matches("/jobs/[^/]+/exceptions")) {
                reply(ex, 200, "{\"root-exception\":\"java.lang.IllegalArgumentException: boom\"}");
            } else if (path.matches("/jobs/[^/]+/metrics")) {
                reply(ex, 200, "[{\"id\":\"numRestarts\",\"value\":\"2\"}]");
            } else if (path.matches("/jobs/[^/]+")) {
                String id = path.substring("/jobs/".length());
                String state = jobStates.get(id);
                String vertex = "RUNNING".equals(state) ? "RUNNING" : "CREATED";
                reply(ex, 200, "{\"jid\":\"" + id + "\",\"name\":\"dimster-abc\",\"state\":\"" + state
                        + "\",\"vertices\":[{\"id\":\"v1\",\"status\":\"" + vertex + "\"}]}");
            } else {
                reply(ex, 404, "{\"errors\":[\"Not found: " + path + "\"]}");
            }
        }

        private static void reply(HttpExchange ex, int status, String body) throws IOException {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(status, bytes.length);
            ex.getResponseBody().write(bytes);
            ex.close();
        }
    }

    private StubFlink flink;
    private FlinkPipeline pipeline;

    @Before
    public void setUp() throws IOException {
        flink = new StubFlink();
        flink.start();
        Path jar = tmp.newFile("dimster-flink-runner.jar").toPath();
        Files.write(jar, new byte[]{1, 2, 3});
        pipeline = new FlinkPipeline(new FlinkRestClient(flink.url(), jar), new AtomicBoolean(false),
                java.time.Duration.ofSeconds(1));
    }

    @After
    public void tearDown() {
        flink.server.stop(0);
    }

    private static FlinkSpec spec(int parallelism) {
        var spec = new FlinkSpec();
        spec.sql = "SELECT * FROM source";
        spec.parallelism = parallelism;
        spec.outputPartitionsPerTopic = 2;
        return spec;
    }

    private static ResolvedDataset clicks() {
        var spec = new DatasetSpec();
        spec.preset = "clicks";
        spec.format = RecordFormat.AVRO;
        return DatasetResolver.resolve(spec);
    }

    private static VirtualTopology topology() {
        var planner = new TopologyPlanner(List.of("w1"), TopologyPlanner.WorkerMode.MIXED,
                new TopologyPlanner.RatioSplit(1, 1), () -> "abc");
        return planner.plan(new TopologyPlanner.TopologySpec("dimster", 2, 2, 1, 1, 1,
                ConsumerType.CONSUMER_GROUP, 0, 0, 1, ConsumerProcMode.BLOCKING, 10000,
                MessageDistributorType.NO_KEY, 1.0, false, false, 2));
    }

    private static KafkaConfig kafka() {
        var config = new KafkaConfig();
        config.commonConfig = new LinkedHashMap<>(Map.of("bootstrap.servers", "kafka:9092"));
        return config;
    }

    @Test
    public void startUploadsOnceSubmitsAndWaitsForRunning() throws Exception {
        String jobId = pipeline.start(spec(2), clicks(), topology(), kafka());
        assertEquals("job0", jobId);
        assertEquals(1, flink.uploads.size());
        assertTrue(flink.runBodies.getFirst().contains("dev.dimster.flink.runner.DimsterSqlJob"));
        assertEquals(2, pipeline.restarts());

        pipeline.stop();
        assertEquals(List.of("job0"), flink.cancelled);
        assertNull(pipeline.jobId());
        pipeline.stop(); // idempotent
        assertEquals(1, flink.cancelled.size());

        // The next test point reuses the uploaded jar.
        pipeline.start(spec(1), clicks(), topology(), kafka());
        assertEquals(1, flink.uploads.size());
    }

    @Test
    public void lostJarIsUploadedAgain() throws Exception {
        pipeline.start(spec(1), clicks(), topology(), kafka());
        pipeline.stop();
        flink.loseJar = true;
        pipeline.start(spec(1), clicks(), topology(), kafka());
        assertEquals(2, flink.uploads.size());
    }

    @Test
    public void tooFewSlotsFailsBeforeSubmitting() {
        flink.slotsAvailable = 1;
        var e = assertThrows(IllegalStateException.class,
                () -> pipeline.start(spec(2), clicks(), topology(), kafka()));
        assertTrue(e.getMessage(), e.getMessage().contains("needs 2 free task slots"));
        assertTrue(flink.runBodies.isEmpty());
    }

    @Test
    public void slotsAreCountedForTheMostParallelOperator() {
        flink.slotsAvailable = 2;
        FlinkSpec fanOut = spec(1);
        fanOut.sinkParallelism = 4;
        var e = assertThrows(IllegalStateException.class,
                () -> pipeline.start(fanOut, clicks(), topology(), kafka()));
        assertTrue(e.getMessage(), e.getMessage().contains("needs 4 free task slots"));
    }

    @Test
    public void jobThatFailsOnStartupReportsTheRootException() {
        flink.initialState = "FAILED";
        var e = assertThrows(IllegalStateException.class,
                () -> pipeline.start(spec(1), clicks(), topology(), kafka()));
        assertTrue(e.getMessage(), e.getMessage().contains("boom"));
        assertNull(pipeline.jobId());
    }

    @Test
    public void leftoverDimsterJobsAreCancelled() throws Exception {
        flink.jobStates.put("old", "RUNNING");
        flink.jobNames.put("old", "dimster-old");
        flink.jobStates.put("other", "RUNNING");
        flink.jobNames.put("other", "someone-elses-job");
        flink.jobStates.put("done", "FINISHED");
        flink.jobNames.put("done", "dimster-done");
        pipeline.cancelLeftovers();
        assertEquals(List.of("old"), flink.cancelled);
    }

    @Test
    public void unreachableClusterExplainsHowToDeployFlink() {
        var unreachable = new FlinkPipeline(new FlinkRestClient("http://127.0.0.1:1", Path.of("x.jar")),
                new AtomicBoolean(false));
        var e = assertThrows(IllegalStateException.class, unreachable::checkReachable);
        assertTrue(e.getMessage(), e.getMessage().contains("dimster deploy"));
    }
}
