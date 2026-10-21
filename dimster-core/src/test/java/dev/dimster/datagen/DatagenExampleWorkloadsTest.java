package dev.dimster.datagen;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.dimster.ScenarioExpander;
import dev.dimster.ScenarioExpander.TestPoint;
import dev.dimster.engines.correctness.CorrectnessWorkload;
import dev.dimster.engines.run.Workload;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Parses and expands the example workloads under run/workloads/datagen-test
 * with the coordinator's strict YAML settings, so the documented examples
 * can't drift from the workload schema or the dataset validation rules.
 */
public class DatagenExampleWorkloadsTest {

    private static final File DIR = new File("../run/workloads/datagen-test");

    // Same settings as BenchmarkRunner's workload mapper.
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

    @Test
    public void formatsByCompressionExpandsToSixDatasetPoints() throws Exception {
        Workload w = YAML.readValue(new File(DIR, "datagen-formats-x-compression.yaml"), Workload.class);
        List<TestPoint> points = ScenarioExpander.expand(w.baseWorkload, w.scenarios, null, null, null, w.runConfig);
        assertEquals(6, points.size());
        assertEquals(RecordFormat.AVRO, points.get(0).dataset().format());
        assertEquals(RecordFormat.JSON, points.get(5).dataset().format());
        assertEquals("zstd", points.get(5).kafkaConfig().producerConfig.get("compression.type"));
        assertEquals("user_id", points.get(0).dataset().keyColumn());
    }

    @Test
    public void cardinalitySweepAppliesItsColumnOverrides() throws Exception {
        Workload w = YAML.readValue(new File(DIR, "datagen-cardinality-sweep.yaml"), Workload.class);
        List<TestPoint> points = ScenarioExpander.expand(w.baseWorkload, w.scenarios, null, null, null, w.runConfig);
        assertEquals(6, points.size());
        assertEquals(1000, column(points.get(0), "customer_name").cardinality());
        assertEquals(1000, column(points.get(5), "country").runLength());
    }

    @Test
    public void correctnessExampleResolvesItsBaseDataset() throws Exception {
        CorrectnessWorkload w = YAML.readValue(new File(DIR, "datagen-correctness.yaml"), CorrectnessWorkload.class);
        List<TestPoint> points = ScenarioExpander.expand(w.baseWorkload, w.scenarios, null);
        assertEquals("taxi", points.get(0).dataset().preset());
    }

    @Test
    public void columnRecipesResolveAndGenerateWithinTheirDocumentedBounds() throws Exception {
        Workload w = YAML.readValue(new File(DIR, "datagen-column-recipes.yaml"), Workload.class);
        TestPoint point = ScenarioExpander.expand(w.baseWorkload, w.scenarios, null, null, null, w.runConfig).get(0);
        assertEquals(25, point.dataset().columns().size());

        RowGenerator rows = new DatasetGenerator(point.dataset()).newRowGenerator("producer-0", 0);
        ObjectMapper json = new ObjectMapper();
        for (int i = 0; i < 2000; i++) {
            var row = json.readTree(rows.next(new byte[]{0}));
            int score = row.get("score").asInt();
            assertTrue(score >= 0 && score <= 100);
            long signup = row.get("signup_time").asLong();
            assertTrue(signup >= 1577836800000L && signup <= 1735603200000L);
            int birthDay = row.get("birth_day").asInt();
            assertTrue(birthDay >= -7305 && birthDay <= 13148);
            assertTrue(List.of("S", "M", "L", "XL").contains(row.get("size").asText()));
            int rating = row.get("rating").asInt();
            assertTrue(rating >= 1 && rating <= 5);
        }
    }

    private static ResolvedDataset.Column column(TestPoint point, String name) {
        return point.dataset().columns().stream().filter(c -> c.name().equals(name)).findFirst().orElseThrow();
    }
}
