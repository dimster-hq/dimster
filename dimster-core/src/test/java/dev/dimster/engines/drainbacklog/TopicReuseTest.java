package dev.dimster.engines.drainbacklog;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.dimster.ScenarioExpander;
import dev.dimster.ScenarioExpander.TestPoint;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * How a persistent backlog's topics are named and sized, and when the topics
 * found on the cluster under those names can be drained as-is.
 */
public class TopicReuseTest {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

    private static TestPoint point(int topics, double backlogSizeGB, int messageSize, int groups) throws Exception {
        DrainBacklogWorkload wl = YAML.readValue("""
                name: kept
                drainBacklogConfig:
                  backlogSizeGB: %s
                  reuseTopic: true
                baseWorkload:
                  topics: %d
                  partitionsPerTopic: 2
                  messageSize: %d
                  consumerGroupsPerTopic: %d
                """.formatted(backlogSizeGB, topics, messageSize, groups), DrainBacklogWorkload.class);
        return ScenarioExpander.expand(wl.baseWorkload, wl.scenarios, null, wl.drainBacklogConfig).getFirst();
    }

    // ─── Naming and sizing ─────────────────────────────────────────────

    @Test
    public void topics_are_numbered_under_the_identity_prefix() throws Exception {
        TestPoint p = point(3, 0.1, 1000, 1);
        String prefix = BacklogReuse.persistentTopicPrefix("id");
        assertEquals(List.of(prefix + "-0000", prefix + "-0001", prefix + "-0002"), TopicReuse.topicNames(p, "id"));
    }

    @Test
    public void min_records_is_the_build_stop_condition() throws Exception {
        assertEquals(100_000, TopicReuse.minRecords(point(1, 0.1, 1000, 1)));
        // Each consumer group counts towards the backlog size.
        assertEquals(50_000, TopicReuse.minRecords(point(1, 0.1, 1000, 2)));
        // Rounds up: the build stops at the first count reaching the size.
        assertEquals(333_334, TopicReuse.minRecords(point(1, 0.001, 3, 1)));
    }

    // ─── Assessment ────────────────────────────────────────────────────

    private static final List<String> NAMES = List.of("kept-0000", "kept-0001");
    private static final TopicPartition A0 = new TopicPartition("kept-0000", 0);
    private static final TopicPartition A1 = new TopicPartition("kept-0000", 1);
    private static final TopicPartition B0 = new TopicPartition("kept-0001", 0);
    private static final TopicPartition B1 = new TopicPartition("kept-0001", 1);
    private static final Map<String, Integer> BOTH = Map.of("kept-0000", 2, "kept-0001", 2);
    private static final Map<TopicPartition, Long> STARTS = Map.of(A0, 0L, A1, 0L, B0, 0L, B1, 0L);
    private static final Map<TopicPartition, Long> ENDS = Map.of(A0, 30L, A1, 30L, B0, 20L, B1, 20L);

    private static TopicReuse.Assessment assess(long minRecords, Map<String, Integer> partitionCounts,
                                                Map<TopicPartition, Long> starts, Map<TopicPartition, Long> ends) {
        return TopicReuse.assess(NAMES, 2, minRecords, partitionCounts, starts, ends);
    }

    private static String unusableReason(TopicReuse.Assessment a) {
        assertTrue("expected Unusable, got " + a, a instanceof TopicReuse.Unusable);
        return ((TopicReuse.Unusable) a).reason();
    }

    @Test
    public void no_topics_is_absent() {
        assertTrue(assess(100, Map.of(), Map.of(), Map.of()) instanceof TopicReuse.Absent);
    }

    @Test
    public void a_complete_backlog_is_usable_with_its_offsets_as_records() {
        var a = assess(100, BOTH, STARTS, ENDS);
        var usable = (TopicReuse.Usable) a;
        assertEquals(100, usable.records());
        assertEquals(ENDS, usable.endOffsets());
    }

    @Test
    public void too_few_records_is_an_interrupted_build() {
        assertTrue(unusableReason(assess(101, BOTH, STARTS, ENDS)).contains("interrupted"));
    }

    @Test
    public void a_missing_topic_is_unusable() {
        assertTrue(unusableReason(assess(100, Map.of("kept-0000", 2), Map.of(), Map.of())).contains("kept-0001"));
    }

    @Test
    public void a_different_partition_count_is_unusable() {
        var why = unusableReason(assess(100, Map.of("kept-0000", 2, "kept-0001", 3), STARTS, ENDS));
        assertTrue(why, why.contains("3 partitions"));
    }

    @Test
    public void a_deleted_head_is_unusable() {
        var starts = Map.of(A0, 0L, A1, 5L, B0, 0L, B1, 0L);
        assertTrue(unusableReason(assess(10, BOTH, starts, ENDS)).contains("retention"));
    }
}
