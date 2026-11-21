package dev.dimster.kafka;

import static org.junit.Assert.*;

import java.lang.reflect.Proxy;
import java.util.*;

import org.junit.Test;

import dev.dimster.spi.kafka.RawAdmin;

/**
 * The topic-tag ownership rules that let several coordinators (one per env)
 * share a Kafka cluster: each names and cleans up only its own tag's topics
 * and groups. See {@link KafkaCluster}.
 */
public class KafkaClusterOwnershipTest {

    @Test
    public void tagged_names_live_under_dimster_tag() {
        KafkaCluster cluster = new KafkaCluster(null, new KafkaConfig(), "localB");
        assertEquals("dimster-localB", cluster.topicPrefix());
        assertEquals("dimster-localB-grp-", cluster.groupPrefix());
        assertEquals("dimster-localB-avail-probe", cluster.availProbeTopic());
        assertEquals("dimst-no-delete-localB", cluster.persistentTopicPrefix());
    }

    @Test
    public void untagged_names_are_the_plain_dimster_ones() {
        KafkaCluster cluster = new KafkaCluster(null, new KafkaConfig(), null);
        assertEquals(KafkaCluster.TOPIC_PREFIX, cluster.topicPrefix());
        assertEquals(KafkaCluster.TEST_GROUP_PREFIX, cluster.groupPrefix());
        assertEquals(KafkaCluster.AVAIL_PROBE_TOPIC, cluster.availProbeTopic());
        assertEquals(KafkaCluster.PERSISTENT_TOPIC_PREFIX, cluster.persistentTopicPrefix());
    }

    @Test
    public void a_tag_owns_only_its_own_topics() {
        assertTrue(KafkaCluster.ownsTopic("a", "dimster-a-Xy_z-0000"));
        assertTrue(KafkaCluster.ownsTopic("a", "dimster-a-Xy-z-0000-out"));
        assertTrue(KafkaCluster.ownsTopic("a", "dimster-a-avail-probe"));
        // Tags that extend or resemble "a" are other envs' topics.
        assertFalse(KafkaCluster.ownsTopic("a", "dimster-ab-Xyz-0000"));
        assertFalse(KafkaCluster.ownsTopic("a", "dimster-a.b-Xyz-0000"));
        assertFalse(KafkaCluster.ownsTopic("ab", "dimster-a-bXyz-0000"));
        // Untagged names and kept backlogs belong to no tag.
        assertFalse(KafkaCluster.ownsTopic("a", "dimster-Xyz-0000"));
        assertFalse(KafkaCluster.ownsTopic("a", "dimster-avail-probe"));
        assertFalse(KafkaCluster.ownsTopic("a", "dimst-no-delete-a-0123456789abcdef-0000"));
    }

    @Test
    public void a_tag_owns_only_its_own_groups() {
        assertTrue(KafkaCluster.ownsGroup("a", "dimster-a-grp-000-xyz"));
        assertTrue(KafkaCluster.ownsGroup("a", "dimster-a-grp-flink-xyz"));
        assertFalse(KafkaCluster.ownsGroup("a", "dimster-ab-grp-000-xyz"));
        assertFalse(KafkaCluster.ownsGroup("a", "dimster-grp-000-xyz"));
        // Untagged cleanup keeps to untagged groups.
        assertTrue(KafkaCluster.ownsGroup(null, "dimster-grp-000-xyz"));
        assertFalse(KafkaCluster.ownsGroup(null, "dimster-a-grp-000-xyz"));
    }

    @Test
    public void untagged_cleanup_keeps_its_original_reach() {
        // Embedded mode keeps the pre-tag behaviour: every dimster* topic, but
        // never a kept backlog.
        assertTrue(KafkaCluster.ownsTopic(null, "dimster-Xyz-0000"));
        assertTrue(KafkaCluster.ownsTopic(null, "dimster-avail-probe"));
        assertTrue(KafkaCluster.ownsTopic(null, "dimster-a-Xyz-0000"));
        assertFalse(KafkaCluster.ownsTopic(null, "dimst-no-delete-0123456789abcdef-0000"));
    }

    @Test
    public void tags_must_not_contain_a_dash_or_be_empty() {
        KafkaCluster.validateTag(null);
        KafkaCluster.validateTag("localMid");
        KafkaCluster.validateTag("a.b_c9");
        for (String bad : List.of("", "a-b", "a b", "a/b", "ä")) {
            assertThrows(bad, IllegalArgumentException.class, () -> KafkaCluster.validateTag(bad));
            assertThrows(bad, IllegalArgumentException.class,
                    () -> new KafkaCluster(null, new KafkaConfig(), bad));
        }
    }

    @Test
    public void cleanup_deletes_only_the_tags_topics_and_groups() {
        Set<String> topics = Set.of("dimster-a-X1-0000", "dimster-a-avail-probe", "dimster-a-X2-0000",
                "dimster-b-X3-0000", "dimster-Old-0000", "dimst-no-delete-a-0123456789abcdef-0000");
        Set<String> groups = Set.of("dimster-a-grp-000-x", "dimster-b-grp-000-y", "dimster-grp-000-z");
        FakeAdmin fake = new FakeAdmin(topics, groups);
        KafkaCluster cluster = new KafkaCluster(fake.proxy(), new KafkaConfig(), "a");

        cluster.deleteTestTopics(Set.of("dimster-a-X2-0000"));
        cluster.deleteTestGroups();

        assertEquals(Set.of("dimster-a-X1-0000", "dimster-a-avail-probe"), fake.deletedTopics);
        assertEquals(Set.of("dimster-a-grp-000-x"), fake.deletedConsumerGroups);
        assertEquals(Set.of("dimster-a-grp-000-x"), fake.deletedShareGroups);
    }

    /** The few RawAdmin calls cleanup makes, recorded; anything else is unexpected. */
    private static final class FakeAdmin {
        final Set<String> topics;
        final Set<String> groups;
        final Set<String> deletedTopics = new HashSet<>();
        final Set<String> deletedConsumerGroups = new HashSet<>();
        final Set<String> deletedShareGroups = new HashSet<>();

        FakeAdmin(Set<String> topics, Set<String> groups) {
            this.topics = topics;
            this.groups = groups;
        }

        @SuppressWarnings("unchecked")
        RawAdmin proxy() {
            return (RawAdmin) Proxy.newProxyInstance(RawAdmin.class.getClassLoader(),
                    new Class<?>[]{RawAdmin.class}, (p, method, args) -> switch (method.getName()) {
                        case "listTopicNames" -> topics;
                        case "deleteTopics" -> {
                            deletedTopics.addAll((Set<String>) args[0]);
                            yield null;
                        }
                        // Same ids for both kinds, so each cleanup pass sees every group.
                        case "listGroupIds" -> groups;
                        case "deleteConsumerGroups" -> {
                            deletedConsumerGroups.addAll((Collection<String>) args[0]);
                            yield null;
                        }
                        case "deleteShareGroups" -> {
                            deletedShareGroups.addAll((Collection<String>) args[0]);
                            yield null;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }
    }
}
