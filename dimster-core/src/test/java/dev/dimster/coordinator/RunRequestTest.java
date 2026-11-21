package dev.dimster.coordinator;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import org.junit.Test;

/** The run request's topic tag, which scopes a coordinator's Kafka topics and groups. */
public class RunRequestTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    public void topic_tag_is_read_from_the_request() throws Exception {
        RunRequest request = JSON.readValue("{\"mode\":\"run\",\"topicTag\":\"localB\"}", RunRequest.class);
        assertEquals("localB", request.topicTag());
    }

    @Test
    public void a_request_without_a_tag_keeps_the_untagged_names() throws Exception {
        RunRequest request = JSON.readValue("{\"mode\":\"run\"}", RunRequest.class);
        assertNull(request.topicTag());
    }

    @Test
    public void a_tag_that_could_overlap_another_is_rejected() {
        // A '-' would let "a" own "a-b"'s topics, so the request is refused
        // before the coordinator starts (the /run handler answers 400).
        assertThrows(ValueInstantiationException.class,
                () -> JSON.readValue("{\"mode\":\"run\",\"topicTag\":\"a-b\"}", RunRequest.class));
    }
}
