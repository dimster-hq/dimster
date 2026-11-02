package dev.dimster.engines.run;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * The measured-phase window is read back by the CLI (results.rs
 * extract_resource_windows) to query Prometheus per test point, so its JSON
 * names and format are a contract.
 */
public class RunResultTest {

    @Test
    public void testEntrySerialisesItsMeasuredWindowAsInstants() throws Exception {
        RunResult.TestEntry entry = new RunResult.TestEntry();
        entry.label = "p1";
        entry.measureStartTime = Instant.ofEpochMilli(1_000).toString();
        entry.measureEndTime = Instant.ofEpochMilli(61_000).toString();

        JsonNode json = new ObjectMapper().readTree(new ObjectMapper().writeValueAsString(entry));

        assertEquals("1970-01-01T00:00:01Z", json.get("measureStartTime").asText());
        assertEquals("1970-01-01T00:01:01Z", json.get("measureEndTime").asText());
    }

    @Test
    public void testEntryWithoutAWindowOmitsIt() throws Exception {
        // As on the synthetic "(merged)" repeat-aggregate.
        RunResult.TestEntry entry = new RunResult.TestEntry();
        entry.label = "p1";

        JsonNode json = new ObjectMapper().readTree(new ObjectMapper().writeValueAsString(entry));

        assertFalse(json.has("measureStartTime"));
        assertFalse(json.has("measureEndTime"));
    }
}
