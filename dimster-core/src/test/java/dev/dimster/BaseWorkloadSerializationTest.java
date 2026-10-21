package dev.dimster;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that BaseWorkload's payloadFile field is omitted from result JSON
 * when unset (the default), and included when the user explicitly sets it.
 *
 * Background: payloadFile is only consumed when useRandomizedPayloads=false.
 * Previously its default value ("payload/payload-1KB.data") leaked into result
 * JSON for every run, falsely implying it was the active payload source.
 */
public class BaseWorkloadSerializationTest {

    @Test
    public void payloadFileOmittedWhenNull() throws Exception {
        var bw = new BaseWorkload();
        String json = new ObjectMapper().writeValueAsString(bw);
        assertFalse("payloadFile should be omitted when null: " + json,
                json.contains("payloadFile"));
    }

    @Test
    public void payloadFileIncludedWhenSet() throws Exception {
        var bw = new BaseWorkload();
        bw.useRandomizedPayloads = false;
        bw.payloadFile = "payload/custom.data";
        String json = new ObjectMapper().writeValueAsString(bw);
        assertTrue("payloadFile should appear when set: " + json,
                json.contains("\"payloadFile\":\"payload/custom.data\""));
    }

    @Test
    public void datasetOmittedWhenNullAndRoundTripsWhenSet() throws Exception {
        var mapper = new ObjectMapper();
        assertFalse(mapper.writeValueAsString(new BaseWorkload()).contains("dataset"));

        var bw = new BaseWorkload();
        bw.dataset = new dev.dimster.datagen.DatasetSpec();
        bw.dataset.preset = "clicks";
        bw.dataset.format = dev.dimster.datagen.RecordFormat.JSON;
        String json = mapper.writeValueAsString(bw);
        assertTrue(json, json.contains("\"dataset\":{\"preset\":\"clicks\",\"format\":\"JSON\"}"));
        var copy = mapper.readValue(json, BaseWorkload.class);
        assertTrue("clicks".equals(copy.dataset.preset));
    }
}
