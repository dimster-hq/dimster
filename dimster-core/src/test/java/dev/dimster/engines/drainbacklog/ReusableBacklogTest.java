package dev.dimster.engines.drainbacklog;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dimster.engines.drainbacklog.ReusableBacklog.Provenance;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * What each drain of a reused backlog reports. Backlogs are built outside the
 * test points, so the build time is reported once, on the backlog's first
 * recorded drain, and later drains point back at that one.
 */
public class ReusableBacklogTest {

    private static ReusableBacklog built(long buildMs) {
        var b = new ReusableBacklog();
        b.buildDurationMs = buildMs;
        return b;
    }

    @Test
    public void first_recorded_drain_reports_the_build() {
        assertEquals(new Provenance(null, 72_000, false), built(72_000).recordDrain(0, false));
    }

    @Test
    public void later_drains_point_at_the_first_and_report_no_build() {
        ReusableBacklog b = built(72_000);
        b.recordDrain(2, false);
        assertEquals(new Provenance(2, 0, false), b.recordDrain(3, false));
        assertEquals(new Provenance(2, 0, false), b.recordDrain(4, false));
    }

    @Test
    public void the_warm_up_is_not_a_recorded_drain_but_is_noted_on_the_first() {
        ReusableBacklog b = built(72_000);
        assertNull(b.recordDrain(0, true));
        assertEquals(new Provenance(null, 72_000, true), b.recordDrain(0, false));
        assertEquals(new Provenance(0, 0, false), b.recordDrain(1, false));
    }

    /** A resumed run picks the backlog up from the checkpointed partial result. */
    @Test
    public void provenance_survives_a_checkpoint() throws Exception {
        ReusableBacklog b = built(72_000);
        b.builtForWarmUp = true;
        b.recordDrain(0, true);
        b.recordDrain(0, false);
        ObjectMapper json = new ObjectMapper();
        ReusableBacklog back = json.readValue(json.writeValueAsString(b), ReusableBacklog.class);
        assertTrue(back.builtForWarmUp);
        assertEquals(72_000, back.buildDurationMs);
        assertEquals(new Provenance(0, 0, false), back.recordDrain(1, false));
    }
}
